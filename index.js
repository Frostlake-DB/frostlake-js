/**
 * A zero-dependency Node.js driver for Frostlake, speaking the engine's HTTP
 * protocol against a running DatabaseHttpServer.
 *
 *   import { connect } from "frostlake";
 *   const conn = await connect("frostlake://localhost:18082/MY_DB?schema=PUBLIC");
 *   const { rows } = await conn.execute("SELECT id, name FROM people WHERE id = ?", [1]);
 *
 * Parameters are inlined client-side (the protocol has no server-side binding),
 * with the same rules as Frostlake's JDBC driver. Rows are plain objects keyed by
 * column name, or arrays in column order with { rowMode: "array" }; integral
 * NUMBER columns arrive as JS numbers, BOOLEAN as booleans, temporals as their
 * ISO text, BINARY as Buffers, and ARRAY/OBJECT/VARIANT as Snowflake's text
 * rendering.
 */

export class FrostlakeError extends Error {}

/**
 * The engine no longer holds the connection's session, and the statement did not run.
 *
 * The session expired, was released, or went with a server restart, and it held something
 * a fresh session would not have: an open transaction, or context set up with USE, SET,
 * ALTER SESSION or a temporary object. Running the statement in a fresh session would put
 * it somewhere its author did not intend, so it is refused instead. Also raised when the
 * engine refuses the fresh session that was to replace a lost one. The connection stays
 * usable: its next statement starts a fresh session on the DSN's scope.
 */
export class SessionLostError extends FrostlakeError {}

export async function connect(dsn) {
  const cfg = parseDsn(dsn);
  const conn = new Connection(cfg);
  await conn.ping();
  return conn;
}

function parseDsn(dsn) {
  let u;
  try {
    u = new URL(dsn);
  } catch {
    throw new FrostlakeError(`invalid DSN: ${dsn}`);
  }
  if (u.protocol !== "frostlake:" && u.protocol !== "http:") {
    throw new FrostlakeError("DSN must start with frostlake:// or http://");
  }
  if (!u.host) throw new FrostlakeError("DSN is missing host[:port]");
  // frostlake:// without an explicit port means the server default, 18082
  // (an http:// DSN keeps URL semantics, port 80).
  const host = !u.port && u.protocol === "frostlake:" ? `${u.host}:18082` : u.host;
  return {
    baseUrl: `http://${host}`,
    database: u.pathname.replace(/^\/+|\/+$/g, "") || null,
    schema: u.searchParams.get("schema"),
  };
}

function quoteIdent(name) {
  // A valid unquoted identifier passes through bare in either case — the
  // engine uppercases it, like Snowflake and like the JDBC driver's URL
  // handling. Anything else is quoted, preserving exact case.
  return /^[A-Za-z_][A-Za-z0-9_$]*$/.test(name) ? name : `"${name.replaceAll('"', '""')}"`;
}

export class Connection {
  #baseUrl;
  #sessionId = null;
  // The DSN's scope: the statements that put a fresh session where the connection asked
  // to be. #pendingUse holds the part not yet on the session; all of it goes back on a
  // session that replaces a lost one.
  #scope;
  #pendingUse = [];
  #queue = Promise.resolve();
  #closed = false;
  #closing = null;
  #closeTimeoutMs;
  // Whether the engine reports `newSession`, which arrived together with requireSession
  // and DELETE /api/sessions. null until the first answer that names a session.
  #tracksSessions = null;
  // Set once a statement left state behind that a fresh session would not have.
  #dirty = false;
  #inTransaction = false;
  autoCommit = true;

  constructor(cfg) {
    this.#baseUrl = cfg.baseUrl;
    if (cfg.database) this.#pendingUse.push(`USE DATABASE ${quoteIdent(cfg.database)}`);
    if (cfg.schema) this.#pendingUse.push(`USE SCHEMA ${quoteIdent(cfg.schema)}`);
    this.#scope = [...this.#pendingUse];
    this.#closeTimeoutMs = cfg.closeTimeoutMs ?? CLOSE_TIMEOUT_MS;
  }

  /** The engine's id for the connection's session, once it has named one; else null. */
  get sessionId() {
    return this.#sessionId;
  }

  async ping() {
    let resp;
    try {
      resp = await fetch(`${this.#baseUrl}/api/health`);
    } catch (e) {
      throw new FrostlakeError(`cannot reach server at ${this.#baseUrl}: ${e.message}`);
    }
    if (!resp.ok) throw new FrostlakeError(`server unhealthy: HTTP ${resp.status}`);
  }

  /**
   * Execute one statement. Returns { columns, rows, rowCount } where rows are
   * objects keyed by column name and rowCount is the affected-row count for DML.
   * With { rowMode: "array" } each row is instead its cells in column order,
   * which keeps columns that share a name apart.
   *
   * Statements are serialized per connection, in call order: the sessionId is
   * only learned from the first response, so concurrent round-trips would each
   * get their own server session and split the connection's state.
   */
  async execute(sql, binds = [], options = {}) {
    if (this.#closed) throw new FrostlakeError("connection is closed");
    const { multiStatementCount, rowMode = "object" } = options;
    if (rowMode !== "object" && rowMode !== "array") {
      throw new FrostlakeError(`rowMode must be "object" or "array", not ${rowMode}`);
    }
    const rendered = binds.length > 0 ? substitute(sql, binds) : sql;
    const run = this.#queue.then(async () => {
      await this.#applyScope();
      return this.#run(rendered, multiStatementCount);
    });
    this.#queue = run.catch(() => {});
    return shapeResult(await run, rowMode);
  }

  async begin() {
    await this.#enqueue(async () => {
      await this.#applyScope();
      // BEGIN carries autocommit off itself. The connection's own flag follows only once
      // BEGIN took, in this same turn of the queue: a statement queued before begin() still
      // goes out with autocommit on, and a begin() that fails leaves it on.
      await this.#run("BEGIN", undefined, false);
      this.autoCommit = false;
    });
  }

  async commit() {
    await this.#enqueue(async () => {
      await this.#applyScope();
      await this.#run("COMMIT");
      this.autoCommit = true;
    });
  }

  async rollback() {
    await this.#enqueue(async () => {
      await this.#applyScope();
      await this.#run("ROLLBACK");
      this.autoCommit = true;
    });
  }

  // Runs `job` on the connection's queue, after everything queued before it.
  #enqueue(job) {
    if (this.#closed) throw new FrostlakeError("connection is closed");
    const run = this.#queue.then(job);
    this.#queue = run.catch(() => {});
    return run;
  }

  /**
   * Close the connection and release its engine session with DELETE /api/sessions/{id},
   * which also rolls back a transaction the session left open. An engine without that
   * endpoint gets a ROLLBACK for an open transaction instead, and keeps the session until
   * its own idle expiry. Statements already queued finish first. The release is one
   * best-effort request bounded to five seconds: closing never rejects, and closing again
   * sends nothing.
   */
  close() {
    if (this.#closing === null) {
      this.#closed = true;
      this.#closing = this.#queue.then(() => this.#release());
      this.#queue = this.#closing;
    }
    return this.#closing;
  }

  // A failed USE stays queued, so every later statement keeps failing instead of
  // silently running against the server's default database. These are the driver's
  // own, one statement each, so they never carry the count the caller declared for
  // their own statement.
  async #applyScope() {
    let restarted = false;
    while (this.#pendingUse.length > 0) {
      const pending = this.#pendingUse;
      const out = await this.#post(pending[0]);
      if (out === null) {
        // Gone before the scope was on it, the session held nothing yet: the scope starts
        // over on a fresh one. Only once, though; the engine refusing a session it has
        // just started is reported instead.
        if (restarted) throw new SessionLostError("the engine refused a session it had just started");
        restarted = true;
        this.#sessionId = null;
        this.#pendingUse = [...this.#scope];
        continue;
      }
      if (!out.success) throw new FrostlakeError(out.errorMessage || "statement failed");
      // An answer that says the engine replaced the session put the whole scope back in
      // line (#absorb), and that line starts over from its first statement.
      if (this.#pendingUse === pending) pending.shift();
    }
  }

  // `autoCommit` is this request's own: the connection's, unless the caller says otherwise
  // (BEGIN goes out with it off before the connection's flag follows).
  async #run(sql, multiStatementCount, autoCommit = this.autoCommit) {
    let out = await this.#post(sql, multiStatementCount, autoCommit);
    if (out === null) out = await this.#recover(sql, multiStatementCount, autoCommit);
    if (!out.success) throw new FrostlakeError(out.errorMessage || "statement failed");
    this.#track(sql, autoCommit);
    return out;
  }

  // One POST /api/execute, without any recovery: the engine's answer, or null when it
  // refused the session id as unknown (requireSession), in which case nothing ran.
  async #post(sql, multiStatementCount, autoCommit = this.autoCommit) {
    const payload = { sql, autoCommit };
    const sentId = Boolean(this.#sessionId);
    let required = false;
    if (sentId) {
      payload.sessionId = this.#sessionId;
      // Resume this session or refuse: without the flag the engine starts a fresh session
      // under the same id when the old one is gone, and the statement runs without the
      // context the connection set up. Sent only to an engine that has shown it knows it.
      if (this.#tracksSessions === true) {
        payload.requireSession = true;
        required = true;
      }
    }
    // Absent unless the caller asked, so the session's MULTI_STATEMENT_COUNT still answers for
    // every request that says nothing. Zero is a real answer ("any number"), not nothing to say.
    if (multiStatementCount !== undefined && multiStatementCount !== null) {
      payload.multiStatementCount = multiStatementCount;
    }
    let resp;
    try {
      resp = await fetch(`${this.#baseUrl}/api/execute`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
    } catch (e) {
      throw new FrostlakeError(`request failed: ${e.message}`);
    }
    // Failed statements answer with a non-2xx status AND the error payload in the body.
    let out;
    try {
      out = await resp.json();
    } catch {
      throw new FrostlakeError(`HTTP ${resp.status} with unreadable body`);
    }
    if (required && resp.status === 404 && !out.success && !out.sessionId) return null;
    this.#absorb(out, sentId);
    return out;
  }

  // Take the session an answer names, and what it says about the engine.
  #absorb(out, sentId) {
    if (!out.sessionId) return;
    this.#sessionId = out.sessionId;
    if (typeof out.newSession === "boolean") {
      this.#tracksSessions = true;
      // The engine ran the statement in a fresh session in place of ours: whatever the old
      // one held is gone, and the scope goes back on before the next statement.
      if (out.newSession && sentId) this.#forgetSession();
    } else if (this.#tracksSessions === null) {
      this.#tracksSessions = false;
    }
  }

  #forgetSession() {
    this.#inTransaction = false;
    this.#dirty = false;
    this.#pendingUse = [...this.#scope];
  }

  // The engine no longer knows the session (it expired, was released, or the server
  // restarted), and nothing ran. With a transaction or a moved context gone with it,
  // re-running would put the statement somewhere its author did not intend, so that is
  // refused; otherwise a fresh session on the DSN's scope takes over and the statement is
  // sent once more.
  async #recover(sql, multiStatementCount, autoCommit) {
    const hadTransaction = this.#inTransaction;
    const hadContext = this.#dirty;
    this.#sessionId = null;
    this.#forgetSession();
    if (hadTransaction) {
      // The transaction ended with its session, as a rollback would have ended it.
      this.autoCommit = true;
      throw new SessionLostError(
        "the engine no longer holds this connection's session (it expired, was released, or the server restarted), so its open transaction is gone; the statement did not run");
    }
    if (hadContext) {
      throw new SessionLostError(
        "the engine no longer holds this connection's session (it expired, was released, or the server restarted), and the context set up on it (USE, SET, ALTER SESSION or a temporary object) went with it, so the statement was not re-run; the next statement starts a fresh session on the connection's scope");
    }
    await this.#applyScope();
    const out = await this.#post(sql, multiStatementCount, autoCommit);
    if (out === null) throw new SessionLostError("the engine refused a session it had just started");
    return out;
  }

  // Follow what a statement that ran did to the session: whether it left state a fresh
  // session would not have, and whether it opened or ended a transaction. With autocommit
  // off, a statement outside an explicit transaction opens one of its own.
  #track(sql, autoCommit) {
    for (const statement of splitStatements(sql)) {
      if (touchesSession(statement)) this.#dirty = true;
      const effect = transactionEffect(statement);
      if (effect === TRANSACTION_BEGINS) this.#inTransaction = true;
      else if (effect === TRANSACTION_ENDS) this.#inTransaction = false;
      else if (!autoCommit) this.#inTransaction = true;
    }
  }

  async #release() {
    const sessionId = this.#sessionId;
    const inTransaction = this.#inTransaction;
    this.#sessionId = null;
    this.#inTransaction = false;
    if (!sessionId) return;
    const signal = AbortSignal.timeout(this.#closeTimeoutMs);
    try {
      let resp = null;
      if (this.#tracksSessions === true) {
        resp = await fetch(`${this.#baseUrl}/api/sessions/${encodeURIComponent(sessionId)}`,
          { method: "DELETE", signal });
      } else if (inTransaction) {
        resp = await fetch(`${this.#baseUrl}/api/execute`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ sql: "ROLLBACK", autoCommit: this.autoCommit, sessionId }),
          signal,
        });
      }
      await resp?.arrayBuffer();
    } catch {
      // Best effort: the engine's own idle expiry is the fallback.
    }
  }
}

// How long close() waits for the engine to release the session.
const CLOSE_TIMEOUT_MS = 5000;

// The complete set of count columns the engine emits for DML results, matched
// exactly so an aliased look-alike ("number of rows once") stays a query
// result. "multi-joined" is recognized, so an UPDATE result collapses, but per
// Snowflake semantics it is not part of the affected-row count.
const SUMMED_COUNT_COLUMNS = new Set([
  "number of rows inserted",
  "number of rows updated",
  "number of rows deleted",
]);
const COUNT_COLUMNS = new Set([...SUMMED_COUNT_COLUMNS, "number of multi-joined rows updated"]);

function shapeResult(out, rowMode) {
  const sets = (out.resultSets ?? []).map((rs) => shapeOne(rs, rowMode));
  if (sets.length === 0) return { columns: [], rows: [], rowCount: 0, resultSets: [] };
  const { columns, rows, rowCount } = sets[0];
  return { columns, rows, rowCount, resultSets: sets };
}

function shapeOne(rs, rowMode) {
  // `length` is a text column's width in characters and a binary column's in
  // bytes; every other type sends none, and so does a server that predates the
  // field. Absent stays absent — null, never a width of 0.
  const columns = (rs.columns ?? []).map((c) => ({
    name: c.name,
    dataType: c.dataType,
    length: c.length ?? null,
  }));
  const types = columns.map((c) => String(c.dataType ?? "").toUpperCase());
  const cells = (rs.rows ?? []).map((raw) => columns.map((c, i) => convert(raw[i], types[i])));
  // DML answers with a single row of count columns: one for INSERT/DELETE,
  // several for MERGE and UPDATE. Like the JDBC driver, sum them into rowCount.
  const names = columns.map((c) => c.name.toLowerCase());
  if (cells.length === 1 && names.length > 0
      && names.every((n) => COUNT_COLUMNS.has(n))
      && names.some((n) => SUMMED_COUNT_COLUMNS.has(n))) {
    let total = 0;
    names.forEach((n, i) => {
      if (SUMMED_COUNT_COLUMNS.has(n)) total += Number(cells[0][i]);
    });
    return { columns: [], rows: [], rowCount: total };
  }
  // Keyed by name, a row keeps only the last of two columns that share one (a
  // self-join's e.NAME and m.NAME); an array row keeps every cell, in column order.
  const rows = rowMode === "array" ? cells : cells.map((row) => {
    const byName = {};
    columns.forEach((c, i) => {
      byName[c.name] = row[i];
    });
    return byName;
  });
  return { columns, rows, rowCount: rows.length };
}

function convert(value, dataType) {
  if (value === null || value === undefined) return null;
  // BINARY crosses the JSON wire as bare hex — decode it so a bound Buffer
  // reads back as a Buffer. Everything else passes through as the wire
  // delivers it: numbers/booleans/text natively, and ARRAY/OBJECT/VARIANT
  // cells as Snowflake's TEXT rendering — a SQL NULL array element appears
  // as `undefined`, so that text is not always parseable JSON. Precision
  // limits are declared, not silent: see README.
  if (dataType.includes("BINARY") && typeof value === "string"
      && /^(?:[0-9A-Fa-f]{2})*$/.test(value)) {
    return Buffer.from(value, "hex");
  }
  return value;
}

// ── client-side parameter binding ──────────────────────────────────────────

export function substitute(sql, binds) {
  let out = "";
  let next = 0;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (ch === "'") {
      const j = skipString(sql, i);
      out += sql.slice(i, j);
      i = j - 1;
    } else if (ch === '"') {
      const j = skipQuoted(sql, i, '"');
      out += sql.slice(i, j);
      i = j - 1;
    } else if (ch === "-" && sql[i + 1] === "-") {
      const j = skipLine(sql, i);
      out += sql.slice(i, j);
      i = j - 1;
    } else if (ch === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      const j = end < 0 ? sql.length : end + 2;
      out += sql.slice(i, j);
      i = j - 1;
    } else if (ch === "/" && sql[i + 1] === "/") {
      const j = skipLine(sql, i);
      out += sql.slice(i, j);
      i = j - 1;
    } else if (NAME_START.test(ch)) {
      // a name is read whole, as the lexer reads one: the $$ in a$$b opens no dollar-quoted body
      const j = skipName(sql, i);
      out += sql.slice(i, j);
      i = j - 1;
    } else if (ch === "$" && sql[i + 1] === "$") {
      // dollar-quoted string — non-greedy to the next $$, like the lexer's
      // DOLLAR_QUOTED_STRING: '$$' .*? '$$'
      const end = sql.indexOf("$$", i + 2);
      const j = end < 0 ? sql.length : end + 2;
      out += sql.slice(i, j);
      i = j - 1;
    } else if (ch === "?") {
      if (next >= binds.length) throw new FrostlakeError("not enough bind values for placeholders");
      out += formatLiteral(binds[next++]);
    } else {
      out += ch;
    }
  }
  if (next < binds.length) {
    throw new FrostlakeError(`too many bind values: ${binds.length} given, ${next} placeholder${next === 1 ? "" : "s"}`);
  }
  return out;
}

function skipString(s, i) {
  let j = i + 1;
  while (j < s.length) {
    if (s[j] === "\\") j += 2;                     // backslash always escapes
    else if (s[j] === "'") {
      if (s[j + 1] === "'") j += 2;
      else return j + 1;
    } else j++;
  }
  return j;
}

function skipQuoted(s, i, q) {
  let j = i + 1;
  while (j < s.length) {
    if (s[j] === q) {
      if (s[j + 1] === q) { j += 2; continue; }
      return j + 1;
    }
    j++;
  }
  return j;
}

// A line comment ends at a line feed or a carriage return, whichever comes first, as the
// lexer's LINE_COMMENT does: ('--' | '//') ~[\r\n]*. The line end itself is left unread.
function skipLine(s, i) {
  let j = i;
  while (j < s.length && s[j] !== "\n" && s[j] !== "\r") j++;
  return j;
}

const NAME_START = /[A-Za-z_]/;
const NAME_PART = /[A-Za-z0-9_$]/;

// The end of the name that starts at i, read as the lexer reads an IDENTIFIER:
// [a-zA-Z_][a-zA-Z0-9_$]*.
function skipName(s, i) {
  let j = i + 1;
  while (j < s.length && NAME_PART.test(s[j])) j++;
  return j;
}

function encodeString(text) {
  return "'" + text.replaceAll("\\", "\\\\").replaceAll("'", "''") + "'";
}

export function formatLiteral(v) {
  if (v === null || v === undefined) return "NULL";
  switch (typeof v) {
    case "boolean": return String(v);
    // A negative number goes in parentheses, which is one value wherever it lands: bare,
    // 3-? would become 3--5, a comment, and ?::VARCHAR would cast before it negates.
    case "number":
      if (!Number.isFinite(v)) return raise(`non-finite number ${v}`);
      return v < 0 ? `(${v})` : String(v);
    case "bigint": return v < 0n ? `(${v})` : v.toString();
    case "string": return encodeString(v);
    case "object":
      if (v instanceof Date) return `'${v.toISOString().replace("Z", "")}'::TIMESTAMP_NTZ`;
      if (v instanceof Uint8Array) return "X'" + Buffer.from(v).toString("hex") + "'";
      if (Array.isArray(v)) return "[" + v.map(formatLiteral).join(", ") + "]";
      return raise(`unsupported bind type ${v.constructor?.name ?? "object"}`);
    default:
      return raise(`unsupported bind type ${typeof v}`);
  }
}

function raise(msg) {
  throw new FrostlakeError(msg);
}

// ── session tracking ───────────────────────────────────────────────────────

const TRANSACTION_BEGINS = "begins";
const TRANSACTION_ENDS = "ends";

// The words that may sit between CREATE/DROP/ALTER and the kind of object being named.
const OBJECT_MODIFIERS = new Set([
  "OR", "REPLACE", "TRANSIENT", "TEMPORARY", "TEMP", "VOLATILE", "LOCAL", "GLOBAL", "SECURE",
  "IF", "NOT", "EXISTS", "PUBLIC", "PRIVATE", "ICEBERG", "DYNAMIC", "HYBRID", "EVENT",
  "RECURSIVE", "MATERIALIZED", "EXTERNAL",
]);
const TEMPORARY_MODIFIERS = new Set(["TEMPORARY", "TEMP", "VOLATILE"]);

// Index just past the literal, quoted identifier, $$ body or comment starting at i, or -1
// when i is code: the constructs substitute() steps over.
function skipNonCode(sql, i) {
  const ch = sql[i];
  const next = sql[i + 1];
  // A name holds no separator, and the $$ inside one opens no body.
  if (NAME_START.test(ch)) return skipName(sql, i);
  if (ch === "'") return skipString(sql, i);
  if (ch === '"') return skipQuoted(sql, i, '"');
  if ((ch === "-" && next === "-") || (ch === "/" && next === "/")) return skipLine(sql, i);
  if (ch === "/" && next === "*") {
    const end = sql.indexOf("*/", i + 2);
    return end < 0 ? sql.length : end + 2;
  }
  if (ch === "$" && next === "$") {
    const end = sql.indexOf("$$", i + 2);
    return end < 0 ? sql.length : end + 2;
  }
  return -1;
}

// The request split on its top-level semicolons; one inside a literal, a quoted
// identifier, a $$ body or a comment does not split. Blank pieces are dropped. A scripting
// block is split along with everything else, which only makes the session checks more
// willing to flag a request: the safe direction to be wrong in.
function splitStatements(sql) {
  const pieces = [];
  let start = 0;
  let i = 0;
  while (i < sql.length) {
    const past = skipNonCode(sql, i);
    if (past >= 0) {
      i = past;
    } else if (sql[i] === ";") {
      pieces.push(sql.slice(start, i));
      i++;
      start = i;
    } else {
      i++;
    }
  }
  pieces.push(sql.slice(start));
  return pieces.filter((piece) => piece.trim() !== "");
}

function isWordChar(ch) {
  return ch === "_" || ch === "$" || /[\p{L}\p{Nd}]/u.test(ch);
}

// Up to `limit` leading words of a statement, upper-cased, skipping whitespace and
// comments and stopping at the first thing that is not a word.
function leadingWords(statement, limit) {
  const words = [];
  let i = 0;
  while (words.length < limit && i < statement.length) {
    const ch = statement[i];
    const next = statement[i + 1];
    if (/\s/.test(ch)) {
      i++;
    } else if ((ch === "-" && next === "-") || (ch === "/" && next === "/")) {
      i = skipLine(statement, i);
    } else if (ch === "/" && next === "*") {
      const end = statement.indexOf("*/", i + 2);
      i = end < 0 ? statement.length : end + 2;
    } else if (isWordChar(ch)) {
      const start = i;
      while (i < statement.length && isWordChar(statement[i])) i++;
      words.push(statement.slice(start, i).toUpperCase());
    } else {
      break;
    }
  }
  return words;
}

// Whether a statement leaves behind state a fresh session would not have: a moved scope
// (USE, CREATE or DROP of a DATABASE or SCHEMA), a session variable or setting (SET,
// UNSET, ALTER SESSION), or a temporary object. CREATE TABLE and its kind leave the
// session as it was.
function touchesSession(statement) {
  const [verb, ...rest] = leadingWords(statement, 16);
  if (verb === "USE" || verb === "SET" || verb === "UNSET") return true;
  if (verb !== "ALTER" && verb !== "CREATE" && verb !== "DROP") return false;
  let kind = 0;
  while (kind < rest.length && OBJECT_MODIFIERS.has(rest[kind])) kind++;
  const named = rest[kind];
  if (verb === "ALTER") return named === "SESSION";
  if (named === "DATABASE" || named === "SCHEMA") return true;
  return verb === "CREATE" && rest.slice(0, kind).some((word) => TEMPORARY_MODIFIERS.has(word));
}

// Whether a statement opens or ends a transaction. BEGIN on its own (or with
// TRANSACTION, WORK or NAME) opens one; BEGIN followed by a statement opens a scripting
// block instead.
function transactionEffect(statement) {
  const [first, second] = leadingWords(statement, 2);
  if (first === "BEGIN" && (second === undefined || ["TRANSACTION", "WORK", "NAME"].includes(second))) {
    return TRANSACTION_BEGINS;
  }
  if (first === "START" && second === "TRANSACTION") return TRANSACTION_BEGINS;
  if (first === "COMMIT" || first === "ROLLBACK") return TRANSACTION_ENDS;
  return null;
}
