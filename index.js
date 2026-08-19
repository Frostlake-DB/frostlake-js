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
 * column name; integral NUMBER columns arrive as JS numbers, BOOLEAN as booleans,
 * temporals as their ISO text, BINARY as Buffers, and ARRAY/OBJECT/VARIANT as
 * Snowflake's text rendering.
 */

export class FrostlakeError extends Error {}

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
  #pendingUse = [];
  #queue = Promise.resolve();
  #closed = false;
  autoCommit = true;

  constructor(cfg) {
    this.#baseUrl = cfg.baseUrl;
    if (cfg.database) this.#pendingUse.push(`USE DATABASE ${quoteIdent(cfg.database)}`);
    if (cfg.schema) this.#pendingUse.push(`USE SCHEMA ${quoteIdent(cfg.schema)}`);
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
   *
   * Statements are serialized per connection, in call order: the sessionId is
   * only learned from the first response, so concurrent round-trips would each
   * get their own server session and split the connection's state.
   */
  async execute(sql, binds = []) {
    if (this.#closed) throw new FrostlakeError("connection is closed");
    const rendered = binds.length > 0 ? substitute(sql, binds) : sql;
    const run = this.#queue.then(async () => {
      // A failed USE stays queued, so every later statement keeps failing
      // instead of silently running against the server's default database.
      while (this.#pendingUse.length > 0) {
        await this.#roundTrip(this.#pendingUse[0]);
        this.#pendingUse.shift();
      }
      return this.#roundTrip(rendered);
    });
    this.#queue = run.catch(() => {});
    return shapeResult(await run);
  }

  async begin() {
    this.autoCommit = false;
    await this.execute("BEGIN");
  }

  async commit() {
    await this.execute("COMMIT");
    this.autoCommit = true;
  }

  async rollback() {
    await this.execute("ROLLBACK");
    this.autoCommit = true;
  }

  async close() {
    this.#closed = true;
  }

  async #roundTrip(sql) {
    const payload = { sql, autoCommit: this.autoCommit };
    if (this.#sessionId) payload.sessionId = this.#sessionId;
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
    if (out.sessionId) this.#sessionId = out.sessionId;
    if (!out.success) throw new FrostlakeError(out.errorMessage || "statement failed");
    return out;
  }
}

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

function shapeResult(out) {
  const sets = (out.resultSets ?? []).map(shapeOne);
  if (sets.length === 0) return { columns: [], rows: [], rowCount: 0, resultSets: [] };
  const { columns, rows, rowCount } = sets[0];
  return { columns, rows, rowCount, resultSets: sets };
}

function shapeOne(rs) {
  const columns = (rs.columns ?? []).map((c) => ({ name: c.name, dataType: c.dataType }));
  const types = columns.map((c) => String(c.dataType ?? "").toUpperCase());
  const rows = (rs.rows ?? []).map((raw) => {
    const row = {};
    columns.forEach((c, i) => {
      row[c.name] = convert(raw[i], types[i]);
    });
    return row;
  });
  // DML answers with a single row of count columns: one for INSERT/DELETE,
  // several for MERGE and UPDATE. Like the JDBC driver, sum them into rowCount.
  const names = columns.map((c) => c.name.toLowerCase());
  if (rows.length === 1 && names.length > 0
      && names.every((n) => COUNT_COLUMNS.has(n))
      && names.some((n) => SUMMED_COUNT_COLUMNS.has(n))) {
    let total = 0;
    names.forEach((n, i) => {
      if (SUMMED_COUNT_COLUMNS.has(n)) total += Number(rows[0][columns[i].name]);
    });
    return { columns: [], rows: [], rowCount: total };
  }
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

function skipLine(s, i) {
  const j = s.indexOf("\n", i);
  return j < 0 ? s.length : j + 1;
}

function encodeString(text) {
  return "'" + text.replaceAll("\\", "\\\\").replaceAll("'", "''") + "'";
}

export function formatLiteral(v) {
  if (v === null || v === undefined) return "NULL";
  switch (typeof v) {
    case "boolean": return String(v);
    case "number": return Number.isFinite(v) ? String(v) : raise(`non-finite number ${v}`);
    case "bigint": return v.toString();
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
