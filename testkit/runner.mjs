/**
 * Replays the engine's testkit corpus through THIS driver.
 *
 * The corpus belongs to the engine: language-neutral JSON suites under
 * engine/src/test/resources/testkit/suites, their format in the SCHEMA.md beside
 * them. This file is only the JS runner, a port of the engine's Java reference
 * (dev.frostlake.testkit: JsonSuiteTest, Compare, HttpBackend). Every statement
 * travels the driver's core API, connect() then execute(), over HTTP to a
 * running DatabaseHttpServer, so suites added on the engine side are picked up
 * here with no change. `node --test` runs it too, through test/testkit.test.js,
 * when FL_CORPUS is set.
 *
 *   FL_CORPUS=/path/to/frostlake/engine/src/test/resources/testkit \
 *   FROSTLAKE_URL=frostlake://127.0.0.1:18082 node testkit/runner.mjs
 *
 * Environment:
 *   FL_CORPUS                 the testkit directory to replay, its suite files in
 *                             suites/*.json. Without it, the run skips.
 *   FROSTLAKE_URL             the engine to attach to. Without it, FROSTLAKE_CLASSPATH
 *                             (and JAVA_HOME) boot a private one, as the regular
 *                             suite does; with neither, the run skips.
 *   FROSTLAKE_TESTKIT_REPORT  the TSV report, one row per case; by default
 *                             results/testkit-js.tsv. missing-apis-js.md lands
 *                             beside it.
 *   FROSTLAKE_TESTKIT_FILTER  only the suites whose file name contains one of these
 *                             comma-separated words.
 *
 * Semantics, as the reference runs them:
 *   - a case whose skip clause names `js` or `http` is skipped: this driver rides
 *     the HTTP transport, so what that transport cannot do, it cannot either;
 *   - every case runs on a connection of its own, so on one session: the reset
 *     (MULTI_STATEMENT_COUNT = 0, a fresh test_db.test_schema, made current), then
 *     the steps in order, the first failed check ending the case;
 *   - capabilities: SESSION, COLUMN_NAMES, UPDATE_COUNT (the rowCount the driver
 *     folds a DML count grid into, or else the reference's derivation from a grid
 *     of "number of …" columns). There is no ERROR_CODE: a refusal carries its
 *     message only, so an expected error's code or sqlState is recorded as a
 *     missing API rather than failed;
 *   - rows are read as arrays ({ rowMode: "array" }), so columns that share a name
 *     each keep their own cell;
 *   - values compare after the reference's normalisation: NULL and booleans folded,
 *     anything numeric rounded to 10 significant digits, everything else trimmed
 *     text. A VARIANT, OBJECT or ARRAY cell arrives as its JSON text and is decoded
 *     one level, as the reference decodes it.
 *
 * Suite files run in name order: account-level objects outlive the per-case reset,
 * and the corpus is written for that order.
 */

import { spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { connect, FrostlakeError } from "../index.js";

const BACKEND = "js";
// The names a skip clause may use for this runner: its own, and the transport it rides.
const SKIP_NAMES = new Set([BACKEND, "http"]);

const RESET = [
  "ALTER SESSION SET MULTI_STATEMENT_COUNT = 0",
  "CREATE OR REPLACE DATABASE test_db",
  "USE DATABASE test_db",
  "CREATE OR REPLACE SCHEMA test_schema",
  "USE SCHEMA test_schema",
];

const CHECKOUT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
// What a run without FL_CORPUS says, and what test/testkit.test.js skips with.
export const NO_CORPUS = "set FL_CORPUS to frostlake's engine/src/test/resources/testkit to replay the testkit corpus";
const CELL_SEPARATOR = "\u001f";
const MAX_REPORTED = 20;
const MAX_DETAIL = 1000;

// ── value normalisation (Compare.norm) ─────────────────────────────────────

/**
 * The shared normalisation, applied to both sides before they compare: NULL and
 * the empty string alike, booleans in any case, and anything numeric rounded to
 * 10 significant digits (half up) with trailing zeros stripped, as plain text.
 */
function norm(raw) {
  if (raw === null || raw === undefined) return "NULL";
  const value = javaTrim(String(raw));
  if (value === "" || equalsIgnoreCase(value, "null")) return "NULL";
  if (equalsIgnoreCase(value, "true")) return "TRUE";
  if (equalsIgnoreCase(value, "false")) return "FALSE";
  const number = parseDecimal(value);
  return number === null ? value : roundedPlain(number);
}

// String.trim() in Java: every code unit up to U+0020 goes, and nothing else.
function javaTrim(text) {
  let start = 0;
  let end = text.length;
  while (start < end && text.charCodeAt(start) <= 0x20) start++;
  while (end > start && text.charCodeAt(end - 1) <= 0x20) end--;
  return text.slice(start, end);
}

// String.equalsIgnoreCase in Java: same length, then code unit by code unit.
function equalsIgnoreCase(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    const y = b[i];
    if (x === y) continue;
    const ux = simpleUpper(x);
    const uy = simpleUpper(y);
    if (ux === uy || ux.toLowerCase() === uy.toLowerCase()) continue;
    return false;
  }
  return true;
}

// Character.toUpperCase maps one code unit to one; a longer special casing (ß → SS) leaves it.
function simpleUpper(ch) {
  const upper = ch.toUpperCase();
  return upper.length === 1 ? upper : ch;
}

const DECIMAL_DIGIT = /^\p{Nd}$/u;

// Character.digit(ch, 10): a Unicode decimal digit's value, or -1. Every decimal-digit run
// in the Basic Multilingual Plane is a whole 0-9 sequence, so the run behind a digit
// counts its value.
function digitValue(ch) {
  const code = ch.charCodeAt(0);
  if (code >= 0x30 && code <= 0x39) return code - 0x30;
  if (code < 0x80 || !DECIMAL_DIGIT.test(ch)) return -1;
  let below = 0;
  while (DECIMAL_DIGIT.test(String.fromCharCode(code - below - 1))) below++;
  return below % 10;
}

/**
 * What new BigDecimal(text) reads: an optional sign, digits with at most one
 * decimal point (at least one digit), then an optional exponent. Returns
 * { negative, digits, scale } for digits × 10^-scale, or null for anything else.
 */
function parseDecimal(text) {
  let i = 0;
  let negative = false;
  if (text[i] === "+" || text[i] === "-") {
    negative = text[i] === "-";
    i++;
  }
  let digits = "";
  let fraction = 0;
  let dot = false;
  for (; i < text.length; i++) {
    const ch = text[i];
    if (ch === "e" || ch === "E") break;
    if (ch === ".") {
      if (dot) return null;
      dot = true;
      continue;
    }
    const digit = digitValue(ch);
    if (digit < 0) return null;
    digits += digit;
    if (dot) fraction++;
  }
  if (digits === "") return null;
  let exponent = 0;
  if (i < text.length) {
    i++;
    let negativeExponent = false;
    if (text[i] === "+" || text[i] === "-") {
      negativeExponent = text[i] === "-";
      i++;
    }
    if (i >= text.length) return null;
    for (; i < text.length; i++) {
      const digit = digitValue(text[i]);
      if (digit < 0) return null;
      exponent = exponent * 10 + digit;
      if (exponent > 2147483647) return null; // Java: exponent overflow
    }
    if (negativeExponent) exponent = -exponent;
  }
  const scale = fraction - exponent;
  if (scale > 2147483647 || scale < -2147483648) return null;
  return { negative, digits, scale };
}

// round(new MathContext(10)) — HALF_UP — then stripTrailingZeros().toPlainString().
function roundedPlain({ negative, digits, scale }) {
  let coefficient = digits.replace(/^0+/, "");
  if (coefficient === "") return "0";
  if (coefficient.length > 10) {
    const dropped = coefficient.length - 10;
    let kept = BigInt(coefficient.slice(0, 10));
    if (coefficient.charCodeAt(10) >= 0x35) kept += 1n; // the first dropped digit decides
    coefficient = kept.toString();
    scale -= dropped;
  }
  let end = coefficient.length;
  while (end > 1 && coefficient[end - 1] === "0") end--;
  scale -= coefficient.length - end;
  coefficient = coefficient.slice(0, end);
  let plain;
  if (scale <= 0) {
    plain = coefficient + "0".repeat(-scale);
  } else if (coefficient.length > scale) {
    plain = `${coefficient.slice(0, coefficient.length - scale)}.${coefficient.slice(coefficient.length - scale)}`;
  } else {
    plain = `0.${"0".repeat(scale - coefficient.length)}${coefficient}`;
  }
  return negative ? `-${plain}` : plain;
}

// ── reading a result the way the reference's HttpBackend does ───────────────

const SEMI_STRUCTURED = new Set(["VARIANT", "OBJECT", "ARRAY"]);

/**
 * A driver cell as the wire's text. The driver hands a BINARY cell over as a
 * Buffer decoded from the wire's upper-case hex, and every other cell as the
 * wire delivered it.
 */
function cellText(value) {
  if (value === null || value === undefined) return null;
  if (Buffer.isBuffer(value)) return value.toString("hex").toUpperCase();
  return String(value);
}

/**
 * The value a semi-structured cell carries, as the suites record it
 * (SemiStructuredCells.value): a cell that is a JSON string becomes its content,
 * and any other cell stays exactly as it came.
 */
function semiStructuredValue(cell) {
  if (cell === null) return null;
  const decoded = jsonStringContent(cell);
  return decoded === null ? cell : decoded;
}

// The reference's JSON reader, cut down to the one question asked of a cell: is the whole
// text a JSON string, and if so, what does it hold?
function jsonStringContent(text) {
  let at = skipJavaWhitespace(text, 0);
  if (text[at] !== '"') return null;
  at++;
  let out = "";
  for (;;) {
    if (at >= text.length) return null;
    const ch = text[at++];
    if (ch === '"') break;
    if (ch !== "\\") {
      out += ch;
      continue;
    }
    if (at >= text.length) return null;
    const escaped = text[at++];
    switch (escaped) {
      case '"': out += '"'; break;
      case "\\": out += "\\"; break;
      case "/": out += "/"; break;
      case "n": out += "\n"; break;
      case "t": out += "\t"; break;
      case "r": out += "\r"; break;
      case "b": out += "\b"; break;
      case "f": out += "\f"; break;
      case "u": {
        const hex = text.slice(at, at + 4);
        if (hex.length < 4 || !/^[+-]?[0-9A-Fa-f]+$/.test(hex)) return null;
        out += String.fromCharCode(parseInt(hex, 16) & 0xffff);
        at += 4;
        break;
      }
      default:
        return null;
    }
  }
  return skipJavaWhitespace(text, at) === text.length ? out : null;
}

// Character.isWhitespace, as the reference's reader skips it: the ASCII controls TAB to CR and
// FS to US, and every Unicode space separator except the three non-breaking ones.
function skipJavaWhitespace(text, at) {
  while (at < text.length && isJavaWhitespace(text.charCodeAt(at))) at++;
  return at;
}

function isJavaWhitespace(code) {
  return (code >= 0x09 && code <= 0x0d) || (code >= 0x1c && code <= 0x20) || code === 0x1680
    || (code >= 0x2000 && code <= 0x2006) || (code >= 0x2008 && code <= 0x200a)
    || code === 0x2028 || code === 0x2029 || code === 0x205f || code === 0x3000;
}

/**
 * One statement's outcome in the reference's shape: the first result's column
 * names and grid as text (null when there is no grid), the DML count (-1 when
 * none), or the refusal's message.
 */
function outcomeOf(result) {
  const first = result.resultSets[0];
  if (first === undefined) return { columns: null, rows: null, updateCount: -1, error: null };
  if (first.columns.length === 0) {
    // The driver folds a DML count grid into its affected-row count and hands no grid over.
    return { columns: [], rows: null, updateCount: first.rowCount, error: null };
  }
  const columns = first.columns.map((c) => c.name);
  const semi = first.columns.map((c) => SEMI_STRUCTURED.has(String(c.dataType ?? "").toUpperCase()));
  const rows = first.rows.map((row) => row.map((value, i) => {
    const cell = cellText(value);
    return semi[i] ? semiStructuredValue(cell) : cell;
  }));
  return { columns, rows, updateCount: derivedUpdateCount(columns, rows), error: null };
}

/**
 * The count a DML grid that the driver left as a result carries
 * (ExecResult.deriveUpdateCountFromGrid): the first cell of a single row whose
 * columns are all named "number of …".
 */
function derivedUpdateCount(columns, rows) {
  if (columns.length === 0 || rows.length !== 1) return -1;
  for (const name of columns) {
    if (name === null || name === undefined || !String(name).toLowerCase().startsWith("number of")) return -1;
  }
  const cell = rows[0][0] === null ? "" : javaTrim(rows[0][0]);
  return /^[+-]?\d+$/.test(cell) ? Number(cell) : -1;
}

// The driver's own words for a request that never got an answer from the engine. Every
// other FrostlakeError out of execute() is the engine's refusal of the statement.
const TRANSPORT_FAILURE = /^(?:request failed: |HTTP \d+ with unreadable body$|connection is closed$)/;

class TransportError extends Error {}

async function run(conn, sql) {
  let result;
  try {
    // Array rows: keyed by name, a row would keep only the last of two same-named columns.
    result = await conn.execute(sql, [], { rowMode: "array" });
  } catch (e) {
    if (e instanceof FrostlakeError && !TRANSPORT_FAILURE.test(e.message)) {
      return { columns: null, rows: null, updateCount: -1, error: e.message };
    }
    throw new TransportError(e.message);
  }
  return outcomeOf(result);
}

// ── checking a step (Compare.check) ────────────────────────────────────────

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

// The reference reads a JSON number as a BigDecimal and anything else through String.valueOf.
function asString(value) {
  return value === null || value === undefined ? "null" : String(value);
}

function intField(object, key) {
  const value = object?.[key];
  return typeof value === "number" ? Math.trunc(value) : null;
}

function strField(object, key) {
  const value = object?.[key];
  return value === null || value === undefined ? null : String(value);
}

/**
 * Checks one step's expect block against its outcome. Returns
 * { passed, detail, missing }, where missing names the checks this transport
 * cannot express.
 */
function check(expect, outcome) {
  const error = isObject(expect) ? expect.error : undefined;
  if (isObject(error)) return checkRefusal(error, outcome);
  if (outcome.error !== null) return fail(`unexpected error: ${outcome.error}`);
  if (!isObject(expect)) return pass();
  if ("value" in expect) {
    const want = asString(expect.value);
    const got = firstCell(outcome.rows);
    if (norm(want) !== norm(got)) return fail(`value [${got}] != expected [${want}]`);
  }
  if (Array.isArray(expect.rows)) {
    const want = expect.rows.map((row) => (Array.isArray(row) ? row : []).map((c) => (c === null ? null : asString(c))));
    const diff = gridDiff(want, outcome.rows ?? [], expect.ordered === true);
    if (diff !== null) return fail(diff);
  }
  const rowCount = intField(expect, "rowCount");
  if (rowCount !== null) {
    const got = outcome.rows === null ? 0 : outcome.rows.length;
    if (got !== rowCount) return fail(`rowCount ${got} != expected ${rowCount}`);
  }
  if (Array.isArray(expect.columns)) {
    const mismatch = columnMismatch(expect.columns, outcome.columns ?? []);
    if (mismatch !== null) return fail(mismatch);
  }
  const updateCount = intField(expect, "updateCount");
  if (updateCount !== null && outcome.updateCount !== updateCount) {
    return fail(`updateCount ${outcome.updateCount} != expected ${updateCount}`);
  }
  return pass();
}

function checkRefusal(error, outcome) {
  if (outcome.error === null) return fail("expected an error, statement succeeded");
  const want = strField(error, "messageContains");
  if (want !== null && !outcome.error.toLowerCase().includes(want.toLowerCase())) {
    return fail(`error message [${outcome.error}] does not contain [${want}]`);
  }
  if (strField(error, "code") === null && strField(error, "sqlState") === null) return pass();
  return pass(["ERROR_CODE: cannot check error code/sqlState (backend reports message only)"]);
}

function pass(missing = []) {
  return { passed: true, detail: "", missing };
}

function fail(detail) {
  return { passed: false, detail, missing: [] };
}

function firstCell(rows) {
  if (rows === null || rows.length === 0 || rows[0].length === 0) return null;
  return rows[0][0];
}

function columnMismatch(expected, actual) {
  if (actual.length !== expected.length) {
    return `column count ${actual.length} != expected ${expected.length} [${actual.join(", ")}]`;
  }
  for (let i = 0; i < expected.length; i++) {
    if (!equalsIgnoreCase(asString(expected[i]), String(actual[i]))) {
      return `column[${i}] [${actual[i]}] != expected [${expected[i]}]`;
    }
  }
  return null;
}

function gridDiff(want, got, ordered) {
  const expected = canon(want);
  const actual = canon(got);
  if (!ordered) {
    expected.sort(byCodeUnits);
    actual.sort(byCodeUnits);
  }
  if (expected.length === actual.length && expected.every((row, i) => row === actual[i])) return null;
  const show = (grid) => `[${grid.map((row) => row.slice(0, -1).split(CELL_SEPARATOR).join(" | ")).join(", ")}]`;
  return `rows differ: expected ${show(expected)} got ${show(actual)}`;
}

function canon(grid) {
  return grid.map((row) => row.map((cell) => norm(cell) + CELL_SEPARATOR).join(""));
}

function byCodeUnits(a, b) {
  return a < b ? -1 : a > b ? 1 : 0;
}

// ── running the corpus ─────────────────────────────────────────────────────

function skipHit(test) {
  const backends = test.skip?.backends;
  if (!Array.isArray(backends)) return null;
  for (const name of backends) {
    if (SKIP_NAMES.has(String(name).toLowerCase())) return String(name);
  }
  return null;
}

async function runCase(url, test, missing, where) {
  let conn = null;
  let step = "";
  try {
    conn = await connect(url);
    for (const sql of RESET) {
      const outcome = await run(conn, sql);
      if (outcome.error !== null) {
        return { status: "ERROR", step, detail: `resetContext failed on '${sql}': ${outcome.error}` };
      }
    }
    const steps = Array.isArray(test.steps) ? test.steps : [];
    for (let i = 0; i < steps.length; i++) {
      step = String(i + 1);
      const sql = String(steps[i].sql);
      const verdict = check(steps[i].expect, await run(conn, sql));
      for (const note of verdict.missing) missing.push({ note, where: `${where} step ${step}` });
      if (!verdict.passed) return { status: "FAIL", step, detail: `${verdict.detail} | sql: ${sql}` };
    }
    return { status: "PASS", step: "", detail: "" };
  } catch (e) {
    const kind = e instanceof TransportError || e instanceof FrostlakeError ? "transport" : e.name;
    return { status: "ERROR", step, detail: `${kind}: ${e.message}` };
  } finally {
    await conn?.close();
  }
}

/**
 * The suite files of the testkit directory FL_CORPUS names, in name order: every
 * one, or with a filter only those whose name holds one of its comma-separated
 * words. A directory holding none throws: the corpus was asked for, and it is not
 * there.
 */
export function suiteFiles(corpus, filter = "") {
  const directory = resolve(corpus, "suites");
  let names = [];
  try {
    names = readdirSync(directory).filter((name) => name.endsWith(".json"));
  } catch { /* no directory: reported below */ }
  if (names.length === 0) throw new Error(`FL_CORPUS=${corpus}: no suite files (*.json) in ${directory}`);
  const words = filter.split(",").map((w) => w.trim().toLowerCase()).filter((w) => w !== "");
  return names
    .filter((name) => words.length === 0 || words.some((w) => name.slice(0, -5).toLowerCase().includes(w)))
    .sort(byCodeUnits)
    .map((name) => join(directory, name));
}

function tsvField(value) {
  const text = String(value).replace(/[\t\r\n]+/g, " ");
  return text.length > MAX_DETAIL ? `${text.slice(0, MAX_DETAIL)}…` : text;
}

function freePort() {
  return new Promise((resolvePort, reject) => {
    const probe = net.createServer();
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address();
      probe.close(() => resolvePort(port));
    });
    probe.on("error", reject);
  });
}

/**
 * The engine to replay against: FROSTLAKE_URL, or a private one booted from
 * FROSTLAKE_CLASSPATH in a user.home of its own (an engine keeps stage files
 * there, which a concurrent run must not share). Null when neither is set.
 */
async function engine() {
  if (process.env.FROSTLAKE_URL) return { url: process.env.FROSTLAKE_URL, stop: () => {} };
  const classpath = process.env.FROSTLAKE_CLASSPATH;
  if (!classpath) return null;
  const java = process.env.JAVA_HOME ? join(process.env.JAVA_HOME, "bin", "java") : "java";
  const home = mkdtempSync(join(tmpdir(), "frostlake-testkit-"));
  const port = await freePort();
  const server = spawn(java, [`-Duser.home=${home}`, "-cp", classpath,
    "dev.frostlake.http.DatabaseHttpServer", String(port)], { cwd: home, stdio: "ignore" });
  let exited = null;
  server.on("exit", (code) => { exited = code ?? "signal"; });
  const stop = () => {
    server.kill();
    rmSync(home, { recursive: true, force: true });
  };
  for (let i = 0; i < 300 && exited === null; i++) {
    try {
      const resp = await fetch(`http://127.0.0.1:${port}/api/health`);
      if (resp.ok) return { url: `frostlake://127.0.0.1:${port}`, stop };
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  stop();
  throw new Error(exited === null ? `the engine never became healthy on port ${port}`
    : `the engine exited during startup (${exited})`);
}

export async function main() {
  const corpus = process.env.FL_CORPUS;
  if (!corpus) {
    console.log(`testkit [${BACKEND}]: ${NO_CORPUS} - skipping`);
    return 0;
  }
  let files;
  try {
    files = suiteFiles(corpus, process.env.FROSTLAKE_TESTKIT_FILTER ?? "");
  } catch (e) {
    console.error(`testkit [${BACKEND}]: ${e.message}`);
    return 2;
  }
  const target = await engine();
  if (target === null) {
    console.log(`testkit [${BACKEND}]: no engine (set FROSTLAKE_URL, or FROSTLAKE_CLASSPATH to boot one) - skipping`);
    return 0;
  }
  const report = resolve(process.env.FROSTLAKE_TESTKIT_REPORT || join(CHECKOUT, "results", `testkit-${BACKEND}.tsv`));
  console.log(`testkit [${BACKEND}]: ${files.length} suite file(s) from ${resolve(corpus, "suites")} against ${target.url}`);

  const counts = { PASS: 0, FAIL: 0, ERROR: 0, SKIP: 0 };
  const lines = ["suite\ttest\tstatus\tfailedStep\tdetail\tms"];
  const missing = [];
  const notable = [];
  const started = performance.now();
  try {
    for (let f = 0; f < files.length; f++) {
      const document = JSON.parse(readFileSync(files[f], "utf8"));
      const suite = String(document.suite ?? basename(files[f], ".json"));
      for (const test of Array.isArray(document.tests) ? document.tests : []) {
        const name = String(test.name ?? "?");
        const hit = skipHit(test);
        let verdict;
        let ms = 0;
        if (hit !== null) {
          verdict = { status: "SKIP", step: "", detail: `skip[${hit}]: ${test.skip.reason ?? ""}` };
        } else {
          const begin = performance.now();
          verdict = await runCase(target.url, test, missing, `${suite}/${name}`);
          ms = Math.round(performance.now() - begin);
        }
        counts[verdict.status]++;
        lines.push([suite, name, verdict.status, verdict.step, verdict.detail, ms].map(tsvField).join("\t"));
        if ((verdict.status === "FAIL" || verdict.status === "ERROR") && notable.length < MAX_REPORTED) {
          notable.push(`  ${suite} / ${name}: ${verdict.status} step ${verdict.step || "-"}: `
            + tsvField(verdict.detail).slice(0, 300));
        }
      }
      if ((f + 1) % 100 === 0) {
        console.log(`  ... ${f + 1}/${files.length} files: ${counts.PASS} pass, ${counts.FAIL} fail, `
          + `${counts.ERROR} error, ${counts.SKIP} skip`);
      }
    }
  } finally {
    target.stop();
  }

  mkdirSync(dirname(report), { recursive: true });
  writeFileSync(report, `${lines.join("\n")}\n`);
  const notes = join(dirname(report), `missing-apis-${BACKEND}.md`);
  rmSync(notes, { force: true });
  if (missing.length > 0) {
    const tally = new Map();
    for (const { note, where } of missing) {
      if (!tally.has(note)) tally.set(note, { count: 0, example: where });
      tally.get(note).count++;
    }
    let text = `# Missing APIs for backend \`${BACKEND}\`\n\n`
      + "Checks the suites ask for that this transport cannot express. They are not failures:\n"
      + "the day the API exists, they light up.\n\n";
    for (const [note, { count, example }] of tally) {
      text += `- ${note}: ${count} check(s), e.g. ${example}\n`;
    }
    writeFileSync(notes, text);
  }

  const seconds = ((performance.now() - started) / 1000).toFixed(1);
  if (notable.length > 0) {
    console.log("first failures:");
    for (const line of notable) console.log(line);
  }
  console.log(`report: ${report}${missing.length > 0 ? `, and ${missing.length} missing-API check(s) in ${notes}` : ""}`);
  console.log(`${counts.FAIL} failed and ${counts.ERROR} errored in ${seconds}s`);
  console.log(`testkit [${BACKEND}]: ${counts.PASS} passed, ${counts.FAIL + counts.ERROR} failed, ${counts.SKIP} skipped`);
  return counts.FAIL + counts.ERROR > 0 ? 1 : 0;
}

// Run as a program, not when test/testkit.test.js imports main() to run it itself.
if (process.argv[1] !== undefined && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await main();
}
