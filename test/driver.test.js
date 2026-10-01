import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import net from "node:net";
import http from "node:http";
import { connect, substitute, FrostlakeError } from "../index.js";
import snowflake from "../compat.js";

// Boots a real DatabaseHttpServer from FROSTLAKE_CLASSPATH; skips when unset.
const cp = process.env.FROSTLAKE_CLASSPATH;
let server = null;
let dsn = null;
let httpBase = null;

before(async () => {
  if (!cp) return;
  const java = process.env.JAVA_HOME ? `${process.env.JAVA_HOME}/bin/java` : "java";
  const port = await freePort();
  server = spawn(java, ["-cp", cp, "dev.frostlake.http.DatabaseHttpServer", String(port)], { stdio: "ignore" });
  const base = `http://127.0.0.1:${port}`;
  for (let i = 0; i < 100; i++) {
    try {
      const resp = await fetch(`${base}/api/health`);
      if (resp.ok) break;
    } catch { /* not up yet */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  dsn = `frostlake://127.0.0.1:${port}`;
  httpBase = base;
});

after(() => server?.kill());

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

const integration = { skip: cp ? false : "FROSTLAKE_CLASSPATH not set" };

test("substitution skips literals, identifiers and comments", () => {
  const out = substitute("SELECT 'a?b', \"c?d\", ? -- e?f\n, ? /* g?h */", ["x", 2]);
  assert.equal(out, "SELECT 'a?b', \"c?d\", 'x' -- e?f\n, 2 /* g?h */");
});

test("string encoding doubles backslashes then quotes", () => {
  assert.equal(substitute("SELECT ?", ["Ada O'Hara \\ Byron"]), "SELECT 'Ada O''Hara \\\\ Byron'");
});

test("ddl, dml and typed query", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_test_db");
  await conn.execute("USE DATABASE js_test_db");
  await conn.execute("CREATE TABLE people (id INTEGER, name VARCHAR, score FLOAT, ok BOOLEAN)");
  const ins = await conn.execute("INSERT INTO people VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
    [1, "Ada O'Hara \\ Byron", 9.5, true, 2, "Grace", 8.25, false]);
  assert.equal(ins.rowCount, 2);
  const { rows } = await conn.execute("SELECT id, name, score, ok FROM people WHERE id = ?", [1]);
  assert.deepEqual(rows, [{ ID: 1, NAME: "Ada O'Hara \\ Byron", SCORE: 9.5, OK: true }]);
  await conn.close();
});

test("session state persists across statements", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_sess_db");
  await conn.execute("USE DATABASE js_sess_db");
  await conn.execute("CREATE TABLE t1 (a INTEGER)");
  await conn.execute("INSERT INTO t1 VALUES (7)");
  const { rows } = await conn.execute("SELECT a FROM t1");
  assert.deepEqual(rows, [{ A: 7 }]);
  await conn.close();
});

test("transaction rollback", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_tx_db");
  await conn.execute("USE DATABASE js_tx_db");
  await conn.execute("CREATE TABLE acc (n INTEGER)");
  await conn.execute("INSERT INTO acc VALUES (1)");
  await conn.begin();
  await conn.execute("INSERT INTO acc VALUES (2)");
  await conn.rollback();
  const { rows } = await conn.execute("SELECT COUNT(*) AS n FROM acc");
  assert.equal(rows[0].N, 1);
  await conn.close();
});

test("error surface", integration, async () => {
  const conn = await connect(dsn);
  await assert.rejects(conn.execute("SELECT FROM nowhere"), /SQL compilation error/);
  await conn.close();
});

test("dollar-quoted strings survive binding end-to-end", integration, async () => {
  const conn = await connect(dsn);
  const { rows } = await conn.execute("SELECT $$a?b$$ AS S, ? AS N", [7]);
  assert.deepEqual(rows, [{ S: "a?b", N: 7 }]);
  await conn.close();
});

test("a failed USE from the DSN keeps failing instead of falling through", integration, async () => {
  const conn = await connect(`${dsn}/NO_SUCH_DB`);
  await assert.rejects(conn.execute("SELECT 1 AS X"), /does not exist/);
  await assert.rejects(conn.execute("SELECT CURRENT_DATABASE() AS D"), /does not exist/);
  await conn.close();
});

test("concurrent executes share one server session", integration, async () => {
  const before = (await (await fetch(`${httpBase}/api/sessions`)).json()).activeSessions;
  const conn = await connect(dsn);
  const results = await Promise.all([
    conn.execute("SELECT 1 AS A"),
    conn.execute("SELECT 2 AS A"),
    conn.execute("SELECT 3 AS A"),
  ]);
  const after = (await (await fetch(`${httpBase}/api/sessions`)).json()).activeSessions;
  assert.deepEqual(results.map((r) => r.rows[0].A), [1, 2, 3]);
  assert.equal(after - before, 1);
  await conn.close();
});

test("merge, update and delete report affected-row counts", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_dml_db");
  await conn.execute("USE DATABASE js_dml_db");
  await conn.execute("CREATE TABLE tgt (id INTEGER, v VARCHAR)");
  await conn.execute("CREATE TABLE src (id INTEGER, v VARCHAR)");
  await conn.execute("INSERT INTO tgt VALUES (1, 'old')");
  await conn.execute("INSERT INTO src VALUES (1, 'new'), (2, 'ins')");
  const merge = await conn.execute(
    "MERGE INTO tgt USING src ON tgt.id = src.id WHEN MATCHED THEN UPDATE SET v = src.v WHEN NOT MATCHED THEN INSERT VALUES (src.id, src.v)");
  assert.equal(merge.rowCount, 2);
  assert.deepEqual(merge.rows, []);
  assert.deepEqual(merge.columns, []);
  const upd = await conn.execute("UPDATE tgt SET v = 'x' WHERE id IN (1, 2)");
  assert.equal(upd.rowCount, 2);
  const del = await conn.execute("DELETE FROM tgt WHERE id IN (1, 2)");
  assert.equal(del.rowCount, 2);
  await conn.close();
});

test("transaction commit persists", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_commit_db");
  await conn.execute("USE DATABASE js_commit_db");
  await conn.execute("CREATE TABLE acc (n INTEGER)");
  await conn.begin();
  await conn.execute("INSERT INTO acc VALUES (1), (2)");
  await conn.commit();
  const { rows } = await conn.execute("SELECT COUNT(*) AS n FROM acc");
  assert.equal(rows[0].N, 2);
  await conn.close();
});

test("empty results, NULLs and temporals keep their shape", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_shape_db");
  await conn.execute("USE DATABASE js_shape_db");
  await conn.execute("CREATE TABLE t (a INTEGER)");
  const empty = await conn.execute("SELECT a FROM t");
  assert.equal(empty.columns.length, 1);
  assert.deepEqual(empty.rows, []);
  assert.equal(empty.rowCount, 0);
  const nulls = await conn.execute("SELECT ? AS N, NULL AS M", [null]);
  assert.deepEqual(nulls.rows, [{ N: null, M: null }]);
  await conn.execute("CREATE TABLE ts (d DATE, t TIMESTAMP_NTZ)");
  await conn.execute("INSERT INTO ts VALUES ('2026-08-19', '2026-08-19 12:30:45')");
  const ts = await conn.execute("SELECT d, t FROM ts");
  assert.deepEqual(ts.rows, [{ D: "2026-08-19", T: "2026-08-19 12:30:45.000" }]);
  await conn.close();
});

test("execute after close throws", integration, async () => {
  const conn = await connect(dsn);
  await conn.close();
  await assert.rejects(conn.execute("SELECT 1"), /connection is closed/);
});

test("dsn database and schema preselect, case-insensitively", integration, async () => {
  const admin = await connect(dsn);
  await admin.execute("CREATE OR REPLACE DATABASE js_case_db");
  await admin.execute("CREATE SCHEMA js_case_db.sc1");
  await admin.close();
  const conn = await connect(`${dsn}/js_case_db?schema=sc1`);
  const { rows } = await conn.execute("SELECT CURRENT_DATABASE() AS D, CURRENT_SCHEMA() AS S");
  assert.deepEqual(rows, [{ D: "JS_CASE_DB", S: "SC1" }]);
  await conn.close();
});

test("a look-alike count alias stays a query result", integration, async () => {
  const conn = await connect(dsn);
  const r = await conn.execute(`SELECT 42 AS "number of rows once"`);
  assert.deepEqual(r.rows, [{ "number of rows once": 42 }]);
  assert.equal(r.rowCount, 1);
  await conn.close();
});

test("multi-statement executes expose every result set", integration, async () => {
  const conn = await connect(dsn);
  // A pack has to be asked for: a session takes one statement per request until it says otherwise.
  await conn.execute("ALTER SESSION SET MULTI_STATEMENT_COUNT = 0");
  const r = await conn.execute("SELECT 1 AS A; SELECT 2 AS B");
  assert.deepEqual(r.rows, [{ A: 1 }]);
  assert.equal(r.resultSets.length, 2);
  assert.deepEqual(r.resultSets[1].rows, [{ B: 2 }]);
  await conn.close();
});

test("semi-structured cells read back as Snowflake text", integration, async () => {
  const conn = await connect(dsn);
  const { rows, columns } = await conn.execute(
    `SELECT [1, 'x'] AS A, OBJECT_CONSTRUCT('k', 1) AS O, PARSE_JSON('{"a": [1, 2]}') AS V`);
  assert.deepEqual(columns.map((c) => c.dataType), ["ARRAY", "OBJECT", "VARIANT"]);
  assert.deepEqual(rows, [{ A: '[1,"x"]', O: '{"k":1}', V: '{"a":[1,2]}' }]);
  const holes = await conn.execute("SELECT [1, NULL] AS H");
  assert.deepEqual(holes.rows, [{ H: "[1,undefined]" }]);
  await conn.close();
});

test("binary cells read back as Buffers", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_bin_db");
  await conn.execute("USE DATABASE js_bin_db");
  await conn.execute("CREATE TABLE bin (b BINARY)");
  const payload = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
  await conn.execute("INSERT INTO bin VALUES (?)", [payload]);
  const { rows, columns } = await conn.execute("SELECT b FROM bin");
  assert.equal(columns[0].dataType, "BINARY");
  assert.ok(Buffer.isBuffer(rows[0].B));
  assert.deepEqual(rows[0].B, payload);
  await conn.close();
});

test("text and binary columns report their declared width", integration, async (t) => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_width_db");
  await conn.execute("USE DATABASE js_width_db");
  await conn.execute("CREATE TABLE widths (s VARCHAR(9), b BINARY(5), n NUMBER(10,2), u VARCHAR)");
  const { columns } = await conn.execute("SELECT s, b, n, u FROM widths");
  // An engine that predates the field sends no width at all, and this driver
  // supports those: with nothing to report the checks below are skipped rather
  // than passed, so a green tick never claims a width the wire never carried.
  if (columns[0].length === null) {
    await conn.close();
    t.skip("this engine sends no column length");
    return;
  }
  // Characters for the text column, bytes for the binary one.
  assert.equal(columns[0].length, 9);
  assert.equal(columns[1].length, 5);
  // Every other type carries no width at all: null, never 0.
  assert.equal(columns[2].length, null);
  // Declared without a width, a text column still reports the maximum.
  assert.equal(columns[3].length, 16777216);
  await conn.close();
});

test("compat connect surfaces unreachable-server errors", async () => {
  const connection = snowflake.createConnection({ host: "127.0.0.1", port: 1 });
  await assert.rejects(connection.connectAsync(), FrostlakeError);
  await new Promise((resolve, reject) => {
    connection.connect((err) => {
      try {
        assert.ok(err instanceof FrostlakeError);
        resolve();
      } catch (e) {
        reject(e);
      }
    });
  });
});

test("compat promise-style execute, callback connect and destroy", integration, async () => {
  const port = Number(new URL(dsn.replace("frostlake:", "http:")).port);
  const connection = snowflake.createConnection({ host: "127.0.0.1", port });
  await new Promise((resolve, reject) => {
    connection.connect((err) => (err ? reject(err) : resolve()));
  });
  const { rows, rowCount } = await connection.execute({ sqlText: "SELECT ? AS ONE", binds: [1] });
  assert.deepEqual(rows, [{ ONE: 1 }]);
  assert.equal(rowCount, 1);
  await new Promise((resolve, reject) => {
    connection.destroy((err) => (err ? reject(err) : resolve()));
  });
});

test("snowflake-sdk compat facade", integration, async () => {
  const connection = snowflake.createConnection({
    host: "127.0.0.1",
    port: Number(new URL(dsn.replace("frostlake:", "http:")).port),
  });
  await connection.connectAsync();
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: "SELECT 1 AS one, 'x' AS s",
      complete: (err, stmt, rows) => {
        if (err) return reject(err);
        try {
          assert.deepEqual(rows, [{ ONE: 1, S: "x" }]);
          assert.equal(stmt.getNumRows(), 1);
          resolve();
        } catch (e) {
          reject(e);
        }
      },
    });
  });
});

// A stub engine that keeps every execute body, so what the driver PUTS ON THE WIRE is checked
// rather than only what comes back.
async function recordingEngine(bodies) {
  const srv = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk) => { raw += chunk; });
    req.on("end", () => {
      res.writeHead(200, { "content-type": "application/json" });
      if (req.url === "/api/execute") {
        bodies.push(JSON.parse(raw));
        res.end(JSON.stringify({ success: true, sessionId: "s1", resultSets: [] }));
      } else {
        res.end(JSON.stringify({ status: "healthy" }));
      }
    });
  });
  await new Promise((resolve) => srv.listen(0, "127.0.0.1", resolve));
  return { srv, dsn: `frostlake://127.0.0.1:${srv.address().port}` };
}

test("a request carries no statement count unless the caller asks for one", async () => {
  const bodies = [];
  const { srv, dsn: stub } = await recordingEngine(bodies);
  const conn = await connect(stub);
  await conn.execute("SELECT 1");
  await conn.execute("SELECT 1; SELECT 2", [], { multiStatementCount: 2 });
  await conn.execute("SELECT 1; SELECT 2", [], { multiStatementCount: 0 });
  await conn.close();
  srv.close();
  assert.equal("multiStatementCount" in bodies[0], false, "an unasked request declared a count");
  assert.equal(bodies[1].multiStatementCount, 2);
  // Zero is a real answer - "any number" - not nothing to say.
  assert.equal(bodies[2].multiStatementCount, 0);
});

test("the sdk-shaped facade takes the count as a statement parameter", async () => {
  const bodies = [];
  const { srv, dsn: stub } = await recordingEngine(bodies);
  const url = new URL(stub.replace("frostlake://", "http://"));
  const conn = snowflake.createConnection({ host: url.hostname, port: Number(url.port) });
  await conn.connectAsync();
  await conn.execute({ sqlText: "SELECT 1; SELECT 2", parameters: { MULTI_STATEMENT_COUNT: 2 } });
  await conn.execute({ sqlText: "SELECT 1" });
  srv.close();
  assert.equal(bodies[0].multiStatementCount, 2);
  assert.equal("multiStatementCount" in bodies[1], false);
});

test("a pack runs on the caller's own count, leaving the session alone", integration, async (t) => {
  const conn = await connect(dsn);
  const packed = await conn.execute("SELECT 1 AS a; SELECT 2 AS b", [], { multiStatementCount: 2 });
  assert.ok(packed);
  // Nothing was changed on the session, so a pack nobody asked for is still refused.
  //
  // Only an engine carrying the statement-count gate refuses one at all, and this driver
  // supports older ones. Against one of those the refusal never comes, so the check is
  // skipped rather than passed: a green tick would claim an engine had been checked for
  // a refusal it does not make.
  let gated = true;
  try {
    await conn.execute("SELECT 1; SELECT 2");
    gated = false;
  } catch { /* the gate refused it, as the assertion below spells out */ }
  if (!gated) {
    await conn.close();
    t.skip("this engine accepts a pack nobody asked for, so it has no refusal to assert");
    return;
  }
  await assert.rejects(() => conn.execute("SELECT 1; SELECT 2"), /did not match the desired statement count/);
  await conn.close();
});

// A stub engine that answers every execute with one canned reply, so a row shape the server
// sends can be checked with no engine running.
async function answeringEngine(reply) {
  const srv = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(req.url === "/api/execute" ? reply : { status: "healthy" }));
    });
  });
  await new Promise((resolve) => srv.listen(0, "127.0.0.1", resolve));
  return { srv, dsn: `frostlake://127.0.0.1:${srv.address().port}` };
}

test("array rows keep columns that share a name apart", async () => {
  const { srv, dsn: stub } = await answeringEngine({
    success: true,
    sessionId: "s1",
    resultSets: [{
      columns: [
        { name: "ID", dataType: "NUMBER" },
        { name: "ID", dataType: "NUMBER" },
        { name: "B", dataType: "BINARY" },
      ],
      rows: [[1, 2, "DEAD"], [3, null, null]],
      rowCount: 2,
      updateCount: -1,
    }],
  });
  try {
    const conn = await connect(stub);
    const arrays = await conn.execute("SELECT a.id, b.id, a.b FROM a JOIN b", [], { rowMode: "array" });
    assert.deepEqual(arrays.columns.map((c) => c.name), ["ID", "ID", "B"]);
    assert.deepEqual(arrays.rows, [[1, 2, Buffer.from([0xde, 0xad])], [3, null, null]]);
    assert.equal(arrays.rowCount, 2);
    assert.deepEqual(arrays.resultSets[0].rows, arrays.rows);
    // Keyed by name, the first ID is lost to the second: the default shape is unchanged.
    const objects = await conn.execute("SELECT a.id, b.id, a.b FROM a JOIN b");
    assert.deepEqual(objects.rows, [{ ID: 2, B: Buffer.from([0xde, 0xad]) }, { ID: null, B: null }]);
    await assert.rejects(conn.execute("SELECT 1", [], { rowMode: "tuple" }), /rowMode must be "object" or "array"/);
    await conn.close();
    const url = new URL(stub.replace("frostlake://", "http://"));
    const facade = snowflake.createConnection({ host: url.hostname, port: Number(url.port) });
    await facade.connectAsync();
    const rows = await new Promise((resolve, reject) => {
      facade.execute({
        sqlText: "SELECT a.id, b.id, a.b FROM a JOIN b",
        rowMode: "array",
        complete: (err, stmt, got) => (err ? reject(err) : resolve(got)),
      });
    });
    assert.deepEqual(rows, [[1, 2, Buffer.from([0xde, 0xad])], [3, null, null]]);
  } finally {
    srv.close();
  }
});

test("array rows still fold a DML count grid into rowCount", async () => {
  const { srv, dsn: stub } = await answeringEngine({
    success: true,
    sessionId: "s1",
    resultSets: [{
      columns: [
        { name: "number of rows inserted", dataType: "NUMBER" },
        { name: "number of rows updated", dataType: "NUMBER" },
      ],
      rows: [[2, 1]],
      rowCount: 1,
      updateCount: 3,
    }],
  });
  try {
    const conn = await connect(stub);
    const merge = await conn.execute(
      "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)",
      [], { rowMode: "array" });
    assert.deepEqual(merge.columns, []);
    assert.deepEqual(merge.rows, []);
    assert.equal(merge.rowCount, 3);
    await conn.close();
  } finally {
    srv.close();
  }
});

test("a self-join's same-named columns both read back as arrays", integration, async () => {
  const conn = await connect(dsn);
  await conn.execute("CREATE OR REPLACE DATABASE js_rowmode_db");
  await conn.execute("USE DATABASE js_rowmode_db");
  await conn.execute("CREATE TABLE emp (id INTEGER, name VARCHAR, mgr_id INTEGER)");
  await conn.execute("INSERT INTO emp VALUES (1, 'CEO', NULL), (2, 'Alice', 1), (3, 'Bob', 1)");
  const { columns, rows } = await conn.execute(
    "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr_id = m.id ORDER BY e.id", [], { rowMode: "array" });
  assert.deepEqual(columns.map((c) => c.name), ["NAME", "NAME"]);
  assert.deepEqual(rows, [["Alice", "CEO"], ["Bob", "CEO"]]);
  await conn.close();
});
