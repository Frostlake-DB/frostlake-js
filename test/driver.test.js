import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import net from "node:net";
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
