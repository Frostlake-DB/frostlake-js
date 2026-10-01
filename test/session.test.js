import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import net from "node:net";
import http from "node:http";
import { connect, Connection, FrostlakeError, SessionLostError } from "../index.js";
import snowflake from "../compat.js";

// How a connection keeps its idea of the engine session in step with the engine's: first
// over a scripted stand-in engine, then against a real one booted from FROSTLAKE_CLASSPATH.

// A stand-in engine that answers each request from a script and records what it was sent,
// so a scenario sees every round trip it makes. The health check is answered on its own;
// anything else the script did not expect is recorded and answered with a body that is not
// an engine's.
async function scriptedEngine() {
  const requests = [];
  const unscripted = [];
  const script = [];
  const held = [];
  const srv = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk) => { raw += chunk; });
    req.on("end", () => {
      if (req.url === "/api/health") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "healthy", activeSessions: 0 }));
        return;
      }
      const body = raw === "" ? null : JSON.parse(raw);
      requests.push({ method: req.method, url: req.url, body });
      const next = script.shift();
      if (next === undefined) {
        unscripted.push({ method: req.method, url: req.url, body });
        res.writeHead(500, { "content-type": "text/html" });
        res.end("<html>unscripted</html>");
      } else if (next.kind === "drop") {
        req.socket.destroy();
      } else if (next.kind === "hang") {
        held.push(res);
      } else if (next.kind === "text") {
        res.writeHead(next.status, { "content-type": "text/html" });
        res.end(next.text);
      } else {
        res.writeHead(next.status, { "content-type": "application/json" });
        res.end(JSON.stringify(next.body));
      }
    });
  });
  await new Promise((resolve) => srv.listen(0, "127.0.0.1", resolve));
  const port = srv.address().port;
  return {
    port,
    dsn: `frostlake://127.0.0.1:${port}`,
    requests,
    unscripted,
    reply(body, status = 200) { script.push({ kind: "reply", status, body }); },
    // Answer with a body that is not JSON, the way a proxy in the way does.
    replyText(text, status) { script.push({ kind: "text", status, text }); },
    // Close the socket without answering.
    drop() { script.push({ kind: "drop" }); },
    // Hold the request unanswered until the engine stops.
    hang() { script.push({ kind: "hang" }); },
    pending() { return script.length; },
    executes() { return requests.filter((r) => r.url === "/api/execute").map((r) => r.body); },
    statements() { return this.executes().map((b) => b.sql); },
    async stop() {
      for (const res of held) res.socket?.destroy();
      srv.closeAllConnections?.();
      await new Promise((resolve) => srv.close(resolve));
    },
  };
}

const statusSet = () => ({
  columns: [{ name: "status", dataType: "VARCHAR" }],
  rows: [["Statement executed successfully."]], rowCount: 1, updateCount: -1,
});
const numberSet = (name, value) => ({
  columns: [{ name, dataType: "NUMBER", precision: 38, scale: 0 }],
  rows: [[value]], rowCount: 1, updateCount: -1,
});
// An answer from an engine that reports newSession (0.1.0 and later).
const answer = (sessionId, started, ...sets) =>
  ({ success: true, sessionId, newSession: started, errorMessage: null, resultSets: sets });
// An answer from an engine that predates newSession (0.0.7).
const legacyAnswer = (sessionId, ...sets) =>
  ({ success: true, sessionId, errorMessage: null, resultSets: sets });
// The 404 a requireSession request gets when its session is gone: nothing ran.
const gone = (sessionId) => ({
  success: false, sessionId: null, newSession: false, resultSets: [],
  errorMessage: `Session '${sessionId}' does not exist or has expired.`,
});
const released = { success: true, sessionId: null, newSession: false, resultSets: [] };

const SCOPE = ["USE DATABASE APP", "USE SCHEMA PUBLIC"];

// A connection on the DSN's scope that has run one statement on session s1.
async function opened(engine, { legacy = false } = {}) {
  if (legacy) {
    for (const set of [statusSet(), statusSet(), numberSet("N", 1)]) engine.reply(legacyAnswer("s1", set));
  } else {
    engine.reply(answer("s1", true, statusSet()));
    engine.reply(answer("s1", false, statusSet()));
    engine.reply(answer("s1", false, numberSet("N", 1)));
  }
  const conn = await connect(`${engine.dsn}/APP?schema=PUBLIC`);
  await conn.execute("SELECT 1 AS N");
  return conn;
}

function allAnswered(engine) {
  assert.deepEqual(engine.unscripted, []);
  assert.equal(engine.pending(), 0);
}

// Rejects with SessionLostError, which is still a FrostlakeError.
async function sessionLost(promise, pattern) {
  let caught = null;
  try {
    await promise;
  } catch (e) {
    caught = e;
  }
  assert.ok(caught instanceof SessionLostError, `expected SessionLostError, got ${caught?.constructor?.name}: ${caught?.message}`);
  assert.ok(caught instanceof FrostlakeError);
  if (pattern) assert.match(caught.message, pattern);
  return caught;
}

// Runs a scenario against a fresh scripted engine and stops it afterwards.
function scripted(name, body) {
  test(name, async () => {
    const engine = await scriptedEngine();
    try {
      await body(engine);
    } finally {
      await engine.stop();
    }
  });
}

scripted("the session is required only once the engine has said it can", async (engine) => {
  const conn = await opened(engine);
  const [first, second, third] = engine.executes();
  // The first request names no session, so it has nothing to require.
  assert.equal("sessionId" in first, false);
  assert.equal("requireSession" in first, false);
  // Its answer carried newSession, so from then on the session is required.
  assert.equal(second.sessionId, "s1");
  assert.equal(second.requireSession, true);
  assert.equal(third.requireSession, true);
  assert.deepEqual(engine.statements(), [...SCOPE, "SELECT 1 AS N"]);
  assert.equal(conn.sessionId, "s1");
  allAnswered(engine);
});

scripted("an older engine is never sent requireSession, and closing sends it nothing", async (engine) => {
  const conn = await opened(engine, { legacy: true });
  engine.reply(legacyAnswer("s1", numberSet("N", 2)));
  await conn.execute("SELECT 2 AS N");
  for (const body of engine.executes().slice(1)) {
    assert.equal(body.sessionId, "s1");
    assert.equal("requireSession" in body, false);
  }
  await conn.close();
  // No DELETE either: an engine that predates newSession has no such endpoint.
  assert.deepEqual(engine.requests.map((r) => r.method), ["POST", "POST", "POST", "POST"]);
  allAnswered(engine);
});

scripted("an answer naming no session settles nothing", async (engine) => {
  // A refusal of the request itself carries no session and says nothing about the engine.
  engine.reply({ success: false, sessionId: null, errorMessage: "SQL is required" }, 400);
  engine.reply(answer("n1", true, numberSet("N", 1)));
  engine.reply(answer("n1", false, numberSet("N", 2)));
  const conn = await connect(engine.dsn);
  await assert.rejects(conn.execute(" "), /SQL is required/);
  await conn.execute("SELECT 1 AS N");
  await conn.execute("SELECT 2 AS N");
  const bodies = engine.executes();
  assert.equal("requireSession" in bodies[1], false);
  assert.equal(bodies[2].requireSession, true);
  allAnswered(engine);
});

scripted("a lost session is replaced on the DSN's scope and the statement sent once more", async (engine) => {
  const conn = await opened(engine);
  engine.reply(gone("s1"), 404);
  engine.reply(answer("s2", true, statusSet()));
  engine.reply(answer("s2", false, statusSet()));
  engine.reply(answer("s2", false, numberSet("N", 2)));
  const { rows } = await conn.execute("SELECT 2 AS N");
  assert.deepEqual(rows, [{ N: 2 }]);
  assert.deepEqual(engine.statements(), [...SCOPE, "SELECT 1 AS N", "SELECT 2 AS N", ...SCOPE, "SELECT 2 AS N"]);
  const bodies = engine.executes();
  // The replacement starts without an id, and everything after it names the new one.
  assert.equal("sessionId" in bodies[4], false);
  assert.deepEqual([bodies[5].sessionId, bodies[6].sessionId], ["s2", "s2"]);
  assert.equal(conn.sessionId, "s2");
  allAnswered(engine);
});

scripted("a second 404 is reported rather than retried", async (engine) => {
  const conn = await opened(engine);
  engine.reply(gone("s1"), 404);
  engine.reply(answer("s2", true, statusSet()));
  engine.reply(answer("s2", false, statusSet()));
  engine.reply(gone("s2"), 404);
  await sessionLost(conn.execute("SELECT 2 AS N"), /just started/);
  assert.equal(engine.statements().filter((s) => s === "SELECT 2 AS N").length, 2);
  allAnswered(engine);
});

scripted("a lost session with an open transaction is reported, not replaced", async (engine) => {
  const conn = await opened(engine);
  engine.reply(answer("s1", false, statusSet()));
  await conn.begin();
  engine.reply(gone("s1"), 404);
  await sessionLost(conn.execute("INSERT INTO T VALUES (1)"), /transaction/);
  assert.deepEqual(engine.statements(), [...SCOPE, "SELECT 1 AS N", "BEGIN", "INSERT INTO T VALUES (1)"]);
  // The transaction went with its session, as a rollback would have ended it.
  assert.equal(conn.autoCommit, true);
  // The connection stays usable: the next statement starts a fresh session on the scope.
  engine.reply(answer("s2", true, statusSet()));
  engine.reply(answer("s2", false, statusSet()));
  engine.reply(answer("s2", false, numberSet("N", 3)));
  await conn.execute("SELECT 3 AS N");
  assert.deepEqual(engine.statements().slice(5), [...SCOPE, "SELECT 3 AS N"]);
  assert.equal("sessionId" in engine.executes()[5], false);
  assert.equal(engine.executes()[7].autoCommit, true);
  allAnswered(engine);
});

scripted("a commit that finds the session gone says the transaction went with it", async (engine) => {
  const conn = await opened(engine);
  engine.reply(answer("s1", false, statusSet()));
  engine.reply(answer("s1", false, numberSet("number of rows inserted", 1)));
  await conn.begin();
  await conn.execute("INSERT INTO T VALUES (1)");
  engine.reply(gone("s1"), 404);
  await sessionLost(conn.commit(), /transaction/);
  // Not sent again on a fresh session, where it would have "succeeded" with nothing to commit.
  assert.equal(engine.statements().filter((s) => s === "COMMIT").length, 1);
  allAnswered(engine);
});

scripted("a lost session whose context moved is reported, not replaced", async (engine) => {
  const conn = await opened(engine);
  engine.reply(answer("s1", false, statusSet()));
  await conn.execute("USE SCHEMA OTHER");
  engine.reply(gone("s1"), 404);
  await sessionLost(conn.execute("SELECT * FROM T"), /context/);
  assert.deepEqual(engine.statements(), [...SCOPE, "SELECT 1 AS N", "USE SCHEMA OTHER", "SELECT * FROM T"]);
  // Starting over on the scope puts the connection back where it began.
  engine.reply(answer("s2", true, statusSet()));
  engine.reply(answer("s2", false, statusSet()));
  engine.reply(answer("s2", false, numberSet("N", 4)));
  await conn.execute("SELECT 4 AS N");
  assert.deepEqual(engine.statements().slice(5), [...SCOPE, "SELECT 4 AS N"]);
  allAnswered(engine);
});

// What each statement leaves behind decides whether a lost session is replaced after it.
const MOVING = [
  "USE SCHEMA s", "use database d", "SET x = 1", "UNSET x", "ALTER SESSION SET TIMEZONE = 'UTC'",
  "CREATE OR REPLACE DATABASE d", "CREATE SCHEMA IF NOT EXISTS s", "DROP DATABASE d",
  "CREATE TEMPORARY TABLE t (a INT)", "CREATE OR REPLACE TEMP TABLE t (a INT)",
  "CREATE LOCAL TEMPORARY TABLE t (a INT)", "/* c */ -- x\n use schema s",
  "SELECT 1; USE SCHEMA s", "BEGIN", "begin transaction", "BEGIN WORK", "BEGIN NAME t1",
  "START TRANSACTION",
];
const STAYING = [
  "CREATE TABLE t (a INT)", "CREATE OR REPLACE TRANSIENT TABLE t (a INT)", "ALTER TABLE t ADD COLUMN b INT",
  "SELECT 1", "INSERT INTO t VALUES (1)", "SELECT 'USE SCHEMA s'", "SELECT 1 -- ; USE SCHEMA s",
  "SELECT $$USE SCHEMA s; SET x = 1$$", "BEGIN LET x := 1; RETURN x; END", "BEGIN TRANSACTION; COMMIT",
  "BEGIN; ROLLBACK WORK",
];

scripted("which statements leave the session holding something a fresh one would lack", async (engine) => {
  for (const sql of [...MOVING, ...STAYING]) {
    const moving = MOVING.includes(sql);
    const conn = await opened(engine);
    engine.reply(answer("s1", false, statusSet()));
    await conn.execute(sql, [], { multiStatementCount: 0 });
    engine.reply(gone("s1"), 404);
    if (moving) {
      await sessionLost(conn.execute("SELECT 9 AS N"));
    } else {
      engine.reply(answer("s2", true, statusSet()));
      engine.reply(answer("s2", false, statusSet()));
      engine.reply(answer("s2", false, numberSet("N", 9)));
      const { rows } = await conn.execute("SELECT 9 AS N");
      assert.deepEqual(rows, [{ N: 9 }], sql);
    }
    allAnswered(engine);
    engine.requests.length = 0;
  }
});

scripted("a server that replaced the session gets the scope back first", async (engine) => {
  // An engine that ran a statement in a fresh session in place of ours says so; the scope
  // goes back on before the next statement.
  const conn = await opened(engine);
  engine.reply(answer("s1", true, numberSet("N", 2)));
  await conn.execute("SELECT 2 AS N");
  engine.reply(answer("s1", false, statusSet()));
  engine.reply(answer("s1", false, statusSet()));
  engine.reply(answer("s1", false, numberSet("N", 3)));
  await conn.execute("SELECT 3 AS N");
  assert.deepEqual(engine.statements().slice(4), [...SCOPE, "SELECT 3 AS N"]);
  allAnswered(engine);
});

scripted("a session replaced while the scope goes on gets all of it", async (engine) => {
  // An older engine's answer, then one saying the engine ran USE SCHEMA in a fresh session
  // in place of ours: the fresh one gets the whole scope, database first.
  engine.reply(legacyAnswer("s1", statusSet()));
  engine.reply(answer("s1", true, statusSet()));
  engine.reply(answer("s1", false, statusSet()));
  engine.reply(answer("s1", false, statusSet()));
  engine.reply(answer("s1", false, numberSet("N", 1)));
  const conn = await connect(`${engine.dsn}/APP?schema=PUBLIC`);
  await conn.execute("SELECT 1 AS N");
  assert.deepEqual(engine.statements(), [...SCOPE, ...SCOPE, "SELECT 1 AS N"]);
  allAnswered(engine);
});

scripted("closing releases the session once", async (engine) => {
  const conn = await opened(engine);
  engine.reply(released);
  await conn.close();
  const last = engine.requests.at(-1);
  assert.deepEqual([last.method, last.url, last.body], ["DELETE", "/api/sessions/s1", null]);
  const count = engine.requests.length;
  await conn.close();
  assert.equal(engine.requests.length, count);
  assert.equal(conn.sessionId, null);
  await assert.rejects(conn.execute("SELECT 1"), /connection is closed/);
  allAnswered(engine);
});

scripted("closing waits for the statements already queued", async (engine) => {
  const conn = await opened(engine);
  engine.reply(answer("s1", false, numberSet("N", 2)));
  engine.reply(released);
  const pending = conn.execute("SELECT 2 AS N");
  await conn.close();
  assert.deepEqual((await pending).rows, [{ N: 2 }]);
  assert.deepEqual(engine.requests.slice(-2).map((r) => r.method), ["POST", "DELETE"]);
  allAnswered(engine);
});

scripted("a connection that never ran anything closes without a request", async (engine) => {
  const conn = await connect(`${engine.dsn}/APP`);
  await conn.close();
  assert.deepEqual(engine.requests, []);
});

scripted("closing with a transaction open leaves the rollback to the release", async (engine) => {
  const conn = await opened(engine);
  engine.reply(answer("s1", false, statusSet()));
  await conn.begin();
  engine.reply(released);
  await conn.close();
  assert.equal(engine.requests.at(-1).method, "DELETE");
  assert.equal(engine.statements().includes("ROLLBACK"), false);
  allAnswered(engine);
});

scripted("closing on an older engine rolls back an open transaction instead", async (engine) => {
  const conn = await opened(engine, { legacy: true });
  engine.reply(legacyAnswer("s1", statusSet()));
  await conn.begin();
  engine.reply(legacyAnswer("s1", statusSet()));
  await conn.close();
  assert.equal(engine.statements().at(-1), "ROLLBACK");
  assert.deepEqual(engine.requests.map((r) => r.method), ["POST", "POST", "POST", "POST", "POST"]);
  allAnswered(engine);
});

scripted("closing never rejects", async (engine) => {
  const refusals = [
    () => engine.reply(gone("s1"), 404),
    () => engine.reply({ error: "Method not allowed" }, 405),
    () => engine.drop(),
  ];
  for (const script of refusals) {
    const conn = await opened(engine);
    script();
    await conn.close();
    assert.equal(engine.requests.at(-1).method, "DELETE");
    allAnswered(engine);
    engine.requests.length = 0;
  }
});

scripted("closing is bounded when the engine does not answer", async (engine) => {
  engine.reply(answer("s1", true, numberSet("N", 1)));
  const conn = new Connection({ baseUrl: `http://127.0.0.1:${engine.port}`, closeTimeoutMs: 300 });
  await conn.execute("SELECT 1 AS N");
  engine.hang();
  const started = Date.now();
  await conn.close();
  assert.ok(Date.now() - started < 5000, "close() waited past its bound");
  assert.equal(engine.requests.at(-1).method, "DELETE");
});

test("closing after the engine went away does not reject", async () => {
  const engine = await scriptedEngine();
  const conn = await opened(engine);
  await engine.stop();
  await conn.close();
});

scripted("the sdk-shaped facade releases the session on destroy", async (engine) => {
  engine.reply(answer("s1", true, numberSet("ONE", 1)));
  engine.reply(released);
  const connection = snowflake.createConnection({ host: "127.0.0.1", port: engine.port });
  await connection.connectAsync();
  await connection.execute({ sqlText: "SELECT 1 AS ONE" });
  await new Promise((resolve, reject) => {
    connection.destroy((err) => (err ? reject(err) : resolve()));
  });
  assert.equal(engine.requests.at(-1).method, "DELETE");
  allAnswered(engine);
});

// ── a BEGIN that did not take ──────────────────────────────────────────────

const refusedBy = (message, started = false) =>
  ({ success: false, sessionId: "s1", newSession: started, errorMessage: message, resultSets: [] });
const NO_SUCH_OBJECT = "SQL compilation error:\nObject does not exist, or operation cannot be performed.";

// A connection with no scope that has run one statement on session s1.
async function plain(engine) {
  engine.reply(answer("s1", true, numberSet("N", 1)));
  const conn = await connect(engine.dsn);
  await conn.execute("SELECT 1 AS N");
  return conn;
}

async function assertBeginLeftAutocommitOn(engine, conn) {
  engine.reply(answer("s1", false, numberSet("number of rows inserted", 1)));
  await conn.execute("INSERT INTO T VALUES (1)");
  const insert = engine.executes().filter((b) => b.sql === "INSERT INTO T VALUES (1)").at(-1);
  assert.equal(insert.autoCommit, true, "the INSERT went out inside a transaction nobody opened");
  assert.equal(conn.autoCommit, true);
  for (const body of engine.executes().filter((b) => b.sql === "BEGIN")) assert.equal(body.autoCommit, false);
  allAnswered(engine);
}

scripted("a refused BEGIN leaves autocommit on", async (engine) => {
  const conn = await plain(engine);
  engine.reply(refusedBy("SQL compilation error:\nnot now"));
  await assert.rejects(conn.begin(), /not now/);
  await assertBeginLeftAutocommitOn(engine, conn);
});

scripted("an unreadable answer to BEGIN leaves autocommit on", async (engine) => {
  const conn = await plain(engine);
  engine.replyText("<html><body>502 Bad Gateway</body></html>", 502);
  await assert.rejects(conn.begin(), /unreadable body/);
  await assertBeginLeftAutocommitOn(engine, conn);
});

scripted("a hang-up on BEGIN leaves autocommit on", async (engine) => {
  const conn = await plain(engine);
  engine.drop();
  await assert.rejects(conn.begin(), /request failed/);
  await assertBeginLeftAutocommitOn(engine, conn);
});

scripted("a refused queued USE ahead of BEGIN leaves autocommit on", async (engine) => {
  engine.reply(refusedBy(NO_SUCH_OBJECT, true));
  const conn = await connect(`${engine.dsn}/NOPE`);
  await assert.rejects(conn.begin(), /does not exist/);
  engine.reply(answer("s1", false, statusSet()));
  engine.reply(answer("s1", false, numberSet("number of rows inserted", 1)));
  await conn.execute("INSERT INTO T VALUES (1)");
  // BEGIN never went out, and the USE the engine took later went out with autocommit on,
  // as did the INSERT behind it.
  assert.deepEqual(engine.statements(), ["USE DATABASE NOPE", "USE DATABASE NOPE", "INSERT INTO T VALUES (1)"]);
  assert.deepEqual(engine.executes().map((b) => b.autoCommit), [true, true, true]);
  assert.equal(conn.autoCommit, true);
  allAnswered(engine);
});

scripted("a statement queued before begin() goes out with autocommit on", async (engine) => {
  const conn = await plain(engine);
  engine.reply(answer("s1", false, numberSet("number of rows inserted", 1)));
  engine.reply(answer("s1", false, statusSet()));
  const first = conn.execute("INSERT INTO T VALUES (1)");
  const began = conn.begin();
  await first;
  await began;
  const [, insert, begin] = engine.executes();
  assert.equal(insert.autoCommit, true, "a statement issued before begin() went out inside its transaction");
  assert.deepEqual([begin.sql, begin.autoCommit], ["BEGIN", false]);
  assert.equal(conn.autoCommit, false);
  allAnswered(engine);
});

scripted("a statement queued right after commit() or rollback() goes out with autocommit on", async (engine) => {
  const conn = await plain(engine);
  for (let i = 0; i < 6; i++) engine.reply(answer("s1", false, statusSet()));
  await conn.begin();
  const committed = conn.commit();
  const afterCommit = conn.execute("SELECT 'after commit'");
  await committed;
  await afterCommit;
  await conn.begin();
  const rolledBack = conn.rollback();
  const afterRollback = conn.execute("SELECT 'after rollback'");
  await rolledBack;
  await afterRollback;
  assert.deepEqual(engine.executes().slice(1).map((b) => `${b.sql} ${b.autoCommit}`), [
    "BEGIN false", "COMMIT false", "SELECT 'after commit' true",
    "BEGIN false", "ROLLBACK false", "SELECT 'after rollback' true",
  ]);
  allAnswered(engine);
});

// ── against the engine ─────────────────────────────────────────────────────

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

// Asked over the wire rather than of the driver: an engine that answers newSession also
// takes requireSession and releases sessions on DELETE.
async function engineReleasesSessions(t) {
  const probe = await (await fetch(`${httpBase}/api/execute`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sql: "SELECT 1" }),
  })).json();
  if (!("newSession" in probe)) {
    t.skip("this engine predates requireSession and session release");
    return false;
  }
  await releaseOutOfBand(probe.sessionId);
  return true;
}

async function releaseOutOfBand(sessionId) {
  const resp = await fetch(`${httpBase}/api/sessions/${sessionId}`, { method: "DELETE" });
  assert.equal(resp.status, 200);
  await resp.arrayBuffer();
}

async function activeSessions() {
  return (await (await fetch(`${httpBase}/api/sessions`)).json()).activeSessions;
}

async function scoped(database) {
  const admin = await connect(dsn);
  await admin.execute(`CREATE OR REPLACE DATABASE ${database}`);
  await admin.execute(`CREATE SCHEMA ${database}.s2`);
  await admin.execute(`CREATE TABLE ${database}.s2.t (a INTEGER)`);
  await admin.close();
  const conn = await connect(`${dsn}/${database}?schema=s2`);
  await conn.execute("SELECT 1");
  return conn;
}

test("a session released out of band is replaced on the DSN's scope", integration, async (t) => {
  if (!(await engineReleasesSessions(t))) return;
  const conn = await scoped("js_lost_db");
  const lost = conn.sessionId;
  await releaseOutOfBand(lost);
  const { rows } = await conn.execute("SELECT CURRENT_DATABASE() AS D, CURRENT_SCHEMA() AS S");
  assert.deepEqual(rows, [{ D: "JS_LOST_DB", S: "S2" }]);
  assert.notEqual(conn.sessionId, lost);
  await conn.close();
});

test("a session released with a transaction open is reported", integration, async (t) => {
  if (!(await engineReleasesSessions(t))) return;
  const conn = await scoped("js_losttx_db");
  await conn.begin();
  await conn.execute("INSERT INTO t VALUES (1)");
  await releaseOutOfBand(conn.sessionId);
  await sessionLost(conn.execute("INSERT INTO t VALUES (2)"), /transaction/);
  // Still usable, on a fresh session: the release rolled the first row back, and the
  // second never ran.
  const { rows } = await conn.execute("SELECT COUNT(*) AS N FROM t");
  assert.deepEqual(rows, [{ N: 0 }]);
  await conn.close();
});

test("a begin() that failed leaves the next statement committing", integration, async () => {
  const admin = await connect(dsn);
  await admin.execute("CREATE OR REPLACE DATABASE js_failedbegin_db");
  const conn = await connect(`${dsn}/js_failedbegin_db?schema=s_late`);
  // USE SCHEMA is refused while the schema does not exist, and with it the begin() that
  // puts the DSN's scope on first.
  await assert.rejects(conn.begin(), /does not exist/);
  await admin.execute("CREATE SCHEMA js_failedbegin_db.s_late");
  await admin.execute("CREATE TABLE js_failedbegin_db.s_late.t (a INTEGER)");
  await conn.execute("INSERT INTO t VALUES (1)");
  // No transaction was opened, so the row committed on its own: a second session sees it.
  const { rows } = await admin.execute("SELECT COUNT(*) AS N FROM js_failedbegin_db.s_late.t");
  assert.deepEqual(rows, [{ N: 1 }]);
  assert.equal(conn.autoCommit, true);
  await conn.close();
  await admin.close();
});

test("closing releases the session", integration, async (t) => {
  if (!(await engineReleasesSessions(t))) return;
  const conn = await connect(dsn);
  await conn.execute("SELECT 1");
  const before = await activeSessions();
  await conn.close();
  assert.equal(await activeSessions(), before - 1);
  await conn.close();
  assert.equal(await activeSessions(), before - 1);
});
