# frostlake-js

A zero-dependency Node.js driver for [Frostlake](https://frostlake.dev), speaking the
engine's HTTP protocol against a running `DatabaseHttpServer`. Node ≥ 18 (built-in
`fetch`), ESM, no runtime dependencies.

## Engine version

Requires a Frostlake engine **0.2.0 or newer**. Ask a running server which one it is with
`SELECT CURRENT_VERSION()` — every release answers it, so the check works against any engine.

The driver versions independently of the engine: it speaks the HTTP protocol, not
the jar, so this is a floor rather than a lockstep pin.

## Usage

```js
import { connect } from "frostlake";

const conn = await connect("frostlake://localhost:18082/MY_DB?schema=PUBLIC");
// omitting the port means the server default, 18082

await conn.execute("CREATE TABLE people (id INTEGER, name VARCHAR)");
const ins = await conn.execute("INSERT INTO people VALUES (?, ?), (?, ?)",
  [1, "Ada", 2, "Grace"]);
console.log(ins.rowCount); // 2

const { rows } = await conn.execute("SELECT id, name FROM people WHERE id = ?", [1]);
console.log(rows); // [ { ID: 1, NAME: 'Ada' } ]

await conn.close();
```

`execute` resolves to `{ columns, rows, rowCount }`: `rows` are plain objects keyed by
column name, `columns` is `[{ name, dataType, length }]`, and for DML (including MERGE)
`rowCount` is the affected-row count. `length` is the width a text or binary column was
declared with — characters for `VARCHAR(9)`, bytes for `BINARY(5)`, and the maximum
(16777216 / 8388608) for one declared without a width; it is `null` for every other
type, which is how "the server said nothing" is spelled rather than a width of `0`. A failed statement rejects with a
`FrostlakeError` carrying the engine's error message; `connect()` rejects the same
way when the server is unreachable. Statements on one connection are serialized in
call order, so `Promise.all` over a connection is safe and stays on one session.
When `sql` contains several `;`-separated statements, every result is on
`resultSets` (each `{ columns, rows, rowCount }`), with the top-level fields
mirroring the first.

### Rows as arrays

Keyed by column name, a row keeps only the last of two columns that share a name — a
self-join's `SELECT e.name, m.name` reads back as one `NAME`. Ask for arrays to keep
every cell, in column order (`columns[i]` describes cell `i`):

```js
const { rows } = await conn.execute(
  "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr_id = m.id", [], { rowMode: "array" });
console.log(rows); // [ [ 'Alice', 'CEO' ] ]
```

`rowMode` is `"object"` (the default) or `"array"`, as on the account's own Node connector;
the facade's `execute` takes it too.

### Transactions

```js
await conn.begin();
await conn.execute("INSERT INTO acc VALUES (2)");
await conn.rollback();   // or conn.commit()
```

`begin()` turns autocommit off for the connection; `commit()`/`rollback()` restore it. So
does losing the session with the transaction open (see *Session lifetime*). Each takes
effect in its turn among the connection's queued statements, and only once its statement
took. A statement queued before `begin()` still runs outside the transaction. A `begin()`
that rejects (a refused DSN `USE`, a refused or lost `BEGIN`) leaves autocommit on, so the
next statement commits on its own.

### Bind values

Parameters are inlined client-side (`?` placeholders); placeholders inside string
literals (including `$$…$$` dollar-quoted ones), quoted identifiers and comments
are left alone. A bind-count mismatch in either direction throws.

| JS value | SQL literal |
| --- | --- |
| `null` / `undefined` | `NULL` |
| `boolean`, `number`, `bigint` | as written |
| `string` | `'…'` (backslashes and quotes escaped) |
| `Date` | `'…'::TIMESTAMP_NTZ` |
| `Buffer` / `Uint8Array` | `X'hex'` |
| `Array` | `[…]` (elements formatted recursively) |

Integral `NUMBER` columns arrive as JS numbers; values beyond 2^53 lose precision,
and so do `NUMBER(p,s)` decimal digits beyond what a JS double holds — cast to
`VARCHAR` in SQL if you need exact digits. `BINARY` columns read back as `Buffer`s.
`ARRAY`/`OBJECT`/`VARIANT` columns read back as Snowflake's text rendering — note a
SQL `NULL` array element appears as `undefined`, so that text is not always
parseable JSON.

### Several statements in one request

A request carries one statement unless it says otherwise, so `A; B` is refused with
`Actual statement count 2 did not match the desired statement count 1.` Ask for a pack per call:

```js
await conn.execute("SELECT 1; SELECT 2", [], { multiStatementCount: 2 });
```

`0` allows any number. The count applies to that one request: it does not change the session's
`MULTI_STATEMENT_COUNT`, so anything else sharing the connection is unaffected and there is nothing
to restore afterwards. `ALTER SESSION SET MULTI_STATEMENT_COUNT = n` still works when a whole session
should carry packs.

Through the facade it is a statement parameter, as it is on the account's own Node connector:

```js
connection.execute({ sqlText: "SELECT 1; SELECT 2", parameters: { MULTI_STATEMENT_COUNT: 2 } });
```

## Session lifetime

A connection holds one engine session, which carries its current database and schema,
session variables, `ALTER SESSION` settings and an open transaction. The engine names the
session in its first answer (`conn.sessionId` reads it back), and every later request names
it again.

- **What is sent.** Once the engine has shown that it tracks sessions (its answers carry
  `newSession`, as engines from 0.1.0 do), every request that names the session also sends
  `requireSession: true`: resume this session, or refuse. An engine whose answers lack the
  field is never sent it.
- **After a lost session.** The engine forgets a session that sat idle for 30 minutes, was
  released, or went with a restart, and it refuses a request that requires it (HTTP 404)
  without running anything. The driver drops the session and then:
  - when the session held nothing a fresh one would lack, it starts a fresh session on the
    DSN's scope (its `USE DATABASE` / `USE SCHEMA`) and sends the statement once more. A
    second refusal rejects;
  - when the session held an open transaction, or context set up with `USE`, `SET` /
    `UNSET`, `ALTER SESSION`, a temporary object, or a `CREATE` / `DROP` of a database or
    schema, it rejects with `SessionLostError` (exported beside `FrostlakeError`, and a
    subclass of it) instead. The statement did not run: in a fresh session it would run
    somewhere its author did not intend. The connection stays usable, and its next statement
    starts a fresh session on the DSN's scope. A lost transaction turns autocommit back on,
    as `rollback()` would.

  The driver reads what a statement did from its text: `BEGIN` / `START TRANSACTION` and
  `COMMIT` / `ROLLBACK` for the transaction, and the statements above for context. With
  autocommit off, any statement counts as holding a transaction.
- **Close.** `close()` waits for the statements already queued, then releases the session
  with `DELETE /api/sessions/{id}`, which rolls back a transaction left open. An engine
  without that endpoint gets a `ROLLBACK` for an open transaction instead, and keeps the
  session until its own idle expiry. Either is one request, bounded to 5 seconds. `close()`
  never rejects, and a second `close()` sends nothing. The facade's `destroy()` is `close()`.

## snowflake-sdk-style facade

Code written against the official Snowflake Node.js connector's basic surface can
switch with an import change:

```js
import snowflake from "frostlake/compat";

const connection = snowflake.createConnection({ host: "localhost", port: 18082 });
await connection.connectAsync();          // or connection.connect(callback)
connection.execute({
  sqlText: "SELECT ? AS one",
  binds: [1],
  complete: (err, stmt, rows) => console.log(rows), // [ { ONE: 1 } ]
});
```

Covered: `createConnection`, `connect`/`connectAsync`, `execute({sqlText, binds,
complete})`, `destroy`. `execute` also returns a promise of the core result, so the
facade works without callbacks too.

## Running the tests

The integration tests boot a real server from the engine's compiled classes:

```sh
export JAVA_HOME=/path/to/jdk17
export FROSTLAKE_CLASSPATH="/path/to/frostlake/engine/target/classes:<engine deps>"
npm test
```

Without `FROSTLAKE_CLASSPATH` the integration tests are skipped and only the
unit tests run: substitution, and the session scenarios in `test/session.test.js`, which
play against a scripted stand-in engine.

## Testkit corpus runner

`testkit/runner.mjs` replays the engine's testkit corpus — the language-neutral JSON
suites in the frostlake repo's `engine/src/test/resources/testkit/suites`, format in the
`SCHEMA.md` beside them — through this driver, each case on a session of its own, and
compares the results the way the engine's reference runner does. `FL_CORPUS` names that
testkit directory (an absolute path is safest); without it nothing is replayed. With it,
`npm test` replays the corpus as well, on an engine set up as for the tests, and fails on
a failed case:

```sh
FL_CORPUS=/path/to/frostlake/engine/src/test/resources/testkit npm test
```

On its own, point the runner at a running server, started fresh (account-level objects
outlive the per-case reset):

```sh
FL_CORPUS=/path/to/frostlake/engine/src/test/resources/testkit \
FROSTLAKE_URL=frostlake://127.0.0.1:18082 node testkit/runner.mjs
```

Or leave `FROSTLAKE_URL` unset and export `FROSTLAKE_CLASSPATH` (and `JAVA_HOME`), as for
the tests, to have it boot a private engine. `FROSTLAKE_TESTKIT_FILTER=word1,word2`
replays only the suites whose file name contains one of the words.

Every case becomes a row of `results/testkit-js.tsv` (or `FROSTLAKE_TESTKIT_REPORT`):
PASS, FAIL, ERROR, or SKIP when its skip clause names `js` or `http`.
`missing-apis-js.md` beside it lists the checks this transport cannot express — an
expected error's code or SQLSTATE — which are not failures. The run ends with
`testkit [js]: <passed> passed, <failed> failed, <skipped> skipped` and exits 1 when any
case failed or errored.

## Protocol

One `POST /api/execute` per statement with `{ sql, sessionId, autoCommit }`; the
server issues the `sessionId` on first contact and the driver echoes it back, so
session state (current database/schema, transactions) persists across statements.
Requests that name the session also carry `requireSession: true` once the engine has
answered with `newSession`, and `close()` sends `DELETE /api/sessions/{id}` (see *Session
lifetime*). `GET /api/health` backs `connect()`'s reachability check.
