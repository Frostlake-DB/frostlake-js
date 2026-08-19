# frostlake-js

A zero-dependency Node.js driver for [Frostlake](https://frostlake.dev), speaking the
engine's HTTP protocol against a running `DatabaseHttpServer`. Node ≥ 18 (built-in
`fetch`), ESM, no runtime dependencies.

## Engine version

Requires a Frostlake engine **0.0.7 or newer**. Ask a running server which one it is with
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
column name, `columns` is `[{ name, dataType }]`, and for DML (including MERGE)
`rowCount` is the affected-row count. A failed statement rejects with a
`FrostlakeError` carrying the engine's error message; `connect()` rejects the same
way when the server is unreachable. Statements on one connection are serialized in
call order, so `Promise.all` over a connection is safe and stays on one session.
When `sql` contains several `;`-separated statements, every result is on
`resultSets` (each `{ columns, rows, rowCount }`), with the top-level fields
mirroring the first.

### Transactions

```js
await conn.begin();
await conn.execute("INSERT INTO acc VALUES (2)");
await conn.rollback();   // or conn.commit()
```

`begin()` turns autocommit off for the connection; `commit()`/`rollback()` restore it.

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
substitution unit tests run.

## Protocol

One `POST /api/execute` per statement with `{ sql, sessionId, autoCommit }`; the
server issues the `sessionId` on first contact and the driver echoes it back, so
session state (current database/schema, transactions) persists across statements.
`GET /api/health` backs `connect()`'s reachability check.
