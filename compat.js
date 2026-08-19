/**
 * A snowflake-sdk-shaped facade over the Frostlake driver, so code written against
 * the official Snowflake Node.js connector's basic surface can switch with an
 * import change:
 *
 *   import snowflake from "frostlake/compat";
 *   const connection = snowflake.createConnection({ host: "localhost", port: 18082, database: "MY_DB" });
 *   connection.connect((err, conn) => { ... });
 *   connection.execute({ sqlText: "SELECT ?", binds: [1], complete: (err, stmt, rows) => { ... } });
 *
 * Covered: createConnection, connect/connectAsync, execute with sqlText/binds/complete
 * (rows as objects keyed by column name, like the SDK), destroy. Everything beyond
 * that basic surface is out of scope.
 */

import { Connection as CoreConnection, FrostlakeError } from "./index.js";

class CompatConnection {
  #core;

  constructor(options) {
    const host = options.host ?? "localhost";
    const port = options.port ?? 18082;
    this.#core = new CoreConnection({
      baseUrl: `http://${host}:${port}`,
      database: options.database ?? null,
      schema: options.schema ?? null,
    });
  }

  connect(callback) {
    this.#core.ping().then(
      () => callback?.(null, this),
      (err) => callback?.(err, this),
    );
    return this;
  }

  async connectAsync() {
    await this.#core.ping();
    return this;
  }

  execute({ sqlText, binds = [], complete }) {
    const run = this.#core.execute(sqlText, binds);
    if (complete) {
      run.then(
        ({ rows, rowCount, columns }) => {
          const stmt = {
            getSqlText: () => sqlText,
            getNumRows: () => (rows.length > 0 ? rows.length : rowCount),
            getColumns: () => columns.map((c) => ({ getName: () => c.name, getType: () => c.dataType })),
          };
          complete(null, stmt, rows);
        },
        (err) => complete(err, { getSqlText: () => sqlText }, undefined),
      );
    }
    return run;
  }

  destroy(callback) {
    this.#core.close().then(
      () => callback?.(null, this),
      (err) => callback?.(err, this),
    );
  }
}

export function createConnection(options) {
  if (!options || typeof options !== "object") {
    throw new FrostlakeError("createConnection requires an options object");
  }
  return new CompatConnection(options);
}

export default { createConnection };
