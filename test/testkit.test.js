import { test } from "node:test";
import assert from "node:assert/strict";
import { main, NO_CORPUS, suiteFiles } from "../testkit/runner.mjs";

// The engine's testkit corpus, replayed through testkit/runner.mjs when FL_CORPUS names it:
// against FROSTLAKE_URL, or a private engine the runner boots from FROSTLAKE_CLASSPATH.
test("the testkit corpus replays without a failed case", { skip: process.env.FL_CORPUS ? false : NO_CORPUS }, async (t) => {
  // A directory without suites fails here, before the engine is looked for.
  suiteFiles(process.env.FL_CORPUS);
  if (!process.env.FROSTLAKE_URL && !process.env.FROSTLAKE_CLASSPATH) {
    t.skip("FROSTLAKE_URL and FROSTLAKE_CLASSPATH not set");
    return;
  }
  assert.equal(await main(), 0, "the replay failed: its output above names the failed cases");
});
