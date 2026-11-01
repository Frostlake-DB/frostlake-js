import { test } from "node:test";
import assert from "node:assert/strict";
import { connect, formatLiteral, substitute, FrostlakeError } from "../index.js";

// ── substitute ─────────────────────────────────────────────────────────────

test("substitute leaves dollar-quoted strings alone", () => {
  assert.equal(
    substitute("SELECT $$a?b$$ AS S, ? AS N", [7]),
    "SELECT $$a?b$$ AS S, 7 AS N",
  );
});

test("substitute passes an unterminated dollar-quote through", () => {
  assert.equal(substitute("SELECT $$a?b", []), "SELECT $$a?b");
});

test("substitute writes a negative number in parentheses", () => {
  assert.equal(substitute("SELECT 3-?", [-5]), "SELECT 3-(-5)");
  assert.equal(substitute("SELECT ?::VARCHAR", [-5]), "SELECT (-5)::VARCHAR");
  assert.equal(substitute("SELECT ?, ?", [-1.5, -7n]), "SELECT (-1.5), (-7)");
  assert.equal(substitute("SELECT ?, ?, ?", [5, 0, 7n]), "SELECT 5, 0, 7");
  assert.equal(formatLiteral([1, -2]), "[1, (-2)]");
});

test("substitute reads a name whole: the $$ inside one opens no body", () => {
  assert.equal(substitute("SELECT 1 AS a$$b, ?", [7]), "SELECT 1 AS a$$b, 7");
  assert.equal(substitute("SELECT a$$b, ? FROM t$$ WHERE c$ = ?", [7, 8]), "SELECT a$$b, 7 FROM t$$ WHERE c$ = 8");
  assert.equal(substitute("SELECT $$a?b$$, x$$, ?", [7]), "SELECT $$a?b$$, x$$, 7");
});

test("substitute ends a line comment at a carriage return", () => {
  assert.equal(substitute("SELECT 1 -- c\r, ?", [7]), "SELECT 1 -- c\r, 7");
  assert.equal(substitute("SELECT 1 // c?\r\n, ?", [7]), "SELECT 1 // c?\r\n, 7");
  assert.equal(substitute("SELECT 1 -- c?\n, ?", [7]), "SELECT 1 -- c?\n, 7");
});

test("substitute throws when binds run out", () => {
  assert.throws(() => substitute("SELECT ?, ?", [1]), FrostlakeError);
});

test("substitute throws on leftover binds", () => {
  assert.throws(() => substitute("SELECT ?", [1, 2]), FrostlakeError);
  assert.throws(() => substitute("SELECT 'a?b'", ["x"]), FrostlakeError);
});

// ── formatLiteral ──────────────────────────────────────────────────────────

test("formatLiteral renders each bind type", () => {
  assert.equal(formatLiteral(null), "NULL");
  assert.equal(formatLiteral(undefined), "NULL");
  assert.equal(formatLiteral(true), "true");
  assert.equal(formatLiteral(42.5), "42.5");
  assert.equal(formatLiteral(9007199254740993n), "9007199254740993");
  assert.equal(formatLiteral("O'Hara \\"), "'O''Hara \\\\'");
  assert.equal(
    formatLiteral(new Date(Date.UTC(2026, 0, 2, 3, 4, 5, 678))),
    "'2026-01-02T03:04:05.678'::TIMESTAMP_NTZ",
  );
  assert.equal(formatLiteral(Buffer.from([0xde, 0xad])), "X'dead'");
  assert.equal(formatLiteral(new Uint8Array([0x01, 0xff])), "X'01ff'");
  assert.equal(formatLiteral([1, "a", [true, null]]), "[1, 'a', [true, NULL]]");
});

test("formatLiteral rejects values with no SQL shape", () => {
  assert.throws(() => formatLiteral(NaN), FrostlakeError);
  assert.throws(() => formatLiteral(Infinity), FrostlakeError);
  assert.throws(() => formatLiteral({ a: 1 }), FrostlakeError);
});

// ── connect / DSN errors (no server involved) ──────────────────────────────

test("connect rejects malformed DSNs as FrostlakeError", async () => {
  await assert.rejects(connect("not a dsn"), FrostlakeError);
  await assert.rejects(connect("mysql://localhost:3306/db"), FrostlakeError);
  await assert.rejects(connect("frostlake:///db"), FrostlakeError);
});

test("connect wraps an unreachable server in FrostlakeError", async () => {
  await assert.rejects(connect("frostlake://127.0.0.1:1"), FrostlakeError);
});

test("a frostlake:// DSN without a port means the server default, 18082", async () => {
  // .invalid never resolves (RFC 2606), so this fails fast — the error text
  // carries the base URL the driver actually targeted.
  await assert.rejects(connect("frostlake://nonexistent.invalid/db"), /:18082/);
});
