import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import worker, { dispatch, WORKFLOW_BY_CRON } from "../src/index.js";

test("every configured cron maps to a workflow, and only those", () => {
  const config = JSON.parse(
    readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8").replace(/^\s*\/\/.*$/gm, ""),
  );
  assert.deepEqual([...config.triggers.crons].sort(), Object.keys(WORKFLOW_BY_CRON).sort());
  assert.equal(config.workers_dev, false);
  assert.equal(config.routes, undefined);
});

test("dispatch posts to the workflow on main with the token", async () => {
  const calls = [];
  const fake = async (url, init) => {
    calls.push({ url, init });
    return new Response(null, { status: 204 });
  };
  await dispatch("hourly.yml", "tok", fake);
  assert.equal(
    calls[0].url,
    "https://api.github.com/repos/sKoih-pond/osrs-ge-market-intel/actions/workflows/hourly.yml/dispatches",
  );
  assert.equal(calls[0].init.headers.Authorization, "Bearer tok");
  assert.deepEqual(JSON.parse(calls[0].init.body), { ref: "main" });
});

test("a refused dispatch fails the invocation", async () => {
  const fake = async () => new Response("Bad credentials", { status: 401 });
  await assert.rejects(dispatch("hourly.yml", "tok", fake), /HTTP 401/);
  await assert.rejects(dispatch("hourly.yml", "", fake), /not set/);
});

test("an unknown cron fails rather than dispatching nothing silently", async () => {
  await assert.rejects(worker.scheduled({ cron: "0 0 * * *" }, { GITHUB_DISPATCH_TOKEN: "t" }), /no workflow/);
});
