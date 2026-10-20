// ge-clock: the pipeline's hourly clock (ADR-0017). GitHub never fired this repository's
// `schedule` events, so Cloudflare Cron Triggers call GitHub's workflow_dispatch API instead.
// No fetch handler, no route and no workers.dev URL: nothing can reach this Worker over HTTP.

export const REPO = "sKoih-pond/osrs-ge-market-intel";

// One cron expression per workflow; keep in step with "triggers" in wrangler.jsonc.
export const WORKFLOW_BY_CRON = {
  "6,26 * * * *": "hourly.yml",
  "13,43 * * * *": "watchdog.yml",
};

export async function dispatch(workflow, token, fetchImpl = fetch) {
  if (!token) throw new Error("GITHUB_DISPATCH_TOKEN is not set");
  const resp = await fetchImpl(
    `https://api.github.com/repos/${REPO}/actions/workflows/${workflow}/dispatches`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ge-clock (+https://github.com/sKoih-pond/osrs-ge-market-intel)",
      },
      body: JSON.stringify({ ref: "main" }),
    },
  );
  if (resp.status !== 204) {
    // Thrown errors show as failed invocations in the Worker's logs.
    throw new Error(`dispatch ${workflow} failed: HTTP ${resp.status} ${(await resp.text()).slice(0, 200)}`);
  }
}

export default {
  async scheduled(controller, env) {
    const workflow = WORKFLOW_BY_CRON[controller.cron];
    if (!workflow) throw new Error(`no workflow for cron "${controller.cron}"`);
    await dispatch(workflow, env.GITHUB_DISPATCH_TOKEN);
    console.log(JSON.stringify({ event: "dispatched", workflow, cron: controller.cron }));
  },
};
