// GE screener, v1 data contract. No build step and no dependencies (M1).
"use strict";

const DATA = "/ge/data/v1/";
const gp = new Intl.NumberFormat("en-AU");
const LIMITED_BY = { volume: "market volume", buy_limit: "buy limit", capital: "capital" };
const GRADE_LABEL = { A: "sound", B: "usable with care", C: "withheld" };

function utc(iso, withDate = true) {
  if (!iso) return "unknown";
  const d = new Date(iso);
  const opts = { timeZone: "UTC", hour: "2-digit", minute: "2-digit", hour12: false };
  if (withDate) Object.assign(opts, { day: "numeric", month: "short", year: "numeric" });
  return `${d.toLocaleString("en-AU", opts)} UTC`;
}

function ago(minutes) {
  if (minutes < 90) return `${Math.round(minutes)} minutes`;
  const hours = minutes / 60;
  return hours < 48 ? `${hours.toFixed(1)} hours` : `${Math.round(hours / 24)} days`;
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "text") node.textContent = v;
    else node.setAttribute(k, v);
  }
  for (const c of children) node.append(c);
  return node;
}

async function load(name) {
  const resp = await fetch(`${DATA}${name}.json`, { cache: "no-cache" });
  if (!resp.ok) throw new Error(`${name}.json: HTTP ${resp.status}`);
  return resp.json();
}

// AT3: every page says so at the top once the newest data is over the stale limit.
function staleness(status) {
  const banner = document.getElementById("stale");
  const ageMin = (Date.now() - Date.parse(status.data_as_of)) / 60000;
  if (ageMin > status.stale_after_minutes) {
    banner.textContent =
      `Stale data: the newest prices are ${ago(ageMin)} old (data to ${utc(status.data_as_of)}). ` +
      "The hourly pipeline has not published since, so ideas below may no longer hold.";
    banner.hidden = false;
  }
}

function fail(err) {
  const banner = document.getElementById("stale");
  banner.textContent = `The data could not be loaded (${err.message}). Try again in a few minutes.`;
  banner.hidden = false;
  document.getElementById("asof").textContent = "No data loaded.";
}

function fact(dl, term, value, defId) {
  const dt = defId ? el("dt", {}, el("a", { href: `#def-${defId}`, text: term })) : el("dt", { text: term });
  dl.append(dt, el("dd", {}, value));
}

function card(idea) {
  const facts = el("dl", { class: "facts" });
  fact(facts, "Buy at", `${gp.format(idea.buy_price)} GP`, "buy_price");
  fact(facts, "Sell at", `${gp.format(idea.sell_price)} GP`, "sell_price");
  fact(facts, "Tax", idea.tax_exempt ? "0 GP (exempt)" : `${gp.format(idea.tax_per_item)} GP`, "tax_per_item");
  fact(facts, "Net margin", `${gp.format(idea.net_margin)} GP each`, "net_margin");
  fact(
    facts,
    "Quantity",
    `${gp.format(idea.feasible_quantity)}, set by ${LIMITED_BY[idea.quantity_limited_by]}`,
    "feasible_quantity",
  );
  fact(
    facts,
    "Price window",
    `${utc(idea.price_window_start)} to ${utc(idea.price_window_end, false)}`,
    "price_window",
  );
  const grade = el(
    "p",
    { class: "grade" },
    el("a", { href: "#def-data_grade", class: `badge grade-${idea.data_grade}`, "aria-label": `Data grade ${idea.data_grade}, ${GRADE_LABEL[idea.data_grade]}` },
      `Data ${idea.data_grade}`),
    ` ${GRADE_LABEL[idea.data_grade]}: ${idea.data_grade_reason}`,
  );
  return el(
    "li",
    { class: "idea" },
    el("h3", {}, `${idea.rank}. ${idea.name}`),
    el("p", { class: "net" }, el("a", { href: "#def-net_gp", text: "Net" }), ` ${gp.format(idea.net_gp)} GP`),
    grade,
    facts,
  );
}

async function screener() {
  try {
    const [snapshot, status] = await Promise.all([load("snapshot"), load("status")]);
    staleness(status);
    document.getElementById("asof").textContent =
      `Prices from the hour ending ${utc(snapshot.data_as_of)}. Published ${utc(snapshot.generated_at)}.`;
    const c = snapshot.counts;
    document.getElementById("summary").textContent =
      `${gp.format(c.ideas)} items clear the tax with grade A or B data; the top ${c.ideas_published} are listed. ` +
      `${gp.format(c.withheld_grade_c_with_margin)} more show a margin but are withheld because their data is grade C.`;
    const list = document.getElementById("ideas");
    list.replaceChildren(...snapshot.ideas.map(card));
    if (!snapshot.ideas.length) list.replaceChildren(el("li", { text: "No item clears the tax with sound data this hour. Check back after the next update." }));
  } catch (err) {
    fail(err);
  }
}

function share(x) {
  return x === null ? "not yet measured" : `${(x * 100).toFixed(1)}%`;
}

async function statusPage() {
  try {
    const status = await load("status");
    staleness(status);
    document.getElementById("asof").textContent =
      `Data to ${utc(status.data_as_of)}, published ${utc(status.generated_at)}.`;
    const last = document.getElementById("last");
    const run = status.last_successful_run;
    fact(last, "Last successful run", run ? `${utc(run.finished_at)} (hour ending ${utc(run.interval_end, false)})` : "none recorded yet");
    fact(last, "Newest data", utc(status.data_as_of));
    const f = status.freshness_30d;
    const k = status.completeness_30d;
    const totals = document.getElementById("totals");
    fact(totals, "Hours published within 45 minutes", `${share(f.share_on_time)} (${gp.format(f.hours_on_time)} of ${gp.format(f.hours_expected)} hours; target 95%)`);
    fact(totals, "Buckets recorded", `${share(k.share_present)} (${gp.format(k.buckets_present)} of ${gp.format(k.buckets_expected)})`);
    fact(totals, "Outage buckets", gp.format(k.outage_buckets));
    const body = document.querySelector("#days tbody");
    body.replaceChildren(
      ...status.days.slice().reverse().map((d) =>
        el(
          "tr",
          {},
          el("th", { scope: "row", text: d.date }),
          el("td", { text: d.hours_expected ? `${d.hours_on_time} of ${d.hours_expected}` : "none due" }),
          el("td", { text: `${gp.format(d.buckets_present)} of ${gp.format(d.buckets_expected)}` }),
          el("td", { text: gp.format(d.outage_buckets) }),
        ),
      ),
    );
  } catch (err) {
    fail(err);
  }
}

if (document.body.dataset.page === "status") statusPage();
else screener();
