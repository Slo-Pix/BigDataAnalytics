"use strict";

const livePresets = {
  latest: {
    name: "Latest observations",
    sql: "SELECT title, category, infohash, tracker, seeders, leechers, scraped_at\nFROM live_latest\nWHERE scraped_at >= now() - interval '10 minutes'\nORDER BY scraped_at DESC, seeders DESC\nLIMIT 50",
  },
  categories: {
    name: "Activity by category",
    sql: "SELECT category, count(DISTINCT infohash) AS torrents,\n       max(seeders) AS peak_tracker_seeders, max(scraped_at) AS last_seen\nFROM live_latest\nWHERE scraped_at >= now() - interval '10 minutes'\nGROUP BY category\nORDER BY torrents DESC",
  },
  minutes: {
    name: "Minute throughput",
    sql: "SELECT minute, sum(rows) AS observations\nFROM live_minutes\nWHERE minute >= now() - interval '30 minutes'\nGROUP BY minute\nORDER BY minute DESC",
  },
  retained: {
    name: "All retained observations",
    sql: "SELECT title, category, tracker, seeders, leechers, scraped_at\nFROM live_latest\nORDER BY seeders DESC\nLIMIT 100",
  },
  batches: {
    name: "Stream batch ledger",
    sql: "SELECT batch_id, rows, applied_at\nFROM live_batches\nORDER BY applied_at DESC\nLIMIT 30",
  },
};
const liveViewState = { sql: livePresets.latest.sql, result: null, targets: 500, context: null, actionBusy: false, actionError: null, queryBusy: false };
const liveValue = value => value !== null && typeof value === "object" ? JSON.stringify(value) : value;

async function liveApi(path = "", options = {}, signal) {
  const response = await fetch(`/api/contentwatch/live${path}`, { ...options, signal, headers: { ...options.headers, "X-CWatch-Request": "1" } });
  const body = await response.json();
  if (!response.ok) throw new Error(typeof body.detail === "string" ? body.detail : `Live request failed (${response.status}).`);
  return body;
}

function livePage(signal) {
  const context = { signal, timer: null, refreshing: false };
  liveViewState.context = context;
  root.innerHTML = workShell(
    heading("Live monitoring & queries", "Collect fresh tracker observations, then explore the committed data.") +
    `<div class="notice">${icon("info")}Live monitoring checks known hashes from the historical catalogue. It does not discover new releases or prove infringement. Data is isolated from the research and classroom-demo datasets.</div>
    <section class="panel">
      <div class="panel-head"><div><h2>Collection controls</h2><p>Tracker scraper → Kafka → Spark → PostgreSQL + Parquet</p></div><span id="live-phase" class="badge" role="status">Connecting…</span></div>
      <form id="live-start-form" class="live-controls">
        <label class="field">Historically busiest known hashes<select name="targets">${[100, 500, 2000].map(n => `<option value="${n}" ${liveViewState.targets === n ? "selected" : ""}>${number(n)} torrent hashes</option>`).join("")}</select></label>
        <button id="live-start" class="button" type="submit" disabled>${icon("eye")}Start live monitoring</button>
        <button id="live-stop" class="button outline" type="button" data-action="live-stop" disabled>Stop collection</button>
      </form>
      <p id="live-message" class="fineprint" role="status">Checking the live pipeline…</p>
      <div id="live-errors" aria-live="polite"></div>
      <p class="fineprint">Starting resumes retained data and checkpoints; nothing is reset. Collection continues when you leave this tab. Stopping keeps committed data queryable. Initial startup can take 1–2 minutes.</p>
    </section>
    <div id="live-stats" class="stats">${stat("Committed observations", "—", "Across retained live batches", "layers", true)}${stat("Monitored torrents", "—", "Distinct observed hashes", "eye")}${stat("Committed batches", "—", "Serving-layer transaction ledger", "grid")}${stat("Last observation", "—", "Freshness is checked every 5 seconds", "bell")}</div>
    <div class="dashboard-grid">
      <section class="panel"><div class="panel-head"><div><h2>Incoming observations</h2><p>Committed records per minute · last 20 minutes</p></div>${icon("layers")}</div><div id="live-throughput" class="empty-state">No committed observations yet.</div></section>
      <section class="panel"><div class="panel-head"><div><h2>Current serving snapshot</h2><p>Highest tracker-reported counts across retained records</p></div>${icon("eye")}</div><div id="live-top" class="empty-state">Start monitoring to populate this view.</div></section>
    </div>
    <section class="panel">
      <div class="panel-head"><div><h2>Query your live data</h2><p>Read-only PostgreSQL · 5-second timeout · at most 200 returned rows</p></div></div>
      <div class="live-presets" aria-label="Example queries">${Object.entries(livePresets).map(([key, preset]) => `<button class="button outline small" type="button" data-live-preset="${key}">${preset.name}</button>`).join("")}</div>
      <form id="live-query-form"><label class="field">Query<textarea id="live-sql" name="sql" required maxlength="10000" spellcheck="false">${esc(liveViewState.sql)}</textarea></label>
        <div class="live-query-actions"><button id="live-run-query" class="button" type="submit">${icon("search")}Run query</button><button id="live-export" class="button outline" type="button" data-action="live-export" disabled>${icon("download")}Export results</button><span class="fineprint no-margin">Queries run on click, not on each refresh.</span></div>
      </form>
      <details class="live-schema"><summary>Available views & metric definitions</summary><dl>
        <dt>live_latest</dt><dd>title, category, infohash, tracker, seeders, leechers, completed, scraped_at — newest observation per hash and tracker.</dd>
        <dt>live_minutes</dt><dd>minute, tracker, rows, total_seeders, total_leechers — counts accumulated from scrape events, not distinct peers.</dd>
        <dt>live_batches</dt><dd>query, batch_id, rows, applied_at — committed Spark serving batches.</dd>
      </dl><p>Use a recent scraped_at filter for fresh activity. Views retain earlier runs. Do not sum peer counts across trackers as unique people. Full event history is retained in the isolated Parquet output, not exposed by these SQL views.</p></details>
      <div id="live-query-results" aria-live="polite"><div class="empty-state">Choose an example or write a SELECT query, then run it.</div></div>
    </section>`, "live");
  signal.addEventListener("abort", () => clearTimeout(context.timer), { once: true });
  document.querySelector("#live-sql").addEventListener("input", event => { liveViewState.sql = event.target.value; });
  if (liveViewState.result) renderLiveQuery(liveViewState.result);
  refreshLive(context);
}

async function refreshLive(context) {
  if (context.signal.aborted || context.refreshing) return;
  clearTimeout(context.timer);
  context.refreshing = true;
  try {
    const status = await liveApi("", {}, context.signal);
    if (context.signal.aborted) return;
    const active = ["starting", "collecting", "stopping"].includes(status.phase);
    document.querySelector("#live-start").disabled = !adminRole() || active || liveViewState.actionBusy;
    document.querySelector("#live-stop").disabled = !adminRole() || !active || status.phase === "stopping" || liveViewState.actionBusy;
    document.querySelector('#live-start-form select').disabled = active;
    const badge = document.querySelector("#live-phase");
    badge.textContent = status.phase === "collecting" ? (status.data?.fresh ? "Fresh observations" : "Awaiting fresh data") : displayLabel(status.phase);
    badge.classList.toggle("live-fresh", status.phase === "collecting" && status.data?.fresh);
    document.querySelector("#live-message").textContent = status.message + (status.started_at ? ` Started ${date(status.started_at)} · ${number(status.targets)} hashes.` : "");
    document.querySelector("#live-errors").innerHTML = [liveViewState.actionError, status.error, status.data_error].filter(Boolean).map(error => `<div class="notice error" role="alert">${icon("info")}${esc(error)}</div>`).join("");
    const data = status.data;
    document.querySelector("#live-run-query").disabled = !editorRole() || liveViewState.queryBusy;
    document.querySelector("#live-stats").innerHTML =
      stat("Committed observations", data ? number(data.observations) : "—", "Across all retained live runs", "layers", true) +
      stat("Monitored torrents", data ? number(data.torrents) : "—", "Distinct hashes in the serving snapshot", "eye") +
      stat("Committed batches", data ? number(data.batches) : "—", "Serving-layer transaction ledger", "grid") +
      stat("Last observation", data?.last_observed ? new Date(data.last_observed).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "—", data?.last_observed ? date(data.last_observed) + (data.fresh ? " · fresh" : " · stale") : "No committed data yet", "bell");
    const throughput = document.querySelector("#live-throughput");
    throughput.className = data?.minutes.length ? "live-bars" : "empty-state";
    throughput.innerHTML = data?.minutes.length ? data.minutes.map(row => {
      const max = Math.max(...data.minutes.map(item => item.rows), 1);
      return `<div><span>${esc(new Date(row.minute).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }))}</span><div class="bar-track"><div class="bar-fill" style="width:${row.rows / max * 100}%"></div></div><strong>${number(row.rows)}</strong></div>`;
    }).join("") : "No recently committed observations. Startup or unavailable trackers may delay data.";
    const top = document.querySelector("#live-top");
    top.className = data?.top.length ? "table-wrap" : "empty-state";
    top.innerHTML = data?.top.length ? `<table><thead><tr><th>Torrent record</th><th class="numeric">Max seeders</th><th>Last seen</th></tr></thead><tbody>${data.top.map(row => `<tr><td>${esc(row.title)}<span class="title-meta">${esc(categoryName[row.category] || "Unclassified")}</span></td><td class="numeric">${number(row.seeders)}</td><td>${esc(date(row.scraped_at))}</td></tr>`).join("")}</tbody></table>` : "No retained observations yet.";
  } catch (error) {
    if (!context.signal.aborted) {
      document.querySelector("#live-errors").innerHTML = `<div class="notice error" role="alert">${esc(error.message)}</div>`;
      document.querySelector("#live-phase").textContent = "Status unavailable";
      document.querySelector("#live-phase").classList.remove("live-fresh");
      document.querySelector("#live-message").textContent = "Status could not be refreshed. Displayed metrics are from the last successful refresh.";
      document.querySelector("#live-start").disabled = true;
      document.querySelector("#live-stop").disabled = true;
    }
  } finally {
    context.refreshing = false;
    if (!context.signal.aborted) context.timer = setTimeout(() => refreshLive(context), 5000);
  }
}

async function liveAction(action) {
  const context = liveViewState.context;
  if (!context || context.signal.aborted || liveViewState.actionBusy) return;
  liveViewState.actionBusy = true;
  liveViewState.actionError = null;
  document.querySelector("#live-start").disabled = true;
  document.querySelector("#live-stop").disabled = true;
  liveViewState.targets = Number(document.querySelector('#live-start-form select').value);
  try {
    await liveApi(`/${action}`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      ...(action === "start" ? { body: JSON.stringify({ targets: liveViewState.targets }) } : {}),
    }, context.signal);
    if (!context.signal.aborted) {
      notify(action === "start" ? "Startup requested. Watch the pipeline status for readiness." : "Stop requested. Retained data will remain available.");
    }
  } catch (error) {
    if (!context.signal.aborted) {
      liveViewState.actionError = error.message;
      document.querySelector("#live-errors").innerHTML = `<div class="notice error" role="alert">${esc(error.message)}</div>`;
    }
  } finally {
    liveViewState.actionBusy = false;
    if (!context.signal.aborted) refreshLive(context);
  }
}

function renderLiveQuery(result) {
  document.querySelector("#live-query-results").innerHTML =
    `<div class="panel-head mt"><p>${number(result.rows.length)} rows · ${result.seconds}s · queried ${esc(date(result.queried_at))}${result.truncated ? '<span class="badge">Limited to 200 rows</span>' : ""}</p></div>` +
    (result.rows.length ? `<div class="table-wrap live-results"><table><thead><tr>${result.columns.map(column => `<th>${esc(column)}</th>`).join("")}</tr></thead><tbody>${result.rows.map(row => `<tr>${row.map(value => `<td>${esc(liveValue(value) ?? "—")}</td>`).join("")}</tr>`).join("")}</tbody></table></div>` : '<div class="empty-state">The query returned no rows. Start collection, wait for a committed batch, or broaden the time filter.</div>');
  document.querySelector("#live-export").disabled = !result.rows.length;
}

async function runLiveQuery() {
  const context = liveViewState.context;
  if (!context || context.signal.aborted) return;
  const button = document.querySelector("#live-run-query");
  if (button.disabled) return;
  liveViewState.sql = document.querySelector("#live-sql").value;
  liveViewState.result = null;
  liveViewState.queryBusy = true;
  button.disabled = true;
  document.querySelector("#live-export").disabled = true;
  document.querySelector("#live-query-results").innerHTML = '<div class="loading" role="status"><span class="spinner"></span>Running read-only query…</div>';
  try {
    const result = await liveApi("/query", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sql: liveViewState.sql }),
    }, context.signal);
    if (!context.signal.aborted) {
      liveViewState.result = result;
      renderLiveQuery(result);
    }
  } catch (error) {
    if (!context.signal.aborted) document.querySelector("#live-query-results").innerHTML = `<div class="notice error mt" role="alert">${esc(error.message)}</div>`;
  } finally {
    liveViewState.queryBusy = false;
    if (!context.signal.aborted) button.disabled = !editorRole();
  }
}

document.addEventListener("submit", event => {
  if (event.target.id === "live-start-form") { event.preventDefault(); liveAction("start"); }
  if (event.target.id === "live-query-form") { event.preventDefault(); runLiveQuery(); }
});
document.addEventListener("click", event => {
  const button = event.target.closest("button");
  if (!button || button.disabled) return;
  if (button.dataset.livePreset) {
    liveViewState.sql = livePresets[button.dataset.livePreset].sql;
    document.querySelector("#live-sql").value = liveViewState.sql;
    document.querySelector("#live-sql").focus();
  }
  if (button.dataset.action === "live-stop") liveAction("stop");
  if (button.dataset.action === "live-export" && liveViewState.result) {
    const result = liveViewState.result;
    const lines = [
      [...result.columns, "queried_at", "data_source"].map(csvCell).join(","),
      ...result.rows.map(row => [...row, result.queried_at, result.source].map(value => csvCell(liveValue(value))).join(",")),
    ];
    download("exposureiq-live-query.csv", "\uFEFF" + lines.join("\r\n"), "text/csv;charset=utf-8");
    notify(`Exported ${result.rows.length} query result rows.`);
  }
});
