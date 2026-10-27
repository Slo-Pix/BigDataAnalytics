"use strict";

const companySession = { actor: null, route: "", search: [], result: null };
async function companyApi(path, options = {}, signal) {
  const response = await fetch(path, {
    ...options, signal,
    headers: { "Content-Type": "application/json", "X-CWatch-Request": "1", ...options.headers },
  });
  const body = response.headers.get("content-type")?.includes("json") ? await response.json() : null;
  if (!response.ok) {
    const error = new Error(typeof body?.detail === "string" ? body.detail : body?.detail ? JSON.stringify(body.detail) : `Request failed (${response.status}).`);
    error.status = response.status;
    throw error;
  }
  return body;
}
const companyPost = (path, data, method = "POST") => companyApi(path, { method, body: JSON.stringify(data) });
const editorRole = () => ["admin", "analyst"].includes(companySession.actor?.role);
const adminRole = () => companySession.actor?.role === "admin";
const companyMessage = (message, bad = false) => {
  const element = document.querySelector("#company-message");
  if (element) { element.className = `notice ${bad ? "error" : ""}`; element.textContent = message; element.hidden = false; }
  else notify(message);
};
const companyNotice = () => '<div id="company-message" hidden role="status"></div>';
const valueInput = (name, label, type = "text", required = true, extra = "") => `<label class="field">${label}<input name="${name}" type="${type}" ${required ? "required" : ""} ${extra}></label>`;
const companyTable = (headers, rows) => rows.length ? `<div class="table-wrap"><table><thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join("")}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join("")}</tr>`).join("")}</tbody></table></div>` : '<div class="empty-state">No records yet. Start with the actions above.</div>';

async function requireCompany(signal) {
  try {
    companySession.actor = await companyApi("/api/auth/me", {}, signal);
    loadScopedWatchlist(companySession.actor);
    return true;
  } catch (error) {
    if (signal.aborted) return false;
    if (error.status === 401) {
      companySession.actor = null;
      location.hash = "#/login";
    } else root.innerHTML = publicShell(`<section class="section"><h1>Workspace unavailable</h1><p>${esc(error.message)}</p><a class="button mt" href="#/login">Sign in</a></section>`);
    return false;
  }
}

function accountPage(mode) {
  document.title = `ExposureIQ — ${mode === "login" ? "Sign in" : "Create company account"}`;
  const join = mode === "join", register = mode === "register" || join;
  const token = new URLSearchParams(location.hash.split("?")[1] || "").get("token") || "";
  root.innerHTML = publicShell(`<section class="section auth-panel"><div>${eyebrow("Your company. Your workspace.")}<h1>${join ? "Join your team." : register ? "Create your workspace." : "Welcome back."}</h1></div><div>${companyNotice()}<form id="account-form" data-mode="${mode}">
    ${register && !join ? valueInput("company", "Company name", "text", true, 'maxlength="150" autocomplete="organization"') : ""}
    ${register ? valueInput("name", "Your name", "text", true, 'minlength="2" maxlength="100" autocomplete="name"') : ""}
    ${valueInput("email", "Email", "email", true, 'maxlength="200" autocomplete="email"')}
    ${valueInput("password", "Password", "password", true, `minlength="${register ? 12 : 1}" maxlength="256" autocomplete="${register ? "new-password" : "current-password"}"`)}
    ${join ? `<label class="field">Invitation token<input name="invite" value="${esc(token)}" required maxlength="100" autocomplete="off"></label>` : ""}
    <button class="button mt" type="submit">${join ? "Join company" : register ? "Create company account" : "Sign in"} ${icon("arrow")}</button>
  </form><p class="fineprint">${register ? '<a href="#/login">Already have an account? Sign in →</a>' : '<a href="#/register">Create a company workspace →</a>'}</p></div></section>`);
}

async function companyPage(route, signal) {
  companySession.route = route;
  const names = { portfolio: "Company catalogue", titlecheck: "Title-level live check", cases: "Investigations", alerts: "Alerts & rules", companyreports: "Company reports", team: "Team & API access", audit: "Audit history", health: "System health" };
  document.title = `ExposureIQ — ${names[route]}`;
  root.innerHTML = workShell(heading(names[route]) + companyNotice() + '<div id="company-content">' + loading() + "</div>", route);
  try {
    if (route === "portfolio") await renderPortfolio(signal);
    else if (route === "titlecheck") await renderTitleSearch(signal);
    else if (route === "cases") await renderCases(signal);
    else if (route === "alerts") await renderAlerts(signal);
    else if (route === "companyreports") await renderCompanyReports(signal);
    else if (route === "team") await renderTeam(signal);
    else if (route === "audit") {
      const data = await companyApi("/api/company/audit", {}, signal);
      if (!signal.aborted) document.querySelector("#company-content").innerHTML = `<section class="panel">${companyTable(["When", "Action", "Entity", "Details"], data.rows.map(row => [esc(date(row.created_at)), esc(row.action), esc(row.entity), esc(row.detail)]))}</section>`;
    } else if (route === "health") {
      const data = await companyApi("/api/health/details", {}, signal);
      if (!signal.aborted) document.querySelector("#company-content").innerHTML = `<section class="panel"><h2>${esc(displayLabel(data.status))} · local pilot</h2><dl class="detail-grid"><div><dt>Company database</dt><dd>${number(data.database_bytes)} bytes</dd></div><div><dt>Maintenance</dt><dd>${esc(data.maintenance_error || "No reported errors")}</dd></div><div><dt>Email delivery</dt><dd>${data.email_configured ? "Configured" : "Not configured; alerts remain in-app"}</dd></div><div><dt>Webhook delivery</dt><dd>${data.webhook_configured ? "Configured" : "Not configured"}</dd></div></dl><p class="fineprint">The local Kafka/Spark collector is shared infrastructure controlled by company administrators. Private portfolio, cases, alerts and reports are separated by organisation. Back up the company database using the documented management command.</p><a class="button outline mt" href="/docs" target="_blank" rel="noopener">Open API documentation ↗</a></section>`;
    }
  } catch (error) {
    if (!signal.aborted) document.querySelector("#company-content").innerHTML = errorPanel(error);
  }
}

async function renderPortfolio(signal) {
  const data = await companyApi("/api/company/portfolio", {}, signal);
  if (signal.aborted) return;
  companySession.portfolio = data.rows;
  document.querySelector("#company-content").innerHTML = `<section class="panel"><div class="panel-head"><div><h2>Owned or represented titles</h2><p>Verify matches and your rights before investigating. CSV imports stay pending until approved.</p></div><a class="button outline small" href="#/titlecheck">Find a title ↗</a></div>
    ${editorRole() ? `<form id="portfolio-import" class="live-controls"><label class="field">Import CSV (title, category, optional external_id)<input name="file" type="file" accept=".csv,text/csv" required></label><button class="button" type="submit">Import for review</button></form><button class="button outline small mt" data-company-action="csv-template">Download CSV template</button>` : ""}
    <p class="fineprint">Up to 200 rows per import. Categories: movie, tv, game. An identifier improves matching but does not establish ownership. Latest 200 portfolio records displayed.</p>
    ${companyTable(["Title", "Category", "Match status", "Actions"], data.rows.map(row => {
      const candidates = Array.isArray(row.candidates) ? row.candidates : [];
      return [esc(row.title), esc(row.category), esc(displayLabel(row.status)), editorRole() ?
        `${row.status === "approved" ? `<button class="button small outline" data-portfolio-check="${row.id}">Check seeders</button> <button class="button small outline" data-portfolio-case="${row.id}">Investigate</button>` :
          `<form class="approve-form live-controls" data-id="${row.id}"><label class="field">Candidate match<select name="key" required><option value="">Select a verified match</option>${candidates.map(candidate => `<option value="${esc(candidate.key)}">${esc(candidate.title)} (${esc(candidate.category)})</option>`).join("")}</select></label><button class="button small" type="submit">Approve</button></form><a class="button small outline" href="#/titlecheck">Search for a better match</a>`}
          <button class="button small outline" data-portfolio-remove="${row.id}">Remove</button>` : "Read-only"];
    }))}</section>`;
}

async function renderTitleSearch(signal) {
  document.querySelector("#company-content").innerHTML = `<section class="panel"><h2>What would you like to check?</h2><p class="fineprint">Search a movie, TV series or game, or use an IMDb/Steam identifier. Only catalogue-matched hashes can be checked; new releases may not be represented.</p><form id="title-search-form" class="filters"><label class="field search">Title or identifier<input name="q" minlength="2" maxlength="150" required placeholder="Interstellar, The Sopranos, or a Steam ID"></label><label class="field">Content type<select name="category"><option value="all">All content</option><option value="movie">Movies</option><option value="tv">Television</option><option value="game">Games</option></select></label><button class="button" type="submit">Find matches</button></form><div id="title-search-results"></div></section><div id="title-check-results"></div>`;
  const history = document.createElement("section");
  history.className = "panel";
  history.innerHTML = "<h2>Saved company checks</h2>" + loading();
  document.querySelector("#company-content").append(history);
  const data = await companyApi("/api/company/checks", {}, signal);
  if (signal.aborted) return;
  companySession.checks = data.rows;
  history.innerHTML = `<h2>Saved company checks</h2><p class="fineprint">Previously requested checks for approved company titles. Counts describe the recorded time, not necessarily current availability. Download the complete observation, coverage and error record.</p>${companyTable(["Title", "Checked", "Largest reported swarm", "Coverage", "Record"], data.rows.map(row => [esc(row.result.title), esc(date(row.checked_at)), row.max_seeders === null ? "Unavailable" : number(row.max_seeders), `${row.result.responding_trackers}/${row.result.expected_trackers} trackers · ${row.result.sampled_hashes}/${row.result.matched_hashes} hashes`, `<button class="button small outline" data-check-download="${row.id}">Download JSON</button>`]))}`;
}

async function runTitleCheck(body) {
  companyMessage("Checking public trackers. Partial failures will be shown, not counted as zero seeders.");
  try {
    const result = await companyPost("/api/titles/check", body);
    companySession.result = result;
    const target = document.querySelector("#title-check-results");
    const html = `<section class="panel"><div class="panel-head"><div><h2>${esc(result.title)}</h2><p>Checked ${esc(date(result.checked_at))}</p></div><button class="button outline small" data-company-action="export-check">${icon("download")}Export observations</button></div><div class="stats">${stat("Largest observed swarm", result.max_seeders === null ? "Unavailable" : number(result.max_seeders), "Max seeders reported for one hash on one tracker", "eye", true)}${stat("Responding trackers", `${result.responding_trackers}/${result.expected_trackers}`, "Failures are not zero counts", "layers")}${stat("Sampled hashes", number(result.sampled_hashes), `Of ${number(result.matched_hashes)} known matches`, "grid")}${stat("Observation mode", result.fresh ? "Fresh" : "Unavailable", "On-demand public UDP tracker queries", "bell")}</div><div class="notice">${esc(result.notice)}</div>${result.errors.map(error => `<div class="notice error">${esc(error.tracker)}: ${esc(error.message)}</div>`).join("")}${companyTable(["Torrent hash", "Tracker", "Seeders", "Leechers", "Observed"], result.observations.map(row => [esc(row.infohash), esc(row.tracker), number(row.seeders), number(row.leechers), esc(date(row.checked_at))]))}<p class="fineprint">Use these observations to prioritise review. Counts overlap across trackers and releases and do not establish unique downloads, infringement, or revenue loss. TV results may include several seasons/episodes; verify each record.</p></section>`;
    if (target) { target.innerHTML = html; target.scrollIntoView({ behavior: "smooth", block: "start" }); }
    else {
      document.querySelector("#detail-content").innerHTML = `<button class="icon-button" data-action="close-detail" aria-label="Close">${icon("close")}</button>${html}`;
      document.querySelector("#detail").showModal();
    }
    companyMessage(result.fresh ? "Tracker observations received. Portfolio checks are saved and evaluated against your rules." : "No tracker responded successfully. This is not evidence that no seeders exist.", !result.fresh);
  } catch (error) { companyMessage(error.message, true); }
}

async function renderCases(signal) {
  const [cases, portfolio, members] = await Promise.all([companyApi("/api/company/cases", {}, signal), companyApi("/api/company/portfolio", {}, signal), companyApi("/api/auth/members", {}, signal)]);
  if (signal.aborted) return;
  document.querySelector("#company-content").innerHTML = `<section class="panel"><h2>Evidence-led review</h2><p class="fineprint">Assign work, document matching and rights checks, then record authorised actions and outcomes. A case is not an infringement determination.</p>${editorRole() ? `<form id="case-create" class="form-grid">
    <label class="field">Approved title<select name="portfolio_id" required>${portfolio.rows.filter(row => row.status === "approved").map(row => `<option value="${row.id}" ${String(row.id) === sessionStorage.getItem("cwatch-case-title") ? "selected" : ""}>${esc(row.title)}</option>`).join("")}</select></label>
    ${valueInput("title", "Investigation title", "text", true, 'maxlength="150"')}
    <label class="field">Assignee<select name="assignee_id"><option value="">Unassigned</option>${members.rows.filter(row => row.role !== "viewer").map(row => `<option value="${row.id}">${esc(row.name)}</option>`).join("")}</select></label>
    <label class="field wide">Description<textarea name="description" maxlength="4000" required placeholder="What needs to be verified?"></textarea></label><button class="button" type="submit">Create investigation</button>
    </form>` : ""}</section><section class="panel">${companyTable(["Investigation", "Status", "Assignee", "Action"], cases.rows.map(row => [esc(row.title), esc(displayLabel(row.status)), esc(row.assignee_name || row.assignee_id || "Unassigned"), `<button class="button outline small" data-case-open="${row.id}">Open case</button>`]))}</section><div id="case-detail"></div>`;
}

async function openCase(id) {
  try {
    const [item, members] = await Promise.all([companyApi(`/api/company/cases/${id}`), companyApi("/api/auth/members")]);
    const record = item.case || item;
    document.querySelector("#case-detail").innerHTML = `<section class="panel"><h2>${esc(record.title)}</h2><p class="fineprint">${esc(record.description)}</p><div class="notice">Verify title matches, ownership, territory and source coverage before referring this case to an authorised enforcement provider. ExposureIQ does not send takedown notices.</div>${editorRole() ? `<form id="case-update" data-id="${id}" class="live-controls"><label class="field">Status<select name="status">${["open", "investigating", "actioned", "closed"].map(status => `<option value="${status}" ${record.status === status ? "selected" : ""}>${displayLabel(status)}</option>`).join("")}</select></label><label class="field">Assignee<select name="assignee_id"><option value="">Unassigned</option>${members.rows.filter(row => row.role !== "viewer").map(row => `<option value="${row.id}" ${row.id === record.assignee_id ? "selected" : ""}>${esc(row.name)}</option>`).join("")}</select></label><button class="button" type="submit">Update case</button></form><form id="case-note" data-id="${id}" class="mt"><label class="field">Review note<textarea name="body" required maxlength="4000" placeholder="Document your evidence, decision, action or follow-up."></textarea></label><button class="button outline mt" type="submit">Add note</button></form>` : ""}<h3 class="mt">Activity notes</h3>${(item.notes || record.notes || []).map(note => `<div class="step"><div><p>${esc(note.body)}</p><small>${esc(note.author_name || note.user_id || "")} · ${esc(date(note.created_at))}</small></div></div>`).join("") || '<p class="fineprint">No notes yet.</p>'}</section>`;
    document.querySelector("#case-detail").scrollIntoView({ behavior: "smooth" });
    if (item.check_summary) {
      const check = item.check_summary;
      const evidence = document.createElement("div");
      evidence.className = "notice mt";
      evidence.textContent = `Latest saved check: ${date(check.checked_at)}. Maximum reported seeders: ${check.max_seeders === null ? "Unavailable" : number(check.max_seeders)}. Tracker coverage: ${check.result.responding_trackers}/${check.result.expected_trackers}. Counts apply to that timestamp. Full observation records are available in Check live seeders → Saved company checks.`;
      document.querySelector("#case-detail .panel").append(evidence);
    }
  } catch (error) { companyMessage(error.message, true); }
}

async function renderAlerts(signal) {
  const [alerts, rules, portfolio] = await Promise.all([companyApi("/api/company/alerts", {}, signal), companyApi("/api/company/rules", {}, signal), companyApi("/api/company/portfolio", {}, signal)]);
  if (signal.aborted) return;
  document.querySelector("#company-content").innerHTML = `<section class="panel"><h2>Explainable alert rules</h2><p class="fineprint">Rules evaluate saved portfolio checks only when a tracker actually responds. Trigger metric: maximum reported seeders for a single hash/tracker. One unresolved alert per rule/title prevents duplicates. Optional delivery runs once per minute when configured; failures remain visible.</p>${editorRole() ? `<form id="rule-create" class="form-grid">${valueInput("name", "Rule name", "text", true, 'maxlength="100"')}<label class="field">Title scope<select name="portfolio_id"><option value="">All approved company titles</option>${portfolio.rows.filter(row => row.status === "approved").map(row => `<option value="${row.id}">${esc(row.title)}</option>`).join("")}</select></label>${valueInput("threshold", "Trigger at or above this seeder count", "number", true, 'min="0" max="100000000" value="10"')}<button class="button" type="submit">Create rule</button></form>` : ""}
    ${companyTable(["Rule", "Threshold", "Enabled", "Action"], rules.rows.map(row => [esc(row.name), number(row.threshold), row.enabled ? "Yes" : "No", editorRole() ? `<button class="button small outline" data-rule-toggle="${row.id}" data-enabled="${row.enabled ? "false" : "true"}">${row.enabled ? "Disable" : "Enable"}</button>` : "Read-only"]))}</section>
    <section class="panel"><h2>Company alerts</h2>${companyTable(["Alert", "Status", "Delivery", "Action"], alerts.rows.map(row => [esc(row.message || row.title), esc(displayLabel(row.status)), `${esc(displayLabel(row.delivery_status || "In-app"))}${(row.deliveries || []).map(delivery => `<p class="fineprint">${esc(displayLabel(delivery.channel))}: ${esc(displayLabel(delivery.status))} ${esc(delivery.error || "")}</p>`).join("")}${editorRole() && row.deliveries?.some(delivery => ["failed", "unconfigured"].includes(delivery.status)) ? `<button class="button small outline" data-alert-retry="${row.id}">Retry delivery</button>` : ""}`, editorRole() ? `<form class="alert-update live-controls" data-id="${row.id}"><select name="status" aria-label="Alert status">${["new", "reviewed", "resolved"].map(status => `<option value="${status}" ${row.status === status ? "selected" : ""}>${displayLabel(status)}</option>`).join("")}</select><button class="button small outline">Save</button></form>` : "Read-only"]))}</section>`;
}

async function renderCompanyReports(signal) {
  const [summary, schedules] = await Promise.all([companyApi("/api/company/reports/summary", {}, signal), companyApi("/api/company/reports", {}, signal)]);
  if (signal.aborted) return;
  companySession.summary = summary;
  document.querySelector("#company-content").innerHTML = `<section class="panel"><div class="panel-head"><div><h2>Company review summary</h2><p>Company records and saved tracker checks</p></div><button class="button outline" data-company-action="print-report">Print / Save PDF</button></div><div id="printable-company-report"><h2>ExposureIQ · Review summary</h2><h3>${esc(companySession.actor.company)}</h3><p class="fineprint">Generated ${esc(date(summary.generated_at))} · Source: company workflow database and saved title checks.</p><div class="report-summary">${Object.entries(summary).filter(([key]) => !["generated_at", "limitations", "org_id", "counts"].includes(key)).map(([key, value]) => `<div class="settings-row"><strong>${esc(key.replaceAll("_", " "))}</strong><span>${value && typeof value === "object" ? Object.entries(value).map(([label, count]) => `${esc(displayLabel(label))}: ${esc(count)}`).join("<br>") : esc(value)}</span></div>`).join("")}</div><p class="fineprint">${esc(Array.isArray(summary.limitations) ? summary.limitations.join(" ") : summary.limitations || "Observations are not unique people, proof of infringement, or estimated losses. Validate rights and matches before acting.")}</p></div><div class="live-query-actions"><button class="button outline" data-company-action="portfolio-export">Export company CSV</button><button class="button outline" data-company-action="summary-export">Export summary JSON</button></div></section>
    <section class="panel"><h2>Scheduled report snapshots</h2><p class="fineprint">A local background worker creates due daily/weekly summaries while the server is running. Snapshots are retained; they are not automatically emailed. Download the latest snapshot below.</p>${editorRole() ? `<form id="report-schedule" class="live-controls">${valueInput("name", "Schedule name", "text", true, 'maxlength="100"')}<label class="field">Frequency<select name="frequency"><option value="daily">Daily</option><option value="weekly">Weekly</option></select></label><button class="button" type="submit">Schedule report</button></form>` : ""}${companyTable(["Report", "Frequency", "Snapshot"], schedules.rows.map(row => [esc(row.name), esc(displayLabel(row.frequency)), `<button class="button outline small" data-report-download="${row.id}">Download latest snapshot</button>`]))}</section>`;
}

async function renderTeam(signal) {
  const [members, keys] = await Promise.all([companyApi("/api/auth/members", {}, signal), companyApi("/api/auth/keys", {}, signal)]);
  if (signal.aborted) return;
  document.querySelector("#company-content").innerHTML = `<section class="panel"><h2>Company members</h2>${companyTable(["Name", "Email", "Role"], members.rows.map(row => [esc(row.name), esc(row.email), `<form class="member-role live-controls" data-id="${row.id}"><select name="role" aria-label="Member role">${["admin", "analyst", "viewer"].map(role => `<option value="${role}" ${row.role === role ? "selected" : ""}>${displayLabel(role)}</option>`).join("")}</select><button class="button outline small">Update role</button></form>`]))}
    <form id="team-invite" class="live-controls mt"><label class="field">Invite role<select name="role"><option value="analyst">Analyst</option><option value="viewer">Viewer</option><option value="admin">Admin</option></select></label><button class="button">Create 24-hour invitation</button></form><div id="invitation-result"></div></section>
    <section class="panel"><h2>Read-only integration API</h2><p class="fineprint">Keys inherit this company’s read-only scope, are hashed in storage, and allow up to 120 requests/minute. Send Authorization: Bearer TOKEN. Mutating requests are not allowed with a read-only key. API documentation: <a href="/docs" target="_blank" rel="noopener">/docs ↗</a></p><form id="api-key-create" class="live-controls">${valueInput("name", "Integration name", "text", true, 'minlength="2" maxlength="80"')}<button class="button">Create API key</button></form><div id="api-key-result"></div>${companyTable(["Name", "Created", "Status", "Action"], keys.rows.map(row => [esc(row.name), esc(date(row.created_at)), row.revoked ? "Revoked" : "Active", row.revoked ? "—" : `<button class="button outline small" data-key-revoke="${row.id}">Revoke</button>`]))}</section>`;
}

document.addEventListener("submit", async event => {
  const form = event.target;
  const handled = ["account-form", "portfolio-import", "title-search-form", "case-create", "case-update", "case-note", "rule-create", "report-schedule", "team-invite", "api-key-create"].includes(form.id) || form.matches(".approve-form,.member-role,.alert-update");
  if (!handled) return;
  event.preventDefault();
  const button = form.querySelector('button[type="submit"],button:not([type])');
  if (button?.disabled) return;
  if (button) button.disabled = true;
  const data = Object.fromEntries(new FormData(form));
  try {
    if (form.id === "account-form") {
      await companyPost(form.dataset.mode === "login" ? "/api/auth/login" : "/api/auth/register", data);
      location.hash = "#/portfolio";
    } else if (form.id === "portfolio-import") {
      const file = form.querySelector('input[type="file"]').files[0];
      if (!file || file.size > 1_000_000) throw new Error("Choose a CSV file no larger than 1 MB.");
      await companyPost("/api/company/portfolio/import", { csv_text: await file.text() });
      await render(); companyMessage("Catalogue imported for match review.");
    } else if (form.matches(".approve-form")) {
      await companyPost(`/api/company/portfolio/${form.dataset.id}/approve`, { key: data.key }); await render();
    } else if (form.id === "title-search-form") {
      const result = await companyApi("/api/titles?" + new URLSearchParams(data));
      companySession.search = result.rows;
      document.querySelector("#title-search-results").innerHTML = `<p class="fineprint">${esc(result.notice)}</p>${companyTable(["Matched title", "Category", "Known hashes", "Action"], result.rows.map((row, i) => [esc(row.title), esc(row.category), number(row.matched_hashes), editorRole() ? `<button type="button" class="button outline small" data-title-check="${i}">Check now</button> <button type="button" class="button small" data-title-add="${i}">Add to catalogue</button>` : "Read-only"]))}`;
    } else if (form.id === "case-create") {
      await companyPost("/api/company/cases", { ...data, portfolio_id: Number(data.portfolio_id), assignee_id: data.assignee_id ? Number(data.assignee_id) : null }); await render();
    } else if (form.id === "case-update") {
      await companyPost(`/api/company/cases/${form.dataset.id}`, { ...data, assignee_id: data.assignee_id ? Number(data.assignee_id) : null }, "PATCH"); await render(); if (document.querySelector("#case-detail")) await openCase(form.dataset.id);
    } else if (form.id === "case-note") {
      await companyPost(`/api/company/cases/${form.dataset.id}/notes`, data); await openCase(form.dataset.id);
    } else if (form.id === "rule-create") {
      await companyPost("/api/company/rules", { ...data, threshold: Number(data.threshold), portfolio_id: data.portfolio_id ? Number(data.portfolio_id) : null }); await render();
    } else if (form.matches(".alert-update")) {
      await companyPost(`/api/company/alerts/${form.dataset.id}`, data, "PATCH"); await render();
    } else if (form.id === "report-schedule") {
      await companyPost("/api/company/reports", data); await render();
    } else if (form.id === "team-invite") {
      const result = await companyPost("/api/auth/invitations", data);
      document.querySelector("#invitation-result").innerHTML = `<label class="field mt">Share this single-use invitation privately. Expires in 24 hours.<input readonly value="${esc(location.origin + '/#/join?token=' + encodeURIComponent(result.invite))}"></label>`;
    } else if (form.matches(".member-role")) {
      await companyPost(`/api/auth/members/${form.dataset.id}`, data, "PATCH"); await render();
    } else if (form.id === "api-key-create") {
      const result = await companyPost("/api/auth/keys", data);
      document.querySelector("#api-key-result").innerHTML = `<label class="field mt">Copy now; the full read-only key is shown only once.<input readonly value="${esc(result.token)}" autocomplete="off"></label>`;
    }
  } catch (error) { companyMessage(error.message, true); }
  finally { if (button?.isConnected) button.disabled = false; }
});
document.addEventListener("click", async event => {
  const button = event.target.closest("button");
  if (!button || button.disabled) return;
  const ds = button.dataset;
  try {
    if (ds.titleCheck !== undefined) { button.disabled = true; await runTitleCheck({ key: companySession.search[Number(ds.titleCheck)].key }); }
    if (ds.titleAdd !== undefined) { await companyPost("/api/company/portfolio", { key: companySession.search[Number(ds.titleAdd)].key }); companyMessage("Added an approved match to your company catalogue. Verify that your company owns or represents the title."); }
    if (ds.portfolioCheck) { button.disabled = true; await runTitleCheck({ portfolio_id: Number(ds.portfolioCheck) }); }
    if (ds.portfolioCase) { sessionStorage.setItem("cwatch-case-title", ds.portfolioCase); location.hash = "#/cases"; }
    if (ds.portfolioRemove && confirm("Remove this company catalogue record? Linked investigations may prevent removal.")) { await companyApi(`/api/company/portfolio/${ds.portfolioRemove}`, { method: "DELETE" }); await render(); }
    if (ds.caseOpen) await openCase(ds.caseOpen);
    if (ds.checkDownload) {
      const check = companySession.checks.find(row => String(row.id) === ds.checkDownload);
      download(`exposureiq-check-${check.id}.json`, JSON.stringify(check.result, null, 2), "application/json");
    }
    if (ds.ruleToggle) { await companyPost(`/api/company/rules/${ds.ruleToggle}`, { enabled: ds.enabled === "true" }, "PATCH"); await render(); }
    if (ds.keyRevoke && confirm("Revoke this API key immediately?")) { await companyApi(`/api/auth/keys/${ds.keyRevoke}`, { method: "DELETE" }); await render(); }
    if (ds.reportDownload) { const result = await companyApi(`/api/company/reports/${ds.reportDownload}`); if (!result.summary) throw new Error("No saved snapshot yet. The local scheduler checks due reports once per minute."); download("exposureiq-scheduled-report.json", JSON.stringify(result, null, 2), "application/json"); }
    if (ds.companyAction === "logout") { await companyPost("/api/auth/logout", {}); companySession.actor = null; companySession.summary = null; companySession.result = null; companySession.search = []; companySession.portfolio = []; companySession.checks = []; state.saved = []; watchlistKey = ""; sessionStorage.removeItem("cwatch-case-title"); location.hash = "#/"; }
    if (ds.companyAction === "csv-template") download("exposureiq-catalogue-template.csv", "title,category,external_id\nInterstellar,movie,tt0816692\nThe Sopranos,tv,\nPortal 2,game,\n", "text/csv");
    if (ds.companyAction === "export-check") {
      const result = companySession.result;
      const columns = ["title", "title_key", "category", "infohash", "tracker", "seeders", "leechers", "checked_at", "sampled_hashes", "matched_hashes", "responding_trackers", "expected_trackers", "data_source", "methodology"];
      const rows = result.observations.map(row => ({ ...result, ...row, title_key: result.key, data_source: "on-demand UDP trackers", methodology: "Reported counts overlap; not unique people, downloads, verified infringement or losses." }));
      download("exposureiq-title-observations.csv", "\uFEFF" + [columns.map(csvCell).join(","), ...rows.map(row => columns.map(key => csvCell(row[key])).join(","))].join("\r\n"), "text/csv;charset=utf-8");
    }
    if (ds.companyAction === "summary-export") download("exposureiq-company-summary.json", JSON.stringify(companySession.summary, null, 2), "application/json");
    if (ds.companyAction === "portfolio-export") {
      const response = await fetch("/api/company/reports/portfolio.csv");
      if (!response.ok) throw new Error("Company export failed.");
      download("exposureiq-company-portfolio.csv", await response.text(), "text/csv;charset=utf-8");
    }
    if (ds.companyAction === "print-report") { document.body.classList.add("print-company-report"); window.print(); }
    if (ds.alertRetry) { await companyPost(`/api/company/alerts/${ds.alertRetry}/retry-delivery`, {}); await render(); }
  } catch (error) { companyMessage(error.message, true); }
  finally { if (button.isConnected) button.disabled = false; }
});
window.addEventListener("afterprint", () => document.body.classList.remove("print-company-report"));
