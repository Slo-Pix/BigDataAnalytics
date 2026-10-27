/* ExposureIQ: an offline-capable interface over the local research catalogue. */
"use strict";
const root = document.querySelector("#app");
const icons = {
  arrow: '<path d="M5 12h14m-6-6 6 6-6 6"/>',
  grid: '<rect x="3" y="3" width="7" height="7" rx="2"/><rect x="14" y="3" width="7" height="7" rx="2"/><rect x="3" y="14" width="7" height="7" rx="2"/><rect x="14" y="14" width="7" height="7" rx="2"/>',
  search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/>',
  eye: '<path d="M3 12h4l3-7 4 14 3-7h4"/>',
  film: '<rect x="3" y="3" width="18" height="18" rx="3"/><path d="M7 3v18M17 3v18M3 8h4m-4 8h4m10-8h4m-4 8h4"/>',
  tv: '<rect x="3" y="6" width="18" height="14" rx="3"/><path d="m8 2 4 4 4-4M9 23h6"/>',
  game: '<path d="M7 7h10c3 0 6 11 4 13s-5-3-7-3h-4c-2 0-5 5-7 3S4 7 7 7Z"/><path d="M6 12h5m-2.5-2.5v5M16 11h.01M18 14h.01"/>',
  bookmark: '<path d="M6 3h12v18l-6-4-6 4Z"/>',
  report: '<path d="M14 3H5v18h14V8ZM14 3v5h5M8 12h8M8 16h6"/>',
  bell: '<path d="M5 16v-6a7 7 0 0 1 14 0v6l2 2H3ZM10 21h4"/>',
  settings: '<path d="M4 7h16M4 17h16"/><circle cx="9" cy="7" r="3"/><circle cx="15" cy="17" r="3"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/>',
  download: '<path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  check: '<path d="m5 12 4 4L19 6"/>',
  layers: '<path d="m12 3 10 5-10 5L2 8Zm-10 9 10 5 10-5M2 16l10 5 10-5"/>',
};
const icon = name => `<svg viewBox="0 0 24 24" aria-hidden="true">${icons[name] || icons.eye}</svg>`;
const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const displayLabel = value => String(value ?? "").replaceAll("_", " ").replace(/\b[a-z]/g, letter => letter.toUpperCase());
const number = value => value == null ? "—" : new Intl.NumberFormat("en").format(value);
const short = value => new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
const categoryName = { movie: "Movie", tv: "TV series", game: "Game" };
const date = value => value ? new Date(value).toLocaleString([], { dateStyle: "medium", timeStyle: "short" }) : "Not available";
const brand = () => '<a class="brand" href="#/" aria-label="ExposureIQ"><span class="brand-name">Exposure<span class="brand-iq">IQ</span></span></a>';
const btn = (label, href, kind = "", arrow = false) => `<a class="button ${kind}" href="${href}">${label}${arrow ? `<span class="arrow">↗</span>` : ""}</a>`;
const state = { rows: [], total: 0, offset: 0, filters: { q: "", category: "all", year: "", min_seeders: "", active: false }, controller: null, route: "", saved: [], storageError: "" };
let toastTimer;
let watchlistKey = "";
function loadScopedWatchlist(actor) {
  const key = `cwatch.watchlist.${actor.org_id}.${actor.user_id}`;
  if (watchlistKey === key) return;
  watchlistKey = key;
  state.saved = [];
  state.storageError = "";
  try {
    const saved = JSON.parse(localStorage.getItem(key) || "[]");
    if (!Array.isArray(saved) || !saved.every(row => row && typeof row.infohash === "string" && typeof row.display_title === "string" && ["movie", "tv", "game"].includes(row.category))) {
      throw new Error("Stored watchlist has an invalid format.");
    }
    state.saved = saved;
  } catch (error) {
    state.storageError = `Browser watchlist could not be loaded: ${error.message}`;
  }
}
function notify(message) {
  const toast = document.querySelector("#toast");
  toast.textContent = message;
  toast.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { toast.hidden = true; }, 4500);
}
function saveWatchlist(next) {
  try {
    localStorage.setItem(watchlistKey, JSON.stringify(next));
    state.saved = next;
    return true;
  } catch (error) {
    notify(`Watchlist was not saved: ${error.message}`);
    return false;
  }
}
function publicShell(content, active = "") {
  if (active) return `<section id="${active}" class="landing-section" aria-label="${active}">${content}</section>`;
  return `<div class="frame"><header class="site-header">${brand()}<div class="header-actions"><a href="#/login">Sign in</a>${btn("Create workspace", "#/register", "", true)}</div></header><main id="main" tabindex="-1">${content}</main><footer class="site-footer">${brand()}<div class="footer-links"><a href="#/platform">Platform</a><a href="#/pricing">Pricing</a><a href="#/contact">Contact</a><a href="#/about">Data & methodology</a></div><p>© ${new Date().getFullYear()} ExposureIQ · Local company pilot</p></footer></div>`;
}
function eyebrow(text) { return `<div class="eyebrow"><span class="dot"></span>${text}</div>`; }
function cta() {
  return `<section class="section cta"><div>${eyebrow("A clearer view starts here")}<h2>Your content. A bigger picture.</h2><p>Put public torrent observations to work for your catalogue.</p></div>${btn("Explore the workspace", "#/workspace", "", true)}</section>`;
}
function home() {
  return publicShell(`<section class="hero"><div class="hero-copy">${eyebrow("Exposure intelligence. In focus.")}<h1>Great content.<br>A clearer view<br>of <span class="soft">its exposure.</span></h1><p>See how your movies, shows, and games appear across monitored torrent activity. Less noise. More perspective.</p><div class="hero-actions">${btn("Explore the platform", "#/workspace", "", true)}${btn("Let’s talk", "#/contact", "outline")}</div><div class="hero-foot"><span>Metadata only</span><span>No content downloads</span><span>No peer IP storage</span></div></div><div class="hero-art"><img src="/contentwatch-art.svg" alt="A sculpted observation lens above layers of silver, navy and blue data"><div class="art-label top">${icon("layers")} One view. Three content worlds.</div><div class="art-label bottom"><span class="dot"></span>Built on a billion-row research foundation</div></div><div class="hero-index">See beyond the surface <span>↘</span><b>01</b></div></section>
  <div class="intro-strip"><span>ONE SHARED PLATFORM. EVERY KIND OF STORY.</span><div class="category-list"><span>${icon("film")} Movies</span><span>${icon("tv")} Television</span><span>${icon("game")} Games</span></div></div>
  <section class="section"><div class="section-head"><div>${eyebrow("From observations to understanding")}<h2>Know where to look.<br>Know what matters.</h2></div><p>A focused workspace for rights holders and publishing teams. Explore observations, build a watchlist, and bring context to every conversation.</p></div><div class="cards">${feature("search", "Find your content", "Search historical torrent records across films, TV, and games. Narrow the view by category, upload year, and observed activity.", "01")}${feature("eye", "Keep it in view", "Save relevant torrent records to a personal watchlist. Open the source details and understand exactly what was observed.", "02", true)}${feature("report", "Share a clearer story", "Export filtered observations and watchlists with their timestamps. Turn catalogue exploration into a reviewable report.", "03")}</div></section>
  <section class="section split"><div>${eyebrow("Big data. Human perspective.")}<h2>The pipeline is complex.<br>Your next step<br>shouldn’t be.</h2><p class="fineprint">A research foundation of 1.38 billion historical records. A purpose-built interface for the questions that matter to your team.</p>${btn("Meet the platform", "#/platform", "outline mt", true)}</div><div>${step("01", "Discover", "Upload your catalogue and approve title matches.")}${step("02", "Investigate", "Check current tracker counts, assign cases, and document review.")}${step("03", "Take it with you", "Export traceable reports for authorised content-protection teams.")}</div></section>${platform()}${pricing()}${about()}${contact()}${cta()}`);
}
function feature(symbol, title, copy, num, green = false) {
  return `<article class="feature-card ${green ? "green" : ""}"><span class="card-num">${num}</span><div class="icon-box">${icon(symbol)}</div><h3>${title}</h3><p>${copy}</p></article>`;
}
function step(num, title, copy) { return `<div class="step"><span>${num}</span><div><h3>${title}</h3><p>${copy}</p></div></div>`; }
function subhero(kicker, title, copy) { return `<section class="subhero">${eyebrow(kicker)}<h2>${title}</h2><p>${copy}</p></section>`; }
function platform() {
  return publicShell(`${subhero("Designed around your catalogue", "One workspace.<br>Every content world.", "Movie distributors, television rights holders, and game publishers share a challenge: making sense of exposure. Start with a clear, traceable view.")}
  <section class="section"><div class="cards">${feature("film", "For film teams", "Explore film listings, upload years, historical tracker observations, and matched IMDb identifiers.", "01")}${feature("tv", "For television teams", "Find series-linked torrent records and review the underlying listing. These records are not a complete episode-level catalogue.", "02", true)}${feature("game", "For game publishers", "Explore matched games, Steam identifiers, and historical activity. Edition and version matches need human review.", "03")}</div><div class="preview"><div class="preview-toolbar"><strong>From catalogue to reviewed action</strong><span class="mini-badge"><span class="dot"></span>Working local pilot</span></div><div class="table-wrap"><table><thead><tr><th>Workflow</th><th>What you can do today</th><th>Available</th></tr></thead><tbody>${[
    ["Company workspace", "Create accounts, invite your team, and assign access roles"],
    ["Private catalogue", "Import CSV, review candidate matches, and approve titles"],
    ["Live title checks", "Request current seeder counts with timestamps and tracker coverage"],
    ["Investigations & alerts", "Assign cases, document decisions, and configure threshold rules"],
    ["Reports & integrations", "Export CSV/JSON/PDF, schedule summaries, and create read-only API keys"],
    ["Research & live SQL", "Filter historical records or query the shared streaming pipeline"],
  ].map(([label, copy]) => `<tr><td>${label}</td><td>${copy}</td><td>${icon("check")}</td></tr>`).join("")}</tbody></table></div></div></section>${cta()}`, "platform");
}
function pricing() {
  const plan = (name, price, sub, description, features, featured = false) => `<article class="pricing-card ${featured ? "featured" : ""}"><span class="eyebrow no-margin">${featured ? "For growing teams" : "Find your fit"}</span><h2>${name}</h2><div class="price">${price} <small>${sub}</small></div><p>${description}</p><ul>${features.map(text => `<li>${text}</li>`).join("")}</ul>${btn(name === "Explorer" ? "Open prototype" : "Discuss a pilot", name === "Explorer" ? "#/workspace" : "#/contact", featured ? "" : "outline", true)}</article>`;
  return publicShell(`${subhero("Simple beginnings. Room to grow.", "A plan for your<br>next perspective.", "Explore the prototype today. Help shape a future pilot around your catalogue and your team.")}
  <section class="section"><div class="cards">${plan("Explorer", "Free", "local pilot", "Try the working company workflows on your machine.", ["Company accounts & roles", "Approved catalogue & live title checks", "Investigations & threshold alerts", "Exports & scheduled summaries"])}${plan("Team pilot", "Let’s talk", "scope-based", "A proposed guided evaluation for a rights-holder team.", ["Define your catalogue & use cases", "Agree matching-quality targets", "Plan source coverage & freshness", "Evaluate reporting workflows"], true)}${plan("Enterprise", "Custom", "future deployment", "Discuss requirements for a commercial deployment.", ["Deployment & integration scoping", "Access-control requirements", "Data licensing assessment", "Support & reliability planning"])}</div></section>
  <section class="section"><div class="section-head"><h2>A few things to know.</h2></div><div class="faq"><details><summary>Can I monitor live tracker activity?</summary><p>Yes. Check a selected known title on demand, or start the shared live pipeline and query committed observations with read-only SQL. The research explorer remains historical. This does not discover new releases, prove infringement, or identify individual downloaders.</p></details><details><summary>Can my company use this commercially today?</summary><p>Not as a production service. This is a local company pilot, and some underlying sources have non-commercial restrictions. Suitable data rights and production safeguards must be established first.</p></details><details><summary>Do I need an account to explore?</summary><p>The public site is open. The workspace requires an account. Company catalogue, cases, alerts and reports are server-stored and organisation-isolated. Personal watchlists are separately scoped by company and user in this browser, not synchronised across devices.</p></details></div></section>`, "pricing");
}
function contact() {
  return publicShell(`${subhero("Let’s bring your catalogue into focus", "Start a conversation.", "Tell us what you publish, what you need to understand, and where a clearer view could help.")}
  <section class="section split"><div class="contact-copy"><h2>Built for a<br>better conversation.</h2><p>A meaningful pilot starts with your real workflow, not a feature checklist.</p><div class="contact-item">01 / Your catalogue<span>Movies, television, games — or a little of everything.</span></div><div class="contact-item">02 / Your questions<span>Coverage, matching, reporting, or analyst effort.</span></div><div class="contact-item">03 / A realistic scope<span>Agree what to measure before promising outcomes.</span></div></div><form id="contact-form"><div class="form-grid"><label class="field">Your name<input required name="name" autocomplete="name" maxlength="100" placeholder="Alex Morgan"></label><label class="field">Work email<input required type="email" name="email" autocomplete="email" maxlength="200" placeholder="alex@company.com"></label><label class="field wide">Company<input required name="company" autocomplete="organization" maxlength="150" placeholder="Your studio or publishing team"></label><label class="field">Your content<select name="content"><option>Movies</option><option>Television</option><option>Games</option><option>Mixed catalogue</option></select></label><label class="field">What brings you here?<select name="interest"><option>Explore a pilot</option><option>Understand the data</option><option>Discuss an integration</option><option>Academic collaboration</option></select></label><label class="field wide">What would you like to understand?<textarea required name="message" maxlength="3000" placeholder="Tell us about your catalogue and monitoring workflow…"></textarea></label><label class="field wide"><span><input style="width:auto;min-height:0" type="checkbox" required> I understand this creates a local file, not a submitted enquiry.</span></label></div><button class="button mt" type="submit">Download enquiry ${icon("download")}</button><p id="contact-status" class="form-message" role="status"></p></form></section>`, "contact");
}
function about() {
  return publicShell(`${subhero("Clarity includes the limitations", "Good intelligence<br>starts with honest data.", "ExposureIQ translates a distributed big-data research project into an explorable product prototype. Here is what the observations do — and do not — mean.")}
  <section class="section split"><div><h2>A billion-row foundation.<br>A carefully scoped view.</h2></div><div>${step("01", "Historical, not universal", "The listing catalogue ends in May 2023; the main tracker collection was on 10 October 2026. A missing record is not proof of no exposure.")}${step("02", "Observations, not people", "Seeders and leechers are tracker-reported counts. They do not establish unique users, downloads, infringement, or lost revenue.")}${step("03", "Traceable, not overclaimed", "Every record includes a hash and observation window. Matches may be imperfect; validate them before making decisions.")}</div></section>
  `, "about");
}
const workLinks = [["portfolio", "film", "Company catalogue"], ["titlecheck", "search", "Check live seeders"], ["cases", "report", "Investigations"], ["alerts", "bell", "Alerts & rules"], ["companyreports", "report", "Company reports"], ["live", "eye", "Live pipeline & SQL"], ["explorer", "layers", "Research catalogue"], ["watchlist", "bookmark", "Personal watchlist"], ["team", "settings", "Team & API"], ["audit", "report", "Audit history"], ["health", "grid", "System health"]];
function workShell(content, active) {
  return `<div class="frame"><div class="workspace">
    <aside class="work-sidebar">${brand()}
      <div class="company"><span class="avatar">IQ</span><div><strong>${esc(companySession.actor?.company || "Company workspace")}</strong><small>${esc(companySession.actor?.name)} · ${esc(displayLabel(companySession.actor?.role))}</small></div></div>
      <div class="nav-label">Intelligence workspace</div>
      <nav class="work-nav" aria-label="Workspace navigation">${workLinks.filter(([path]) => !["team", "audit", "health"].includes(path) || adminRole()).map(([path, symbol, label]) => `<a href="#/${path}" ${active === path ? 'class="active" aria-current="page"' : ""}>${icon(symbol)}${label}</a>`).join("")}</nav>
      <div class="sidebar-bottom"><div class="nav-label">Your workspace</div><nav class="work-nav">
        <a href="#/settings" ${active === "settings" ? 'class="active" aria-current="page"' : ""}>${icon("settings")}Settings & data</a>
        <a href="#/">${icon("arrow")}Back to website</a>
      </nav><button class="button outline small" data-company-action="logout">Sign out</button></div>
    </aside>
    <main id="main" class="work-main" tabindex="-1">
      <header class="work-topbar"><div>Workspace <span>/</span> ${esc(active === "workspace" ? "Overview" : active.charAt(0).toUpperCase() + active.slice(1))}</div>
        <div>${["live", "explorer", "watchlist", "activity", "reports", "workspace"].includes(active) ? `<span class="badge"><span class="dot"></span>${active === "live" ? "Shared live pipeline" : "Historical dataset"}</span>` : ""}<a class="avatar" href="#/settings" aria-label="Workspace settings">IQ</a></div>
      </header>${state.storageError ? `<div class="notice error" role="alert">${esc(state.storageError)}</div>` : ""}${content}
    </main></div></div>`;
}
const heading = (title, subtitle = "", action = "") => `<div class="work-title"><div><h1>${title}</h1>${subtitle ? `<p>${subtitle}</p>` : ""}</div>${action}</div>`;
const historicalNotice = () => `<div class="notice">${icon("info")}Historical observations, not live detection. The main collection is from 10 Oct 2026. Counts represent torrents and tracker-reported peers, not unique titles, people, or lost sales.</div>`;
const stat = (label, value, note, symbol, accent = false) => `<article class="stat ${accent ? "accent" : ""}"><div class="stat-label">${label}${icon(symbol)}</div><strong ${label === "Your watchlist" ? 'data-watch-count' : ""}>${value}</strong><small>${note}</small></article>`;
async function fetchCatalogue(params = {}, signal) {
  const search = new URLSearchParams(Object.entries(params).filter(([, value]) => value !== "" && value !== false));
  const response = await fetch(`/api/contentwatch/catalogue?${search}`, { signal });
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(typeof body?.detail === "string" ? body.detail : `Catalogue request failed (${response.status}). Check the local data service.`);
  }
  return response.json();
}
function errorPanel(error) { return `<div class="notice error" role="alert">${icon("info")}<div>${esc(error.message)}<br><button class="button small outline mt" data-action="retry">Try again</button></div></div>`; }
function loading() { return '<div class="loading" role="status"><span class="spinner"></span>Bringing your catalogue into focus…</div>'; }
function table(rows, removable = false) {
  if (!rows.length) return `<div class="empty-state">${icon("search")}<h2>Nothing in this view yet.</h2><p>Try a broader search or add records from the catalogue.</p></div>`;
  return `<div class="table-wrap"><table><thead><tr><th>Torrent record / matched title</th><th>Category</th><th>Upload year</th><th class="numeric">Median seeders</th><th class="numeric">Median leechers</th><th>${removable ? "Remove" : "Save"}</th></tr></thead><tbody>${rows.map(row => `<tr><td><button class="title-button" data-detail="${esc(row.infohash)}">${esc(row.display_title || "Untitled record")}</button><span class="title-meta">${esc(row.infohash.slice(0, 12))}… · ${row.last_slot ? esc(date(row.last_slot)) : "No timestamp"}</span></td><td><span class="badge ${esc(row.category)}">${esc(categoryName[row.category] || row.category)}</span></td><td>${number(row.added_year)}</td><td class="numeric">${number(row.seeders_median)}</td><td class="numeric">${number(row.leechers_median)}</td><td><button class="icon-button ${state.saved.some(item => item.infohash === row.infohash) ? "saved" : ""}" data-save="${esc(row.infohash)}" aria-label="${removable ? "Remove from" : state.saved.some(item => item.infohash === row.infohash) ? "Remove from" : "Add to"} watchlist: ${esc(row.display_title)}" aria-pressed="${state.saved.some(item => item.infohash === row.infohash)}">${icon(removable ? "close" : "bookmark")}</button></td></tr>`).join("")}</tbody></table></div>`;
}
async function dashboard(signal) {
  root.innerHTML = workShell(heading("A clearer view of your content.", "Your starting point for historical exposure intelligence.", btn("Explore catalogue", "#/explorer", "", true)) + historicalNotice() + '<div id="dashboard-data">' + loading() + "</div>", "workspace");
  try {
    const [all, movies, tv, games] = await Promise.all([fetchCatalogue({ limit: 6 }, signal), ...["movie", "tv", "game"].map(category => fetchCatalogue({ category, limit: 1 }, signal))]);
    if (signal.aborted) return;
    state.rows = all.rows;
    const data = [["movie", movies.total], ["tv", tv.total], ["game", games.total]];
    document.querySelector("#dashboard-data").innerHTML = `<div class="stats">${stat("Observed torrent records", short(all.total), "Across the historical collection", "layers", true)}${stat("Movies & television", short(movies.total + tv.total), "Torrent records, not unique titles", "film")}${stat("Game records", short(games.total), "Matched to the game catalogue", "game")}${stat("Your watchlist", number(state.saved.length), "Saved only in this browser", "bookmark")}</div><div class="dashboard-grid"><section class="panel"><div class="panel-head"><div><h2>One catalogue. Three content worlds.</h2><p>Composition of the historical observation table</p></div>${icon("layers")}</div>${data.map(([category, count]) => `<div class="distribution-row"><span>${icon(category === "movie" ? "film" : category)}${categoryName[category]}</span><strong>${number(count)}</strong></div><div class="bar-track"><div class="bar-fill ${category}" style="width:${all.total ? count / all.total * 100 : 0}%"></div></div>`).join("")}<p class="fineprint">Coverage reflects the available source catalogue, not the whole torrent ecosystem.</p></section><section class="panel"><div class="panel-head"><h2>Make this view yours.</h2>${icon("bookmark")}</div><div class="workspace-cta"><h3>Start with what<br>matters to you.</h3><p>Find relevant titles, check the match, and save torrent records for a focused review.</p>${btn("Build your watchlist", "#/explorer", "lime", true)}</div></section></div><section class="panel"><div class="panel-head"><div><h2>A look inside the catalogue</h2><p>Sample records in dataset order · not a popularity ranking</p></div>${btn("View all", "#/explorer", "outline small")}</div>${table(all.rows)}</section>`;
  } catch (error) { if (!signal.aborted) document.querySelector("#dashboard-data").innerHTML = errorPanel(error); }
}
function filters() {
  const f = state.filters;
  return `<form id="filter-form" class="filters"><label class="field search">Search titles<input type="search" name="q" maxlength="150" value="${esc(f.q)}" placeholder="Search a movie, series, or game…"></label><label class="field">Content type<select name="category">${[["all", "All content"], ["movie", "Movies"], ["tv", "Television"], ["game", "Games"]].map(([value, name]) => `<option value="${value}" ${f.category === value ? "selected" : ""}>${name}</option>`).join("")}</select></label><label class="field">Upload year<input type="number" name="year" min="1900" max="2100" placeholder="Any year" value="${esc(f.year)}"></label><label class="field">Min. median seeders<input type="number" name="min_seeders" min="0" max="100000000" placeholder="Any activity" value="${esc(f.min_seeders)}"></label><label class="check"><input type="checkbox" name="active" ${f.active ? "checked" : ""}>Observed active</label><button class="button small" type="submit">${icon("search")}Apply filters</button><button class="button small light" type="button" data-action="reset">Reset</button></form>`;
}
async function explorer(signal) {
  root.innerHTML = workShell(heading("Catalogue explorer", "Search movies, television, and games. Find the records that matter.", '<button class="button outline small" data-action="export-page" disabled>' + icon("download") + "Export page</button>") + historicalNotice() + `<section class="panel">${filters()}<div id="catalogue-results">${loading()}</div></section>`, "explorer");
  try {
    const result = await fetchCatalogue({ ...state.filters, offset: state.offset }, signal);
    if (signal.aborted) return;
    state.rows = result.rows;
    state.total = result.total;
    document.querySelector("#catalogue-results").innerHTML = `<div class="panel-head"><span class="fineprint no-margin">${number(result.total)} matching torrent records · dataset order</span><span class="badge">25 per page</span></div>${table(result.rows)}<div class="pagination"><span>${result.total ? `${number(state.offset + 1)}–${number(state.offset + result.rows.length)} of ${number(result.total)}` : "0 results"}<br>Browse up to 10,025 records per filter. Narrow your search for more.</span><div><button class="button outline small" data-action="prev" ${state.offset === 0 ? "disabled" : ""}>← Previous</button><button class="button outline small" data-action="next" ${state.offset + 25 >= result.total || state.offset >= 10000 ? "disabled" : ""}>Next →</button></div></div>`;
    document.querySelector('[data-action="export-page"]').disabled = !result.rows.length;
  } catch (error) { if (!signal.aborted) document.querySelector("#catalogue-results").innerHTML = errorPanel(error); }
}
function watchlist() {
  return workShell(heading("Your watchlist", "A focused collection of torrent records, saved on this browser.", btn("Add from catalogue", "#/explorer", "", true)) + '<div class="notice">' + icon("bookmark") + 'Personal browser storage only. This is not a shared company portfolio; clearing browser data removes these records.</div>' + `<section class="panel"><div class="panel-head"><h2>${number(state.saved.length)} saved records</h2><button class="button outline small" data-action="export-watchlist" ${state.saved.length ? "" : "disabled"}>${icon("download")}Export watchlist</button></div>${table(state.saved, true)}</section>`, "watchlist");
}
function review() {
  const active = state.saved.filter(row => row.seeders_median > 0);
  return workShell(heading("Review queue", "A simple historical activity rule applied to your saved records.") + `<div class="notice">${icon("info")}This is not a live alert feed. Rule: saved records with historical median seeders greater than zero. No notifications are sent; no infringement or urgency is inferred.</div><section class="panel"><div class="panel-head"><h2>${number(active.length)} records with observed activity</h2><span class="badge">Browser-local watchlist</span></div>${active.length ? active.map(row => `<div class="alert-row"><span class="avatar">${icon("eye")}</span><div><h3>${esc(row.display_title)}</h3><p>${number(row.seeders_median)} median seeders · last observed ${esc(date(row.last_slot))}</p></div><button class="button outline small" data-detail="${esc(row.infohash)}">Review record ↗</button></div>`).join("") : `<div class="empty-state">${icon("bell")}<h2>Nothing to review yet.</h2><p>Save records with observed activity to see them here.</p>${btn("Explore catalogue", "#/explorer", "outline")}</div>`}</section>`, "activity");
}
function reports() {
  return workShell(heading("Reports & exports", "Take the observations with you. Keep the context attached.") + historicalNotice() + `<div class="cards"><article class="report-card">${icon("bookmark")}<h2>Watchlist export</h2><p>Download all ${number(state.saved.length)} saved records, including identifiers, historical peer counts, and observation timestamps.</p><span class="badge">CSV · browser-local records</span><button class="button outline" data-action="export-watchlist" ${state.saved.length ? "" : "disabled"}>${icon("download")}Download CSV</button></article><article class="report-card">${icon("search")}<h2>Filtered catalogue</h2><p>Choose your filters in the catalogue, then export the displayed page. Exported rows match that page, not the entire result set.</p><span class="badge">CSV · up to 25 records per page</span>${btn("Open catalogue", "#/explorer", "outline", true)}</article><article class="report-card">${icon("report")}<h2>Methodology note</h2><p>Download a plain-text guide to source coverage, metric definitions, historical limitations, and prototype boundaries.</p><span class="badge">TXT · supporting context</span><button class="button outline" data-action="methodology">${icon("download")}Download note</button></article></div><section class="panel mt"><div class="panel-head"><h2>A report is a starting point, not a verdict.</h2></div><p class="fineprint no-margin">Tracker counts cannot establish unique downloads, lost revenue, or infringement. Verify title matches and rights ownership independently. These exports are not takedown notices or legally certified evidence.</p></section>`, "reports");
}
function settings() {
  return workShell(heading("Workspace & data", "Know what is connected, what is stored, and what is still a prototype.") + `<section class="panel settings-list">${[
    ["Access model", "Authenticated company workspaces with administrator, analyst and viewer roles. Private company records are separated from shared public research metadata.", "Local pilot"],
    ["Observation sources", "Historical catalogue: curated/swarm_torrent_day. Live tab: isolated ExposureIQ tracker snapshots, minute aggregates, and committed batches.", "Read-only queries"],
    ["Live monitoring", "Start or stop collection of known hashes in the Live monitoring tab. It resumes checkpoints and retains earlier observations.", "On demand"],
    ["Watchlist storage", "Stored in this browser’s local storage; not uploaded or synced.", `${state.saved.length} records`],
    ["New-release discovery", "Not connected. The historical RARBG listing catalogue ends in May 2023.", "Not available"],
    ["Notifications", "Portfolio checks evaluate company threshold rules. In-app alerts work immediately; email and webhooks require operator configuration. Report snapshots run while the server is online.", "Configurable"],
    ["Commercial usage", "Requires source licensing review, including replacement or licensing of non-commercial inputs.", "Not production-ready"],
  ].map(([title, copy, status]) => `<div class="settings-row"><div><h3>${title}</h3><p>${copy}</p></div><span class="badge">${esc(status)}</span></div>`).join("")}<div class="settings-row"><div><h3>Clear personal watchlist</h3><p>Remove saved records from this browser. Download an export first if needed.</p></div><button class="button outline small" data-action="clear-watchlist" ${state.saved.length ? "" : "disabled"}>Clear watchlist</button></div></section>${btn("Read the methodology", "#/about", "outline", true)} <a class="button outline" href="#/">Back to website</a>`, "settings");
}
function showDetail(hash) {
  const row = [...state.rows, ...state.saved].find(item => item.infohash === hash);
  if (!row) return notify("This record is no longer in the current view. Refresh the catalogue.");
  const saved = state.saved.some(item => item.infohash === hash);
  document.querySelector("#detail-content").innerHTML = `<div class="dialog-head"><div><span class="badge ${esc(row.category)}">${esc(categoryName[row.category])}</span><h2 class="mt">${esc(row.display_title)}</h2></div><button class="icon-button" data-action="close-detail" aria-label="Close record details">${icon("close")}</button></div><p class="detail-hash">Torrent infohash<br>${esc(row.infohash)}</p><dl class="detail-grid">${[
    ["Upload year", row.added_year], ["Original language", row.original_language || "Not recorded"],
    ["Median seeders", number(row.seeders_median)], ["Median leechers", number(row.leechers_median)],
    ["First observation (local time)", date(row.first_slot)], ["Last observation (local time)", date(row.last_slot)],
    ["IMDb identifier", row.imdb_id || "Not matched"], ["Steam identifier", row.steam_app_id || "Not matched"],
  ].map(([label, value]) => `<div><dt>${label}</dt><dd>${esc(value ?? "Not recorded")}</dd></div>`).join("")}</dl><div class="notice">${icon("info")}Source: curated/swarm_torrent_day. Historical tracker observations; a title match is not proof of infringement. Different records may refer to the same title.</div><button class="button" data-save="${esc(row.infohash)}">${icon("bookmark")}${saved ? "Remove from watchlist" : "Save to watchlist"}</button>`;
  const dialog = document.querySelector("#detail");
  if (!dialog.open) dialog.showModal();
}
function download(filename, content, type) {
  const url = URL.createObjectURL(new Blob([content], { type }));
  const a = document.createElement("a");
  a.href = url; a.download = filename;
  document.body.append(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function csvCell(value) {
  const text = String(value ?? "");
  // Spreadsheet applications can execute formulas even inside quoted CSV cells.
  return `"${(/^[\s]*[=+\-@]/.test(text) ? "'" + text : text).replaceAll('"', '""')}"`;
}
function exportRows(rows, name) {
  if (!rows.length) return notify("There are no records to export.");
  const keys = ["infohash", "display_title", "category", "added_year", "seeders_median", "leechers_median", "first_slot", "last_slot", "original_language", "imdb_id", "steam_app_id"];
  const columns = [...keys, "data_source", "observation_mode"];
  const lines = [columns.map(csvCell).join(","), ...rows.map(row => [...keys.map(key => row[key]), "curated/swarm_torrent_day", "historical"].map(csvCell).join(","))];
  download(`exposureiq-${name}-${new Date().toISOString().slice(0, 10)}.csv`, "\uFEFF" + lines.join("\r\n"), "text/csv;charset=utf-8");
  notify(`Exported ${rows.length} historical torrent records.`);
}
const methodology = `EXPOSUREIQ — METHODOLOGY & LIMITATIONS

Authenticated local company pilot; not a production or licensed commercial service.
Source: curated/swarm_torrent_day, derived from the historical research pipeline.
Main tracker collection: 10 October 2026. Historical listing catalogue ends May 2023.
Optional Live monitoring collects fresh observations of known hashes into an isolated Kafka topic,
PostgreSQL schema and Parquet output. It does not discover new releases.
Live SQL views expose latest per-hash/per-tracker records, per-minute aggregates and batch metadata.
These views retain earlier runs; use a scraped_at time filter to select recent observations.
SQL runs through a restricted read-only database login, with a 5-second timeout and 200-row result cap.
Rows represent torrent infohashes, NOT unique content titles or unique people.
Median seeders/leechers summarise observed tracker-reported peers across collection slots.
First/last observation timestamps describe the collection window, not first-ever leak dates.
No copyrighted media is downloaded and no peer IP addresses are stored.
Coverage is limited to the source catalogue and monitored trackers.
Absence of a record does not establish absence of exposure.
Title matches require human review. Game editions and TV episodes may be ambiguous.
Counts cannot establish infringement, unique downloads, lost revenue or global market share.
Review queue: saved records with historical median seeders > 0. No live alerts are sent.
Watchlists are stored locally in the browser, not shared or company-isolated.
CSV page exports contain only the displayed page, not all matching results.
Commercial deployment requires source-by-source data rights review and production safeguards.
`;
async function render() {
  state.controller?.abort();
  state.controller = new AbortController();
  const signal = state.controller.signal;
  const incoming = location.hash.replace(/^#\/?/, "").split("?")[0] || "home";
  const section = ["platform", "pricing", "about", "contact"].includes(incoming) ? incoming : null;
  const route = section ? "home" : incoming;
  state.route = route;
  document.querySelector("#detail").close();
  const pages = { home, platform, pricing, contact, about, watchlist, activity: review, reports, settings };
  const titles = { home: "See the bigger picture", platform: "The platform", pricing: "Plans & pilots", contact: "Let’s talk", about: "Data & methodology", workspace: "Overview", live: "Live monitoring & queries", explorer: "Catalogue explorer", watchlist: "Your watchlist", activity: "Review queue", reports: "Reports", settings: "Workspace & data" };
  document.title = `ExposureIQ — ${titles[route] || "Page not found"}`;
  if (["login", "register", "join"].includes(route)) { accountPage(route); return; }
  if (route !== "home" && !await requireCompany(signal)) return;
  if (signal.aborted) return;
  if (["portfolio", "titlecheck", "cases", "alerts", "companyreports", "team", "audit", "health"].includes(route)) {
    document.title = "ExposureIQ — Company workspace"; await companyPage(route, signal); return;
  }
  if (route === "live") livePage(signal);
  else if (route === "workspace") await dashboard(signal);
  else if (route === "explorer") await explorer(signal);
  else root.innerHTML = pages[route] ? pages[route]() : publicShell(subhero("A different perspective", "This page isn’t in view.", "Return to the website or open the workspace.") + `<section class="section">${btn("Back home", "#/", "", true)}</section>`);
  if (section) requestAnimationFrame(() => scrollLanding(section));
}
root.addEventListener("submit", event => {
  if (event.target.id === "filter-form") {
    event.preventDefault();
    const data = new FormData(event.target);
    state.filters = { q: data.get("q").trim(), category: data.get("category"), year: data.get("year"), min_seeders: data.get("min_seeders"), active: data.has("active") };
    state.offset = 0;
    render();
  }
  if (event.target.id === "contact-form") {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(event.target));
    download("exposureiq-enquiry.txt", "EXPOSUREIQ — UNSENT ENQUIRY\n\n" + Object.entries(data).map(([key, value]) => `${key}: ${value}`).join("\n\n"), "text/plain;charset=utf-8");
    document.querySelector("#contact-status").textContent = "Enquiry file downloaded. Nothing has been submitted; share the file with the project team.";
  }
});
document.addEventListener("click", event => {
  if (event.target.closest(".skip")) {
    event.preventDefault();
    document.querySelector("#main")?.focus();
    return;
  }
  const target = event.target.closest("button");
  if (!target || target.disabled) return;
  if (target.dataset.detail) return showDetail(target.dataset.detail);
  if (target.dataset.save) {
    const hash = target.dataset.save;
    const row = [...state.rows, ...state.saved].find(item => item.infohash === hash);
    if (!row) return notify("Record is unavailable. Refresh this view.");
    const exists = state.saved.some(item => item.infohash === hash);
    if (!saveWatchlist(exists ? state.saved.filter(item => item.infohash !== hash) : [...state.saved, row])) return;
    document.querySelectorAll("[data-watch-count]").forEach(count => { count.textContent = number(state.saved.length); });
    notify(exists ? "Removed from your browser watchlist." : "Saved to your browser watchlist.");
    if (document.querySelector("#detail").open) {
      if (exists) document.querySelector("#detail").close();
      else showDetail(hash);
    }
    if (["watchlist", "activity"].includes(state.route)) {
      root.innerHTML = state.route === "watchlist" ? watchlist() : review();
    } else {
      document.querySelectorAll("[data-save]").forEach(button => {
        if (button.dataset.save === hash) {
          button.classList.toggle("saved", !exists);
          button.setAttribute("aria-pressed", String(!exists));
          button.setAttribute("aria-label", `${exists ? "Add to" : "Remove from"} watchlist: ${row.display_title}`);
        }
      });
    }
    return;
  }
  switch (target.dataset.action) {
    case "retry": render(); break;
    case "reset": state.filters = { q: "", category: "all", year: "", min_seeders: "", active: false }; state.offset = 0; render(); break;
    case "prev": state.offset = Math.max(0, state.offset - 25); render(); break;
    case "next": state.offset += 25; render(); break;
    case "export-page": exportRows(state.rows, "catalogue-page"); break;
    case "export-watchlist": exportRows(state.saved, "watchlist"); break;
    case "methodology": download("exposureiq-methodology.txt", methodology, "text/plain;charset=utf-8"); break;
    case "close-detail": document.querySelector("#detail").close(); break;
    case "clear-watchlist":
      if (confirm("Remove all saved records from this browser? This cannot be undone.") && saveWatchlist([])) { render(); notify("Browser watchlist cleared."); }
      break;
  }
});
function scrollLanding(section) {
  document.querySelector(`#${section}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
  document.querySelectorAll(".site-nav a").forEach(link => {
    link.classList.toggle("active", link.hash === `#/${section}`);
    if (link.hash === `#/${section}`) link.setAttribute("aria-current", "location");
    else link.removeAttribute("aria-current");
  });
}
window.addEventListener("hashchange", () => {
  const section = location.hash.replace("#/", "");
  if (["platform", "pricing", "about", "contact"].includes(section) && document.querySelector(".hero")) scrollLanding(section);
  else { window.scrollTo(0, 0); render(); }
});
document.addEventListener("click", event => {
  const link = event.target.closest('.site-nav a,.footer-links a,a.button');
  if (link && ["#/platform", "#/pricing", "#/about", "#/contact"].includes(link.hash) && location.hash === link.hash && document.querySelector(".hero")) {
    event.preventDefault(); scrollLanding(link.hash.replace("#/", ""));
  }
});
render();
