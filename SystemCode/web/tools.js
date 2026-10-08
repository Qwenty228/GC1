// Data tools page (tools.html, not the reader view): collector status, processing activity and every
// stored article. The status and articles load when opened and when asked; the processing activity
// is polled every few seconds while the page is visible. Like the reader it only reads stored data
// and status files, so nothing here calls a model or starts a collector.

(function () {
  "use strict";

  const R = window.StockNewsRender;
  const $ = (id) => document.getElementById(id);
  const PAGE = 50;
  const MAX_ARTICLES = 200; // /api/articles limit
  const REQUEST_TIMEOUT_MS = 15000;
  const SYMBOL = /^[A-Z0-9.\-]{1,12}$/;
  const state = { status: null, limit: PAGE, run: 0 };

  async function getJson(url) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const response = await fetch(url, { headers: { Accept: "application/json" }, signal: controller.signal,
                                          cache: "no-store" });
      let data = null;
      try {
        data = await response.json();
      } catch (_) {
        data = null;
      }
      if (!response.ok || !data || data.error) throw new Error((data && data.error) || `HTTP ${response.status}`);
      return data;
    } finally {
      clearTimeout(timer);
    }
  }

  function describe(error) {
    if (error && error.name === "AbortError") return "the server did not answer in time";
    if (error instanceof TypeError) return "the server could not be reached";
    return (error && error.message) || "unknown error";
  }

  const sample = () => $("sample-toggle").checked;

  async function loadStatus() {
    const el = $("collector-status");
    try {
      state.status = await getJson(`/api/status?${new URLSearchParams(sample() ? { include_sample: "true" } : {})}`);
      const collector = R.collectorStatus(state.status);
      el.className = collector.cls;
      el.innerHTML = collector.html;
      $("processing-status").textContent = R.processingStatus(state.status);
      renderCompanies();
    } catch (error) {
      el.className = "status-error";
      el.innerHTML = `${R.icon("error")}Could not load the status: ${R.esc(describe(error))}`;
    }
  }

  function renderCompanies() {
    const select = $("company-select");
    const current = select.value;
    const symbols = ((state.status && state.status.symbols) || []).filter((s) => SYMBOL.test(s));
    select.innerHTML = `<option value="">All companies</option>` +
      symbols.map((s) => `<option value="${R.esc(s)}">${R.esc(s)}</option>`).join("");
    select.value = symbols.includes(current) ? current : "";
  }

  async function loadArticles() {
    const run = ++state.run;
    const p = new URLSearchParams({ limit: state.limit, source: $("source-filter").value });
    if (sample()) p.set("include_sample", "true");
    if ($("company-select").value) p.set("symbol", $("company-select").value);
    const list = $("articles-list");
    list.setAttribute("aria-busy", "true");
    if (!list.children.length) $("articles-state").innerHTML = `<div class="state" role="status">Loading articles…</div>`;
    try {
      const data = await getJson(`/api/articles?${p}`);
      if (run !== state.run) return;
      const features = (state.status && state.status.features) || {};
      const ctx = { groupingEnabled: features.similarity_enabled };
      $("articles-state").innerHTML = data.articles.length ? "" : R.articlesEmpty($("source-filter").value, sample());
      list.innerHTML = data.articles.map((a) => `<article class="card article-card">${R.articleCard(a, ctx)}</article>`)
        .join("");
      const more = data.total > data.articles.length && state.limit < MAX_ARTICLES;
      $("articles-footer").innerHTML = data.articles.length ? `<span class="muted">Showing ${R.esc(data.articles.length)}
        of ${R.esc(data.total)}, newest first.</span>${more
        ? `<button type="button" class="button secondary" id="articles-more">Show more articles</button>` : ""}` : "";
    } catch (error) {
      if (run !== state.run) return;
      $("articles-state").innerHTML = R.loadError("the articles", describe(error), false);
    } finally {
      if (run === state.run) list.setAttribute("aria-busy", "false");
    }
  }

  function reload() {
    state.limit = PAGE;
    loadArticles();
  }

  // ------------------------------------------------------------------ processing activity (live)
  // Each part is redrawn only when its HTML changed, so an opened item, focus and scrolling survive the polling.
  const ACTIVITY_MS = 2000;
  const ACTIVITY_RETRY_MS = 15000;
  const activity = { timer: null, failures: 0, open: new Set(), drawn: new Map() };

  function patch(id, html) {
    if (activity.drawn.get(id) === html) return false;
    activity.drawn.set(id, html);
    $(id).innerHTML = html;
    return true;
  }

  async function loadActivity() {
    clearTimeout(activity.timer);
    activity.timer = null;
    try {
      const data = await getJson("/api/activity");
      const now = new Date(data.server_time);
      for (const [key, name] of [["worker", "The ingestion worker"], ["summary", "The startup summary catch-up"]]) {
        const proc = data[key];
        const status = R.activityStatus(proc, name, now);
        $(`activity-${key}-status`).className = `small ${status.cls}`;
        patch(`activity-${key}-status`, status.html);
        patch(`activity-${key}-current`, R.activityCurrent(proc, now));
        if (patch(`activity-${key}-recent`, R.activityRecent(proc, now) ||
          `<p class="muted small">Nothing processed yet.</p>`)) {
          for (const item of $(`activity-${key}-recent`).querySelectorAll("details[data-key]")) {
            item.open = activity.open.has(item.dataset.key); // opened items stay open across redraws
          }
        }
      }
      activity.failures = 0;
      $("activity-status").className = "small muted";
      $("activity-status").textContent = `Live · checked at ${new Date().toLocaleTimeString()}`;
    } catch (error) {
      activity.failures += 1;
      $("activity-status").className = "small status-error";
      $("activity-status").innerHTML = `${R.icon("error")}Could not load the processing activity:
        ${R.esc(describe(error))}. Trying again shortly.`;
    } finally {
      if (!document.hidden) activity.timer = setTimeout(loadActivity, activity.failures ? ACTIVITY_RETRY_MS : ACTIVITY_MS);
    }
  }

  // `toggle` does not bubble, so it is caught on the way down.
  document.addEventListener("toggle", (event) => {
    const item = event.target;
    if (!(item instanceof HTMLDetailsElement) || !item.dataset.key) return;
    if (item.open) activity.open.add(item.dataset.key);
    else activity.open.delete(item.dataset.key);
  }, true);
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearTimeout(activity.timer); // no polling while nobody can see the page
      activity.timer = null;
    } else {
      loadActivity();
    }
  });

  $("source-filter").addEventListener("change", reload);
  $("company-select").addEventListener("change", reload);
  $("sample-toggle").addEventListener("change", () => {
    loadStatus();
    reload();
  });
  $("refresh-now").addEventListener("click", () => {
    loadStatus();
    loadArticles();
  });
  document.addEventListener("click", (event) => {
    if (event.target.id === "articles-more") {
      state.limit = Math.min(state.limit + PAGE, MAX_ARTICLES);
      loadArticles();
    }
  });

  loadActivity();
  loadStatus().then(loadArticles);
})();
