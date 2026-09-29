// Stock News page: reads stored data from the local API and keeps it current by polling.
//
// Polling is GET-only. The API never calls a model and never starts a collector, so
// refreshing (in any number of tabs) cannot trigger ingestion, grouping or summaries.
// One refresh cycle runs at a time (a request made meanwhile is queued; a filter or tab
// change cancels the running cycle instead of waiting for it), each request has its own
// timeout, and each part of the page is drawn as soon as its own response arrives, so a
// slow or failing endpoint never holds back the others. Polling pauses while the tab is
// hidden, failures back off (20 s doubling to 5 min), and a failed refresh keeps the
// content already shown. Cards are keyed by their stable
// ids and patched in place, so open details, focus and reading position survive.

(function () {
  "use strict";

  const R = window.StockNewsRender;
  const $ = (id) => document.getElementById(id);
  const POLL_MS = 20000;
  const MAX_BACKOFF_MS = 300000;
  const REQUEST_TIMEOUT_MS = 15000;
  const PAGE = 50;
  const MAX_EVENTS = 100;   // /api/events limit
  const MAX_ARTICLES = 200; // /api/articles limit
  const TABS = ["events", "articles", "explorer"];
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const SYMBOL = /^[A-Z0-9.\-]{1,12}$/;

  const state = {
    tab: "events",
    sample: false,
    source: "all",
    company: "NVDA",
    companyFromUrl: false,
    gen: 0, // bumped when filters change: responses for the old filters are dropped
    status: null,
    events: { items: null, limit: PAGE, error: null },
    articles: { items: null, total: 0, limit: PAGE, error: null },
    details: new Map(), // event_id -> {data} | {gone: true} | {data?, error}
    expanded: new Set(),
    baseline: { events: null, articles: null }, // id -> first-seen signature, for "New" / "Updated"
    revealEvent: null, // an event to scroll to once the list has it (article link or #event=<id>)
  };
  const poll = { inFlight: false, again: false, timer: null, failures: 0, lastOk: null, lastFailAt: null,
                 error: null, nextAt: null, controller: null };

  // ------------------------------------------------------------------ data
  // `signal` cancels the whole refresh cycle; each request also gets its own deadline.
  async function getJson(url, signal) {
    const own = new AbortController();
    const timer = setTimeout(() => own.abort(), REQUEST_TIMEOUT_MS);
    const cancel = () => own.abort();
    if (signal.aborted) cancel();
    else signal.addEventListener("abort", cancel, { once: true });
    try {
      const response = await fetch(url, { headers: { Accept: "application/json" }, signal: own.signal,
                                          cache: "no-store" });
      let data = null;
      try {
        data = await response.json();
      } catch (error) {
        if (error && error.name === "AbortError") throw error;
        data = null;
      }
      if (!response.ok || !data || data.error) {
        const error = new Error((data && data.error) || `HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      return data;
    } finally {
      clearTimeout(timer);
      signal.removeEventListener("abort", cancel);
    }
  }

  function describe(error) {
    if (error && (error.name === "AbortError" || error.name === "TimeoutError")) return "the server did not answer in time";
    if (error instanceof TypeError) return "the server could not be reached";
    return (error && error.message) || "unknown error";
  }

  function query(extra) {
    const p = new URLSearchParams(extra || {});
    if (state.sample) p.set("include_sample", "true");
    if (state.company) p.set("symbol", state.company);
    return p.toString();
  }

  const eventSignature = (e) => [e.documents, e.summary ? e.summary.version : 0, e.summary_state,
    e.conflict ? `${e.conflict.status}:${e.conflict.count}:${e.conflict.reviews}` : ""].join("|");
  const articleSignature = (a) => a.revision_id;

  // Returns the items first seen now (none on the first load after a filter change).
  function track(kind, items, idOf, signatureOf) {
    let base = state.baseline[kind];
    if (!base) {
      state.baseline[kind] = new Map(items.map((i) => [idOf(i), { sig: signatureOf(i), isNew: false }]));
      return [];
    }
    const fresh = [];
    for (const item of items) {
      const entry = base.get(idOf(item));
      if (!entry) {
        base.set(idOf(item), { sig: signatureOf(item), isNew: true });
        fresh.push(item);
      } else {
        entry.current = signatureOf(item);
      }
    }
    return fresh;
  }

  function unique(items, idOf) {
    const seen = new Set();
    return items.filter((i) => !seen.has(idOf(i)) && seen.add(idOf(i)));
  }

  async function loadStatus(signal, gen) {
    const status = await getJson(`/api/status?${new URLSearchParams(state.sample ? { include_sample: "true" } : {})}`, signal);
    if (gen === state.gen) state.status = status;
  }

  async function loadEvents(signal, gen) {
    try {
      const data = await getJson(`/api/events?${query({ limit: state.events.limit })}`, signal);
      if (gen !== state.gen) return;
      const items = unique(data.events, (e) => e.event_id);
      const fresh = track("events", items, (e) => e.event_id, eventSignature);
      state.events.items = items;
      state.events.error = null;
      if (fresh.length) announce(`${R.plural(fresh.length, "new event")}.`);
    } catch (error) {
      if (gen === state.gen && !signal.aborted) state.events.error = describe(error);
      throw error;
    }
  }

  async function loadDetails(signal, gen) {
    const listed = new Set((state.events.items || []).map((e) => e.event_id));
    const ids = [...state.expanded].filter((id) => listed.has(id) || id === state.revealEvent);
    let failure = null;
    for (let i = 0; i < ids.length; i += 4) { // a few at a time, never a burst
      await Promise.all(ids.slice(i, i + 4).map(async (id) => {
        try {
          const detail = await getJson(`/api/events/${encodeURIComponent(id)}?${query()}`, signal);
          if (gen === state.gen) state.details.set(id, { data: detail });
        } catch (error) {
          if (gen !== state.gen || signal.aborted) return; // filters changed or the cycle was replaced
          if (error.status === 404) {
            state.details.set(id, { gone: true });
          } else {
            state.details.set(id, { ...(state.details.get(id) || {}), error: describe(error) });
            failure = failure || error;
          }
        }
      }));
    }
    if (failure) throw failure;
  }

  async function loadArticles(signal, gen) {
    try {
      const data = await getJson(`/api/articles?${query({ limit: state.articles.limit, source: state.source })}`, signal);
      if (gen !== state.gen) return;
      const items = unique(data.articles, (a) => a.document_id);
      const fresh = track("articles", items, (a) => a.document_id, articleSignature);
      state.articles.items = items;
      state.articles.total = data.total;
      state.articles.error = null;
      if (fresh.length) announce(`${R.plural(fresh.length, "new article")}.`);
    } catch (error) {
      if (gen === state.gen && !signal.aborted) state.articles.error = describe(error);
      throw error;
    }
  }

  // ------------------------------------------------------------------ polling
  async function refresh() {
    if (poll.inFlight) {
      poll.again = true; // no overlapping requests: run once more when this cycle ends
      return;
    }
    clearTimeout(poll.timer);
    poll.timer = null;
    poll.inFlight = true;
    const gen = state.gen;
    const tab = state.tab;
    const controller = new AbortController();
    poll.controller = controller;
    const current = () => gen === state.gen && !controller.signal.aborted;
    // Draw each part as soon as its own response is in: a slow endpoint never holds back the others.
    const drawn = (job) => job.finally(() => { if (current()) safeRender(); });
    setBusy(tab, true);
    const jobs = [drawn(loadStatus(controller.signal, gen))];
    if (tab === "events") {
      jobs.push(drawn((async () => {
        let failure = null;
        try {
          await loadEvents(controller.signal, gen);
        } catch (error) {
          failure = error;
        }
        await loadDetails(controller.signal, gen);
        if (failure) throw failure;
      })()));
    } else if (tab === "articles") {
      jobs.push(drawn(loadArticles(controller.signal, gen)));
    }
    let results = [];
    try {
      results = await Promise.allSettled(jobs);
    } finally {
      poll.inFlight = false;
      poll.controller = null;
      setBusy(tab, false);
    }
    if (current()) {
      const failed = results.find((r) => r.status === "rejected");
      if (failed) {
        if (!poll.failures) announce("Refresh failed. The page keeps showing the data it already has.");
        poll.failures += 1;
        poll.error = describe(failed.reason);
        poll.lastFailAt = new Date();
      } else {
        if (poll.failures) announce("Connection restored. The page is up to date.");
        poll.failures = 0;
        poll.error = null;
        poll.lastOk = new Date();
      }
      safeRender();
    }
    if (poll.again) {
      poll.again = false;
      refresh();
      return;
    }
    schedule();
  }

  // A filter or tab change: stop waiting for the running cycle (its answers no longer matter) and run a new one.
  function supersede() {
    if (poll.controller) poll.controller.abort();
    poll.again = poll.inFlight;
  }

  function schedule() {
    clearTimeout(poll.timer);
    poll.timer = null;
    poll.nextAt = null;
    if (!document.hidden) {
      const delay = poll.failures ? Math.min(POLL_MS * 2 ** poll.failures, MAX_BACKOFF_MS) : POLL_MS;
      poll.nextAt = new Date(Date.now() + delay);
      poll.timer = setTimeout(refresh, delay);
    }
    renderRefreshStatus();
  }

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearTimeout(poll.timer); // no polling while nobody can see the page
      poll.timer = null;
      poll.nextAt = null;
    } else {
      refresh();
    }
  });

  // ------------------------------------------------------------------ DOM patching
  const slotHtml = new WeakMap();

  function focusKey(el) {
    if (el.matches("summary")) {
      const details = el.closest("details[data-key]");
      return details ? JSON.stringify(["summary", details.dataset.key]) : null;
    }
    if (el.matches("a[href]")) return JSON.stringify(["a", el.getAttribute("href"), el.textContent]);
    return null;
  }

  function findFocusKey(slot, key) {
    const [kind, a, b] = JSON.parse(key);
    if (kind === "summary") {
      return [...slot.querySelectorAll("details[data-key] > summary")].find((s) => s.parentElement.dataset.key === a);
    }
    return [...slot.querySelectorAll("a[href]")].find((x) => x.getAttribute("href") === a && x.textContent === b);
  }

  // Replace a slot's content only when it changed, keeping open <details> and focus.
  function setSlot(card, name, html) {
    const slot = card.querySelector(`:scope > [data-slot="${name}"]`);
    if (!slot || slotHtml.get(slot) === html) return;
    const open = new Set([...slot.querySelectorAll("details[data-key][open]")].map((d) => d.dataset.key));
    const active = document.activeElement;
    const key = active && slot.contains(active) ? focusKey(active) : null;
    slot.innerHTML = html;
    slotHtml.set(slot, html);
    for (const d of slot.querySelectorAll("details[data-key]")) if (open.has(d.dataset.key)) d.open = true;
    if (key) {
      const again = findFocusKey(slot, key);
      if (again) again.focus({ preventScroll: true });
    }
  }

  function setHtml(el, html) {
    if (slotHtml.get(el) === html) return;
    el.innerHTML = html;
    slotHtml.set(el, html);
  }

  // Keep the first visible card where it is on screen when cards are added or removed above it.
  function captureAnchor(list) {
    if (window.scrollY <= 0) return null; // at the top, new cards simply appear
    for (const el of list.children) {
      const rect = el.getBoundingClientRect();
      if (rect.bottom > 0) return { el, top: rect.top };
    }
    return null;
  }

  function restoreAnchor(anchor) {
    if (!anchor || !anchor.el.isConnected) return;
    const delta = anchor.el.getBoundingClientRect().top - anchor.top;
    if (Math.abs(delta) > 1) window.scrollBy(0, delta);
  }

  function patchList(list, items, idOf, create, update) {
    const anchor = captureAnchor(list);
    const focused = list.contains(document.activeElement) ? document.activeElement : null;
    const existing = new Map([...list.children].map((el) => [el.dataset.id, el]));
    let previous = null;
    for (const item of items) {
      const id = idOf(item);
      let el = existing.get(id);
      if (el) existing.delete(id);
      else el = create(id);
      update(el, item);
      const wanted = previous ? previous.nextElementSibling : list.firstElementChild;
      if (wanted !== el) list.insertBefore(el, wanted);
      previous = el;
    }
    for (const el of existing.values()) el.remove();
    if (focused && focused.isConnected && document.activeElement !== focused) focused.focus({ preventScroll: true });
    restoreAnchor(anchor);
  }

  function setBusy(tab, busy) {
    const list = $(`${tab}-list`);
    if (list) list.setAttribute("aria-busy", String(busy));
  }

  function announce(text) {
    const el = $("announcer");
    el.textContent = "";
    setTimeout(() => { el.textContent = text; }, 50);
  }

  // ------------------------------------------------------------------ rendering
  function processing() {
    const s = state.status;
    if (!s) return {};
    const c = s.collector || {};
    if (c.state === "running" && c.processing) return { grouping: c.processing.similarity, summaries: c.processing.summaries };
    return { grouping: s.features && s.features.similarity_enabled, summaries: s.features && s.features.summary_enabled };
  }

  function ctx(kind) {
    const base = state.baseline[kind];
    const flags = processing();
    return {
      isNew: (id) => Boolean(base && base.get(id) && base.get(id).isNew),
      isUpdated: (id) => {
        const entry = base && base.get(id);
        return Boolean(entry && !entry.isNew && entry.current !== undefined && entry.current !== entry.sig);
      },
      summariesEnabled: flags.summaries,
      groupingEnabled: flags.grouping,
    };
  }

  function renderRefreshStatus() {
    const el = $("refresh-status");
    const alert = $("refresh-alert");
    let cls;
    let html;
    if (poll.error && poll.lastOk) {
      cls = "status-warn";
      html = `${R.icon("warn")}Refresh failed ${R.clock(poll.lastFailAt)}; showing data from ${R.clock(poll.lastOk)}`;
    } else if (poll.error) {
      cls = "status-error";
      html = `${R.icon("error")}Could not load data`;
    } else if (poll.lastOk) {
      cls = "status-ok";
      html = `${R.icon("ok")}Updated ${R.clock(poll.lastOk)} · refreshes every ${POLL_MS / 1000} s`;
    } else {
      cls = "status-off";
      html = `${R.icon("off")}Loading…`;
    }
    el.className = cls;
    setHtml(el, html);
    alert.hidden = !poll.error;
    if (poll.error) {
      setHtml($("refresh-alert-text"), `${R.icon("warn")}<strong>Could not refresh:</strong> ${R.esc(poll.error)}.
        ${poll.lastOk ? `The page keeps showing the data from ${R.clock(poll.lastOk)}.` : ""}
        ${poll.nextAt ? `Next attempt at ${R.clock(poll.nextAt)}.` : ""}`);
    }
  }

  function renderHeader() {
    renderRefreshStatus();
    const collector = R.collectorStatus(state.status);
    const el = $("collector-status");
    if (poll.error && state.status) {
      // The server cannot be reached, so the collectors' current state is unknown: show the last report as such.
      el.className = "status-off";
      setHtml(el, `Last known (${R.clock(poll.lastOk)}): ${collector.html}`);
    } else {
      el.className = collector.cls;
      setHtml(el, collector.html);
    }
    setHtml($("processing-status"), R.esc(R.processingStatus(state.status)));
    $("sample-banner").hidden = !state.sample;
    renderCompanies();
  }

  function renderCompanies() {
    const select = $("company-select");
    if (document.activeElement === select || !state.status) return; // never rebuild under the user's hands
    const symbols = (state.status.symbols || []).filter((s) => SYMBOL.test(s));
    if (!state.companyFromUrl && symbols.length && !symbols.includes(state.company)) {
      state.company = symbols[0]; // NVDA is only the initial guess; follow the configured symbols
      resetData();
      poll.again = true; // render runs at the end of a refresh cycle, which then starts the next one
    }
    const options = [...new Set([...symbols, state.company].filter(Boolean))];
    const html = options.map((s) => `<option value="${R.esc(s)}">${R.esc(s)}</option>`).join("") +
      `<option value="">All companies</option>`;
    setHtml(select, html);
    select.value = state.company;
  }

  function createEventCard(id) {
    const el = document.createElement("article");
    el.className = "card event-card";
    el.dataset.id = id;
    el.id = `event-${id}`;
    el.setAttribute("aria-labelledby", `event-${id}-title`);
    el.innerHTML = `<div data-slot="head"></div><div data-slot="conflict"></div><div data-slot="summary"></div>
      <div class="card-actions"><button type="button" class="link-button details-toggle" aria-expanded="false"
        aria-controls="event-${id}-details"></button></div>
      <div class="details" id="event-${id}-details" data-slot="details" hidden></div>`;
    return el;
  }

  function detailHtml(id) {
    const entry = state.details.get(id);
    if (!entry) return `<p class="muted" role="status">Loading sources and evidence…</p>`;
    if (entry.gone) {
      return `<p class="state-note">This event is no longer available. It may have been merged into another event.</p>`;
    }
    if (!entry.data) return R.loadError("the event details", entry.error || "unknown error");
    return R.eventDetails(entry.data);
  }

  function updateEventCard(el, e) {
    const c = ctx("events");
    setSlot(el, "head", R.eventHead(e, c));
    setSlot(el, "conflict", R.eventConflict(e));
    setSlot(el, "summary", R.eventSummary(e, c));
    const open = state.expanded.has(e.event_id);
    const button = el.querySelector(".details-toggle");
    button.setAttribute("aria-expanded", String(open));
    const label = open ? "Hide sources and evidence" : `Show sources and evidence (${e.documents})`;
    if (button.textContent !== label) button.textContent = label;
    el.querySelector(':scope > [data-slot="details"]').hidden = !open;
    if (open) setSlot(el, "details", detailHtml(e.event_id));
  }

  function createArticleCard(id) {
    const el = document.createElement("article");
    el.className = "card article-card";
    el.dataset.id = id;
    el.id = `article-${id}`;
    el.innerHTML = `<div data-slot="body"></div>`;
    return el;
  }

  function updateArticleCard(el, a) {
    setSlot(el, "body", R.articleCard(a, ctx("articles")));
  }

  function renderEvents() {
    const events = state.events;
    const list = $("events-list");
    if (events.items === null) {
      setHtml($("events-state"), events.error ? R.loadError("events", events.error)
        : `<div class="state" role="status">Loading events…</div>`);
      list.replaceChildren();
      setHtml($("events-footer"), "");
      return;
    }
    setHtml($("events-state"), events.items.length ? "" : R.eventsEmpty(state.status, state.sample));
    patchList(list, events.items, (e) => e.event_id, createEventCard, updateEventCard);
    // For automated checks: "list" once events are shown, "detail" once an open event's details are shown.
    document.body.dataset.eventsReady = [...state.expanded].some((id) => (state.details.get(id) || {}).data)
      ? "detail" : "list";
    const full = events.items.length >= events.limit;
    setHtml($("events-footer"), events.items.length ? `<span class="muted">${R.esc(R.plural(events.items.length,
      "event"))} shown, most recent first.</span>${full && events.limit < MAX_EVENTS
      ? `<button type="button" class="button secondary" id="events-more">Show more events</button>` : ""}` : "");
    revealPending();
  }

  function renderArticles() {
    const articles = state.articles;
    const list = $("articles-list");
    if (articles.items === null) {
      setHtml($("articles-state"), articles.error ? R.loadError("articles", articles.error)
        : `<div class="state" role="status">Loading articles…</div>`);
      list.replaceChildren();
      setHtml($("articles-footer"), "");
      return;
    }
    setHtml($("articles-state"), articles.items.length ? "" : R.articlesEmpty(state.source, state.sample));
    patchList(list, articles.items, (a) => a.document_id, createArticleCard, updateArticleCard);
    const more = articles.total > articles.items.length && articles.limit < MAX_ARTICLES;
    setHtml($("articles-footer"), articles.items.length ? `<span class="muted">Showing ${R.esc(articles.items.length)}
      of ${R.esc(articles.total)}, newest first.</span>${more
      ? `<button type="button" class="button secondary" id="articles-more">Show more articles</button>` : ""}` : "");
  }

  function render() {
    renderHeader();
    if (state.tab === "events") renderEvents();
    else if (state.tab === "articles") renderArticles();
  }

  // A rendering bug must not stop polling or leave the page half-updated without a trace.
  function safeRender() {
    try {
      render();
    } catch (error) {
      console.error("Stock News: rendering failed", error);
    }
  }

  function revealPending() {
    const id = state.revealEvent;
    if (!id || state.events.items === null) return;
    const card = document.getElementById(`event-${id}`);
    if (card) {
      state.revealEvent = null;
      card.scrollIntoView({ block: "start" }); // the user asked for this event
      const title = card.querySelector(".card-title");
      if (title) {
        title.tabIndex = -1;
        title.focus({ preventScroll: true });
      }
    } else if (state.events.limit < MAX_EVENTS && state.events.items.length >= state.events.limit) {
      state.events.limit = MAX_EVENTS; // look further back once
      poll.again = true;
    } else {
      state.revealEvent = null;
      announce("That event is not in the list of recent events.");
    }
  }

  // ------------------------------------------------------------------ interaction
  function resetData() {
    state.gen += 1;
    state.events = { items: null, limit: PAGE, error: null };
    state.articles = { items: null, total: 0, limit: PAGE, error: null };
    state.baseline = { events: null, articles: null };
    state.details.clear();
    supersede(); // a cycle for the old filters may be running: cancel it and follow with one for the new ones
  }

  // The hash is the whole view state (#tab=articles&sample=1&source=sec&company=NVDA); missing keys are defaults.
  // #event=<id> (also used by the earlier page) opens that event.
  function readHash() {
    const p = new URLSearchParams(location.hash.replace(/^#/, ""));
    state.tab = TABS.includes(p.get("tab")) ? p.get("tab") : "events";
    state.sample = p.get("sample") === "1";
    state.source = ["news", "sec"].includes(p.get("source")) ? p.get("source") : "all";
    if (p.has("company")) {
      const company = (p.get("company") || "").toUpperCase();
      if (company === "" || SYMBOL.test(company)) {
        state.company = company;
        state.companyFromUrl = true;
      }
    }
    const event = p.get("event");
    return event && UUID.test(event) ? event : null;
  }

  function writeHash() {
    const p = new URLSearchParams();
    if (state.tab !== "events") p.set("tab", state.tab);
    if (state.sample) p.set("sample", "1");
    if (state.source !== "all") p.set("source", state.source);
    if (state.companyFromUrl || state.company !== "NVDA") p.set("company", state.company);
    const hash = p.toString();
    history.replaceState(null, "", hash ? `#${hash}` : location.pathname + location.search);
  }

  function syncControls() {
    $("sample-toggle").checked = state.sample;
    $("source-filter").value = state.source;
  }

  function selectTab(name, { focus = false, load = true } = {}) {
    state.tab = name;
    for (const tab of TABS) {
      const selected = tab === name;
      const button = $(`tab-${tab}`);
      button.setAttribute("aria-selected", String(selected));
      button.tabIndex = selected ? 0 : -1;
      $(`panel-${tab}`).hidden = !selected;
    }
    $("source-filter-label").hidden = name !== "articles";
    if (focus) $(`tab-${name}`).focus();
    writeHash();
    safeRender();
    if (load) {
      supersede(); // do not wait for a cycle that is loading another tab
      refresh();
    }
  }

  function openEvent(id) {
    if (!UUID.test(id)) return;
    state.expanded.add(id);
    state.revealEvent = id;
    if (state.tab !== "events") selectTab("events");
    else {
      render();
      refresh();
    }
  }

  const tablist = document.querySelector('[role="tablist"]');
  tablist.addEventListener("click", (event) => {
    const tab = event.target.closest('[role="tab"]');
    if (tab) selectTab(tab.id.replace("tab-", ""));
  });
  tablist.addEventListener("keydown", (event) => {
    const index = TABS.indexOf(state.tab);
    const next = { ArrowRight: index + 1, ArrowLeft: index - 1, Home: 0, End: TABS.length - 1 }[event.key];
    if (next === undefined) return;
    event.preventDefault();
    selectTab(TABS[(next + TABS.length) % TABS.length], { focus: true });
  });

  $("events-list").addEventListener("click", (event) => {
    const button = event.target.closest(".details-toggle");
    if (!button) return;
    const card = button.closest(".event-card");
    const id = card.dataset.id;
    if (state.expanded.has(id)) state.expanded.delete(id);
    else state.expanded.add(id);
    const item = (state.events.items || []).find((e) => e.event_id === id);
    if (item) updateEventCard(card, item);
    if (state.expanded.has(id)) refresh(); // loads (or refreshes) the details; queued if a cycle is running
  });

  $("articles-list").addEventListener("click", (event) => {
    const link = event.target.closest("a[data-event-link]");
    if (!link) return;
    event.preventDefault();
    openEvent(link.dataset.eventLink);
  });

  document.addEventListener("click", (event) => {
    if (event.target.id === "events-more") {
      state.events.limit = Math.min(state.events.limit + PAGE, MAX_EVENTS);
      state.baseline.events = null; // older events loaded now are not "new"
      refresh();
    } else if (event.target.id === "articles-more") {
      state.articles.limit = Math.min(state.articles.limit + PAGE, MAX_ARTICLES);
      state.baseline.articles = null;
      refresh();
    }
  });

  $("sample-toggle").addEventListener("change", (event) => {
    state.sample = event.target.checked;
    resetData();
    writeHash();
    render();
    refresh();
  });
  $("source-filter").addEventListener("change", (event) => {
    state.source = event.target.value;
    resetData();
    writeHash();
    render();
    refresh();
  });
  $("company-select").addEventListener("change", (event) => {
    state.company = event.target.value;
    state.companyFromUrl = true;
    resetData();
    writeHash();
    render();
    refresh();
  });
  $("refresh-now").addEventListener("click", () => refresh());

  window.addEventListener("hashchange", () => {
    if (location.hash === "#main") return; // the skip link, not a view change
    const event = readHash();
    syncControls();
    resetData();
    selectTab(state.tab);
    if (event) openEvent(event);
  });

  // ------------------------------------------------------------------ start
  const initialEvent = readHash();
  syncControls();
  selectTab(state.tab, { load: false });
  if (initialEvent) openEvent(initialEvent);
  else refresh();
})();
