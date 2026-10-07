// Stock News reader: one feed of AI news summaries read from the local API, kept current by polling.
//
// Polling is GET-only. The API never calls a model and never starts a collector, so reading,
// expanding or refreshing (in any number of tabs) cannot trigger ingestion, grouping or summaries.
// One refresh cycle runs at a time (a request made meanwhile is queued; a filter change cancels
// the running cycle instead of waiting for it), each request has its own timeout, and each part of
// the page is drawn as soon as its own response arrives, so a slow or failing endpoint never holds
// back the others. Polling pauses while the tab is hidden, failures back off (20 s doubling to
// 5 min), and a failed refresh keeps the content already shown. Cards are keyed by their stable
// ids and patched in place, so open summaries and sources, focus and reading position survive.
//
// Views: "All companies" (optionally narrowed to one company), and "My watchlist": the companies the
// reader follows, saved in this browser only (watchlist.js, by SEC CIK). The watchlist is sent to the
// API as a filter (company=<cik>, applied before paging), so no stored story is missed and a story
// about several followed companies is one card. Following a company changes nothing on the server.
//
// The view is in the address hash: #view=watchlist, #company=NVDA (one company; empty for all),
// #event=<id> opens that story's sources, #story=<id> scrolls back to a story (set when leaving for
// its page), and #sample=1 (demos and tests only; the page has no control for it) adds the fictional
// sample stories, each marked Sample. Without a view in the hash, the last view chosen in this
// browser is used.

(function () {
  "use strict";

  const R = window.StockNewsRender;
  const W = window.StockNewsWatchlist;
  const $ = (id) => document.getElementById(id);
  const POLL_MS = 20000;
  const MAX_BACKOFF_MS = 300000;
  const REQUEST_TIMEOUT_MS = 15000;
  const CLOCK_MS = 30000; // redraws "Updated … ago"; no request
  const COVERAGE_MS = 600000; // the company list rarely changes: reloaded every 10 minutes
  const PAGE = 50;
  const MAX_EVENTS = 100; // /api/events limit
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const SYMBOL = /^[A-Z0-9.\-]{1,12}$/;

  const saved = W.load();
  const state = {
    sample: false,
    view: "all", // "all" | "watchlist" | "company"
    company: "", // the one company of the "company" view
    watchlist: saved.companies, // SEC CIKs, as stored in this browser
    watchStatus: saved.status, // how the stored value read: empty | ok | repaired | invalid
    storageAvailable: saved.available,
    coverage: null, // /api/coverage: the companies a reader can follow
    coverageError: null,
    coverageAt: 0,
    followOpen: false,
    gen: 0, // bumped when filters change: responses for the old filters are dropped
    status: null,
    activity: null, // /api/activity: what the worker / summary catch-up is processing right now
    events: { items: null, limit: PAGE, error: null },
    details: new Map(), // event_id -> {data} | {gone: true} | {data?, error}
    sourcesOpen: new Set(), // cards whose Sources panel is open
    readMore: new Set(), // cards showing the whole summary
    seen: null, // event ids already shown, to announce new stories
    revealEvent: null, // an event to scroll to once the list has it (#event=<id> or #story=<id>)
    revealQuiet: false, // #story: scroll back to it without opening its sources or announcing a miss
  };
  const poll = { inFlight: false, again: false, timer: null, failures: 0, lastOk: null, lastFailAt: null,
                 error: null, nextAt: null, controller: null };

  // ------------------------------------------------------------------ watchlist
  function offered() {
    return state.coverage ? state.coverage.companies.map((c) => c.cik) : null;
  }
  // The followed companies this site still offers (all stored ones until the company list has loaded).
  function following() {
    const list = offered();
    return list ? W.sanitize(state.watchlist, list).active : state.watchlist;
  }
  function ignoredSaved() {
    const list = offered();
    return list ? W.sanitize(state.watchlist, list).unknown : [];
  }
  function companyOf(cik) {
    return (state.coverage && state.coverage.companies.find((c) => c.cik === cik)) || null;
  }

  function setWatchlist(companies, message) {
    state.watchlist = companies;
    state.watchStatus = companies.length ? "ok" : "empty";
    state.storageAvailable = W.save(companies) && state.storageAvailable;
    if (message) announce(message);
    if (state.view === "watchlist") {
      resetData();
      safeRender();
      refresh();
    } else {
      safeRender();
    }
  }

  function toggleFollow(cik, follow) {
    const current = following(); // a change drops saved companies the site no longer offers
    const next = follow ? [...current.filter((c) => c !== cik), cik] : current.filter((c) => c !== cik);
    const company = companyOf(cik);
    const name = company ? `${company.symbol} (${company.name})` : "The company";
    setWatchlist(next, `${name} ${follow ? "added to" : "removed from"} your watchlist.`);
  }

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

  // The current view's filters go to the API: the server filters before paging.
  function query(extra, filters = true) {
    const p = new URLSearchParams(extra || {});
    if (state.sample) p.set("include_sample", "true");
    if (filters && state.view === "company" && state.company) p.set("symbol", state.company);
    if (filters && state.view === "watchlist") for (const cik of following()) p.append("company", cik);
    return p.toString();
  }

  // The number of stories first seen now (none on the first load after a filter change).
  function countNew(items) {
    const ids = items.map((e) => e.event_id);
    if (!state.seen) {
      state.seen = new Set(ids);
      return 0;
    }
    const fresh = ids.filter((id) => !state.seen.has(id));
    fresh.forEach((id) => state.seen.add(id));
    return fresh.length;
  }

  function unique(items, idOf) {
    const seen = new Set();
    return items.filter((i) => !seen.has(idOf(i)) && seen.add(idOf(i)));
  }

  async function loadStatus(signal, gen) {
    const status = await getJson(`/api/status?${new URLSearchParams(state.sample ? { include_sample: "true" } : {})}`, signal);
    if (gen === state.gen) state.status = status;
  }

  // Never fails the refresh: the line about what is being processed is extra.
  async function loadActivity(signal) {
    try {
      state.activity = await getJson("/api/activity", signal);
    } catch (_) {
      if (!signal.aborted) state.activity = null;
    }
  }

  // Never fails the refresh: without the list the page still shows the news, and says the list is missing.
  async function loadCoverage(signal) {
    if (state.coverage && Date.now() - state.coverageAt < COVERAGE_MS) return;
    try {
      const data = await getJson("/api/coverage", signal);
      state.coverage = { companies: (data.companies || []).filter((c) => W.isCik(c.cik)),
                         collecting: data.collecting_symbols || null };
      state.coverageError = null;
      state.coverageAt = Date.now();
    } catch (error) {
      if (!signal.aborted) state.coverageError = describe(error);
    }
  }

  async function loadEvents(signal, gen) {
    if (state.view === "watchlist" && !following().length) {
      state.events.items = []; // nothing followed: no request, the page asks the reader to choose companies
      state.events.error = null;
      return;
    }
    try {
      const data = await getJson(`/api/events?${query({ limit: state.events.limit })}`, signal);
      if (gen !== state.gen) return;
      const items = unique(data.events, (e) => e.event_id);
      const fresh = countNew(items);
      state.events.items = items;
      state.events.error = null;
      if (fresh) announce(`${R.plural(fresh, "new story", "new stories")}.`);
    } catch (error) {
      if (gen === state.gen && !signal.aborted) state.events.error = describe(error);
      throw error;
    }
  }

  async function loadDetails(signal, gen) {
    const listed = new Set((state.events.items || []).map((e) => e.event_id));
    const ids = [...state.sourcesOpen].filter((id) => listed.has(id) || (id === state.revealEvent && !state.revealQuiet));
    let failure = null;
    for (let i = 0; i < ids.length; i += 4) { // a few at a time, never a burst
      await Promise.all(ids.slice(i, i + 4).map(async (id) => {
        try {
          const detail = await getJson(`/api/events/${encodeURIComponent(id)}?${query({}, false)}`, signal);
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
    const controller = new AbortController();
    poll.controller = controller;
    const current = () => gen === state.gen && !controller.signal.aborted;
    // Draw each part as soon as its own response is in: a slow endpoint never holds back the others.
    const drawn = (job) => job.finally(() => { if (current()) safeRender(); });
    setBusy(true);
    const watchlistView = state.view === "watchlist";
    const jobs = [
      drawn(loadStatus(controller.signal, gen)),
      drawn(loadActivity(controller.signal)),
      drawn((async () => {
        // The watchlist is checked against the company list before it is used as a filter.
        if (watchlistView) await loadCoverage(controller.signal);
        let failure = null;
        try {
          await loadEvents(controller.signal, gen);
        } catch (error) {
          failure = error;
        }
        await loadDetails(controller.signal, gen);
        if (failure) throw failure;
      })()),
    ];
    if (!watchlistView) jobs.push(drawn(loadCoverage(controller.signal)));
    let results = [];
    try {
      results = await Promise.allSettled(jobs);
    } finally {
      poll.inFlight = false;
      poll.controller = null;
      setBusy(false);
    }
    if (current()) {
      const failed = results.find((r) => r.status === "rejected");
      if (failed) {
        if (!poll.failures) announce("Refresh failed. The page keeps showing the news it already has.");
        poll.failures += 1;
        poll.error = describe(failed.reason);
        poll.lastFailAt = new Date();
      } else {
        if (poll.failures) announce("Connection restored. The news is up to date.");
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

  // A filter change: stop waiting for the running cycle (its answers no longer matter) and run a new one.
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
    renderStatus();
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
    if (el.matches("[data-focus]")) return JSON.stringify(["focus", el.dataset.focus]);
    if (el.matches("a[href]")) return JSON.stringify(["a", el.getAttribute("href"), el.textContent]);
    return null;
  }

  function findFocusKey(slot, key) {
    const [kind, a, b] = JSON.parse(key);
    if (kind === "summary") {
      return [...slot.querySelectorAll("details[data-key] > summary")].find((s) => s.parentElement.dataset.key === a);
    }
    if (kind === "focus") return [...slot.querySelectorAll("[data-focus]")].find((x) => x.dataset.focus === a);
    return [...slot.querySelectorAll("a[href]")].find((x) => x.getAttribute("href") === a && x.textContent === b);
  }

  const slotOf = (card, name) => card.querySelector(`:scope > [data-slot="${name}"]`);

  // Replace an element's content only when it changed, keeping open <details> and focus.
  function setSlotEl(slot, html) {
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

  function setSlot(card, name, html) {
    setSlotEl(slotOf(card, name), html);
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

  function setBusy(busy) {
    $("events-list").setAttribute("aria-busy", String(busy));
  }

  function announce(text) {
    const el = $("announcer");
    el.textContent = "";
    setTimeout(() => { el.textContent = text; }, 50);
  }

  // ------------------------------------------------------------------ rendering
  function ctx() {
    const s = state.status;
    const c = (s && s.collector) || {};
    const summaries = c.state === "running" && c.processing ? c.processing.summaries
      : s && s.features && s.features.summary_enabled;
    return { summariesEnabled: summaries, company: state.view === "company" ? state.company : "",
             followed: new Set(following()) };
  }

  // When the collectors are not running, refreshing cannot bring new stories: say so in plain words.
  function collectionNote() {
    const c = state.status && state.status.collector;
    if (!c) return "";
    if (c.state === "stopped" || c.state === "not_running") {
      return "New stories are not being collected right now, so this feed may be out of date.";
    }
    if (c.state === "stale") return "News collection may have stopped, so this feed may be out of date.";
    return "";
  }

  function renderStatus() {
    const el = $("refresh-status");
    const now = new Date();
    let cls = "";
    let html;
    if (poll.error && poll.lastOk) {
      cls = " status-warn";
      html = `${R.icon("warn")}Could not refresh · last updated ${R.esc(R.ago(poll.lastOk, now))}`;
    } else if (poll.error) {
      cls = " status-error";
      html = `${R.icon("error")}Could not load the news`;
    } else if (poll.lastOk) {
      html = `Updated ${R.esc(R.ago(poll.lastOk, now))}`;
    } else {
      html = "Loading…";
    }
    el.className = `refresh-status${cls}`;
    setHtml(el, html);
    el.title = poll.lastOk ? `Last updated at ${poll.lastOk.toLocaleTimeString()}` : "";
    const working = poll.error ? "" : R.activityLine(state.activity);
    $("activity-line").hidden = !working;
    setHtml($("activity-line"), working);
    const note = poll.error ? "" : collectionNote();
    $("feed-note").hidden = !note;
    setHtml($("feed-note"), note ? `${R.icon("info")}${R.esc(note)}` : "");
    $("refresh-alert").hidden = !poll.error;
    if (poll.error) {
      setHtml($("refresh-alert-text"), `${R.icon("warn")}<strong>Could not refresh the news:</strong>
        ${R.esc(poll.error)}. ${poll.lastOk ? `The page keeps showing the news loaded at ${R.clock(poll.lastOk)}.` : ""}
        ${poll.nextAt ? `Next attempt at ${R.clock(poll.nextAt)}.` : ""}`);
    }
  }

  // The view switch, the one-company filter (All companies view only) and the line saying what is shown.
  function renderToolbar() {
    const followed = following();
    for (const button of document.querySelectorAll(".view-switch [data-view]")) {
      const on = button.dataset.view === "watchlist" ? state.view === "watchlist" : state.view !== "watchlist";
      button.setAttribute("aria-pressed", String(on));
    }
    setHtml($("watchlist-count"), followed.length ? `(${followed.length})` : "");
    $("company-filter").hidden = state.view === "watchlist";
    const select = $("company-select");
    if (document.activeElement !== select) { // never rebuild under the user's hands
      const followedSet = new Set(followed);
      const options = state.coverage
        ? state.coverage.companies.map((c) => ({ value: c.symbol, label: `${c.symbol} · ${c.name}${
            followedSet.has(c.cik) ? " (following)" : ""}` }))
        : ((state.status && state.status.symbols) || []).filter((s) => SYMBOL.test(s)).map((s) => ({ value: s, label: s }));
      if (state.company && !options.some((o) => o.value === state.company)) {
        options.push({ value: state.company, label: state.company });
      }
      setHtml(select, `<option value="">All companies</option>` +
        options.map((o) => `<option value="${R.esc(o.value)}">${R.esc(o.label)}</option>`).join(""));
      select.value = state.view === "company" ? state.company : "";
    }
    let note = "";
    if (state.view === "watchlist" && followed.length) {
      const chips = followed.map(companyOf).filter(Boolean).map((c) =>
        `<span class="ticker followed" title="${R.esc(c.name)}"><span aria-hidden="true">★</span>${R.esc(c.symbol)}</span>`);
      note = `<span>Showing stories about the companies you follow${chips.length ? ":" : ""}</span>${chips.length
        ? `<span class="tickers">${chips.join("")}</span>` : ""}<button type="button" class="link-button"
        data-open-follow>Edit watchlist</button>`;
    } else if (state.view === "company" && state.company) {
      note = `Showing stories about ${R.esc(state.company)} only.`;
    }
    const view = $("view-note");
    view.hidden = !note;
    setSlotEl(view, note);
    const label = followed.length ? `Following ${followed.length}` : "Follow companies";
    if ($("follow-toggle-label").textContent !== label) $("follow-toggle-label").textContent = label;
    $("follow-toggle").querySelector(".icon").textContent = followed.length ? "★" : "☆";
    $("follow-toggle").setAttribute("aria-expanded", String(state.followOpen));
  }

  // The follow panel. Checkbox states are set after drawing, so ticking one never redraws the list (or moves focus).
  function renderFollow() {
    const panel = $("follow-panel");
    panel.hidden = !state.followOpen;
    if (!state.followOpen) return;
    const followed = new Set(following());
    const parts = [];
    if (!state.storageAvailable) {
      parts.push(R.notice("warn", "This browser is not saving site data", "<p>Your watchlist works on this page, " +
        "but it will be forgotten when you leave or reload it.</p>"));
    }
    if (state.watchStatus === "invalid" || state.watchStatus === "repaired") {
      parts.push(`<p class="state-note">Part of the watchlist saved earlier in this browser could not be read, so it
        was ignored.</p>`);
    }
    const ignored = ignoredSaved();
    if (ignored.length) {
      parts.push(`<p class="state-note">${R.esc(R.plural(ignored.length, "company"))} you followed earlier ${
        ignored.length === 1 ? "is" : "are"} no longer offered here and ${ignored.length === 1 ? "is" : "are"} ignored.
        ${ignored.length === 1 ? "It is" : "They are"} removed from this browser the next time you change your
        watchlist.</p>`);
    }
    if (!state.coverage) {
      parts.push(state.coverageError
        ? `${R.loadError("the list of companies", state.coverageError, false)}
           <p><button type="button" class="button secondary" data-retry-coverage>Try again</button></p>`
        : `<p class="muted" role="status">Loading companies…</p>`);
    } else if (!state.coverage.companies.length) {
      parts.push(`<p class="state-note">No companies are available to follow yet.</p>`);
    } else {
      const notCollected = state.coverage.companies.some((c) => c.collecting === false);
      parts.push(`<fieldset class="follow-fieldset"><legend class="sr-only">Companies to follow</legend>
        <ul class="follow-list">${state.coverage.companies.map((c) => `<li><label class="follow-item">
          <input type="checkbox" data-cik="${R.esc(c.cik)}" data-focus="follow-${R.esc(c.cik)}">
          <span class="ticker">${R.esc(c.symbol)}</span><span class="follow-text"><span class="follow-name">${
          R.esc(c.name)}</span>${c.collecting === false ? `<span class="follow-flag">Not collected now</span>` : ""}</span>
          </label></li>`).join("")}
        </ul></fieldset>${notCollected ? `<p class="muted small">“Not collected now”: the news collectors are not
        configured for this company, so only stories already stored can be shown.</p>` : ""}`);
    }
    const actions = [`<button type="button" class="button" data-follow-done>Done</button>`];
    if (state.view !== "watchlist" && followed.size) {
      actions.push(`<button type="button" class="button secondary" data-view="watchlist">Show my watchlist</button>`);
    }
    if (followed.size) actions.push(`<button type="button" class="link-button" data-follow-clear>Unfollow all</button>`);
    parts.push(`<div class="follow-actions">${actions.join("")}</div>`);
    setSlotEl($("follow-body"), parts.join(""));
    for (const box of $("follow-body").querySelectorAll("input[data-cik]")) {
      box.checked = followed.has(box.dataset.cik);
    }
  }

  function storyHref(e) {
    return `/events/${encodeURIComponent(e.event_id)}${e.is_sample ? "?sample=1" : ""}#from=${
      encodeURIComponent(feedHash())}`;
  }

  function createEventCard(id) {
    const safe = R.esc(id);
    const el = document.createElement("article");
    el.className = "card news-card";
    el.dataset.id = id;
    el.id = `event-${id}`;
    el.setAttribute("aria-labelledby", `event-${id}-title`);
    el.innerHTML = `<div data-slot="head"></div><div data-slot="conflict"></div>
      <div class="summary-preview" data-slot="preview"></div>
      <div class="summary-full" id="event-${safe}-summary" data-slot="full" hidden></div>
      <div class="card-actions">
        <button type="button" class="toggle" data-action="summary" aria-expanded="false"
          aria-controls="event-${safe}-summary" aria-describedby="event-${safe}-title">
          <span class="label">Read more</span><span class="chev" aria-hidden="true"></span></button>
        <button type="button" class="toggle" data-action="sources" aria-expanded="false"
          aria-controls="event-${safe}-sources" aria-describedby="event-${safe}-title">
          <span class="label">Sources</span><span class="chev" aria-hidden="true"></span></button>
        <a class="story-link" data-story-link href="/events/${safe}" aria-describedby="event-${safe}-title">Story
          &amp; timeline<span class="arrow" aria-hidden="true">→</span></a>
      </div>
      <div class="sources-panel" id="event-${safe}-sources" data-slot="sources" hidden></div>`;
    return el;
  }

  function setToggle(button, expanded, label) {
    button.setAttribute("aria-expanded", String(expanded));
    const text = button.querySelector(".label");
    if (text.textContent !== label) text.textContent = label;
  }

  function detailHtml(id) {
    const entry = state.details.get(id);
    if (!entry) return `<p class="muted small" role="status">Loading sources…</p>`;
    if (entry.gone) {
      return `<p class="state-note">This story is no longer available. It may have been merged into another story.</p>`;
    }
    if (!entry.data) return R.loadError("the sources", entry.error || "unknown error");
    return R.sourcesPanel(entry.data);
  }

  function updateEventCard(el, e) {
    const id = e.event_id;
    const c = ctx();
    const view = R.summaryView(e, c);
    setSlot(el, "head", R.cardHead(e, c));
    setSlot(el, "conflict", R.conflictLine(e));
    setSlot(el, "preview", view.preview);
    setSlot(el, "full", view.full);
    const reading = view.more && state.readMore.has(id);
    slotOf(el, "preview").hidden = reading;
    slotOf(el, "full").hidden = !reading;
    const more = el.querySelector('.card-actions [data-action="summary"]');
    more.hidden = !view.more;
    setToggle(more, reading, reading ? "Show less" : "Read more");
    const open = state.sourcesOpen.has(id);
    for (const button of el.querySelectorAll('[data-action="sources"]')) {
      button.setAttribute("aria-expanded", String(open));
    }
    setToggle(el.querySelector('.card-actions [data-action="sources"]'), open, `Sources (${e.documents})`);
    const link = el.querySelector("[data-story-link]");
    const href = storyHref(e);
    if (link.getAttribute("href") !== href) link.setAttribute("href", href);
    slotOf(el, "sources").hidden = !open;
    if (open) setSlot(el, "sources", detailHtml(id));
  }

  function renderFeed() {
    const events = state.events;
    const list = $("events-list");
    if (state.view === "watchlist" && !following().length) {
      setHtml($("events-state"), R.watchlistEmpty(ignoredSaved().length));
      list.replaceChildren();
      setHtml($("events-footer"), "");
      document.body.dataset.eventsReady = "empty-watchlist";
      return;
    }
    if (events.items === null) {
      setHtml($("events-state"), events.error ? R.loadError("the news", events.error)
        : `<div class="state" role="status">Loading news…</div>`);
      list.replaceChildren();
      setHtml($("events-footer"), "");
      return;
    }
    const label = state.view === "watchlist" ? "the companies you follow" : state.view === "company" ? state.company : "";
    setHtml($("events-state"), events.items.length ? "" : R.feedEmpty(state.status, label));
    patchList(list, events.items, (e) => e.event_id, createEventCard, updateEventCard);
    // For automated checks: "list" once stories are shown, "detail" once an open Sources panel has its data.
    document.body.dataset.eventsReady = [...state.sourcesOpen].some((id) => (state.details.get(id) || {}).data)
      ? "detail" : "list";
    document.body.dataset.view = state.view;
    const full = events.items.length >= events.limit;
    setHtml($("events-footer"), full && events.limit < MAX_EVENTS
      ? `<button type="button" class="button secondary" id="events-more">Show more news</button>` : "");
    revealPending();
  }

  function render() {
    renderStatus();
    renderToolbar();
    renderFollow();
    $("sample-banner").hidden = !state.sample;
    renderFeed();
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
      card.scrollIntoView({ block: "start" }); // the user asked for this story
      const title = card.querySelector(".card-title");
      if (title) {
        title.tabIndex = -1;
        title.focus({ preventScroll: true });
      }
      if (state.revealQuiet) writeHash(); // #story did its job; keep the address clean
    } else if (state.events.limit < MAX_EVENTS && state.events.items.length >= state.events.limit) {
      state.events.limit = MAX_EVENTS; // look further back once
      poll.again = true;
    } else {
      state.revealEvent = null;
      if (state.revealQuiet) writeHash();
      else announce("That story is not in the list of recent news.");
    }
  }

  // ------------------------------------------------------------------ interaction
  function resetData() {
    state.gen += 1;
    state.events = { items: null, limit: PAGE, error: null };
    state.seen = null;
    state.details.clear();
    supersede(); // a cycle for the old filters may be running: cancel it and follow with one for the new ones
  }

  // #view=watchlist | #company=NVDA, &sample=1, &event=<id> | &story=<id>; missing keys are defaults.
  // Older keys (tab, source) are ignored. Returns the story to open or scroll to.
  function readHash() {
    const p = new URLSearchParams(location.hash.replace(/^#/, ""));
    state.sample = p.get("sample") === "1";
    const view = p.get("view");
    const company = (p.get("company") || "").toUpperCase();
    state.company = "";
    if (view === "watchlist") {
      state.view = "watchlist";
    } else if (company && SYMBOL.test(company)) {
      state.view = "company";
      state.company = company;
    } else if (view === "all" || p.has("company")) {
      state.view = "all";
    } else {
      state.view = W.loadView() === "watchlist" ? "watchlist" : "all"; // the last view chosen in this browser
    }
    const event = p.get("event");
    const story = p.get("story");
    if (event && UUID.test(event)) return { id: event, open: true };
    if (story && UUID.test(story)) return { id: story, open: false };
    return null;
  }

  function feedHash() {
    const p = new URLSearchParams();
    if (state.view === "watchlist") p.set("view", "watchlist");
    else if (state.view === "company" && state.company) p.set("company", state.company);
    if (state.sample) p.set("sample", "1");
    return p.toString();
  }

  function writeHash(extra) {
    const hash = [feedHash(), extra || ""].filter(Boolean).join("&");
    history.replaceState(null, "", hash ? `#${hash}` : location.pathname + location.search);
  }

  function openEvent(target) {
    if (!target || !UUID.test(target.id)) return;
    if (target.open) state.sourcesOpen.add(target.id);
    state.revealEvent = target.id;
    state.revealQuiet = !target.open;
    safeRender();
    refresh();
  }

  function setView(view, company) {
    state.view = view;
    state.company = view === "company" ? company : "";
    W.saveView(view === "watchlist" ? "watchlist" : "all");
    resetData();
    writeHash();
    safeRender();
    refresh();
  }

  function setFollowOpen(open, focusFirst) {
    state.followOpen = open;
    safeRender();
    if (open && focusFirst) {
      const first = $("follow-body").querySelector("input[data-cik], button");
      $("follow-panel").scrollIntoView({ block: "nearest" });
      if (first) first.focus();
    }
    if (open && !state.coverage) refresh(); // the company list is part of every refresh cycle
  }

  function toggle(set, id) {
    if (set.has(id)) set.delete(id);
    else set.add(id);
  }

  $("events-list").addEventListener("click", (event) => {
    const link = event.target.closest("a[data-story-link]");
    if (link) {
      // Leaving for the story page: mark this story in the feed's own address, so Back returns to it.
      if (!event.defaultPrevented && event.button === 0 && !event.ctrlKey && !event.metaKey && !event.shiftKey
          && !event.altKey) {
        writeHash(`story=${encodeURIComponent(link.closest(".news-card").dataset.id)}`);
      }
      return;
    }
    const button = event.target.closest("button[data-action]");
    if (!button) return;
    const card = button.closest(".news-card");
    const id = card.dataset.id;
    const item = (state.events.items || []).find((e) => e.event_id === id);
    if (button.dataset.action === "summary") {
      toggle(state.readMore, id);
      if (item) updateEventCard(card, item);
      // Collapsing a long summary can move the button above the screen: keep it in view.
      if (!state.readMore.has(id)) button.scrollIntoView({ block: "nearest" });
    } else {
      toggle(state.sourcesOpen, id);
      if (item) updateEventCard(card, item);
      if (state.sourcesOpen.has(id)) refresh(); // loads (or refreshes) the sources; queued if a cycle is running
    }
  });

  document.addEventListener("click", (event) => {
    const target = event.target.closest("button");
    if (!target) return;
    if (target.id === "events-more") {
      state.events.limit = Math.min(state.events.limit + PAGE, MAX_EVENTS);
      state.seen = null; // older stories loaded now are not "new"
      refresh();
    } else if (target.id === "retry-now") {
      refresh();
    } else if (target.id === "follow-toggle") {
      setFollowOpen(!state.followOpen, false);
    } else if (target.hasAttribute("data-view")) {
      if (target.closest("#follow-panel")) state.followOpen = false;
      if (target.dataset.view === "watchlist" ? state.view !== "watchlist" : state.view !== "all") {
        setView(target.dataset.view);
      } else {
        safeRender();
      }
    } else if (target.hasAttribute("data-open-follow")) {
      setFollowOpen(true, true);
    } else if (target.hasAttribute("data-follow-done")) {
      setFollowOpen(false);
      $("follow-toggle").focus();
    } else if (target.hasAttribute("data-follow-clear")) {
      setWatchlist([], "You no longer follow any companies.");
    } else if (target.hasAttribute("data-retry-coverage")) {
      state.coverageError = null;
      state.coverageAt = 0;
      safeRender();
      refresh();
    }
  });

  $("follow-body").addEventListener("change", (event) => {
    const box = event.target.closest("input[data-cik]");
    if (box && W.isCik(box.dataset.cik)) toggleFollow(box.dataset.cik, box.checked);
  });

  $("follow-panel").addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setFollowOpen(false);
      $("follow-toggle").focus();
    }
  });

  $("company-select").addEventListener("change", (event) => {
    const value = event.target.value;
    setView(value ? "company" : "all", value);
  });

  // Another tab changed the watchlist: follow it here too.
  window.addEventListener("storage", (event) => {
    if (event.key !== W.KEY) return;
    const latest = W.load();
    state.watchlist = latest.companies;
    state.watchStatus = latest.status;
    if (state.view === "watchlist") {
      resetData();
      safeRender();
      refresh();
    } else {
      safeRender();
    }
  });

  window.addEventListener("hashchange", () => {
    if (location.hash === "#main") return; // the skip link, not a view change
    const target = readHash();
    resetData();
    writeHash();
    safeRender();
    if (target) openEvent(target);
    else refresh();
  });

  // Back from a story page with the page restored from the browser's cache: nothing reloads, so take the story
  // marker from the address here, and bring the feed up to date.
  window.addEventListener("pageshow", (event) => {
    if (!event.persisted) return;
    const latest = W.load();
    state.watchlist = latest.companies;
    state.watchStatus = latest.status;
    const target = readHash();
    if (target) openEvent(target);
    else refresh();
  });

  setInterval(() => { if (!document.hidden) renderStatus(); }, CLOCK_MS);

  // ------------------------------------------------------------------ start
  const initial = readHash();
  writeHash(initial && !initial.open ? `story=${initial.id}` : "");
  safeRender();
  if (initial) openEvent(initial);
  else refresh();
})();
