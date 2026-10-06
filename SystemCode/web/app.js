// Stock News reader: one feed of AI news summaries read from the local API, kept current by polling.
//
// Polling is GET-only. The API never calls a model and never starts a collector, so reading,
// expanding or refreshing (in any number of tabs) cannot trigger ingestion, grouping or summaries.
// One refresh cycle runs at a time (a request made meanwhile is queued; a company change cancels
// the running cycle instead of waiting for it), each request has its own timeout, and each part of
// the page is drawn as soon as its own response arrives, so a slow or failing endpoint never holds
// back the others. Polling pauses while the tab is hidden, failures back off (20 s doubling to
// 5 min), and a failed refresh keeps the content already shown. Cards are keyed by their stable
// ids and patched in place, so open summaries and sources, focus and reading position survive.
//
// The view is in the address hash: #company=NVDA (empty for all companies), #event=<id> opens that
// story's sources, and #sample=1 (demos and tests only; the page has no control for it) adds the
// fictional sample stories, each marked Sample.

(function () {
  "use strict";

  const R = window.StockNewsRender;
  const $ = (id) => document.getElementById(id);
  const POLL_MS = 20000;
  const MAX_BACKOFF_MS = 300000;
  const REQUEST_TIMEOUT_MS = 15000;
  const CLOCK_MS = 30000; // redraws "Updated … ago"; no request
  const PAGE = 50;
  const MAX_EVENTS = 100; // /api/events limit
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const SYMBOL = /^[A-Z0-9.\-]{1,12}$/;

  const state = {
    sample: false,
    company: "NVDA",
    companyFromUrl: false,
    gen: 0, // bumped when filters change: responses for the old filters are dropped
    status: null,
    events: { items: null, limit: PAGE, error: null },
    details: new Map(), // event_id -> {data} | {gone: true} | {data?, error}
    sourcesOpen: new Set(), // cards whose Sources panel is open
    readMore: new Set(), // cards showing the whole summary
    seen: null, // event ids already shown, to announce new stories
    revealEvent: null, // an event to scroll to once the list has it (#event=<id>)
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

  async function loadEvents(signal, gen) {
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
    const ids = [...state.sourcesOpen].filter((id) => listed.has(id) || id === state.revealEvent);
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
    const jobs = [
      drawn(loadStatus(controller.signal, gen)),
      drawn((async () => {
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

  // Replace a slot's content only when it changed, keeping open <details> and focus.
  function setSlot(card, name, html) {
    const slot = slotOf(card, name);
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
    return { summariesEnabled: summaries, company: state.company };
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
    slotOf(el, "sources").hidden = !open;
    if (open) setSlot(el, "sources", detailHtml(id));
  }

  function renderFeed() {
    const events = state.events;
    const list = $("events-list");
    if (events.items === null) {
      setHtml($("events-state"), events.error ? R.loadError("the news", events.error)
        : `<div class="state" role="status">Loading news…</div>`);
      list.replaceChildren();
      setHtml($("events-footer"), "");
      return;
    }
    setHtml($("events-state"), events.items.length ? "" : R.feedEmpty(state.status, state.company));
    patchList(list, events.items, (e) => e.event_id, createEventCard, updateEventCard);
    // For automated checks: "list" once stories are shown, "detail" once an open Sources panel has its data.
    document.body.dataset.eventsReady = [...state.sourcesOpen].some((id) => (state.details.get(id) || {}).data)
      ? "detail" : "list";
    const full = events.items.length >= events.limit;
    setHtml($("events-footer"), full && events.limit < MAX_EVENTS
      ? `<button type="button" class="button secondary" id="events-more">Show more news</button>` : "");
    revealPending();
  }

  function render() {
    renderStatus();
    renderCompanies();
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
    } else if (state.events.limit < MAX_EVENTS && state.events.items.length >= state.events.limit) {
      state.events.limit = MAX_EVENTS; // look further back once
      poll.again = true;
    } else {
      state.revealEvent = null;
      announce("That story is not in the list of recent news.");
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

  // #company=NVDA&sample=1&event=<id>; missing keys are defaults. Older keys (tab, source) are ignored.
  function readHash() {
    const p = new URLSearchParams(location.hash.replace(/^#/, ""));
    state.sample = p.get("sample") === "1";
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
    if (state.sample) p.set("sample", "1");
    if (state.companyFromUrl || state.company !== "NVDA") p.set("company", state.company);
    const hash = p.toString();
    history.replaceState(null, "", hash ? `#${hash}` : location.pathname + location.search);
  }

  function openEvent(id) {
    if (!UUID.test(id)) return;
    state.sourcesOpen.add(id);
    state.revealEvent = id;
    safeRender();
    refresh();
  }

  function toggle(set, id) {
    if (set.has(id)) set.delete(id);
    else set.add(id);
  }

  $("events-list").addEventListener("click", (event) => {
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
    if (event.target.id === "events-more") {
      state.events.limit = Math.min(state.events.limit + PAGE, MAX_EVENTS);
      state.seen = null; // older stories loaded now are not "new"
      refresh();
    } else if (event.target.id === "retry-now") {
      refresh();
    }
  });

  $("company-select").addEventListener("change", (event) => {
    state.company = event.target.value;
    state.companyFromUrl = true;
    resetData();
    writeHash();
    safeRender();
    refresh();
  });

  window.addEventListener("hashchange", () => {
    if (location.hash === "#main") return; // the skip link, not a view change
    const event = readHash();
    resetData();
    writeHash();
    safeRender();
    if (event) openEvent(event);
    else refresh();
  });

  setInterval(() => { if (!document.hidden) renderStatus(); }, CLOCK_MS);

  // ------------------------------------------------------------------ start
  const initialEvent = readHash();
  writeHash();
  safeRender();
  if (initialEvent) openEvent(initialEvent);
  else refresh();
})();
