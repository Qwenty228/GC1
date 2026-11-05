// Stock News story page (/events/<event_id>): one story's summary, conflict status, timeline, sources and quotes.
//
// GET-only like the feed: reading this page never calls a model or starts a collector. The address is stable:
// /events/<id>, plus ?sample=1 for a fictional sample story (they are hidden otherwise). The feed adds
// #from=<its own view> to its links, so "Back to the news" returns to the same view (watchlist, one company,
// sample mode) and scrolls back to this story; a direct load goes back to the feed's default view.
// Companies the reader follows are marked from the watchlist saved in this browser (watchlist.js).

(function () {
  "use strict";

  const R = window.StockNewsRender;
  const W = window.StockNewsWatchlist;
  const F = window.StockNewsFilters;
  const T = window.StockNewsTour;
  const $ = (id) => document.getElementById(id);
  const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const SYMBOL = /^[A-Z0-9.\-]{1,12}$/;
  const REQUEST_TIMEOUT_MS = 15000;

  let raw = location.pathname.replace(/^\/events\//, "").replace(/\/+$/, "");
  try {
    raw = decodeURIComponent(raw);
  } catch (_) { /* a malformed escape is an invalid link, reported below */ }
  const eventId = UUID.test(raw) ? raw.toLowerCase() : null;
  const sample = new URLSearchParams(location.search).get("sample") === "1";
  const from = readFrom();
  const state = { detail: null, timeline: null, first: true };

  // Only the feed's own view and filter keys are carried back (validated); anything else in the address is ignored.
  function readFrom() {
    const p = new URLSearchParams(location.hash.replace(/^#/, ""));
    if (!p.has("from")) return null;
    const feed = new URLSearchParams(p.get("from") || "");
    const back = new URLSearchParams();
    const company = (feed.get("company") || "").toUpperCase();
    if (feed.get("view") === "watchlist") back.set("view", "watchlist");
    else if (SYMBOL.test(company)) back.set("company", company);
    F.write(F.read(feed), back);
    if (feed.get("sample") === "1") back.set("sample", "1");
    return back;
  }

  function backHref() {
    if (!from) return "/";
    const p = new URLSearchParams(from);
    if (eventId) p.set("story", eventId);
    return `/#${p.toString()}`;
  }

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
      if (!response.ok || !data || data.error) {
        const error = new Error((data && data.error) || `HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      return data;
    } finally {
      clearTimeout(timer);
    }
  }

  function describe(error) {
    if (error && (error.name === "AbortError" || error.name === "TimeoutError")) return "the server did not answer in time";
    if (error instanceof TypeError) return "the server could not be reached";
    return (error && error.message) || "unknown error";
  }

  function show(html, storyState) {
    const story = $("story");
    story.innerHTML = html;
    story.setAttribute("aria-busy", "false");
    document.body.dataset.storyState = storyState;
  }

  function ctx() {
    return { followed: new Set(W.load().companies) };
  }

  function timelineState() {
    return state.timeline && !state.timeline.error ? "ready" : state.timeline ? "partial" : "loading";
  }

  function render() {
    const d = state.detail;
    show(R.storyPage(d, state.timeline, ctx()), timelineState());
    document.title = `${(d.summary && d.summary.headline) || d.title || "Story"} · StockEvent Lens`;
    if (state.first) {
      state.first = false;
      const title = $("story-title");
      if (title) title.focus({ preventScroll: true }); // a new page: start reading at its headline
    }
  }

  // Only the timeline slot changes when the timeline arrives: focus and reading position stay where they are.
  function renderTimeline() {
    const slot = $("timeline-slot");
    if (!slot) return;
    slot.innerHTML = R.timelineBlock(state.timeline);
    document.body.dataset.storyState = timelineState();
  }

  async function loadTimeline() {
    const query = sample ? "?include_sample=true" : "";
    try {
      state.timeline = await getJson(`/api/events/${encodeURIComponent(eventId)}/timeline${query}`);
    } catch (error) {
      state.timeline = { error: describe(error) };
    }
  }

  async function load() {
    $("sample-banner").hidden = !sample;
    $("back-link").setAttribute("href", backHref());
    if (!eventId) {
      show(`<div class="state"><h2>This is not a valid story link</h2><p>Story addresses look like
        <code>/events/&lt;story id&gt;</code>. <a href="/">Go to the news</a>.</p></div>`, "invalid");
      return;
    }
    $("story").setAttribute("aria-busy", "true");
    $("story").innerHTML = R.storySkeleton(); // also when trying again after an error
    document.body.dataset.storyState = "loading";
    const query = sample ? "?include_sample=true" : "";
    state.timeline = null;
    const timeline = loadTimeline(); // in parallel with the story itself
    try {
      state.detail = await getJson(`/api/events/${encodeURIComponent(eventId)}${query}`);
    } catch (error) {
      if (error.status === 404) {
        show(`<div class="state"><h2>Story not found</h2><p>There is no story at this address. The link may be
          incomplete, or the story is no longer available.</p>${sample ? "" : `<p class="small">Fictional sample
          stories only open with <code>?sample=1</code> at the end of the address.</p>`}
          <p><a href="${R.esc(backHref())}">Back to the news</a></p></div>`, "not_found");
      } else {
        show(`${R.loadError("the story", describe(error), false)}<p><button type="button" class="button secondary"
          data-retry>Try again</button></p>`, "error");
      }
      return;
    }
    render(); // the story first; the timeline fills its slot when it arrives
    await timeline;
    renderTimeline();
  }

  document.addEventListener("click", async (event) => {
    const tour = event.target.closest("#tour-button");
    if (tour) {
      T.start(T.tours.story, { opener: tour, name: "story" });
      return;
    }
    if (event.target.closest("[data-retry]")) {
      load();
      return;
    }
    if (event.target.closest("[data-retry-timeline]")) {
      state.timeline = null;
      renderTimeline();
      await loadTimeline();
      renderTimeline();
      return;
    }
    const scroll = event.target.closest("[data-scroll-to]");
    if (scroll) {
      const target = $(scroll.dataset.scrollTo);
      if (target) {
        target.scrollIntoView({ block: "start" });
        target.tabIndex = -1;
        target.focus({ preventScroll: true });
      }
      return;
    }
    const back = event.target.closest("#back-link");
    if (back && from && event.button === 0 && !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey) {
      // Came straight from the feed: going back restores it as it was (its address already marks this story).
      let referrer = null;
      try {
        referrer = document.referrer ? new URL(document.referrer) : null;
      } catch (_) {
        referrer = null;
      }
      if (referrer && referrer.origin === location.origin && referrer.pathname === "/" && history.length > 1) {
        event.preventDefault();
        history.back();
      }
    }
  });

  load();
})();
