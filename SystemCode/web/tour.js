// The guided tour: a spotlight on one part of the page at a time, with a short explanation beside it. The feed
// opens it once by itself (the first visit in this browser, never in sample mode or when a link opens a story);
// the Tour button in the header opens it again on the feed or the story page.
//
// The tour is a modal dialog: Tab stays inside it, Esc or "Skip tour" closes it, the arrow keys step through it,
// and focus returns to the button that opened it. A step whose part of the page is not there (no stories yet, a
// story without a warning) is shown in the middle of the screen instead. Which tours were offered is remembered in
// this browser only (key "stocknews.tour"). Nothing here talks to the server.

(function (root) {
  "use strict";

  const R = root.StockNewsRender;
  const KEY = "stocknews.tour";
  const PAD = 6; // spotlight margin around the part of the page
  const GAP = 14; // between the spotlight and the step card
  const EDGE = 16; // the card's distance from the window edges
  let active = null;

  function storage() {
    try {
      return root.localStorage;
    } catch (_) {
      return null;
    }
  }

  function offered() {
    try {
      const data = JSON.parse((storage() && storage().getItem(KEY)) || "null");
      return data && data.version === 1 && Array.isArray(data.seen) ? data.seen : [];
    } catch (_) {
      return [];
    }
  }

  // True once a tour has been shown in this browser; false also when the browser cannot remember it (a tour
  // that would then open on every visit is never opened by itself).
  function seen(name) {
    return !storage() || offered().includes(name);
  }

  function markSeen(name) {
    try {
      const list = offered();
      if (!list.includes(name)) storage().setItem(KEY, JSON.stringify({ version: 1, seen: [...list, name] }));
    } catch (_) { /* not remembered: the Tour button still works */ }
  }

  const reduced = () => root.matchMedia && root.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const narrow = () => root.innerWidth <= 640;

  // A step's part of the page: one element, or several next to each other that one spotlight covers together.
  // Hidden parts of the page are not pointed at; null when none of them is on the page.
  function find(target) {
    const got = typeof target === "function" ? target() : target ? document.querySelector(target) : null;
    const els = (Array.isArray(got) ? got : [got]).filter((el) => {
      if (!el || !el.isConnected) return false;
      const r = el.getBoundingClientRect();
      return r.width || r.height;
    });
    return els.length ? els : null;
  }

  function box(els) {
    const rects = els.map((el) => el.getBoundingClientRect());
    return { top: Math.min(...rects.map((r) => r.top)), bottom: Math.max(...rects.map((r) => r.bottom)),
             left: Math.min(...rects.map((r) => r.left)), right: Math.max(...rects.map((r) => r.right)) };
  }

  const FOCUSABLE = "button, [href], input, select, textarea, [tabindex]:not([tabindex='-1'])";

  function build() {
    const wrap = document.createElement("div");
    wrap.className = "tour";
    wrap.innerHTML = `<div class="tour-spot" aria-hidden="true"></div>
      <div class="tour-card" role="dialog" aria-modal="true" aria-labelledby="tour-title" aria-describedby="tour-body">
        <div class="tour-top"><p class="tour-count" id="tour-count"></p><button type="button" class="tour-close"
          data-tour="close" aria-label="Close the tour">${R.icon("x")}</button></div>
        <h2 class="tour-title" id="tour-title" tabindex="-1"></h2>
        <div class="tour-body" id="tour-body" aria-live="polite"></div>
        <div class="tour-dots" aria-hidden="true"></div>
        <div class="tour-actions"><button type="button" class="link-button" data-tour="close">Skip tour</button>
          <span class="tour-nav"><button type="button" class="button secondary" data-tour="back">Back</button><button
            type="button" class="button" data-tour="next">Next</button></span></div>
      </div>`;
    return wrap;
  }

  // Place the spotlight on the step's part of the page and the card next to it (below, else above, else over the
  // bottom of the window); without a part, or on a phone, the card sits in the middle or at the bottom.
  function layout(wrap, el) {
    const card = wrap.querySelector(".tour-card");
    card.classList.toggle("is-sheet", Boolean(el) && narrow());
    card.classList.toggle("is-center", !el);
    wrap.classList.toggle("no-target", !el);
    return card;
  }

  function place() {
    if (!active) return;
    const { wrap, step } = active;
    const spot = wrap.querySelector(".tour-spot");
    const el = find(step.target);
    const W = root.innerWidth;
    const H = root.innerHeight;
    const card = layout(wrap, el);
    if (!el) {
      Object.assign(spot.style, { top: `${H / 2}px`, left: `${W / 2}px`, width: "0px", height: "0px" });
      Object.assign(card.style, { top: "", left: "", bottom: "" });
      return;
    }
    const r = box(el);
    const top = Math.max(r.top - PAD, -PAD);
    const bottom = Math.min(r.bottom + PAD, H + PAD);
    const left = Math.max(r.left - PAD, 4);
    const right = Math.min(r.right + PAD, W - 4);
    Object.assign(spot.style, { top: `${top}px`, left: `${left}px`, width: `${Math.max(right - left, 0)}px`,
                                height: `${Math.max(bottom - top, 0)}px` });
    if (narrow()) {
      Object.assign(card.style, { top: "", left: "", bottom: "" });
      return;
    }
    const cw = card.offsetWidth;
    const ch = card.offsetHeight;
    let y;
    if (bottom + GAP + ch <= H - EDGE) y = bottom + GAP;
    else if (top - GAP - ch >= EDGE) y = top - GAP - ch;
    else y = H - EDGE - ch; // no room beside it: over the bottom of the window
    const x = Math.min(Math.max(left, EDGE), W - EDGE - cw);
    Object.assign(card.style, { top: `${Math.max(y, EDGE)}px`, left: `${Math.max(x, EDGE)}px`, bottom: "" });
  }

  let frame = 0;
  function schedule() {
    if (!frame) frame = root.requestAnimationFrame(() => { frame = 0; place(); });
  }

  // Bring the step's part of the page into view: on a computer, the part and the card below it centred together
  // when both fit (else the part from its top); on a phone near the top, as the card covers the bottom.
  function reveal(el, cardHeight) {
    const r = box(el);
    const H = root.innerHeight;
    const pair = r.bottom - r.top + PAD * 2 + GAP + cardHeight;
    let goal;
    if (narrow()) goal = r.top - 72;
    else if (pair <= H - EDGE * 2) goal = r.top - PAD - (H - pair) / 2;
    else goal = r.top - 24;
    if (Math.abs(goal) > 8) root.scrollBy({ top: goal, behavior: reduced() ? "auto" : "smooth" });
  }

  // Follow the step's part of the page as it changes size (sources that finish loading) or moves (a story added
  // above it by the feed's own refresh).
  function watch(a, els) {
    if (a.observer) a.observer.disconnect();
    a.observer = els && root.ResizeObserver ? new root.ResizeObserver(schedule) : null;
    if (a.observer) for (const el of [...els, document.body]) a.observer.observe(el);
  }

  // A step may change the page while it is shown (`before`, e.g. open a story's sources) and put it back when it
  // is left (`after`), whether for another step or by closing the tour.
  function show(index) {
    const a = active;
    if (a.shown && a.shown.after) a.shown.after();
    a.index = index;
    a.step = a.steps[index];
    a.shown = a.step;
    const { wrap, step, steps } = a;
    if (step.before) step.before();
    const last = index === steps.length - 1;
    wrap.querySelector(".tour-count").textContent = `Step ${index + 1} of ${steps.length}`;
    wrap.querySelector(".tour-title").textContent = step.title;
    const el = find(step.target);
    wrap.querySelector(".tour-body").innerHTML = (typeof step.body === "function" ? step.body(Boolean(el)) : step.body);
    wrap.querySelector(".tour-dots").innerHTML = steps.map((_, i) =>
      `<span class="${i === index ? "is-current" : i < index ? "is-done" : ""}"></span>`).join("");
    wrap.querySelector('[data-tour="back"]').hidden = index === 0;
    wrap.querySelector('[data-tour="next"]').textContent = last ? (step.finish || "Finish") : index === 0
      ? (step.start || "Start the tour") : "Next";
    wrap.classList.add("is-moving"); // the spotlight glides to its new place
    clearTimeout(a.movingTimer);
    a.movingTimer = setTimeout(() => wrap.classList.remove("is-moving"), 320);
    watch(a, el);
    if (el) reveal(el, layout(wrap, el).offsetHeight);
    place();
    wrap.querySelector(".tour-title").focus({ preventScroll: true });
  }

  function onKey(event) {
    if (!active) return;
    const card = active.wrap.querySelector(".tour-card");
    if (event.key === "Escape") {
      event.preventDefault();
      close();
    } else if (event.key === "ArrowRight" && !event.altKey) {
      event.preventDefault();
      next();
    } else if (event.key === "ArrowLeft" && !event.altKey) {
      event.preventDefault();
      back();
    } else if (event.key === "Tab") {
      const items = [...card.querySelectorAll(FOCUSABLE)].filter((x) => !x.hidden && x.offsetParent !== null);
      if (!items.length) return;
      const first = items[0];
      const lastItem = items[items.length - 1];
      const inside = card.contains(document.activeElement);
      if (event.shiftKey && (!inside || document.activeElement === first)) {
        event.preventDefault();
        lastItem.focus();
      } else if (!event.shiftKey && (!inside || document.activeElement === lastItem)) {
        event.preventDefault();
        first.focus();
      }
    }
  }

  function onClick(event) {
    const button = event.target.closest("[data-tour]");
    if (!button) return;
    const what = button.dataset.tour;
    if (what === "close") close();
    else if (what === "next") next();
    else if (what === "back") back();
  }

  function next() {
    if (active.index >= active.steps.length - 1) close();
    else show(active.index + 1);
  }

  function back() {
    if (active.index > 0) show(active.index - 1);
  }

  function start(steps, options = {}) {
    if (active) close();
    if (!steps || !steps.length) return;
    const wrap = build();
    document.body.appendChild(wrap);
    document.body.classList.add("tour-open");
    active = { steps, wrap, index: 0, step: steps[0], shown: null, observer: null,
               opener: options.opener || document.activeElement, onClose: options.onClose, movingTimer: 0 };
    if (options.name) markSeen(options.name);
    wrap.addEventListener("click", onClick);
    document.addEventListener("keydown", onKey, true);
    root.addEventListener("resize", schedule);
    root.addEventListener("scroll", schedule, { passive: true });
    show(0);
  }

  function close() {
    if (!active) return;
    const { wrap, opener, onClose, movingTimer, shown, observer } = active;
    active = null;
    clearTimeout(movingTimer);
    if (observer) observer.disconnect();
    if (shown && shown.after) shown.after();
    document.removeEventListener("keydown", onKey, true);
    root.removeEventListener("resize", schedule);
    root.removeEventListener("scroll", schedule);
    wrap.remove();
    document.body.classList.remove("tour-open");
    const back = opener && opener.isConnected && opener !== document.body ? opener : document.getElementById("main");
    if (back) back.focus({ preventScroll: true });
    if (onClose) onClose();
  }

  // ------------------------------------------------------------------ the tours
  const first = (selector) => () => document.querySelector(selector);
  const visible = (selector) => () => {
    const el = document.querySelector(selector);
    return el && !el.hidden ? el : null;
  };

  // The "Read more" and "Sources" steps open those hidden parts of one story to show them (by clicking its buttons,
  // as a reader would) and close again only what they opened when the step is left, so the feed is as it was.
  // The story: the first one near the top whose summary is longer than its card shows, else the first one.
  const demo = { id: null, opened: [] };
  const cards = () => [...document.querySelectorAll("#events-list .news-card")];
  const cardButton = (card, action) => card && card.querySelector(`.card-actions [data-action="${action}"]`);

  function demoCard(fresh) {
    const all = cards();
    let card = !fresh && all.find((c) => c.dataset.id === demo.id);
    if (!card) {
      card = all.slice(0, 5).find((c) => !cardButton(c, "summary").hidden) || all[0] || null;
      demo.id = card ? card.dataset.id : null;
    }
    return card;
  }

  function openPart(action, fresh) {
    const card = demoCard(fresh);
    const button = cardButton(card, action);
    if (!button || button.hidden || button.getAttribute("aria-expanded") === "true") return;
    button.click();
    demo.opened.push({ id: card.dataset.id, action });
  }

  function closeOpened() {
    for (const { id, action } of demo.opened.splice(0)) {
      const button = cardButton(cards().find((c) => c.dataset.id === id), action);
      if (button && button.getAttribute("aria-expanded") === "true") button.click();
    }
  }

  const slots = (...names) => () => {
    const card = demoCard();
    return card && names.map((name) => card.querySelector(name.startsWith(".") ? name : `[data-slot="${name}"]`));
  };

  // The "Follow companies" step opens the follow panel to show it, and closes it again only if the tour opened it.
  let followOpenedByTour = false;
  const followToggle = () => document.getElementById("follow-toggle");

  function openFollow() {
    const toggle = followToggle();
    if (toggle && toggle.getAttribute("aria-expanded") !== "true") {
      toggle.click();
      followOpenedByTour = true;
    }
  }

  function closeFollow() {
    const toggle = followToggle();
    if (followOpenedByTour && toggle && toggle.getAttribute("aria-expanded") === "true") toggle.click();
    followOpenedByTour = false;
  }

  const followTargets = () => {
    const panel = visible("#follow-panel")();
    return panel ? [followToggle(), panel] : [followToggle()];
  };

  const feed = [
    {
      title: "Welcome to StockEvent Lens",
      body: `<p>StockEvent Lens groups related stock news into <strong>stories</strong>. Each story has an AI summary
        and links to the articles and SEC filings it was written from.</p><p>This short tour shows how to read the
        feed and find your way around. It takes about a minute.</p>`,
    },
    {
      title: "Each card is one story",
      target: first("#events-list .news-card"),
      body: (here) => `<p>Reports about the same event are grouped into one story. Above the headline are the
        companies it is about and the time of its latest report.</p><p>Text under the <strong>AI summary</strong>
        label was written by AI from the story's sources.${here ? "" : " Stories appear here as news is collected."}</p>`,
    },
    {
      title: "Read the whole summary",
      target: slots("preview", "full", "update", ".card-actions"), // the hidden one of preview / full drops out
      before: () => openPart("summary", true),
      after: closeOpened,
      body: (here) => {
        const full = here && demoCard().querySelector('[data-slot="full"]');
        return full && !full.hidden
          ? `<p>A card starts with the beginning of the AI summary. <strong>Read more</strong> opens all of it, as on
            this story now, and <strong>Show less</strong> folds it away again.</p>`
          : `<p>When an AI summary is longer than its card shows, <strong>Read more</strong> opens all of it and
            <strong>Show less</strong> folds it away again.${here ? " This story's summary already fits on its card."
              : ""}</p>`;
      },
    },
    {
      title: "Check the sources",
      target: slots(".card-actions", "sources"),
      before: () => openPart("sources"),
      after: closeOpened,
      body: (here) => `<p><strong>Sources</strong> ${here ? "opens this list" : "lists"}: every original article and
        filing in the story with a link, and under <strong>Supporting quotes</strong> the passage behind each
        statement in the summary.</p><p><strong>Story &amp; timeline</strong> opens the story's own page, with
        every report in the order it happened.</p>`,
    },
    {
      title: "When sources disagree",
      target: first("#events-list .conflict-line"),
      body: (here) => `<p>When reports in a story contradict each other, its card shows a warning${here
        ? "." : ` like this one:</p><p class="conflict-line tour-example">${R.icon("warn")}<strong>Conflicting
        reports—under review</strong>`}</p><p><strong>Compare the claims</strong> shows what each source says, with
        quotes. Check the originals before relying on either.</p>`,
    },
    {
      title: "Search and filter",
      target: first("#filter-form"),
      body: `<p>Search headlines, summaries, company names and tickers. Narrow the feed by date, to stories with an
        <strong>SEC filing</strong>, or to stories where <strong>sources disagree</strong>.</p><p>Your search and
        filters are kept in the page address, so a bookmark brings them back.</p>`,
    },
    {
      title: "Your watchlist",
      target: followTargets,
      before: openFollow,
      after: closeFollow,
      body: `<p>Tick the companies you care about here. <strong>My watchlist</strong> then shows only their stories;
        <strong>All companies</strong> shows every story, and the list beside it picks one company.</p><p>Your
        watchlist is saved in this browser only. There is no account.</p>`,
    },
    {
      title: "See what is new",
      target: () => visible("#fresh-note")() || document.querySelector("#events-list .day-head"),
      body: `<p>Stories are grouped by the day of their latest report. When you come back, <strong>New</strong> and
        <strong>Updated</strong> badges mark what arrived since your last visit, and <strong>Mark all as
        read</strong> clears them.</p><p>The page updates itself every 20 seconds, so there is no need to reload.</p>`,
    },
    {
      title: "You are all set",
      target: first("#tour-button"),
      finish: "Start reading",
      body: `<p>Open this tour again any time with <strong>Tour</strong>. <strong>Data tools</strong>, beside it,
        shows what the collectors stored and what is being processed right now.</p>`,
    },
  ];

  const story = [
    {
      title: "A story's own page",
      target: first("#back-link"),
      start: "Next",
      body: `<p>This page has everything about one story. <strong>Back to the news</strong> returns to the feed as
        you left it: the same view, search, filters and place in the list.</p>`,
    },
    {
      title: "The story",
      target: first(".story-head"),
      body: `<p>The headline and the companies the story is about; the ones you follow are marked with a star. A
        warning appears here when its sources disagree.</p>`,
    },
    {
      title: "The AI summary",
      target: () => { const h = document.getElementById("summary-heading"); return h && h.closest("section"); },
      body: `<p>Written by AI from the sources listed further down. Its version number goes up when new information
        changes it. Check the originals before relying on it.</p>`,
    },
    {
      title: "Timeline",
      target: () => { const h = document.getElementById("timeline-heading"); return h && h.closest("section"); },
      body: `<p>Every report in the order it happened, by each source's own publication time. A label says what each
        report added: the first report, more coverage, a substantive update or a conflicting report.</p>`,
    },
    {
      title: "Sources and evidence",
      target: () => { const h = document.getElementById("sources-heading"); return h && h.closest("section"); },
      finish: "Finish",
      body: `<p>Each original article or filing with a link, and under <strong>Supporting quotes</strong> the
        passage behind each statement in the summary.</p>`,
    },
  ];

  const api = { KEY, start, close, seen, markSeen, isOpen: () => Boolean(active), tours: { feed, story } };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.StockNewsTour = api;
})(this);
