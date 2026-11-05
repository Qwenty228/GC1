// "New" and "Updated" since the reader's last visit, remembered in THIS browser only (localStorage), like the
// watchlist. Nothing is sent to the server.
//
// Stored value (key "stocknews.visits"): {"version": 1, "since": "<ISO>" | null, "last_seen": "<ISO>"}.
//   last_seen  refreshed while a feed page is open and visible
//   since      the baseline for the markers: the end of the previous visit. A visit ends after 30 minutes with
//              no open feed page, so reloading or coming back from a story keeps the same markers. "Mark all as
//              read" moves it to now. The first visit ever has no baseline and shows no markers.
// A story is "new" when its first report is after the baseline and "updated" when only a later report is. The times
// are the sources' own report times, so a report collected late, after the reader left, is not marked.

(function (root) {
  "use strict";

  const KEY = "stocknews.visits";
  const VERSION = 1;
  const GAP_MS = 30 * 60 * 1000;

  function storage() {
    try {
      return root.localStorage;
    } catch (_) {
      return null;
    }
  }

  function time(value) {
    const t = typeof value === "string" ? Date.parse(value) : NaN;
    return Number.isNaN(t) ? null : t;
  }

  function read() {
    const s = storage();
    try {
      const data = JSON.parse((s && s.getItem(KEY)) || "null");
      if (!data || data.version !== VERSION) return null;
      return { since: time(data.since), lastSeen: time(data.last_seen) };
    } catch (_) {
      return null;
    }
  }

  function write(since, lastSeen) {
    const s = storage();
    try {
      if (s) s.setItem(KEY, JSON.stringify({ version: VERSION, since: since === null ? null : new Date(since).toISOString(),
                                             last_seen: new Date(lastSeen).toISOString() }));
    } catch (_) { /* markers are a convenience: without storage they are simply not shown */ }
  }

  // Starts (or continues) a visit; returns the baseline in ms, or null when there is none.
  function start(now) {
    const saved = read();
    let since = null;
    if (saved && saved.lastSeen !== null) {
      since = now - saved.lastSeen > GAP_MS ? saved.lastSeen : saved.since;
    }
    write(since, now);
    return since;
  }

  // The page is still open: extend this visit. Returns the stored baseline (another tab may have changed it).
  function touch(now) {
    const saved = read();
    const since = saved ? saved.since : null;
    write(since, now);
    return since;
  }

  // Everything shown is read: the baseline is now, or the newest report shown if this browser's clock is behind.
  function markRead(now, events) {
    const newest = Math.max(now, ...(events || []).map((e) => time(e.last_source_time) || 0));
    write(newest, now);
    return newest;
  }

  // "new" | "updated" | "" for an event, against the baseline (ms or null).
  function freshness(event, since) {
    if (since === null || since === undefined) return "";
    const first = time(event.first_source_time);
    const last = time(event.last_source_time);
    if (first !== null && first > since) return "new";
    if (last !== null && last > since) return "updated";
    return "";
  }

  const api = { KEY, VERSION, GAP_MS, start, touch, markRead, freshness, read };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.StockNewsVisits = api;
})(this);
