// The reader's watchlist, saved in THIS browser only (localStorage). It is not an account setting and does not
// follow the reader to another browser or device. Companies are stored by SEC CIK, which does not change when a
// ticker does. Nothing here talks to the server: the page sends the CIKs as a filter, and the server only reads.
//
// Stored value (key "stocknews.watchlist"): {"version": 1, "companies": ["0001045810", ...], "saved_at": "<ISO>"}.
// Anything else — malformed JSON, another version, non-CIK entries — is read as an empty or partial list and
// reported, never thrown; the stored value is only replaced when the reader changes the watchlist. Storage that
// is blocked (private windows, disabled site data) makes the watchlist last for this page view only.

(function (root) {
  "use strict";

  const KEY = "stocknews.watchlist";
  const VIEW_KEY = "stocknews.view";
  const VERSION = 1;
  const MAX = 50;
  const CIK = /^\d{1,10}$/;

  function storage() {
    try {
      const s = root.localStorage;
      const probe = "stocknews.probe";
      s.setItem(probe, "1");
      s.removeItem(probe);
      return s;
    } catch (_) {
      return null;
    }
  }

  // {companies, status, dropped}: status is "empty", "ok", "repaired" (some entries ignored) or "invalid".
  function parse(raw) {
    if (raw === null || raw === undefined || raw === "") return { companies: [], status: "empty", dropped: 0 };
    let data;
    try {
      data = JSON.parse(raw);
    } catch (_) {
      return { companies: [], status: "invalid", dropped: 0 };
    }
    if (!data || typeof data !== "object" || data.version !== VERSION || !Array.isArray(data.companies)) {
      return { companies: [], status: "invalid", dropped: 0 };
    }
    const companies = [];
    let dropped = 0;
    for (const item of data.companies) {
      const cik = typeof item === "string" ? item.trim() : typeof item === "number" ? String(item) : "";
      if (!CIK.test(cik)) {
        dropped += 1;
        continue;
      }
      const padded = cik.padStart(10, "0");
      if (!companies.includes(padded)) companies.push(padded);
    }
    if (companies.length > MAX) {
      dropped += companies.length - MAX;
      companies.length = MAX;
    }
    return { companies, status: dropped ? "repaired" : companies.length ? "ok" : "empty", dropped };
  }

  // {companies, status, dropped, available}: `available` is false when the browser blocks storage.
  function load() {
    const s = storage();
    if (!s) return { companies: [], status: "empty", dropped: 0, available: false };
    let raw = null;
    try {
      raw = s.getItem(KEY);
    } catch (_) {
      return { companies: [], status: "empty", dropped: 0, available: false };
    }
    return { ...parse(raw), available: true };
  }

  // Writes the list (already validated by the caller); returns false when the browser refuses.
  function save(companies) {
    const s = storage();
    if (!s) return false;
    try {
      s.setItem(KEY, JSON.stringify({ version: VERSION, companies: companies.slice(0, MAX),
                                      saved_at: new Date().toISOString() }));
      return true;
    } catch (_) {
      return false;
    }
  }

  // Split a stored list into companies the site still offers and ones it no longer lists.
  function sanitize(companies, offered) {
    const known = new Set(offered);
    return { active: companies.filter((c) => known.has(c)), unknown: companies.filter((c) => !known.has(c)) };
  }

  function loadView() {
    const s = storage();
    try {
      const value = s && s.getItem(VIEW_KEY);
      return value === "watchlist" || value === "all" ? value : null;
    } catch (_) {
      return null;
    }
  }

  function saveView(view) {
    const s = storage();
    try {
      if (s && (view === "watchlist" || view === "all")) s.setItem(VIEW_KEY, view);
    } catch (_) { /* a preference only */ }
  }

  const api = { KEY, VIEW_KEY, VERSION, MAX, parse, load, save, sanitize, loadView, saveView,
                isCik: (v) => typeof v === "string" && /^\d{10}$/.test(v) };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else root.StockNewsWatchlist = api;
})(this);
