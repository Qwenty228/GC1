"""A minimal Chrome DevTools Protocol driver for headless Edge / Chrome, for page tests (no Node or Playwright).

The browser runs with a throwaway profile and background traffic switched off. It is driven over the DevTools
WebSocket on loopback, so the tests' no-outbound-socket guard still holds. Requests matching ``fail_requests``
patterns are failed by the driver itself (to show the page's error states).
"""

from __future__ import annotations

import itertools
import json
import shutil
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

CANDIDATES = (r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
              r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
              r"C:\Program Files\Google\Chrome\Application\chrome.exe")


def find_browser() -> str | None:
    found = [shutil.which(n) for n in ("msedge", "chrome", "chromium", "google-chrome")] + list(CANDIDATES)
    return next((c for c in found if c and Path(c).exists()), None)


class PageError(AssertionError):
    pass


class Browser:
    def __init__(self, executable: str):
        from websockets.sync.client import connect

        self._profile = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)  # never the user's profile
        self.proc = subprocess.Popen(
            [executable, "--headless=new", "--disable-gpu", "--no-first-run", "--no-default-browser-check",
             "--disable-extensions", "--disable-background-networking", "--disable-component-update", "--disable-sync",
             "--disable-default-apps", "--no-pings", "--disable-domain-reliability", "--remote-debugging-port=0",
             f"--user-data-dir={self._profile.name}", "about:blank"],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        port_file = Path(self._profile.name) / "DevToolsActivePort"
        deadline = time.monotonic() + 30
        while not (port_file.exists() and port_file.read_text().strip()):
            if time.monotonic() > deadline or self.proc.poll() is not None:
                self.close()
                raise RuntimeError("the browser did not open its DevTools port")
            time.sleep(0.1)
        port = int(port_file.read_text().splitlines()[0])
        page = None
        while page is None and time.monotonic() < deadline:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/list", timeout=5) as response:
                page = next((t for t in json.load(response) if t.get("type") == "page"), None)
            if page is None:
                time.sleep(0.1)
        if page is None:
            self.close()
            raise RuntimeError("the browser has no page target")
        self.ws = connect(page["webSocketDebuggerUrl"], max_size=None, open_timeout=10)
        self._ids = itertools.count(1)
        self._failing = False
        self.send("Page.enable")
        self.send("Runtime.enable")

    # ------------------------------------------------------------------ protocol
    def _handle_event(self, message: dict) -> None:
        if message.get("method") == "Fetch.requestPaused":
            request_id = message["params"]["requestId"]
            if self._failing:
                self._post("Fetch.failRequest", requestId=request_id, errorReason="ConnectionRefused")
            else:
                self._post("Fetch.continueRequest", requestId=request_id)

    def _post(self, method: str, **params) -> int:
        message_id = next(self._ids)
        self.ws.send(json.dumps({"id": message_id, "method": method, "params": params}))
        return message_id

    def send(self, method: str, **params) -> dict:
        message_id = self._post(method, **params)
        while True:
            message = json.loads(self.ws.recv(timeout=30))
            if message.get("id") == message_id:
                if "error" in message:
                    raise PageError(f"{method}: {message['error']}")
                return message.get("result", {})
            self._handle_event(message)

    def evaluate(self, expression: str):
        result = self.send("Runtime.evaluate", expression=expression, returnByValue=True, awaitPromise=True)
        if "exceptionDetails" in result:
            raise PageError(f"script error in {expression!r}: {result['exceptionDetails']}")
        return result["result"].get("value")

    # ------------------------------------------------------------------ helpers
    def goto(self, url: str) -> None:
        self.send("Page.navigate", url=url)

    def reload(self) -> None:
        self.send("Page.reload", ignoreCache=True)

    def wait_for(self, expression: str, timeout: float = 20.0):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = self.evaluate(expression)
            except PageError:
                last = None  # e.g. the page is navigating
            if last:
                return last
            time.sleep(0.1)
        raise PageError(f"timed out waiting for {expression!r} (last value {last!r}); page text: "
                        f"{self.evaluate('document.body ? document.body.innerText.slice(0, 600) : null')!r}")

    def fail_requests(self, url_pattern: str | None) -> None:
        """Fail every request matching the pattern (None: stop)."""
        if url_pattern is None:
            self._failing = False
            self.send("Fetch.disable")
        else:
            self._failing = True
            self.send("Fetch.enable", patterns=[{"urlPattern": url_pattern, "requestStage": "Request"}])

    def close(self) -> None:
        try:
            if getattr(self, "ws", None) is not None:
                self.ws.close()
        finally:
            if self.proc.poll() is None:
                self.proc.kill()
                try:
                    self.proc.wait(10)
                except subprocess.TimeoutExpired:
                    pass
            self._profile.cleanup()
