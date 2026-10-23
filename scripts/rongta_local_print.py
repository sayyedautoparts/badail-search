"""Local print agent: website -> Rongta, no Chrome print dialog."""
from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOST = "127.0.0.1"
PORT = 17891
PRINTER = "Rongta RP4xx Series"
ALLOWED_ORIGINS = (
    "https://www.sayyedstore.com",
    "https://sayyedstore.com",
    "https://badail-search.vercel.app",
    "http://127.0.0.1:8765",
    "http://localhost:8765",
    "http://127.0.0.1:8000",
    "http://localhost:8000",
)
ROOT = Path(__file__).resolve().parent
PRINT_PS1 = ROOT / "print_rongta_image.ps1"
SITES = [
    (os.environ.get("BADAIL_PRINT_SITE") or "https://www.sayyedstore.com").rstrip("/"),
    "https://sayyedstore.com",
]
PRINT_KEY = (os.environ.get("PRINT_AGENT_KEY") or "badail-rongta").strip()
LOG_PATH = Path(os.environ.get("LOCALAPPDATA") or ".") / "BadailPrint" / "agent.log"


def cors_origin(origin: str) -> str:
    if not origin:
        return "*"
    if origin in ALLOWED_ORIGINS:
        return origin
    if origin.startswith("https://") and "vercel.app" in origin and "badail-search" in origin:
        return origin
    if origin.startswith(("http://127.0.0.1", "http://localhost", "https://127.0.0.1", "https://localhost")):
        return origin
    return origin


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def _cors(self, origin: str) -> None:
        self.send_header("Access-Control-Allow-Origin", cors_origin(origin) or "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Content-Type, Access-Control-Request-Private-Network, Access-Control-Request-Local-Network",
        )
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Access-Control-Allow-Local-Network", "true")
        self.send_header("Access-Control-Max-Age", "600")
        self.send_header("Vary", "Origin")

    def do_OPTIONS(self) -> None:  # noqa: N802
        origin = self.headers.get("Origin") or ""
        self.send_response(204)
        self._cors(origin)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        origin = self.headers.get("Origin") or ""
        if self.path.split("?", 1)[0] in ("/ping", "/health"):
            body = json.dumps({"ok": True, "printer": PRINTER, "port": PORT}).encode("utf-8")
            self.send_response(200)
            self._cors(origin)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self._cors(origin)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        origin = self.headers.get("Origin") or ""
        if self.path.split("?", 1)[0] != "/print":
            self.send_response(404)
            self._cors(origin)
            self.end_headers()
            return
        try:
            n = int(self.headers.get("Content-Length") or "0")
            if n <= 0 or n > 8_000_000:
                raise ValueError("invalid body size")
            raw = self.rfile.read(n)
            payload = json.loads(raw.decode("utf-8"))
            image = str(payload.get("image") or "")
            copies = max(1, min(50, int(payload.get("copies") or 1)))
            printer = str(payload.get("printer") or PRINTER).strip() or PRINTER
            width_mm = float(payload.get("widthMm") or 105)
            height_mm = float(payload.get("heightMm") or 28)
            if "," in image:
                image = image.split(",", 1)[1]
            blob = base64.b64decode(image)
            print_png_blob(blob, copies, printer, width_mm, height_mm)
            out = {"ok": True, "copies": copies, "printer": printer}
            code = 200
        except Exception as exc:
            out = {"ok": False, "message": str(exc)}
            code = 500
        body = json.dumps(out, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors(origin)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _log(msg: str) -> None:
    line = "[%s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), msg)
    try:
        print(line, flush=True)
    except Exception:
        pass
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def print_png_blob(blob: bytes, copies: int, printer: str, width_mm: float, height_mm: float) -> None:
    if len(blob) < 32:
        raise ValueError("empty image")
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    tmp.write(blob)
    tmp.close()
    cmd = [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(PRINT_PS1),
        "-ImagePath",
        tmp.name,
        "-PrinterName",
        printer,
        "-Copies",
        str(copies),
        "-WidthMm",
        str(width_mm),
        "-HeightMm",
        str(height_mm),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    try:
        Path(tmp.name).unlink()
    except OSError:
        pass
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "print failed").strip()
        raise RuntimeError(err[-800:])


def _site_json(method: str, path: str, body: dict | None = None, timeout: int = 20) -> dict:
    last_err = None
    payload = json.dumps(body).encode("utf-8") if body is not None else None
    curl = Path(os.environ.get("SystemRoot") or r"C:\Windows") / "System32" / "curl.exe"
    for site in SITES:
        url = site + path
        try:
            if curl.exists():
                cmd = [
                    str(curl), "-sS", "-m", str(timeout),
                    "-H", "X-Print-Key: %s" % PRINT_KEY,
                    "-H", "Accept: application/json",
                    "-X", method,
                ]
                if payload is not None:
                    cmd.extend(["-H", "Content-Type: application/json", "--data-binary", "@-"])
                cmd.append(url)
                run_kw = {"capture_output": True, "text": True, "timeout": timeout + 5}
                if payload is not None:
                    run_kw["input"] = payload.decode("utf-8")
                proc = subprocess.run(cmd, **run_kw)
                if proc.returncode != 0:
                    raise RuntimeError((proc.stderr or proc.stdout or "curl fail").strip()[:300])
                raw = proc.stdout or ""
                return json.loads(raw) if raw.strip() else {}
            headers = {"X-Print-Key": PRINT_KEY, "Accept": "application/json"}
            if payload is not None:
                headers["Content-Type"] = "application/json"
            req = urllib.request.Request(url, data=payload, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except Exception as exc:
            last_err = exc
            continue
    raise RuntimeError(str(last_err or "print site unreachable"))


def poll_cloud_jobs() -> None:
    _log("polling print jobs from %s" % SITES[0])
    while True:
        try:
            data = _site_json("GET", "/print_jobs/next")
            job = (data or {}).get("job")
            if not job:
                time.sleep(2)
                continue
            job_id = int(job.get("id") or 0)
            copies = max(1, min(50, int(job.get("copies") or 1)))
            width_mm = float(job.get("widthMm") or 105)
            height_mm = float(job.get("heightMm") or 28)
            image = str(job.get("image") or "")
            blob = base64.b64decode(image)
            try:
                print_png_blob(blob, copies, PRINTER, width_mm, height_mm)
                _site_json("POST", "/print_jobs/ack", {"id": job_id, "ok": True})
                _log("printed cloud job %s x%s" % (job_id, copies))
            except Exception as exc:
                try:
                    _site_json("POST", "/print_jobs/ack", {"id": job_id, "ok": False, "message": str(exc)[:400]})
                except Exception:
                    pass
                _log("cloud job %s failed: %s" % (job_id, exc))
        except Exception as exc:
            _log("poll error: %s" % exc)
            time.sleep(4)


def main() -> None:
    poller = threading.Thread(target=poll_cloud_jobs, name="rongta-cloud-poll", daemon=True)
    poller.start()
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    _log("Rongta local print agent on http://%s:%s" % (HOST, PORT))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
