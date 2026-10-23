"""Local print agent: website -> Rongta, no Chrome print dialog."""
from __future__ import annotations

import base64
import json
import subprocess
import sys
import tempfile
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


def cors_origin(origin: str) -> str:
    if origin in ALLOWED_ORIGINS:
        return origin
    if origin.startswith("https://") and "vercel.app" in origin and "badail-search" in origin:
        return origin
    return ALLOWED_ORIGINS[0]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def _cors(self, origin: str) -> None:
        self.send_header("Access-Control-Allow-Origin", cors_origin(origin))
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Access-Control-Max-Age", "600")
        self.send_header("Vary", "Origin")

    def do_OPTIONS(self) -> None:  # noqa: N802
        origin = self.headers.get("Origin") or ""
        self.send_response(204)
        self._cors(origin)
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


def main() -> None:
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print("Rongta local print agent on http://%s:%s" % (HOST, PORT), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
