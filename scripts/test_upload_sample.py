# -*- coding: utf-8 -*-
"""رفع تجريبي سريع: قشطان + أصغر ملفات رئيسي/فسب."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load_env() -> None:
    for envp in (ROOT / "scripts" / "daily_upload.local.env", ROOT / ".env"):
        if not envp.is_file():
            continue
        for raw in envp.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = val


def main() -> int:
    _load_env()
    import app as m

    base = Path(r"G:\ملفاتي")
    results: list[tuple] = []
    t0 = time.perf_counter()

    # 1) قشطان
    wiper = base / "الشغل" / "قشطان.xlsx"
    content = wiper.read_bytes()
    h, sz = m.file_fingerprint(content)
    n = m.process_wiper_excel_file(content, "قشطان.xlsx", h, sz)
    results.append(("قشطان.xlsx", "wiper", n))
    print(f"OK wiper قشطان.xlsx rows={n}")

    # 2) أصغر 3 ملفات قطع سيارات
    cars_root = base / "قطع سيارات"
    cars = sorted(
        [p for p in cars_root.rglob("*.xlsx") if not p.name.startswith("~$")],
        key=lambda p: p.stat().st_size,
    )[:3]
    for fp in cars:
        rel = fp.relative_to(cars_root).as_posix()
        source_key = f"قطع سيارات/{rel}"
        content = fp.read_bytes()
        h, sz = m.file_fingerprint(content)
        if m.is_same_uploaded_file(source_key, h, sz):
            print(f"SKIP {source_key}")
            results.append((source_key, "skip", 0))
            continue
        rows, items = m.process_excel_file(content, source_key, h, sz)
        print(f"OK {source_key} rows={rows} items={items}")
        results.append((source_key, "ok", rows))

    # 3) أصغر ملف قطع فسب
    fsb_root = base / "قطع فسب"
    fsb = sorted(
        [p for p in fsb_root.rglob("*.xlsx") if not p.name.startswith("~$")],
        key=lambda p: p.stat().st_size,
    )
    if fsb:
        fp = fsb[0]
        rel = fp.relative_to(fsb_root).as_posix()
        source_key = f"قطع فسب/{rel}"
        content = fp.read_bytes()
        h, sz = m.file_fingerprint(content)
        if m.is_same_uploaded_file(source_key, h, sz):
            print(f"SKIP {source_key}")
            results.append((source_key, "skip", 0))
        else:
            rows, items = m.process_excel_file(content, source_key, h, sz)
            print(f"OK {source_key} rows={rows} items={items}")
            results.append((source_key, "ok", rows))

    print(f"--- done in {int(time.perf_counter() - t0)}s ---")
    for r in results:
        print(r)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
