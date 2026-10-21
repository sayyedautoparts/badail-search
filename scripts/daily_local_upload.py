# -*- coding: utf-8 -*-
"""
رفع محلي كل 10 أيام من G:\\ملفاتي إلى قاعدة بيانات الإنتاج (Neon):
  1) مجلد قطع سيارات              → منتجات
  2) مجلد قطع فسب                  → منتجات
  3) مجلد الشغل\\الطلبيات (+ ملف) → منتجات
  4) ملف الشغل\\قشطان.xlsx         → قشطان

يشغَّل على جهاز ويندوز الذي فيه قرص G: (لا يعمل على Vercel).
"""
from __future__ import annotations

import argparse
import os
import platform
import socket
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}
DEFAULT_BASE = r"G:\ملفاتي"


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


def _ensure_db_env() -> None:
    for candidate in (
        ROOT / "scripts" / "daily_upload.local.env",
        ROOT / ".env",
        ROOT / ".env.local",
    ):
        _load_env_file(candidate)
    db = (
        os.getenv("DATABASE_URL")
        or os.getenv("NEON_DATABASE_URL")
        or os.getenv("SUPABASE_DB_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
        or ""
    ).strip()
    if not db.startswith("postgres"):
        raise SystemExit(
            "DATABASE_URL / NEON_DATABASE_URL غير مضبوط.\n"
            "ضع الرابط في scripts/daily_upload.local.env مثل:\n"
            "DATABASE_URL=postgresql://..."
        )


def _machine_identity() -> dict[str, str]:
    host = (socket.gethostname() or "").strip()
    comp = (os.getenv("COMPUTERNAME") or platform.node() or host or "").strip()
    user = (os.getenv("USERNAME") or os.getenv("USER") or "").strip()
    return {"hostname": host, "computername": comp, "username": user}


def _host_allowed(log) -> bool:
    """
    إن وُجد BADAIL_UPLOAD_HOSTS: يسمح فقط إذا طابق أحد التوكنات
    اسم الجهاز أو المستخدم (جزئياً، بدون حساسية لحالة الأحرف).
    مثال: Asim,sayyed
    """
    raw = (os.getenv("BADAIL_UPLOAD_HOSTS") or "").strip()
    if not raw:
        return True
    tokens = [t.strip().lower() for t in raw.replace(";", ",").split(",") if t.strip()]
    if not tokens:
        return True
    ident = _machine_identity()
    hay = " | ".join(ident.values()).lower()
    log(f"هوية الجهاز: hostname={ident['hostname']} user={ident['username']}")
    for tok in tokens:
        if tok and tok in hay:
            log(f"مسموح بالرفع على هذا الجهاز (طابق: {tok})")
            return True
    log(
        "تخطي الرفع بدون رفع أي ملف: هذا الجهاز غير مسموح في BADAIL_UPLOAD_HOSTS="
        + raw
        + f" | الحالي={hay}"
        + " — أضف اسم الجهاز أو المستخدم إلى BADAIL_UPLOAD_HOSTS في daily_upload.local.env"
    )
    return False


def _resolve_base(cli_base: str | None) -> Path:
    env_base = (os.getenv("BADAIL_EXCEL_BASE") or "").strip()
    if cli_base and cli_base != DEFAULT_BASE:
        return Path(cli_base)
    if env_base:
        return Path(env_base)
    if cli_base:
        return Path(cli_base)
    return Path(DEFAULT_BASE)


def _iter_excel(folder: Path) -> list[Path]:
    files: list[Path] = []
    if not folder.is_dir():
        return files
    for p in folder.rglob("*"):
        if p.is_file() and p.suffix.lower() in EXCEL_EXTS and not p.name.startswith("~$"):
            files.append(p)
    files.sort(key=lambda x: str(x).lower())
    return files


def _source_key(folder_label: str, folder_root: Path, file_path: Path) -> str:
    rel = file_path.relative_to(folder_root).as_posix()
    return f"{folder_label}/{rel}"


def _process_product(app, content: bytes, source_key: str) -> tuple[str, int, int]:
    content_hash, file_size = app.file_fingerprint(content)
    if app.is_same_uploaded_file(source_key, content_hash, file_size):
        return "skipped", 0, 0
    rows, items = app.process_excel_file(content, source_key, content_hash, file_size)
    return "ok", int(rows), int(items)


def _process_wiper(app, content: bytes, source_key: str) -> tuple[str, int]:
    content_hash, file_size = app.file_fingerprint(content)
    if app.is_same_wiper_upload(source_key, content_hash, file_size):
        return "skipped", 0
    n = app.process_wiper_excel_file(content, source_key, content_hash, file_size)
    return "ok", int(n)


def _upload_folder_as_products(
    app,
    folder: Path,
    folder_label: str,
    log,
    *,
    dry_run: bool = False,
) -> dict:
    stats = {"files": 0, "ok": 0, "skipped": 0, "failed": 0, "rows": 0, "items": 0}
    files = _iter_excel(folder)
    log(f"[{folder_label}] ملفات Excel: {len(files)} — {folder}")
    for i, fp in enumerate(files, 1):
        stats["files"] += 1
        source_key = _source_key(folder_label, folder, fp)
        size_mb = fp.stat().st_size / (1024 * 1024)
        log(f"  ({i}/{len(files)}) {source_key} ({size_mb:.1f} MB)")
        if dry_run:
            continue
        try:
            content = fp.read_bytes()
            status, rows, items = _process_product(app, content, source_key)
            if status == "skipped":
                stats["skipped"] += 1
                log("    → تخطي (بدون تغيير)")
            else:
                stats["ok"] += 1
                stats["rows"] += rows
                stats["items"] += items
                log(f"    → تم: {rows} صف / {items} صنف")
        except Exception as exc:
            stats["failed"] += 1
            log(f"    → فشل: {exc}")
            log(traceback.format_exc(limit=4))
    return stats


def _upload_single_product_file(
    app,
    path: Path,
    source_key: str,
    log,
    *,
    dry_run: bool = False,
) -> dict:
    stats = {"files": 1, "ok": 0, "skipped": 0, "failed": 0, "rows": 0, "items": 0}
    if not path.is_file():
        stats["failed"] = 1
        log(f"[ملف] غير موجود: {path}")
        return stats
    size_mb = path.stat().st_size / (1024 * 1024)
    log(f"[ملف] {source_key} ({size_mb:.1f} MB) — {path}")
    if dry_run:
        return stats
    try:
        content = path.read_bytes()
        status, rows, items = _process_product(app, content, source_key)
        if status == "skipped":
            stats["skipped"] = 1
            log("    → تخطي (بدون تغيير)")
        else:
            stats["ok"] = 1
            stats["rows"] = rows
            stats["items"] = items
            log(f"    → تم: {rows} صف / {items} صنف")
    except Exception as exc:
        stats["failed"] = 1
        log(f"    → فشل: {exc}")
        log(traceback.format_exc(limit=4))
    return stats


def _upload_wiper_file(app, path: Path, source_key: str, log, *, dry_run: bool = False) -> dict:
    stats = {"files": 1, "ok": 0, "skipped": 0, "failed": 0, "rows": 0}
    if not path.is_file():
        stats["failed"] = 1
        log(f"[قشطان] الملف غير موجود: {path}")
        return stats
    size_mb = path.stat().st_size / (1024 * 1024)
    log(f"[قشطان] {source_key} ({size_mb:.1f} MB) — {path}")
    if dry_run:
        return stats
    try:
        content = path.read_bytes()
        status, rows = _process_wiper(app, content, source_key)
        if status == "skipped":
            stats["skipped"] = 1
            log("    → تخطي (بدون تغيير)")
        else:
            stats["ok"] = 1
            stats["rows"] = rows
            log(f"    → تم: {rows} صف قشطان")
    except Exception as exc:
        stats["failed"] = 1
        log(f"    → فشل: {exc}")
        log(traceback.format_exc(limit=4))
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily local Excel upload to production DB")
    parser.add_argument(
        "--base",
        default=DEFAULT_BASE,
        help="المجلد الأب (افتراضي G:\\ملفاتي أو BADAIL_EXCEL_BASE)",
    )
    parser.add_argument("--dry-run", action="store_true", help="عرض الملفات فقط دون رفع")
    parser.add_argument(
        "--log-dir",
        default=str(ROOT / "scripts" / "logs"),
        help="مجلد سجلات التشغيل",
    )
    parser.add_argument(
        "--force-host",
        action="store_true",
        help="تجاهل قيد BADAIL_UPLOAD_HOSTS لهذا التشغيل",
    )
    args = parser.parse_args()

    # حمّل env مبكراً لقراءة BADAIL_* حتى في dry-run
    for candidate in (
        ROOT / "scripts" / "daily_upload.local.env",
        ROOT / ".env",
        ROOT / ".env.local",
    ):
        _load_env_file(candidate)

    base = _resolve_base(args.base)
    cars = base / "قطع سيارات"
    fsb = base / "قطع فسب"
    orders_dir = base / "الشغل" / "الطلبيات"
    orders_file = base / "الشغل" / "الطلبيات.xlsx"
    wiper = base / "الشغل" / "قشطان.xlsx"

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"daily_upload_{stamp}.log"

    def log(msg: str) -> None:
        line = f"{datetime.now().strftime('%H:%M:%S')} {msg}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    t0 = time.perf_counter()
    ident = _machine_identity()
    log("=== بدء الرفع اليومي ===")
    log(f"base={base}")
    log(f"computer={ident['computername']} user={ident['username']}")

    if not args.force_host and not _host_allowed(log):
        log(f"السجل: {log_path}")
        return 0

    if not base.is_dir():
        log(f"المجلد الأساس غير موجود: {base}")
        return 1

    if not args.dry_run:
        _ensure_db_env()
        import app as app_mod  # noqa: WPS433 — بعد ضبط env
    else:
        app_mod = None  # type: ignore

    s1 = _upload_folder_as_products(app_mod, cars, "قطع سيارات", log, dry_run=args.dry_run)
    s2 = _upload_folder_as_products(app_mod, fsb, "قطع فسب", log, dry_run=args.dry_run)
    s_ord_dir = _upload_folder_as_products(
        app_mod, orders_dir, "الطلبيات", log, dry_run=args.dry_run
    )
    s_ord_file = (
        _upload_single_product_file(
            app_mod, orders_file, "الطلبيات/الطلبيات.xlsx", log, dry_run=args.dry_run
        )
        if orders_file.is_file()
        else {"files": 0, "ok": 0, "skipped": 0, "failed": 0, "rows": 0, "items": 0}
    )
    s3 = _upload_wiper_file(app_mod, wiper, "قشطان.xlsx", log, dry_run=args.dry_run)

    elapsed = int(time.perf_counter() - t0)
    failed = (
        s1["failed"]
        + s2["failed"]
        + s_ord_dir["failed"]
        + s_ord_file["failed"]
        + s3["failed"]
    )
    log(
        "=== انتهى === "
        f"سيارات ok={s1['ok']} skip={s1['skipped']} fail={s1['failed']} | "
        f"فسب ok={s2['ok']} skip={s2['skipped']} fail={s2['failed']} | "
        f"طلبيات ok={s_ord_dir['ok'] + s_ord_file['ok']} "
        f"skip={s_ord_dir['skipped'] + s_ord_file['skipped']} "
        f"fail={s_ord_dir['failed'] + s_ord_file['failed']} | "
        f"قشطان ok={s3['ok']} skip={s3['skipped']} fail={s3['failed']} | "
        f"{elapsed}s"
    )
    log(f"السجل: {log_path}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
