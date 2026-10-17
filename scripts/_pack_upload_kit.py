# -*- coding: utf-8 -*-
"""Build one-click kit inside G:\\ملفاتي\\مدير الطلبيات"""
from __future__ import annotations

import shutil
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = Path(r"G:\ملفاتي")
DEST = BASE / "مدير الطلبيات"
KIT = DEST / "_رفع_تلقائي"
PY_VER = "3.12.7"
PY_ZIP_URL = f"https://www.python.org/ftp/python/{PY_VER}/python-{PY_VER}-embed-amd64.zip"
BOM = b"\xef\xbb\xbf"


def write_bom_text(path: Path, text: str) -> None:
    path.write_bytes(BOM + text.encode("utf-8"))


def write_bat(path: Path, lines: list[str]) -> None:
    path.write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")


def copy_ps1(src: Path, dst: Path) -> None:
    data = src.read_bytes()
    if data.startswith(BOM):
        data = data[len(BOM) :]
    dst.write_bytes(BOM + data)


def _download(url: str, dest: Path) -> None:
    import ssl
    import subprocess

    dest.parent.mkdir(parents=True, exist_ok=True)
    r = subprocess.run(
        ["curl.exe", "-L", "--fail", "-o", str(dest), url],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0 and dest.is_file() and dest.stat().st_size > 1_000_000:
        return
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(url, context=ctx) as resp, dest.open("wb") as fh:
        shutil.copyfileobj(resp, fh)


def ensure_portable_python(runtime: Path) -> None:
    exe = runtime / "python.exe"
    if exe.is_file():
        print("portable python already present")
        return
    runtime.mkdir(parents=True, exist_ok=True)
    zpath = ROOT / "scripts" / f"_cache_python-{PY_VER}-embed-amd64.zip"
    if not zpath.is_file() or zpath.stat().st_size < 1_000_000:
        print("Downloading portable Python...")
        _download(PY_ZIP_URL, zpath)
    print("Extracting portable Python...")
    with zipfile.ZipFile(zpath, "r") as zf:
        zf.extractall(runtime)
    # enable site
    for pth in runtime.glob("python*._pth"):
        lines = pth.read_text(encoding="utf-8", errors="replace").splitlines()
        out = []
        for line in lines:
            if line.strip().startswith("#") and "import site" in line:
                out.append("import site")
            else:
                out.append(line)
        if "import site" not in out:
            out.append("import site")
        pth.write_text("\n".join(out) + "\n", encoding="ascii")
    print("portable python ready:", exe)


def main() -> None:
    if not DEST.is_dir():
        raise SystemExit(f"missing {DEST}")
    if KIT.exists():
        shutil.rmtree(KIT)
    (KIT / "scripts" / "logs").mkdir(parents=True, exist_ok=True)

    files = [
        "scripts/daily_local_upload.py",
        "scripts/run_daily_upload.ps1",
        "scripts/install_daily_upload_task.ps1",
        "scripts/setup_upload_on_this_pc.ps1",
        "scripts/bootstrap_all.ps1",
        "scripts/daily_upload.local.env",
        "requirements.txt",
        "app.py",
        "hesabate_store_api.py",
    ]
    for rel in files:
        src = ROOT / rel
        dst = KIT / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if rel.endswith(".ps1"):
            copy_ps1(src, dst)
        else:
            shutil.copy2(src, dst)
        print("copied", rel)

    # env keys
    envp = KIT / "scripts" / "daily_upload.local.env"
    out: list[str] = []
    seen_base = seen_hosts = False
    for line in envp.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s.startswith("BADAIL_EXCEL_BASE"):
            if seen_base:
                continue
            out.append(r"BADAIL_EXCEL_BASE=G:\ملفاتي")
            seen_base = True
            continue
        if s.startswith("BADAIL_UPLOAD_HOSTS"):
            if seen_hosts:
                continue
            out.append("BADAIL_UPLOAD_HOSTS=")
            seen_hosts = True
            continue
        out.append(line)
    if not seen_base:
        out.append(r"BADAIL_EXCEL_BASE=G:\ملفاتي")
    if not seen_hosts:
        out.append("BADAIL_UPLOAD_HOSTS=")
    write_bom_text(envp, "\n".join(out) + "\n")

    ensure_portable_python(KIT / "runtime" / "python")

    # Single button the user should press
    write_bat(
        DEST / "اضغط هنا - نفّذ كل شيء.bat",
        [
            "@echo off",
            "chcp 65001 >nul",
            'cd /d "%~dp0_رفع_تلقائي"',
            "echo ============================================",
            "echo   ONE CLICK: Python + setup + upload",
            "echo ============================================",
            "echo.",
            'powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\\bootstrap_all.ps1"',
            "echo.",
            "pause",
        ],
    )

    # keep short aliases
    write_bat(
        DEST / "اضغط هنا - تفعيل الرفع التلقائي.bat",
        [
            "@echo off",
            "chcp 65001 >nul",
            'cd /d "%~dp0_رفع_تلقائي"',
            'powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\\bootstrap_all.ps1"',
            "pause",
        ],
    )
    write_bat(
        DEST / "اضغط هنا - رفع الآن.bat",
        [
            "@echo off",
            "chcp 65001 >nul",
            'cd /d "%~dp0_رفع_تلقائي"',
            'powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\\run_daily_upload.ps1"',
            "pause",
        ],
    )

    # remove old launchers from base root if any
    for p in (
        BASE / "اضغط هنا - تفعيل الرفع التلقائي.bat",
        BASE / "اضغط هنا - رفع الآن.bat",
        BASE / "_رفع_تلقائي",
    ):
        try:
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p)
        except Exception:
            pass

    print("READY:", DEST / "اضغط هنا - نفّذ كل شيء.bat")


if __name__ == "__main__":
    main()
