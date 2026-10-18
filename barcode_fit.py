"""
حساب أبعاد Code128 لطباعة اللزقات (Rongta 203dpi).
Code128 فقط — حد أدنى مفضّل وحدتان (0.25مم) للقراءة.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

PRINT_BARCODE_CFG = {
    "dpi": 203,
    "label_w_mm": 50.0,
    "label_h_mm": 25.0,
    "margin_mm": 1.0,
    "min_module_mm": 0.25,
    "max_module_mm": 0.5,
    "min_module_dots": 2,
    "max_module_dots": 3,
    "bar_h_min_mm": 8.0,
    "bar_h_max_mm": 12.0,
    "quiet_modules_each": 10,
    "text_reserve_mm": 2.9,
}


def barcode_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return "".join(ch for ch in str(value) if ch not in "\r\n")


def code128_modules(n: int) -> int:
    if n < 0:
        n = 0
    return 11 * (n + 2) + 13 + 20


@dataclass(frozen=True)
class BarcodeFitResult:
    ok: bool
    mode: str
    value: str
    n: int
    modules: int
    module_mm: float
    module_dots: int
    bar_hmm: float
    symbol_w_mm: float
    symbol_h_mm: float
    available_mm: float
    warning: str = ""


def compute_barcode_fit(
    value: Optional[str],
    *,
    label_w_mm: Optional[float] = None,
    label_h_mm: Optional[float] = None,
    margin_mm: Optional[float] = None,
    cfg: Optional[dict] = None,
) -> BarcodeFitResult:
    c = dict(PRINT_BARCODE_CFG)
    if cfg:
        c.update(cfg)
    lw = float(label_w_mm if label_w_mm is not None else c["label_w_mm"])
    margin = float(margin_mm if margin_mm is not None else c["margin_mm"])
    text = barcode_text(value)
    n = len(text)
    modules = code128_modules(n)
    avail = lw - 2 * margin
    dots_per_mm = float(c["dpi"]) / 25.4
    min_dots = int(c["min_module_dots"])
    max_dots = int(c["max_module_dots"])

    if not text or not any(ch.isalnum() for ch in text):
        return BarcodeFitResult(
            False, "fail", text, n, modules, 0, 0, 0, 0, 0, avail, "قيمة غير صالحة"
        )

    avail_dots = int(avail * dots_per_mm)
    module_dots = avail_dots // modules if modules else 0
    if module_dots > max_dots:
        module_dots = max_dots
    if module_dots >= min_dots:
        module_mm = module_dots / dots_per_mm
        bar_h = float(c["bar_h_min_mm"])
        return BarcodeFitResult(
            True,
            "horizontal",
            text,
            n,
            modules,
            round(module_mm, 6),
            module_dots,
            bar_h,
            round(modules * module_mm, 4),
            bar_h,
            round(avail, 4),
            "",
        )

    # لا يتسع بوحدة 2 — نبلّغ (الرسم في الواجهة قد يجرّب وحدة 1 بحذر)
    need = modules * float(c["min_module_mm"])
    return BarcodeFitResult(
        False,
        "fail",
        text,
        n,
        modules,
        0,
        0,
        0,
        0,
        0,
        round(avail, 4),
        f"يحتاج ≈{need:.1f} مم بوحدة قابلة للمسح على عرض اللزقة",
    )
