"""
حساب أبعاد Code128 لطباعة اللزقات (Rongta 203dpi).

لا يُضغط الرمز كصورة. X-dimension عدد صحيح من نقاط الطابعة.
Quiet Zone = 10 وحدات من كل جهة.
الحد الموثوق للمسح الحراري: نقطتان (≈0.25مم). نقطة واحدة = تحذير فقط.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

PRINT_BARCODE_CFG = {
    "dpi": 203,
    "label_w_mm": 50.0,
    "label_h_mm": 26.0,
    "sheet_w_mm": 105.0,
    "sheet_h_mm": 28.0,
    "gap_mm": 3.0,
    "margin_mm": 1.0,
    "min_module_mm": 0.25,  # 2 نقاط @203dpi — حد المسح الموثوق
    "max_module_mm": 0.375,  # 3 نقاط
    "min_module_dots": 2,
    "absolute_min_module_dots": 1,
    "max_module_dots": 3,
    "bar_h_min_mm": 8.0,
    "bar_h_max_mm": 12.0,
    "quiet_modules_each": 10,
    "text_reserve_mm": 2.4,
}


def barcode_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return "".join(ch for ch in str(value) if ch not in "\r\n")


def code128_symbol_modules(text: str) -> int:
    """وحدات الرمز بدون Quiet Zone (start + data + check + stop)."""
    if not text:
        return 11 * 2 + 13
    if all(ch.isdigit() for ch in text):
        data_symbols = (len(text) // 2) + (len(text) % 2)
        return 11 * (data_symbols + 2) + 13
    n = len(text)
    return 11 * (n + 2) + 13


def code128_modules(n: int) -> int:
    """أسوأ حالة Code128B + Quiet 10× يمين/يسار (للتوافق مع الاختبارات القديمة)."""
    if n < 0:
        n = 0
    return 11 * (n + 2) + 13 + 20


def dots_per_mm(dpi: float) -> float:
    return float(dpi) / 25.4


def mm_from_dots(dots: int, dpi: float) -> float:
    return dots / dots_per_mm(dpi)


def css_px_as_mm_96dpi(dots: int) -> float:
    """لو عُرضت نقاط الكانفاس كـ HTML width بالبكسل عند 96dpi — هذا التشويه الممنوع."""
    return dots * 25.4 / 96.0


def sheet_geometry_ok(
    sheet_w: float = 105.0,
    sheet_h: float = 28.0,
    label_w: float = 50.0,
    label_h: float = 26.0,
    gap: float = 3.0,
    margin: float = 1.0,
) -> bool:
    return (
        abs((margin + label_w + gap + label_w + margin) - sheet_w) < 0.01
        and abs((margin + label_h + margin) - sheet_h) < 0.01
    )


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
    scale: float = 1.0
    css_squash: bool = False


def compute_barcode_fit(
    value: Optional[str],
    *,
    label_w_mm: Optional[float] = None,
    label_h_mm: Optional[float] = None,
    margin_mm: Optional[float] = None,
    cfg: Optional[dict] = None,
    symbol_modules: Optional[int] = None,
) -> BarcodeFitResult:
    c = dict(PRINT_BARCODE_CFG)
    if cfg:
        c.update(cfg)
    lw = float(label_w_mm if label_w_mm is not None else c["label_w_mm"])
    lh = float(label_h_mm if label_h_mm is not None else c["label_h_mm"])
    margin = float(margin_mm if margin_mm is not None else c["margin_mm"])
    text = barcode_text(value)
    n = len(text)
    quiet = int(c["quiet_modules_each"])
    body = int(symbol_modules) if symbol_modules is not None else code128_symbol_modules(text)
    modules = body + 2 * quiet
    avail = lw - 2 * margin
    dpi = float(c["dpi"])
    dpm = dots_per_mm(dpi)
    max_dots = int(c["max_module_dots"])
    min_dots = int(c["min_module_dots"])
    abs_min = int(c.get("absolute_min_module_dots", 1))
    bar_h = min(float(c["bar_h_max_mm"]), max(float(c["bar_h_min_mm"]), lh - float(c["text_reserve_mm"])))

    if not text or not any(ch.isalnum() for ch in text):
        return BarcodeFitResult(
            False, "fail", text, n, modules, 0, 0, 0, 0, 0, avail, "قيمة غير صالحة", 1.0, False
        )

    avail_dots = int(avail * dpm)
    module_dots = 0
    for d in range(max_dots, abs_min - 1, -1):
        if modules * d <= avail_dots:
            module_dots = d
            break

    if module_dots < 1:
        need = modules * (abs_min / dpm)
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
            f"لا يتسع حتى عند X={abs_min} نقطة — وسّع منطقة الباركود (يحتاج ≈{need:.1f} مم)",
            1.0,
            False,
        )

    module_mm = module_dots / dpm
    warning = ""
    ok = True
    if module_dots < min_dots:
        warning = (
            f"X={module_dots} نقطة فقط (أقل من الحد الموثوق {min_dots}). "
            "لم يُضغط كصورة. وسّع منطقة الباركود لتحسين المسح."
        )
    return BarcodeFitResult(
        ok,
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
        warning,
        1.0,
        False,
    )
