"""
حساب أبعاد Code128 الديناميكية لطباعة اللزقات (Rongta 203dpi).

معادلة الوحدات:
  modules = 11 * (n + 2) + 13 + 20
حيث n = طول القيمة حرفياً (شامل الفراغات)، و20 = Quiet Zone (10 لكل جهة).

Code128 فقط — بلا QR / DataMatrix.
لا تُضغط الفراغات ولا يُجرى trim — فقط حذف \\r و \\n.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# —— إعدادات مركزية (مصدر الحقيقة) ——
PRINT_BARCODE_CFG = {
    "dpi": 203,  # Rongta RP4xx Series
    "label_w_mm": 50.0,
    "label_h_mm": 25.0,
    "margin_mm": 1.5,
    "min_module_mm": 0.125,  # 1 dot @ 203dpi — آخر حل للطويل (ما زال Code128)
    "pref_min_module_mm": 0.25,  # 2 dots — المفضّل
    "max_module_mm": 0.5,  # ~4 dots
    "min_module_dots": 1,
    "pref_min_module_dots": 2,
    "max_module_dots": 4,
    "bar_h_min_mm": 10.0,
    "bar_h_max_mm": 15.0,
    "quiet_modules_each": 10,
    "text_reserve_mm": 2.9,
}


def barcode_text(value: Optional[str]) -> str:
    """القيمة المطبوعة حرفياً — احذف فقط CR/LF."""
    if value is None:
        return ""
    return "".join(ch for ch in str(value) if ch not in "\r\n")


def code128_modules(n: int) -> int:
    """modules = 11*(n+2) + 13 + 20 (Quiet Zone مضمّن)."""
    if n < 0:
        n = 0
    return 11 * (n + 2) + 13 + 20


@dataclass(frozen=True)
class BarcodeFitResult:
    ok: bool
    mode: str  # horizontal | rotated | fail
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


def _clamp_bar_h(cfg: dict, avail_for_bars_mm: float) -> float:
    lo = float(cfg["bar_h_min_mm"])
    hi = float(cfg["bar_h_max_mm"])
    if avail_for_bars_mm >= lo:
        return min(hi, avail_for_bars_mm)
    return max(5.0, min(hi, avail_for_bars_mm))


def _try_code128(
    *,
    value: str,
    n: int,
    modules: int,
    available_mm: float,
    bar_hmm: float,
    mode: str,
    cfg: dict,
    min_dots: Optional[int] = None,
) -> Optional[BarcodeFitResult]:
    dpi = float(cfg["dpi"])
    dots_per_mm = dpi / 25.4
    min_d = int(min_dots if min_dots is not None else cfg["min_module_dots"])
    max_dots = int(cfg["max_module_dots"])
    max_mm = float(cfg["max_module_mm"])

    if available_mm <= 0 or modules <= 0:
        return None

    avail_dots = int(available_mm * dots_per_mm)
    module_dots = avail_dots // modules
    if module_dots < min_d:
        return None
    if module_dots > max_dots:
        module_dots = max_dots

    module_mm = module_dots / dots_per_mm
    if module_mm > max_mm + 1e-9:
        module_dots = min(max_dots, int(max_mm * dots_per_mm + 1e-9))
        if module_dots < min_d:
            return None
        if modules * module_dots > avail_dots:
            return None
        module_mm = module_dots / dots_per_mm

    symbol_w = modules * module_mm
    warn = ""
    if module_dots < int(cfg["pref_min_module_dots"]):
        warn = "Code128 بوحدة دقيقة (1 نقطة) ليتسع — ما زال باركود خطي قابل للمسح"
    return BarcodeFitResult(
        ok=True,
        mode=mode,
        value=value,
        n=n,
        modules=modules,
        module_mm=round(module_mm, 6),
        module_dots=module_dots,
        bar_hmm=round(bar_hmm, 3),
        symbol_w_mm=round(symbol_w, 4),
        symbol_h_mm=round(bar_hmm, 3),
        available_mm=round(available_mm, 4),
        warning=warn,
    )


def compute_barcode_fit(
    value: Optional[str],
    *,
    label_w_mm: Optional[float] = None,
    label_h_mm: Optional[float] = None,
    margin_mm: Optional[float] = None,
    cfg: Optional[dict] = None,
) -> BarcodeFitResult:
    """
    Code128 فقط: horizontal → rotated 90° → fail.
    بلا QR / DataMatrix.
    """
    c = dict(PRINT_BARCODE_CFG)
    if cfg:
        c.update(cfg)
    lw = float(label_w_mm if label_w_mm is not None else c["label_w_mm"])
    lh = float(label_h_mm if label_h_mm is not None else c["label_h_mm"])
    margin = float(margin_mm if margin_mm is not None else c["margin_mm"])
    text_reserve = float(c["text_reserve_mm"])

    value_exact = barcode_text(value)
    n = len(value_exact)
    modules = code128_modules(n)

    if not value_exact or not any(ch.isalnum() for ch in value_exact):
        return BarcodeFitResult(
            ok=False,
            mode="fail",
            value=value_exact,
            n=n,
            modules=modules,
            module_mm=0.0,
            module_dots=0,
            bar_hmm=0.0,
            symbol_w_mm=0.0,
            symbol_h_mm=0.0,
            available_mm=max(0.0, lw - 2 * margin),
            warning="قيمة باركود غير صالحة للطباعة",
        )

    avail_w = lw - 2 * margin
    bar_h = _clamp_bar_h(c, lh - 2 * margin - text_reserve)
    pref = int(c["pref_min_module_dots"])
    absolute_min = int(c["min_module_dots"])

    # أ) أفقي — فضّل وحدة ≥2 ثم اسمح بـ 1 للطويل
    for min_d in (pref, absolute_min):
        hit = _try_code128(
            value=value_exact,
            n=n,
            modules=modules,
            available_mm=avail_w,
            bar_hmm=bar_h,
            mode="horizontal",
            cfg=c,
            min_dots=min_d,
        )
        if hit:
            return hit

    # ب) تدوير 90°
    avail_rot = lh - 2 * margin
    bar_h_rot = _clamp_bar_h(c, lw - 2 * margin - 1.0)
    for min_d in (pref, absolute_min):
        hit = _try_code128(
            value=value_exact,
            n=n,
            modules=modules,
            available_mm=avail_rot,
            bar_hmm=bar_h_rot,
            mode="rotated",
            cfg=c,
            min_dots=min_d,
        )
        if hit:
            return BarcodeFitResult(
                **{
                    **hit.__dict__,
                    "warning": (hit.warning + " · " if hit.warning else "")
                    + "Code128 مُدار 90° ليتسع على اللزقة",
                }
            )

    need_mm = modules * float(c["pref_min_module_mm"])
    return BarcodeFitResult(
        ok=False,
        mode="fail",
        value=value_exact,
        n=n,
        modules=modules,
        module_mm=0.0,
        module_dots=0,
        bar_hmm=0.0,
        symbol_w_mm=0.0,
        symbol_h_mm=0.0,
        available_mm=round(avail_w, 4),
        warning=(
            f"الباركود أطول من اللزقة حتى بوحدة Code128 الدنيا "
            f"(يحتاج ≈{need_mm:.1f} مم). استخدم لزقة أعرض — لن يُحوَّل إلى QR."
        ),
    )
