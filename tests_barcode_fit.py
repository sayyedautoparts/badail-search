# -*- coding: utf-8 -*-
"""Unit tests لحساب أبعاد الباركود الديناميكي."""

import unittest

from barcode_fit import (
    PRINT_BARCODE_CFG,
    barcode_text,
    code128_modules,
    compute_barcode_fit,
)


class TestBarcodeText(unittest.TestCase):
    def test_keeps_internal_spaces(self):
        raw = "223112U001        HI"
        out = barcode_text(raw)
        self.assertEqual(out, raw)
        self.assertEqual(out.count(" "), 8)

    def test_strips_only_crlf(self):
        raw = "AB\rC\nD  E"
        self.assertEqual(barcode_text(raw), "ABCD  E")

    def test_no_trim(self):
        raw = "  ABC  "
        self.assertEqual(barcode_text(raw), "  ABC  ")


class TestCode128Modules(unittest.TestCase):
    def test_formula(self):
        # modules = 11*(n+2) + 13 + 20
        for n in (0, 5, 12, 20, 40):
            self.assertEqual(code128_modules(n), 11 * (n + 2) + 13 + 20)

    def test_spaces_count_in_n(self):
        val = "223112U001        HI"
        n = len(barcode_text(val))
        self.assertEqual(n, 20)
        self.assertEqual(code128_modules(n), 11 * 22 + 13 + 20)


class TestComputeFit(unittest.TestCase):
    def test_short_wide_modules(self):
        # قصير → يتسع بخط عريض (قرب الحد الأقصى)
        r = compute_barcode_fit("12345")
        self.assertTrue(r.ok)
        self.assertEqual(r.mode, "horizontal")
        self.assertEqual(r.value, "12345")
        self.assertGreaterEqual(r.module_dots, 2)
        self.assertLessEqual(r.module_dots, 4)
        self.assertGreaterEqual(r.module_mm, PRINT_BARCODE_CFG["min_module_mm"] - 1e-6)
        self.assertLessEqual(r.module_mm, PRINT_BARCODE_CFG["max_module_mm"] + 1e-6)
        self.assertLessEqual(r.symbol_w_mm, r.available_mm + 1e-6)

    def test_long_fits_at_minimum(self):
        # أطول ما يدخل أفقياً عند 2 dots على لزقة 50مم
        # modules*0.25 <= 47 → n تقريباً <= 12
        val = "A" * 12
        r = compute_barcode_fit(val)
        self.assertTrue(r.ok)
        self.assertEqual(r.mode, "horizontal")
        self.assertEqual(r.n, 12)
        self.assertEqual(r.value, val)
        self.assertGreaterEqual(r.module_dots, 2)

    def test_very_long_fallback_rotate_or_datamatrix(self):
        # طويل جداً لن يدخل أفقياً بـ 0.25مم
        val = "223112U001                    HI19EXTRALONG"
        r = compute_barcode_fit(val)
        self.assertEqual(r.value, val)  # لم تُقصّ القيمة
        self.assertIn(" ", r.value)
        self.assertTrue(r.ok)
        self.assertIn(r.mode, ("rotated", "datamatrix"))

    def test_spaces_preserved_and_counted(self):
        val = "223112U001        HI"
        r = compute_barcode_fit(val)
        self.assertEqual(r.value, val)
        self.assertEqual(r.n, len(val))
        self.assertEqual(r.modules, code128_modules(len(val)))
        # على 50×25 غالباً DataMatrix أو rotated لأن 275*0.25 > 47
        self.assertTrue(r.ok)
        self.assertIn(r.mode, ("horizontal", "rotated", "datamatrix"))

    def test_fail_when_impossible(self):
        # لزقة ضيقة جداً + نص طويل
        val = "X" * 80
        r = compute_barcode_fit(val, label_w_mm=20, label_h_mm=12, margin_mm=1.5)
        if r.ok:
            self.assertEqual(r.mode, "datamatrix")
        else:
            self.assertEqual(r.mode, "fail")
            self.assertTrue(r.warning)


if __name__ == "__main__":
    unittest.main()
