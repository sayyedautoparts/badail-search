# -*- coding: utf-8 -*-
"""Unit tests لحساب أبعاد الباركود الديناميكي — Code128 فقط."""

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
        for n in (0, 5, 12, 20, 40):
            self.assertEqual(code128_modules(n), 11 * (n + 2) + 13 + 20)

    def test_spaces_count_in_n(self):
        val = "223112U001        HI"
        n = len(barcode_text(val))
        self.assertEqual(n, 20)
        self.assertEqual(code128_modules(n), 11 * 22 + 13 + 20)


class TestComputeFit(unittest.TestCase):
    def test_short_wide_modules(self):
        r = compute_barcode_fit("12345")
        self.assertTrue(r.ok)
        self.assertEqual(r.mode, "horizontal")
        self.assertEqual(r.value, "12345")
        self.assertGreaterEqual(r.module_dots, 2)
        self.assertLessEqual(r.module_dots, 4)
        self.assertNotEqual(r.mode, "datamatrix")

    def test_long_fits_at_minimum(self):
        val = "A" * 12
        r = compute_barcode_fit(val)
        self.assertTrue(r.ok)
        self.assertEqual(r.mode, "horizontal")
        self.assertEqual(r.value, val)
        self.assertGreaterEqual(r.module_dots, 1)

    def test_very_long_stays_code128(self):
        val = "223112U001                    HI19EXTRALONG"
        r = compute_barcode_fit(val)
        self.assertEqual(r.value, val)
        self.assertIn(" ", r.value)
        self.assertNotEqual(r.mode, "datamatrix")
        if r.ok:
            self.assertIn(r.mode, ("horizontal", "rotated"))
            self.assertGreaterEqual(r.module_dots, 1)
        else:
            self.assertEqual(r.mode, "fail")
            self.assertIn("QR", r.warning)
            self.assertIn("لزقة", r.warning)

    def test_spaces_preserved_and_counted(self):
        val = "223112U001        HI"
        r = compute_barcode_fit(val)
        self.assertEqual(r.value, val)
        self.assertEqual(r.n, len(val))
        self.assertEqual(r.modules, code128_modules(len(val)))
        self.assertNotEqual(r.mode, "datamatrix")
        self.assertTrue(r.ok)
        self.assertIn(r.mode, ("horizontal", "rotated"))

    def test_never_datamatrix_or_qr(self):
        for val in ("12345", "A" * 12, "223112U001        HI", "X" * 80):
            r = compute_barcode_fit(val)
            self.assertNotEqual(r.mode, "datamatrix")
            self.assertNotEqual(r.mode, "qr")


if __name__ == "__main__":
    unittest.main()
