# -*- coding: utf-8 -*-
import unittest

from barcode_fit import barcode_text, code128_modules, compute_barcode_fit


class TestBarcodeFit(unittest.TestCase):
    def test_spaces_kept(self):
        raw = "223112U001        HI"
        self.assertEqual(barcode_text(raw), raw)

    def test_short_ok(self):
        r = compute_barcode_fit("12345")
        self.assertTrue(r.ok)
        self.assertGreaterEqual(r.module_dots, 2)
        self.assertEqual(r.mode, "horizontal")

    def test_formula_counts_spaces(self):
        val = "223112U001        HI"
        self.assertEqual(len(val), 20)
        self.assertEqual(code128_modules(20), 11 * 22 + 13 + 20)

    def test_long_fits_by_reducing_x(self):
        # مثل NiceLabel: الطويل يقلّل X إلى نقطة صحيحة بدل Resize
        r = compute_barcode_fit("223112U001        HI")
        self.assertTrue(r.ok)
        self.assertGreaterEqual(r.module_dots, 1)
        self.assertLessEqual(r.symbol_w_mm, r.available_mm + 0.05)
        self.assertEqual(r.mode, "horizontal")

    def test_never_qr_mode(self):
        r = compute_barcode_fit("223112U001        HI")
        self.assertNotEqual(r.mode, "datamatrix")
        self.assertNotEqual(r.mode, "qr")


if __name__ == "__main__":
    unittest.main()
