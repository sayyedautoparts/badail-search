# -*- coding: utf-8 -*-
import unittest

from barcode_fit import (
    barcode_text,
    code128_modules,
    code128_symbol_modules,
    compute_barcode_fit,
    css_px_as_mm_96dpi,
    mm_from_dots,
    sheet_geometry_ok,
)


class TestBarcodeFit(unittest.TestCase):
    def test_spaces_kept(self):
        raw = "223112U001        HI"
        self.assertEqual(barcode_text(raw), raw)

    def test_short_numeric_fixed_x(self):
        r = compute_barcode_fit("12345")
        self.assertTrue(r.ok)
        self.assertEqual(r.module_dots, 3)
        self.assertEqual(r.scale, 1.0)
        self.assertFalse(r.css_squash)
        self.assertLessEqual(r.symbol_w_mm, r.available_mm + 0.05)
        self.assertEqual(r.mode, "horizontal")

    def test_ean13_length_numeric(self):
        r = compute_barcode_fit("1234567890123")
        self.assertTrue(r.ok)
        self.assertGreaterEqual(r.module_dots, 2)
        self.assertEqual(r.scale, 1.0)
        self.assertFalse(r.css_squash)
        self.assertLessEqual(r.symbol_w_mm, r.available_mm + 0.05)

    def test_long_numeric_integer_x(self):
        r = compute_barcode_fit("123456789012345678901234")
        self.assertTrue(r.ok)
        self.assertGreaterEqual(r.module_dots, 1)
        self.assertEqual(r.scale, 1.0)
        self.assertFalse(r.css_squash)
        self.assertLessEqual(r.symbol_w_mm, r.available_mm + 0.05)

    def test_alphanumeric(self):
        r = compute_barcode_fit("ABC123XY")
        self.assertTrue(r.ok)
        self.assertGreaterEqual(r.module_dots, 2)
        self.assertEqual(r.scale, 1.0)
        self.assertFalse(r.css_squash)

    def test_too_long_does_not_squash(self):
        huge = "A" * 80
        r = compute_barcode_fit(huge)
        self.assertFalse(r.ok)
        self.assertEqual(r.scale, 1.0)
        self.assertFalse(r.css_squash)
        self.assertEqual(r.module_dots, 0)
        self.assertIn("وسّع", r.warning)

    def test_spaced_alnum_no_css_squash(self):
        r = compute_barcode_fit("223112U001        HI")
        self.assertEqual(r.scale, 1.0)
        self.assertFalse(r.css_squash)
        self.assertNotEqual(r.mode, "datamatrix")
        self.assertNotEqual(r.mode, "qr")
        if r.ok:
            self.assertLessEqual(r.symbol_w_mm, r.available_mm + 0.05)

    def test_formula_counts_spaces(self):
        val = "223112U001        HI"
        self.assertEqual(len(val), 20)
        self.assertEqual(code128_modules(20), 11 * 22 + 13 + 20)

    def test_digit_pairs_smaller_than_b(self):
        self.assertLess(code128_symbol_modules("1234567890123"), code128_symbol_modules("A" * 13))

    def test_sheet_two_labels(self):
        self.assertTrue(sheet_geometry_ok())
        self.assertFalse(sheet_geometry_ok(sheet_w=103, label_h=25, margin=0, gap=3))

    def test_left_and_right_same_available_width(self):
        left = compute_barcode_fit("12345", label_w_mm=50, margin_mm=1.5)
        right = compute_barcode_fit("12345", label_w_mm=50, margin_mm=1.5)
        self.assertEqual(left.symbol_w_mm, right.symbol_w_mm)
        self.assertEqual(left.module_dots, right.module_dots)

    def test_never_map_canvas_dots_as_96dpi_css_px(self):
        dots = 384
        native_mm = mm_from_dots(dots, 203)
        wrong_mm = css_px_as_mm_96dpi(dots)
        self.assertAlmostEqual(native_mm, 48.0, delta=0.2)
        self.assertGreater(wrong_mm, native_mm * 1.8)

    def test_height_independent_of_x(self):
        short = compute_barcode_fit("12")
        longish = compute_barcode_fit("ABCDE12345")
        self.assertEqual(short.bar_hmm, longish.bar_hmm)


if __name__ == "__main__":
    unittest.main()
