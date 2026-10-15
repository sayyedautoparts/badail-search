"""اختبارات سريعة لتطبيع الباركود في تبويب المواقع.
تشغيل من جذر المشروع:
  python tests_location_barcode.py
"""
from __future__ import annotations

import app as m


def _eq(a, b, label: str) -> None:
    if a != b:
        raise AssertionError(f"{label}: expected {b!r}, got {a!r}")


def main() -> None:
    # فواصل ومسافات
    _eq(m.normalize_barcode_lookup_key("123-22311"), "12322311", "hyphen digits")
    _eq(m.normalize_barcode_lookup_key("18843 10062"), "1884310062", "spaced digits")
    _eq(m.normalize_barcode_lookup_key("0 1234-AB"), "01234AB", "alnum leading zero")

    # أرقام عربية
    _eq(m.normalize_barcode_lookup_key("٠١٢٣-٤٥"), "012345", "arabic digits")

    # حروف
    _eq(m.normalize_barcode_lookup_key("ab-0012"), "AB0012", "letters uppercase")

    # مفاتيح بحث متعددة للأرقام فقط (بأصفار بادئة)
    keys = m.barcode_search_keys("012322311")
    _eq("12322311" in keys, True, "strip form in keys")
    _eq("012322311" in keys, True, "full digit form in keys")

    # مطابقة صف مع فواصل في الخلية
    ks = set(m.barcode_search_keys("12322311"))
    _eq(m.barcode_cell_matches_search_keys("123-22311", ks), True, "cell with hyphen matches")
    _eq(m.barcode_cell_matches_search_keys("18843 10062", set(m.barcode_search_keys("1884310062"))), True, "spaced cell")

    # صفور بادئة رقمية (تُحفظ في المفتاح الكانوني؛ البحث يجرب أيضاً الشكل بلا أصفار بادئة)
    _eq(m.normalize_barcode_lookup_key("000123"), "000123", "leading zeros preserved in canonical")
    _eq("123" in m.barcode_search_keys("000123"), True, "no-zero variant in search keys")

    # باركودات الوحدات (K): كل قيمة بعد ':'
    units = m._extract_location_unit_barcodes_from_cell(":512312321 :4255761249700")
    _eq(units, ["512312321", "4255761249700"], "extract unit barcodes colon")
    _eq(
        m.location_row_normalized_barcode_value("9988", ":512312321 :4255761249700"),
        " 9988 512312321 4255761249700 ",
        "normalized combined barcodes",
    )
    _eq(
        m.barcode_cell_matches_search_keys(
            "9988 | :512312321 :4255761249700",
            set(m.barcode_search_keys("4255761249700")),
        ),
        True,
        "units barcode matches query",
    )

    # حساباتي other_codes: عدة باركودات بلا ':'
    hesabate_units = m._extract_location_unit_barcodes_from_cell("111222333 444555666,777888999")
    _eq(hesabate_units, ["111222333", "444555666", "777888999"], "hesabate other_codes split")
    _eq(
        m.location_row_normalized_barcode_value("9988", "111222333 444555666"),
        " 9988 111222333 444555666 ",
        "normalized hesabate multi barcodes",
    )
    # لا تُدمج باركودات طويلة متعددة في مفتاح واحد
    _eq(
        m.location_row_normalized_barcode_value("1234567890123,9876543210987", ""),
        " 1234567890123 9876543210987 ",
        "main field multi barcodes not merged",
    )
    # مسافات تنسيق داخل باركود واحد قصير الأجزاء تُدمَج
    _eq(
        m.location_row_normalized_barcode_value("18843 10062", ""),
        " 1884310062 ",
        "spaced single barcode rejoined",
    )
    nb_multi = m.location_row_normalized_barcode_value("AAA", "BBB CCC")
    _eq(
        m._location_row_matches_barcode_keys_exact(
            {"barcode": "AAA", "normalized_barcode": nb_multi},
            set(m.barcode_search_keys("CCC")),
        ),
        True,
        "search hits secondary barcode",
    )
    keys_idx = m._fast_cache_barcode_keys("MAIN1", "SEC2 SEC3 SEC4")
    _eq("SEC3" in keys_idx, True, "index includes later other_codes")

    # فاصلة عربية و + داخل خانة الباركود نفسها
    ar = m._extract_location_unit_barcodes_from_cell("1234567890123،9876543210987")
    _eq(ar, ["1234567890123", "9876543210987"], "arabic comma split in barcode field")
    plus = m._extract_location_unit_barcodes_from_cell("1234567890123+9876543210987")
    _eq(plus, ["1234567890123", "9876543210987"], "plus split in barcode field")
    glued = m._extract_location_unit_barcodes_from_cell("12345678901239876543210987")
    _eq(glued, ["1234567890123", "9876543210987"], "concatenated EAN13 pair")
    nb = m.location_row_normalized_barcode_value("1111111111111،2222222222222", "")
    _eq(
        m._location_row_matches_barcode_keys_exact(
            {"barcode": "1111111111111،2222222222222", "normalized_barcode": nb},
            set(m.barcode_search_keys("2222222222222")),
        ),
        True,
        "search secondary barcode inside barcode field",
    )
    # لا يدمج عند مطابقة الخلية
    _eq(
        m.barcode_cell_matches_search_keys(
            "1111111111111،2222222222222",
            set(m.barcode_search_keys("2222222222222")),
        ),
        True,
        "cell match uses split not merge",
    )

    # مطابقة دقيقة: لا يطابق جزء من باركود أطول
    _eq(
        m.barcode_cell_matches_search_keys("123456789", set(m.barcode_search_keys("12345"))),
        False,
        "substring of longer barcode must not match",
    )
    _eq(
        m._location_row_matches_barcode_keys_exact(
            {"barcode": "99912345999", "normalized_barcode": " 99912345999 "},
            set(m.barcode_search_keys("12345")),
        ),
        False,
        "exact helper rejects embedded digits",
    )
    _eq(
        m._location_row_matches_barcode_keys_exact(
            {"barcode": "12345", "normalized_barcode": " 12345 "},
            set(m.barcode_search_keys("12345")),
        ),
        True,
        "exact helper accepts full barcode token",
    )

    print("tests_location_barcode: OK (all assertions passed)")


if __name__ == "__main__":
    main()
