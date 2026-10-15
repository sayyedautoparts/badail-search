# -*- coding: utf-8 -*-
"""اختبارات مسار جلب صنف واحد — بدون تنزيل كتالوج وبدون باركود كـ pro_id."""
from __future__ import annotations

import hesabate_store_api as h


def _eq(a, b, msg: str) -> None:
    if a != b:
        raise AssertionError(f"{msg}: {a!r} != {b!r}")


def test_looks_like_product_id() -> None:
    _eq(h.looks_like_hesabate_product_id("42"), True, "internal id")
    _eq(h.looks_like_hesabate_product_id("abc"), False, "non-digit")
    _eq(
        h.looks_like_hesabate_product_id("1234567890123", not_barcode_keys=["1234567890123"]),
        False,
        "barcode key rejected as pro_id",
    )
    _eq(
        h.looks_like_hesabate_product_id("99", not_barcode_keys=["1234567890123"]),
        True,
        "short id allowed beside barcode keys",
    )


def test_fetch_lookup_never_uses_barcode_as_pro_id(monkey_calls: list) -> None:
    """يتأكد أن المسار التفاعلي لا يرسل الباركود في pro_id ولا all=1."""

    def fake_download(host, token, type_name, extra=None, **kwargs):
        monkey_calls.append(dict(extra or {}))
        return {"status": "succes", "table": []}, "https"

    orig = h.store_api_download
    h.store_api_download = fake_download  # type: ignore
    try:
        h.invalidate_products_mem_cache("example.hesabate.com")
        products, scheme, mode, n = h.fetch_products_for_lookup(
            "example.hesabate.com",
            "tok",
            barcode_keys=["1234567890123"],
            text_query="",
            pro_id="1234567890123",  # باركود خاطئ كـ pro_id — يجب رفضه
            allow_full_catalog=False,
            bypass_mem_cache=True,
        )
        _eq(mode, "single_miss", "miss without full catalog")
        _eq(products.get("table"), [], "empty table")
        assert n >= 1
        for call in monkey_calls:
            _eq(call.get("all"), "0", "interactive must use all=0")
            # الباركود يذهب لحقل barcode وليس pro_id
            if call.get("barcode"):
                _eq(call.get("pro_id"), "", "barcode field must not set pro_id")
            else:
                # لا يُسمح بـ pro_id=الباركود
                assert call.get("pro_id") != "1234567890123"
    finally:
        h.store_api_download = orig  # type: ignore


def test_fetch_lookup_uses_real_pro_id(monkey_calls: list) -> None:
    def fake_download(host, token, type_name, extra=None, **kwargs):
        monkey_calls.append(dict(extra or {}))
        return {
            "status": "succes",
            "table": [{"id": "77", "name": "X", "barcode": "999", "amount": "1", "price": "2"}],
        }, "https"

    orig = h.store_api_download
    h.store_api_download = fake_download  # type: ignore
    try:
        h.invalidate_products_mem_cache("example.hesabate.com")
        products, scheme, mode, n = h.fetch_products_for_lookup(
            "example.hesabate.com",
            "tok",
            barcode_keys=["999"],
            pro_id="77",
            allow_full_catalog=False,
            bypass_mem_cache=True,
        )
        _eq(mode, "single_pro_id", "mode")
        _eq(len(products.get("table") or []), 1, "one hit")
        _eq(monkey_calls[0].get("pro_id"), "77", "real pro_id sent")
        _eq(monkey_calls[0].get("all"), "0", "all=0")
    finally:
        h.store_api_download = orig  # type: ignore


def test_reject_full_catalog_in_interactive() -> None:
    big = [{"id": str(i), "barcode": str(i), "name": "n"} for i in range(100)]

    def fake_download(host, token, type_name, extra=None, **kwargs):
        return {"status": "succes", "table": big}, "https"

    orig = h.store_api_download
    h.store_api_download = fake_download  # type: ignore
    try:
        h.invalidate_products_mem_cache("example.hesabate.com")
        products, scheme, mode, n = h.fetch_products_for_lookup(
            "example.hesabate.com",
            "tok",
            barcode_keys=["5"],
            allow_full_catalog=False,
            bypass_mem_cache=True,
        )
        _eq(mode, "single_miss", "full dump rejected")
        _eq(products.get("table"), [], "no rows from rejected full dump")
    finally:
        h.store_api_download = orig  # type: ignore


def main() -> None:
    test_looks_like_product_id()
    test_fetch_lookup_never_uses_barcode_as_pro_id([])
    test_fetch_lookup_uses_real_pro_id([])
    test_reject_full_catalog_in_interactive()
    print("tests_hesabate_lookup: OK")


if __name__ == "__main__":
    main()
