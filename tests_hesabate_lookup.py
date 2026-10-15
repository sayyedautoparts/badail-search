# -*- coding: utf-8 -*-
"""اختبارات مسار جلب صنف واحد — pro_id+all=1، بدون فلتر باركود وهمي، بدون fallback خاطئ."""
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


def test_fetch_lookup_never_uses_barcode_as_pro_id(monkey_calls: list) -> None:
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
        _eq(mode, "barcode_filter_unsupported", "no barcode filter path")
        _eq(products.get("table"), [], "empty table")
        _eq(n, 0, "no API call when only barcode")
        _eq(monkey_calls, [], "must not download catalog for barcode-only")
    finally:
        h.store_api_download = orig  # type: ignore


def test_fetch_lookup_uses_real_pro_id_all1(monkey_calls: list) -> None:
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
            barcode_keys=["OLD-BARCODE"],
            pro_id="77",
            allow_full_catalog=False,
            bypass_mem_cache=True,
        )
        _eq(mode, "single_pro_id", "mode")
        # pro_id هو الهوية — حتى لو الباركود المطلوب قديم يختلف عن الحالي
        _eq(len(products.get("table") or []), 1, "one hit")
        _eq(products["table"][0]["barcode"], "999", "updated barcode kept")
        _eq(monkey_calls[0].get("pro_id"), "77", "real pro_id sent")
        _eq(monkey_calls[0].get("all"), "1", "all=1 required for single item")
        _eq(n, 1, "one request")
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
            pro_id="5",
            allow_full_catalog=False,
            bypass_mem_cache=True,
        )
        _eq(mode, "rejected_full_catalog", "full dump rejected")
        _eq(products.get("table"), [], "no rows from rejected full dump")
    finally:
        h.store_api_download = orig  # type: ignore


def test_no_loose_fallback_small_unrelated_rows() -> None:
    """لا نعرض صفوفاً غير مطابقة حتى لو الرد صغير."""

    def fake_download(host, token, type_name, extra=None, **kwargs):
        return {
            "status": "succes",
            "table": [
                {"id": "1", "barcode": "AAA", "name": "wrong"},
                {"id": "2", "barcode": "BBB", "name": "also wrong"},
            ],
        }, "https"

    orig = h.store_api_download
    h.store_api_download = fake_download  # type: ignore
    try:
        h.invalidate_products_mem_cache("example.hesabate.com")
        products, scheme, mode, n = h.fetch_products_for_lookup(
            "example.hesabate.com",
            "tok",
            barcode_keys=["ZZZ"],
            pro_id="99",
            allow_full_catalog=False,
            bypass_mem_cache=True,
        )
        # pro_id 99 not in table → miss; must NOT return AAA/BBB
        _eq(products.get("table"), [], "no unrelated rows")
        _eq(mode, "single_miss", "miss")
    finally:
        h.store_api_download = orig  # type: ignore


def test_single_mode_skips_lookup_tables() -> None:
    calls: list[str] = []

    def fake_auth_cached(*a, **k):
        return "tok", "https", False

    def fake_download(host, token, type_name, extra=None, **kwargs):
        calls.append(type_name)
        if type_name == "products":
            return {
                "status": "succes",
                "table": [{"id": "9", "barcode": "111", "name": "N", "amount": "1", "price": "1"}],
            }, "https"
        return {"status": "succes", "table": []}, "https"

    orig_auth = h.store_api_auth_cached
    orig_dl = h.store_api_download
    h.store_api_auth_cached = fake_auth_cached  # type: ignore
    h.store_api_download = fake_download  # type: ignore
    try:
        out = h.fetch_location_catalog(
            "example.hesabate.com",
            "u",
            "p",
            products_mode="single",
            lookup_pro_id="9",
            lookup_barcode_keys=["111"],
            need_lookup_refresh=True,
            need_model_refresh=True,
            lookup_company=None,
            lookup_trades=None,
            lookup_model=None,
            bypass_mem_cache=True,
        )
        _eq(calls, ["products"], "only products in single mode")
        _eq(out.get("timing_ms", {}).get("skipped_lookup_tables"), True, "skipped tables")
        _eq(len((out.get("products") or {}).get("table") or []), 1, "one product")
    finally:
        h.store_api_auth_cached = orig_auth  # type: ignore
        h.store_api_download = orig_dl  # type: ignore


def main() -> None:
    test_looks_like_product_id()
    test_fetch_lookup_never_uses_barcode_as_pro_id([])
    test_fetch_lookup_uses_real_pro_id_all1([])
    test_reject_full_catalog_in_interactive()
    test_no_loose_fallback_small_unrelated_rows()
    test_single_mode_skips_lookup_tables()
    print("tests_hesabate_lookup: OK")


if __name__ == "__main__":
    main()
