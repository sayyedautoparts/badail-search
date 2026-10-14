# -*- coding: utf-8 -*-
"""
عميل Hesabate Store API لاكتشاف المواقع.
التوثيق: https://documenter.getpostman.com/view/5117721/2sAYdcrXsJ
"""
from __future__ import annotations

import json
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple

RecordRequestFn = Callable[[str, str, str], None]

_TOKEN_CACHE: Dict[str, Tuple[str, float]] = {}
_TOKEN_TTL_SEC = 3300.0
_PREFERRED_SCHEME: Dict[str, str] = {}
_REAUTH_LOCKS: Dict[str, threading.Lock] = {}


class StoreApiError(Exception):
    pass


def normalize_store_api_host(host: str) -> str:
    s = str(host or "").strip()
    if not s:
        return ""
    s = re.sub(r"^https?://", "", s, flags=re.I)
    s = s.split("/")[0].strip()
    return s.rstrip(".")


def _ssl_context(verify_ssl: bool) -> ssl.SSLContext:
    if verify_ssl:
        return ssl.create_default_context()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _post_form(
    host: str,
    fields: Dict[str, Any],
    *,
    verify_ssl: bool = False,
    timeout: float = 180.0,
    scheme: str = "https",
    record_request: Optional[RecordRequestFn] = None,
) -> Dict[str, Any]:
    h = normalize_store_api_host(host)
    if not h:
        raise StoreApiError("رابط سيرفر حساباتي فارغ.")
    url = f"{scheme}://{h}/store_api.php"
    clean = {k: "" if v is None else str(v) for k, v in fields.items()}
    data = urllib.parse.urlencode(clean).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "User-Agent": "BadailSearch/StoreAPI",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        raw = urllib.request.urlopen(req, timeout=timeout, context=_ssl_context(verify_ssl)).read()
        if record_request:
            record_request(h, str(clean.get("action") or ""), str(clean.get("type") or ""))
    except urllib.error.HTTPError as e:
        if record_request:
            record_request(h, str(clean.get("action") or ""), str(clean.get("type") or ""))
        raise StoreApiError(f"HTTP {e.code} من {h}") from e
    except urllib.error.URLError as e:
        raise StoreApiError(f"تعذّر الاتصال بـ {h}: {e.reason}") from e
    text = raw.decode("utf-8", "replace").strip()
    if not text:
        raise StoreApiError("استجابة فارغة من السيرفر.")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        raise StoreApiError(f"استجابة غير JSON: {text[:200]}") from e
    if not isinstance(payload, dict):
        raise StoreApiError("شكل الاستجابة غير متوقع.")
    return payload


def _post_form_auto_scheme(
    host: str,
    fields: Dict[str, Any],
    *,
    verify_ssl: bool,
    timeout: float,
    record_request: Optional[RecordRequestFn] = None,
    preferred_scheme: str = "",
) -> Tuple[Dict[str, Any], str]:
    h = normalize_store_api_host(host)
    pref = preferred_scheme or _PREFERRED_SCHEME.get(h) or ""
    schemes = (pref, "https", "http") if pref in ("https", "http") else ("https", "http")
    seen = set()
    order = []
    for s in schemes:
        if s and s not in seen:
            seen.add(s)
            order.append(s)
    last_err: Optional[Exception] = None
    for scheme in order:
        try:
            payload = _post_form(
                host,
                fields,
                verify_ssl=verify_ssl,
                timeout=timeout,
                scheme=scheme,
                record_request=record_request,
            )
            if h:
                _PREFERRED_SCHEME[h] = scheme
            return payload, scheme
        except StoreApiError as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    raise StoreApiError("تعذّر الاتصال بالسيرفر.")


def _auth_cache_key(host: str, username: str) -> str:
    return f"{normalize_store_api_host(host)}|{str(username or '').strip()}"


def invalidate_token(host: str, username: str) -> None:
    _TOKEN_CACHE.pop(_auth_cache_key(host, username), None)


def store_api_auth(
    host: str,
    username: str,
    password: str,
    *,
    verify_ssl: bool = False,
    record_request: Optional[RecordRequestFn] = None,
    preferred_scheme: str = "",
) -> Tuple[str, str]:
    payload, scheme = _post_form_auto_scheme(
        host,
        {"username": username, "password": password, "action": "Auth"},
        verify_ssl=verify_ssl,
        timeout=60.0,
        record_request=record_request,
        preferred_scheme=preferred_scheme,
    )
    if payload.get("result") == "Auth" and payload.get("token"):
        return str(payload["token"]), scheme
    if str(payload.get("status") or "").lower() == "error":
        code = str(payload.get("code") or "")
        msg = str(payload.get("msg") or "فشل تسجيل الدخول")
        if code == "102":
            msg = (
                "فشل تسجيل الدخول: تحقق من المستخدم/كلمة المرور "
                "وصلاحية E-Commerce Link في حساباتي."
            )
        raise StoreApiError(msg)
    raise StoreApiError("فشل تسجيل الدخول — تحقق من الرابط والمستخدم وكلمة المرور.")


def store_api_auth_cached(
    host: str,
    username: str,
    password: str,
    *,
    verify_ssl: bool = False,
    force: bool = False,
    record_request: Optional[RecordRequestFn] = None,
    preferred_scheme: str = "",
    token_from_store: str = "",
    token_exp_mono: float = 0.0,
) -> Tuple[str, str, bool]:
    """
    يُرجع (token, scheme, auth_was_requested).
    يستخدم كاش الذاكرة ثم token_from_store إن لم ينتهِ.
    """
    key = _auth_cache_key(host, username)
    if not force:
        hit = _TOKEN_CACHE.get(key)
        if hit and time.monotonic() < hit[1]:
            return hit[0], (_PREFERRED_SCHEME.get(normalize_store_api_host(host)) or preferred_scheme or "https"), False
        if token_from_store and token_exp_mono and time.monotonic() < token_exp_mono:
            _TOKEN_CACHE[key] = (token_from_store, token_exp_mono)
            return token_from_store, (preferred_scheme or "https"), False
    token, scheme = store_api_auth(
        host,
        username,
        password,
        verify_ssl=verify_ssl,
        record_request=record_request,
        preferred_scheme=preferred_scheme,
    )
    _TOKEN_CACHE[key] = (token, time.monotonic() + _TOKEN_TTL_SEC)
    return token, scheme, True


def store_api_download(
    host: str,
    token: str,
    type_name: str,
    extra: Optional[Dict[str, Any]] = None,
    *,
    verify_ssl: bool = False,
    reauth: Optional[Any] = None,
    record_request: Optional[RecordRequestFn] = None,
    preferred_scheme: str = "",
) -> Tuple[Dict[str, Any], str]:
    fields: Dict[str, Any] = {"token": token, "action": "download", "type": type_name}
    if extra:
        fields.update(extra)
    payload, scheme = _post_form_auto_scheme(
        host,
        fields,
        verify_ssl=verify_ssl,
        timeout=180.0,
        record_request=record_request,
        preferred_scheme=preferred_scheme,
    )
    status = str(payload.get("status") or "").lower()
    if status == "error":
        code = str(payload.get("code") or "")
        msg = str(payload.get("msg") or "خطأ من السيرفر")
        if code == "103":
            return {"status": "succes", "table": [], "note": payload.get("note")}, scheme
        raise StoreApiError(f"{msg} (code={code})")
    if payload.get("result") == "Auth":
        if callable(reauth):
            new_token = reauth()
            return store_api_download(
                host,
                new_token,
                type_name,
                extra,
                verify_ssl=verify_ssl,
                reauth=None,
                record_request=record_request,
                preferred_scheme=scheme or preferred_scheme,
            )
        raise StoreApiError("انتهت صلاحية الجلسة — أعد المزامنة.")
    return payload, scheme


def _table_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = payload.get("table") or []
    return [r for r in rows if isinstance(r, dict)]


def _id_name_map(payload: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in _table_rows(payload):
        rid = str(row.get("id") or "").strip()
        name = str(row.get("name") or "").strip()
        if rid and name:
            out[rid] = name
    return out


def _qty_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    return s


def _text(v: Any) -> str:
    return str(v or "").strip()


def _product_location(prod: Dict[str, Any]) -> str:
    for key in ("location", "place", "shelf", "bin", "store_place", "موقع", "مكان"):
        val = _text(prod.get(key))
        if val:
            return val
    return ""


def map_products_to_location_rows(
    products_payload: Dict[str, Any],
    *,
    company_payload: Optional[Dict[str, Any]] = None,
    trades_payload: Optional[Dict[str, Any]] = None,
    preserved_locations: Optional[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    companies = _id_name_map(company_payload or {})
    trades = _id_name_map(trades_payload or {})
    preserved = preserved_locations or {}
    out: List[Dict[str, str]] = []
    for prod in _table_rows(products_payload):
        pid = _text(prod.get("id"))
        if not pid:
            continue
        item_name = _text(prod.get("name"))
        barcode = _text(prod.get("barcode"))
        original_number = _text(prod.get("model_no")) or _text(prod.get("other_codes"))
        notes = _text(prod.get("note"))
        company = companies.get(_text(prod.get("company_id")), "")
        brand = trades.get(_text(prod.get("trade_id")), "")
        quantity = _qty_text(prod.get("amount"))
        price = _qty_text(prod.get("price"))
        location = _product_location(prod)
        if not location and barcode:
            location = preserved.get(barcode) or preserved.get(re.sub(r"[\s\-]+", "", barcode)) or ""
        if not any([item_name, barcode, original_number, location, notes, quantity]):
            continue
        out.append(
            {
                "item_name": item_name,
                "location": location,
                "company": company,
                "original_number": original_number,
                "barcode": barcode,
                "brand": brand,
                "notes": notes,
                "quantity": quantity,
                "price": price,
                "product_id": pid,
            }
        )
    return out


def fetch_location_catalog(
    host: str,
    username: str,
    password: str,
    *,
    verify_ssl: bool = False,
    record_request: Optional[RecordRequestFn] = None,
    preferred_scheme: str = "",
    token_from_store: str = "",
    token_exp_mono: float = 0.0,
    lookup_company: Optional[Dict[str, Any]] = None,
    lookup_trades: Optional[Dict[str, Any]] = None,
    need_lookup_refresh: bool = False,
) -> Dict[str, Any]:
    """
    سحب كتالوج المواقع بأقل طلبات ممكنة:
    - Auth فقط إذا انتهى الـ token
    - company/trades فقط إذا انتهى كاش الجداول
    - products دائماً عند المزامنة
    """
    batch_count = {"n": 0}

    def _rec(h: str, action: str, type_name: str) -> None:
        batch_count["n"] += 1
        if record_request:
            record_request(h, action, type_name)

    reauth_lock = _REAUTH_LOCKS.setdefault(_auth_cache_key(host, username), threading.Lock())

    def _reauth() -> str:
        with reauth_lock:
            invalidate_token(host, username)
            tok, _sch, _ = store_api_auth_cached(
                host,
                username,
                password,
                verify_ssl=verify_ssl,
                force=True,
                record_request=_rec,
                preferred_scheme=preferred_scheme,
            )
            return tok

    token, scheme, auth_hit = store_api_auth_cached(
        host,
        username,
        password,
        verify_ssl=verify_ssl,
        force=False,
        record_request=_rec,
        preferred_scheme=preferred_scheme,
        token_from_store=token_from_store,
        token_exp_mono=token_exp_mono,
    )

    company = lookup_company if isinstance(lookup_company, dict) else {}
    trades = lookup_trades if isinstance(lookup_trades, dict) else {}
    lookup_refreshed = False
    if need_lookup_refresh or not company or not trades:
        company, scheme = store_api_download(
            host,
            token,
            "company",
            verify_ssl=verify_ssl,
            reauth=_reauth,
            record_request=_rec,
            preferred_scheme=scheme or preferred_scheme,
        )
        trades, scheme = store_api_download(
            host,
            token,
            "trades",
            verify_ssl=verify_ssl,
            reauth=_reauth,
            record_request=_rec,
            preferred_scheme=scheme or preferred_scheme,
        )
        lookup_refreshed = True

    products, scheme = store_api_download(
        host,
        token,
        "products",
        {
            "pro_id": "",
            "all": "1",
            "view_items_by": "0",
            "production_only": "0",
        },
        verify_ssl=verify_ssl,
        reauth=_reauth,
        record_request=_rec,
        preferred_scheme=scheme or preferred_scheme,
    )

    return {
        "token": token,
        "scheme": scheme,
        "auth_requested": auth_hit,
        "products": products,
        "company": company,
        "trades": trades,
        "lookup_refreshed": lookup_refreshed,
        "request_count": int(batch_count["n"]),
        "token_exp_mono": time.monotonic() + _TOKEN_TTL_SEC,
    }
