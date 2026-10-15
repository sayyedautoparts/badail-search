# -*- coding: utf-8 -*-
"""
عميل Hesabate Store API لاكتشاف المواقع.
التوثيق: https://documenter.getpostman.com/view/5117721/2sAYdcrXsJ

ما هو موثّق/مستخدم هنا:
- Auth
- download type=products|company|trades|model
- products: all=1 للكتالوج الكامل
- products: pro_id + all=1 لصنف واحد (~1 صف، ~1KB) — مُختبر عملياً

ما ثبت بالقياس أنه لا يعمل كفلتر:
- حقل barcode (مع all=0 → 0 صفوف؛ مع all=1 → الكتالوج الكامل ~27k/~18MB)
- all=0 مع pro_id → 0 صفوف

ما ليس في الوثائق ولا يُفترض وجوده:
- جلب تغييرات منذ تاريخ/مؤشر
- Webhooks
- اكتشاف صنف جديد بالباركود دون مزامنة الكتالوج أو معرفة pro_id
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
# كاش منتجات في ذاكرة العملية (نسخ Vercel الدافئة) — للمزامنة فقط بعد تحديث صريح
_PRODUCTS_MEM_CACHE: Dict[str, Any] = {"host": "", "exp": 0.0, "payload": None, "scheme": "", "synced_at": 0.0}
_PRODUCTS_MEM_TTL_SEC = 600.0
_PRODUCTS_MEM_LOCK = threading.Lock()
# حد أقصى لردّ «صنف واحد» — فوقه نعتبر أن API أعاد كتالوجاً ونرفضه في المسار التفاعلي
_SINGLE_LOOKUP_MAX_ROWS = 40
# مهلات: Auth وطلب صنف واحد سريعان عملياً (~0.5–0.7ث)؛ الكتالوج الكامل أكبر بكثير
_TIMEOUT_AUTH_SEC = 30.0
_TIMEOUT_SINGLE_SEC = 25.0
_TIMEOUT_FULL_SEC = 180.0
_TIMEOUT_LOOKUP_TABLE_SEC = 30.0


def _products_cache_get(host: str) -> Optional[Tuple[Dict[str, Any], str]]:
    h = normalize_store_api_host(host)
    with _PRODUCTS_MEM_LOCK:
        if (
            h
            and _PRODUCTS_MEM_CACHE.get("host") == h
            and time.time() < float(_PRODUCTS_MEM_CACHE.get("exp") or 0)
            and isinstance(_PRODUCTS_MEM_CACHE.get("payload"), dict)
        ):
            return _PRODUCTS_MEM_CACHE["payload"], str(_PRODUCTS_MEM_CACHE.get("scheme") or "https")
    return None


def _products_cache_set(host: str, payload: Dict[str, Any], scheme: str) -> None:
    h = normalize_store_api_host(host)
    if not h or not isinstance(payload, dict):
        return
    # لا نكاش ردود صغيرة جداً (فلتر فاشل) — فقط كتالوج كامل تقريباً
    table = payload.get("table") or []
    n = len(table) if isinstance(table, list) else 0
    if n < 50:
        return
    with _PRODUCTS_MEM_LOCK:
        _PRODUCTS_MEM_CACHE["host"] = h
        _PRODUCTS_MEM_CACHE["payload"] = payload
        _PRODUCTS_MEM_CACHE["scheme"] = scheme or "https"
        _PRODUCTS_MEM_CACHE["exp"] = time.time() + _PRODUCTS_MEM_TTL_SEC
        _PRODUCTS_MEM_CACHE["synced_at"] = time.time()


def invalidate_products_mem_cache(host: str = "") -> None:
    """إبطال كاش الذاكرة بعد مزامنة ناجحة أو عند refresh صريح."""
    h = normalize_store_api_host(host)
    with _PRODUCTS_MEM_LOCK:
        if h and _PRODUCTS_MEM_CACHE.get("host") and _PRODUCTS_MEM_CACHE.get("host") != h:
            return
        _PRODUCTS_MEM_CACHE["host"] = ""
        _PRODUCTS_MEM_CACHE["exp"] = 0.0
        _PRODUCTS_MEM_CACHE["payload"] = None
        _PRODUCTS_MEM_CACHE["scheme"] = ""
        _PRODUCTS_MEM_CACHE["synced_at"] = 0.0


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
            "Accept-Encoding": "gzip, deflate",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_context(verify_ssl)) as resp:
            raw = resp.read()
            enc = (resp.headers.get("Content-Encoding") or "").lower()
        if record_request:
            record_request(h, str(clean.get("action") or ""), str(clean.get("type") or ""))
        if "gzip" in enc:
            import gzip

            raw = gzip.decompress(raw)
        elif "deflate" in enc:
            import zlib

            try:
                raw = zlib.decompress(raw)
            except zlib.error:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
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
    """
    طلب واحد ببروتوكول واحد فقط.
    لا انتقال تلقائي https↔http عند أخطاء التطبيق أو الشبكة — يُستخدم preferred أو https.
    """
    h = normalize_store_api_host(host)
    pref = preferred_scheme or _PREFERRED_SCHEME.get(h) or "https"
    if pref not in ("https", "http"):
        pref = "https"
    payload = _post_form(
        host,
        fields,
        verify_ssl=verify_ssl,
        timeout=timeout,
        scheme=pref,
        record_request=record_request,
    )
    if h:
        _PREFERRED_SCHEME[h] = pref
    return payload, pref


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
        timeout=_TIMEOUT_AUTH_SEC,
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
    token_exp_unix: float = 0.0,
) -> Tuple[str, str, bool]:
    """
    يُرجع (token, scheme, auth_was_requested).
    يستخدم كاش الذاكرة ثم token_from_store إن لم ينتهِ.
    ملاحظة: على Vercel الـ monotonic لا يُشارك بين النسخ — لذلك نعتمد token_exp_unix.
    """
    key = _auth_cache_key(host, username)
    if not force:
        hit = _TOKEN_CACHE.get(key)
        if hit and time.monotonic() < hit[1]:
            return hit[0], (_PREFERRED_SCHEME.get(normalize_store_api_host(host)) or preferred_scheme or "https"), False
        # صلاحية مشتركة بين نسخ السيرفرلس
        if token_from_store and token_exp_unix and time.time() < float(token_exp_unix):
            remain = max(30.0, float(token_exp_unix) - time.time())
            _TOKEN_CACHE[key] = (token_from_store, time.monotonic() + min(_TOKEN_TTL_SEC, remain))
            return token_from_store, (preferred_scheme or "https"), False
        # توافق قديم: لا تعتمد على mono المخزّن عبر النسخ
        if token_from_store and (not token_exp_unix) and token_exp_mono and time.monotonic() < token_exp_mono:
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
    timeout: float = _TIMEOUT_FULL_SEC,
) -> Tuple[Dict[str, Any], str]:
    fields: Dict[str, Any] = {"token": token, "action": "download", "type": type_name}
    if extra:
        fields.update(extra)
    payload, scheme = _post_form_auto_scheme(
        host,
        fields,
        verify_ssl=verify_ssl,
        timeout=float(timeout or _TIMEOUT_FULL_SEC),
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
                timeout=timeout,
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


def looks_like_hesabate_product_id(value: Any, *, not_barcode_keys: Optional[List[str]] = None) -> bool:
    """
    المعرّف الداخلي للصنف = حقل id من حساباتي (أرقام).
    لا نقبل قيمة تطابق مفتاح باركود البحث — الباركود ليس pro_id.
    """
    s = _text(value)
    if not s or not s.isdigit():
        return False
    if not_barcode_keys:
        blocked = {_compact_code(k) for k in not_barcode_keys if k}
        blocked.update({_text(k) for k in not_barcode_keys if k})
        if s in blocked or _compact_code(s) in blocked:
            return False
    return True


def _product_location(prod: Dict[str, Any], models: Optional[Dict[str, str]] = None) -> str:
    """
    موقع الرف عند هذا المتجر = حقل الموديل (model_id → model.name) في Store API.
    أمثلة: W2 E116 / A111
    """
    mid = _text(prod.get("model_id"))
    if models and mid and mid not in ("0",):
        mname = _text(models.get(mid))
        if mname:
            return mname
    for key in ("location", "place", "shelf", "bin", "store_place", "موقع", "مكان"):
        val = _text(prod.get(key))
        if val and val not in ("0", "0.0"):
            return val
    return ""


def _compact_code(v: Any) -> str:
    """تطبيع كود/باركود مثل مفاتيح البحث: أرقام وحروف فقط."""
    s = _text(v).upper()
    if not s:
        return ""
    return re.sub(r"[^A-Z0-9]", "", s)


def _key_variants(code: str) -> List[str]:
    c = _compact_code(code)
    if not c:
        return []
    out = [c]
    if c.isdigit():
        stripped = c.lstrip("0") or "0"
        if stripped not in out:
            out.append(stripped)
    return out


def row_matches_barcode_keys(row: Dict[str, str], barcode_keys: List[str]) -> bool:
    if not barcode_keys:
        return False
    keys: set = set()
    for k in barcode_keys:
        for v in _key_variants(k):
            keys.add(v)
    if not keys:
        return False
    candidates: List[str] = []
    for raw in (row.get("barcode"), row.get("original_number"), row.get("notes")):
        raw_s = _text(raw)
        parts = re.split(r"[\s,/|;]+", raw_s) if raw_s else []
        if raw_s and raw_s not in parts:
            parts.insert(0, raw_s)
        for part in parts:
            for v in _key_variants(part):
                if v not in candidates:
                    candidates.append(v)
    for c in candidates:
        if c in keys:
            return True
    return False


def row_matches_text_query(row: Dict[str, str], query: str) -> bool:
    q = _text(query)
    if not q:
        return False
    q_compact = _compact_code(q)
    q_lower = q.lower()
    fields = [
        _text(row.get("original_number")),
        _text(row.get("barcode")),
        _text(row.get("item_name")),
        _text(row.get("notes")),
        _text(row.get("company")),
        _text(row.get("brand")),
    ]
    for f in fields:
        if not f:
            continue
        if q_lower in f.lower():
            return True
        fc = _compact_code(f)
        if q_compact and fc and (q_compact in fc or fc in q_compact):
            return True
    return False


def filter_location_rows(
    rows: List[Dict[str, str]],
    *,
    barcode_keys: Optional[List[str]] = None,
    text_query: str = "",
) -> List[Dict[str, str]]:
    """تصفية صفوف المواقع حسب باركود أو رقم أصلي/نص."""
    keys = [k for k in (barcode_keys or []) if k]
    text = _text(text_query)
    if not keys and not text:
        return []
    out: List[Dict[str, str]] = []
    seen: set = set()
    for row in rows:
        ok = False
        if keys and row_matches_barcode_keys(row, keys):
            ok = True
        elif text and row_matches_text_query(row, text):
            ok = True
        if not ok:
            continue
        dedupe_key = (
            _compact_code(row.get("product_id"))
            or _compact_code(row.get("barcode"))
            or f"{_compact_code(row.get('original_number'))}|{_text(row.get('item_name')).lower()}"
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(row)
    return out


def filter_raw_products(
    products_payload: Dict[str, Any],
    *,
    barcode_keys: Optional[List[str]] = None,
    text_query: str = "",
    max_hits: int = 30,
) -> List[Dict[str, Any]]:
    """
    تصفية سريعة على جدول products الخام قبل التحويل الكامل.
    مطابقة الباركود/الرقم: مساواة تامة بعد التطبيع (مع/بدون أصفار بادئة) — بدون contains
    حتى لا تُرجع مئات النتائج الخاطئة وتكسر الطلب.
    """
    keys: set = set()
    for k in barcode_keys or []:
        for v in _key_variants(k):
            keys.add(v)
    text = _text(text_query)
    text_keys: set = set()
    if text:
        for part in re.split(r"[\s,/|;]+", text):
            for v in _key_variants(part):
                text_keys.add(v)
        for v in _key_variants(text):
            text_keys.add(v)
    text_lower = text.lower()
    if not keys and not text:
        return []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for prod in _table_rows(products_payload):
        pid = _text(prod.get("id"))
        if not pid or pid in seen:
            continue
        # بحث الباركود: طابق حقل barcode أولاً بدقة، ثم other_codes كأكواد إضافية فقط
        # لا نطابق model_no هنا حتى لا تُرجع عدة أصناف لنفس رقم الموديل
        hit = False
        if keys:
            bc_keys: set = set()
            for raw in (prod.get("barcode"), prod.get("other_codes")):
                raw_s = _text(raw)
                if not raw_s:
                    continue
                # فواصل لاتينية + عربية + + و \\ حتى لا تُدمج عدة باركودات في مفتاح واحد
                parts = re.split(r"[\s,/|;\\+،؛]+", raw_s)
                if raw_s and raw_s not in parts:
                    parts.insert(0, raw_s)
                for part in parts:
                    part = part.strip().strip(":")
                    if not part:
                        continue
                    for v in _key_variants(part):
                        bc_keys.add(v)
            for c in bc_keys:
                if c in keys:
                    hit = True
                    break
        name = _text(prod.get("name"))
        note = _text(prod.get("note"))
        if (not hit) and text and not keys:
            # بحث نصي/رقم أصلي فقط عند عدم وجود مفاتيح باركود
            for f in (name, note, _text(prod.get("barcode")), _text(prod.get("model_no")), _text(prod.get("other_codes"))):
                if not f:
                    continue
                if text_lower and text_lower in f.lower():
                    hit = True
                    break
                for fc in _key_variants(f):
                    if fc in text_keys:
                        hit = True
                        break
                if hit:
                    break
        if not hit:
            continue
        seen.add(pid)
        out.append(prod)
        if len(out) >= max(1, int(max_hits)):
            break
    return out


def map_products_to_location_rows(
    products_payload: Dict[str, Any],
    *,
    company_payload: Optional[Dict[str, Any]] = None,
    trades_payload: Optional[Dict[str, Any]] = None,
    model_payload: Optional[Dict[str, Any]] = None,
    preserved_locations: Optional[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    companies = _id_name_map(company_payload or {})
    trades = _id_name_map(trades_payload or {})
    models = _id_name_map(model_payload or {})
    preserved = preserved_locations or {}
    out: List[Dict[str, str]] = []
    for prod in _table_rows(products_payload):
        pid = _text(prod.get("id"))
        if not pid:
            continue
        item_name = _text(prod.get("name"))
        # الباركود فقط من حقل barcode — لا نخلطه مع model_no/other_codes
        barcode = _text(prod.get("barcode"))
        original_number = _text(prod.get("model_no"))
        other_codes = _text(prod.get("other_codes"))
        notes = _text(prod.get("note"))
        if other_codes and other_codes != original_number:
            # تبقى للملاحظات/العرض؛ البحث يعتمد على unit_barcodes → normalized_barcode
            notes = f"{notes} {other_codes}".strip() if notes else other_codes
        company = companies.get(_text(prod.get("company_id")), "")
        brand = trades.get(_text(prod.get("trade_id")), "")
        quantity = _qty_text(prod.get("amount"))
        price = _qty_text(prod.get("price"))
        # الأولوية لموديل API (هذا هو موقع الرف)، ثم الكاش المحفوظ
        location = _product_location(prod, models)
        if not location:
            # جرّب الباركود الرئيسي ثم كل other_codes
            for cand in [barcode] + re.split(r"[\s,/|;]+", other_codes):
                cand = _text(cand)
                if not cand:
                    continue
                for key in (cand, re.sub(r"[\s\-]+", "", cand)):
                    val = _text(preserved.get(key))
                    if val and val not in ("0", "0.0"):
                        location = val
                        break
                if location:
                    break
        if not any([item_name, barcode, original_number, location, notes, quantity, other_codes]):
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
                # كل الباركودات الإضافية من حساباتي — تُفهرس كلها في normalized_barcode
                "unit_barcodes": other_codes,
            }
        )
    return out


def _products_download_extra(
    *,
    pro_id: str = "",
    all_flag: str = "1",
    barcode: str = "",
    extra_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        "pro_id": pro_id,
        "all": all_flag,
        "view_items_by": "0",
        "production_only": "0",
    }
    if barcode:
        fields["barcode"] = barcode
    if extra_fields:
        fields.update(extra_fields)
    return fields


def _rows_look_filtered(rows: List[Dict[str, Any]], *, barcode_keys: Optional[List[str]] = None, text_query: str = "") -> bool:
    """هل الرد يبدو نتيجة مفلترة (صنف/عدة أصناف) وليس الكتالوج الكامل؟"""
    if not rows:
        return False
    if len(rows) > 40:
        return False
    if barcode_keys:
        hits = filter_raw_products({"table": rows}, barcode_keys=barcode_keys, max_hits=40)
        return bool(hits)
    if text_query:
        hits = filter_raw_products({"table": rows}, text_query=text_query, max_hits=40)
        return bool(hits)
    return len(rows) <= 5


def fetch_products_for_lookup(
    host: str,
    token: str,
    *,
    barcode_keys: Optional[List[str]] = None,
    text_query: str = "",
    pro_id: str = "",
    verify_ssl: bool = False,
    reauth: Optional[Any] = None,
    record_request: Optional[RecordRequestFn] = None,
    preferred_scheme: str = "",
    allow_full_catalog: bool = False,
    bypass_mem_cache: bool = False,
    metrics: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], str, str, int]:
    """
    جلب صنف واحد — منفصل عن استيراد الكتالوج.

    - allow_full_catalog=True: للمزامنة الخلفية فقط (كتالوج all=1 بلا pro_id).
    - المسار التفاعلي الفعّال: pro_id الداخلي + all=1 فقط (~1 صف).
    - حقل barcode لا يُفلتر على سيرفر حساباتي (قياس حي) — لا يُرسل هنا لتجنّب
      تنزيل ~18MB ثم رفضه بعد التحميل.
    - لا تحويل الباركود/النص إلى pro_id.
    - لا fallback لعرض صفوف غير مطابقة.
    """
    keys = [k for k in (barcode_keys or []) if k]
    qt = _text(text_query)
    pid = _text(pro_id)
    scheme = preferred_scheme or "https"
    req_n = 0
    m = metrics if isinstance(metrics, dict) else None

    def _note(**kwargs: Any) -> None:
        if m is not None:
            m.update(kwargs)

    def _hits_from(products: Dict[str, Any]) -> List[Dict[str, Any]]:
        if keys:
            return filter_raw_products(products, barcode_keys=keys, max_hits=10)
        if pid and looks_like_hesabate_product_id(pid, not_barcode_keys=keys):
            out = []
            for row in _table_rows(products):
                if _text(row.get("id")) == pid:
                    out.append(row)
                    if len(out) >= 10:
                        break
            if out:
                return out
        if qt:
            return filter_raw_products(products, text_query=qt, max_hits=10)
        return []

    # 1) كاش ذاكرة — يُتخطى عند refresh صريح؛ نُرجع النتائج المطابقة فقط وليس الكتالوج
    if not bypass_mem_cache:
        cached = _products_cache_get(host)
        if cached:
            products, scheme = cached
            hits = _hits_from(products)
            if hits:
                _note(products_fetch_mode="mem_cache", products_rows=len(hits), products_ms=0, products_bytes=0)
                return {"status": "succes", "table": hits}, scheme, "mem_cache", 0

    # 2) كتالوج كامل — مسار المزامنة فقط
    if allow_full_catalog:
        t0 = time.perf_counter()
        products, scheme = store_api_download(
            host,
            token,
            "products",
            _products_download_extra(pro_id="", all_flag="1"),
            verify_ssl=verify_ssl,
            reauth=reauth,
            record_request=record_request,
            preferred_scheme=scheme or preferred_scheme,
            timeout=_TIMEOUT_FULL_SEC,
        )
        req_n += 1
        rows_n = len(_table_rows(products))
        _note(
            products_ms=int((time.perf_counter() - t0) * 1000),
            products_rows=rows_n,
            products_bytes=len(json.dumps(products, ensure_ascii=False).encode("utf-8")) if rows_n < 500 else -1,
        )
        _products_cache_set(host, products, scheme)
        hits = _hits_from(products)
        mode = "full_extract" if hits else "full_miss"
        _note(products_fetch_mode=mode)
        return products, scheme, mode, req_n

    # 3) مسار تفاعلي: pro_id + all=1 فقط (مُختبر: ~0.6ث، صف واحد)
    if looks_like_hesabate_product_id(pid, not_barcode_keys=keys):
        extra = _products_download_extra(pro_id=pid, all_flag="1")
        t0 = time.perf_counter()
        try:
            products, scheme = store_api_download(
                host,
                token,
                "products",
                extra,
                verify_ssl=verify_ssl,
                reauth=reauth,
                record_request=record_request,
                preferred_scheme=scheme or preferred_scheme,
                timeout=_TIMEOUT_SINGLE_SEC,
            )
            req_n += 1
        except StoreApiError as e:
            _note(products_error=str(e)[:200], products_ms=int((time.perf_counter() - t0) * 1000))
            return {"status": "succes", "table": []}, scheme, "single_miss", req_n
        rows = _table_rows(products)
        elapsed = int((time.perf_counter() - t0) * 1000)
        resp_bytes = len(json.dumps(products, ensure_ascii=False).encode("utf-8"))
        _note(products_ms=elapsed, products_rows=len(rows), products_bytes=resp_bytes)
        # رفض الكتالوج الكامل داخل الطلب التفاعلي (لو تجاهل السيرفر pro_id)
        if len(rows) > _SINGLE_LOOKUP_MAX_ROWS:
            _note(products_fetch_mode="rejected_full_catalog")
            return {"status": "succes", "table": []}, scheme, "rejected_full_catalog", req_n
        # المعرّف الداخلي هو الهوية الثابتة — الباركود قد يتغيّر في حساباتي
        hits = [r for r in rows if _text(r.get("id")) == pid]
        if hits:
            _note(products_fetch_mode="single_pro_id")
            return {"status": "succes", "table": hits}, scheme, "single_pro_id", req_n
        _note(products_fetch_mode="single_miss")
        return {"status": "succes", "table": []}, scheme, "single_miss", req_n

    # 4) باركود/نص بلا pro_id معروف: حساباتي لا يدعم فلترة باركود فعّالة
    #    إرسال barcode+all=1 ينزّل الكتالوج كاملاً (~18MB / 25–50ث) ثم يُرفض بعد التحميل.
    mode = "barcode_filter_unsupported" if keys else "single_miss"
    _note(
        products_fetch_mode=mode,
        products_ms=0,
        products_rows=0,
        products_bytes=0,
        barcode_filter_effective=False,
    )
    return {"status": "succes", "table": []}, scheme, mode, req_n


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
    token_exp_unix: float = 0.0,
    lookup_company: Optional[Dict[str, Any]] = None,
    lookup_trades: Optional[Dict[str, Any]] = None,
    lookup_model: Optional[Dict[str, Any]] = None,
    need_lookup_refresh: bool = False,
    need_model_refresh: bool = True,
    # عند البحث عن صنف واحد: لا تنزّل كل الكتالوج إن أمكن
    lookup_barcode_keys: Optional[List[str]] = None,
    lookup_text_query: str = "",
    lookup_pro_id: str = "",
    products_mode: str = "all",  # all | single
    allow_full_on_single: bool = False,
    bypass_mem_cache: bool = False,
    force_model_refresh: bool = False,
) -> Dict[str, Any]:
    """
    سحب كتالوج المواقع بأقل طلبات ممكنة:
    - Auth فقط إذا انتهى الـ token
    - products single: لا يجلب company/trades افتراضياً — يعرض الصنف فوراً
    - force_model_refresh: يجلب model حتى في single لتحديث أسماء المواقع
    - products all: يحدّث company/trades/model حسب الحاجة ثم الكتالوج
    موقع الرف = model.name عبر model_id (عند توفر كاش model)
    """
    batch_count = {"n": 0}
    timing: Dict[str, Any] = {"auth_ms": 0, "company_ms": 0, "trades_ms": 0, "model_ms": 0}
    t_all = time.perf_counter()

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

    t0 = time.perf_counter()
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
        token_exp_unix=token_exp_unix,
    )
    timing["auth_ms"] = int((time.perf_counter() - t0) * 1000)
    timing["auth_requested"] = bool(auth_hit)

    company = lookup_company if isinstance(lookup_company, dict) else {}
    trades = lookup_trades if isinstance(lookup_trades, dict) else {}
    model = lookup_model if isinstance(lookup_model, dict) else {}
    lookup_refreshed = False
    model_refreshed = False
    single_mode = products_mode == "single" and bool(
        lookup_barcode_keys or lookup_text_query or lookup_pro_id
    )

    # في جلب صنف واحد: لا نعطّل العرض بانتظار company/trades
    # لكن عند force_model_refresh نجلب model لتحديث اسم الموقع
    if not single_mode:
        if need_lookup_refresh or not company or not trades:
            t0 = time.perf_counter()
            company, scheme = store_api_download(
                host,
                token,
                "company",
                verify_ssl=verify_ssl,
                reauth=_reauth,
                record_request=_rec,
                preferred_scheme=scheme or preferred_scheme,
                timeout=_TIMEOUT_LOOKUP_TABLE_SEC,
            )
            timing["company_ms"] = int((time.perf_counter() - t0) * 1000)
            t0 = time.perf_counter()
            trades, scheme = store_api_download(
                host,
                token,
                "trades",
                verify_ssl=verify_ssl,
                reauth=_reauth,
                record_request=_rec,
                preferred_scheme=scheme or preferred_scheme,
                timeout=_TIMEOUT_LOOKUP_TABLE_SEC,
            )
            timing["trades_ms"] = int((time.perf_counter() - t0) * 1000)
            lookup_refreshed = True
            need_model_refresh = True

        if need_model_refresh or not model:
            t0 = time.perf_counter()
            model, scheme = store_api_download(
                host,
                token,
                "model",
                verify_ssl=verify_ssl,
                reauth=_reauth,
                record_request=_rec,
                preferred_scheme=scheme or preferred_scheme,
                timeout=_TIMEOUT_LOOKUP_TABLE_SEC,
            )
            timing["model_ms"] = int((time.perf_counter() - t0) * 1000)
            model_refreshed = True
    elif force_model_refresh:
        t0 = time.perf_counter()
        model, scheme = store_api_download(
            host,
            token,
            "model",
            verify_ssl=verify_ssl,
            reauth=_reauth,
            record_request=_rec,
            preferred_scheme=scheme or preferred_scheme,
            timeout=_TIMEOUT_LOOKUP_TABLE_SEC,
        )
        timing["model_ms"] = int((time.perf_counter() - t0) * 1000)
        model_refreshed = True

    products_fetch_mode = "full"
    products_metrics: Dict[str, Any] = {}
    if single_mode:
        products, scheme, products_fetch_mode, _delta = fetch_products_for_lookup(
            host,
            token,
            barcode_keys=lookup_barcode_keys,
            text_query=lookup_text_query,
            pro_id=lookup_pro_id,
            verify_ssl=verify_ssl,
            reauth=_reauth,
            record_request=_rec,
            preferred_scheme=scheme or preferred_scheme,
            allow_full_catalog=False,
            bypass_mem_cache=bool(bypass_mem_cache),
            metrics=products_metrics,
        )
        timing.update(products_metrics)
    else:
        t0 = time.perf_counter()
        products, scheme = store_api_download(
            host,
            token,
            "products",
            _products_download_extra(pro_id="", all_flag="1"),
            verify_ssl=verify_ssl,
            reauth=_reauth,
            record_request=_rec,
            preferred_scheme=scheme or preferred_scheme,
            timeout=_TIMEOUT_FULL_SEC,
        )
        timing["products_ms"] = int((time.perf_counter() - t0) * 1000)
        timing["products_rows"] = len(_table_rows(products))
        _products_cache_set(host, products, scheme)

    timing["total_ms"] = int((time.perf_counter() - t_all) * 1000)
    timing["request_count"] = int(batch_count["n"])
    timing["skipped_lookup_tables"] = bool(single_mode and not model_refreshed and not lookup_refreshed)

    return {
        "token": token,
        "scheme": scheme,
        "auth_requested": auth_hit,
        "products": products,
        "company": company,
        "trades": trades,
        "model": model,
        "lookup_refreshed": lookup_refreshed,
        "model_refreshed": model_refreshed,
        "products_fetch_mode": products_fetch_mode,
        "request_count": int(batch_count["n"]),
        "timing_ms": timing,
        "token_exp_mono": time.monotonic() + _TOKEN_TTL_SEC,
        "token_exp_unix": time.time() + _TOKEN_TTL_SEC,
    }
