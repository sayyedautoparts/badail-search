import io
import logging
import os
import re
import socket
import sqlite3
import threading
import time
import unicodedata
import hashlib
import json
import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

logger = logging.getLogger("hesabate_sync")
from openpyxl import load_workbook
from pydantic import BaseModel, ConfigDict, field_validator

import hesabate_store_api as hesabate_api

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload
except Exception:  # pragma: no cover
    service_account = None
    build = None
    MediaIoBaseDownload = None


DEFAULT_DB_PATH = "/tmp/search_data.db" if os.getenv("VERCEL") else str(Path(__file__).with_name("search_data.db"))
DB_PATH = Path(os.getenv("APP_DB_PATH", DEFAULT_DB_PATH))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
SUPABASE_POSTGRES_URL = (
    os.getenv("SUPABASE_DB_URL")
    or os.getenv("SUPABASE_DATABASE_URL")
    or os.getenv("SUPABASE_POOLER_URL")
    or ""
).strip()
NEON_POSTGRES_URL = (os.getenv("NEON_DATABASE_URL") or "").strip()
POSTGRES_DB_URL = (SUPABASE_POSTGRES_URL or NEON_POSTGRES_URL or os.getenv("DATABASE_URL") or "").strip()
USE_POSTGRES = POSTGRES_DB_URL.startswith("postgres")


def detect_db_backend() -> str:
    if not USE_POSTGRES:
        return "sqlite"
    host = POSTGRES_DB_URL.lower()
    if "supabase" in host:
        return "supabase"
    if "neon" in host:
        return "neon"
    return "postgres"


DB_BACKEND = detect_db_backend()
CURRENT_YEAR = datetime.now().year
DIGIT_TRANS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
ARABIC_LETTER_TRANS = str.maketrans(
    {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ٱ": "ا",
        "ى": "ي",
        "ؤ": "و",
        "ئ": "ي",
        "ة": "ه",
        "ء": "",
        "ـ": "",
    }
)
APP_VERSION = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]
APP_DIR = Path(__file__).resolve().parent
APP_ICON_PATH = APP_DIR / "static" / "app-icon.png"
APPLE_TOUCH_ICON_PATH = APP_DIR / "static" / "apple-touch-icon.png"


def app_icon_cache_query() -> str:
    """بصمة قصيرة لملف الأيقونة — تُستخدم في ?v= لإجبار المتصفح على جلب شعار جديد."""
    try:
        return hashlib.sha256(APP_ICON_PATH.read_bytes()).hexdigest()[:12]
    except OSError:
        return "0"


def apple_touch_icon_cache_query() -> str:
    """بصمة لأيقونة «إضافة للشاشة الرئيسية» على iOS (منفصلة عن favicon التبويب)."""
    try:
        return hashlib.sha256(APPLE_TOUCH_ICON_PATH.read_bytes()).hexdigest()[:12]
    except OSError:
        return app_icon_cache_query()


INGEST_PARSER_VERSION = "5"
WIPER_PARSER_VERSION = "wiper-1"
LOCATION_PARSER_VERSION = "location-8"
LOCATION_SEARCH_LIMIT_DEFAULT = 200
LOCATION_SEARCH_LIMIT_MAX = 500
LOCATION_BARCODE_LIKE_FALLBACK_MIN_LEN = 5
LOCATION_BARCODE_LIKE_FALLBACK_FETCH_CAP = 450
LOCATION_BARCODE_SEARCH_KEYS_MAX = 6
BARCODE_SEARCH_DEBUG = os.getenv("BARCODE_SEARCH_DEBUG", "").strip().lower() in ("1", "true", "yes")

_location_norm_schema_lock = threading.Lock()
_location_norm_schema_ready = False
# اسم مصدر ثابت لرفع المواقع من تبويب «اكتشاف الموقع» (لا يعتمد على اسم الملف على الجهاز)
LOCATION_TAB_IMPORT_SOURCE_FILE = "مواقع_من_التطبيق.xlsx"
LOCATION_API_SOURCE_FILE = "حساباتي_API"
HESABATE_API_HOST = (
    os.getenv("HESABATE_API_HOST") or os.getenv("STORE_API_HOST") or ""
).strip()
HESABATE_API_USERNAME = (
    os.getenv("HESABATE_API_USERNAME") or os.getenv("STORE_API_USER") or ""
).strip()
HESABATE_API_PASSWORD = os.getenv("HESABATE_API_PASSWORD") or os.getenv("STORE_API_PASS") or ""
HESABATE_API_VERIFY_SSL = os.getenv("HESABATE_API_VERIFY_SSL", "").strip().lower() in (
    "1",
    "true",
    "yes",
)
try:
    # افتراضي 4 ساعات — GitHub يطرق المجدول؛ السيرفر يفرض الفاصل
    # (نافذة صمت ليلاً: انظر hesabate_auto_sync_allowed_now)
    HESABATE_SYNC_MIN_INTERVAL_SEC = max(
        60, int(os.getenv("HESABATE_SYNC_MIN_INTERVAL_SEC") or "14400")
    )
except (TypeError, ValueError):
    HESABATE_SYNC_MIN_INTERVAL_SEC = 14400
# صمت المزامنة التلقائية: 20:00–08:00 بتوقيت فلسطين — التشغيل 8 صباحًا حتى 8 مساءً
HESABATE_AUTO_SYNC_TZ = os.getenv("HESABATE_AUTO_SYNC_TZ") or "Asia/Jerusalem"
HESABATE_AUTO_SYNC_QUIET_START_HOUR = 21  # بعد آخر دفعة 8 مساءً
HESABATE_AUTO_SYNC_QUIET_END_HOUR = 8  # 8 صباحاً
# قفل مزامنة مشترك بين نسخ Vercel (session advisory lock)
_HESABATE_SYNC_ADVISORY_LOCK_KEY = 87233401
try:
    HESABATE_LOOKUP_CACHE_DAYS = max(1, min(90, int(os.getenv("HESABATE_LOOKUP_CACHE_DAYS") or "7")))
except (TypeError, ValueError):
    HESABATE_LOOKUP_CACHE_DAYS = 7
try:
    HESABATE_MONTHLY_REQUEST_LIMIT = max(
        1, int(os.getenv("HESABATE_MONTHLY_REQUEST_LIMIT") or "100000")
    )
except (TypeError, ValueError):
    HESABATE_MONTHLY_REQUEST_LIMIT = 100000
_hesabate_sync_lock = threading.Lock()
_hesabate_sync_inflight = False
_hesabate_usage_mem_lock = threading.Lock()
_hesabate_usage_pending: dict[str, Any] = {
    "delta": 0,
    "last_action": "",
    "last_type": "",
    "last_host": "",
    "last_at": "",
}


class DatabaseUnavailable(Exception):
    """Neon/Postgres down — must not become an uncaught 500."""


DB_QUOTA_MSG = (
    "قاعدة البيانات Neon متوقفة لأن الحساب تجاوز الحصة (Quota). "
    "افتح https://console.neon.tech وارفع الخطة أو انتظر تجديد الحصة، ثم أعد البحث."
)


def _postgres_connect():
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; add it to requirements first.")
    extra: dict[str, Any] = {"row_factory": dict_row, "connect_timeout": 8}
    # Vercel has no outbound IPv6; force IPv4 hostaddr when possible.
    try:
        from urllib.parse import urlparse

        parsed = urlparse(POSTGRES_DB_URL)
        host = parsed.hostname
        port = parsed.port or 5432
        if host:
            infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
            if infos:
                extra["hostaddr"] = infos[0][4][0]
    except Exception:
        extra.pop("hostaddr", None)
    return psycopg.connect(POSTGRES_DB_URL, **extra)


class DBConnection:
    def __init__(self, conn: Any, postgres: bool):
        self._conn = conn
        self._postgres = postgres

    def _adapt_sql(self, sql: str) -> str:
        if not self._postgres:
            return sql
        # Keep SQL source code sqlite-friendly and adapt placeholders for postgres.
        return sql.replace("?", "%s")

    def execute(self, sql: str, params: tuple | list = ()):
        return self._conn.execute(self._adapt_sql(sql), params)

    def executemany(self, sql: str, params_seq: list[tuple] | list[list]):
        if not self._postgres:
            return self._conn.executemany(self._adapt_sql(sql), params_seq)
        with self._conn.cursor() as cur:
            cur.executemany(self._adapt_sql(sql), params_seq)
            return cur

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    @property
    def postgres(self) -> bool:
        return self._postgres


def sql_row_to_dict(row: Any) -> dict[str, Any]:
    """تحويل صف قاعدة البيانات إلى dict — dict(sqlite3.Row) قد يرمي TypeError في بايثون 3.14+."""
    if isinstance(row, dict):
        return dict(row)
    keys_fn = getattr(row, "keys", None)
    if callable(keys_fn):
        keys = list(keys_fn())
        if keys:
            return {k: row[k] for k in keys}
    try:
        return dict(row)
    except Exception as exc:
        raise TypeError(f"unsupported row type {type(row)!r}") from exc


def _json_safe_value(v: Any) -> Any:
    """قيم آمنة لـ JSON (تفادي bytes/Decimal/datetime وغيرها التي تكسر FastAPI)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return float(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8", errors="replace")
    if isinstance(v, str):
        return v
    try:
        from decimal import Decimal

        if isinstance(v, Decimal):
            return str(v)
    except Exception:
        pass
    try:
        from datetime import date, datetime

        if isinstance(v, (date, datetime)):
            return v.isoformat()
    except Exception:
        pass
    return str(v)


def get_db() -> DBConnection:
    if USE_POSTGRES:
        try:
            conn = _postgres_connect()
        except DatabaseUnavailable:
            raise
        except Exception as exc:
            msg = str(exc).lower()
            if "quota" in msg or "exceeded" in msg:
                raise DatabaseUnavailable(DB_QUOTA_MSG) from exc
            raise DatabaseUnavailable(
                "تعذر الاتصال بقاعدة البيانات. أعد المحاولة بعد قليل."
            ) from exc
        return DBConnection(conn, postgres=True)

    conn = sqlite3.connect(DB_PATH, timeout=120)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout = 120000")
    except Exception:
        pass
    return DBConnection(conn, postgres=False)


def init_db() -> None:
    conn = get_db()
    try:
        if conn.postgres:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uploaded_files (
                    id BIGSERIAL PRIMARY KEY,
                    file_name TEXT UNIQUE NOT NULL,
                    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    rows_count INTEGER NOT NULL DEFAULT 0,
                    content_hash TEXT,
                    file_size BIGINT NOT NULL DEFAULT 0,
                    parser_version TEXT NOT NULL DEFAULT ''
                )
                """
            )
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uploaded_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT UNIQUE NOT NULL,
                    uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    rows_count INTEGER NOT NULL DEFAULT 0,
                    content_hash TEXT,
                    file_size INTEGER NOT NULL DEFAULT 0
                )
                """
            )
        # Keep compatibility with older DBs created before these columns existed.
        if conn.postgres:
            columns = {
                row["column_name"]
                for row in conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'uploaded_files'
                    """
                ).fetchall()
            }
        else:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(uploaded_files)").fetchall()}
        if "content_hash" not in columns:
            conn.execute("ALTER TABLE uploaded_files ADD COLUMN content_hash TEXT")
        if "file_size" not in columns:
            conn.execute("ALTER TABLE uploaded_files ADD COLUMN file_size INTEGER NOT NULL DEFAULT 0")
        if "parser_version" not in columns:
            conn.execute("ALTER TABLE uploaded_files ADD COLUMN parser_version TEXT NOT NULL DEFAULT ''")
        if conn.postgres:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id BIGSERIAL PRIMARY KEY,
                    item_name TEXT,
                    item_number TEXT,
                    company_number TEXT,
                    original_numbers TEXT,
                    notes TEXT,
                    alternatives TEXT,
                    size_width TEXT,
                    size_diameter TEXT,
                    size_height TEXT,
                    source_file TEXT NOT NULL,
                    source_sheet TEXT NOT NULL
                )
                """
            )
            product_columns = {
                row["column_name"]
                for row in conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'products'
                    """
                ).fetchall()
            }
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_name TEXT,
                    item_number TEXT,
                    company_number TEXT,
                    original_numbers TEXT,
                    notes TEXT,
                    alternatives TEXT,
                    size_width TEXT,
                    size_diameter TEXT,
                    size_height TEXT,
                    source_file TEXT NOT NULL,
                    source_sheet TEXT NOT NULL
                )
                """
            )
            product_columns = {row["name"] for row in conn.execute("PRAGMA table_info(products)").fetchall()}
        if "company_number" not in product_columns:
            conn.execute("ALTER TABLE products ADD COLUMN company_number TEXT")
        if "notes" not in product_columns:
            conn.execute("ALTER TABLE products ADD COLUMN notes TEXT")
        if "size_width" not in product_columns:
            conn.execute("ALTER TABLE products ADD COLUMN size_width TEXT")
        if "size_diameter" not in product_columns:
            conn.execute("ALTER TABLE products ADD COLUMN size_diameter TEXT")
        if "size_height" not in product_columns:
            conn.execute("ALTER TABLE products ADD COLUMN size_height TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_item_name ON products(item_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_alternatives ON products(alternatives)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_source_file ON products(source_file)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_company_number ON products(company_number)")
        if conn.postgres:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS wiper_rows (
                    id BIGSERIAL PRIMARY KEY,
                    car_name TEXT,
                    car_model TEXT,
                    engine TEXT,
                    wiper_location TEXT,
                    wiper_number TEXT,
                    source_file TEXT NOT NULL,
                    source_sheet TEXT NOT NULL
                )
                """
            )
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS wiper_rows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    car_name TEXT,
                    car_model TEXT,
                    engine TEXT,
                    wiper_location TEXT,
                    wiper_number TEXT,
                    source_file TEXT NOT NULL,
                    source_sheet TEXT NOT NULL
                )
                """
            )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wiper_car_name ON wiper_rows(car_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_wiper_source_file ON wiper_rows(source_file)")
        if conn.postgres:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS location_rows (
                    id BIGSERIAL PRIMARY KEY,
                    item_name TEXT,
                    location TEXT,
                    company TEXT,
                    original_number TEXT,
                    barcode TEXT,
                    brand TEXT,
                    notes TEXT,
                    quantity TEXT,
                    price TEXT,
                    source_file TEXT NOT NULL,
                    source_sheet TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS original_number TEXT"
            )
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS brand TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS notes TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS quantity TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS price TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS normalized_barcode TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS normalized_text TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS hesabate_product_id TEXT")
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS location_rows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_name TEXT,
                    location TEXT,
                    company TEXT,
                    original_number TEXT,
                    barcode TEXT,
                    brand TEXT,
                    notes TEXT,
                    quantity TEXT,
                    price TEXT,
                    source_file TEXT NOT NULL,
                    source_sheet TEXT NOT NULL
                )
                """
            )
            try:
                _loc_cols = {r["name"] for r in conn.execute("PRAGMA table_info(location_rows)").fetchall()}
                for col in (
                    "original_number",
                    "brand",
                    "notes",
                    "quantity",
                    "price",
                    "normalized_barcode",
                    "normalized_text",
                    "hesabate_product_id",
                ):
                    if col not in _loc_cols:
                        conn.execute(f"ALTER TABLE location_rows ADD COLUMN {col} TEXT")
            except Exception:
                pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_location_item_name ON location_rows(item_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_location_barcode ON location_rows(barcode)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_location_source_file ON location_rows(source_file)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_location_norm_barcode ON location_rows(normalized_barcode)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_location_hesabate_product_id ON location_rows(hesabate_product_id)"
        )
        try:
            # هوية مستقرة لمصدر حساباتي فقط — لا تمس Excel/البدائل
            if conn.postgres:
                conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_location_hesabate_pid
                    ON location_rows (source_file, hesabate_product_id)
                    WHERE hesabate_product_id IS NOT NULL AND TRIM(hesabate_product_id) <> ''
                    """
                )
            else:
                conn.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_location_hesabate_pid
                    ON location_rows (source_file, hesabate_product_id)
                    WHERE hesabate_product_id IS NOT NULL AND TRIM(hesabate_product_id) <> ''
                    """
                )
        except Exception:
            pass
        # Postgres: فهرس B-tree على نص طويل جداً يفشل (حد ~2704 بايت لصف الفهرس). LIKE %…% لا يستفيد منه عملياً.
        if conn.postgres:
            conn.execute("DROP INDEX IF EXISTS idx_location_norm_text")
        else:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_location_norm_text ON location_rows(normalized_text)")
        if conn.postgres:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shelf_locations (
                    barcode_key TEXT PRIMARY KEY,
                    location TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS shelf_locations (
                    barcode_key TEXT PRIMARY KEY,
                    location TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_shelf_locations_location ON shelf_locations(location)")
        # كاش أصناف حساباتي + فهرس باركود للجلب الفوري لصنف واحد
        if conn.postgres:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hesabate_product_cache (
                    product_id TEXT PRIMARY KEY,
                    barcode TEXT,
                    model_no TEXT,
                    other_codes TEXT,
                    name TEXT,
                    note TEXT,
                    company_id TEXT,
                    trade_id TEXT,
                    model_id TEXT,
                    amount TEXT,
                    price TEXT,
                    synced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hesabate_barcode_index (
                    barcode_key TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    PRIMARY KEY (barcode_key, product_id)
                )
                """
            )
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hesabate_product_cache (
                    product_id TEXT PRIMARY KEY,
                    barcode TEXT,
                    model_no TEXT,
                    other_codes TEXT,
                    name TEXT,
                    note TEXT,
                    company_id TEXT,
                    trade_id TEXT,
                    model_id TEXT,
                    amount TEXT,
                    price TEXT,
                    synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hesabate_barcode_index (
                    barcode_key TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    PRIMARY KEY (barcode_key, product_id)
                )
                """
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hesabate_barcode_index_key ON hesabate_barcode_index(barcode_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hesabate_product_cache_barcode ON hesabate_product_cache(barcode)"
        )
        conn.commit()
    finally:
        conn.close()


def file_fingerprint(file_bytes: bytes) -> tuple[str, int]:
    digest = hashlib.sha256(file_bytes).hexdigest()
    return digest, len(file_bytes)


def is_same_uploaded_file(file_name: str, content_hash: str, file_size: int) -> bool:
    source_key = normalize_source_file_name(file_name)
    conn = get_db()
    try:
        row = conn.execute(
            """
            SELECT content_hash, file_size, COALESCE(parser_version, '') AS parser_version
            FROM uploaded_files
            WHERE file_name = ?
            """,
            (source_key,),
        ).fetchone()
        if not row:
            return False
        return (
            (row["content_hash"] == content_hash)
            and (int(row["file_size"] or 0) == int(file_size))
            and (str(row["parser_version"] or "") == INGEST_PARSER_VERSION)
        )
    finally:
        conn.close()


def get_sync_meta() -> dict:
    conn = get_db()
    try:
        files = conn.execute(
            """
            SELECT file_name, COALESCE(content_hash, '') AS content_hash, COALESCE(file_size, 0) AS file_size, uploaded_at
            FROM uploaded_files
            ORDER BY file_name
            """
        ).fetchall()
        total_rows = conn.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"]
        updated_at = conn.execute("SELECT MAX(uploaded_at) AS ts FROM uploaded_files").fetchone()["ts"] or ""

        if files:
            fingerprint_input = "|".join(
                f"{f['file_name']}:{f['content_hash']}:{f['file_size']}:{f['uploaded_at']}" for f in files
            )
        else:
            fingerprint_input = f"rows:{total_rows}:updated:{updated_at}"
        version = hashlib.sha256(fingerprint_input.encode("utf-8")).hexdigest()[:16]
        return {"version": version, "updated_at": updated_at, "total_rows": total_rows, "files_count": len(files)}
    finally:
        conn.close()


def get_sync_rows() -> list[dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT item_name, item_number, company_number, original_numbers, notes, alternatives,
                   size_width, size_diameter, size_height, source_file, source_sheet
            FROM products
            ORDER BY id
            """
        ).fetchall()
        return [sql_row_to_dict(r) for r in rows]
    finally:
        conn.close()


class UploadRowsStartIn(BaseModel):
    file_name: str
    content_hash: str
    file_size: int


class UploadRowIn(BaseModel):
    item_name: str = ""
    item_number: str = ""
    company_number: str = ""
    original_numbers: str = ""
    notes: str = ""
    alternatives: str = ""
    size_width: str = ""
    size_diameter: str = ""
    size_height: str = ""
    source_sheet: str = "Sheet1"


class UploadRowsChunkIn(BaseModel):
    file_name: str
    rows: list[UploadRowIn]


class UploadRowsFinishIn(BaseModel):
    file_name: str
    content_hash: str
    file_size: int


class UploadRowsAbortIn(BaseModel):
    file_name: str


def _excel_json_cell_str(v: Any) -> str:
    """تحويل قيمة خلية Excel عند وصولها كـ JSON (رقم/نص/فارغ) إلى نص آمن للاستيراد."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float):
        if v != v:  # NaN
            return ""
        if abs(v - int(v)) < 1e-9:
            return str(int(v))
        return str(v).strip()
    if isinstance(v, int):
        return str(v)
    return str(v).strip()


class LocationRowIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    item_name: str = ""
    location: str = ""
    company: str = ""
    original_number: str = ""
    barcode: str = ""
    unit_barcodes: str = ""
    brand: str = ""
    notes: str = ""
    quantity: str = ""
    price: str = ""
    source_sheet: str = "Sheet1"

    @field_validator(
        "item_name",
        "location",
        "company",
        "original_number",
        "barcode",
        "unit_barcodes",
        "brand",
        "notes",
        "quantity",
        "price",
        "source_sheet",
        mode="before",
    )
    @classmethod
    def _coerce_excel_cell(cls, v: Any) -> str:
        return _excel_json_cell_str(v)


class LocationRowsStartIn(BaseModel):
    file_name: str
    content_hash: str
    file_size: int
    replace_entire_table: bool = False


class LocationRowsChunkIn(BaseModel):
    file_name: str
    rows: list[LocationRowIn]


class LocationRowsFinishIn(BaseModel):
    file_name: str
    content_hash: str
    file_size: int


class HesabateLookupIn(BaseModel):
    barcode: str = ""
    q: str = ""
    cache_only: bool = False
    # يتجاوز كاش الذاكرة وكاش DB ويجلب بالمعرّف الداخلي فقط (pro_id+all=1)
    refresh: bool = False
    # زر «جلب الصنف» فقط: إن لم يوجد pro_id محلي استورد الكتالوج ثم أعد البحث عن الصنف
    discover_new: bool = False
    # طباعة الباركود: تطابق تام بدون حذف أصفار وبدون بحث برقم الصنف
    barcode_exact: bool = False


class LocationRowsAbortIn(BaseModel):
    file_name: str


class WiperRowIn(BaseModel):
    car_name: str = ""
    car_model: str = ""
    engine: str = ""
    wiper_location: str = ""
    wiper_number: str = ""
    source_sheet: str = "Sheet1"


class WiperRowsStartIn(BaseModel):
    file_name: str
    content_hash: str
    file_size: int


class WiperRowsChunkIn(BaseModel):
    file_name: str
    rows: list[WiperRowIn]


class WiperRowsFinishIn(BaseModel):
    file_name: str
    content_hash: str
    file_size: int


class WiperRowsAbortIn(BaseModel):
    file_name: str


def upsert_uploaded_file_meta(file_name: str, rows_count: int, content_hash: str, file_size: int) -> None:
    source_key = normalize_source_file_name(file_name)
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO uploaded_files (file_name, rows_count, content_hash, file_size, parser_version)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(file_name) DO UPDATE SET
                uploaded_at = CURRENT_TIMESTAMP,
                rows_count = excluded.rows_count,
                content_hash = excluded.content_hash,
                file_size = excluded.file_size,
                parser_version = excluded.parser_version
            """,
            (source_key, rows_count, content_hash, file_size, INGEST_PARSER_VERSION),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_wiper_file_meta(file_name: str, rows_count: int, content_hash: str, file_size: int) -> None:
    source_key = normalize_source_file_name(file_name)
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO uploaded_files (file_name, rows_count, content_hash, file_size, parser_version)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(file_name) DO UPDATE SET
                uploaded_at = CURRENT_TIMESTAMP,
                rows_count = excluded.rows_count,
                content_hash = excluded.content_hash,
                file_size = excluded.file_size,
                parser_version = excluded.parser_version
            """,
            (source_key, rows_count, content_hash, file_size, WIPER_PARSER_VERSION),
        )
        conn.commit()
    finally:
        conn.close()


def is_wiper_spreadsheet_filename(file_name: str) -> bool:
    """يُعرَف ملف القشطان بالاسم (قشط/قشاط/wiper) مع دعم يونيكود NFC/NFD وأسماء ملفات عربية."""
    if not file_name:
        return False
    low = str(file_name).lower()
    if "wiper" in low or "wipers" in low:
        return True
    base = Path(str(file_name)).name
    variants = [
        base,
        unicodedata.normalize("NFC", base),
        unicodedata.normalize("NFD", base),
        str(file_name),
        unicodedata.normalize("NFC", str(file_name)),
    ]
    arabic_frags = ("قشط", "قشاط", "قشطان")
    for form in variants:
        for frag in arabic_frags:
            if frag in form:
                return True
    nb = normalize_text(unicodedata.normalize("NFC", base))
    for frag in arabic_frags:
        if normalize_text(frag) in nb:
            return True
    return False


def excel_bytes_looks_like_wiper_table(file_bytes: bytes) -> bool:
    """
    إن وُجد في الصف الأول إشارة لأعمدة القشط/المساحات يُعامل الملف كقشطان
    حتى لو تعذّر إرسال الاسم العربي من المتصفح بشكل صحيح.
    """
    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
        try:
            if not wb.sheetnames:
                return False
            ws = wb[wb.sheetnames[0]]
            preview = list(ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=6, values_only=True))
            if not preview:
                return False
            cells = [clean_cell(v) for v in preview[0]]
            joined = normalize_text(" ".join(cells))
            if any(k in joined for k in ("قشاط", "قشط", "مساح", "ماسحه", "ماسحة")):
                return True
            if "wiper" in joined:
                return True
            return False
        finally:
            wb.close()
    except Exception:
        return False


def should_process_as_wiper_spreadsheet(file_name: str, file_bytes: bytes) -> bool:
    return is_wiper_spreadsheet_filename(file_name) or excel_bytes_looks_like_wiper_table(file_bytes)


def is_same_wiper_upload(file_name: str, content_hash: str, file_size: int) -> bool:
    source_key = normalize_source_file_name(file_name)
    conn = get_db()
    try:
        row = conn.execute(
            """
            SELECT content_hash, file_size, COALESCE(parser_version, '') AS parser_version
            FROM uploaded_files
            WHERE file_name = ?
            """,
            (source_key,),
        ).fetchone()
        if not row:
            return False
        return (
            (row["content_hash"] == content_hash)
            and (int(row["file_size"] or 0) == int(file_size))
            and (str(row["parser_version"] or "") == WIPER_PARSER_VERSION)
        )
    finally:
        conn.close()


def upsert_location_file_meta(file_name: str, rows_count: int, content_hash: str, file_size: int) -> None:
    source_key = normalize_source_file_name(file_name)
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO uploaded_files (file_name, rows_count, content_hash, file_size, parser_version)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(file_name) DO UPDATE SET
                uploaded_at = CURRENT_TIMESTAMP,
                rows_count = excluded.rows_count,
                content_hash = excluded.content_hash,
                file_size = excluded.file_size,
                parser_version = excluded.parser_version
            """,
            (source_key, rows_count, content_hash, file_size, LOCATION_PARSER_VERSION),
        )
        conn.commit()
    finally:
        conn.close()


def is_location_spreadsheet_filename(file_name: str) -> bool:
    """ملف مواقع المخزن: من الاسم أو من صف العناوين."""
    if not file_name:
        return False
    low = str(file_name).lower()
    for frag in (
        "location",
        "warehouse",
        "barcode",
        "stock",
        "shelf",
        "bin",
        "rack",
    ):
        if frag in low:
            return True
    base = Path(str(file_name)).name
    variants = [
        base,
        unicodedata.normalize("NFC", base),
        unicodedata.normalize("NFD", base),
        str(file_name),
    ]
    for form in variants:
        for frag in (
            "موقع",
            "مواقع",
            "اكتشاف",
            "مخزن",
            "مستودع",
            "رف",
            "باركود",
            "اماكن",
            "أماكن",
        ):
            if frag in form:
                return True
    nb = normalize_text(unicodedata.normalize("NFC", base))
    for frag in ("موقع", "باركود", "مخزن", "اكتشاف"):
        if normalize_text(frag) in nb:
            return True
    return False


def excel_bytes_looks_like_location_table(file_bytes: bytes) -> bool:
    try:
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
        try:
            if not wb.sheetnames:
                return False
            ws = wb[wb.sheetnames[0]]
            preview = list(ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=7, values_only=True))
            if not preview:
                return False
            cells = [clean_cell(v) for v in preview[0]]
            joined = normalize_text(" ".join(cells))
            has_loc = any(k in joined for k in ("موقع", "مكان", "رف", "location", "bin", "shelf"))
            has_bc = any(k in joined for k in ("باركود", "barcode", "ean", "upc"))
            has_item = any(k in joined for k in ("صنف", "اسم", "وصف", "item", "sku"))
            if has_bc and (has_loc or has_item):
                return True
            if "شركة" in joined or "company" in joined:
                if has_loc and has_item:
                    return True
            return False
        finally:
            wb.close()
    except Exception:
        return False


def should_process_as_location_spreadsheet(file_name: str, file_bytes: bytes) -> bool:
    return is_location_spreadsheet_filename(file_name) or excel_bytes_looks_like_location_table(file_bytes)


def is_same_location_upload(file_name: str, content_hash: str, file_size: int) -> bool:
    source_key = normalize_source_file_name(file_name)
    conn = get_db()
    try:
        row = conn.execute(
            """
            SELECT content_hash, file_size, COALESCE(parser_version, '') AS parser_version
            FROM uploaded_files
            WHERE file_name = ?
            """,
            (source_key,),
        ).fetchone()
        if not row:
            return False
        return (
            (row["content_hash"] == content_hash)
            and (int(row["file_size"] or 0) == int(file_size))
            and (str(row["parser_version"] or "") == LOCATION_PARSER_VERSION)
        )
    finally:
        conn.close()


# أخطاء إملائية شائعة ↔ الشكل الصحيح (بحث أي شكل يعيد الاثنين)
SEARCH_SPELLING_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"focus", "foucs"}),
    frozenset({"tucson", "toucson"}),
)
SEARCH_SPELLING_CANON: dict[str, str] = {
    "foucs": "focus",
    "toucson": "tucson",
}


def apply_spelling_aliases(text: str) -> str:
    """توحيد Foucs→Focus و Toucson→Tucson داخل النص المطبع (كلمات لاتينية فقط)."""
    if not text:
        return text

    def repl(m: re.Match[str]) -> str:
        w = m.group(0)
        return SEARCH_SPELLING_CANON.get(w, w)

    return re.sub(r"[a-z]+", repl, text)


def spelling_query_variants(token: str) -> list[str]:
    """كل أشكال الكتابة المكافئة لتوسيع LIKE في SQL (foucs↔focus، toucson↔tucson)."""
    raw = str(token or "").translate(DIGIT_TRANS).strip().lower()
    raw = re.sub(r"[\s\-_]+", " ", raw).strip()
    if not raw:
        return []
    group: frozenset[str] | None = None
    for g in SEARCH_SPELLING_GROUPS:
        if raw in g:
            group = g
            break
    if group is None:
        # توكن مطبّع مسبقاً (مثل focus بعد توحيد foucs)
        canon_raw = apply_spelling_aliases(raw)
        for g in SEARCH_SPELLING_GROUPS:
            if canon_raw in g:
                group = g
                break
    if group is None:
        return [raw]
    out: list[str] = []
    if raw in group:
        out.append(raw)
    else:
        canon = apply_spelling_aliases(raw)
        if canon in group:
            out.append(canon)
    for v in sorted(group):
        if v not in out:
            out.append(v)
    return out


def normalize_text(text: str) -> str:
    text = str(text).translate(DIGIT_TRANS).translate(ARABIC_LETTER_TRANS).strip().lower()
    text = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", text)
    text = re.sub(r"[\s\-_]+", " ", text)
    text = apply_spelling_aliases(text)
    return text


def normalize_text_compact(text: str) -> str:
    n = normalize_text(text)
    return re.sub(r"[\s\-_/]+", "", n)


# فواصل شائعة في خانة باركود حساباتي/Excel (بما فيها الفاصلة العربية و+)
_BARCODE_FIELD_SPLIT_RE = re.compile(
    r"[\s,/|;\\+]+|"
    r"[،؛]+|"  # فاصلة/فاصلة منقوطة عربية
    r"[\u200e\u200f\u202a-\u202e]+|"  # علامات اتجاه
    r"(?:<br\s*/?>)|(?:&nbsp;)",
    flags=re.I,
)


def _split_concatenated_digit_barcodes(digits: str) -> list[str]:
    """
    إن وُجدت أرقام ملتصقة بدون فاصل (مثل باركودين EAN-13)، حاول فصلها بأطوال شائعة.
    الأولوية للأطوال المتساوية المعروفة (13/12/14/8) حتى لا يُقطع باركود بالخطأ.
    """
    d = str(digits or "")
    if not d.isdigit() or len(d) < 16:
        return [d] if d else []
    for part_len in (13, 12, 14, 8):
        if len(d) % part_len == 0 and len(d) // part_len >= 2:
            parts = [d[i : i + part_len] for i in range(0, len(d), part_len)]
            if all(len(p) >= 6 for p in parts):
                return parts
    lengths = (13, 12, 14, 8)
    for first_len in lengths:
        if len(d) <= first_len:
            continue
        rest = d[first_len:]
        if len(rest) in lengths:
            a, b = d[:first_len], rest
            if len(a) >= 6 and len(b) >= 6:
                return [a, b]
    return [d]


def _coalesce_short_digit_barcode_parts(parts: list[str]) -> list[str]:
    """
    باركود واحد قد يُكتب بمسافات تنسيق (18843 10062) — أعد دمجه.
    أما القطع الطويلة (>=8) فتبقى باركودات مستقلة متعددة.
    """
    if len(parts) < 2:
        return parts
    out: list[str] = []
    buf: list[str] = []

    def flush() -> None:
        nonlocal buf
        if not buf:
            return
        if len(buf) >= 2 and all(p.isdigit() and len(p) < 8 for p in buf):
            joined = "".join(buf)
            if 6 <= len(joined) <= 18:
                out.append(joined)
            else:
                out.extend(buf)
        else:
            out.extend(buf)
        buf = []

    for p in parts:
        if p.isdigit() and len(p) < 8:
            buf.append(p)
        else:
            flush()
            out.append(p)
    flush()
    return out


def _should_index_compact_spaced_barcode(raw: str, parts: list[str], full: str) -> bool:
    """
    باركود حساباتي بمسافات تنسيق + لاحقة (مثل 223112U001        HI → 223112U001HI).
    لا ندمج باركودين رقميين طويلين منفصلين.
    """
    if not full or not raw or not re.search(r"\s", raw):
        return False
    if len(parts) >= 2 and all(p.isdigit() and len(p) >= 8 for p in parts):
        return False
    # لاحقة قصيرة / حروف داخل الباركود → شكل واحد مدمج للبحث
    if any(re.search(r"[A-Za-z]", p) for p in (parts or [full])):
        return True
    # أجزاء رقمية قصيرة بمسافات تنسيق (يُغطّيها coalesce غالباً؛ نُبقي المدمج احتياطاً)
    if len(parts) >= 2 and all(p.isdigit() and len(p) < 8 for p in parts):
        return True
    return False


def _extract_location_unit_barcodes_from_cell(unit_barcodes_raw: str) -> list[str]:
    """
    استخراج كل الباركودات من خانة قد تحتوي أكثر من قيمة:
    - Excel: ':512312321 :4255761249700'
    - حساباتي: مسافة / فاصلة عربية أو لاتينية / ; / | / + / \\ / سطر جديد
    - أرقام ملتصقة بطول باركودين معروفين
    - مسافات التنسيق داخل باركود واحد قصير الأجزاء تُدمَج
    - باركود حساباتي مع لاحقة بعد مسافات (223112U001        HI) يُفهرس مدمجاً + أجزاؤه
    """
    raw = str(unit_barcodes_raw or "").strip()
    if not raw:
        return []
    out: list[str] = []

    def _add(part: str) -> None:
        nb = normalize_barcode_lookup_key(part)
        if not nb:
            return
        pieces = _split_concatenated_digit_barcodes(nb) if nb.isdigit() else [nb]
        for p in pieces:
            if p and p not in out:
                out.append(p)

    raw_parts: list[str] = []
    # 1) صيغة Excel: كل قيمة بعد ':'
    for part in re.findall(r":\s*([^\s:;,|/\\+،؛]+)", raw):
        part = part.strip()
        if part:
            raw_parts.append(part)
    # 2) إزالة مقاطع ':code' ثم تقسيم بكل الفواصل المعروفة
    cleaned = re.sub(r":\s*[^\s:;,|/\\+،؛]+", " ", raw)
    cleaned = cleaned.replace("\r", "\n")
    for part in _BARCODE_FIELD_SPLIT_RE.split(cleaned):
        part = part.strip().strip(":").strip()
        if part:
            raw_parts.append(part)
    if not raw_parts:
        raw_parts = [raw]
    for part in _coalesce_short_digit_barcode_parts(raw_parts):
        _add(part)
    # الشكل المدمج الكامل (مسافات التنسيق + لاحقة HI/KI19…)
    full = normalize_barcode_lookup_key(raw)
    if full and full not in out and _should_index_compact_spaced_barcode(raw, list(out), full):
        out.insert(0, full)
    return out


def location_row_normalized_barcode_value(barcode_raw: str, unit_barcodes_raw: str = "") -> str:
    """
    تطبيع كل باركودات الصف كمفاتيح مستقلة:
    - حقل الباركود الرئيسي (قد يحتوي أكثر من باركود)
    - other_codes / باركود الوحدات
    يُخزَّن: " KEY1 KEY2 KEY3 " للمطابقة الدقيقة بالتوكن.
    """
    out: list[str] = []
    for src in (barcode_raw, unit_barcodes_raw):
        for tok in _extract_location_unit_barcodes_from_cell(src):
            if tok not in out:
                out.append(tok)
    if not out:
        return ""
    return f" {' '.join(out)} "


# عند تغيير منطق تفكيك الباركود المتعدد — نفرض إعادة فهرسة حتى لو لم يتغير كتالوج حساباتي
HESABATE_BARCODE_PARSER_VERSION = "multi-bc-5"


def location_row_normalized_text_value(
    item_name: str,
    company: str,
    brand: str,
    notes: str,
    original_number: str,
) -> str:
    """
    حقول نصية للبحث (بدون الباركود): نسخة مطبّعة + مدمجة + رقم أصلي بلا أصفار بادئة لتحسين التطابق.
    """
    raw = f"{item_name or ''} {company or ''} {brand or ''} {notes or ''} {original_number or ''}"
    spaced = normalize_text(raw)
    compact = normalize_text_compact(raw)
    parts: list[str] = []
    if spaced:
        parts.append(spaced)
    if compact:
        parts.append(compact)
    od = normalize_barcode(original_number or "")
    if od:
        parts.append(od.lstrip("0") or "0")
    return " ".join(parts).strip()


def ensure_location_rows_import_schema(conn: DBConnection) -> None:
    """
    قبل INSERT مواقع (رفع Excel أو دفعات): يضمن أعمدة quantity وnormalized_* وhesabate_product_id.
    يُستدعى من مسارات الرفع لتفادي أخطاء «no column named …» على Vercel/Neon.
    """
    try:
        if conn.postgres:
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS quantity TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS normalized_barcode TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS normalized_text TEXT")
            conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS hesabate_product_id TEXT")
        else:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(location_rows)").fetchall()}
            for col in ("quantity", "normalized_barcode", "normalized_text", "hesabate_product_id"):
                if col not in cols:
                    conn.execute(f"ALTER TABLE location_rows ADD COLUMN {col} TEXT")
                    cols.add(col)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_location_hesabate_product_id ON location_rows(hesabate_product_id)"
        )
        try:
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_location_hesabate_pid
                ON location_rows (source_file, hesabate_product_id)
                WHERE hesabate_product_id IS NOT NULL AND TRIM(hesabate_product_id) <> ''
                """
            )
        except Exception:
            pass
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def ensure_location_normalized_schema(conn: DBConnection) -> None:
    """يضمن وجود أعمدة البحث المطبّع + فهارسها — مرة واحدة لكل عملية خادم (لا نكرر ALTER/INDEX كل بحث).
    على Postgres لا يُنشأ فهرس normalized_text (صفوف طويلة تكسر حد B-tree؛ البحث يبقى عبر الاستعلام نفسه)."""
    global _location_norm_schema_ready
    if _location_norm_schema_ready:
        return
    with _location_norm_schema_lock:
        if _location_norm_schema_ready:
            return
        try:
            if conn.postgres:
                conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS quantity TEXT")
                conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS normalized_barcode TEXT")
                conn.execute("ALTER TABLE location_rows ADD COLUMN IF NOT EXISTS normalized_text TEXT")
            else:
                cols = {r["name"] for r in conn.execute("PRAGMA table_info(location_rows)").fetchall()}
                for col in ("quantity", "normalized_barcode", "normalized_text"):
                    if col not in cols:
                        conn.execute(f"ALTER TABLE location_rows ADD COLUMN {col} TEXT")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_location_norm_barcode ON location_rows(normalized_barcode)")
            if conn.postgres:
                conn.execute("DROP INDEX IF EXISTS idx_location_norm_text")
            else:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_location_norm_text ON location_rows(normalized_text)")
            conn.commit()
            _location_norm_schema_ready = True
        except Exception:
            # Postgres: أي فشل يُبقي المعاملة في حالة aborted — لازم rollback وإلا كل استعلام لاحق يفشل بصمت ثم 500
            try:
                conn.rollback()
            except Exception:
                pass


def backfill_location_normalized_columns(
    conn: DBConnection, *, max_per_boot: int = 4000, batch_size: int = 800
) -> None:
    """
    تعبئة الأعمدة المطبّعة للصفوف القديمة — على دفعات مع commit.
    لا تُستدعى عند إقلاع التطبيق (لتفادي تجمد Vercel)؛ تُستدعى من بحث المواقع لإكمال الترحيل تدريجياً.
    """
    if max_per_boot <= 0:
        return
    processed = 0
    while processed < max_per_boot:
        try:
            rows = conn.execute(
                """
                SELECT id, item_name, company, brand, notes, original_number, barcode
                FROM location_rows
                WHERE normalized_text IS NULL
                   OR normalized_barcode IS NULL
                   OR (
                     COALESCE(TRIM(barcode), '') <> ''
                     AND TRIM(COALESCE(normalized_barcode, '')) = ''
                   )
                LIMIT ?
                """,
                (batch_size,),
            ).fetchall()
        except Exception:
            return
        if not rows:
            return
        updates: list[tuple[str, str, Any]] = []
        for r in rows:
            rd = sql_row_to_dict(r)
            nb = location_row_normalized_barcode_value(str(rd.get("barcode") or ""))
            nt = location_row_normalized_text_value(
                str(rd.get("item_name") or ""),
                str(rd.get("company") or ""),
                str(rd.get("brand") or ""),
                str(rd.get("notes") or ""),
                str(rd.get("original_number") or ""),
            )
            updates.append((nb, nt, rd["id"]))
        conn.executemany(
            "UPDATE location_rows SET normalized_barcode = ?, normalized_text = ? WHERE id = ?",
            updates,
        )
        conn.commit()
        processed += len(rows)


def _norm_segment_word_set(nseg: str) -> set[str]:
    """كلمات نص مُطبَّع (لتفادي انطباق حرف d داخل كلمة cady)."""
    out: set[str] = set()
    for w in (nseg or "").split():
        w = w.strip(".,|;:()[]")
        if w:
            out.add(w)
    return out


def _token_in_norm_segment(ntok: str, nseg: str) -> bool:
    if not ntok:
        return False
    wset = _norm_segment_word_set(nseg)
    if len(ntok) <= 2:
        return ntok in wset
    if ntok in wset:
        return True
    return ntok in nseg


def _token_appears_in_alternatives_column(ntok: str, alternatives: str) -> bool:
    """التوكن يظهر في عمود البدائل ككلمة مستقلة وليس حرفاً داخل كلمة أخرى."""
    nalt = normalize_text(alternatives)
    if not ntok:
        return False
    wset = _norm_segment_word_set(nalt)
    if len(ntok) <= 2:
        return ntok in wset
    if ntok in wset:
        return True
    return ntok in nalt


def _tokens_matching_alternatives_column(text_tokens: list[str], alternatives: str) -> list[str]:
    """كلمات البحث التي تخص عمود البدائل فقط (تجاهل ما يطابق اسم الصنف أو الملف فقط)."""
    seen: set[str] = set()
    out: list[str] = []
    for t in text_tokens:
        nt = normalize_text(t)
        if not nt or nt in seen:
            continue
        if not _token_appears_in_alternatives_column(nt, alternatives):
            continue
        seen.add(nt)
        out.append(nt)
    return out


def _is_word_char_for_alt_match(ch: str) -> bool:
    """حرف يُعتبر جزءاً من «كلمة» لحدود التطابق (طراز، سعة، إلخ)."""
    return bool(ch) and (ch.isalnum() or ch in "+.")


def _find_whole_word_span(haystack: str, needle: str) -> tuple[int, int] | None:
    """أول ظهور لـ needle في haystack ككلمة كاملة؛ يعيد (بداية، نهاية) على النص الأصلي."""
    if not needle or not haystack:
        return None
    h = haystack.casefold()
    n = needle.casefold()
    pos = 0
    while True:
        idx = h.find(n, pos)
        if idx < 0:
            return None
        end = idx + len(n)
        left_ok = idx == 0 or not _is_word_char_for_alt_match(haystack[idx - 1])
        right_ok = end >= len(haystack) or not _is_word_char_for_alt_match(haystack[end])
        if left_ok and right_ok:
            return (idx, end)
        pos = idx + 1


def _anchor_priority_for_slice_token(t: str) -> tuple[int, int]:
    """
    ترتيب أصغر = مرساة أفضل لبداية المقطع المعروض.
    نفضّل طرازاً/اسماً (fabia، octavia، عربي) على رقم سعة مكرر (1.9 يظهر في أكثر من مقطع).
    """
    nt = normalize_text(t)
    if not nt:
        return (99, 0)
    if re.fullmatch(r"\d{1,2}\.\d{1,3}", nt):
        return (4, -len(nt))
    if re.fullmatch(r"\d{1,2}", nt):
        return (3, -len(nt))
    if re.search(r"[a-z\u0600-\u06ff]", nt):
        return (0, -len(nt))
    return (2, -len(nt))


def _best_anchor_span_for_slice(alternatives: str, tokens: list[str]) -> tuple[int, int] | None:
    """موضع بداية المقطع: مرساة من التوكن الأنسب لا أبكر رقم في النص."""
    if not tokens:
        return None
    ordered = sorted(tokens, key=_anchor_priority_for_slice_token)
    for tok in ordered:
        span = _find_whole_word_span(alternatives, tok)
        if span is not None:
            return span
    return None


def _slice_alternatives_from_first_match_to_slash(alternatives: str, tokens: list[str]) -> str | None:
    """
    من مرساة أنسب (طراز/نص قبل 1.9 إن وُجد) حتى أول «/» بعدها،
    أو حتى نهاية الخانة إن لم يوجد /.
    """
    span = _best_anchor_span_for_slice(alternatives, tokens)
    if span is None:
        return None
    st, _ = span
    slash_at = _next_segment_slash(alternatives, st)
    if slash_at < 0:
        return alternatives[st:].strip()
    return alternatives[st:slash_at].strip()


def tokenize_query(text: str) -> list[str]:
    raw = str(text).translate(DIGIT_TRANS).strip().lower()
    raw = re.sub(r"[،,;/|]+", " ", raw)
    # أقواس حول سعة المحرك مثل (1.6) → 1.6
    raw = re.sub(r"[\(\)]+", " ", raw)
    # دمج سعة المحرك: 1 . 6 أو 1,6 أو 1،6 → 1.6 (السنة بدون نقطة عشرية)
    raw = re.sub(r"(\d{1,2})\s*[.,،]\s*(\d{1,3})(?!\d)", r"\1.\2", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    tokens = [t for t in raw.split(" ") if t]
    # Standalone symbols are separators, not meaningful tokens.
    return [t for t in tokens if t not in {"+", "-", "–", "—"}]


def split_query_display_words(q: str) -> list[str]:
    """كلمات الاستعلام مع الإبقاء على شكل الكتابة (للعرض تحت نتيجة البحث)."""
    raw = str(q).translate(DIGIT_TRANS).strip()
    raw = re.sub(r"[،,;/|]+", " ", raw)
    raw = re.sub(r"[\(\)]+", " ", raw)
    raw = re.sub(r"(\d{1,2})\s*[.,،]\s*(\d{1,3})(?!\d)", r"\1.\2", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    tokens = [t for t in raw.split(" ") if t]
    return [t for t in tokens if t not in {"+", "-", "–", "—"}]


def display_forms_for_text_tokens(text_tokens: list[str], raw_query: str) -> list[str]:
    """يطابق كل توكن بحث مع أول كلمة في الاستعلام الأصلي لنفس التطبيع (للحفاظ على Cady مثلاً)."""
    words = split_query_display_words(raw_query)
    used_word_idx: set[int] = set()
    forms: list[str] = []
    for t in text_tokens:
        nt = normalize_text(t)
        found: str | None = None
        for i, w in enumerate(words):
            if i in used_word_idx:
                continue
            if normalize_text(w) == nt:
                found = w
                used_word_idx.add(i)
                break
        forms.append(found if found is not None else t)
    return forms


def parse_year_token(token: str) -> int | None:
    cleaned = re.sub(r"[^\d]", "", str(token).translate(DIGIT_TRANS))
    if len(cleaned) not in (2, 4):
        return None
    year = int(cleaned)
    if len(cleaned) == 2:
        return 2000 + year if year <= 30 else 1900 + year
    return year


def parse_query_year_token(token: str) -> list[int] | None:
    raw = str(token).translate(DIGIT_TRANS).strip().lower()

    # 1.6 / 2.5 = سعة محرك وليست سنة
    if re.fullmatch(r"\d{1,2}\.\d{1,3}", raw):
        return None

    # +15 or 15+ => 2015 .. current year
    if re.fullmatch(r"(?:\+\d{2,4}|\d{2,4}\+)", raw):
        start_token = raw[1:] if raw.startswith("+") else raw[:-1]
        start = parse_year_token(start_token)
        if start is None:
            return None
        return list(range(start, CURRENT_YEAR + 1))

    # 10-15 => 2010 .. 2015 (range token only)
    match = re.fullmatch(r"(\d{2,4})\s*[-–—]\s*(\d{2,4})", raw)
    if match:
        start = parse_year_token(match.group(1))
        end = parse_year_token(match.group(2))
        if start is None or end is None:
            return None
        low = min(start, end)
        high = max(start, end)
        return list(range(low, high + 1))

    # -07 or 07- => up to 2007 (inclusive).
    if re.fullmatch(r"(?:-\d{2,4}|\d{2,4}-)", raw):
        end_token = raw[1:] if raw.startswith("-") else raw[:-1]
        end = parse_year_token(end_token)
        if end is None:
            return None
        return list(range(1900, end + 1))

    # Single explicit 2/4-digit token is a single year (e.g. 11 => 2011).
    # One-digit tokens (e.g. 2) remain normal text for model numbers.
    if re.fullmatch(r"\d{2,4}", raw):
        year = parse_year_token(raw)
        if year is not None:
            return [year]

    # Other tokens (e.g. 2, 1.6, octavia2) are normal text tokens.
    return None


def is_explicit_year_operator_token(token: str) -> bool:
    raw = str(token).translate(DIGIT_TRANS).strip().lower()
    return bool(
        re.fullmatch(r"(?:\+\d{2,4}|\d{2,4}\+)", raw)
        or re.fullmatch(r"(\d{2,4})\s*[-–—]\s*(\d{2,4})", raw)
        or re.fullmatch(r"(?:-\d{2,4}|\d{2,4}-)", raw)
    )


def year_in_range_text(year: int, text: str) -> bool:
    if not text:
        return False

    # Keep range symbols like "-" and "+" for year parsing.
    normalized = str(text).translate(DIGIT_TRANS).strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)

    # Exact standalone year tokens (e.g. 2011, 11).
    for match in re.finditer(r"(?<!\d)(\d{4})(?!\d)", normalized):
        if int(match.group(1)) == year:
            return True
    for match in re.finditer(r"(?<!\d)(\d{2})(?!\d)", normalized):
        token_year = parse_year_token(match.group(1))
        if token_year == year:
            return True

    # Chained plus markers, e.g. +03,+08 => 03-07 and +08.
    plus_marks: list[tuple[int, int]] = []
    for match in re.finditer(r"(?:\+\s*(\d{2,4})(?!\d)|(?<!\d)(\d{2,4})\s*\+(?!\d))", normalized):
        token = match.group(1) or match.group(2)
        start = parse_year_token(token)
        if start is not None:
            plus_marks.append((match.start(), start))
    if plus_marks:
        plus_marks.sort(key=lambda x: x[0])
        for i, (_, start) in enumerate(plus_marks):
            if i + 1 < len(plus_marks):
                next_start = plus_marks[i + 1][1]
                end = next_start - 1
            else:
                end = CURRENT_YEAR
            if start <= year <= end:
                return True

    # Supports formats like: "lancer +04" or "lancer 04+" => 2004 .. current year.
    for match in re.finditer(r"\+\s*(\d{2,4})(?!\d)", normalized):
        start = parse_year_token(match.group(1))
        if start is not None and start <= year <= CURRENT_YEAR:
            return True
    for match in re.finditer(r"(?<!\d)(\d{2,4})\s*\+(?!\d)", normalized):
        start = parse_year_token(match.group(1))
        if start is not None and start <= year <= CURRENT_YEAR:
            return True

    for match in re.finditer(r"(?<!\d)(\d{2,4})\s*[-–—]\s*(\d{2,4})(?!\d)", normalized):
        start = parse_year_token(match.group(1))
        end = parse_year_token(match.group(2))
        if start is None or end is None:
            continue
        low = min(start, end)
        high = max(start, end)
        if low <= year <= high:
            return True

    # Chained minus markers, e.g. -03,-08 => <=03 and 04-08.
    minus_marks: list[tuple[int, int]] = []
    for match in re.finditer(r"(?:[-–—]\s*(\d{2,4})(?!\d)|(?<!\d)(\d{2,4})\s*[-–—](?!\s*\d))", normalized):
        token = match.group(1) or match.group(2)
        end = parse_year_token(token)
        if end is not None:
            minus_marks.append((match.start(), end))
    if minus_marks:
        minus_marks.sort(key=lambda x: x[0])
        first_end = minus_marks[0][1]
        if year <= first_end:
            return True
        for i in range(1, len(minus_marks)):
            prev_end = minus_marks[i - 1][1]
            cur_end = minus_marks[i][1]
            low = prev_end + 1
            high = cur_end
            if low <= high and low <= year <= high:
                return True

    # Supports open-ended formats like: "-07" or "07-" => up to 2007.
    for match in re.finditer(r"(?<!\d)[-–—]\s*(\d{2,4})(?!\d)", normalized):
        end = parse_year_token(match.group(1))
        if end is not None and year <= end:
            return True
    for match in re.finditer(r"(?<!\d)(\d{2,4})\s*[-–—](?!\s*\d)", normalized):
        end = parse_year_token(match.group(1))
        if end is not None and year <= end:
            return True
    return False


_AT_MT_SPLIT_PATTERN = re.compile(
    r"(?i)(?<![A-Za-z0-9/])(?:at\s*/\s*mt|mt\s*/\s*at)(?![A-Za-z0-9/])"
)


def _at_mt_protected_ranges(text: str) -> list[tuple[int, int]]:
    """نطاقات at/mt و mt/at (أي حالة أحرف) — لا نعتبر / داخلها فاصلاً بين السيارات."""
    return [(m.start(), m.end()) for m in _AT_MT_SPLIT_PATTERN.finditer(text or "")]


def _char_in_any_range(idx: int, ranges: list[tuple[int, int]]) -> bool:
    return any(a <= idx < b for a, b in ranges)


def _next_segment_slash(alternatives: str, start: int) -> int:
    """أول / بعد start يكون فاصلاً بين مقطعين وليس جزءاً من at/mt."""
    ranges = _at_mt_protected_ranges(alternatives)
    i = start
    n = len(alternatives)
    while i < n:
        if alternatives[i] != "/":
            i += 1
            continue
        if _char_in_any_range(i, ranges):
            i += 1
            continue
        return i
    return -1


def split_alternative_segments(alternatives: str) -> list[str]:
    if not alternatives:
        return []
    ranges = _at_mt_protected_ranges(alternatives)
    parts: list[str] = []
    cur_start = 0
    for i, ch in enumerate(alternatives):
        is_delim = ch in "|,\n;" or (ch == "/" and not _char_in_any_range(i, ranges))
        if is_delim:
            piece = alternatives[cur_start:i].strip()
            if piece:
                parts.append(piece)
            cur_start = i + 1
    tail = alternatives[cur_start:].strip()
    if tail:
        parts.append(tail)
    return parts


def split_year_chunks(text: str) -> list[str]:
    if not text:
        return []
    chunks = [
        c.strip()
        for c in re.findall(r"[^/|,\n;]*?(?:\+\s*\d{2,4}|\d{2,4}\s*[-–—]\s*\d{2,4}|\d{2,4}\s*[-–—])", text)
        if c and c.strip()
    ]
    return chunks or [text.strip()]


def year_match_score(segment: str, years: list[int]) -> int:
    if not segment or not years:
        return 0

    normalized = str(segment).translate(DIGIT_TRANS).strip().lower()
    best = 0

    # Exact year token (e.g. 2011 or 11) - highest precision.
    for y in years:
        for match in re.finditer(r"(?<!\d)(\d{4})(?!\d)", normalized):
            if int(match.group(1)) == y:
                best = max(best, 6000)
        for match in re.finditer(r"(?<!\d)(\d{2})(?!\d)", normalized):
            yy = parse_year_token(match.group(1))
            if yy == y:
                best = max(best, 5500)

    # +15 or 15+ => start..current (prefer higher start when matching same year).
    for match in re.finditer(r"(?:\+\s*(\d{2,4})|(?<!\d)(\d{2,4})\s*\+)", normalized):
        token = match.group(1) or match.group(2)
        start = parse_year_token(token)
        if start is None:
            continue
        if all(start <= y <= CURRENT_YEAR for y in years):
            best = max(best, 4000 + start)

    # -07 or 07- => up to end year (prefer tighter upper bound).
    for match in re.finditer(r"(?:[-–—]\s*(\d{2,4})|(?<!\d)(\d{2,4})\s*[-–—](?!\s*\d))", normalized):
        token = match.group(1) or match.group(2)
        end = parse_year_token(token)
        if end is None:
            continue
        if all(y <= end for y in years):
            best = max(best, 3000 + end)

    # 10-15 => closed range (prefer narrower range when matching).
    for match in re.finditer(r"(?<!\d)(\d{2,4})\s*[-–—]\s*(\d{2,4})(?!\d)", normalized):
        start = parse_year_token(match.group(1))
        end = parse_year_token(match.group(2))
        if start is None or end is None:
            continue
        low = min(start, end)
        high = max(start, end)
        if all(low <= y <= high for y in years):
            width = max(1, high - low + 1)
            best = max(best, 5000 - width)

    return best


def extract_matched_alternative(
    alternatives: str,
    text_tokens: list[str],
    years: list[int],
    raw_query: str = "",
) -> str:
    """
    نص العرض تحت النتيجة الزرقاء: من أول ظهور لكلمة من البحث تظهر في خانة البدائل
    حتى أول «/» بعدها، أو حتى آخر الخانة إن لم يوجد / بعدها.
    إن لم ينجح ذلك يُستخدم التطابق بالشرائح كاحتياطي.
    """
    _ = raw_query  # محجوز للتوافق مع الاستدعاءات.
    if not (alternatives or "").strip():
        return ""
    segments = split_alternative_segments(alternatives)
    if not segments:
        return ""

    alt_tokens = _tokens_matching_alternatives_column(text_tokens, alternatives)
    if alt_tokens:
        tokens_use = alt_tokens
    elif years:
        tokens_use = []
    else:
        tokens_use = [normalize_text(t) for t in text_tokens if normalize_text(t)]

    if tokens_use:
        sliced = _slice_alternatives_from_first_match_to_slash(alternatives, tokens_use)
        if sliced:
            if (not years) or any(year_in_range_text(y, sliced) for y in years):
                return sliced

    def seg_matches_tokens(nseg: str) -> bool:
        if not tokens_use:
            return True
        return all(_token_in_norm_segment(tok, nseg) for tok in tokens_use)

    candidates: list[str] = []
    for segment in segments:
        nseg = normalize_text(segment)
        if not seg_matches_tokens(nseg):
            continue
        has_year = (not years) or any(year_in_range_text(y, segment) for y in years)
        if has_year:
            candidates.append(segment)
    if candidates:
        return max(candidates, key=len).strip()

    pool: list[str] = list(segments)
    if years:
        year_pool = [s for s in segments if any(year_in_range_text(y, s) for y in years)]
        if year_pool:
            pool = year_pool

    if tokens_use:
        best_seg = ""
        best_score = -1
        for segment in pool:
            nseg = normalize_text(segment)
            score = sum(1 for tok in tokens_use if _token_in_norm_segment(tok, nseg))
            if score > best_score or (score == best_score and len(segment) > len(best_seg)):
                best_score = score
                best_seg = segment
        if best_score > 0:
            return best_seg.strip()

    if years:
        year_hits = [s for s in segments if any(year_in_range_text(y, s) for y in years)]
        if year_hits:
            return max(year_hits, key=len).strip()
        return ""

    return ""


def row_matches_query(row: sqlite3.Row, text_tokens: list[str], years: list[int]) -> bool:
    """تطابق نصي: كل كلمات البحث يجب أن تظهر في الصف (حقيبة كلمات — ترتيب الكلمات لا يهم)."""
    item_name = row["item_name"] or ""
    alternatives = row["alternatives"] or ""
    source_file = row["source_file"] or ""
    item_norm = normalize_text(item_name)
    alt_norm = normalize_text(alternatives)
    file_norm = normalize_text(source_file)
    combined = normalize_text(f"{item_name} {alternatives} {source_file}")
    combined_compact = normalize_text_compact(f"{item_name} {alternatives} {source_file}")

    for token in text_tokens:
        nt = normalize_text(token)
        nct = normalize_text_compact(token)
        if nt not in combined and (nct and nct not in combined_compact):
            return False

    if years:
        item_tokens = [normalize_text(t) for t in text_tokens if normalize_text(t) in item_norm]
        alt_tokens = [normalize_text(t) for t in text_tokens if normalize_text(t) in alt_norm]
        file_tokens = [normalize_text(t) for t in text_tokens if normalize_text(t) in file_norm]
        item_year_ok = bool(item_tokens) and any(year_in_range_text(y, item_name) for y in years)

        segments = split_alternative_segments(alternatives)
        alt_year_ok = False
        if segments:
            for segment in segments:
                nseg = normalize_text(segment)
                has_alt_tokens = (not alt_tokens) or all(t in nseg for t in alt_tokens)
                has_year = any(year_in_range_text(y, segment) for y in years)
                if has_alt_tokens and has_year:
                    alt_year_ok = True
                    break

        # If query tokens are present in alternatives, year must match alternatives context.
        if alt_tokens:
            if not alt_year_ok:
                return False
        # If tokens are only in source_file context, do not force year match.
        elif file_tokens and not item_tokens:
            pass
        # Otherwise, rely on item-name year context (or alternative year if available).
        elif not (item_year_ok or alt_year_ok):
            return False

    return True


def row_matches_number_query(row: sqlite3.Row, number_tokens: list[str]) -> bool:
    if not number_tokens:
        return True

    number_space = normalize_text(
        f"{row['company_number'] or ''} {row['original_numbers'] or ''} {row['notes'] or ''}"
    )
    return all(normalize_text(t) in number_space for t in number_tokens)


def wiper_tokens_and_years(q: str) -> tuple[list[str], list[int]]:
    tokens = tokenize_query(q or "")
    parsed = [parse_query_year_token(t) for t in tokens]
    text_tokens = [t for t, ys in zip(tokens, parsed) if ys is None]
    years = sorted({y for ys in parsed if ys for y in ys})
    return text_tokens, years


def wiper_field_matches_tokens_and_years(field_val: str, text_tokens: list[str], years: list[int]) -> bool:
    """نفس منطق تطابق الكلمات المطبّعة (مع النسخة المدمجة) + السنوات داخل خانة واحدة."""
    fv = field_val or ""
    if text_tokens:
        combined = normalize_text(fv)
        combined_compact = normalize_text_compact(fv)
        for token in text_tokens:
            nt = normalize_text(token)
            nct = normalize_text_compact(token)
            if nt not in combined and (nct and nct not in combined_compact):
                return False
    if years:
        if not any(year_in_range_text(y, fv) for y in years):
            return False
    return True


def reverse_wiper_engine_display(engine: str | None) -> str:
    """
    عرض عمود الماتور في نتائج القشطان فقط: عكس ترتيب الأجزاء المفصولة بمسافات
    (مثال: «D 1.6» → «1.6 D»). القيمة في قاعدة البيانات تبقى كما في Excel.
    """
    s = str(engine or "").strip()
    if not s:
        return ""
    tokens = s.split()
    if len(tokens) < 2:
        return s
    return " ".join(reversed(tokens))


def normalize_barcode(raw: str) -> str:
    """أرقام عربية/فارسية → إنجليزية، ثم إبقاء الأرقام فقط (بدون مسافات أو رموز). مثل 18843 10062 → 1884310062."""
    if not raw:
        return ""
    s = str(raw).translate(DIGIT_TRANS)
    return re.sub(r"[^\d]", "", s)


_INVISIBLE_OR_SOFT_HYPHEN_RE = re.compile(r"[\u200B-\u200D\uFEFF\u2060\u00AD]")


def strip_barcode_input_noise(s: str) -> str:
    """NFKC + إزالة مسافات/فاصلات غير مرئية قبل تطبيع الباركود."""
    if not s:
        return ""
    t = unicodedata.normalize("NFKC", str(s))
    return _INVISIBLE_OR_SOFT_HYPHEN_RE.sub("", t)


def normalize_barcode_lookup_key(raw: str) -> str:
    """
    المفتاح الكانوني الواحد للتخزين والمساواة الدقيقة (يتبعها barcode_search_keys للبحث):
    - أرقام عربية/فارسية → لاتينية، أحرف لاتينية كبيرة
    - حذف المسافات، الشرطات، الشرطات السفلية، / \\ . ، ( ) وأي فاصل غير أبجدي رقمي
    - الإبقاء على A-Z و 0-9 فقط
    - إن كانت النتيجة أرقاماً فقط: إزالة الأصفار البادئة
    - إن وُجدت أحرف: الإبقاء على الأصفار البادئة كما هي
    """
    if not raw:
        return ""
    s = strip_barcode_input_noise(raw).translate(DIGIT_TRANS).strip()
    alnum = re.sub(r"[^A-Za-z0-9]", "", s)
    if not alnum:
        return ""
    up = alnum.upper()
    if up.isdigit():
        # الإبقاء على الأصفار البادئة في القيمة المخزّنة؛ إزالة الأصفار تُدار عبر barcode_search_keys عند البحث
        return up
    return up


def _barcode_spaced_first_segment_key(s: str) -> str:
    """الجزء الأول من باركود بمسافات تنسيق (223112U001 من 223112U001        HI)."""
    if not s or not re.search(r"\s", s):
        return ""
    first = next((p.strip() for p in _BARCODE_FIELD_SPLIT_RE.split(s) if p and p.strip()), "")
    if not first:
        return ""
    fk = re.sub(r"[^A-Za-z0-9]", "", first.translate(DIGIT_TRANS)).upper()
    return fk if fk and len(fk) >= 6 else ""


def barcode_search_keys_exact(raw: str) -> list[str]:
    """
    مفاتيح تطابق تام — بدون حذف أصفار بادئة.
    إن وُجدت مسافات تنسيق (223112U001        HI): المدمج الكامل + الجزء الأول
    حتى يطابق الفهرس قبل إعادة البناء والصف المخزّن بالمسافات.
    """
    if not (raw and str(raw).strip()):
        return []
    s = strip_barcode_input_noise(raw).translate(DIGIT_TRANS).strip()
    alnum = re.sub(r"[^A-Za-z0-9]", "", s)
    if not alnum:
        return []
    out: list[str] = [alnum.upper()]
    fk = _barcode_spaced_first_segment_key(s)
    if fk and fk not in out:
        out.append(fk)
    return out


def barcode_search_keys(raw: str) -> list[str]:
    """
    مفاتيح آمنة للبحث: المفتاح الكانوني + للأرقام فقط نضيف الشكل بأصفار بادئة إن اختلف
    (مثال المخزن 12322311 والاستعلام 012322311 بعد الضغط النصي).
    باركود بمسافات/لاحقة (223112U001        HI): المدمج + الجزء الأول.
    """
    if not (raw and str(raw).strip()):
        return []
    s = strip_barcode_input_noise(raw).translate(DIGIT_TRANS).strip()
    alnum = re.sub(r"[^A-Za-z0-9]", "", s)
    if not alnum:
        return []
    up = alnum.upper()
    out: list[str] = []
    if up.isdigit():
        full = up
        stripped = full.lstrip("0") or "0"
        for k in (full, stripped):
            if k and k not in out:
                out.append(k)
    else:
        out.append(up)
        fk = _barcode_spaced_first_segment_key(s)
        if fk and fk not in out:
            out.append(fk)
    return out[:LOCATION_BARCODE_SEARCH_KEYS_MAX]


def barcode_cell_matches_search_keys(
    cell_raw: str, key_set: set[str], *, exact: bool = False
) -> bool:
    """هل عمود barcode الخام يطابق أي مفتاح — يفكك الباركودات المتعددة أولاً (لا يدمجها)."""
    if not key_set:
        return False
    candidates: list[str] = []
    # الشكل المدمج للخانة كاملة (223112U001        HI → 223112U001HI)
    full = normalize_barcode_lookup_key(cell_raw)
    if full:
        if exact:
            candidates.append(full)
        else:
            for k in barcode_search_keys(full):
                if k and k not in candidates:
                    candidates.append(k)
    # مهم: لا نستدعي barcode_search_keys على النص الكامل إن احتوى عدة باركودات
    # لأنه يحذف الفواصل ويدمجها في مفتاح واحد خاطئ — لذلك نفكك ثم نطابق.
    tokens = _extract_location_unit_barcodes_from_cell(cell_raw)
    if not tokens and full:
        tokens = [full]
    for tok in tokens:
        if exact:
            alnum = re.sub(r"[^A-Za-z0-9]", "", str(tok or "").translate(DIGIT_TRANS)).upper()
            if alnum and alnum not in candidates:
                candidates.append(alnum)
        else:
            for k in barcode_search_keys(tok):
                if k and k not in candidates:
                    candidates.append(k)
    for k in candidates:
        if k in key_set:
            return True
    return False


def _barcode_search_debug_log(
    enabled: bool, msg: str, *, raw: str = "", keys: list[str] | None = None, kind: str = ""
) -> None:
    if not (BARCODE_SEARCH_DEBUG or enabled):
        return
    parts = [msg]
    if raw:
        parts.append(f"raw={raw!r}")
    if keys is not None:
        parts.append(f"keys={keys!r}")
    if kind:
        parts.append(f"match={kind}")
    try:
        print("[barcode-search] " + " | ".join(parts), flush=True)
    except Exception:
        pass


def _like_pattern_escape_sql(s: str) -> str:
    return (
        str(s)
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


def is_digits_only_query_token(token: str) -> bool:
    """التوكن نصه كله أرقام (بعد تطبيع الأرقام العربية) — مسموح بمسافات داخل التوكن تُزال."""
    nb = normalize_barcode(token)
    if not nb:
        return False
    compact = re.sub(r"\s+", "", str(token).translate(DIGIT_TRANS))
    return compact.isdigit()


def coalesce_location_barcode_query_tokens(raw_q: str, text_tokens: list[str]) -> list[str]:
    """
    إن كان الاستعلام مجموعة أرقام منفصلة بمسافات (18843 10062) ندمجها لتوكن واحد للمطابقة مع الباركود المخزّن.
    لا يُدمج إن وُجدت كلمات نصية (مثل كيا 18843).
    """
    if not text_tokens:
        return text_tokens
    if not all(is_digits_only_query_token(t) for t in text_tokens):
        return text_tokens
    merged = normalize_barcode(raw_q)
    merged_stripped = merged.lstrip("0") or "0"
    if len(merged_stripped) < 4:
        return text_tokens
    # قبول أطوال عملية للباركود الرقمي (هيونداي/كيا وغيرها)
    if len(merged) > 20:
        return text_tokens
    return [merged_stripped]


def _location_like_pattern_contains(norm_token: str) -> str | None:
    """LIKE %…% مع إزالة رموز خاصة قد تكسر النمط؛ None إن لم يبقَ محتوى (لا نستخدم % وحده)."""
    safe = re.sub(r"[%_\\]+", " ", str(norm_token or "")).strip()
    if not safe:
        return None
    return f"%{safe}%"


def location_query_canonical_barcode(raw_q: str) -> str:
    """المفتاح الكانوني الأول (للتوافق والعرض) — انظر barcode_search_keys للبحث الفعلي."""
    return normalize_barcode_lookup_key((raw_q or "").strip())


def location_text_search_norm_tokens(raw_q: str) -> list[str]:
    tokens = tokenize_query(raw_q or "")
    parsed = [parse_query_year_token(t) for t in tokens]
    text_tokens = [t for t, ys in zip(tokens, parsed) if ys is None]
    merged = coalesce_location_barcode_query_tokens(raw_q or "", text_tokens)
    out: list[str] = []
    for t in merged:
        nt = normalize_text(t)
        if nt:
            out.append(nt)
    return out


def is_meaningful_shelf_location(location: Any) -> bool:
    loc = str(location or "").strip()
    if not loc:
        return False
    if loc in {"0", "0.0", "-", "—", "null", "none", "None"}:
        return False
    return True


def clean_shelf_location(location: Any) -> str:
    loc = str(location or "").strip()
    return loc if is_meaningful_shelf_location(loc) else ""


def _location_search_row_rank(d: dict[str, Any]) -> tuple[int, int]:
    """أقل = أفضل: صفوف حساباتي_API ثم الأحدث id."""
    api_rank = 0 if str(d.get("source_file") or "").strip() == LOCATION_API_SOURCE_FILE else 1
    try:
        rid = -int(d.get("id") or 0)
    except (TypeError, ValueError):
        rid = 0
    return (api_rank, rid)


def _location_search_dedupe_keys(d: dict[str, Any]) -> list[str]:
    """مفاتيح دمج لنفس الصنف: product_id ثم باركود ثم أصلي|اسم."""
    keys: list[str] = []
    pid = str(d.get("hesabate_product_id") or "").strip()
    if pid:
        keys.append(f"pid:{pid}")
    bc_raw = str(d.get("barcode") or "")
    bc = normalize_barcode_lookup_key(bc_raw)
    if bc:
        keys.append(f"bc:{bc}")
    for tok in _extract_location_unit_barcodes_from_cell(bc_raw):
        for k in barcode_search_keys(tok):
            keys.append(f"bc:{k}")
    on = str(d.get("original_number") or "").strip().lower()
    nm = str(d.get("item_name") or "").strip().lower()
    if on or nm:
        keys.append(f"nm:{on}|{nm}")
    out: list[str] = []
    for k in keys:
        if k and k not in out:
            out.append(k)
    return out


def _prepare_location_search_rows(rows: list[Any]) -> list[dict[str, Any]]:
    """يفضّل صف حساباتي_API عند تكرار نفس الصنف (باركود / product_id)."""
    winners: list[dict[str, Any]] = []
    key_to_winner_idx: dict[str, int] = {}
    anon_i = 0
    for row in rows:
        d = sql_row_to_dict(row)
        rid = d.get("id", "")
        d["row_key"] = f"{d.get('source_file', '')}|{d.get('source_sheet', '')}|{rid}"
        d["location"] = clean_shelf_location(d.get("location"))
        dkeys = _location_search_dedupe_keys(d)
        if not dkeys:
            anon_i += 1
            dkeys = [f"__anon_{anon_i}"]
        collide_idx = None
        for k in dkeys:
            if k in key_to_winner_idx:
                collide_idx = key_to_winner_idx[k]
                break
        if collide_idx is None:
            idx = len(winners)
            winners.append(d)
            for k in dkeys:
                key_to_winner_idx[k] = idx
            continue
        prev = winners[collide_idx]
        if _location_search_row_rank(d) < _location_search_row_rank(prev):
            winners[collide_idx] = d
            for k, ix in list(key_to_winner_idx.items()):
                if ix == collide_idx:
                    del key_to_winner_idx[k]
            for k in dkeys:
                key_to_winner_idx[k] = collide_idx
            # أعد ربط مفاتيح الصف السابق غير المتعارضة إن لزم — يكفي استبدال الفائز
        else:
            for k in dkeys:
                key_to_winner_idx.setdefault(k, collide_idx)
    return [{str(k): _json_safe_value(v) for k, v in d.items()} for d in winners]


def location_catalog_row_count() -> int:
    conn = get_db()
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM location_rows").fetchone()
        return int(sql_row_to_dict(row).get("c") or 0) if row is not None else 0
    except Exception:
        return 0
    finally:
        conn.close()


def _search_locations_sql_only(
    conn: DBConnection,
    *,
    mode: str,
    canonical_barcode: str,
    barcode_keys: list[str] | None,
    norm_tokens: list[str],
    limit: int,
    offset: int,
    barcode_exact: bool = False,
) -> tuple[list[Any], bool]:
    lim = max(1, min(limit, LOCATION_SEARCH_LIMIT_MAX))
    off = max(0, offset)
    fetch = lim + 1
    cols = (
        "id, item_name, location, company, original_number, barcode, brand, notes, quantity, price, "
        "source_file, source_sheet, hesabate_product_id"
    )
    if mode == "barcode":
        keys = [k for k in (barcode_keys or []) if k]
        if not keys and canonical_barcode:
            keys = [canonical_barcode]
        keys = list(dict.fromkeys(keys))
        if not keys:
            return [], False
        placeholders = ", ".join(["?"] * len(keys))
        # مطابقة دقيقة فقط على الباركود (توكن كامل) — بدون LIKE فرعي وبدون رقم أصلي/نص
        # الشكل المخزّن: " KEY1 KEY2 " لذا % KEY % = مساواة توكن وليس جزءاً من باركود أطول
        spaced_keys = [f" {k} " for k in keys]
        token_parts: list[str] = []
        token_params: list[str] = []
        for k in keys:
            esc = _like_pattern_escape_sql(k)
            token_parts.append("normalized_barcode LIKE ? ESCAPE '\\'")
            token_params.append(f"% {esc} %")
        token_sql = " OR ".join(token_parts)
        bc_cols = cols + ", normalized_barcode"
        rows = conn.execute(
            f"""
            SELECT {bc_cols}
            FROM location_rows
            WHERE normalized_barcode IN ({placeholders})
               OR normalized_barcode IN ({placeholders})
               OR barcode IN ({placeholders})
               OR ({token_sql})
            ORDER BY CASE WHEN source_file = ? THEN 0 ELSE 1 END, id DESC
            LIMIT ? OFFSET ?
            """,
            (*keys, *spaced_keys, *keys, *token_params, LOCATION_API_SOURCE_FILE, fetch, off),
        ).fetchall()
        # تحقق نهائي: رفض أي تطابق فرعي بالخطأ
        key_set = set(keys)
        exact_rows: list[Any] = []
        for row in rows:
            rd = sql_row_to_dict(row)
            if _location_row_matches_barcode_keys_exact(rd, key_set, exact=barcode_exact):
                exact_rows.append(row)
        rows = exact_rows
    else:
        if not norm_tokens:
            return [], False
        # لكل توكن: كل أشكال الكتابة المكافئة (foucs↔focus …) كـ OR داخل التوكن، وAND بين التوكنات
        token_pats: list[list[str]] = []
        for t in norm_tokens:
            pats = [
                p
                for p in (
                    _location_like_pattern_contains(v) for v in spelling_query_variants(t)
                )
                if p
            ]
            pats = list(dict.fromkeys(pats))
            if pats:
                token_pats.append(pats)
        if not token_pats:
            return [], False
        # مربع النص: normalized_text إن وُجد، أو مطابقة على أعمدة الصنف/الشركة/العلامة/الرقم/الملاحظات
        # (صفوف بلا normalized_text أو فارغ لا تظهر إن اعتمدنا normalized_text فقط)
        norm_and = " AND ".join(
            "(" + " OR ".join(["normalized_text LIKE ?"] * len(pats)) + ")" for pats in token_pats
        )
        part_norm = (
            "(normalized_text IS NOT NULL AND TRIM(COALESCE(normalized_text,'')) <> '' AND "
            f"{norm_and})"
        )
        legacy_groups: list[str] = []
        for pats in token_pats:
            var_ors: list[str] = []
            for _ in pats:
                var_ors.append(
                    "("
                    + " OR ".join(
                        [
                            "lower(COALESCE(item_name,'')) LIKE ?",
                            "lower(COALESCE(company,'')) LIKE ?",
                            "lower(COALESCE(brand,'')) LIKE ?",
                            "lower(COALESCE(original_number,'')) LIKE ?",
                            "lower(COALESCE(barcode,'')) LIKE ?",
                            "lower(COALESCE(notes,'')) LIKE ?",
                        ]
                    )
                    + ")"
                )
            legacy_groups.append("(" + " OR ".join(var_ors) + ")")
        part_legacy = "(" + " AND ".join(legacy_groups) + ")"
        where_sql = f"(({part_norm}) OR ({part_legacy}))"
        params: list[Any] = []
        for pats in token_pats:
            params.extend(pats)
        for pats in token_pats:
            for pat in pats:
                params.extend([pat, pat, pat, pat, pat, pat])
        params.extend([LOCATION_API_SOURCE_FILE, fetch, off])
        rows = conn.execute(
            f"""
            SELECT {cols}
            FROM location_rows
            WHERE {where_sql}
            ORDER BY CASE WHEN source_file = ? THEN 0 ELSE 1 END, id DESC
            LIMIT ? OFFSET ?
            """,
            params,
        ).fetchall()
    has_more = len(rows) > lim
    return rows[:lim], has_more


def _location_row_matches_barcode_keys_exact(
    rd: dict[str, Any], key_set: set[str], *, exact: bool = False
) -> bool:
    """مطابقة باركود دقيقة فقط (عمود الباركود + التوكنات في normalized_barcode) — بدون رقم أصلي/نص."""
    if not key_set:
        return False
    if barcode_cell_matches_search_keys(str(rd.get("barcode") or ""), key_set, exact=exact):
        return True
    nb = str(rd.get("normalized_barcode") or "").strip()
    if nb:
        for tok in nb.split():
            if exact:
                alnum = re.sub(r"[^A-Za-z0-9]", "", str(tok or "").translate(DIGIT_TRANS)).upper()
                if alnum and alnum in key_set:
                    return True
            else:
                if tok in key_set:
                    return True
                for k in barcode_search_keys(tok):
                    if k in key_set:
                        return True
    return False


def _location_row_matches_barcode_keys(rd: dict[str, Any], key_set: set[str]) -> bool:
    """توافق قديم — يوجّه للمطابقة الدقيقة على الباركود فقط."""
    return _location_row_matches_barcode_keys_exact(rd, key_set, exact=False)


def _search_locations_barcode_python_fallback(
    conn: DBConnection,
    key_set: set[str],
    limit: int,
    offset: int,
    *,
    barcode_exact: bool = False,
) -> tuple[list[Any], bool]:
    """صفوف بلا تطبيع جيد — مطابقة مفاتيح الباركود فقط في بايثون."""
    if not key_set:
        return [], False
    cap = 20000
    rows = conn.execute(
        """
        SELECT id, item_name, location, company, original_number, barcode, brand, notes, quantity, price,
               source_file, source_sheet, normalized_barcode
        FROM location_rows
        WHERE COALESCE(TRIM(barcode), '') <> ''
          AND (
            normalized_barcode IS NULL
            OR TRIM(COALESCE(normalized_barcode, '')) = ''
          )
        ORDER BY CASE WHEN source_file = ? THEN 0 ELSE 1 END, id DESC
        LIMIT ?
        """,
        (LOCATION_API_SOURCE_FILE, cap),
    ).fetchall()
    matched: list[Any] = []
    for row in rows:
        rd = sql_row_to_dict(row)
        if _location_row_matches_barcode_keys_exact(rd, key_set, exact=barcode_exact):
            matched.append(row)
    lim = max(1, min(limit, LOCATION_SEARCH_LIMIT_MAX))
    off = max(0, offset)
    chunk = matched[off : off + lim + 1]
    has_more = len(chunk) > lim
    return chunk[:lim], has_more


def _search_locations_barcode_like_fallback(
    conn: DBConnection,
    key_set: set[str],
    limit: int,
    offset: int,
    *,
    barcode_exact: bool = False,
) -> tuple[list[Any], bool]:
    """
    بيانات قديمة سيئة التطبيع: LIKE على عمود barcode فقط ثم تحقق بمساواة المفاتيح.
    عند barcode_exact لا يُستخدم هذا المسار من search_locations (يُستدعى فقط من المسار العادي).
    """
    if not key_set:
        return [], False
    needles = sorted(key_set, key=len, reverse=True)
    needle = next((n for n in needles if len(n) >= LOCATION_BARCODE_LIKE_FALLBACK_MIN_LEN), "")
    if not needle:
        return [], False
    pat = f"%{_like_pattern_escape_sql(needle)}%"
    rows = conn.execute(
        """
        SELECT id, item_name, location, company, original_number, barcode, brand, notes, quantity, price,
               source_file, source_sheet, normalized_barcode
        FROM location_rows
        WHERE COALESCE(TRIM(barcode), '') <> ''
          AND LENGTH(barcode) <= 128
          AND barcode LIKE ? ESCAPE '\\'
        ORDER BY CASE WHEN source_file = ? THEN 0 ELSE 1 END, id DESC
        LIMIT ?
        """,
        (pat, LOCATION_API_SOURCE_FILE, LOCATION_BARCODE_LIKE_FALLBACK_FETCH_CAP),
    ).fetchall()
    matched: list[Any] = []
    for row in rows:
        rd = sql_row_to_dict(row)
        if _location_row_matches_barcode_keys_exact(rd, key_set, exact=barcode_exact):
            matched.append(row)
    lim = max(1, min(limit, LOCATION_SEARCH_LIMIT_MAX))
    off = max(0, offset)
    chunk = matched[off : off + lim + 1]
    has_more = len(chunk) > lim
    return chunk[:lim], has_more


def _spaced_barcode_like_prefixes(key_set: set[str]) -> list[str]:
    """
    بادئات LIKE لباركود حساباتي بمسافات: من 223112U001HI → 223112U001
    حتى يطابق الحقل المخزّن «223112U001        HI».
    """
    prefixes: list[str] = []
    for k in sorted(key_set, key=len, reverse=True):
        if len(k) < 6:
            continue
        cands: list[str] = []
        # جسم الباركود …U001 ثم لاحقة HI/KI19 — لا تقطع من وسط الرقم
        m = re.match(r"^(\d{2,}[A-Z]\d{2,})([A-Z]{1,4}\d{0,4})$", k)
        if m:
            cands.append(m.group(1))
        elif re.search(r"[A-Z]", k):
            # الجزء الأول بدون لاحقة إن وُجد كمفتاح منفصل
            cands.append(k)
        for pref in cands:
            if pref and pref not in prefixes:
                prefixes.append(pref)
        if len(prefixes) >= 6:
            break
    return prefixes


def _search_locations_barcode_spaced_compact_fallback(
    conn: DBConnection,
    key_set: set[str],
    limit: int,
    offset: int,
    *,
    barcode_exact: bool = True,
) -> tuple[list[Any], bool]:
    """
    باركودات حساباتي بمسافات داخل الحقل (223112U001        HI):
    الفهرس قد لا يحتوي الشكل المدمج بعد — نبحث بالبادئة ثم نتحقق بالمساواة المدمجة.
    """
    if not key_set:
        return [], False
    prefixes = _spaced_barcode_like_prefixes(key_set)
    if not prefixes:
        return [], False
    matched: list[Any] = []
    seen_ids: set[Any] = set()
    for pref in prefixes:
        pat = f"{_like_pattern_escape_sql(pref)}%"
        rows = conn.execute(
            """
            SELECT id, item_name, location, company, original_number, barcode, brand, notes, quantity, price,
                   source_file, source_sheet, normalized_barcode
            FROM location_rows
            WHERE COALESCE(TRIM(barcode), '') <> ''
              AND LENGTH(barcode) <= 128
              AND barcode LIKE ? ESCAPE '\\'
            ORDER BY CASE WHEN source_file = ? THEN 0 ELSE 1 END, id DESC
            LIMIT ?
            """,
            (pat, LOCATION_API_SOURCE_FILE, LOCATION_BARCODE_LIKE_FALLBACK_FETCH_CAP),
        ).fetchall()
        for row in rows:
            rd = sql_row_to_dict(row)
            rid = rd.get("id")
            if rid in seen_ids:
                continue
            if _location_row_matches_barcode_keys_exact(rd, key_set, exact=barcode_exact):
                seen_ids.add(rid)
                matched.append(row)
    lim = max(1, min(limit, LOCATION_SEARCH_LIMIT_MAX))
    off = max(0, offset)
    chunk = matched[off : off + lim + 1]
    has_more = len(chunk) > lim
    return chunk[:lim], has_more


def dedupe_rows_by_normalized_original(rows: list[Any]) -> list[Any]:
    """
    إن تكرر نفس الرقم الأصلي (بعد التطبيع) لأكثر من صف — حتى من ملفات مختلفة —
    يُعتبر صنفاً واحداً ويُعرض صف واحد في جدول النتائج (يُحتفظ بأول صف حسب الترتيب الحالي).
    الصفوف بلا رقم أصلي مُعرَّف لا تُدمج مع بعضها.
    """
    out: list[Any] = []
    seen_orig: set[str] = set()
    for r in rows:
        d = sql_row_to_dict(r)
        orig_c = normalize_text_compact(d.get("original_numbers") or "")
        if not orig_c:
            out.append(r)
            continue
        if orig_c in seen_orig:
            continue
        seen_orig.add(orig_c)
        out.append(r)
    return out


def _row_search_relevance_score(row: Any, text_tokens: list[str], years: list[int]) -> tuple[int, int, int, str]:
    """درجة أعلى = الصف أنسب لكلمات البحث الحالية (لاختيار مَن يُبقى عند تكرار الرقم الأصلي)."""
    d = sql_row_to_dict(row)
    item = d.get("item_name") or ""
    alt = d.get("alternatives") or ""
    bag = normalize_text(f"{item} {alt}")
    bag_c = normalize_text_compact(f"{item} {alt}")
    item_n = normalize_text(item)
    alt_n = normalize_text(alt)
    alt_c = normalize_text_compact(alt)
    item_c = normalize_text_compact(item)
    score = 0
    for t in text_tokens:
        nt = normalize_text(t)
        if not nt:
            continue
        nct = normalize_text_compact(nt)
        in_bag = nt in bag or (bool(nct) and nct in bag_c)
        if not in_bag:
            continue
        in_alt = nt in alt_n or (bool(nct) and nct in alt_c)
        in_item = nt in item_n or (bool(nct) and nct in item_c)
        if in_alt:
            score += 5
        elif in_item:
            score += 2
    year_bonus = 0
    if years:
        if any(year_in_range_text(y, alt) for y in years):
            year_bonus = 3
        elif any(year_in_range_text(y, item) for y in years):
            year_bonus = 1
    detail = min(len(alt), 5000)
    return (score + year_bonus, year_bonus, detail, item)


def dedupe_rows_by_normalized_original_for_search(
    rows: list[Any],
    text_tokens: list[str],
    years: list[int],
) -> list[Any]:
    """
    دمج تكرار الرقم الأصلي مع اختيار الصف الأكثر ارتباطاً بالاستعلام الحالي
    (كلمات البحث في البدائل تُوزَّن أعلى من الاسم فقط)، حتى لا يختفي صف مثل Octavia
    لأن صفاً آخر بنفس الرقم سبقه أبجدياً (مثل Cady Life).
    """
    winners: dict[str, Any] = {}
    for r in rows:
        d = sql_row_to_dict(r)
        oc = normalize_text_compact(d.get("original_numbers") or "")
        if not oc:
            continue
        if oc not in winners:
            winners[oc] = r
        else:
            s_new = _row_search_relevance_score(r, text_tokens, years)
            s_old = _row_search_relevance_score(winners[oc], text_tokens, years)
            if s_new > s_old:
                winners[oc] = r

    out: list[Any] = []
    emitted: set[str] = set()
    for r in rows:
        d = sql_row_to_dict(r)
        oc = normalize_text_compact(d.get("original_numbers") or "")
        if not oc:
            out.append(r)
            continue
        if oc in emitted:
            continue
        emitted.add(oc)
        out.append(winners[oc])
    return out


def _numbers_in_normalized_text(text: str) -> list[float]:
    """Extract numeric literals from normalized size text (handles 'قطر 98 مم', '29 x 98', etc.)."""
    out: list[float] = []
    for m in re.finditer(r"\d+(?:\.\d+)?", text or ""):
        try:
            out.append(float(m.group(0)))
        except Exception:
            continue
    return out


def match_size_value(cell_value: str, query_value: str) -> bool:
    q = normalize_text(query_value).replace(",", ".")
    if not q:
        return True
    c = normalize_text(cell_value).replace(",", ".")
    if c == q:
        return True
    # Tolerate different formatting such as "10.0" vs "10"
    try:
        if float(c) == float(q):
            return True
    except Exception:
        pass
    # Query is a plain number but cell has extra text (units, labels, etc.)
    try:
        qf = float(q)
    except Exception:
        return q in c
    for n in _numbers_in_normalized_text(c):
        if n == qf:
            return True
    return False


def match_size_value_driveshaft(cell_value: str, query_value: str) -> bool:
    """
    مطابقة قياسات مصلب الدراي شفط؛ إذا كتب المستخدم رقمًا من رقم واحد (مثل 9 بدل 98)
    يُقبل إذا كان أي رقم في الخلية يبدأ بهذا الرقم (98 تطابق استعلام 9).
    """
    if match_size_value(cell_value, query_value):
        return True
    q = normalize_text(query_value).replace(",", ".")
    if not re.fullmatch(r"\d", q):
        return False
    t = normalize_text(cell_value).replace(",", ".")
    for m in re.finditer(r"\d+(?:\.\d+)?", t):
        s = m.group(0)
        sn = s.lstrip("0") or "0"
        if s.startswith(q) or sn.startswith(q):
            return True
    return False


def normalized_path_matches_driveshaft(sf: str) -> bool:
    """
    True if source_file path looks like a drive-shaft (مصلبات) Excel source.
    Accepts spelling/layout variants often seen in Drive folder names.
    """
    if not sf:
        return False
    if "مصلبة دراي شفط" in sf or "مصلبات دراي شفط" in sf or "مصلب دراي شفط" in sf:
        return True
    # ة -> ه in normalize_text، «مصلب» يغطي مصلبات/مصلبه؛ قد يُكتب «مصلب» بدون تاء
    if "مصلب" not in sf:
        return False
    has_shaft = ("شفط" in sf) or ("shaft" in sf)
    has_drive = any(k in sf for k in ("دراي", "دري", "dry", "drive"))
    # مجلدات باسم «مصلب دراي» بدون «شفط» أو العكس
    return bool(has_shaft or has_drive)


def is_driveshaft_size_query(size_type: str) -> bool:
    st = normalize_text(size_type)
    if not st:
        return False
    if st in (
        "مصلبات دراي شفط",
        "مصلبه دراي شفط",
        "مصلبات",
        "مصلب دراي شفط",
        "مصلب دراي",
    ):
        return True
    if "مصلب" in st and any(x in st for x in ("شفط", "دراي", "دري", "dry", "drive", "shaft")):
        return True
    return False


def file_matches_size_type(source_file: str, size_type: str) -> bool:
    st = normalize_text(size_type)
    if not st:
        return True
    sf = normalize_text(source_file)

    if st == "لبادات":
        return "لبادات" in sf

    if st in ["بيلية", "بيل", "بلي"]:
        return any(key in sf for key in ["نابات + بيل عجل", "بيل مشكلة", "بيـل مشكلة"])

    if is_driveshaft_size_query(size_type):
        return normalized_path_matches_driveshaft(sf)

    return True


def file_matches_size_type_with_fallback(row: Any, source_file: str, size_type: str, sw: str, sd: str) -> bool:
    """مسار الملف أو (مصلبات + قياسان في الاستعلام يطابقان B/C مع تجاهل مسار عند غيبته في source_file)."""
    if not normalize_text(size_type):
        return True
    if file_matches_size_type(source_file, size_type):
        return True
    if not is_driveshaft_size_query(size_type):
        return False
    sf = normalize_text(source_file)
    if "لبادات" in sf:
        return False
    if "بيل مشكلة" in sf or "بيـل مشكلة" in sf or normalize_text("نابات + بيل عجل") in sf:
        return False
    if not (normalize_text(sw) and normalize_text(sd)):
        return False
    rw = (row["size_width"] or "").strip()
    rd = (row["size_diameter"] or "").strip()
    if not rw or not rd:
        return False
    return (match_size_value_driveshaft(rw, sw) and match_size_value_driveshaft(rd, sd)) or (
        match_size_value_driveshaft(rw, sd) and match_size_value_driveshaft(rd, sw)
    )


def row_matches_size_filters(size_type: str, row: Any, sw: str, sd: str, sh: str) -> bool:
    """مطابقة أبعاد البحث؛ لمصلبات الدراي شفط: تبديل B/C مسموح ورقم واحد يطابق بادئة (9≈98)."""
    if is_driveshaft_size_query(size_type):
        wv = row["size_width"] or ""
        dv = row["size_diameter"] or ""
        hv = row["size_height"] or ""
        swq, sdq = normalize_text(sw), normalize_text(sd)
        if swq and sdq:
            dim_ok = (match_size_value_driveshaft(wv, sw) and match_size_value_driveshaft(dv, sd)) or (
                match_size_value_driveshaft(wv, sd) and match_size_value_driveshaft(dv, sw)
            )
        elif swq:
            dim_ok = match_size_value_driveshaft(wv, sw) or match_size_value_driveshaft(dv, sw)
        elif sdq:
            dim_ok = match_size_value_driveshaft(wv, sd) or match_size_value_driveshaft(dv, sd)
        else:
            dim_ok = True
        return dim_ok and match_size_value(hv, sh)
    return (
        match_size_value(row["size_width"] or "", sw)
        and match_size_value(row["size_diameter"] or "", sd)
        and match_size_value(row["size_height"] or "", sh)
    )


def is_size_file(source_file: str) -> bool:
    sf = normalize_text(source_file)
    return (
        ("لبادات" in sf)
        or ("بيل" in sf)
        or ("بيلية" in sf)
        or ("نابات" in sf)
        or normalized_path_matches_driveshaft(sf)
    )


def build_size_label_display(source_file: str, w: str, d: str, h: str) -> str:
    """عرض قياسات بدون «x» الإنجليزية وبدون كلمة Height؛ مصلبات: بُعدان فقط."""
    if not is_size_file(source_file or ""):
        return ""
    sw, sd, sh = (w or "").strip(), (d or "").strip(), (h or "").strip()
    sf = normalize_text(source_file or "")
    if normalized_path_matches_driveshaft(sf) or (is_size_file(source_file or "") and not sh):
        parts = [p for p in (sw, sd) if p]
    else:
        parts = [p for p in (sw, sd, sh) if p]
    return " · ".join(parts) if parts else ""


def preview_row_is_likely_header_row(cells: list[str]) -> bool:
    """يقلل اختيار صف بيانات كأنه صف عناوين (مشكلة ثيرموستات وغيره)."""
    nonempty = [str(c).strip() for c in cells if c and str(c).strip()]
    if len(nonempty) < 2:
        return True
    data_like = 0
    for v in nonempty[:14]:
        if len(v) < 5:
            continue
        dig = sum(1 for ch in v if ch.isdigit())
        if len(v) >= 10 and dig / len(v) > 0.4:
            data_like += 1
        elif len(v) >= 6 and dig / len(v) > 0.55:
            data_like += 1
    return data_like < max(2, len(nonempty) // 3)


def extract_file_search_hints(query: str, tokens: list[str]) -> tuple[list[str], list[str]]:
    qn = normalize_text(query)
    hints: list[str] = []
    consumed_tokens: set[str] = set()

    def consume(*parts: str) -> None:
        for p in parts:
            consumed_tokens.add(normalize_text(p))

    if ("فلتر" in qn or "فلاتر" in qn) and "هواء" in qn:
        hints.extend(["فلتر هواء", "فلاتر هواء"])
        consume("فلتر", "فلاتر", "هواء")
    if ("فلتر" in qn or "فلاتر" in qn) and "زيت" in qn:
        hints.extend(["فلتر زيت", "فلاتر زيت"])
        consume("فلتر", "فلاتر", "زيت")
    if ("فلتر" in qn or "فلاتر" in qn) and ("سولار" in qn or "ديزل" in qn):
        hints.extend(["فلتر سولار", "فلاتر السولار", "فلاتر سولار", "فلتر ديزل", "فلاتر ديزل"])
        consume("فلتر", "فلاتر", "سولار", "ديزل", "السولار")

    cleaned_tokens = [t for t in tokens if normalize_text(t) not in consumed_tokens]
    dedup_hints = list(dict.fromkeys(hints))
    return dedup_hints, cleaned_tokens


def file_search_hint_matches_path(path: str, hint: str) -> bool:
    """
    تطابق تلميح البحث عن الملف مع مسار التخزين.
    يقبل العبارة كاملة في المسار، أو وجود كل كلمات التلميح في المسار منفصلة
    (مثل ملف «فلتر بنزين + سولار» مع تلميح «فلتر سولار»).
    """
    pn = normalize_text(path or "")
    hn = normalize_text(hint or "")
    if not hn:
        return True
    if hn in pn:
        return True
    significant = [w for w in hn.split() if len(w) >= 2]
    if len(significant) < 2:
        return hn in pn

    def word_in_path(w: str) -> bool:
        if w in pn:
            return True
        if w.startswith("ال") and len(w) > 2 and w[2:] in pn:
            return True
        return False

    return all(word_in_path(w) for w in significant)


def file_hints_sql_clause(hints: list[str]) -> tuple[str, list[str]]:
    """
    WHERE فرعي لـ SQLite: لكل تلميح إما LIKE للجملة أو (LIKE لكل كلمة مع AND)
    ليتوافق مع file_search_hint_matches_path.
    """
    or_parts: list[str] = []
    params: list[str] = []
    for h in hints:
        hn = normalize_text(h)
        if not hn:
            continue
        significant = [w for w in hn.split() if len(w) >= 2]
        if len(significant) >= 2:
            and_sql = " AND ".join(["lower(source_file) LIKE ?" for _ in significant])
            or_parts.append(f"({and_sql})")
            params.extend(f"%{w}%" for w in significant)
        else:
            or_parts.append("lower(source_file) LIKE ?")
            params.append(f"%{hn}%")
    if not or_parts:
        return "1=1", []
    return "(" + " OR ".join(or_parts) + ")", params


def get_header_index(header_cells: list[str], keywords: list[str]) -> int | None:
    normalized_headers = [normalize_text(h) for h in header_cells]
    normalized_keywords = [normalize_text(k) for k in keywords]

    for idx, h in enumerate(normalized_headers):
        if h in normalized_keywords:
            return idx
    for idx, h in enumerate(normalized_headers):
        if any(k in h for k in normalized_keywords):
            return idx
    return None


# عمود «رقم الصنف» في الواجهة = رقم الشركات في الملف — يُفصل عن «رقم الصنف» الداخلي في الإكسل
HEADER_COMPANY_NUMBER = [
    "رقم الشركات",
    "رقم الشركة",
    "رقم الشركه",
    "رقم شركة",
    "رقم شركات",
    "ارقام الشركه",
    "أرقام الشركه",
    "ارقام الشركات",
    "أرقام الشركات",
    "ارقام شركات",
    "أرقام شركات",
    "company number",
    "رقم الشركات الموحد",
    "رقم المرجع",
    "المرجع",
    "مرجع الشركة",
    "كود الشركة",
    "كود الصنف",
    "رقم الصنف التجاري",
    "الصنف التجاري",
    "الكود التجاري",
    "كود المنتج",
    "رقم المنتج التجاري",
    "رقم التاجر",
    "رقم المورد",
    "كود التخزين",
    "رقم التعريف",
    "رقم مرجع",
    "part number",
    "reference",
    "stock code",
    "sku",
]
HEADER_ORIGINAL_NUMBERS = [
    "الرقم الاصلي",
    "الرقم الأصلي",
    "رقم اصلي",
    "رقم أصلي",
    "ارقام اصلية",
    "أرقام أصلية",
    "ارقام أصلية",
    "أرقام اصلية",
    "original number",
    "الرقم الاصلي للقطعة",
    "الرقم الأصلي للقطعة",
]
HEADER_ALTERNATIVES = [
    "متشابهات",
    "متشابهاب",
    "المتشابهات",
    "التشابهات",
    "المشابهات",
    "المشابهين",
    "البدائل",
    "بدائل",
    "البديلة",
    "السيارات البديلة",
    "السيارة البديلة",
    "سيارات مشابهة",
    "سيارة مشابهة",
    "alternatives",
    "alternative",
    "سيارات بديلة",
    "السيارات المعادلة",
]
HEADER_ITEM_NAME = [
    "اسم السيارة",
    "اسم الصنف",
    "اسم السيارة/الصنف",
    "النوع",
    "نوع",
    "نوع السيارة",
    "نوع المركبة",
    "نوع المركبه",
    "الصنف",
    "item name",
    "name",
]
# أرقام داخلية / قطعة — بدون رقم الشركات حتى لا يُلتقط عمود الشركات قبل تعيينه صراحة
HEADER_ITEM_NUMBER = [
    "رقم الصنف",
    "رقم القطعة",
    "رقم الصنف الدولي",
    "item number",
    "part number",
]
HEADER_NOTES = ["ملاحظات", "ملاحظة", "notes"]


def _header_has_original_marker(h: str) -> bool:
    return ("اصلي" in h) or ("أصلي" in h) or ("اصليه" in h)


def _header_bad_for_original(h: str) -> bool:
    """لا نعتبر عمود «متشابهات/بدائل» عمودًا للرقم الأصلي إلا إذا ذُكر الأصلي في نفس العنوان."""
    if "متشابه" in h or "تشابه" in h:
        return not _header_has_original_marker(h)
    if "البدائل" in h or "بدائل" in h:
        return not _header_has_original_marker(h)
    return False


def _header_bad_for_alt(h: str) -> bool:
    """لا نلتقط عمود رقم الشركات أو الرقم الأصلي كعمود للبدائل."""
    if "رقم الشركات" in h or "رقم الشركة" in h:
        return True
    if "رقم شركة" in h and "بديل" not in h and "متشابه" not in h and "بدائل" not in h:
        return True
    if _header_has_original_marker(h) and "متشابه" not in h and "بديل" not in h and "بدائل" not in h and "تشابه" not in h:
        return True
    return False


def _header_bad_for_company(h: str) -> bool:
    """رقم الصنف (الواجهة) من عمود رقم الشركات فقط — لا نلتقط عمود النوع/الصنف."""
    if not h:
        return False
    if "رقم الشركات" in h or "رقم الشركة" in h:
        return False
    if "شركات" in h and "رقم" in h:
        return False
    if "شركة" in h and "رقم" in h and "نوع" not in h:
        return False
    if "نوع" in h and "شركة" not in h and "شركات" not in h:
        # «رقم نوع القطعة» وما شابهها قد يكون عمود المرجع التجاري وليس «نوع السيارة» فقط
        if (
            "قطعه" in h
            or "قطعة" in h
            or "القطعه" in h
            or "القطعة" in h
            or "صنف" in h
        ):
            return False
        return True
    if h in ("الصنف", "صنف") or (len(h) <= 12 and "صنف" in h and "رقم" not in h and "شرك" not in h):
        return True
    return False


def find_header_column(
    header_cells: list[str],
    keywords: list[str],
    *,
    used: set[int] | None = None,
    min_substring_len: int = 6,
    reject: Callable[[str], bool] | None = None,
) -> int | None:
    """
    يختار عمودًا واحدًا: مطابقة كاملة للعنوان أولًا، ثم جزئية بشرط طول الكلمة لتفادي أخطاء (مثل احتواء «رقم الصنف» على عنوان طويل خاطئ).
    عند عدة مطابقات: أطول كلمة مفتاحية ثم أقرب عمود لليسار.
    """
    if used is None:
        used = set()
    norm_headers = [normalize_text(h) for h in header_cells]
    sorted_kw = sorted({normalize_text(k) for k in keywords if normalize_text(k)}, key=len, reverse=True)
    if not sorted_kw:
        return None

    for idx, h in enumerate(norm_headers):
        if idx in used or not h:
            continue
        if reject and reject(h):
            continue
        if h in sorted_kw:
            return idx

    best_idx: int | None = None
    best_key: tuple[int, int] | None = None  # (-len(kw), col_index) minimal = longer kw, then left column
    for idx, h in enumerate(norm_headers):
        if idx in used or not h:
            continue
        if reject and reject(h):
            continue
        for kw in sorted_kw:
            if len(kw) < min_substring_len:
                continue
            if kw in h:
                key = (-len(kw), idx)
                if best_key is None or key < best_key:
                    best_key = key
                    best_idx = idx
    return best_idx


# عناوين عمود «رقم الصنف» في الواجهة (بعد normalize_text) — تشمل أشكال الكتابة الشائعة في الإكسل
COMPANY_HEADER_CANONICAL: frozenset[str] = frozenset(
    {
        normalize_text("رقم الشركات"),
        normalize_text("رقم الشركة"),
        normalize_text("رقم شركات"),
        normalize_text("ارقام الشركات"),
        normalize_text("أرقام الشركات"),
        normalize_text("ارقام الشركة"),
        normalize_text("أرقام الشركة"),
    }
)


def normalize_header_cell_for_match(raw: str) -> str:
    """إزالة مسافات غريبة وحروف عرض صفرية قد تمنع مطابقة عنوان «رقم الشركات»."""
    t = normalize_text(str(raw or ""))
    for ch in ("\u200c", "\u200b", "\ufeff", "\xa0"):
        t = t.replace(ch, "")
    return re.sub(r"\s+", " ", t).strip()


def find_company_column_by_exact_header(header_cells: list[str]) -> int | None:
    """
    عمود رقم الصنف = عمود عنوانه يطابق «رقم الشركات» أو مرادفاته بعد التطبيع.
    """
    for idx, raw in enumerate(header_cells):
        h = normalize_header_cell_for_match(raw)
        if not h:
            continue
        if h in COMPANY_HEADER_CANONICAL:
            return idx
        # عنوان طويل قليلاً لكنه واضح (مثل تعليق في الخلية)
        if len(h) <= 32 and "رقم" in h and ("شركات" in h or h.endswith("شركه")):
            if not _header_bad_for_company(h):
                return idx
    return None


def resolve_product_column_indices(header_cells: list[str]) -> dict[str, int | None]:
    used: set[int] = set()

    def assign(
        keywords: list[str],
        min_sub: int,
        reject: Callable[[str], bool] | None = None,
    ) -> int | None:
        idx = find_header_column(
            header_cells, keywords, used=used, min_substring_len=min_sub, reject=reject
        )
        if idx is not None:
            used.add(idx)
        return idx

    # أولوية مطلقة: عمود عنوانه بالضبط «رقم الشركات» في صف العناوين
    company_index = find_company_column_by_exact_header(header_cells)
    if company_index is not None:
        used.add(company_index)
    else:
        company_index = assign(HEADER_COMPANY_NUMBER, 6, reject=_header_bad_for_company)
    original_index = assign(HEADER_ORIGINAL_NUMBERS, 6, reject=_header_bad_for_original)
    alt_index = assign(HEADER_ALTERNATIVES, 5, reject=_header_bad_for_alt)

    if original_index is not None and alt_index == original_index:
        alt_index = None

    item_name_index = assign(HEADER_ITEM_NAME, 4)
    notes_index = assign(HEADER_NOTES, 4)

    item_number_index = find_header_column(
        header_cells, HEADER_ITEM_NUMBER, used=used, min_substring_len=6
    )
    if item_number_index is not None:
        used.add(item_number_index)
    elif company_index is not None:
        item_number_index = company_index
    else:
        item_number_index = 4 if 4 not in used else None
        if item_number_index is None:
            item_number_index = 4

    # مطابقة مرنة لعناوين «رقم/أرقام الشركات» إذا فاتتها القائمة (تاء مربوطة، دمج خلايا، إلخ)
    if company_index is None:
        skip_loose = {x for x in (original_index, alt_index, notes_index) if x is not None}
        for i, raw in enumerate(header_cells):
            if i in skip_loose:
                continue
            h = normalize_text(str(raw or ""))
            if not h:
                continue
            if ("رقم" in h or "كود" in h or "ارقام" in h) and ("شركات" in h or "شركه" in h):
                if _header_bad_for_company(h):
                    continue
                company_index = i
                break

    # غالبًا «رقم الشركات» بجانب «الرقم الأصلي» — جرّب يمين ثم يسار (ملفات ثيرموستات: الأصلي ثم الشركات)
    if company_index is None and original_index is not None:
        blocked = {x for x in (item_name_index, alt_index, original_index, notes_index) if x is not None}
        for delta in (1, -1, 2, -2):
            fb = original_index + delta
            if fb < 0 or fb in blocked:
                continue
            if fb >= len(header_cells):
                continue
            company_index = fb
            break

    return {
        "company_index": company_index,
        "original_index": original_index,
        "alt_index": alt_index,
        "item_name_index": item_name_index,
        "item_number_index": item_number_index,
        "notes_index": notes_index,
    }


def sanitize_alternatives_vs_original(alternatives: str, original_numbers: str) -> str:
    """خانة البدائل تبقى فارغة إن كانت فارغة في الملف؛ ولا نخزّن نصًا مطابقًا للرقم الأصلي."""
    a = (alternatives or "").strip()
    if not a:
        return ""
    o = (original_numbers or "").strip()
    if not o:
        return a
    if normalize_text(a) == normalize_text(o):
        return ""
    # فراغات/أسطر زائدة أو اختلاف بسيط يجعل النص يبدو مثل الأصلي في الواجهة
    if normalize_text_compact(a) == normalize_text_compact(o):
        return ""
    return a


def clean_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "none" else text


def forward_fill_header_row(raw_cells: list[object] | tuple[object, ...]) -> list[str]:
    """عند دمج خلايا العنوان أفقيًا تبقى الخلايا التالية فارغة في openpyxl — ننقل عنوان العمود للخلايا الفارغة."""
    cleaned = [clean_cell(v) for v in raw_cells]
    out: list[str] = []
    carry = ""
    for s in cleaned:
        if s:
            carry = s
            out.append(s)
        else:
            out.append(carry)
    return out


def pick_item_name(values: list[str], preferred_idx: int | None) -> str:
    def val(i: int) -> str:
        return values[i].strip() if 0 <= i < len(values) and values[i] else ""

    primary = val(preferred_idx if preferred_idx is not None else -1)
    if primary:
        return primary

    # Prefer early text-like columns if explicit header is missing.
    for i in [1, 0, 2, 3, 4, 5]:
        v = val(i)
        if not v:
            continue
        if re.search(r"[A-Za-z\u0600-\u06FF]", v):
            return v

    # Last fallback: first non-empty cell in early columns.
    for i in range(0, min(10, len(values))):
        v = val(i)
        if v:
            return v
    return ""


def normalize_source_file_name(file_name: str) -> str:
    value = str(file_name or "").strip().replace("\\", "/")
    value = re.sub(r"/+", "/", value).lstrip("./")
    return value or "unnamed.xlsx"


def load_gdrive_service_account_info() -> dict:
    raw_json = (os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON") or "").strip()
    raw_json_b64 = (os.getenv("GDRIVE_SERVICE_ACCOUNT_JSON_B64") or "").strip()
    if raw_json:
        return json.loads(raw_json)
    if raw_json_b64:
        return json.loads(base64.b64decode(raw_json_b64).decode("utf-8"))
    raise RuntimeError("Missing Google service account credentials")


def get_gdrive_service():
    if service_account is None or build is None:
        raise RuntimeError("Google Drive dependencies are not installed")
    info = load_gdrive_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def is_openpyxl_supported_file_name(file_name: str) -> bool:
    lower = str(file_name or "").lower()
    return lower.endswith(".xlsx") or lower.endswith(".xlsm")


def list_gdrive_excel_files(service, folder_id: str) -> list[dict]:
    excel_mime_types = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel.sheet.macroEnabled.12",
        "application/vnd.ms-excel.sheet.binary.macroEnabled.12",
        "application/vnd.ms-excel",
    }
    found_files: list[dict] = []
    stack: list[tuple[str, str]] = [(folder_id, "")]
    while stack:
        current_folder_id, current_path = stack.pop()
        page_token = None
        while True:
            resp = (
                service.files()
                .list(
                    q=f"'{current_folder_id}' in parents and trashed = false",
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    pageToken=page_token,
                    pageSize=1000,
                )
                .execute()
            )
            for f in resp.get("files", []):
                name = str(f.get("name") or "").strip()
                if not name:
                    continue
                relative_name = f"{current_path}/{name}" if current_path else name
                mime = str(f.get("mimeType") or "")
                if mime == "application/vnd.google-apps.folder":
                    stack.append((str(f.get("id")), relative_name))
                    continue
                is_excel = mime in excel_mime_types or re.search(r"\.(xlsx|xlsm|xlsb|xls)$", name, re.I)
                if not is_excel:
                    continue
                found_files.append(
                    {
                        "id": str(f.get("id") or ""),
                        "name": name,
                        "relative_name": relative_name,
                        "mimeType": mime,
                        "modifiedTime": str(f.get("modifiedTime") or ""),
                        "size": int(f.get("size") or 0),
                    }
                )
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    return found_files


def download_gdrive_file_bytes(service, file_id: str) -> bytes:
    if MediaIoBaseDownload is None:
        raise RuntimeError("Google Drive download helper is unavailable")
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request, chunksize=2 * 1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buffer.getvalue()


def process_excel_file(file_bytes: bytes, file_name: str, content_hash: str, file_size: int) -> tuple[int, int]:
    conn = get_db()
    inserted_rows = 0
    unique_item_names: set[str] = set()
    source_key = normalize_source_file_name(file_name)

    try:
        conn.execute("DELETE FROM products WHERE source_file = ?", (source_key,))
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
        try:
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                rows_to_insert = []
                preview_rows = list(ws.iter_rows(min_row=1, max_row=30, min_col=1, max_col=40, values_only=True))
                header_candidates = [
                    "اسم الصنف",
                    "اسم السيارة",
                    "رقم الصنف",
                    "رقم الشركة",
                    "رقم الشركات",
                    "الرقم الاصلي",
                    "البدائل",
                    "متشابهات",
                ]
                best_idx = 0
                best_score = -10
                for idx, prow in enumerate(preview_rows):
                    cells = [clean_cell(v) for v in (prow or ())]
                    score = sum(1 for k in header_candidates if get_header_index(cells, [k]) is not None)
                    if not preview_row_is_likely_header_row(cells):
                        score -= 12
                    if score > best_score:
                        best_score = score
                        best_idx = idx
                header_row = preview_rows[best_idx] if preview_rows else ()
                header_cells = forward_fill_header_row(list(header_row or ()))
                data_start_row = best_idx + 2

                cols = resolve_product_column_indices(header_cells)
                company_index = cols["company_index"]
                original_index = cols["original_index"]
                alt_index = cols["alt_index"]
                item_name_index = cols["item_name_index"]
                item_number_index = cols["item_number_index"]
                notes_index = cols["notes_index"]
                if item_name_index is None:
                    item_name_index = 0

                for row in ws.iter_rows(min_row=data_start_row, min_col=1, max_col=40, values_only=True):
                    values = [clean_cell(v) for v in row]

                    def by_idx(idx: int | None, default: str = "") -> str:
                        if idx is None:
                            return default
                        if idx < 0 or idx >= len(values):
                            return default
                        return values[idx]

                    item_name = pick_item_name(values, item_name_index)
                    item_number = by_idx(item_number_index)
                    # رقم الصنف في الواجهة = عمود رقم الشركات؛ إن كانت الخلية فارغة (دمج) نجرّب عمودًا مجاورًا للرقم الأصلي
                    company_number = by_idx(company_index)
                    original_numbers = by_idx(original_index)
                    if (
                        not (company_number or "").strip()
                        and (original_numbers or "").strip()
                        and original_index is not None
                    ):
                        o_c = normalize_text_compact(original_numbers)
                        skip_adj = {
                            i
                            for i in (
                                company_index,
                                alt_index,
                                notes_index,
                                item_name_index,
                                item_number_index,
                            )
                            if i is not None
                        }
                        for delta in (1, -1):
                            j = original_index + delta
                            if j in skip_adj or j < 0 or j >= len(values):
                                continue
                            cand = (values[j] or "").strip() if j < len(values) else ""
                            if not cand or normalize_text_compact(cand) == o_c:
                                continue
                            company_number = cand
                            break
                    notes = by_idx(notes_index)
                    alternatives = by_idx(alt_index) if alt_index is not None else ""
                    alternatives = sanitize_alternatives_vs_original(alternatives, original_numbers)
                    size_width = by_idx(1)
                    size_diameter = by_idx(2)
                    size_height = by_idx(3)

                    if not any(
                        [
                            item_name,
                            item_number,
                            company_number,
                            original_numbers,
                            notes,
                            alternatives,
                            size_width,
                            size_diameter,
                            size_height,
                        ]
                    ):
                        continue

                    rows_to_insert.append(
                        (
                            item_name,
                            item_number,
                            company_number,
                            original_numbers,
                            notes,
                            alternatives,
                            size_width,
                            size_diameter,
                            size_height,
                            source_key,
                            str(sheet_name),
                        )
                    )
                    if item_name:
                        unique_item_names.add(item_name)

                if rows_to_insert:
                    conn.executemany(
                        """
                        INSERT INTO products (
                            item_name, item_number, company_number, original_numbers, notes, alternatives,
                            size_width, size_diameter, size_height, source_file, source_sheet
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows_to_insert,
                    )
                    inserted_rows += len(rows_to_insert)
        finally:
            wb.close()

        conn.commit()
        upsert_uploaded_file_meta(source_key, inserted_rows, content_hash, file_size)
        return inserted_rows, len(unique_item_names)
    finally:
        conn.close()


def _wiper_row_looks_like_header(cells: list[str]) -> bool:
    """
    تمييز سطر العناوين دون الخلط مع صف بيانات قد يحتوي كلمات مثل «موديل» أو «رقم» كجزء من النص.
    """
    samples = [clean_cell(x) for x in (cells[:5] if cells else [])]
    if samples and len(samples[0]) > 48:
        return False
    joined = normalize_text(" ".join(samples))
    hits = 0
    for kw in ("اسم", "سياره", "سيارة", "موديل", "ماتور", "محرك", "موقع", "قشاط", "قشط", "رقم"):
        if kw in joined:
            hits += 1
    if hits >= 3:
        return True
    if "اسم" in joined and "موديل" in joined:
        return True
    if any(k in joined for k in ("قشاط", "قشط", "مساح", "ماسحه")) and ("موقع" in joined or "رقم" in joined):
        return True
    return False


def process_wiper_excel_file(file_bytes: bytes, file_name: str, content_hash: str, file_size: int) -> int:
    """أعمدة A–E: سيارة، موديل، ماتور، موقع القشاط، رقم القشاط."""
    conn = get_db()
    inserted_rows = 0
    source_key = normalize_source_file_name(file_name)

    try:
        conn.execute("DELETE FROM wiper_rows WHERE source_file = ?", (source_key,))
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
        try:
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                preview = list(ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=5, values_only=True))
                first_row = preview[0] if preview else ()
                first_cells = [clean_cell(v) for v in (first_row or ())]
                data_start_row = 2 if _wiper_row_looks_like_header(first_cells) else 1

                rows_to_insert: list[tuple[str, str, str, str, str, str, str]] = []
                for row in ws.iter_rows(min_row=data_start_row, min_col=1, max_col=5, values_only=True):
                    values = [clean_cell(v) for v in row]
                    while len(values) < 5:
                        values.append("")
                    car_name, car_model, engine, wiper_location, wiper_number = values[:5]
                    if not any([car_name, car_model, engine, wiper_location, wiper_number]):
                        continue
                    rows_to_insert.append(
                        (
                            car_name,
                            car_model,
                            engine,
                            wiper_location,
                            wiper_number,
                            source_key,
                            str(sheet_name),
                        )
                    )

                if rows_to_insert:
                    conn.executemany(
                        """
                        INSERT INTO wiper_rows (
                            car_name, car_model, engine, wiper_location, wiper_number, source_file, source_sheet
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows_to_insert,
                    )
                    inserted_rows += len(rows_to_insert)
        finally:
            wb.close()

        conn.commit()
        upsert_wiper_file_meta(source_key, inserted_rows, content_hash, file_size)
        return inserted_rows
    finally:
        conn.close()


def _location_row_looks_like_header(cells: list[str]) -> bool:
    samples = [clean_cell(x) for x in (cells[:11] if cells else [])]
    joined = normalize_text(" ".join(samples))
    hits = 0
    for kw in (
        "صنف",
        "اسم",
        "موقع",
        "باركود",
        "شركة",
        "منتج",
        "مكان",
        "علامه",
        "ملاحظ",
        "سعر",
        "كميه",
        "كمية",
        "location",
        "barcode",
        "company",
        "brand",
        "price",
    ):
        if kw in joined:
            hits += 1
    return hits >= 2


def process_location_excel_file(
    file_bytes: bytes,
    file_name: str,
    content_hash: str,
    file_size: int,
    *,
    replace_entire_table: bool = False,
) -> int:
    """أعمدة Excel: B رقم أصلي، C صنف، D باركود، E علامة، F شركة، G كمية، I موقع، J ملاحظات، K باركود الوحدات، L سعر."""
    conn = get_db()
    inserted_rows = 0
    source_key = normalize_source_file_name(file_name)

    try:
        ensure_location_rows_import_schema(conn)
        if replace_entire_table:
            conn.execute("DELETE FROM location_rows")
        else:
            conn.execute("DELETE FROM location_rows WHERE source_file = ?", (source_key,))
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
        try:
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                preview = list(ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=12, values_only=True))
                first_row = preview[0] if preview else ()
                first_cells = [clean_cell(v) for v in (first_row or ())]
                data_start_row = 2 if _location_row_looks_like_header(first_cells) else 1

                rows_to_insert: list[tuple[str, ...]] = []
                for row in ws.iter_rows(min_row=data_start_row, min_col=1, max_col=12, values_only=True):
                    values = [clean_cell(v) for v in row]
                    while len(values) < 12:
                        values.append("")
                    original_number = values[1]
                    item_name = values[2]
                    barcode = str(values[3] or "").strip()
                    brand = values[4]
                    company = values[5]
                    quantity = str(values[6] or "").strip()
                    location = values[8]
                    notes = values[9]
                    unit_barcodes = str(values[10] or "").strip()
                    price = values[11]
                    if not any(
                        [
                            item_name,
                            location,
                            company,
                            original_number,
                            barcode,
                            brand,
                            notes,
                            quantity,
                            unit_barcodes,
                            price,
                        ]
                    ):
                        continue
                    merged_barcode = " | ".join(x for x in [barcode, unit_barcodes] if x)
                    nb = location_row_normalized_barcode_value(barcode, unit_barcodes)
                    nt = location_row_normalized_text_value(item_name, company, brand, notes, original_number)
                    rows_to_insert.append(
                        (
                            item_name,
                            location,
                            company,
                            original_number,
                            merged_barcode,
                            brand,
                            notes,
                            quantity,
                            price,
                            source_key,
                            str(sheet_name),
                            nb,
                            nt,
                        )
                    )

                if rows_to_insert:
                    conn.executemany(
                        """
                        INSERT INTO location_rows (
                            item_name, location, company, original_number, barcode,
                            brand, notes, quantity, price, source_file, source_sheet,
                            normalized_barcode, normalized_text
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows_to_insert,
                    )
                    inserted_rows += len(rows_to_insert)
        finally:
            wb.close()

        conn.commit()
        upsert_location_file_meta(source_key, inserted_rows, content_hash, file_size)
        return inserted_rows
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def app_kv_get(key: str, default: str = "") -> str:
    conn = get_db()
    try:
        row = conn.execute("SELECT value FROM app_kv WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        return str(row["value"] if isinstance(row, dict) else row[0] or default)
    except Exception:
        return default
    finally:
        conn.close()


def app_kv_set(key: str, value: str) -> None:
    conn = get_db()
    try:
        if conn.postgres:
            conn.execute(
                """
                INSERT INTO app_kv (key, value) VALUES (?, ?)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )
        else:
            conn.execute(
                "INSERT INTO app_kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
        conn.commit()
    finally:
        conn.close()


def hesabate_configured() -> bool:
    return bool(HESABATE_API_HOST and HESABATE_API_USERNAME and HESABATE_API_PASSWORD)


def ensure_hesabate_product_cache_schema(conn: DBConnection) -> None:
    if conn.postgres:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hesabate_product_cache (
                product_id TEXT PRIMARY KEY,
                barcode TEXT,
                model_no TEXT,
                other_codes TEXT,
                name TEXT,
                note TEXT,
                company_id TEXT,
                trade_id TEXT,
                model_id TEXT,
                amount TEXT,
                price TEXT,
                synced_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hesabate_barcode_index (
                barcode_key TEXT NOT NULL,
                product_id TEXT NOT NULL,
                PRIMARY KEY (barcode_key, product_id)
            )
            """
        )
    else:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hesabate_product_cache (
                product_id TEXT PRIMARY KEY,
                barcode TEXT,
                model_no TEXT,
                other_codes TEXT,
                name TEXT,
                note TEXT,
                company_id TEXT,
                trade_id TEXT,
                model_id TEXT,
                amount TEXT,
                price TEXT,
                synced_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hesabate_barcode_index (
                barcode_key TEXT NOT NULL,
                product_id TEXT NOT NULL,
                PRIMARY KEY (barcode_key, product_id)
            )
            """
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_hesabate_barcode_index_key ON hesabate_barcode_index(barcode_key)"
    )


def hesabate_product_cache_count() -> int:
    conn = get_db()
    try:
        ensure_hesabate_product_cache_schema(conn)
        row = conn.execute("SELECT COUNT(*) AS c FROM hesabate_product_cache").fetchone()
        return int(sql_row_to_dict(row).get("c") or 0) if row is not None else 0
    except Exception:
        return 0
    finally:
        conn.close()


def _fast_cache_barcode_keys(barcode: str, other_codes: str) -> list[str]:
    """مفاتيح باركود خفيفة للفهرس — تشمل كل الباركودات الإضافية وليس الأول فقط."""
    out: list[str] = []
    for raw in (barcode, other_codes):
        for part in _extract_location_unit_barcodes_from_cell(raw):
            # لا تفهرس لاحقات قصيرة جداً (HI) وحدها — تسبب تطابقات خاطئة
            if len(part) < 4 and not part.isdigit():
                continue
            if part not in out:
                out.append(part)
            if part.isdigit():
                stripped = part.lstrip("0") or "0"
                if stripped not in out:
                    out.append(stripped)
            # حد أعلى كافٍ لعدة باركودات + أشكال الأصفار + الشكل المدمج
            if len(out) >= 48:
                return out
    return out


def _lookup_product_cache_rows_by_compact_barcode(
    conn: DBConnection,
    keys: list[str],
    *,
    limit: int = 10,
) -> list[Any]:
    """
    بحث مباشر في كاش المنتجات بالباركود المدمج — يتجاوز فهرس barcode_key القديم.
    يغطي 223112U001        HI عبر LIKE على البادئة ثم مساواة مدمجة.
    """
    key_set = {k for k in keys if k}
    if not key_set:
        return []
    prefixes = _spaced_barcode_like_prefixes(key_set)
    if not prefixes:
        # مفاتيح بلا أحرف: جرّب أطول مفتاح كبادئة
        prefixes = [k for k in sorted(key_set, key=len, reverse=True) if len(k) >= 6][:4]
    if not prefixes:
        return []
    matched: list[Any] = []
    seen: set[str] = set()
    lim = max(1, min(int(limit), 30))
    for pref in prefixes:
        pat = f"{_like_pattern_escape_sql(pref)}%"
        rows = conn.execute(
            """
            SELECT product_id, barcode, model_no, other_codes, name, note,
                   company_id, trade_id, model_id, amount, price
            FROM hesabate_product_cache
            WHERE COALESCE(TRIM(barcode), '') <> ''
              AND (
                barcode LIKE ? ESCAPE '\\'
                OR other_codes LIKE ? ESCAPE '\\'
              )
            LIMIT ?
            """,
            (pat, pat, LOCATION_BARCODE_LIKE_FALLBACK_FETCH_CAP),
        ).fetchall()
        for row in rows:
            rd = sql_row_to_dict(row)
            pid = str(rd.get("product_id") or "").strip()
            if not pid or pid in seen:
                continue
            # طابق الباركود أو other_codes بالشكل المدمج
            blob = f"{rd.get('barcode') or ''} {rd.get('other_codes') or ''}"
            cand_keys = set(_extract_location_unit_barcodes_from_cell(blob))
            full = normalize_barcode_lookup_key(str(rd.get("barcode") or ""))
            if full:
                cand_keys.add(full)
            if key_set & cand_keys:
                seen.add(pid)
                matched.append(row)
                if len(matched) >= lim:
                    return matched
    return matched


def _product_cache_row_tuple(prod: dict) -> tuple[str, ...]:
    pid = str(prod.get("id") or "").strip()
    return (
        pid,
        str(prod.get("barcode") or "").strip(),
        str(prod.get("model_no") or "").strip(),
        str(prod.get("other_codes") or "").strip(),
        str(prod.get("name") or "").strip(),
        str(prod.get("note") or "").strip(),
        str(prod.get("company_id") or "").strip(),
        str(prod.get("trade_id") or "").strip(),
        str(prod.get("model_id") or "").strip(),
        str(prod.get("amount") or "").strip(),
        str(prod.get("price") or "").strip(),
    )


def _product_cache_upsert_sql(postgres: bool) -> str:
    excl = "EXCLUDED" if postgres else "excluded"
    return f"""
INSERT INTO hesabate_product_cache (
    product_id, barcode, model_no, other_codes, name, note,
    company_id, trade_id, model_id, amount, price
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(product_id) DO UPDATE SET
    barcode = {excl}.barcode,
    model_no = {excl}.model_no,
    other_codes = {excl}.other_codes,
    name = {excl}.name,
    note = {excl}.note,
    company_id = {excl}.company_id,
    trade_id = {excl}.trade_id,
    model_id = {excl}.model_id,
    amount = {excl}.amount,
    price = {excl}.price
"""


def _full_rebuild_hesabate_product_cache(conn: DBConnection, table: list) -> int:
    if conn.postgres:
        conn.execute("TRUNCATE hesabate_barcode_index, hesabate_product_cache")
    else:
        conn.execute("DELETE FROM hesabate_barcode_index")
        conn.execute("DELETE FROM hesabate_product_cache")
    prod_batch: list[tuple] = []
    idx_batch: list[tuple[str, str]] = []
    n = 0
    idx_sql = (
        "INSERT INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?) ON CONFLICT DO NOTHING"
        if conn.postgres
        else "INSERT OR IGNORE INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?)"
    )
    for prod in table:
        if not isinstance(prod, dict):
            continue
        row = _product_cache_row_tuple(prod)
        if not row[0]:
            continue
        prod_batch.append(row)
        for k in _fast_cache_barcode_keys(row[1], row[3]):
            idx_batch.append((k, row[0]))
        n += 1
        if len(prod_batch) >= 2500:
            conn.executemany(
                """
                INSERT INTO hesabate_product_cache (
                    product_id, barcode, model_no, other_codes, name, note,
                    company_id, trade_id, model_id, amount, price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                prod_batch,
            )
            prod_batch = []
        if len(idx_batch) >= 5000:
            conn.executemany(idx_sql, idx_batch)
            idx_batch = []
    if prod_batch:
        conn.executemany(
            """
            INSERT INTO hesabate_product_cache (
                product_id, barcode, model_no, other_codes, name, note,
                company_id, trade_id, model_id, amount, price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            prod_batch,
        )
    if idx_batch:
        conn.executemany(idx_sql, idx_batch)
    return n


def _products_catalog_fingerprint(table: list) -> str:
    """بصمة سريعة للكتالوج — تخطّي كتابة Neon إن لم يتغيّر شيء."""
    parts: list[str] = []
    for prod in table:
        if not isinstance(prod, dict):
            continue
        pid = str(prod.get("id") or "").strip()
        if not pid:
            continue
        parts.append(
            "|".join(
                (
                    pid,
                    str(prod.get("barcode") or ""),
                    str(prod.get("price") or ""),
                    str(prod.get("amount") or ""),
                    str(prod.get("name") or ""),
                    str(prod.get("other_codes") or ""),
                    str(prod.get("model_id") or ""),
                )
            )
        )
    parts.sort()
    raw = f"{len(parts)}\n" + "\n".join(parts)
    return hashlib.md5(raw.encode("utf-8", errors="ignore")).hexdigest()


def rebuild_hesabate_barcode_index_from_cache() -> int:
    """إعادة بناء فهرس الباركود من كاش الأصناف — لكل الباركودات في barcode + other_codes."""
    conn = get_db()
    try:
        ensure_hesabate_product_cache_schema(conn)
        if conn.postgres:
            conn.execute("TRUNCATE hesabate_barcode_index")
        else:
            conn.execute("DELETE FROM hesabate_barcode_index")
        idx_sql = (
            "INSERT INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?) ON CONFLICT DO NOTHING"
            if conn.postgres
            else "INSERT OR IGNORE INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?)"
        )
        n = 0
        batch: list[tuple[str, str]] = []
        for row in conn.execute(
            "SELECT product_id, barcode, other_codes FROM hesabate_product_cache"
        ).fetchall():
            rd = sql_row_to_dict(row)
            pid = str(rd.get("product_id") or "").strip()
            if not pid:
                continue
            for k in _fast_cache_barcode_keys(str(rd.get("barcode") or ""), str(rd.get("other_codes") or "")):
                batch.append((k, pid))
                n += 1
                if len(batch) >= 3000:
                    conn.executemany(idx_sql, batch)
                    batch = []
        if batch:
            conn.executemany(idx_sql, batch)
        conn.commit()
        return n
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def upsert_hesabate_product_cache_hits(raw_products: list) -> int:
    """
    كتابة سريعة لنتائج الجلب فقط (صنف أو أكثر) — بدون مسح/مقارنة الكتالوج كامل.
    يكفي لجعل نفس الباركود لحظياً في المرة التالية.
    """
    rows: list[tuple] = []
    for prod in raw_products or []:
        if not isinstance(prod, dict):
            continue
        row = _product_cache_row_tuple(prod)
        if row[0]:
            rows.append(row)
    if not rows:
        return 0
    conn = get_db()
    try:
        ensure_hesabate_product_cache_schema(conn)
        upsert_sql = _product_cache_upsert_sql(conn.postgres)
        conn.executemany(upsert_sql, rows)
        idx_sql = (
            "INSERT INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?) ON CONFLICT DO NOTHING"
            if conn.postgres
            else "INSERT OR IGNORE INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?)"
        )
        pids = [r[0] for r in rows]
        for i in range(0, len(pids), 200):
            chunk = pids[i : i + 200]
            ph = ", ".join(["?"] * len(chunk))
            conn.execute(f"DELETE FROM hesabate_barcode_index WHERE product_id IN ({ph})", chunk)
        idx_batch: list[tuple[str, str]] = []
        for row in rows:
            for k in _fast_cache_barcode_keys(row[1], row[3]):
                idx_batch.append((k, row[0]))
        if idx_batch:
            conn.executemany(idx_sql, idx_batch)
        conn.commit()
        return len(rows)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        conn.close()


def replace_hesabate_product_cache(products_payload: dict) -> int:
    """
    تحديث ذكي لكاش الأصناف:
    - بصمة كتالوج → تخطّي فوري إن لا تغيير
    - أول مرة / تغيير كبير → إعادة بناء كاملة سريعة
    - قطع جديدة أو تعديلات قليلة → upsert للفرق فقط
    """
    table = products_payload.get("table") if isinstance(products_payload, dict) else None
    if not isinstance(table, list) or not table:
        return 0
    fp = _products_catalog_fingerprint(table)
    if fp and fp == str(app_kv_get("hesabate_product_cache_fp", "") or ""):
        app_kv_set("hesabate_product_cache_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return 0
    conn = get_db()
    try:
        ensure_hesabate_product_cache_schema(conn)
        # تحميل خفيف: فقط الحقول المستخدمة في المقارنة
        existing: dict[str, tuple] = {}
        try:
            for row in conn.execute(
                """
                SELECT product_id, barcode, model_no, other_codes, name, note,
                       company_id, trade_id, model_id, amount, price
                FROM hesabate_product_cache
                """
            ).fetchall():
                rd = sql_row_to_dict(row)
                pid = str(rd.get("product_id") or "")
                if pid:
                    existing[pid] = (
                        pid,
                        str(rd.get("barcode") or ""),
                        str(rd.get("model_no") or ""),
                        str(rd.get("other_codes") or ""),
                        str(rd.get("name") or ""),
                        str(rd.get("note") or ""),
                        str(rd.get("company_id") or ""),
                        str(rd.get("trade_id") or ""),
                        str(rd.get("model_id") or ""),
                        str(rd.get("amount") or ""),
                        str(rd.get("price") or ""),
                    )
        except Exception:
            existing = {}

        to_upsert: list[tuple] = []
        for prod in table:
            if not isinstance(prod, dict):
                continue
            row = _product_cache_row_tuple(prod)
            if not row[0]:
                continue
            if existing.get(row[0]) != row:
                to_upsert.append(row)

        total = len(table)
        if not to_upsert:
            app_kv_set("hesabate_product_cache_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            app_kv_set("hesabate_product_cache_rows", str(len(existing) or total))
            if fp:
                app_kv_set("hesabate_product_cache_fp", fp)
            return 0

        # إعادة بناء كاملة إن الكاش فارغ أو التغيير كبير
        if (not existing) or len(to_upsert) > max(400, int(total * 0.30)):
            n = _full_rebuild_hesabate_product_cache(conn, table)
            conn.commit()
            app_kv_set("hesabate_product_cache_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            app_kv_set("hesabate_product_cache_rows", str(n))
            if fp:
                app_kv_set("hesabate_product_cache_fp", fp)
            return n

        # مسار ذكي: فقط القطع الجديدة/المعدّلة
        idx_sql = (
            "INSERT INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?) ON CONFLICT DO NOTHING"
            if conn.postgres
            else "INSERT OR IGNORE INTO hesabate_barcode_index (barcode_key, product_id) VALUES (?, ?)"
        )
        upsert_sql = _product_cache_upsert_sql(conn.postgres)
        batch: list[tuple] = []
        changed_pids: list[str] = []
        for row in to_upsert:
            batch.append(row)
            changed_pids.append(row[0])
            if len(batch) >= 800:
                conn.executemany(upsert_sql, batch)
                batch = []
        if batch:
            conn.executemany(upsert_sql, batch)

        for i in range(0, len(changed_pids), 400):
            chunk = changed_pids[i : i + 400]
            ph = ", ".join(["?"] * len(chunk))
            conn.execute(f"DELETE FROM hesabate_barcode_index WHERE product_id IN ({ph})", chunk)
        idx_batch: list[tuple[str, str]] = []
        for row in to_upsert:
            for k in _fast_cache_barcode_keys(row[1], row[3]):
                idx_batch.append((k, row[0]))
            if len(idx_batch) >= 3000:
                conn.executemany(idx_sql, idx_batch)
                idx_batch = []
        if idx_batch:
            conn.executemany(idx_sql, idx_batch)

        conn.commit()
        final_n = len(existing) + sum(1 for r in to_upsert if r[0] not in existing)
        app_kv_set("hesabate_product_cache_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        app_kv_set("hesabate_product_cache_rows", str(final_n))
        app_kv_set("hesabate_product_cache_delta", str(len(to_upsert)))
        if fp:
            app_kv_set("hesabate_product_cache_fp", fp)
        return len(to_upsert)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def lookup_hesabate_product_cache(
    *,
    barcode_keys: list[str] | None = None,
    text_query: str = "",
    limit: int = 10,
) -> list[dict[str, Any]]:
    """بحث فوري في كاش Neon/SQLite بدون طلب حساباتي."""
    keys = [k for k in (barcode_keys or []) if k]
    qt = str(text_query or "").strip()
    if not keys and not qt:
        return []
    conn = get_db()
    try:
        ensure_hesabate_product_cache_schema(conn)
        rows: list[Any] = []
        if keys:
            placeholders = ", ".join(["?"] * len(keys))
            rows = conn.execute(
                f"""
                SELECT DISTINCT p.product_id, p.barcode, p.model_no, p.other_codes, p.name, p.note,
                       p.company_id, p.trade_id, p.model_id, p.amount, p.price
                FROM hesabate_barcode_index i
                JOIN hesabate_product_cache p ON p.product_id = i.product_id
                WHERE i.barcode_key IN ({placeholders})
                LIMIT ?
                """,
                (*keys, max(1, min(int(limit), 30))),
            ).fetchall()
            # فهرس قديم / باركود بمسافات: طابق عمود barcode مباشرة بالشكل المدمج
            if not rows:
                rows = _lookup_product_cache_rows_by_compact_barcode(
                    conn, keys, limit=max(1, min(int(limit), 30))
                )
        if not rows and qt:
            qkeys = barcode_search_keys(qt)
            if qkeys:
                placeholders = ", ".join(["?"] * len(qkeys))
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT p.product_id, p.barcode, p.model_no, p.other_codes, p.name, p.note,
                           p.company_id, p.trade_id, p.model_id, p.amount, p.price
                    FROM hesabate_barcode_index i
                    JOIN hesabate_product_cache p ON p.product_id = i.product_id
                    WHERE i.barcode_key IN ({placeholders})
                    LIMIT ?
                    """,
                    (*qkeys, max(1, min(int(limit), 30))),
                ).fetchall()
            if not rows:
                esc = f"%{_like_pattern_escape_sql(qt.lower())}%"
                rows = conn.execute(
                    """
                    SELECT product_id, barcode, model_no, other_codes, name, note,
                           company_id, trade_id, model_id, amount, price
                    FROM hesabate_product_cache
                    WHERE lower(COALESCE(model_no,'')) LIKE ? ESCAPE '\\'
                       OR lower(COALESCE(name,'')) LIKE ? ESCAPE '\\'
                       OR lower(COALESCE(note,'')) LIKE ? ESCAPE '\\'
                       OR lower(COALESCE(other_codes,'')) LIKE ? ESCAPE '\\'
                       OR lower(COALESCE(barcode,'')) LIKE ? ESCAPE '\\'
                    LIMIT ?
                    """,
                    (esc, esc, esc, esc, esc, max(1, min(int(limit), 30))),
                ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            rd = sql_row_to_dict(row)
            out.append(
                {
                    "id": rd.get("product_id") or "",
                    "barcode": rd.get("barcode") or "",
                    "model_no": rd.get("model_no") or "",
                    "other_codes": rd.get("other_codes") or "",
                    "name": rd.get("name") or "",
                    "note": rd.get("note") or "",
                    "company_id": rd.get("company_id") or "",
                    "trade_id": rd.get("trade_id") or "",
                    "model_id": rd.get("model_id") or "",
                    "amount": rd.get("amount") or "",
                    "price": rd.get("price") or "",
                }
            )
        return out
    except Exception:
        return []
    finally:
        conn.close()


def _hesabate_usage_month_key() -> str:
    return datetime.now().strftime("%Y-%m")


def _load_hesabate_usage() -> dict:
    month = _hesabate_usage_month_key()
    raw = app_kv_get("hesabate_usage_json", "")
    data: dict = {}
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}
    if str(data.get("month") or "") != month:
        return {
            "month": month,
            "count": 0,
            "last_at": "",
            "last_action": "",
            "last_type": "",
            "last_sync_requests": 0,
            "last_sync_at": "",
        }
    try:
        count = int(data.get("count") or 0)
    except (TypeError, ValueError):
        count = 0
    try:
        last_sync_requests = int(data.get("last_sync_requests") or 0)
    except (TypeError, ValueError):
        last_sync_requests = 0
    return {
        "month": month,
        "count": max(0, count),
        "last_at": str(data.get("last_at") or "").strip(),
        "last_action": str(data.get("last_action") or "").strip(),
        "last_type": str(data.get("last_type") or "").strip(),
        "last_sync_requests": max(0, last_sync_requests),
        "last_sync_at": str(data.get("last_sync_at") or "").strip(),
    }


def _save_hesabate_usage(data: dict) -> None:
    app_kv_set("hesabate_usage_json", json.dumps(data, ensure_ascii=False))


def record_hesabate_api_request(host: str = "", action: str = "", type_name: str = "") -> None:
    """تجميع في الذاكرة أثناء المزامنة لتفادي قفل SQLite."""
    with _hesabate_usage_mem_lock:
        _hesabate_usage_pending["delta"] = int(_hesabate_usage_pending.get("delta") or 0) + 1
        _hesabate_usage_pending["last_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        _hesabate_usage_pending["last_action"] = str(action or "").strip()
        _hesabate_usage_pending["last_type"] = str(type_name or "").strip()
        if host:
            _hesabate_usage_pending["last_host"] = str(host).strip()


def flush_hesabate_usage_pending(*, last_sync_requests: int | None = None) -> None:
    with _hesabate_usage_mem_lock:
        delta = int(_hesabate_usage_pending.get("delta") or 0)
        last_action = str(_hesabate_usage_pending.get("last_action") or "")
        last_type = str(_hesabate_usage_pending.get("last_type") or "")
        last_host = str(_hesabate_usage_pending.get("last_host") or "")
        last_at = str(_hesabate_usage_pending.get("last_at") or "")
        _hesabate_usage_pending["delta"] = 0
    st = _load_hesabate_usage()
    month = _hesabate_usage_month_key()
    if st.get("month") != month:
        st = {
            "month": month,
            "count": 0,
            "last_at": "",
            "last_action": "",
            "last_type": "",
            "last_sync_requests": int(st.get("last_sync_requests") or 0),
            "last_sync_at": str(st.get("last_sync_at") or ""),
        }
    st["count"] = int(st.get("count") or 0) + max(0, delta)
    st["month"] = month
    if last_at:
        st["last_at"] = last_at
    if last_action:
        st["last_action"] = last_action
    if last_type:
        st["last_type"] = last_type
    if last_host:
        st["last_host"] = last_host
    if last_sync_requests is not None:
        st["last_sync_requests"] = max(0, int(last_sync_requests))
        st["last_sync_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    _save_hesabate_usage(st)


def record_hesabate_sync_batch(request_count: int) -> None:
    flush_hesabate_usage_pending(last_sync_requests=int(request_count or 0))


def bump_hesabate_auto_sync_count() -> int:
    """عداد مرات المزامنة التلقائية الناجحة (cron / جدولة) — ليس الزر اليدوي."""
    try:
        n = int(app_kv_get("hesabate_auto_sync_count", "0") or "0") + 1
    except (TypeError, ValueError):
        n = 1
    n = max(1, n)
    app_kv_set("hesabate_auto_sync_count", str(n))
    app_kv_set("hesabate_auto_sync_last_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return n


def touch_hesabate_cron_ping(*, reason: str = "") -> str:
    """يسجّل أن المجدول وصل للسيرفر (حتى لو تخطّى لحداثة البيانات)."""
    at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    app_kv_set("hesabate_cron_last_ping_at", at)
    if reason:
        app_kv_set("hesabate_cron_last_ping_reason", str(reason)[:80])
    return at


def hesabate_auto_sync_stats() -> dict[str, Any]:
    try:
        count = int(app_kv_get("hesabate_auto_sync_count", "0") or "0")
    except (TypeError, ValueError):
        count = 0
    return {
        "auto_sync_count": max(0, count),
        "auto_sync_last_at": app_kv_get("hesabate_auto_sync_last_at", ""),
        "cron_last_ping_at": app_kv_get("hesabate_cron_last_ping_at", ""),
        "cron_last_ping_reason": app_kv_get("hesabate_cron_last_ping_reason", ""),
    }


def hesabate_usage_summary() -> dict:
    st = _load_hesabate_usage()
    with _hesabate_usage_mem_lock:
        pending = int(_hesabate_usage_pending.get("delta") or 0)
    limit = HESABATE_MONTHLY_REQUEST_LIMIT
    count = int(st.get("count") or 0) + max(0, pending)
    remaining = max(0, limit - count)
    pct = (100.0 * count / limit) if limit > 0 else 0.0
    return {
        **st,
        "limit": limit,
        "remaining": remaining,
        "percent": round(pct, 2),
        "configured": hesabate_configured(),
        "host": HESABATE_API_HOST,
        "sync_min_interval_sec": HESABATE_SYNC_MIN_INTERVAL_SEC,
        "last_location_sync_at": app_kv_get("hesabate_last_sync_at", ""),
        "last_location_rows": app_kv_get("hesabate_last_rows", "0"),
        **hesabate_auto_sync_stats(),
    }


def _hesabate_last_sync_age_sec() -> float:
    raw = app_kv_get("hesabate_last_sync_at", "")
    if not raw:
        return 1e12
    try:
        ts = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        return max(0.0, (datetime.now() - ts).total_seconds())
    except Exception:
        return 1e12


def _shelf_barcode_keys(barcode: str, normalized: str = "") -> list[str]:
    keys: list[str] = []
    bc = str(barcode or "").strip()
    nb = str(normalized or "").strip() or location_row_normalized_barcode_value(bc, "")
    compact = re.sub(r"[\s\-]+", "", bc)
    for k in (bc, nb, compact):
        if k and k not in keys:
            keys.append(k)
    return keys


def ensure_shelf_locations_schema(conn: DBConnection) -> None:
    if conn.postgres:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS shelf_locations (
                barcode_key TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    else:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS shelf_locations (
                barcode_key TEXT PRIMARY KEY,
                location TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def _load_preserved_locations(conn: DBConnection) -> dict[str, str]:
    """خريطة باركود → موقع رف (من الجدول الدائم ثم الصفوف الحالية)."""
    out: dict[str, str] = {}
    try:
        ensure_shelf_locations_schema(conn)
        for row in conn.execute("SELECT barcode_key, location FROM shelf_locations").fetchall():
            loc = clean_shelf_location(row["location"])
            key = str(row["barcode_key"] or "").strip()
            if key and loc:
                out[key] = loc
    except Exception:
        pass
    try:
        rows = conn.execute(
            """
            SELECT barcode, location, normalized_barcode
            FROM location_rows
            WHERE COALESCE(TRIM(location), '') <> ''
            """
        ).fetchall()
    except Exception:
        return out
    for row in rows:
        loc = clean_shelf_location(row["location"])
        if not loc:
            continue
        for key in _shelf_barcode_keys(str(row["barcode"] or ""), str(row["normalized_barcode"] or "")):
            if key not in out:
                out[key] = loc
    return out


def upsert_shelf_locations(conn: DBConnection, barcode_to_location: dict[str, str]) -> int:
    ensure_shelf_locations_schema(conn)
    n = 0
    for barcode, location in barcode_to_location.items():
        loc = clean_shelf_location(location)
        if not loc:
            continue
        for key in _shelf_barcode_keys(str(barcode or "")):
            if conn.postgres:
                conn.execute(
                    """
                    INSERT INTO shelf_locations (barcode_key, location, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (barcode_key) DO UPDATE SET
                        location = EXCLUDED.location,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (key, loc),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO shelf_locations (barcode_key, location, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(barcode_key) DO UPDATE SET
                        location = excluded.location,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (key, loc),
                )
            n += 1
    return n


def migrate_current_locations_into_shelf_map(conn: DBConnection) -> int:
    """يحفظ مواقع الرف الحالية في جدول دائم لا تمسحه مزامنة API."""
    mapping: dict[str, str] = {}
    try:
        rows = conn.execute(
            """
            SELECT barcode, location, normalized_barcode
            FROM location_rows
            WHERE COALESCE(TRIM(location), '') <> ''
            """
        ).fetchall()
    except Exception:
        return 0
    for row in rows:
        loc = clean_shelf_location(row["location"])
        if not loc:
            continue
        bc = str(row["barcode"] or "").strip()
        if bc:
            mapping[bc] = loc
    return upsert_shelf_locations(conn, mapping)


def lookup_shelf_location(preserved: dict[str, str], barcode: str) -> str:
    for key in _shelf_barcode_keys(barcode):
        loc = clean_shelf_location(preserved.get(key))
        if loc:
            return loc
    return ""


def merge_shelf_locations_from_excel_bytes(file_bytes: bytes) -> dict[str, Any]:
    """
    يحدّث مواقع الرف من Excel (عمود I) حسب الباركود بدون مسح كتالوج API.
    """
    wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
    mapping: dict[str, str] = {}
    try:
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            preview = list(ws.iter_rows(min_row=1, max_row=1, min_col=1, max_col=12, values_only=True))
            first_row = preview[0] if preview else ()
            first_cells = [clean_cell(v) for v in (first_row or ())]
            data_start_row = 2 if _location_row_looks_like_header(first_cells) else 1
            for row in ws.iter_rows(min_row=data_start_row, min_col=1, max_col=12, values_only=True):
                values = [clean_cell(v) for v in row]
                while len(values) < 12:
                    values.append("")
                barcode = str(values[3] or "").strip()
                unit_barcodes = str(values[10] or "").strip()
                location = clean_shelf_location(values[8])
                if not location:
                    continue
                if barcode:
                    mapping[barcode] = location
                for ub in _extract_location_unit_barcodes_from_cell(unit_barcodes):
                    mapping[ub] = location
    finally:
        wb.close()

    conn = get_db()
    try:
        ensure_location_rows_import_schema(conn)
        saved = upsert_shelf_locations(conn, mapping)
        updated = 0
        for barcode, location in mapping.items():
            nb = location_row_normalized_barcode_value(barcode, "")
            keys = _shelf_barcode_keys(barcode, nb)
            if not keys:
                continue
            placeholders = ", ".join(["?"] * len(keys))
            cur = conn.execute(
                f"""
                UPDATE location_rows
                SET location = ?
                WHERE barcode IN ({placeholders})
                   OR normalized_barcode IN ({placeholders})
                """,
                (location, *keys, *keys),
            )
            try:
                updated += int(cur.rowcount or 0)
            except Exception:
                pass
        conn.commit()
        return {
            "ok": True,
            "shelf_keys": saved,
            "mapped_barcodes": len(mapping),
            "updated_rows": updated,
            "mode": "merge_shelves",
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def _hesabate_search_data_version() -> int:
    try:
        return int(app_kv_get("hesabate_search_version", "0") or "0")
    except (TypeError, ValueError):
        return 0


def _bump_hesabate_search_data_version() -> int:
    ver = _hesabate_search_data_version() + 1
    app_kv_set("hesabate_search_version", str(ver))
    return ver


def _location_api_row_fingerprint(
    item_name: str,
    location: str,
    company: str,
    original_number: str,
    barcode: str,
    brand: str,
    notes: str,
    quantity: str,
    price: str,
    normalized_barcode: str = "",
) -> str:
    # normalized_barcode يشمل الباركود الرئيسي + كل other_codes
    return "|".join(
        (
            item_name,
            location,
            company,
            original_number,
            barcode,
            brand,
            notes,
            quantity,
            price,
            normalized_barcode,
        )
    )


def upsert_location_rows_delta_from_api(
    mapped_rows: list[dict[str, str]],
    *,
    allow_partial: bool = False,
    force_rewrite: bool = False,
    purge_missing: bool = False,
) -> dict[str, int]:
    """
    مزامنة ذكية لصفوف حساباتي فقط (source_file=حساباتي_API) عبر hesabate_product_id:
    - إدخال الجديد
    - تحديث المتغيّر (أو كل الصفوف إن force_rewrite)
    - حذف أصناف حساباتي الغائبة من الكتالوج الجديد إن purge_missing
    لا يحذف Excel/البدائل/مصادر أخرى.
    allow_partial=True: تحديث موجّه لصنف/أصناف قليلة (تجاوز حماية الكتالوج الناقص).
    """
    if not mapped_rows:
        raise RuntimeError("رفض حفظ المواقع: نتيجة API فارغة.")

    conn = get_db()
    source_key = LOCATION_API_SOURCE_FILE
    inserted = 0
    updated = 0
    unchanged = 0
    deleted = 0
    try:
        ensure_location_rows_import_schema(conn)
        ensure_shelf_locations_schema(conn)
        try:
            prev_api = conn.execute(
                "SELECT COUNT(*) AS c FROM location_rows WHERE source_file = ?",
                (source_key,),
            ).fetchone()
            prev_api_count = int(sql_row_to_dict(prev_api).get("c") or 0) if prev_api is not None else 0
        except Exception:
            prev_api_count = 0
        # حماية: لا نكتب فروقات إن السحب ناقص جداً مقارنة بمخزون حساباتي الحالي
        # (لا تنطبق على التحديث الموجّه لصنف واحد)
        if (
            not allow_partial
            and prev_api_count >= 1000
            and len(mapped_rows) < max(500, int(prev_api_count * 0.35))
        ):
            raise RuntimeError(
                f"رفض مزامنة المواقع: السحب ناقص ({len(mapped_rows)} مقابل {prev_api_count} من حساباتي)."
            )

        preserved: dict[str, str] = {}
        try:
            for row in conn.execute("SELECT barcode_key, location FROM shelf_locations").fetchall():
                loc = clean_shelf_location(row["location"])
                key = str(row["barcode_key"] or "").strip()
                if key and loc:
                    preserved[key] = loc
        except Exception:
            preserved = {}

        existing: dict[str, dict[str, Any]] = {}
        for row in conn.execute(
            """
            SELECT id, hesabate_product_id, item_name, location, company, original_number,
                   barcode, brand, notes, quantity, price, normalized_barcode
            FROM location_rows
            WHERE source_file = ?
              AND hesabate_product_id IS NOT NULL
              AND TRIM(hesabate_product_id) <> ''
            """,
            (source_key,),
        ).fetchall():
            rd = sql_row_to_dict(row)
            pid = str(rd.get("hesabate_product_id") or "").strip()
            if not pid:
                continue
            # عند تكرار قديم: أبقِ الأحدث (أكبر id)
            prev = existing.get(pid)
            if prev is None or int(rd.get("id") or 0) >= int(prev.get("id") or 0):
                existing[pid] = rd

        to_insert: list[tuple[str, ...]] = []
        to_update: list[tuple[Any, ...]] = []
        for r in mapped_rows:
            pid = str(r.get("product_id") or "").strip()
            if not pid:
                continue
            item_name = str(r.get("item_name") or "")
            # باركود للطباعة: لا strip — المسافات الداخلية تبقى كما من حساباتي
            barcode = str(r.get("barcode") or "").replace("\r", "").replace("\n", "")
            location = clean_shelf_location(r.get("location"))
            company = str(r.get("company") or "")
            original_number = str(r.get("original_number") or "")
            brand = str(r.get("brand") or "")
            notes = str(r.get("notes") or "")
            quantity = str(r.get("quantity") or "").strip()
            price = str(r.get("price") or "").strip()
            unit_barcodes = str(r.get("unit_barcodes") or "").strip()
            ex = existing.get(pid)
            # جلب صنف واحد: خذ قيم API كما هي — لا تُرجع موقعاً قديماً من shelf/Excel
            if not allow_partial:
                if not location and barcode:
                    location = lookup_shelf_location(preserved, barcode)
                if ex is not None:
                    if not location:
                        location = clean_shelf_location(ex.get("location"))
                    if not company:
                        company = str(ex.get("company") or "")
                    if not brand:
                        brand = str(ex.get("brand") or "")
                    if not original_number:
                        original_number = str(ex.get("original_number") or "")
                    if not notes:
                        notes = str(ex.get("notes") or "")
            else:
                # املأ فقط الحقول غير الموقع إن كانت فارغة (الاسم/الشركة…)
                if ex is not None:
                    if not company:
                        company = str(ex.get("company") or "")
                    if not brand:
                        brand = str(ex.get("brand") or "")
                    if not original_number:
                        original_number = str(ex.get("original_number") or "")
                    if not notes:
                        notes = str(ex.get("notes") or "")
                    if not item_name:
                        item_name = str(ex.get("item_name") or "")
                    if not barcode or not any(ch.isalnum() for ch in barcode):
                        barcode = str(ex.get("barcode") or "").replace("\r", "").replace("\n", "")
            nb = location_row_normalized_barcode_value(barcode, unit_barcodes)
            nt = location_row_normalized_text_value(item_name, company, brand, notes, original_number)
            new_fp = _location_api_row_fingerprint(
                item_name,
                location,
                company,
                original_number,
                barcode,
                brand,
                notes,
                quantity,
                price,
                nb,
            )
            if ex is None:
                to_insert.append(
                    (
                        item_name,
                        location,
                        company,
                        original_number,
                        barcode,
                        brand,
                        notes,
                        quantity,
                        price,
                        source_key,
                        "products",
                        nb,
                        nt,
                        pid,
                    )
                )
                continue
            old_nb = str(ex.get("normalized_barcode") or "")
            old_fp = _location_api_row_fingerprint(
                str(ex.get("item_name") or ""),
                clean_shelf_location(ex.get("location")),
                str(ex.get("company") or ""),
                str(ex.get("original_number") or ""),
                str(ex.get("barcode") or "").strip(),
                str(ex.get("brand") or ""),
                str(ex.get("notes") or ""),
                str(ex.get("quantity") or "").strip(),
                str(ex.get("price") or "").strip(),
                old_nb,
            )
            # جلب موجّه أو إعادة كتابة كاملة: افرض UPDATE حتى لا تبقى قيمة قديمة
            if (not allow_partial) and (not force_rewrite) and old_fp == new_fp:
                unchanged += 1
                continue
            to_update.append(
                (
                    item_name,
                    location,
                    company,
                    original_number,
                    barcode,
                    brand,
                    notes,
                    quantity,
                    price,
                    nb,
                    nt,
                    int(ex["id"]),
                )
            )

        if to_update:
            update_params = [
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    source_key,
                )
                for row in to_update
            ]
            for i in range(0, len(update_params), 800):
                conn.executemany(
                    """
                    UPDATE location_rows SET
                        item_name = ?, location = ?, company = ?, original_number = ?,
                        barcode = ?, brand = ?, notes = ?, quantity = ?, price = ?,
                        normalized_barcode = ?, normalized_text = ?, source_sheet = 'products'
                    WHERE id = ? AND source_file = ?
                    """,
                    update_params[i : i + 800],
                )
            updated = len(to_update)

        insert_sql = """
            INSERT INTO location_rows (
                item_name, location, company, original_number, barcode,
                brand, notes, quantity, price, source_file, source_sheet,
                normalized_barcode, normalized_text, hesabate_product_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for i in range(0, len(to_insert), 800):
            chunk = to_insert[i : i + 800]
            conn.executemany(insert_sql, chunk)
            inserted += len(chunk)

        # جلب موجّه: حدّث كل صفوف حساباتي_API بنفس product_id (حتى المكررة)
        if allow_partial:
            for r in mapped_rows:
                pid = str(r.get("product_id") or "").strip()
                if not pid:
                    continue
                item_name = str(r.get("item_name") or "")
                barcode = str(r.get("barcode") or "").replace("\r", "").replace("\n", "")
                location = clean_shelf_location(r.get("location"))
                company = str(r.get("company") or "")
                original_number = str(r.get("original_number") or "")
                brand = str(r.get("brand") or "")
                notes = str(r.get("notes") or "")
                quantity = str(r.get("quantity") or "").strip()
                price = str(r.get("price") or "").strip()
                unit_barcodes = str(r.get("unit_barcodes") or "").strip()
                nb = location_row_normalized_barcode_value(barcode, unit_barcodes)
                nt = location_row_normalized_text_value(item_name, company, brand, notes, original_number)
                conn.execute(
                    """
                    UPDATE location_rows SET
                        item_name = ?, location = ?, company = ?, original_number = ?,
                        barcode = ?, brand = ?, notes = ?, quantity = ?, price = ?,
                        normalized_barcode = ?, normalized_text = ?, source_sheet = 'products'
                    WHERE source_file = ?
                      AND TRIM(COALESCE(hesabate_product_id, '')) = ?
                    """,
                    (
                        item_name,
                        location,
                        company,
                        original_number,
                        barcode,
                        brand,
                        notes,
                        quantity,
                        price,
                        nb,
                        nt,
                        source_key,
                        pid,
                    ),
                )

        # بعد جلب/تحديث صنف: احذف نسخ Excel القديمة + حدّث shelf_locations
        if allow_partial:
            shelf_map: dict[str, str] = {}
            for r in mapped_rows:
                pid = str(r.get("product_id") or "").strip()
                barcode = str(r.get("barcode") or "").strip()
                unit_barcodes = str(r.get("unit_barcodes") or "").strip()
                location = clean_shelf_location(r.get("location"))
                keys: list[str] = []
                for tok in [barcode] + re.split(r"[\s,/|;]+", unit_barcodes):
                    for k in barcode_search_keys(tok):
                        if k and k not in keys:
                            keys.append(k)
                if location:
                    for k in keys:
                        shelf_map[k] = location
                if pid:
                    conn.execute(
                        """
                        DELETE FROM location_rows
                        WHERE source_file <> ?
                          AND TRIM(COALESCE(hesabate_product_id, '')) = ?
                        """,
                        (source_key, pid),
                    )
                for k in keys:
                    spaced = f"% {k} %"
                    conn.execute(
                        """
                        DELETE FROM location_rows
                        WHERE source_file <> ?
                          AND (
                            TRIM(COALESCE(barcode, '')) = ?
                            OR normalized_barcode LIKE ?
                          )
                        """,
                        (source_key, k, spaced),
                    )
            if shelf_map:
                try:
                    upsert_shelf_locations(conn, shelf_map)
                except Exception:
                    pass

        # تنظيف صفوف حساباتي القديمة بلا product_id بعد نجاح كتالوج كافٍ (مرة ترحيل)
        if not allow_partial and len(mapped_rows) >= 100:
            conn.execute(
                """
                DELETE FROM location_rows
                WHERE source_file = ?
                  AND (hesabate_product_id IS NULL OR TRIM(hesabate_product_id) = '')
                """,
                (source_key,),
            )

        # كتالوج كامل: احذف أصناف حساباتي التي لم تعد في الرد الجديد (البيانات الجديدة هي المصدر)
        if purge_missing and (not allow_partial) and len(mapped_rows) >= max(500, int(prev_api_count * 0.35) if prev_api_count else 500):
            incoming_pids = {
                str(r.get("product_id") or "").strip()
                for r in mapped_rows
                if str(r.get("product_id") or "").strip()
            }
            if incoming_pids:
                existing_pids = set(existing.keys())
                stale = [pid for pid in existing_pids if pid not in incoming_pids]
                for i in range(0, len(stale), 400):
                    chunk = stale[i : i + 400]
                    placeholders = ",".join(["?"] * len(chunk))
                    cur = conn.execute(
                        f"""
                        DELETE FROM location_rows
                        WHERE source_file = ?
                          AND TRIM(COALESCE(hesabate_product_id, '')) IN ({placeholders})
                        """,
                        (source_key, *chunk),
                    )
                    try:
                        deleted += int(cur.rowcount or 0)
                    except Exception:
                        deleted += len(chunk)

        total_api = inserted + updated + unchanged
        if total_api <= 0:
            total_api = len(mapped_rows)
        content_hash = hashlib.sha256(
            f"api-delta:{inserted}:{updated}:{unchanged}:{deleted}:{datetime.now().isoformat()}".encode("utf-8")
        ).hexdigest()
        if conn.postgres:
            conn.execute(
                """
                INSERT INTO uploaded_files (file_name, rows_count, content_hash, file_size, parser_version)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(file_name) DO UPDATE SET
                    uploaded_at = CURRENT_TIMESTAMP,
                    rows_count = EXCLUDED.rows_count,
                    content_hash = EXCLUDED.content_hash,
                    file_size = EXCLUDED.file_size,
                    parser_version = EXCLUDED.parser_version
                """,
                (source_key, total_api, content_hash, 0, LOCATION_PARSER_VERSION),
            )
        else:
            conn.execute(
                """
                INSERT INTO uploaded_files (file_name, rows_count, content_hash, file_size, parser_version)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(file_name) DO UPDATE SET
                    uploaded_at = CURRENT_TIMESTAMP,
                    rows_count = excluded.rows_count,
                    content_hash = excluded.content_hash,
                    file_size = excluded.file_size,
                    parser_version = excluded.parser_version
                """,
                (source_key, total_api, content_hash, 0, LOCATION_PARSER_VERSION),
            )
        conn.commit()
        return {
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "deleted": deleted,
            "total": inserted + updated + unchanged,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def replace_location_rows_from_api(mapped_rows: list[dict[str, str]]) -> int:
    """توافق خلفي: مزامنة دلتا بالـ product_id مع حذف الغائب."""
    stats = upsert_location_rows_delta_from_api(
        mapped_rows, force_rewrite=True, purge_missing=True
    )
    return (
        int(stats.get("inserted") or 0)
        + int(stats.get("updated") or 0)
        + int(stats.get("unchanged") or 0)
    )


def _try_acquire_hesabate_db_lock() -> tuple[Any | None, bool]:
    """قفل Postgres مشترك بين نسخ السيرفرلس — يمنع مسح الجدول من مزامنتين معاً."""
    if not USE_POSTGRES:
        return None, True
    lock_conn = None
    try:
        lock_conn = get_db()
        row = lock_conn.execute(
            "SELECT pg_try_advisory_lock(?) AS ok",
            (_HESABATE_SYNC_ADVISORY_LOCK_KEY,),
        ).fetchone()
        ok = bool(sql_row_to_dict(row).get("ok")) if row is not None else False
        if not ok:
            try:
                lock_conn.close()
            except Exception:
                pass
            return None, False
        # إبقاؤ اتصال القفل مفتوحاً طوال المزامنة
        return lock_conn, True
    except Exception:
        if lock_conn is not None:
            try:
                lock_conn.close()
            except Exception:
                pass
        return None, True


def _release_hesabate_db_lock(lock_conn: Any | None) -> None:
    if lock_conn is None:
        return
    try:
        lock_conn.execute(
            "SELECT pg_advisory_unlock(?)",
            (_HESABATE_SYNC_ADVISORY_LOCK_KEY,),
        )
        lock_conn.commit()
    except Exception:
        try:
            lock_conn.rollback()
        except Exception:
            pass
    try:
        lock_conn.close()
    except Exception:
        pass


def sync_locations_from_hesabate(*, force: bool = False, return_mapped: bool = False) -> dict:
    """
    مزامنة موحّدة: تنزيل كتالوج حساباتي كاملاً (لا يدعم API فرقاً/webhook موثّقاً)،
    ثم حفظ الفروقات فقط في location_rows + product_cache معاً عبر قفل DB مشترك.
    """
    global _hesabate_sync_inflight
    search_ver = _hesabate_search_data_version()
    if not hesabate_configured():
        return {
            "ok": False,
            "skipped": True,
            "reason": "not_configured",
            "message": "أضف HESABATE_API_HOST و HESABATE_API_USERNAME و HESABATE_API_PASSWORD في إعدادات Vercel.",
            "search_version": search_ver,
            "usage": hesabate_usage_summary(),
        }
    if (not force) and (not return_mapped) and _hesabate_last_sync_age_sec() < HESABATE_SYNC_MIN_INTERVAL_SEC:
        return {
            "ok": True,
            "skipped": True,
            "reason": "fresh",
            "message": "البيانات حديثة — لم يُرسل طلب جديد لتوفير الـ Requests.",
            "age_sec": int(_hesabate_last_sync_age_sec()),
            "search_version": search_ver,
            "last_sync_at": app_kv_get("hesabate_last_sync_at", ""),
            "usage": hesabate_usage_summary(),
        }
    if not _hesabate_sync_lock.acquire(blocking=False):
        return {
            "ok": True,
            "skipped": True,
            "reason": "inflight",
            "message": "مزامنة أخرى جارية.",
            "search_version": search_ver,
            "usage": hesabate_usage_summary(),
        }
    _hesabate_sync_inflight = True
    db_lock_conn: Any | None = None
    t_all = time.perf_counter()
    try:
        db_lock_conn, got_db_lock = _try_acquire_hesabate_db_lock()
        if not got_db_lock:
            return {
                "ok": True,
                "skipped": True,
                "reason": "inflight",
                "message": "مزامنة أخرى جارية على الخادم.",
                "search_version": search_ver,
                "usage": hesabate_usage_summary(),
            }
        preferred_scheme = app_kv_get("hesabate_preferred_scheme", "")
        token_store = app_kv_get("hesabate_token", "")
        try:
            token_exp_mono = float(app_kv_get("hesabate_token_exp_mono", "0") or "0")
        except (TypeError, ValueError):
            token_exp_mono = 0.0
        try:
            token_exp_unix = float(app_kv_get("hesabate_token_exp_unix", "0") or "0")
        except (TypeError, ValueError):
            token_exp_unix = 0.0
        lookup_raw = app_kv_get("hesabate_lookup_json", "")
        lookup_company = None
        lookup_trades = None
        lookup_model = None
        need_lookup = True
        if lookup_raw:
            try:
                lookup = json.loads(lookup_raw)
                cached_at = float(lookup.get("cached_at") or 0)
                if cached_at > 0 and (time.time() - cached_at) < (HESABATE_LOOKUP_CACHE_DAYS * 86400):
                    lookup_company = lookup.get("company") if isinstance(lookup.get("company"), dict) else None
                    lookup_trades = lookup.get("trades") if isinstance(lookup.get("trades"), dict) else None
                    lookup_model = lookup.get("model") if isinstance(lookup.get("model"), dict) else None
                    need_lookup = not (lookup_company and lookup_trades)
            except Exception:
                need_lookup = True

        t_api0 = time.perf_counter()
        # أبطل كاش الذاكرة القديم قبل تنزيل الكتالوج حتى لا تُعاد بيانات سابقة أثناء/بعد المزامنة
        hesabate_api.invalidate_products_mem_cache(HESABATE_API_HOST)
        fetched = hesabate_api.fetch_location_catalog(
            HESABATE_API_HOST,
            HESABATE_API_USERNAME,
            HESABATE_API_PASSWORD,
            verify_ssl=HESABATE_API_VERIFY_SSL,
            record_request=record_hesabate_api_request,
            preferred_scheme=preferred_scheme,
            token_from_store=token_store,
            token_exp_mono=token_exp_mono,
            token_exp_unix=token_exp_unix,
            lookup_company=lookup_company,
            lookup_trades=lookup_trades,
            lookup_model=lookup_model,
            need_lookup_refresh=need_lookup,
            need_model_refresh=True,
            products_mode="all",
            bypass_mem_cache=True,
        )
        api_ms = int((time.perf_counter() - t_api0) * 1000)
        products = fetched["products"]
        model_payload = fetched.get("model") or lookup_model or {}
        company_payload = fetched.get("company") or lookup_company or {}
        trades_payload = fetched.get("trades") or lookup_trades or {}
        hasher = hashlib.sha256()
        hasher.update(b"loc-products-v5\n")
        prod_n = 0
        for prow in products.get("table") or []:
            if not isinstance(prow, dict):
                continue
            prod_n += 1
            hasher.update(
                f"{prow.get('id')}|{prow.get('amount')}|{prow.get('barcode')}|{prow.get('price')}|{prow.get('name')}|{prow.get('model_no')}|{prow.get('note')}|{prow.get('company_id')}|{prow.get('trade_id')}|{prow.get('model_id')}|{prow.get('other_codes')}\n".encode(
                    "utf-8",
                    "replace",
                )
            )
        for label, payload in (("m", model_payload), ("c", company_payload), ("t", trades_payload)):
            for mrow in (payload.get("table") if isinstance(payload, dict) else None) or []:
                if not isinstance(mrow, dict):
                    continue
                hasher.update(
                    f"{label}|{mrow.get('id')}|{mrow.get('name')}\n".encode("utf-8", "replace")
                )
        products_hash = hasher.hexdigest()
        prev_hash = app_kv_get("hesabate_products_hash", "")
        request_count = int(fetched.get("request_count") or 0)

        app_kv_set("hesabate_token", str(fetched.get("token") or ""))
        app_kv_set("hesabate_token_exp_mono", str(fetched.get("token_exp_mono") or 0))
        app_kv_set("hesabate_token_exp_unix", str(fetched.get("token_exp_unix") or 0))
        if fetched.get("scheme"):
            app_kv_set("hesabate_preferred_scheme", str(fetched["scheme"]))

        try:
            prev_rows_n = int(app_kv_get("hesabate_last_rows", "0") or "0")
        except (TypeError, ValueError):
            prev_rows_n = 0
        catalog_bloated = prev_rows_n > max(int(prod_n * 1.35), prod_n + 500)
        barcode_parser_stale = (
            str(app_kv_get("hesabate_barcode_parser_ver", "") or "") != HESABATE_BARCODE_PARSER_VERSION
        )
        # بدون force: إن تطابق هاش الكتالوج نكتفي بالتحقق (توفير كتابة DB).
        # مع force (كرون/زر): دائماً إعادة كتابة حساباتي_API لتبقى المصدر الرئيسي المعروض.
        products_unchanged = bool(
            (not force)
            and (not barcode_parser_stale)
            and products_hash
            and products_hash == prev_hash
            and not catalog_bloated
        )

        t_cache0 = time.perf_counter()
        cache_delta = 0
        try:
            if barcode_parser_stale:
                # إجبار إعادة كتابة كاش/فهرس الباركود حتى لو لم تتغير حقول الصنف
                app_kv_set("hesabate_product_cache_fp", "")
            cache_delta = int(replace_hesabate_product_cache(products) or 0)
            if barcode_parser_stale:
                cache_delta = max(cache_delta, rebuild_hesabate_barcode_index_from_cache())
        except Exception as cache_exc:
            logger.warning("hesabate_product_cache_update_failed err=%s", type(cache_exc).__name__)
        cache_ms = int((time.perf_counter() - t_cache0) * 1000)

        if products_unchanged and model_payload:
            record_hesabate_sync_batch(request_count)
            sync_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            app_kv_set("hesabate_last_sync_at", sync_at)
            app_kv_set("hesabate_last_error", "")
            prev_lookup_cached_at = time.time()
            if lookup_raw and not fetched.get("lookup_refreshed"):
                try:
                    prev_lookup_cached_at = float(json.loads(lookup_raw).get("cached_at") or time.time())
                except Exception:
                    prev_lookup_cached_at = time.time()
            app_kv_set(
                "hesabate_lookup_json",
                json.dumps(
                    {
                        "cached_at": time.time() if fetched.get("lookup_refreshed") else prev_lookup_cached_at,
                        "company": fetched.get("company") or lookup_company or {},
                        "trades": fetched.get("trades") or lookup_trades or {},
                        "model": model_payload,
                    },
                    ensure_ascii=False,
                ),
            )
            total_ms = int((time.perf_counter() - t_all) * 1000)
            logger.info(
                "hesabate_sync unchanged api_ms=%s cache_ms=%s total_ms=%s products=%s cache_delta=%s requests=%s",
                api_ms,
                cache_ms,
                total_ms,
                prod_n,
                cache_delta,
                request_count,
            )
            out = {
                "ok": True,
                "skipped": True,
                "reason": "unchanged",
                "message": (
                    "لا تغيير في أصناف حساباتي — تم التحقق من الكتالوج الكامل دون إعادة كتابة البحث. "
                    "(API لا يدعم جلب التغييرات فقط.)"
                ),
                "request_count": request_count,
                "timing_ms": {"api": api_ms, "cache": cache_ms, "total": total_ms},
                "stats": {"inserted": 0, "updated": 0, "unchanged": prod_n, "cache_delta": cache_delta},
                "search_version": search_ver,
                "last_sync_at": sync_at,
                "catalog_mode": "full_download_compare",
                "usage": hesabate_usage_summary(),
            }
            if return_mapped:
                out["mapped"] = hesabate_api.map_products_to_location_rows(
                    products,
                    company_payload=fetched.get("company"),
                    trades_payload=fetched.get("trades"),
                    model_payload=model_payload,
                )
            return out

        if not fetched.get("model_refreshed"):
            try:
                token_now = str(fetched.get("token") or token_store or "")
                scheme_now = str(fetched.get("scheme") or preferred_scheme or "")

                def _reauth_model() -> str:
                    hesabate_api.invalidate_token(HESABATE_API_HOST, HESABATE_API_USERNAME)
                    tok, _sch, _ = hesabate_api.store_api_auth_cached(
                        HESABATE_API_HOST,
                        HESABATE_API_USERNAME,
                        HESABATE_API_PASSWORD,
                        verify_ssl=HESABATE_API_VERIFY_SSL,
                        force=True,
                        record_request=record_hesabate_api_request,
                        preferred_scheme=scheme_now,
                    )
                    return tok

                model_payload, scheme_now = hesabate_api.store_api_download(
                    HESABATE_API_HOST,
                    token_now,
                    "model",
                    verify_ssl=HESABATE_API_VERIFY_SSL,
                    reauth=_reauth_model,
                    record_request=record_hesabate_api_request,
                    preferred_scheme=scheme_now,
                )
                request_count += 1
                fetched["scheme"] = scheme_now
                fetched["model_refreshed"] = True
            except Exception:
                model_payload = fetched.get("model") or lookup_model or {}

        record_hesabate_sync_batch(request_count)
        if fetched.get("scheme"):
            app_kv_set("hesabate_preferred_scheme", str(fetched["scheme"]))
        prev_lookup_cached_at = time.time()
        if lookup_raw and not fetched.get("lookup_refreshed"):
            try:
                prev_lookup_cached_at = float(json.loads(lookup_raw).get("cached_at") or time.time())
            except Exception:
                prev_lookup_cached_at = time.time()
        app_kv_set(
            "hesabate_lookup_json",
            json.dumps(
                {
                    "cached_at": time.time() if fetched.get("lookup_refreshed") else prev_lookup_cached_at,
                    "company": fetched.get("company") or lookup_company or {},
                    "trades": fetched.get("trades") or lookup_trades or {},
                    "model": model_payload,
                },
                ensure_ascii=False,
            ),
        )

        t_map0 = time.perf_counter()
        mapped = hesabate_api.map_products_to_location_rows(
            products,
            company_payload=fetched.get("company"),
            trades_payload=fetched.get("trades"),
            model_payload=model_payload,
        )
        map_ms = int((time.perf_counter() - t_map0) * 1000)

        t_save0 = time.perf_counter()
        # احفظ القيم الجديدة من حساباتي واحذف أصناف حساباتي الغائبة عن الكتالوج
        stats = upsert_location_rows_delta_from_api(
            mapped,
            force_rewrite=bool(force or barcode_parser_stale),
            purge_missing=True,
        )
        save_ms = int((time.perf_counter() - t_save0) * 1000)

        # احفظ مؤشر الكتالوج فقط بعد نجاح الحفظ
        app_kv_set("hesabate_products_hash", products_hash)
        app_kv_set("hesabate_barcode_parser_ver", HESABATE_BARCODE_PARSER_VERSION)
        sync_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        app_kv_set("hesabate_last_sync_at", sync_at)
        app_kv_set("hesabate_last_rows", str(stats.get("total") or len(mapped)))
        app_kv_set("hesabate_last_error", "")
        with_loc = sum(1 for r in mapped if clean_shelf_location(r.get("location")))
        changed = (
            int(stats.get("inserted") or 0)
            + int(stats.get("updated") or 0)
            + int(stats.get("deleted") or 0)
        )
        if changed or force or barcode_parser_stale:
            search_ver = _bump_hesabate_search_data_version()
        total_ms = int((time.perf_counter() - t_all) * 1000)
        logger.info(
            "hesabate_sync ok api_ms=%s map_ms=%s save_ms=%s cache_ms=%s total_ms=%s "
            "inserted=%s updated=%s unchanged=%s deleted=%s cache_delta=%s products=%s requests=%s",
            api_ms,
            map_ms,
            save_ms,
            cache_ms,
            total_ms,
            stats.get("inserted"),
            stats.get("updated"),
            stats.get("unchanged"),
            stats.get("deleted"),
            cache_delta,
            prod_n,
            request_count,
        )
        out = {
            "ok": True,
            "skipped": False,
            "rows": int(stats.get("total") or 0),
            "rows_with_location": with_loc,
            "request_count": request_count,
            "auth_requested": bool(fetched.get("auth_requested")),
            "lookup_refreshed": bool(fetched.get("lookup_refreshed")),
            "model_refreshed": bool(fetched.get("model_refreshed")),
            "stats": {**stats, "cache_delta": cache_delta},
            "timing_ms": {
                "api": api_ms,
                "map": map_ms,
                "save": save_ms,
                "cache": cache_ms,
                "total": total_ms,
            },
            "search_version": search_ver,
            "last_sync_at": sync_at,
            "catalog_mode": "full_download_replace",
            "message": (
                f"تم حفظ بيانات حساباتي الجديدة: +{stats.get('inserted', 0)}، "
                f"تحديث {stats.get('updated', 0)}، حذف قديم {stats.get('deleted', 0)}، "
                f"دون تغيير {stats.get('unchanged', 0)} (من {prod_n} صنف)."
            ),
            "usage": hesabate_usage_summary(),
            "location_note": "موقع الرف = حقل الموديل (model) من حساباتي.",
        }
        if return_mapped:
            out["mapped"] = mapped
        return out
    except hesabate_api.StoreApiError as e:
        try:
            flush_hesabate_usage_pending()
        except Exception:
            pass
        app_kv_set("hesabate_last_error", str(e)[:500])
        logger.warning("hesabate_sync api_error err=%s", type(e).__name__)
        return {
            "ok": False,
            "skipped": False,
            "reason": "api_error",
            "message": str(e),
            "search_version": _hesabate_search_data_version(),
            "last_sync_at": app_kv_get("hesabate_last_sync_at", ""),
            "last_error": str(e)[:500],
            "usage": hesabate_usage_summary(),
        }
    except Exception as e:
        try:
            flush_hesabate_usage_pending()
        except Exception:
            pass
        err = f"{type(e).__name__}: {e}"
        app_kv_set("hesabate_last_error", err[:500])
        logger.warning("hesabate_sync error err=%s", type(e).__name__)
        return {
            "ok": False,
            "skipped": False,
            "reason": "error",
            "message": err,
            "search_version": _hesabate_search_data_version(),
            "last_sync_at": app_kv_get("hesabate_last_sync_at", ""),
            "last_error": err[:500],
            "usage": hesabate_usage_summary(),
        }
    finally:
        _release_hesabate_db_lock(db_lock_conn)
        _hesabate_sync_inflight = False
        try:
            _hesabate_sync_lock.release()
        except Exception:
            pass


def palestine_now() -> datetime:
    """الوقت الحالي بتوقيت فلسطين (Asia/Jerusalem، مع DST)."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(HESABATE_AUTO_SYNC_TZ))
        except Exception:
            pass
    # احتياطي: UTC+2 شتاءً تقريباً
    return datetime.now(timezone.utc) + timedelta(hours=2)


def hesabate_auto_sync_allowed_now() -> tuple[bool, dict[str, Any]]:
    """
    المزامنة التلقائية متاحة من 08:00 حتى 20:59 بتوقيت فلسطين (كل 4 ساعات).
    من 9 مساءً إلى 8 صباحاً: صمت (لا fetch تلقائي).
    """
    now = palestine_now()
    hour = int(now.hour)
    quiet = hour >= HESABATE_AUTO_SYNC_QUIET_START_HOUR or hour < HESABATE_AUTO_SYNC_QUIET_END_HOUR
    meta = {
        "tz": HESABATE_AUTO_SYNC_TZ,
        "local_time": now.strftime("%Y-%m-%d %H:%M:%S%z"),
        "quiet_hours": f"{HESABATE_AUTO_SYNC_QUIET_START_HOUR:02d}:00-{HESABATE_AUTO_SYNC_QUIET_END_HOUR:02d}:00",
        "in_quiet_hours": quiet,
    }
    return (not quiet), meta


def ensure_hesabate_locations_synced(*, force: bool = False) -> dict:
    return sync_locations_from_hesabate(force=force)


def looks_like_item_code(raw: str) -> bool:
    """باركود أو رقم أصلي / كود صنف — وليس اسم عربي طويل فقط."""
    s = str(raw or "").strip()
    if len(s) < 3:
        return False
    return any(ch.isdigit() for ch in s)


def dedupe_location_rows_table() -> int:
    """يحذف الصفوف المكررة (نفس الباركود أو نفس الرقم الأصلي+الاسم) ويبقي الأحدث."""
    conn = get_db()
    removed = 0
    try:
        ensure_location_rows_import_schema(conn)
        if conn.postgres:
            cur = conn.execute(
                """
                DELETE FROM location_rows a
                USING location_rows b
                WHERE a.id < b.id
                  AND COALESCE(NULLIF(TRIM(a.barcode), ''), '') <> ''
                  AND TRIM(a.barcode) = TRIM(b.barcode)
                """
            )
            try:
                removed += int(cur.rowcount or 0)
            except Exception:
                pass
            cur = conn.execute(
                """
                DELETE FROM location_rows a
                USING location_rows b
                WHERE a.id < b.id
                  AND COALESCE(NULLIF(TRIM(a.barcode), ''), '') = ''
                  AND COALESCE(TRIM(a.original_number), '') = COALESCE(TRIM(b.original_number), '')
                  AND COALESCE(TRIM(a.item_name), '') = COALESCE(TRIM(b.item_name), '')
                  AND COALESCE(TRIM(a.original_number), '') <> ''
                """
            )
            try:
                removed += int(cur.rowcount or 0)
            except Exception:
                pass
        else:
            cur = conn.execute(
                """
                DELETE FROM location_rows
                WHERE id NOT IN (
                    SELECT MAX(id) FROM location_rows
                    WHERE COALESCE(TRIM(barcode), '') <> ''
                    GROUP BY TRIM(barcode)
                )
                AND COALESCE(TRIM(barcode), '') <> ''
                """
            )
            try:
                removed += int(cur.rowcount or 0)
            except Exception:
                pass
        conn.commit()
        return removed
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return removed
    finally:
        conn.close()


def upsert_matched_location_rows(mapped_rows: list[dict[str, str]]) -> int:
    """يدرج/يحدّث أصناف مطابقة بالـ product_id فقط — بدون مسح كتالوج أو Excel."""
    if not mapped_rows:
        return 0
    # إن وُجدت معرفات ثابتة استخدم مسار الدلتا الموحّد (جزئي: صنف/قائمة محدودة)
    if any(str(r.get("product_id") or "").strip() for r in mapped_rows):
        stats = upsert_location_rows_delta_from_api(mapped_rows, allow_partial=True)
        changed = int(stats.get("inserted") or 0) + int(stats.get("updated") or 0)
        if changed:
            _bump_hesabate_search_data_version()
        return int(stats.get("total") or 0)
    return 0


def _prepare_live_rows_from_mapped(mapped: list[dict[str, str]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for index, row in enumerate(mapped):
        pid = str(row.get("product_id") or "").strip()
        d = {
            "id": pid or index,
            "hesabate_product_id": pid,
            "product_id": pid,
            "item_name": row.get("item_name") or "",
            "location": clean_shelf_location(row.get("location")),
            "company": row.get("company") or "",
            "original_number": row.get("original_number") or "",
            "barcode": row.get("barcode") or "",
            "brand": row.get("brand") or "",
            "notes": row.get("notes") or "",
            "quantity": row.get("quantity") or "",
            "price": row.get("price") or "",
            "source_file": LOCATION_API_SOURCE_FILE,
            "source_sheet": "products",
        }
        d["row_key"] = f"api|{pid}|{d.get('barcode')}"
        prepared.append({str(k): _json_safe_value(v) for k, v in d.items()})
    return prepared


def resolve_local_hesabate_product_id(
    *,
    barcode_keys: list[str] | None = None,
    text_query: str = "",
) -> str:
    """استخرج المعرّف الداخلي المحفوظ محلياً — لا تستخدم الباركود كـ pro_id."""
    keys = list(barcode_keys or [])
    conn = get_db()
    try:
        ensure_hesabate_product_cache_schema(conn)
        ensure_location_rows_import_schema(conn)
        # 1) فهرس الباركود المشترك (أسرع مصدر لـ product_id)
        if keys:
            placeholders = ", ".join(["?"] * len(keys))
            row = conn.execute(
                f"""
                SELECT product_id FROM hesabate_barcode_index
                WHERE barcode_key IN ({placeholders})
                LIMIT 1
                """,
                tuple(keys[:16]),
            ).fetchone()
            if row:
                pid = str(sql_row_to_dict(row).get("product_id") or "").strip()
                if _is_trusted_hesabate_product_id(pid):
                    return pid
        # 2) كاش المنتجات
        try:
            cached_raw = lookup_hesabate_product_cache(
                barcode_keys=keys or None,
                text_query="" if keys else text_query,
                limit=5,
            )
            for prod in cached_raw or []:
                pid = str(prod.get("id") or "").strip()
                if _is_trusted_hesabate_product_id(pid):
                    return pid
        except Exception:
            pass
        # 3) صفوف البحث المحلية (كل المصادر — ليس حساباتي_API فقط)
        if keys:
            for k in keys[:8]:
                row = conn.execute(
                    """
                    SELECT hesabate_product_id FROM location_rows
                    WHERE hesabate_product_id IS NOT NULL
                      AND TRIM(hesabate_product_id) <> ''
                      AND (
                        barcode = ?
                        OR barcode LIKE ?
                        OR normalized_barcode LIKE ?
                        OR hesabate_product_id = ?
                      )
                    ORDER BY
                      CASE WHEN source_file = ? THEN 0 ELSE 1 END,
                      id DESC
                    LIMIT 1
                    """,
                    (k, f"{k}%", f"% {k} %", k, LOCATION_API_SOURCE_FILE),
                ).fetchone()
                if row:
                    pid = str(sql_row_to_dict(row).get("hesabate_product_id") or "").strip()
                    if _is_trusted_hesabate_product_id(pid):
                        # تحقق: الباركود المدمج يطابق أحد المفاتيح (لا تأخذ لاحقة مشتركة بالخطأ)
                        try:
                            brow = conn.execute(
                                """
                                SELECT barcode, normalized_barcode FROM location_rows
                                WHERE hesabate_product_id = ?
                                ORDER BY CASE WHEN source_file = ? THEN 0 ELSE 1 END, id DESC
                                LIMIT 1
                                """,
                                (pid, LOCATION_API_SOURCE_FILE),
                            ).fetchone()
                            if brow:
                                bd = sql_row_to_dict(brow)
                                if not _location_row_matches_barcode_keys_exact(
                                    bd, set(keys), exact=False
                                ):
                                    continue
                        except Exception:
                            pass
                        return pid
        qt = str(text_query or "").strip()
        if qt:
            row = conn.execute(
                """
                SELECT hesabate_product_id FROM location_rows
                WHERE hesabate_product_id IS NOT NULL
                  AND TRIM(hesabate_product_id) <> ''
                  AND (
                    original_number = ?
                    OR item_name = ?
                    OR barcode = ?
                  )
                ORDER BY
                  CASE WHEN source_file = ? THEN 0 ELSE 1 END,
                  id DESC
                LIMIT 1
                """,
                (qt, qt, qt, LOCATION_API_SOURCE_FILE),
            ).fetchone()
            if row:
                pid = str(sql_row_to_dict(row).get("hesabate_product_id") or "").strip()
                if _is_trusted_hesabate_product_id(pid):
                    return pid
            # رقم OE / كود داخل الملاحظات أو النص المطبّع
            esc = f"%{_like_pattern_escape_sql(qt.lower())}%"
            row = conn.execute(
                """
                SELECT hesabate_product_id FROM location_rows
                WHERE hesabate_product_id IS NOT NULL
                  AND TRIM(hesabate_product_id) <> ''
                  AND (
                    lower(COALESCE(notes,'')) LIKE ? ESCAPE '\\'
                    OR lower(COALESCE(original_number,'')) LIKE ? ESCAPE '\\'
                    OR lower(COALESCE(item_name,'')) LIKE ? ESCAPE '\\'
                    OR lower(COALESCE(normalized_text,'')) LIKE ? ESCAPE '\\'
                  )
                ORDER BY
                  CASE WHEN source_file = ? THEN 0 ELSE 1 END,
                  id DESC
                LIMIT 1
                """,
                (esc, esc, esc, esc, LOCATION_API_SOURCE_FILE),
            ).fetchone()
            if row:
                pid = str(sql_row_to_dict(row).get("hesabate_product_id") or "").strip()
                if _is_trusted_hesabate_product_id(pid):
                    return pid
    except Exception:
        pass
    finally:
        conn.close()
    return ""



def _is_trusted_hesabate_product_id(pid: str) -> bool:
    """معرّف محفوظ في DB/كاش — أرقام فقط، دون رفضه لأنه يطابق نص البحث."""
    s = str(pid or "").strip()
    return bool(s) and s.isdigit() and len(s) <= 12


def backfill_hesabate_product_ids_for_rows(rows: list[dict[str, Any]]) -> list[str]:
    """
    اربط صفوفاً محلية بلا pro_id مع كاش حساباتي (باركود/أصلي/اسم/ملاحظات)
    وحدّث hesabate_product_id عند النجاح.
    """
    if not rows:
        return []
    found: list[str] = []
    seen: set[str] = set()
    conn = get_db()
    try:
        ensure_hesabate_product_cache_schema(conn)
        ensure_location_rows_import_schema(conn)
        for r in rows:
            existing = str(r.get("hesabate_product_id") or "").strip()
            if _is_trusted_hesabate_product_id(existing):
                if existing not in seen:
                    seen.add(existing)
                    found.append(existing)
                continue
            pid = ""
            bc = str(r.get("barcode") or "").strip()
            on = str(r.get("original_number") or "").strip()
            name = str(r.get("item_name") or "").strip()
            notes = str(r.get("notes") or "").strip()
            bkeys = barcode_search_keys(bc) if bc else []
            if bkeys:
                placeholders = ", ".join(["?"] * len(bkeys[:16]))
                row = conn.execute(
                    f"""
                    SELECT product_id FROM hesabate_barcode_index
                    WHERE barcode_key IN ({placeholders})
                    LIMIT 1
                    """,
                    tuple(bkeys[:16]),
                ).fetchone()
                if row:
                    pid = str(sql_row_to_dict(row).get("product_id") or "").strip()
            if not pid:
                cached = lookup_hesabate_product_cache(
                    barcode_keys=bkeys or None,
                    text_query=on or name or notes or "",
                    limit=5,
                )
                for prod in cached or []:
                    cand = str(prod.get("id") or "").strip()
                    if not _is_trusted_hesabate_product_id(cand):
                        continue
                    # فضّل تطابق باركود/أصلي دقيق
                    pbc = str(prod.get("barcode") or "").strip()
                    pmodel = str(prod.get("model_no") or "").strip()
                    if bc and pbc and normalize_barcode_lookup_key(bc) == normalize_barcode_lookup_key(pbc):
                        pid = cand
                        break
                    if on and (on == pmodel or on in str(prod.get("note") or "") or on in str(prod.get("other_codes") or "")):
                        pid = cand
                        break
                    if not pid:
                        pid = cand
            if not _is_trusted_hesabate_product_id(pid):
                continue
            rid = r.get("id")
            try:
                if rid is not None and str(rid).isdigit():
                    conn.execute(
                        """
                        UPDATE location_rows
                        SET hesabate_product_id = ?
                        WHERE id = ?
                          AND (hesabate_product_id IS NULL OR TRIM(hesabate_product_id) = '')
                        """,
                        (pid, int(rid)),
                    )
                elif bc:
                    conn.execute(
                        """
                        UPDATE location_rows
                        SET hesabate_product_id = ?
                        WHERE barcode = ?
                          AND (hesabate_product_id IS NULL OR TRIM(hesabate_product_id) = '')
                        """,
                        (pid, bc),
                    )
            except Exception:
                pass
            r["hesabate_product_id"] = pid
            if pid not in seen:
                seen.add(pid)
                found.append(pid)
        try:
            conn.commit()
        except Exception:
            pass
    except Exception:
        pass
    finally:
        conn.close()
    return found


def resolve_local_hesabate_product_ids(
    *,
    barcode_keys: list[str] | None = None,
    text_query: str = "",
    limit: int = 5,
) -> list[str]:
    """قائمة pro_id محلية مطابقة للبحث — بدون تخمين أو استخدام الباركود كـ pro_id."""
    keys = list(barcode_keys or [])
    qt = str(text_query or "").strip()
    out: list[str] = []
    seen: set[str] = set()
    lim = max(1, min(int(limit), 10))

    def _add(pid: str, *, trusted: bool = False) -> None:
        pid = str(pid or "").strip()
        if not pid or pid in seen:
            return
        # معرّفات DB/الكاش موثوقة — لا تُرفض لأنها تطابق رقم البحث
        if trusted:
            if not _is_trusted_hesabate_product_id(pid):
                return
        elif not hesabate_api.looks_like_hesabate_product_id(pid, not_barcode_keys=keys):
            return
        seen.add(pid)
        out.append(pid)

    # 1) باركود / نص مباشر
    if keys:
        single = resolve_local_hesabate_product_id(barcode_keys=keys, text_query="")
        if single:
            _add(single, trusted=True)
    if qt and len(out) < lim:
        single_t = resolve_local_hesabate_product_id(barcode_keys=None, text_query=qt)
        if single_t:
            _add(single_t, trusted=True)

    # 2) صفوف البحث المحلية (قد يكون pro_id فارغاً في صف Excel)
    rows = _lookup_location_rows_for_live(
        barcode_keys=keys or None, text_query="" if keys else qt, limit=max(lim, 10)
    )
    if not rows and keys and qt:
        rows = _lookup_location_rows_for_live(barcode_keys=None, text_query=qt, limit=max(lim, 10))
    row_barcodes: list[str] = []
    for r in rows:
        _add(str(r.get("hesabate_product_id") or ""), trusted=True)
        bc = str(r.get("barcode") or "").strip()
        if bc:
            row_barcodes.extend(barcode_search_keys(bc)[:6])
        on = str(r.get("original_number") or "").strip()
        if on and on not in row_barcodes:
            row_barcodes.append(on)

    # 3) كاش المنتجات بالاستعلام + باركودات الصفوف المحلية (لربط Excel ↔ حساباتي)
    cache_keys = list(dict.fromkeys((keys or []) + row_barcodes))[:24]
    if len(out) < lim and (cache_keys or qt):
        try:
            cached = lookup_hesabate_product_cache(
                barcode_keys=cache_keys or None,
                text_query="" if cache_keys else qt,
                limit=lim,
            )
            if not cached and qt:
                cached = lookup_hesabate_product_cache(barcode_keys=None, text_query=qt, limit=lim)
            for prod in cached or []:
                _add(str(prod.get("id") or ""), trusted=True)
                if len(out) >= lim:
                    break
        except Exception:
            pass

    # 4) اربط الصفوف المحلية الفارغة بالكاش ثم أعد المحاولة
    if len(out) < lim and rows:
        for pid in backfill_hesabate_product_ids_for_rows(rows):
            _add(pid, trusted=True)
            if len(out) >= lim:
                break

    return out[:lim]


def _lookup_location_rows_for_live(
    *,
    barcode_keys: list[str] | None,
    text_query: str = "",
    limit: int = 10,
    barcode_exact: bool = False,
) -> list[dict[str, Any]]:
    """بحث فوري من جدول البحث المحلي (location_rows) بدون API."""
    conn = get_db()
    try:
        ensure_location_normalized_schema(conn)
        keys = list(barcode_keys or [])
        if keys:
            rows, _ = _search_locations_sql_only(
                conn,
                mode="barcode",
                canonical_barcode=keys[0],
                barcode_keys=keys,
                norm_tokens=[],
                limit=limit,
                offset=0,
                barcode_exact=barcode_exact,
            )
            if not rows:
                rows, _ = _search_locations_barcode_spaced_compact_fallback(
                    conn, set(keys), limit, 0, barcode_exact=barcode_exact
                )
        elif text_query.strip() and not barcode_exact:
            tokens = [t for t in normalize_text(text_query).split() if t][:8]
            rows, _ = _search_locations_sql_only(
                conn,
                mode="text",
                canonical_barcode="",
                barcode_keys=[],
                norm_tokens=tokens,
                limit=limit,
                offset=0,
            )
        else:
            return []
        out: list[dict[str, Any]] = []
        for row in rows:
            rd = sql_row_to_dict(row)
            out.append(
                {
                    "id": rd.get("id"),
                    "item_name": rd.get("item_name") or "",
                    "location": clean_shelf_location(rd.get("location")),
                    "company": rd.get("company") or "",
                    "original_number": rd.get("original_number") or "",
                    "barcode": rd.get("barcode") or "",
                    "brand": rd.get("brand") or "",
                    "notes": rd.get("notes") or "",
                    "quantity": rd.get("quantity") or "",
                    "price": rd.get("price") or "",
                    "source_file": rd.get("source_file") or "",
                    "source_sheet": rd.get("source_sheet") or "",
                    "hesabate_product_id": rd.get("hesabate_product_id") or "",
                    "row_key": f"local|{rd.get('source_file')}|{rd.get('id')}",
                }
            )
        return [{str(k): _json_safe_value(v) for k, v in d.items()} for d in out]
    except Exception:
        return []
    finally:
        conn.close()


def live_lookup_locations_from_hesabate(
    *,
    q_barcode: str = "",
    q_text: str = "",
    barcode_keys: list[str] | None = None,
    cache_only: bool = False,
    refresh: bool = False,
    discover_new: bool = False,
    barcode_exact: bool = False,
) -> dict:
    """
    جلب/تحديث موجّه من حساباتي:
    - بدون refresh: محلي ثم كاش DB
    - مع refresh: يتجاوز الكاش ويجلب عبر pro_id+all=1
    - discover_new=True (زر جلب الصنف فقط): إن غاب pro_id يستورد الكتالوج ثم يعيد البحث
    - البحث التلقائي يبقي discover_new=False (لا كتالوج)
    """
    t_total = time.perf_counter()
    timing: dict[str, Any] = {
        "db_ms": 0,
        "auth_ms": 0,
        "products_ms": 0,
        "save_ms": 0,
        "map_ms": 0,
        "model_ms": 0,
        "catalog_import_ms": 0,
        "total_ms": 0,
    }
    api_cap = {
        "pro_id_all1": True,
        "barcode_filter": False,
        "name_filter": False,
        "model_no_filter": False,
        "docs_keys": ["pro_id", "all", "view_items_by", "production_only"],
        "needed_from_hesabate": (
            "server-side filter by barcode and/or model_no/name that returns "
            "only matching rows (not the full catalog when all=1)"
        ),
    }
    if not hesabate_configured():
        return {
            "ok": False,
            "live": True,
            "total_rows": 0,
            "rows": [],
            "message": "حساباتي غير مُعدّ.",
            "error_kind": "not_configured",
            "api_capabilities": api_cap,
            "usage": hesabate_usage_summary(),
            "timing_ms": timing,
        }
    keys = list(barcode_keys or [])
    qb = str(q_barcode or "").strip()
    # تطابق تام: باركود فقط — لا نبحث برقم الصنف/الاسم
    qt = "" if barcode_exact else str(q_text or "").strip()
    if qb and not keys:
        keys = barcode_search_keys_exact(qb) if barcode_exact else barcode_search_keys(qb)
    stale = _hesabate_last_sync_age_sec() >= HESABATE_SYNC_MIN_INTERVAL_SEC
    t0 = time.perf_counter()

    lookup_raw = app_kv_get("hesabate_lookup_json", "")
    lookup_company = None
    lookup_trades = None
    lookup_model = None
    if lookup_raw:
        try:
            lookup = json.loads(lookup_raw)
            cached_at = float(lookup.get("cached_at") or 0)
            if cached_at > 0 and (time.time() - cached_at) < (HESABATE_LOOKUP_CACHE_DAYS * 86400):
                lookup_company = lookup.get("company") if isinstance(lookup.get("company"), dict) else None
                lookup_trades = lookup.get("trades") if isinstance(lookup.get("trades"), dict) else None
                lookup_model = lookup.get("model") if isinstance(lookup.get("model"), dict) else None
        except Exception:
            pass

    def _finish(payload: dict) -> dict:
        payload.setdefault("timing_ms", timing)
        payload["timing_ms"]["total_ms"] = int((time.perf_counter() - t_total) * 1000)
        payload.setdefault("search_version", _hesabate_search_data_version())
        payload.setdefault("usage", hesabate_usage_summary())
        payload.setdefault("api_capabilities", api_cap)
        payload.setdefault("catalog_downloaded", False)
        return payload

    # ——— كاش سريع (يُتخطى عند refresh) ———
    if not refresh:
        local_rows = _lookup_location_rows_for_live(
            barcode_keys=keys or None,
            text_query="" if keys else qt,
            limit=10,
            barcode_exact=barcode_exact,
        )
        timing["db_ms"] = int((time.perf_counter() - t0) * 1000)
        if local_rows:
            return _finish(
                {
                    "ok": True,
                    "live": True,
                    "fast_path": True,
                    "stale": stale,
                    "needs_sync": stale,
                    "verified": False,
                    "products_fetch_mode": "location_rows",
                    "resolved_pro_id": str(local_rows[0].get("hesabate_product_id") or ""),
                    "resolved_pro_ids": [
                        str(r.get("hesabate_product_id") or "")
                        for r in local_rows
                        if str(r.get("hesabate_product_id") or "").strip()
                    ],
                    "total_rows": len(local_rows),
                    "rows": local_rows,
                    "has_more": False,
                    "limit": len(local_rows),
                    "offset": 0,
                    "message": f"محلي · {len(local_rows)} · {int((time.perf_counter() - t_total) * 1000)}ms",
                    "request_count": 0,
                }
            )
        try:
            t0 = time.perf_counter()
            cached_raw = lookup_hesabate_product_cache(
                barcode_keys=keys or None, text_query="" if (keys or barcode_exact) else qt, limit=10
            )
            if keys and not cached_raw and qt and not barcode_exact:
                cached_raw = lookup_hesabate_product_cache(barcode_keys=None, text_query=qt, limit=10)
            timing["db_ms"] += int((time.perf_counter() - t0) * 1000)
            if cached_raw:
                t0 = time.perf_counter()
                mapped = hesabate_api.map_products_to_location_rows(
                    {"table": cached_raw},
                    company_payload=lookup_company,
                    trades_payload=lookup_trades,
                    model_payload=lookup_model,
                )
                timing["map_ms"] = int((time.perf_counter() - t0) * 1000)
                if mapped:
                    t0 = time.perf_counter()
                    try:
                        upsert_matched_location_rows(mapped)
                    except Exception:
                        pass
                    timing["save_ms"] = int((time.perf_counter() - t0) * 1000)
                    prepared = _prepare_live_rows_from_mapped(mapped)
                    if barcode_exact and keys:
                        key_set = set(keys)
                        prepared = [
                            r
                            for r in prepared
                            if _location_row_matches_barcode_keys_exact(r, key_set, exact=True)
                        ]
                    if prepared:
                        return _finish(
                            {
                                "ok": True,
                                "live": True,
                                "fast_path": True,
                                "stale": stale,
                                "needs_sync": stale,
                                "verified": False,
                                "products_fetch_mode": "db_cache",
                                "resolved_pro_ids": [
                                    str(r.get("hesabate_product_id") or r.get("product_id") or "")
                                    for r in prepared
                                    if str(r.get("hesabate_product_id") or r.get("product_id") or "").strip()
                                ],
                                "total_rows": len(prepared),
                                "rows": prepared,
                                "has_more": False,
                                "limit": len(prepared),
                                "offset": 0,
                                "message": f"كاش · {len(prepared)} · {int((time.perf_counter() - t_total) * 1000)}ms",
                                "request_count": 0,
                            }
                        )
        except Exception:
            pass

    if cache_only:
        return _finish(
            {
                "ok": True,
                "live": True,
                "fast_path": True,
                "pending_refresh": True,
                "products_fetch_mode": "cache_miss",
                "total_rows": 0,
                "rows": [],
                "message": "غير موجود محلياً.",
                "request_count": 0,
            }
        )

    # ——— تحديث موجّه: pro_id فقط ———
    # عند refresh ابحث أيضاً بالنص حتى لو وُجد باركود (أحياناً الباركود قديم والكاش بالاسم/الأصلي)
    # barcode_exact: باركود فقط بدون نص
    pro_ids = resolve_local_hesabate_product_ids(
        barcode_keys=keys or None,
        text_query="" if barcode_exact else qt,
        limit=5,
    )
    local_fallback = _lookup_location_rows_for_live(
        barcode_keys=keys or None,
        text_query="" if keys else qt,
        limit=10,
        barcode_exact=barcode_exact,
    )
    if not local_fallback and keys and qt and not barcode_exact:
        local_fallback = _lookup_location_rows_for_live(
            barcode_keys=None, text_query=qt, limit=10
        )
    # محاولة أخيرة: اربط الصف المحلي بالكاش ثم حدّث
    if not pro_ids and local_fallback:
        pro_ids = backfill_hesabate_product_ids_for_rows(local_fallback)

    # زر «جلب الصنف»: صنف جديد غير معروف محلياً — استيراد كتالوج ثم مطابقة
    catalog_import_used = False
    if not pro_ids and discover_new and not cache_only and not barcode_exact:
        t_imp = time.perf_counter()
        logger.info(
            "hesabate_lookup discover_new importing catalog barcode=%s q=%s",
            qb,
            qt[:80] if qt else "",
        )
        sync_res = ensure_hesabate_locations_synced(force=True)
        timing["catalog_import_ms"] = int((time.perf_counter() - t_imp) * 1000)
        catalog_import_used = not bool(sync_res.get("skipped"))
        if sync_res.get("ok") is False:
            return _finish(
                {
                    "ok": False,
                    "live": True,
                    "products_fetch_mode": "catalog_import_failed",
                    "error_kind": "api_error",
                    "not_found": False,
                    "total_rows": 0,
                    "rows": [],
                    "message": sync_res.get("message") or "فشل استيراد الكتالوج للعثور على الصنف الجديد.",
                    "request_count": int(sync_res.get("request_count") or 0),
                    "timing_ms": {**timing, **(sync_res.get("timing_ms") or {})},
                }
            )
        # بعد الاستيراد: ابحث محلياً ثم حدّث موجّهاً إن وُجد pro_id
        pro_ids = resolve_local_hesabate_product_ids(
            barcode_keys=keys or None, text_query=qt, limit=5
        )
        local_fallback = _lookup_location_rows_for_live(
            barcode_keys=keys or None, text_query="" if keys else qt, limit=10
        )
        if not local_fallback and keys and qt:
            local_fallback = _lookup_location_rows_for_live(
                barcode_keys=None, text_query=qt, limit=10
            )
        if not pro_ids and local_fallback:
            pro_ids = backfill_hesabate_product_ids_for_rows(local_fallback)
        # إن وُجدت صفوف بعد الاستيراد بدون حاجة لطلب صنف إضافي
        if local_fallback and not pro_ids:
            return _finish(
                {
                    "ok": True,
                    "live": True,
                    "verified": True,
                    "products_fetch_mode": "catalog_import_match",
                    "catalog_imported": catalog_import_used,
                    "not_found": False,
                    "total_rows": len(local_fallback),
                    "rows": local_fallback[:10],
                    "resolved_pro_ids": [],
                    "message": (
                        f"تم استيراد الكتالوج وإيجاد {len(local_fallback)} صنف · "
                        f"{timing['catalog_import_ms']}ms"
                    ),
                    "request_count": int(sync_res.get("request_count") or 0),
                }
            )
        if not pro_ids and not local_fallback:
            return _finish(
                {
                    "ok": True,
                    "live": True,
                    "products_fetch_mode": "catalog_import_miss",
                    "catalog_imported": catalog_import_used,
                    "error_kind": "not_found",
                    "not_found": True,
                    "total_rows": 0,
                    "rows": [],
                    "resolved_pro_ids": [],
                    "message": (
                        "تم استيراد الكتالوج من حساباتي لكن لم يُعثر على صنف مطابق "
                        "للباركود/الاسم/الرقم الذي أدخلته."
                    ),
                    "request_count": int(sync_res.get("request_count") or 0),
                }
            )
        # pro_ids موجودة → نتابع الجلب الموجّه أدناه لأحدث سعر/كمية

    # فهرس قديم بلا الشكل المدمج لباركودات المسافات — أعد بناءه من الكاش مرة ثم أعد الحل
    parser_stale = (
        str(app_kv_get("hesabate_barcode_parser_ver", "") or "") != HESABATE_BARCODE_PARSER_VERSION
    )
    if (not pro_ids) and parser_stale and keys and any(re.search(r"[A-Z]", k) for k in keys):
        try:
            rebuilt = int(rebuild_hesabate_barcode_index_from_cache() or 0)
            app_kv_set("hesabate_barcode_parser_ver", HESABATE_BARCODE_PARSER_VERSION)
            logger.info("hesabate_barcode_index rebuilt for spaced keys n=%s", rebuilt)
            pro_ids = resolve_local_hesabate_product_ids(
                barcode_keys=keys or None,
                text_query="" if barcode_exact else qt,
                limit=5,
            )
            local_fallback = _lookup_location_rows_for_live(
                barcode_keys=keys or None,
                text_query="" if keys else qt,
                limit=10,
                barcode_exact=barcode_exact,
            )
            if not pro_ids and local_fallback:
                pro_ids = backfill_hesabate_product_ids_for_rows(local_fallback)
        except Exception as exc:
            logger.warning("hesabate_barcode_index_rebuild_failed err=%s", type(exc).__name__)

    if not pro_ids:
        if local_fallback:
            return _finish(
                {
                    "ok": True,
                    "live": True,
                    "fast_path": True,
                    "verified": False,
                    "products_fetch_mode": "local_only_no_pro_id",
                    "error_kind": "",
                    "not_found": False,
                    "needs_manual_import": False,
                    "total_rows": len(local_fallback),
                    "rows": local_fallback,
                    "resolved_pro_ids": [],
                    "message": (
                        f"نتيجة محلية · {len(local_fallback)} — الصنف غير مربوط بكاش حساباتي بعد. "
                        "اضغط «جلب صنف واحد» لاستيراد الصنف من حساباتي."
                    ),
                    "request_count": 0,
                }
            )
        return _finish(
            {
                "ok": True,
                "live": True,
                "products_fetch_mode": "targeted_search_unsupported",
                "error_kind": "targeted_search_unsupported",
                "not_found": True,
                "barcode_filter_effective": False,
                "need_full_warm": False,
                "needs_manual_import": True,
                "total_rows": 0,
                "rows": [],
                "resolved_pro_ids": [],
                "message": (
                    "الصنف غير موجود في الفهرس المحلي. "
                    "اضغط «جلب صنف واحد» لاستيراده من حساباتي (قد يستغرق دقيقة)."
                ),
                "request_count": 0,
            }
        )

    preferred_scheme = app_kv_get("hesabate_preferred_scheme", "") or "https"
    token_store = app_kv_get("hesabate_token", "")
    try:
        token_exp_mono = float(app_kv_get("hesabate_token_exp_mono", "0") or "0")
    except (TypeError, ValueError):
        token_exp_mono = 0.0
    try:
        token_exp_unix = float(app_kv_get("hesabate_token_exp_unix", "0") or "0")
    except (TypeError, ValueError):
        token_exp_unix = 0.0
    if (not token_exp_unix) or time.time() >= token_exp_unix:
        token_store = ""
        token_exp_mono = 0.0
        token_exp_unix = 0.0

    try:
        hesabate_api.invalidate_products_mem_cache(HESABATE_API_HOST)
        raw_hits: list[dict[str, Any]] = []
        request_count = 0
        model_payload = lookup_model or {}
        company_payload = lookup_company or {}
        trades_payload = lookup_trades or {}
        products_ms_sum = 0
        model_refreshed = False
        scheme = preferred_scheme

        for idx, pid in enumerate(pro_ids):
            fetched = hesabate_api.fetch_location_catalog(
                HESABATE_API_HOST,
                HESABATE_API_USERNAME,
                HESABATE_API_PASSWORD,
                verify_ssl=HESABATE_API_VERIFY_SSL,
                record_request=record_hesabate_api_request,
                preferred_scheme=scheme,
                token_from_store=token_store,
                token_exp_mono=token_exp_mono,
                token_exp_unix=token_exp_unix,
                lookup_company=company_payload,
                lookup_trades=trades_payload,
                lookup_model=model_payload,
                need_lookup_refresh=False,
                need_model_refresh=False,
                products_mode="single",
                lookup_pro_id=pid,
                lookup_barcode_keys=None,
                lookup_text_query="",
                allow_full_on_single=False,
                bypass_mem_cache=True,
                # حدّث جدول model مع كل refresh حتى يتغيّر الموقع/اسم الرف فوراً
                force_model_refresh=bool(refresh) and idx == 0,
            )
            request_count += int(fetched.get("request_count") or 0)
            api_timing = fetched.get("timing_ms") if isinstance(fetched.get("timing_ms"), dict) else {}
            products_ms_sum += int(api_timing.get("products_ms") or 0)
            if idx == 0:
                timing["auth_ms"] = int(api_timing.get("auth_ms") or 0)
                timing["model_ms"] = int(api_timing.get("model_ms") or 0)
            token_store = str(fetched.get("token") or token_store)
            token_exp_mono = float(fetched.get("token_exp_mono") or 0)
            token_exp_unix = float(fetched.get("token_exp_unix") or 0)
            if fetched.get("scheme"):
                scheme = str(fetched["scheme"])
            if fetched.get("model_refreshed") and isinstance(fetched.get("model"), dict):
                model_payload = fetched.get("model") or model_payload
                model_refreshed = True
            products = fetched.get("products") or {}
            table = products.get("table") if isinstance(products, dict) else None
            rows = [r for r in (table or []) if isinstance(r, dict)]
            # حماية: أي رد كبير = كتالوج — ارفضه فوراً
            if len(rows) > 40:
                logger.warning(
                    "hesabate_lookup rejected oversized payload rows=%s pro_id=%s",
                    len(rows),
                    pid,
                )
                continue
            hit = [r for r in rows if str(r.get("id") or "") == pid]
            raw_hits.extend(hit)

        timing["products_ms"] = products_ms_sum
        if request_count:
            record_hesabate_sync_batch(request_count)
        app_kv_set("hesabate_token", token_store)
        app_kv_set("hesabate_token_exp_mono", str(token_exp_mono))
        app_kv_set("hesabate_token_exp_unix", str(token_exp_unix))
        if scheme:
            app_kv_set("hesabate_preferred_scheme", scheme)
        if model_refreshed and isinstance(model_payload, dict):
            try:
                prev_at = time.time()
                if lookup_raw:
                    try:
                        prev_at = float(json.loads(lookup_raw).get("cached_at") or time.time())
                    except Exception:
                        pass
                app_kv_set(
                    "hesabate_lookup_json",
                    json.dumps(
                        {
                            "cached_at": prev_at,
                            "company": company_payload or {},
                            "trades": trades_payload or {},
                            "model": model_payload,
                        },
                        ensure_ascii=False,
                    ),
                )
            except Exception:
                pass

        t0 = time.perf_counter()
        if raw_hits:
            try:
                upsert_hesabate_product_cache_hits(raw_hits)
            except Exception:
                pass
        mapped = hesabate_api.map_products_to_location_rows(
            {"table": raw_hits},
            company_payload=company_payload,
            trades_payload=trades_payload,
            model_payload=model_payload,
        )
        # املأ الموقع/الشركة الفارغة من الصف المحلي قبل العرض والحفظ
        if mapped:
            try:
                conn_prev = get_db()
                try:
                    ensure_location_rows_import_schema(conn_prev)
                    for mrow in mapped:
                        pid = str(mrow.get("product_id") or "").strip()
                        if not pid:
                            continue
                        prev = conn_prev.execute(
                            """
                            SELECT location, company, brand, notes, original_number
                            FROM location_rows
                            WHERE source_file = ?
                              AND hesabate_product_id = ?
                            ORDER BY id DESC
                            LIMIT 1
                            """,
                            (LOCATION_API_SOURCE_FILE, pid),
                        ).fetchone()
                        if not prev:
                            continue
                        pd = sql_row_to_dict(prev)
                        # عند refresh (جلب صنف): لا تخلط قيماً قديمة مع رد حساباتي
                        if refresh:
                            continue
                        if not str(mrow.get("location") or "").strip():
                            mrow["location"] = clean_shelf_location(pd.get("location"))
                        if not str(mrow.get("company") or "").strip():
                            mrow["company"] = str(pd.get("company") or "")
                        if not str(mrow.get("brand") or "").strip():
                            mrow["brand"] = str(pd.get("brand") or "")
                        if not str(mrow.get("notes") or "").strip():
                            mrow["notes"] = str(pd.get("notes") or "")
                        if not str(mrow.get("original_number") or "").strip():
                            mrow["original_number"] = str(pd.get("original_number") or "")
                finally:
                    conn_prev.close()
            except Exception:
                pass
        timing["map_ms"] = int((time.perf_counter() - t0) * 1000)
        if mapped:
            t0 = time.perf_counter()
            try:
                upsert_matched_location_rows(mapped)
                _bump_hesabate_search_data_version()
            except Exception as exc:
                logger.exception("upsert_matched_location_rows failed: %s", exc)
            timing["save_ms"] = int((time.perf_counter() - t0) * 1000)

        prepared = _prepare_live_rows_from_mapped(mapped)
        # حد عرض قائمة متعددة من بحث الاسم
        prepared = prepared[:10]
        if prepared:
            msg = (
                f"محدّث · {len(prepared)} · {int((time.perf_counter() - t_total) * 1000)}ms · "
                f"{request_count} طلب · {len(pro_ids)} pro_id"
            )
            fetch_mode = "single_pro_id"
            not_found = False
        else:
            msg = "هذا الصنف مش داخل على حساباتي"
            fetch_mode = "single_miss"
            not_found = True
        logger.info(
            "hesabate_lookup mode=%s rows=%s req=%s pro_ids=%s timing=%s catalog=0",
            fetch_mode,
            len(prepared),
            request_count,
            pro_ids,
            timing,
        )
        return _finish(
            {
                "ok": True,
                "live": True,
                "fast_path": False,
                "verified": bool(prepared),
                # لا تستبدل single_miss بـ catalog_import_then_single حتى تبقى الواجهة قادرة على إخفاء الجدول
                "products_fetch_mode": (
                    "catalog_import_then_single"
                    if catalog_import_used and prepared
                    else fetch_mode
                ),
                "catalog_imported": bool(catalog_import_used),
                "missing_from_hesabate": bool(not_found and not prepared and pro_ids),
                "resolved_pro_id": pro_ids[0] if pro_ids else "",
                "resolved_pro_ids": pro_ids,
                "barcode_filter_effective": False,
                "needs_sync": False,
                "need_full_warm": False,
                "not_found": not_found,
                "total_rows": len(prepared),
                "rows": prepared,
                "has_more": False,
                "limit": len(prepared),
                "offset": 0,
                "message": (
                    f"صنف جديد · {msg}" if catalog_import_used and prepared else msg
                ),
                "request_count": request_count,
            }
        )
    except hesabate_api.StoreApiError as e:
        try:
            flush_hesabate_usage_pending()
        except Exception:
            pass
        return _finish(
            {
                "ok": False,
                "live": True,
                "total_rows": 0,
                "rows": [],
                "error_kind": "api_error",
                "resolved_pro_ids": pro_ids,
                "message": "تعذّر التحقق من أحدث البيانات",
                "detail": str(e),
            }
        )
    except Exception as e:
        try:
            flush_hesabate_usage_pending()
        except Exception:
            pass
        return _finish(
            {
                "ok": False,
                "live": True,
                "total_rows": 0,
                "rows": [],
                "error_kind": "api_error",
                "resolved_pro_ids": pro_ids,
                "message": "تعذّر التحقق من أحدث البيانات",
                "detail": f"{type(e).__name__}: {e}",
            }
        )




app = FastAPI(title="Advanced Excel Search")
# Do not crash the serverless process if Postgres (Neon) is down/over-quota.
# Import-time failure here is what produced Vercel 500 FUNCTION_INVOCATION_FAILED.
try:
    init_db()
except Exception:
    logger.exception("init_db failed at startup; app will still serve HTML")


@app.exception_handler(DatabaseUnavailable)
def database_unavailable_handler(_request: Request, exc: DatabaseUnavailable) -> JSONResponse:
    return JSONResponse(status_code=503, content={"ok": False, "detail": str(exc)})


@app.get("/app-icon.png")
def app_icon_png() -> FileResponse:
    if not APP_ICON_PATH.is_file():
        raise HTTPException(status_code=404, detail="App icon missing")
    return FileResponse(
        APP_ICON_PATH,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@app.get("/favicon.ico")
def favicon_ico() -> FileResponse:
    """متصفحات كثيرة تطلب /favicon.ico تلقائياً — نعيد نفس شعار PNG."""
    if not APP_ICON_PATH.is_file():
        raise HTTPException(status_code=404, detail="App icon missing")
    return FileResponse(
        APP_ICON_PATH,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@app.get("/apple-touch-icon.png")
def apple_touch_icon_png() -> FileResponse:
    """أيقونة 180×180 لـ Safari: إضافة إلى الشاشة الرئيسية فقط (ليست favicon التبويب)."""
    if not APPLE_TOUCH_ICON_PATH.is_file():
        raise HTTPException(status_code=404, detail="Apple touch icon missing")
    return FileResponse(
        APPLE_TOUCH_ICON_PATH,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@app.get("/")
def home() -> HTMLResponse:
    _icon_q = app_icon_cache_query()
    _at_q = apple_touch_icon_cache_query()
    html = (
        """
<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="theme-color" content="#1a2744" />
  <meta name="apple-mobile-web-app-capable" content="yes" />
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />
  <meta name="apple-mobile-web-app-title" content="تبديل الأصناف" />
  <link rel="icon" type="image/png" sizes="16x16 32x32 48x48 96x96" href="/app-icon.png" />
  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png" />
  <link rel="manifest" href="/manifest.webmanifest" />
  <title>بحث البدائل</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;500;600;700;800&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg0: #eef2f8;
      --bg1: #f7f9fc;
      --card: #ffffff;
      --ink: #152033;
      --muted: #5b667a;
      --line: #d7deea;
      --primary: #0b66ff;
      --primary-ink: #fff;
      --secondary: #e8edf6;
      --secondary-ink: #1a2744;
      --success: #178a4d;
      --camera: #1a2744;
      --focus: #fff4c2;
      --radius: 12px;
    }
    * { box-sizing: border-box; }
    body {
      font-family: "Cairo", "Segoe UI", Tahoma, sans-serif;
      background:
        radial-gradient(1200px 420px at 100% -10%, #d9e7ff 0%, transparent 55%),
        radial-gradient(900px 380px at 0% 0%, #d9f0e4 0%, transparent 50%),
        linear-gradient(180deg, var(--bg1), var(--bg0));
      margin: 0;
      color: var(--ink);
      min-height: 100vh;
    }
    .wrap { max-width: 760px; margin: 14px auto 28px; padding: 0 12px; }
    .app-title {
      margin: 4px 0 12px;
      font-size: 1.35rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      color: var(--ink);
    }
    .card {
      background: var(--card);
      border-radius: var(--radius);
      padding: 14px 14px 16px;
      border: 1px solid rgba(21, 32, 51, .06);
      box-shadow: 0 8px 24px rgba(21, 32, 51, .06);
      margin-bottom: 12px;
    }
    .card h2 {
      margin: 0 0 10px;
      font-size: 1.05rem;
      font-weight: 800;
    }
    .admin-panel {
      margin-bottom: 12px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: rgba(255,255,255,.72);
      overflow: hidden;
    }
    .admin-panel > summary {
      cursor: pointer;
      list-style: none;
      padding: 12px 14px;
      font-weight: 800;
      font-size: 0.95rem;
      color: var(--secondary-ink);
      background: #f3f6fb;
      user-select: none;
    }
    .admin-panel > summary::-webkit-details-marker { display: none; }
    .admin-panel > summary::after {
      content: "▾";
      float: left;
      opacity: .55;
    }
    .admin-panel[open] > summary::after { content: "▴"; }
    .admin-panel .admin-body { padding: 12px 14px 14px; }
    input, select, button {
      width: 100%;
      padding: 12px 14px;
      margin: 6px 0;
      border: 1px solid var(--line);
      border-radius: 10px;
      box-sizing: border-box;
      font-size: 16px;
      font-family: inherit;
      font-weight: 600;
    }
    input:focus, select:focus {
      outline: none;
      border-color: #7aa8ff;
      box-shadow: 0 0 0 3px rgba(11, 102, 255, .15);
    }
    button {
      background: var(--primary);
      color: var(--primary-ink);
      border: none;
      cursor: pointer;
      font-weight: 800;
      min-height: 46px;
      transition: transform .12s ease, filter .12s ease, background .12s ease;
    }
    button:hover { filter: brightness(1.04); }
    button:active { transform: scale(.985); }
    button:disabled { opacity: .65; cursor: wait; }
    .btn-secondary { background: var(--secondary); color: var(--secondary-ink); }
    .btn-success { background: var(--success); color: #fff; }
    .btn-camera { background: var(--camera); color: #fff; }
    .btn-row { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 4px; }
    .btn-row button { flex: 1; min-width: 120px; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; table-layout: fixed; }
    th, td { border: 1px solid #eee; padding: 6px; text-align: right; vertical-align: top; word-wrap: break-word; }
    #resultsTable th:first-child, #resultsTable td:first-child { width: 14%; font-size: 12px; color: #444; }
    #wipersTable th:first-child, #wipersTable td:first-child { width: 12%; font-size: 12px; color: #444; }
    th { background: #fafafa; }
    .muted { color: var(--muted); font-size: 13px; line-height: 1.4; }
    .status-line {
      margin: 8px 0 0;
      padding: 8px 10px;
      border-radius: 10px;
      background: #f4f7fc;
      color: var(--muted);
      font-size: 12.5px;
      line-height: 1.4;
    }
    .pill { display: inline-block; background: #eef4ff; color: #2d5fff; padding: 7px 10px; border-radius: 10px; margin: 4px 4px 0 0; font-size: 12px; text-align: right; }
    .pill-btn { border: none; cursor: pointer; min-width: 180px; }
    .quick-name { display: inline; font-weight: 700; }
    .quick-alt { display: block; margin-top: 3px; color: #4e5f8f; }
    .quick-size { display: block; margin-top: 3px; color: #3e4d76; font-size: 12px; }
    .row-focus { background: var(--focus); transition: background-color 0.25s ease; }
    .upload-label { display: block; margin-top: 8px; font-size: 12px; color: #334; font-weight: 700; }
    .upload-actions { display: flex; gap: 8px; margin-top: 8px; }
    .upload-actions button { flex: 1; }
    .field-label {
      display: block;
      margin: 10px 0 2px;
      font-size: 12.5px;
      font-weight: 800;
      color: var(--secondary-ink);
    }
    .drop-zone { margin-top: 8px; border: 2px dashed #b8c4e6; border-radius: 10px; padding: 12px; text-align: center; color: #445; background: #f8faff; }
    .drop-zone.active { border-color: #2d5fff; background: #eef3ff; }
    .progress-wrap { margin-top: 8px; height: 8px; background: #e8ecf5; border-radius: 999px; overflow: hidden; }
    .progress-bar { height: 100%; width: 0%; background: #2d5fff; transition: width 0.2s ease; }
    .tabs {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 6px;
      margin: 0 0 12px;
      padding: 4px;
      background: #eef2f8;
      border-radius: 12px;
    }
    .tab-btn {
      flex: 1;
      background: transparent;
      color: var(--secondary-ink);
      font-size: 13px;
      font-weight: 800;
      padding: 10px 4px;
      margin: 0;
      min-height: 42px;
      border-radius: 9px;
    }
    .tab-btn.active {
      background: #fff;
      color: var(--primary);
      box-shadow: 0 2px 8px rgba(21, 32, 51, .08);
    }
    .print-barcode-tab-btn {
      display: block;
      width: 100%;
      box-sizing: border-box;
      margin: 0 0 10px;
      padding: 14px 12px;
      min-height: 48px;
      border: none;
      border-radius: 12px;
      background: linear-gradient(180deg, #1b8a52 0%, #146c40 100%);
      color: #fff;
      font-size: 16px;
      font-weight: 900;
      font-family: inherit;
      cursor: pointer;
      box-shadow: 0 4px 14px rgba(20, 108, 64, .28);
    }
    .print-barcode-tab-btn.active,
    .print-barcode-tab-btn:active {
      filter: brightness(1.05);
      box-shadow: 0 2px 10px rgba(20, 108, 64, .4);
    }
    .print-dual-units {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      direction: ltr;
      margin-top: 4px;
      align-items: start;
    }
    .print-side-unit {
      border: 2px solid #b7d8c4;
      border-radius: 14px;
      padding: 12px;
      background: #f4fbf6;
      min-width: 0;
    }
    .print-side-unit[data-side="left"] { border-color: #7eb892; }
    .print-side-unit[data-side="right"] { border-color: #6a9fd4; background: #f3f8fd; }
    .print-side-title {
      margin: 0 0 10px;
      font-weight: 900;
      font-size: 15px;
      text-align: center;
      color: #1f4d32;
    }
    .print-side-unit[data-side="right"] .print-side-title { color: #1e4a75; }
    .print-side-unit .field-label { margin-top: 8px; }
    .print-side-unit .field-label:first-of-type { margin-top: 0; }
    /* معاينة شكل الليبل كما سيُطبع (50×26 مم) داخل كل جهة */
    .print-side-preview {
      display: none;
      margin-top: 10px;
      padding: 10px 8px 16px;
      border-radius: 10px;
      border: 1px dashed #9ec9ae;
      background: #f3f6f4;
      text-align: center;
      overflow: visible;
      max-width: 100%;
      box-sizing: border-box;
    }
    .print-side-unit[data-side="right"] .print-side-preview { border-color: #9bb8d9; background: #f3f6fb; }
    .print-side-preview.empty-slot { opacity: .45; }
    .print-label-mock-caption {
      margin: 0 0 6px;
      font-size: 11px;
      font-weight: 700;
      color: #5b667a;
    }
    .print-label-mock-host {
      /* قياسات اللزقة 50×26 مم داخل حامل 105×28 */
      --mock-scale: 1;
      --print-label-w: 50mm;
      --print-label-h: 26mm;
      width: var(--print-label-w);
      height: var(--print-label-h);
      max-width: none;
      margin: 0 auto calc(var(--print-label-h) * (var(--mock-scale) - 1) + 8px);
      box-sizing: border-box;
      border: none;
      background: #fff;
      overflow: hidden;
      position: relative;
      box-shadow: 0 0 0 1px #222, 0 1px 3px rgba(0,0,0,.08);
      transform: scale(var(--mock-scale));
      transform-origin: top center;
    }
    .print-label-mock-host .print-label {
      width: var(--print-label-w);
      max-width: var(--print-label-w);
      height: var(--print-label-h);
      max-height: var(--print-label-h);
      box-sizing: border-box;
      padding: 0;
      margin: 0;
      position: relative;
      display: block;
      overflow: hidden;
      background: #fff;
      color: #000;
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
    }
    .print-label-mock-host .pl-name,
    .print-label-mock-host .pl-orig,
    .print-label-mock-host .pl-loc,
    .print-label-mock-host .pl-bc-wrap {
      position: absolute;
      box-sizing: border-box;
      overflow: hidden;
      margin: 0;
    }
    .print-label-mock-host .pl-name {
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
      font-weight: 900;
      line-height: 1.02;
      text-align: center;
      white-space: normal;
      word-break: break-word;
      overflow-wrap: anywhere;
    }
    .print-label-mock-host .pl-orig,
    .print-label-mock-host .pl-loc {
      font-weight: 800;
      white-space: nowrap;
      text-align: center;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .print-label-mock-host .pl-bc-wrap {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: flex-start;
      gap: 0;
      padding: 0;
    }
    .print-label-mock-host .pl-bc-wrap.is-bc-unreliable {
      outline: 1px solid #b00020;
      background: #fff5f5;
    }
    .print-label-mock-host .pl-bc-warn {
      flex: 0 0 auto;
      max-width: 100%;
      font-size: 7.5px;
      font-weight: 800;
      color: #b00020;
      line-height: 1.15;
      text-align: center;
      white-space: normal;
    }
    .print-label-mock-host .pl-bc-wrap img.pl-bc,
    .print-label-mock-host .pl-bc-wrap svg {
      display: block;
      margin: 0 auto;
      max-width: none;
      image-rendering: pixelated;
      object-fit: none;
    }
    .print-label-mock-host .pl-bc-text {
      flex: 0 0 3.2mm;
      height: 3.2mm;
      min-height: 3.2mm;
      width: 100%;
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
      font-weight: 900;
      color: #000;
      background: #fff;
      letter-spacing: 0.04em;
      white-space: pre;
      line-height: 3.2mm;
      text-align: center;
      max-width: 100%;
      overflow: hidden;
      position: relative;
      z-index: 3;
    }
    .print-side-preview .pv-compat { display: none !important; }
    .print-side-status {
      margin-top: 6px;
      font-size: 12px;
      min-height: 1.2em;
    }
    .print-layout-settings {
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid #c5d4e8;
      background: #f5f8fc;
    }
    .print-layout-settings summary {
      color: #1e3a5f;
    }
    .print-layout-settings input[type="range"] {
      width: 100%;
      margin-top: 4px;
    }
    @media (max-width: 720px) {
      .print-dual-units { grid-template-columns: 1fr; direction: rtl; }
    }
    .print-label-sheet { display: none; }
    .search-panel { display: none; }
    .search-panel.active { display: block; }
    @media print {
      /* حامل الليبل 105×28 مم · هامش 1 مم · لزقتان 50×26 · فجوة 3 مم */
      @page {
        size: 105mm 28mm;
        margin: 0;
      }
      html, body {
        width: 105mm !important;
        height: auto !important;
        margin: 0 !important;
        padding: 0 !important;
        background: #fff !important;
        -webkit-print-color-adjust: exact !important;
        print-color-adjust: exact !important;
      }
      body.printing-labels > *:not(#printLabelSheet) {
        display: none !important;
      }
      .print-label-sheet {
        display: block !important;
        position: static !important;
        width: 100%;
        margin: 0;
        padding: 0;
        background: #fff;
      }
      .print-label-row {
        display: flex;
        flex-direction: row;
        direction: ltr;
        justify-content: flex-start;
        align-items: flex-start;
        width: 105mm;
        height: 28mm;
        max-height: 28mm;
        box-sizing: border-box;
        margin: 0;
        padding: 1mm;
        gap: 3mm;
        page-break-after: always;
        break-after: page;
        page-break-inside: avoid;
        break-inside: avoid;
        overflow: hidden;
      }
      .print-label-row:last-child {
        page-break-after: auto !important;
        break-after: auto !important;
      }
      .print-label {
        width: 50mm;
        max-width: 50mm;
        height: 26mm;
        max-height: 26mm;
        box-sizing: border-box;
        padding: 0;
        margin: 0;
        text-align: center;
        font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
        color: #000;
        overflow: hidden !important;
        position: relative;
        display: block;
      }
      .print-label-row .print-label:first-child,
      .print-label.print-label-left,
      .print-label-row .print-label:last-child,
      .print-label.print-label-right {
        margin: 0;
        padding: 0;
      }
      .print-label.print-label-empty {
        visibility: hidden;
      }
      .print-label .pl-name,
      .print-label .pl-orig,
      .print-label .pl-loc {
        position: absolute;
        box-sizing: border-box;
        overflow: hidden !important;
        margin: 0;
        padding: 0;
      }
      .print-label .pl-bc-wrap {
        position: absolute;
        box-sizing: border-box;
        overflow: hidden !important;
        margin: 0;
        padding: 0;
      }
      .print-label .pl-name {
        font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
        font-size: 22px;
        font-weight: 900;
        line-height: 1.02;
        letter-spacing: -0.01em;
        white-space: normal !important;
        word-break: break-word;
        overflow-wrap: anywhere;
        text-align: center;
        align-content: center;
      }
      .print-label .pl-orig,
      .print-label .pl-loc {
        font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
        font-size: 8px;
        font-weight: 800;
        white-space: nowrap;
        text-align: center;
        direction: ltr;
        display: flex;
        align-items: center;
        justify-content: center;
      }
      .print-label .pl-meta { display: none !important; }
      .print-label .pl-bc-wrap {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: flex-start;
        gap: 0;
        padding: 0;
        overflow: hidden !important;
      }
      .print-label .pl-bc-wrap.is-bc-unreliable {
        outline: 1px solid #b00020;
        background: #fff5f5;
      }
      .print-label .pl-bc-warn {
        flex: 0 0 auto;
        max-width: 100%;
        font-size: 7.5px;
        font-weight: 800;
        color: #b00020;
        line-height: 1.15;
        text-align: center;
        white-space: normal;
        padding: 0 0.4mm;
      }
      .print-label .pl-bc-wrap svg,
      .print-label .pl-bc-wrap img.pl-bc,
      .print-label .pl-bc-wrap .pl-bc-rot {
        display: block;
        margin: 0 auto;
        overflow: visible;
        image-rendering: pixelated;
        image-rendering: crisp-edges;
        -ms-interpolation-mode: nearest-neighbor;
        object-fit: none;
        max-width: none;
      }
      .print-label .pl-bc-wrap .pl-bc-rot svg {
        shape-rendering: crispEdges;
      }
      .print-label .pl-bc-wrap svg {
        shape-rendering: crispEdges;
      }
      .print-label .pl-bc-text {
        flex: 0 0 3.2mm;
        height: 3.2mm;
        min-height: 3.2mm;
        max-width: 100%;
        width: 100%;
        font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
        font-size: 9px;
        font-weight: 900;
        color: #000;
        background: #fff;
        letter-spacing: .05em;
        margin: 0;
        padding: 0;
        line-height: 3.2mm;
        overflow: hidden !important;
        text-overflow: clip;
        white-space: pre !important;
        direction: ltr;
        box-sizing: border-box;
        text-align: center;
        position: relative;
        z-index: 3;
      }
    }
    /* قياس مؤقت قبل الطباعة حتى يشتغل تكبير/تصغير الاسم */
    .print-label-sheet.print-measure {
      display: block !important;
      position: fixed !important;
      left: -10000px !important;
      top: 0 !important;
      width: 106mm !important;
      visibility: hidden !important;
      pointer-events: none !important;
      z-index: -1 !important;
    }
    .print-label-sheet.print-measure .print-label-row {
      display: flex;
      direction: ltr;
      justify-content: flex-start;
      align-items: flex-start;
      width: 105mm;
      height: 28mm;
      padding: 1mm;
      gap: 3mm;
      box-sizing: border-box;
    }
    .print-label-sheet.print-measure .print-label {
      width: 50mm;
      max-width: 50mm;
      height: 26mm;
      max-height: 26mm;
      box-sizing: border-box;
      padding: 0;
      margin: 0;
      overflow: hidden;
      position: relative;
      display: block;
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
    }
    .print-label-sheet.print-measure .print-label-row .print-label:first-child,
    .print-label-sheet.print-measure .print-label.print-label-left,
    .print-label-sheet.print-measure .print-label-row .print-label:last-child,
    .print-label-sheet.print-measure .print-label.print-label-right {
      margin: 0;
      padding: 0;
    }
    .print-label-sheet.print-measure .pl-name,
    .print-label-sheet.print-measure .pl-orig,
    .print-label-sheet.print-measure .pl-loc,
    .print-label-sheet.print-measure .pl-bc-wrap {
      position: absolute;
      box-sizing: border-box;
      overflow: hidden;
      margin: 0;
    }
    .print-label-sheet.print-measure .pl-name {
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
      font-weight: 900;
      line-height: 1.02;
      white-space: normal;
      word-break: break-word;
      overflow-wrap: anywhere;
      text-align: center;
    }
    .print-label-sheet.print-measure .pl-orig,
    .print-label-sheet.print-measure .pl-loc {
      font-weight: 800;
      white-space: nowrap;
      text-align: center;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .print-label-sheet.print-measure .pl-meta { display: none !important; }
    .print-label-sheet.print-measure .pl-bc-wrap {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: flex-start;
      gap: 0;
      padding: 0;
    }
    .print-label-sheet.print-measure .pl-bc-wrap svg,
    .print-label-sheet.print-measure .pl-bc-wrap img.pl-bc {
      margin: 0 auto;
      max-width: none;
      max-height: none;
      object-fit: none;
      image-rendering: pixelated;
      image-rendering: crisp-edges;
    }
    .print-label-sheet.print-measure .pl-bc-wrap svg {
      shape-rendering: crispEdges;
    }
    .print-label-sheet.print-measure .pl-bc-text {
      flex: 0 0 3.2mm;
      height: 3.2mm;
      min-height: 3.2mm;
      width: 100%;
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
      font-weight: 900;
      color: #000;
      background: #fff;
      letter-spacing: .05em;
      white-space: pre;
      text-align: center;
      max-width: 100%;
      overflow: hidden;
      position: relative;
      z-index: 3;
    }
    /* محرر GUI للزقتين — مقياس شاشة (مم→بكسل) */
    .print-designer-wrap {
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #f7f9fc;
      overflow: hidden;
    }
    .print-designer-wrap > summary {
      cursor: pointer;
      list-style: none;
      padding: 12px 14px;
      font-weight: 800;
      background: #eef3fa;
      user-select: none;
    }
    .print-designer-wrap > summary::-webkit-details-marker { display: none; }
    .print-designer-wrap > summary::after {
      content: "▾";
      float: left;
      opacity: .5;
    }
    .print-designer-wrap[open] > summary::after { content: "▴"; }
    .print-designer-body { padding: 12px; }
    .print-designer-hint {
      margin: 0 0 10px;
      font-size: 12.5px;
      color: var(--muted);
      line-height: 1.45;
    }
    .print-designer-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin-bottom: 10px;
    }
    .print-designer-toolbar .btn-secondary,
    .print-designer-toolbar button {
      width: auto;
      margin: 0;
      padding: 8px 12px;
      font-size: 12px;
    }
    .print-font-tools {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
      padding: 6px 8px;
      border: 1px solid #c5d4e8;
      border-radius: 10px;
      background: #fff;
    }
    .print-font-tools label {
      font-size: 12px;
      font-weight: 800;
      margin: 0;
    }
    #printFontSize {
      min-width: 64px;
      height: 32px;
      font-size: 13px;
      font-weight: 700;
      padding: 2px 6px;
    }
    #printFontBold {
      width: 34px;
      min-width: 34px;
      height: 32px;
      padding: 0;
      font-family: "Times New Roman", serif;
      font-size: 16px;
      font-weight: 900;
      font-style: italic;
    }
    #printFontBold.is-on {
      background: #1e3a5f;
      color: #fff;
      border-color: #1e3a5f;
    }
    #printFontTarget {
      font-size: 12px;
      font-weight: 700;
      color: #4a5b73;
    }
    #printDesignerReadout {
      font-size: 12px;
      font-weight: 700;
      color: #1a2744;
      line-height: 1.4;
    }
    .print-designer-scroll {
      overflow: auto;
      max-width: 100%;
      padding: 20px 16px 32px;
      min-height: 280px;
    }
    .print-designer-sheet {
      position: relative;
      box-sizing: border-box;
      background: #cfd8e6;
      border: none;
      border-radius: 6px;
      margin: 0 auto;
      direction: ltr;
      user-select: none;
      touch-action: none;
      overflow: visible; /* السماح بسحب الخانات خارج حدود اللزقة */
    }
    .print-designer-sheet-frame {
      position: absolute;
      box-sizing: border-box;
      background: #dfe6f1;
      border: 1px dashed #8fa0b8;
      border-radius: 4px;
      pointer-events: none;
    }
    .print-designer-row {
      position: absolute;
      box-sizing: border-box;
      display: flex;
      flex-direction: row;
      direction: ltr;
      overflow: visible;
    }
    .print-designer-label {
      position: relative;
      box-sizing: border-box;
      background: #fff;
      border: 1.5px solid #1a2744;
      overflow: visible; /* مقابض التكبير على حواف الخانات */
      box-shadow: 0 1px 0 rgba(0,0,0,.06);
    }
    .print-designer-label .pdg-side-tag {
      position: absolute;
      top: 2px;
      left: 3px;
      z-index: 8;
      font-size: 9px;
      font-weight: 800;
      color: #5b667a;
      pointer-events: none;
      opacity: .75;
    }
    .print-designer-label[data-side="right"] .pdg-side-tag { left: auto; right: 3px; }
    .pdg-zone {
      position: absolute;
      box-sizing: border-box;
      overflow: visible; /* لازم ظاهرة عشان مقابض التكبير برا الحافة */
      border: 1px solid rgba(45,95,255,.45);
      background: rgba(255,255,255,.92);
      cursor: grab;
      z-index: 2;
      touch-action: none;
    }
    .pdg-zone.is-active {
      outline: 2px solid #2d5fff;
      outline-offset: -1px;
      background: rgba(45,95,255,.07);
      z-index: 4;
    }
    .pdg-zone.is-dragging { cursor: grabbing; z-index: 6; }
    .pdg-zone.is-resizing { cursor: inherit; z-index: 6; }
    .pdg-zone-inner {
      width: 100%;
      height: 100%;
      box-sizing: border-box;
      display: flex;
      align-items: center;
      justify-content: center;
      text-align: center;
      padding: 0;
      overflow: hidden;
      border-radius: 1px;
      font-weight: 800;
      color: #111;
      line-height: 1.05;
      pointer-events: none;
    }
    .pdg-zone[data-zone="name"] .pdg-zone-inner {
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
      font-weight: 900;
      align-content: center;
      white-space: normal;
      word-break: break-word;
      overflow-wrap: anywhere;
    }
    .pdg-zone[data-zone="orig"] .pdg-zone-inner,
    .pdg-zone[data-zone="loc"] .pdg-zone-inner {
      white-space: nowrap;
    }
    .pdg-zone[data-zone="bc"] .pdg-zone-inner {
      flex-direction: column;
      justify-content: flex-start;
      gap: 0;
      padding: 0;
    }
    .pdg-bc-bars {
      width: 92%;
      background: repeating-linear-gradient(
        90deg,
        #000 0,
        #000 1.5px,
        #fff 1.5px,
        #fff 3px
      );
      border: 1px solid #222;
      flex: 1 1 auto;
      min-height: 8px;
      max-height: 100%;
    }
    .pdg-bc-text {
      flex: 0 0 auto;
      min-height: 12px;
      font-family: "Cairo", "Tahoma", "Segoe UI", Arial, sans-serif;
      font-weight: 900;
      color: #000;
      letter-spacing: 0.04em;
      white-space: pre;
      line-height: 1;
      max-width: 100%;
      overflow: hidden;
    }
    .pdg-resize {
      position: absolute;
      z-index: 8;
      background: #2d5fff;
      border: 2px solid #fff;
      box-shadow: 0 1px 3px rgba(0,0,0,.28);
      touch-action: none;
      opacity: .95;
    }
    .pdg-resize:hover,
    .pdg-resize.is-dragging {
      background: #1a46d4;
      opacity: 1;
    }
    /* زاوية: كبّر العرض والارتفاع معاً */
    .pdg-resize-se {
      right: -7px;
      bottom: -7px;
      width: 15px;
      height: 15px;
      border-radius: 3px;
      cursor: nwse-resize;
    }
    /* حافة يمين: كبّر/صغّر العرض فقط */
    .pdg-resize-e {
      right: -6px;
      top: 50%;
      transform: translateY(-50%);
      width: 11px;
      height: 26px;
      border-radius: 3px;
      cursor: ew-resize;
    }
    /* حافة تحت: كبّر/صغّر الارتفاع فقط */
    .pdg-resize-s {
      left: 50%;
      bottom: -6px;
      transform: translateX(-50%);
      width: 26px;
      height: 11px;
      border-radius: 3px;
      cursor: ns-resize;
    }
    .pdg-zone-label {
      position: absolute;
      top: 1px;
      right: 3px;
      font-size: 8px;
      font-weight: 700;
      color: #5b667a;
      pointer-events: none;
      opacity: .7;
      z-index: 2;
    }
    @media (max-width: 720px) {
      .print-designer-sheet { transform-origin: top center; }
    }
    /* جدول المواقع خارج لوحته — لا يظهر في طباعة/رئيسي/قياسات/قشطان */
    body.tab-print #locationsTableWrap,
    body.tab-print #wipersTableWrap,
    body.tab-print #resultsTable,
    body.tab-main #locationsTableWrap,
    body.tab-size #locationsTableWrap,
    body.tab-wiper #locationsTableWrap,
    body.tab-print #names {
      display: none !important;
    }
    .sync-box {
      margin: 0 0 12px;
      padding: 12px;
      background: #eef7f1;
      border-radius: 12px;
      border: 1px solid #b7d7c2;
    }
    .sync-box .sync-title {
      margin: 0 0 6px;
      font-weight: 800;
      color: #1b4332;
      font-size: 0.95rem;
    }
    details.sync-collapse {
      margin: 12px 0 0;
      padding: 0;
      background: #eef7f1;
      border-radius: 12px;
      border: 1px solid #b7d7c2;
      overflow: hidden;
    }
    details.sync-collapse > summary {
      cursor: pointer;
      list-style: none;
      padding: 12px 14px;
      font-weight: 800;
      font-size: 0.95rem;
      color: #1b4332;
      background: #e3f3ea;
      user-select: none;
    }
    details.sync-collapse > summary::-webkit-details-marker { display: none; }
    details.sync-collapse > summary::after {
      content: "▾";
      float: left;
      opacity: .55;
    }
    details.sync-collapse[open] > summary::after { content: "▴"; }
    details.sync-collapse .sync-collapse-body {
      padding: 10px 12px 12px;
      border-top: 1px solid #c5e0d0;
    }
    .sync-meta {
      margin: 0;
      font-size: 12.5px;
      color: var(--muted);
      line-height: 1.4;
    }
    .sync-meta strong { color: var(--ink); font-weight: 800; }
    .wiper-box {
      margin: 0 0 12px;
      padding: 12px;
      background: #f4f6fb;
      border-radius: 12px;
      border: 1px dashed #b8c4e6;
    }
    /* جدول المواقع — عرض كامل بدون سحب، خط يتكيّف مع عرض الخانة */
    #locationsTableWrap {
      overflow-x: hidden;
      overflow-y: visible;
      width: 100%;
      max-width: 100%;
      margin-top: 14px;
      border-radius: 14px;
      border: 1px solid #d5dde9;
      background: #fff;
      box-shadow: 0 6px 20px rgba(21, 32, 51, .07);
    }
    #locationsTable {
      table-layout: fixed;
      width: 100%;
      min-width: 0;
      font-size: 14px;
      border-collapse: collapse;
    }
    #locationsTable th,
    #locationsTable td {
      padding: 0;
      vertical-align: middle;
      border-bottom: 1px solid #e8edf5;
      overflow: hidden;
    }
    #locationsTable tbody tr:nth-child(even) { background: #f7f9fd; }
    #locationsTable tbody tr:hover { background: #eef4ff; }
    #locationsTable tbody tr:last-child td { border-bottom: none; }
    #locationsTable th {
      font-size: 12px;
      font-weight: 800;
      color: #102038;
      background: #eef2f8;
      position: sticky;
      top: 0;
      z-index: 1;
      text-align: right;
      border-bottom: 1px solid #d5dde9;
    }
    #locationsTable .cell {
      display: flex;
      align-items: center;
      justify-content: flex-start;
      box-sizing: border-box;
      width: 100%;
      min-height: 44px;
      padding: 8px 6px;
      overflow: hidden;
    }
    #locationsTable th .cell {
      min-height: 40px;
      padding: 8px 6px;
      font-weight: 800;
      color: #102038;
    }
    #locationsTable .cell-text {
      display: block;
      width: 100%;
      max-width: 100%;
      white-space: normal;
      overflow: visible;
      text-overflow: unset;
      word-break: break-word;
      overflow-wrap: anywhere;
      line-height: 1.35;
      font-variant-numeric: tabular-nums;
    }
    #locationsTable .cell-empty {
      color: #94a3b8;
      font-weight: 600;
      white-space: normal;
    }
    /* الصنف | الموقع | الشركة | الأصلي | الكمية | السعر */
    #locationsTable th:nth-child(1), #locationsTable td:nth-child(1) { width: 30%; }
    #locationsTable th:nth-child(2), #locationsTable td:nth-child(2) { width: 16%; }
    #locationsTable th:nth-child(3), #locationsTable td:nth-child(3) { width: 16%; }
    #locationsTable th:nth-child(4), #locationsTable td:nth-child(4) { width: 16%; }
    #locationsTable th:nth-child(5), #locationsTable td:nth-child(5) { width: 11%; }
    #locationsTable th:nth-child(6), #locationsTable td:nth-child(6) { width: 11%; }
    /* لفّ النص لكل خلايا الجدول */
    #locationsTable td {
      overflow: visible;
      vertical-align: top;
    }
    #locationsTable td .cell {
      align-items: flex-start;
      overflow: visible;
      min-height: 44px;
      height: auto;
    }
    #locationsTable td:nth-child(1) .cell-text { font-weight: 700; color: var(--ink); }
    #locationsTable td:nth-child(2) .cell-text { font-weight: 800; color: #0b3d2e; }
    #locationsTable td:nth-child(3) .cell-text { font-weight: 600; color: #334155; }
    #locationsTable td:nth-child(4) .cell-text { font-weight: 700; color: #1e293b; }
    #locationsTable th:nth-child(5) .cell,
    #locationsTable td:nth-child(5) .cell,
    #locationsTable th:nth-child(6) .cell,
    #locationsTable td:nth-child(6) .cell {
      justify-content: center;
      text-align: center;
      padding-left: 4px;
      padding-right: 4px;
    }
    #locationsTable td:nth-child(5) .cell-text { font-weight: 800; color: #0d47a1; text-align: center; }
    #locationsTable td:nth-child(6) .cell-text { font-weight: 800; color: #1b5e20; text-align: center; }
    @media (max-width: 767px) {
      #locationsTableWrap {
        margin-left: 0;
        margin-right: 0;
        width: 100%;
        max-width: 100%;
        border-radius: 12px;
        overflow-x: hidden;
      }
      #locationsTable {
        min-width: 0;
        width: 100%;
        font-size: 11px;
      }
      #locationsTable th { font-size: 10px; }
      #locationsTable th:nth-child(1), #locationsTable td:nth-child(1) { width: 28%; }
      #locationsTable th:nth-child(2), #locationsTable td:nth-child(2) { width: 15%; }
      #locationsTable th:nth-child(3), #locationsTable td:nth-child(3) { width: 15%; }
      #locationsTable th:nth-child(4), #locationsTable td:nth-child(4) { width: 18%; }
      #locationsTable th:nth-child(5), #locationsTable td:nth-child(5) { width: 12%; }
      #locationsTable th:nth-child(6), #locationsTable td:nth-child(6) { width: 12%; }
      #locationsTable .cell { min-height: 38px; padding: 6px 3px; }
      #locationsTable th .cell { min-height: 34px; padding: 6px 3px; }
    }
    @media (min-width: 768px) {
      #locationsTable { font-size: 15px; }
      #locationsTable th { font-size: 13px; }
      #locationsTable .cell { min-height: 50px; padding: 10px 10px; }
      #locationsTable th .cell { min-height: 44px; }
    }
    .barcode-modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.72); z-index: 9999; align-items: center; justify-content: center; flex-direction: column; padding: 12px; box-sizing: border-box; }
    .barcode-modal-overlay.active { display: flex; }
    .barcode-modal-box { background: #fff; border-radius: 14px; padding: 14px; max-width: min(98vw, 640px); width: 100%; text-align: center; box-shadow: 0 8px 32px rgba(0,0,0,.2); box-sizing: border-box; }
    #barcodeScannerRegion { min-height: 280px; width: 100%; margin: 10px 0; border-radius: 10px; overflow: hidden; background: #111; }
    html.barcode-scan-open, html.barcode-scan-open body { overflow: hidden !important; }
    html.barcode-scan-open body { position: fixed !important; width: 100%; left: 0; right: 0; }
    html.barcode-scan-open #barcodeScannerRegion video {
      object-fit: cover !important;
      width: 100% !important;
      height: 100% !important;
    }
    @media screen and (orientation: landscape) {
      html.barcode-scan-open .barcode-modal-overlay.active {
        inset: auto;
        position: fixed;
        top: 100%;
        left: 0;
        width: 100vh;
        height: 100vw;
        max-width: none;
        transform: rotate(-90deg);
        transform-origin: left top;
        padding: 12px;
      }
    }
    html.barcode-scan-open.scan-is-landscape .barcode-modal-overlay.active.force-portrait {
      inset: auto;
      position: fixed;
      top: 100%;
      left: 0;
      width: 100vh;
      height: 100vw;
      max-width: none;
      transform: rotate(-90deg);
      transform-origin: left top;
      padding: 12px;
    }
    html.barcode-scan-open.scan-is-landscape .barcode-modal-overlay.active.force-portrait-left {
      inset: auto;
      position: fixed;
      top: 0;
      left: 100%;
      width: 100vh;
      height: 100vw;
      max-width: none;
      transform: rotate(90deg);
      transform-origin: left top;
      padding: 12px;
    }
    html.barcode-scan-open.scan-is-landscape .barcode-modal-overlay.active .barcode-modal-box {
      max-width: min(92vh, 640px);
      max-height: 96vw;
      overflow: auto;
    }
    .print-sheet-img {
      display: block;
      width: 105mm;
      height: 28mm;
      image-rendering: pixelated;
      image-rendering: crisp-edges;
      page-break-after: always;
      break-after: page;
    }
    .print-sheet-img:last-child { page-break-after: auto; break-after: auto; }
    .print-designer-label { border: none; outline: 1px solid #1a2744; background-color: #fff; }
    .pdg-zone { background: transparent; border: 1px dashed rgba(45,95,255,.55); }
    .pdg-zone.is-active { background: rgba(45,95,255,.08); }
    .pdg-zone-inner { display: none; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1 class="app-title">بحث البدائل</h1>

    <details class="admin-panel">
      <summary>ملفات وإعدادات</summary>
      <div class="admin-body">
        <button type="button" class="btn-secondary" onclick="pickFolderDeep()">اختيار مجلد</button>
        <div class="upload-actions">
          <button type="button" class="btn-secondary" onclick="clearSelectedFiles()">تفريغ</button>
          <button id="uploadBtn" type="button" onclick="uploadFiles()">رفع</button>
        </div>
        <div id="selectionInfo" class="muted">الملفات: 0</div>
        <div id="progressInfo" class="muted"></div>
        <div class="progress-wrap"><div id="progressBar" class="progress-bar"></div></div>
        <div id="uploadResult" class="muted"></div>
        <div class="upload-actions">
          <button type="button" onclick="syncSnapshotFromServer()">مزامنة الهاتف</button>
          <button type="button" class="btn-secondary" onclick="checkSnapshotUpdate(true, true)">فحص التحديث</button>
        </div>
        <div id="syncInfo" class="muted"></div>
      </div>
    </details>

    <div class="card">
      <button type="button" id="tabPrintBtn" class="print-barcode-tab-btn" onclick="setSearchTab('print')">طباعة باركود</button>
      <div class="tabs">
        <button id="tabMainBtn" type="button" class="tab-btn active" onclick="setSearchTab('main')">رئيسي</button>
        <button id="tabSizeBtn" type="button" class="tab-btn" onclick="setSearchTab('size')">قياسات</button>
        <button id="tabWiperBtn" type="button" class="tab-btn" onclick="setSearchTab('wiper')">قشطان</button>
        <button id="tabLocBtn" type="button" class="tab-btn" onclick="setSearchTab('location')">موقع</button>
      </div>

      <div id="printSearchPanel" class="search-panel">
        <details id="printLayoutDesigner" class="print-designer-wrap" style="margin-top:0;margin-bottom:12px;">
          <summary>شكل الليبل · تصميم GUI — اسحب للنقل · المربعات الزرقا للتكبير/التصغير</summary>
          <div class="print-designer-body">
            <p class="print-designer-hint">
              حامل الليبل <strong>105×28 مم</strong> · هامش <strong>1 مم</strong> من كل الجهات ·
              لزقة <strong>50×26 مم</strong> · فجوة <strong>3 مم</strong> ·
              انقر خانة في الليبل ثم غيّر <strong>حجم الخط</strong> أو <strong>B</strong> (ثخين).
            </p>
            <div class="print-designer-toolbar">
              <button type="button" class="btn-secondary" onclick="resetPrintLayoutSettings()">إعادة الافتراضي للاثنين</button>
              <button type="button" class="btn-secondary" onclick="copyPrintLayoutSide('left','right')">نسخ يسار → يمين</button>
              <button type="button" class="btn-secondary" onclick="copyPrintLayoutSide('right','left')">نسخ يمين → يسار</button>
              <button type="button" class="btn-secondary" onclick="refreshPrintDesignerBoard()">تحديث من الأصناف الحالية</button>
              <button type="button" class="btn-secondary" onclick="uploadPrintLayoutPrefsToServer(true)">اعتماد قياسات GUI على السيرفر</button>
              <div class="print-font-tools">
                <label for="printFontSize">حجم الخط</label>
                <select id="printFontSize" onchange="onPrintFontSizeChange()" disabled>
                  <option value="8">8</option>
                  <option value="9">9</option>
                  <option value="10">10</option>
                  <option value="11">11</option>
                  <option value="12">12</option>
                  <option value="14">14</option>
                  <option value="16">16</option>
                  <option value="18">18</option>
                  <option value="20">20</option>
                  <option value="22">22</option>
                  <option value="24">24</option>
                  <option value="26">26</option>
                  <option value="28">28</option>
                  <option value="32">32</option>
                  <option value="36">36</option>
                  <option value="48">48</option>
                </select>
                <button type="button" id="printFontBold" class="btn-secondary" onclick="onPrintFontBoldToggle()" title="ثخين" disabled>B</button>
                <span id="printFontTarget">حدد عنصراً في الليبل</span>
              </div>
              <div id="printDesignerReadout">—</div>
            </div>
            <div class="print-designer-scroll">
              <div id="printDesignerBoard" class="print-designer-sheet" aria-label="محرر شكل اللزقتين"></div>
            </div>
            <div style="display:none;" aria-hidden="true">
              <input id="printSetNamePx" type="range" min="16" max="56" step="1" value="36" />
              <input id="printSetOriginalPx" type="range" min="6" max="16" step="1" value="9" />
              <input id="printSetLocationPx" type="range" min="6" max="16" step="1" value="9" />
              <input id="printSetBcShiftX" type="range" min="-6" max="6" step="0.5" value="0" />
              <input id="printSetBcShiftY" type="range" min="-5" max="5" step="0.5" value="0" />
              <span id="printSetNamePxVal"></span>
              <span id="printSetOriginalPxVal"></span>
              <span id="printSetLocationPxVal"></span>
              <span id="printSetBcShiftXVal"></span>
              <span id="printSetBcShiftYVal"></span>
            </div>
          </div>
        </details>

        <div class="print-dual-units">
          <div class="print-side-unit" data-side="left">
            <p class="print-side-title">لزق اليسار</p>
            <label class="field-label" for="printQueryLeft">الباركود فقط</label>
            <input id="printQueryLeft" placeholder="باركود اليسار — من قاعدة المواقع" inputmode="text" autocomplete="off" onkeydown="if(event.key==='Enter'){event.preventDefault();resolvePrintItemFromLocal('left',true);}" />
            <input id="printNameLeft" type="hidden" value="" data-from-fetch="1" />
            <div class="btn-row" style="margin-top:8px;">
              <button type="button" id="printFetchBtnLeft" class="btn-secondary" onclick="fetchItemForBarcodePrint('left')">جلب اليسار</button>
            </div>
            <div id="printFetchStatusLeft" class="muted print-side-status"></div>
            <div id="printOriginalWrapLeft" style="display:none;margin-top:8px;">
              <label class="field-label" for="printOriginalSelectLeft">الرقم الأصلي — يسار</label>
              <select id="printOriginalSelectLeft"></select>
            </div>
            <div id="printPreviewLeft" class="print-side-preview" style="display:none;">
              <p class="print-label-mock-caption">معاينة يسار — لزقة 50×26 مم (حامل 105×28)</p>
              <div id="printLabelMockLeft" class="print-label-mock-host"></div>
              <div class="pv-compat" aria-hidden="true">
                <span id="printPvOriginalLeft"></span>
                <span id="printPvLocationLeft"></span>
                <p class="pv-name" id="printPvNameLeft"></p>
                <svg id="printPvBarcodeLeft"></svg>
                <div id="printPvBarcodeTextLeft"></div>
              </div>
            </div>
          </div>
          <div class="print-side-unit" data-side="right">
            <p class="print-side-title">لزق اليمين</p>
            <label class="field-label" for="printQueryRight">الباركود فقط</label>
            <input id="printQueryRight" placeholder="باركود اليمين — من قاعدة المواقع" inputmode="text" autocomplete="off" onkeydown="if(event.key==='Enter'){event.preventDefault();resolvePrintItemFromLocal('right',true);}" />
            <input id="printNameRight" type="hidden" value="" data-from-fetch="1" />
            <div class="btn-row" style="margin-top:8px;">
              <button type="button" id="printFetchBtnRight" class="btn-secondary" onclick="fetchItemForBarcodePrint('right')">جلب اليمين</button>
            </div>
            <div id="printFetchStatusRight" class="muted print-side-status"></div>
            <div id="printOriginalWrapRight" style="display:none;margin-top:8px;">
              <label class="field-label" for="printOriginalSelectRight">الرقم الأصلي — يمين</label>
              <select id="printOriginalSelectRight"></select>
            </div>
            <div id="printPreviewRight" class="print-side-preview" style="display:none;">
              <p class="print-label-mock-caption">معاينة يمين — لزقة 50×26 مم (حامل 105×28)</p>
              <div id="printLabelMockRight" class="print-label-mock-host"></div>
              <div class="pv-compat" aria-hidden="true">
                <span id="printPvOriginalRight"></span>
                <span id="printPvLocationRight"></span>
                <p class="pv-name" id="printPvNameRight"></p>
                <svg id="printPvBarcodeRight"></svg>
                <div id="printPvBarcodeTextRight"></div>
              </div>
            </div>
          </div>
        </div>

        <div class="print-unit-fields" style="margin-top:12px;">
          <label class="field-label" for="printQtyInput">عدد الأوراق (كل ورقة = لزق يسار + لزق يمين معاً)</label>
          <input id="printQtyInput" type="text" inputmode="numeric" pattern="[0-9]*" lang="en" dir="ltr" autocomplete="off" value="1" placeholder="1" style="font-variant-numeric:lining-nums;font-feature-settings:'tnum';text-align:left;" onfocus="this.select()" onmouseup="event.preventDefault()" oninput="onPrintQtyInputChange()" onkeydown="if(event.key==='Enter'){event.preventDefault();printBarcodeLabels();}" />
          <p id="printQtyHint" class="muted" style="margin:4px 0 0;font-size:12px;">كل ورقة تطبع اللزقتين معاً (يسار + يمين)</p>
        </div>
        <p class="muted" style="margin:6px 0 0;font-size:12px;">
          الطابعة: <strong>Rongta RP4xx Series</strong> · Code128 · حامل 105×28 مم · هامش 1 مم · لزقة 50×26 مم · فجوة 3 مم.
        </p>
        <div class="btn-row" style="margin-top:10px;">
          <button type="button" class="btn-success" onclick="printBarcodeLabels()">طباعة على Rongta</button>
        </div>
        <div id="printFetchStatus" class="muted" style="margin-top:8px;"></div>
      </div>

      <div id="mainSearchPanel" class="search-panel active">
        <p id="mainLastUpdate" class="sync-meta" style="margin:0 0 10px;">آخر تحديث: —</p>
        <input id="queryInput" placeholder="اسم الصنف أو البدائل" />
        <input id="numberQueryInput" placeholder="رقم / ملاحظات / رقم أصلي" />
        <button onclick="searchNow('main')">بحث</button>
      </div>

      <div id="sizeSearchPanel" class="search-panel">
        <select id="sizeTypeInput">
          <option value="">النوع (اختياري)</option>
          <option value="لبادات">لبادات</option>
          <option value="بيل">بيل</option>
          <option value="مصلبات دراي شفط">مصلبات دراي شفط</option>
        </select>
        <input id="sizeWidthInput" placeholder="العرض (B)" />
        <input id="sizeDiameterInput" placeholder="القطر (C)" />
        <input id="sizeHeightInput" placeholder="الارتفاع (D)" />
        <button onclick="searchNow('size')">بحث</button>
      </div>

      <div id="wiperSearchPanel" class="search-panel">
        <p id="wiperLastUpdate" class="sync-meta" style="margin:0 0 10px;">آخر تحديث: —</p>
        <div class="wiper-box">
          <input type="file" id="wiperSheetFileInput" accept=".xlsx,.xlsm,.xls" />
          <button type="button" class="btn-secondary" onclick="uploadWiperSheetFile()">رفع ملف القشطان</button>
          <div id="wiperUploadStatus" class="muted"></div>
        </div>
        <input id="wiperCarInput" placeholder="اسم السيارة" />
        <input id="wiperModelInput" placeholder="الموديل (اختياري)" />
        <input id="wiperEngineInput" placeholder="الماتور (اختياري)" />
        <button onclick="searchNow('wiper')">بحث</button>
      </div>

      <div id="locationSearchPanel" class="search-panel">
        <details class="sync-collapse" style="margin:0 0 12px;">
          <summary>حساباتي — تحديث الكل والمزامنة</summary>
          <div class="sync-collapse-body">
            <p id="hesabateSyncStatus" class="sync-meta">جاري التحقق...</p>
            <p id="hesabateRequestCounter" class="sync-meta"><strong>طلبات API:</strong> —</p>
            <p id="hesabateAutoSyncCounter" class="sync-meta"><strong>تحديث تلقائي:</strong> —</p>
            <p class="sync-meta muted" style="margin:6px 0 0;font-size:12px;line-height:1.4;">
              مزامنة تلقائية كل 4 ساعات والتطبيق مغلق (8 صباحاً–8 مساءً بتوقيت فلسطين) · ليلاً متوقفة · بيانات حساباتي هي المعروضة · الزر اليدوي يعمل دائماً
            </p>
            <div class="btn-row" style="margin-top:10px;">
              <button type="button" class="btn-success" onclick="forceHesabateFullImport()" title="تنزيل كل الأصناف وتحديث التعديلات الآن">تحديث الكل الآن</button>
            </div>
            <details style="margin-top:10px;">
              <summary class="muted" style="cursor:pointer;font-size:13px;font-weight:700;">Excel للمواقع</summary>
              <input type="file" id="locationSheetFileInput" accept=".xlsx,.xlsm,.xls" style="margin-top:8px;" />
              <button type="button" class="btn-secondary" onclick="uploadLocationSheetFile()">تحديث المواقع</button>
              <div id="locationUploadStatus" class="muted"></div>
            </details>
          </div>
        </details>
        <label class="field-label" for="locationTextQueryInput">اسم / رقم أصلي / ملاحظات</label>
        <input id="locationTextQueryInput" placeholder="اسم الصنف أو الرقم الأصلي أو رقم من الملاحظات" inputmode="search" autocomplete="off" />
        <label class="field-label" for="locationBarcodeQueryInput">باركود</label>
        <input id="locationBarcodeQueryInput" type="text" placeholder="اكتب أو امسح الباركود" inputmode="text" enterkeyhint="search" autocomplete="off" autocapitalize="characters" spellcheck="false" />
        <div class="btn-row">
          <button type="button" onclick="searchNow('location')" title="بحث محلي في قاعدة البيانات فقط — بدون طلب حساباتي">بحث</button>
          <button type="button" id="hesabateFetchOneBtn" class="btn-secondary" onclick="fetchOneItemFromHesabate()" title="جلب صنف واحد من حساباتي بالباركود أو الاسم أو رقم الملاحظات">جلب صنف واحد</button>
          <button type="button" class="btn-camera" onclick="openLocationBarcodeScanner()">كاميرا</button>
        </div>
      </div>

      <div id="stats" class="status-line" style="display:none;"></div>
      <div id="names"></div>
      <div style="overflow:auto;">
        <table id="resultsTable" style="display:none;">
          <thead>
            <tr>
              <th>الملف</th>
              <th>الصنف</th>
              <th>الرقم</th>
              <th>البدائل</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div id="wipersTableWrap" style="display:none; overflow:auto;">
        <table id="wipersTable" style="display:none;">
          <thead>
            <tr>
              <th>الملف</th>
              <th>السيارة</th>
              <th>الموديل</th>
              <th>الماتور</th>
              <th>الموقع</th>
              <th>الرقم</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div id="locationsTableWrap" style="display:none;">
        <table id="locationsTable" style="display:none;">
          <thead>
            <tr>
              <th><div class="cell"><span class="cell-text">الصنف</span></div></th>
              <th><div class="cell"><span class="cell-text">الموقع</span></div></th>
              <th><div class="cell"><span class="cell-text">الشركة</span></div></th>
              <th><div class="cell"><span class="cell-text">الأصلي</span></div></th>
              <th><div class="cell"><span class="cell-text">الكمية</span></div></th>
              <th><div class="cell"><span class="cell-text">السعر</span></div></th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>

  <div id="printLabelSheet" class="print-label-sheet" aria-hidden="true"></div>

  <div id="barcodeModal" class="barcode-modal-overlay" aria-hidden="true">
    <div class="barcode-modal-box">
      <strong>مسح الباركود</strong>
      <p class="muted" style="margin:8px 0;">ثبّت الكاميرا على الباركود الخطّي</p>
      <p id="barcodeScanStatus" style="margin:10px 0 6px;font-size:14px;font-weight:700;color:#1a237e;">وجّه الكاميرا</p>
      <p id="barcodeScanDebug" class="muted" style="font-size:11px;margin:0 0 8px;line-height:1.35;display:none;word-break:break-all;"></p>
      <div id="barcodeScannerRegion"></div>
      <div class="btn-row" style="margin-top:12px;">
        <button type="button" id="barcodeScanConfirmBtn" class="btn-secondary" disabled>تأكيد</button>
        <button type="button" class="btn-secondary" onclick="closeBarcodeScanner()">إغلاق</button>
      </div>
    </div>
  </div>

  <script>
    let autoRefreshTimer = null;
    let searchDebounceTimer = null;
    const selectedFilesMap = new Map();
    let selectedFileSerial = 0;
    const CLIENT_APP_VERSION = '{APP_VERSION}';
    let activeSearchTab = 'main';
    const OFFLINE_DB_NAME = 'badail_offline_db';
    const OFFLINE_DB_STORE = 'snapshots';
    const OFFLINE_DB_KEY = 'latest';
    /** حد آمن لحجم الملف في طلب واحد (Vercel ~4.5MB للطلب كاملاً مع multipart). */
    const VERCEL_SAFE_UPLOAD_BYTES = 2400000;
    const LOCATION_TAB_SOURCE_FILE = 'مواقع_من_التطبيق.xlsx';
    const SCRIPT_XLSX = 'https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js';
    const SCRIPT_QR = 'https://unpkg.com/html5-qrcode@2.3.8/html5-qrcode.min.js';
    const SCRIPT_JSBARCODE = 'https://cdn.jsdelivr.net/npm/jsbarcode@3.11.6/dist/JsBarcode.all.min.js';
    /** اسم الطابعة كما يظهر في ويندوز بعد تثبيت درايفر Rongta */
    const RONGTA_PRINTER_NAME = 'Rongta RP4xx Series';
    const loadedScripts = {};
    const printSides = {
      left: { item: null, rows: [], lookupGen: 0, debounce: null },
      right: { item: null, rows: [], lookupGen: 0, debounce: null }
    };
    const PRINT_FORM_STORAGE_KEY = 'printBarcodeFormState';
    let printFormSaveTimer = null;
    let printFormRestoreBusy = false;
    let printJobTitleRestore = null;
    function printSideKey(side) {
      return side === 'right' ? 'right' : 'left';
    }
    function printSideCap(side) {
      return printSideKey(side) === 'right' ? 'Right' : 'Left';
    }
    let localSnapshot = { version: '', updated_at: '', total_rows: 0, rows: [] };
    let snapshotLoadPromise = null;
    let lastNotifiedVersion = '';

    function loadExternalScript(src) {
      if (loadedScripts[src]) return loadedScripts[src];
      loadedScripts[src] = new Promise((resolve, reject) => {
        const s = document.createElement('script');
        s.src = src;
        s.async = true;
        s.onload = () => resolve();
        s.onerror = () => {
          loadedScripts[src] = null;
          reject(new Error('script-load-failed'));
        };
        document.head.appendChild(s);
      });
      return loadedScripts[src];
    }
    async function ensureXlsxLib() {
      if (typeof XLSX !== 'undefined') return;
      await loadExternalScript(SCRIPT_XLSX);
    }
    async function ensureQrLib() {
      if (typeof Html5Qrcode !== 'undefined') return;
      await loadExternalScript(SCRIPT_QR);
    }
    async function ensureJsBarcodeLib() {
      if (typeof JsBarcode === 'undefined') await loadExternalScript(SCRIPT_JSBARCODE);
      try { if (document.fonts && document.fonts.ready) await document.fonts.ready; } catch (_) {}
    }
    function setSyncInfo(message) {
      document.getElementById('syncInfo').innerText = message || '';
    }
    function setStats(message) {
      const el = document.getElementById('stats');
      if (!el) return;
      const msg = message || '';
      el.innerText = msg;
      el.style.display = msg ? 'block' : 'none';
    }

    function setBodySearchTabClass(tab) {
      try {
        ['tab-main', 'tab-size', 'tab-wiper', 'tab-location', 'tab-print'].forEach((c) => {
          document.body.classList.remove(c);
        });
        document.body.classList.add('tab-' + (tab || 'main'));
      } catch (_) {}
    }
    function setSearchTab(tab) {
      if (tab === 'size') activeSearchTab = 'size';
      else if (tab === 'wiper') activeSearchTab = 'wiper';
      else if (tab === 'location') activeSearchTab = 'location';
      else if (tab === 'print') activeSearchTab = 'print';
      else activeSearchTab = 'main';
      localStorage.setItem('activeSearchTab', activeSearchTab);
      setBodySearchTabClass(activeSearchTab);
      document.getElementById('tabMainBtn').classList.toggle('active', activeSearchTab === 'main');
      document.getElementById('tabSizeBtn').classList.toggle('active', activeSearchTab === 'size');
      document.getElementById('tabWiperBtn').classList.toggle('active', activeSearchTab === 'wiper');
      document.getElementById('tabLocBtn').classList.toggle('active', activeSearchTab === 'location');
      const printBtn = document.getElementById('tabPrintBtn');
      if (printBtn) printBtn.classList.toggle('active', activeSearchTab === 'print');
      document.getElementById('mainSearchPanel').classList.toggle('active', activeSearchTab === 'main');
      document.getElementById('sizeSearchPanel').classList.toggle('active', activeSearchTab === 'size');
      document.getElementById('wiperSearchPanel').classList.toggle('active', activeSearchTab === 'wiper');
      document.getElementById('locationSearchPanel').classList.toggle('active', activeSearchTab === 'location');
      const printPanel = document.getElementById('printSearchPanel');
      if (printPanel) printPanel.classList.toggle('active', activeSearchTab === 'print');
      if (activeSearchTab === 'location') {
        ensureQrLib().catch(() => {});
        startHesabateLocationPoller();
      } else {
        stopHesabateLocationPoller();
      }
      if (activeSearchTab === 'print') {
        ensureJsBarcodeLib().catch(() => {});
        // لا تعِد الجلب الثقيل عند كل فتح تبويب — يمنع تعليق الواجهة
        const namesEl = document.getElementById('names');
        const resultsTable = document.getElementById('resultsTable');
        if (namesEl) namesEl.innerHTML = '';
        if (resultsTable) resultsTable.style.display = 'none';
        const wiperWrap = document.getElementById('wipersTableWrap');
        const wipersTable = document.getElementById('wipersTable');
        const locWrap = document.getElementById('locationsTableWrap');
        const locTable = document.getElementById('locationsTable');
        if (wiperWrap) wiperWrap.style.display = 'none';
        if (wipersTable) wipersTable.style.display = 'none';
        if (locWrap) locWrap.style.display = 'none';
        if (locTable) locTable.style.display = 'none';
        setStats('');
        // أعد آخر قيم/معاينة إن فُقدت (تحديث صفحة أو غياب الحالة)
        try {
          const leftQ = ((document.getElementById('printQueryLeft') || {}).value || '').trim();
          const rightQ = ((document.getElementById('printQueryRight') || {}).value || '').trim();
          const need =
            (!leftQ && !rightQ)
            || (leftQ && !printSides.left.item && !(printSides.left.rows && printSides.left.rows.length))
            || (rightQ && !printSides.right.item && !(printSides.right.rows && printSides.right.rows.length));
          if (need) restorePrintFormState().catch(() => {});
        } catch (_) {
          restorePrintFormState().catch(() => {});
        }
        // لا تفتح محرر GUI تلقائياً — فقط حدّثه إن كان مفتوحاً يدوياً
        try {
          const des = document.getElementById('printLayoutDesigner');
          if (des && des.open) refreshPrintDesignerBoard();
        } catch (_) {}
        return;
      }
      // احفظ حقول الطباعة قبل مغادرة التبويب
      try { savePrintFormState(); } catch (_) {}
      if (activeSearchTab !== 'location') {
        try { clearLocationTable(null); } catch (_) {}
      }
      try {
        searchNow('auto');
      } catch (_) {}
    }

    let lastHesabateSearchVersion = null;
    function renderHesabateUsage(usage, message, meta) {
      const st = document.getElementById('hesabateSyncStatus');
      const ctr = document.getElementById('hesabateRequestCounter');
      const autoCtr = document.getElementById('hesabateAutoSyncCounter');
      meta = meta || {};
      if (!usage) {
        if (st) st.textContent = message || 'تعذّر قراءة حالة المزامنة.';
        return;
      }
      const count = Number(usage.count || 0);
      const limit = Number(usage.limit || 0);
      const remaining = Number(usage.remaining || 0);
      const lastSync = meta.last_sync_at || usage.last_location_sync_at || usage.last_sync_at || '';
      const rows = usage.last_location_rows || '0';
      const lastErr = meta.last_error || '';
      const autoN = Number(usage.auto_sync_count || 0);
      const autoLast = usage.auto_sync_last_at || '';
      const cronPing = usage.cron_last_ping_at || meta.cron_last_ping_at || '';
      if (ctr) {
        ctr.innerHTML = `<strong>طلبات API:</strong> ${count.toLocaleString('en-US')} / ${limit.toLocaleString('en-US')} (متبقي ${remaining.toLocaleString('en-US')})`;
      }
      if (autoCtr) {
        autoCtr.innerHTML = `<strong>تحديث تلقائي:</strong> ${autoN.toLocaleString('en-US')} مرة`
          + (autoLast ? ` · آخر مرة ${autoLast}` : '')
          + (cronPing ? ` · نبضة مجدول ${cronPing}` : '');
      }
      let line = usage.configured
        ? (message || 'المزامنة تعمل')
        : 'بيانات حساباتي غير مضبوطة';
      if (lastSync) line += ` · ${lastSync}`;
      if (rows && rows !== '0') line += ` · ${rows} صنف`;
      if (lastErr) line += ` · خطأ: ${lastErr}`;
      if (st) st.textContent = line;
    }
    let sharedLocPrintSyncBusy = false;
    let sharedLocPrintSyncTimer = null;
    function hasSharedLocationQuery() {
      try {
        const t = ((document.getElementById('locationTextQueryInput') || {}).value || '').trim();
        const bRaw = (document.getElementById('locationBarcodeQueryInput') || {}).value || '';
        const b = (typeof lightSanitizeBarcodeValue === 'function')
          ? lightSanitizeBarcodeValue(bRaw)
          : String(bRaw).trim();
        return !!(t || b);
      } catch (_) {
        return false;
      }
    }
    async function refreshLoadedPrintSidesFromSharedDb() {
      try {
        for (const side of ['left', 'right']) {
          const raw = ((document.getElementById('printQuery' + printSideCap(side)) || {}).value || '').trim();
          if (!raw) continue;
          const st = printSides[side];
          if (!st || (!(st.rows && st.rows.length) && !st.item)) continue;
          try {
            const rows = await lookupLocalLocationsForPrint(raw);
            if (rows.length) {
              applyPrintLookupRows(side, rows, 'متزامن مع بيانات المواقع', true);
            }
          } catch (_) {}
        }
      } catch (_) {}
    }
    async function syncSharedLocationAndPrintData(source) {
      // المواقع وطباعة الباركود يشتركان بنفس location_rows — بدون حلقات ثقيلة تعلّق الواجهة
      if (sharedLocPrintSyncBusy) return;
      sharedLocPrintSyncBusy = true;
      try {
        if (
          source !== 'location-search'
          && source !== 'version'
          && source !== 'location-fetch'
          && source !== 'location-auto'
          && hasSharedLocationQuery()
          && !locationLiveRefreshBusy
          && activeSearchTab === 'location'
        ) {
          try { await searchNow('location'); } catch (_) {}
        }
        if (source !== 'print-local' && activeSearchTab === 'print') {
          await refreshLoadedPrintSidesFromSharedDb();
        } else if (source === 'location-fetch' || source === 'location-auto' || source === 'full-import') {
          // حدّث معاينة الطباعة فقط إن كانت محمّلة مسبقاً (خفيف)
          await refreshLoadedPrintSidesFromSharedDb();
        }
      } catch (_) {
        /* لا تُسقط أخطاء المزامنة المشتركة واجهة التطبيق */
      } finally {
        sharedLocPrintSyncBusy = false;
      }
    }
    function scheduleSharedLocationAndPrintSync(source) {
      clearTimeout(sharedLocPrintSyncTimer);
      sharedLocPrintSyncTimer = setTimeout(() => {
        syncSharedLocationAndPrintData(source || 'debounced');
      }, 200);
    }
    function applyHesabateSearchVersion(data) {
      const ver = data && data.search_version != null ? Number(data.search_version) : null;
      if (ver == null || Number.isNaN(ver)) return;
      const changed = lastHesabateSearchVersion != null && ver !== lastHesabateSearchVersion;
      lastHesabateSearchVersion = ver;
      if (changed) {
        // لا تُشغّل بحث ثقيل من مجرّد تغيّر الإصدار — يكفي مزامنة خفيفة مؤجّلة
        scheduleSharedLocationAndPrintSync('version');
      }
    }

    async function refreshHesabateStatus() {
      try {
        const res = await fetch('/hesabate/status', { cache: 'no-store' });
        const data = await readJsonSafe(res);
        applyHesabateSearchVersion(data);
        const msg = data.configured
          ? (data.last_error ? 'آخر مزامنة فشلت' : 'المزامنة تعمل')
          : '';
        renderHesabateUsage(data.usage || {}, msg, {
          last_sync_at: data.last_sync_at,
          last_error: data.last_error,
          cron_last_ping_at: (data.usage && data.usage.cron_last_ping_at) || data.cron_last_ping_at || ''
        });
      } catch (_) {
        renderHesabateUsage(null, 'تعذّر الاتصال بحالة API.');
      }
    }

    let hesabateEnsureTimer = null;
    let hesabateGlobalEnsureTimer = null;
    let hesabateEnsureInflight = false;
    let hesabateLookupInflight = null;
    let locationSearchGen = 0;
    let locationLiveRefreshBusy = false;
    const locationLiveRefreshByKey = new Map();
    const HESABATE_CLIENT_POLL_MS = 90000; // ~90ث — تحديث عداد الحالة في تبويب المواقع فقط (بدون مزامنة)
    // فحص خفيف كل 15د؛ السيرفر يمنع طلب API إن كانت البيانات حديثة
    let HESABATE_CLIENT_ENSURE_MS = 15 * 60 * 1000;
    function isCompleteBarcodeQuery(bc) {
      const compact = String(bc || '').replace(/[\\s\\-]/g, '');
      return compact.length >= 8 && /[0-9A-Za-z]/.test(compact);
    }
    function currentLocationBarcodeKey() {
      return lightSanitizeBarcodeValue(
        (document.getElementById('locationBarcodeQueryInput') || {}).value || ''
      ).replace(/[\\s\\-]/g, '').toLowerCase();
    }
    function stopHesabateLocationPoller() {
      if (hesabateEnsureTimer) {
        clearInterval(hesabateEnsureTimer);
        hesabateEnsureTimer = null;
      }
    }
    function startHesabateLocationPoller() {
      stopHesabateLocationPoller();
      // عدّاد الحالة + إعادة البحث إن تغيّر search_version
      refreshHesabateStatus();
      hesabateEnsureTimer = setInterval(() => refreshHesabateStatus(), HESABATE_CLIENT_POLL_MS);
    }
    function stopHesabateAutoEnsure() {
      if (hesabateGlobalEnsureTimer) {
        clearInterval(hesabateGlobalEnsureTimer);
        hesabateGlobalEnsureTimer = null;
      }
    }
    function startHesabateAutoEnsure() {
      // فحص دوري خفيف؛ المزامنة الفعلية فقط إذا فات الفاصل على السيرفر
      stopHesabateAutoEnsure();
      const tick = () => {
        ensureHesabateLocationSyncQuiet(false).catch(() => {});
      };
      (async () => {
        try {
          const res = await fetch('/hesabate/status', { cache: 'no-store' });
          const data = await readJsonSafe(res);
          const sec = Number((data && data.usage && data.usage.sync_min_interval_sec) || data.sync_min_interval_sec || 0);
          const age = Number(data && data.age_sec);
          // إن فات الموعد — زامِن بعد الإقلاع؛ وإلا افحص كل 15د فقط
          if (sec >= 60 && Number.isFinite(age) && age >= sec) {
            setTimeout(tick, 8000);
          }
        } catch (_) {
          setTimeout(tick, 8000);
        }
        hesabateGlobalEnsureTimer = setInterval(tick, HESABATE_CLIENT_ENSURE_MS);
      })();
    }
    async function ensureHesabateLocationSyncQuiet(force) {
      if (hesabateEnsureInflight) return null;
      hesabateEnsureInflight = true;
      try {
        const stEl = document.getElementById('hesabateSyncStatus');
        if (stEl && force) stEl.textContent = 'جاري التحديث من حساباتي...';
        const url = force ? '/hesabate/sync' : '/hesabate/ensure';
        const res = await fetch(url, { method: 'POST', cache: 'no-store' });
        const data = await readJsonSafe(res);
        let msg = data.message || '';
        if (data && data.ok === false) {
          msg = data.message || data.last_error || 'فشل المزامنة — بقيت البيانات السابقة قابلة للبحث.';
        } else if (data && data.ok && data.skipped && data.reason === 'fresh') {
          msg = `النسخة المخزّنة حديثة (قبل ${Number(data.age_sec || 0)} ث) — البحث فوري بدون طلب API.`;
        } else if (data && data.ok && data.skipped && data.reason === 'inflight') {
          msg = 'مزامنة أخرى جارية على الخادم (قفل مشترك بين الأجهزة).';
        } else if (data && data.ok && data.skipped && data.reason === 'unchanged') {
          msg = 'لا تغيير في حساباتي — لم تُعد كتابة الأصناف (توفير).';
        } else if (data && data.ok && !data.skipped) {
          msg = data.message || 'تم تحديث الأصناف من حساباتي.';
        }
        applyHesabateSearchVersion(data);
        renderHesabateUsage(data.usage || {}, msg, {
          last_sync_at: data.last_sync_at,
          last_error: data.last_error || (data.ok === false ? data.message : '')
        });
        return data;
      } catch (_) {
        await refreshHesabateStatus();
        return null;
      } finally {
        hesabateEnsureInflight = false;
      }
    }
    async function forceHesabateLocationSync() {
      await forceHesabateFullImport();
    }
    function isMissingFromHesabate(live) {
      if (!live || live.ok === false) return false;
      const rows = Array.isArray(live.rows) ? live.rows : [];
      if (rows.length) return false;
      if (live.missing_from_hesabate) return true;
      if (live.products_fetch_mode === 'single_miss') return true;
      const hasPro = Array.isArray(live.resolved_pro_ids) && live.resolved_pro_ids.length > 0;
      if (live.not_found && hasPro) return true;
      const msg = String(live.message || '');
      return msg.includes('مش داخل على حساباتي') || msg.includes('لم يُرجع صنفاً مطابقاً');
    }
    function clearLocationTable(statsText) {
      const locWrap = document.getElementById('locationsTableWrap');
      const locTable = document.getElementById('locationsTable');
      if (locTable) {
        const lBody = locTable.querySelector('tbody');
        if (lBody) lBody.innerHTML = '';
        locTable.style.display = 'none';
      }
      if (locWrap) locWrap.style.display = 'none';
      if (statsText != null) setStats(statsText);
    }
    function renderLocationRowsNow(rawRows, statsText) {
      const locWrap = document.getElementById('locationsTableWrap');
      const locTable = document.getElementById('locationsTable');
      // لا تعرض جدول المواقع خارج تبويب «موقع» (مثل طباعة الباركود)
      if (activeSearchTab !== 'location') {
        if (locWrap) locWrap.style.display = 'none';
        if (locTable) locTable.style.display = 'none';
        return 0;
      }
      const seenKeys = new Set();
      const safeRows = [];
      (Array.isArray(rawRows) ? rawRows : []).forEach((r) => {
        const k = String(r.barcode || r.original_number || r.item_name || '').trim().toLowerCase();
        if (k && seenKeys.has(k)) return;
        if (k) seenKeys.add(k);
        safeRows.push(r);
      });
      if (!safeRows.length) {
        clearLocationTable(statsText || 'لا نتيجة.');
        return 0;
      }
      setStats( statsText || `نتائج المواقع: ${safeRows.length}`);
      const lBody = locTable.querySelector('tbody');
      lBody.innerHTML = '';
      const cellHtml = (value) => {
        const t = String(value ?? '').trim();
        if (!t) return `<div class="cell"><span class="cell-text cell-empty">—</span></div>`;
        return `<div class="cell"><span class="cell-text">${escapeHtml(t)}</span></div>`;
      };
      safeRows.forEach((r) => {
        const tr = document.createElement('tr');
        tr.innerHTML =
          `<td>${cellHtml(r.item_name)}</td>` +
          `<td>${cellHtml(r.location)}</td>` +
          `<td>${cellHtml(r.company)}</td>` +
          `<td>${cellHtml(r.original_number)}</td>` +
          `<td>${cellHtml(r.quantity)}</td>` +
          `<td>${cellHtml(r.price)}</td>`;
        lBody.appendChild(tr);
      });
      const showL = safeRows.length > 0;
      locWrap.style.display = showL ? 'block' : 'none';
      locTable.style.display = showL ? 'table' : 'none';
      if (showL) {
        requestAnimationFrame(() => fitLocationsTableText());
      }
      return safeRows.length;
    }
    function fitLocationsTableText() {
      const table = document.getElementById('locationsTable');
      if (!table || table.style.display === 'none') return;
      const isMobile = window.matchMedia('(max-width: 767px)').matches;
      const maxByCol = isMobile ? [12, 11, 11, 11, 12, 12] : [14, 14, 14, 14, 15, 15];
      table.querySelectorAll('thead th, tbody td').forEach((td) => {
        const el = td.querySelector('.cell-text');
        const cell = td.querySelector('.cell');
        if (!el || !cell || el.classList.contains('cell-empty')) return;
        const col = td.cellIndex;
        const maxPx = maxByCol[col] != null ? maxByCol[col] : (isMobile ? 11 : 14);
        // لفّ النص لكل الأعمدة
        el.style.whiteSpace = 'normal';
        el.style.overflow = 'visible';
        el.style.textOverflow = 'unset';
        el.style.wordBreak = 'break-word';
        el.style.overflowWrap = 'anywhere';
        el.style.fontSize = maxPx + 'px';
      });
    }
    function applyHesabateUsageFromResponse(live, message) {
      // حدّث عداد الطلبات في خانة «موقع» بعد أي طلب حساباتي (بما فيه طباعة الباركود)
      if (!live) return;
      try {
        if (live.usage) {
          renderHesabateUsage(live.usage, message || live.message || '', {
            last_sync_at: live.last_sync_at || (live.usage && live.usage.last_location_sync_at) || '',
            last_error: live.ok === false ? (live.message || live.last_error || '') : (live.last_error || '')
          });
          return;
        }
        if (Number(live.request_count || 0) > 0) {
          refreshHesabateStatus();
        }
      } catch (_) {}
    }
    async function lookupHesabateItemOnce(barcode, q, cacheOnly, signal, refresh, discoverNew, barcodeExact) {
      const exact = !!barcodeExact;
      const res = await fetch('/hesabate/lookup', {
        method: 'POST',
        cache: 'no-store',
        signal,
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({
          barcode: barcode || '',
          q: exact ? '' : (q || ''),
          cache_only: !!cacheOnly,
          refresh: !!refresh,
          discover_new: exact ? false : !!discoverNew,
          barcode_exact: exact
        })
      });
      const live = await readJsonSafe(res);
      if (!res.ok) throw new Error((live && live.detail) || `hesabate-lookup-${res.status}`);
      applyHesabateUsageFromResponse(live);
      return live;
    }
    function currentLocationQueryKey(barcode, text) {
      const bc = String(barcode || '').replace(/[\\s\\-]/g, '').toLowerCase();
      if (bc) return 'bc:' + bc;
      return 'q:' + String(text || '').trim().toLowerCase();
    }
    function isCurrentLocationQuery(key, barcode, text) {
      return currentLocationQueryKey(barcode, text) === key;
    }
    async function autoRefreshLocationQuery(barcode, text, gen, hadLocalRows, localMs) {
      const key = currentLocationQueryKey(barcode, text);
      if (!key || key === 'bc:' || key === 'q:') return null;
      if (key.startsWith('bc:') && !isCompleteBarcodeQuery(barcode)) return null;
      if (key.startsWith('q:') && String(text || '').trim().length < 2) return null;
      if (locationLiveRefreshByKey.has(key)) return locationLiveRefreshByKey.get(key);
      const stEl = document.getElementById('hesabateSyncStatus');
      const t0 = performance.now();
      const run = (async () => {
        locationLiveRefreshBusy = true;
        try {
          if (stEl) {
            stEl.textContent = hadLocalRows
              ? 'جاري التحقق من أحدث البيانات...'
              : 'بحث موجّه في حساباتي...';
          }
          const ctrl = new AbortController();
          const to = setTimeout(() => ctrl.abort(), 45000);
          let live = null;
          try {
            live = await lookupHesabateItemOnce(barcode || '', text || '', false, ctrl.signal, true);
          } finally {
            clearTimeout(to);
          }
          const lqBcNow = lightSanitizeBarcodeValue(
            (document.getElementById('locationBarcodeQueryInput') || {}).value || ''
          );
          const lqTextNow = ((document.getElementById('locationTextQueryInput') || {}).value || '').trim();
          if (gen !== locationSearchGen || !isCurrentLocationQuery(key, lqBcNow, lqTextNow)) return live;
          const uiMs = Math.round(performance.now() - t0);
          const timingLine = formatLookupTiming(live, uiMs);
          const rows = Array.isArray(live && live.rows) ? live.rows : [];
          if (live && live.ok === false) {
            if (hadLocalRows) {
              setStats(`نتيجة محلية (${localMs != null ? localMs + 'ms' : 'فوري'}) · تعذّر التحقق من أحدث البيانات` + (timingLine ? ` | ${timingLine}` : ''));
              if (stEl) stEl.textContent = 'تعذّر التحقق من أحدث البيانات';
            } else {
              setStats((live.message || 'تعذّر الجلب.') + (timingLine ? ` | ${timingLine}` : ''));
            }
            if (live.usage) renderHesabateUsage(live.usage, live.message || '');
            return live;
          }
          if (live && live.search_version != null) {
            lastHesabateSearchVersion = Number(live.search_version);
          }
          if (live && live.usage) renderHesabateUsage(live.usage, live.message || '');
          if (live && live.catalog_downloaded) {
            setStats('خطأ: محاولة تنزيل كتالوج من مسار البحث — أُوقفت.');
            return live;
          }
          const unsupported = !!(live && (
            live.error_kind === 'targeted_search_unsupported'
            || live.products_fetch_mode === 'targeted_search_unsupported'
          ));
          if (isMissingFromHesabate(live)) {
            const miss = 'هذا الصنف مش داخل على حساباتي';
            clearLocationTable(miss + (timingLine ? ` | ${timingLine}` : ''));
            if (stEl) stEl.textContent = miss;
          } else if (rows.length) {
            renderLocationRowsNow(
              rows,
              `${live.message || 'محدّث'} | محلي ${localMs != null ? localMs + 'ms' : '—'} → تحديث ${uiMs}ms | ${timingLine}`
            );
            if (stEl) stEl.textContent = live.message || 'تم التحديث من حساباتي';
            scheduleSharedLocationAndPrintSync('location-auto');
          } else if (unsupported || live.products_fetch_mode === 'local_only_no_pro_id') {
            // أبقِ النتائج المحلية ظاهرة — هذه ليست خطأ شبكة
            const soft = hadLocalRows
              ? `نتيجة محلية (${localMs != null ? localMs + 'ms' : 'فوري'}) · حدّث الكل مرة لربط الصنف بحساباتي`
              : (live.message || 'الصنف غير موجود محلياً — اضغط «تحديث الكل الآن» ثم أعد البحث.');
            setStats(soft + (timingLine ? ` | ${timingLine}` : ''));
            if (stEl) stEl.textContent = soft;
          } else if (live && live.not_found) {
            setStats(
              hadLocalRows
                ? `نتيجة محلية (${localMs != null ? localMs + 'ms' : 'فوري'}) · لم يُرجع حساباتي تحديثاً`
                : (live.message || 'لا يوجد صنف مطابق.')
            );
            if (stEl) stEl.textContent = hadLocalRows ? 'بقيت النتيجة المحلية' : 'غير موجود';
          } else if (hadLocalRows) {
            setStats(`نتيجة محلية (${localMs != null ? localMs + 'ms' : 'فوري'}) · تعذّر التحقق من أحدث البيانات`);
            if (stEl) stEl.textContent = 'تعذّر التحقق من أحدث البيانات';
          } else {
            setStats((live && live.message) || 'لا نتيجة.');
          }
          return live;
        } catch (err) {
          const lqBcNow = lightSanitizeBarcodeValue(
            (document.getElementById('locationBarcodeQueryInput') || {}).value || ''
          );
          const lqTextNow = ((document.getElementById('locationTextQueryInput') || {}).value || '').trim();
          if (gen !== locationSearchGen || !isCurrentLocationQuery(key, lqBcNow, lqTextNow)) return null;
          if (hadLocalRows) {
            setStats(`نتيجة محلية (${localMs != null ? localMs + 'ms' : 'فوري'}) · تعذّر التحقق من أحدث البيانات`);
            if (stEl) stEl.textContent = 'تعذّر التحقق من أحدث البيانات';
          } else if (err && err.name === 'AbortError') {
            setStats('انتهت المهلة أثناء الجلب الموجّه.');
          } else {
            setStats((err && err.message) || 'تعذّر الجلب.');
          }
          return null;
        } finally {
          locationLiveRefreshByKey.delete(key);
          locationLiveRefreshBusy = locationLiveRefreshByKey.size > 0;
        }
      })();
      locationLiveRefreshByKey.set(key, run);
      return run;
    }
    function formatLookupTiming(live, uiMs) {
      const t = (live && live.timing_ms) || {};
      const parts = [];
      if (uiMs != null) parts.push(`واجهة ${uiMs}ms`);
      if (t.db_ms != null) parts.push(`DB ${t.db_ms}ms`);
      if (t.auth_ms != null) parts.push(`Auth ${t.auth_ms}ms`);
      if (t.products_ms != null) parts.push(`products ${t.products_ms}ms`);
      if (t.catalog_import_ms != null && Number(t.catalog_import_ms) > 0) {
        parts.push(`استيراد ${t.catalog_import_ms}ms`);
      }
      if (t.products_bytes != null && t.products_bytes >= 0) {
        parts.push(`${Math.round(Number(t.products_bytes) / 1024)}KB`);
      }
      if (t.products_rows != null) parts.push(`${t.products_rows} صف`);
      if (live && live.request_count != null) parts.push(`${live.request_count} طلب`);
      if (t.save_ms != null) parts.push(`حفظ ${t.save_ms}ms`);
      if (t.total_ms != null) parts.push(`خادم ${t.total_ms}ms`);
      return parts.join(' · ');
    }
    async function fetchOneItemFromHesabate() {
      const lqText = document.getElementById('locationTextQueryInput').value.trim();
      let lqBc = lightSanitizeBarcodeValue(document.getElementById('locationBarcodeQueryInput').value);
      if (!lqText && !lqBc) {
        setStats('اكتب باركود أو اسم أو رقم من الملاحظات ثم اضغط «جلب صنف واحد».');
        return;
      }
      if (hesabateLookupInflight) {
        setStats('جلب جارٍ — انتظر...');
        return hesabateLookupInflight;
      }
      const oneBtn = document.getElementById('hesabateFetchOneBtn');
      const stEl = document.getElementById('hesabateSyncStatus');
      if (oneBtn) oneBtn.disabled = true;
      setStats('جاري جلب الصنف من حساباتي...');
      if (stEl) stEl.textContent = 'فحص محلي سريع...';
      const t0 = performance.now();
      const qBody = lqBc ? '' : lqText;
      hesabateLookupInflight = (async () => {
        try {
          // 1) محلي فوري
          let live = await lookupHesabateItemOnce(lqBc, qBody, true, undefined, false, false);
          let localRows = Array.isArray(live && live.rows) ? live.rows : [];
          if (localRows.length) {
            renderLocationRowsNow(localRows, `محلي فوري · ${localRows.length}`);
          }
          // 2) تحديث موجّه؛ إن كان صنفاً جديداً يستورد الكتالوج تلقائياً ثم يعيد المطابقة
          if (stEl) {
            stEl.textContent = localRows.length
              ? 'تحديث هذا الصنف من حساباتي...'
              : 'صنف جديد؟ جاري الجلب من حساباتي (قد يستغرق دقيقة)...';
          }
          if (!localRows.length) {
            setStats('جاري جلب الصنف الجديد من حساباتي...');
          }
          const ctrl = new AbortController();
          // استيراد كتالوج لصنف جديد قد يتجاوز دقيقة
          const to = setTimeout(() => ctrl.abort(), localRows.length ? 45000 : 280000);
          try {
            live = await lookupHesabateItemOnce(
              lqBc, qBody, false, ctrl.signal, true, !localRows.length
            );
          } finally {
            clearTimeout(to);
          }
          // إن بقي بلا نتيجة بعد التحديث الموجّه — جرّب استيراد مرة (صف محلي بلا pro_id)
          if (
            live && live.ok !== false
            && !(Array.isArray(live.rows) && live.rows.length)
            && localRows.length
            && (live.products_fetch_mode === 'local_only_no_pro_id'
              || live.products_fetch_mode === 'targeted_search_unsupported')
          ) {
            if (stEl) stEl.textContent = 'جاري ربط الصنف باستيراد من حساباتي...';
            setStats('جاري استيراد الصنف من حساباتي...');
            const ctrl2 = new AbortController();
            const to2 = setTimeout(() => ctrl2.abort(), 280000);
            try {
              live = await lookupHesabateItemOnce(lqBc, qBody, false, ctrl2.signal, true, true);
            } finally {
              clearTimeout(to2);
            }
          }
          const apiRows = Array.isArray(live && live.rows) ? live.rows : [];
          if (!live || live.ok === false) {
            const err = (live && live.message) || 'تعذر جلب الصنف.';
            if (localRows.length) {
              setStats(`نتيجة محلية · تعذّر التحقق: ${err}`);
            } else {
              setStats(err);
            }
            if (stEl) stEl.textContent = err;
            if (live && live.usage) renderHesabateUsage(live.usage, live.message || '');
            return;
          }
          applyHesabateSearchVersion(live);
          if (live.usage) renderHesabateUsage(live.usage, live.message || '');
          const uiMs = Math.round(performance.now() - t0);
          const timingLine = formatLookupTiming(live, uiMs);
          // المعرف الداخلي ما رجّع صنفاً من حساباتي — امسح الجدول ولا تعرض المحلي
          if (isMissingFromHesabate(live) || (!apiRows.length && live.not_found)) {
            const miss = 'هذا الصنف مش داخل على حساباتي';
            clearLocationTable(miss + (timingLine ? ` | ${timingLine}` : ''));
            if (stEl) stEl.textContent = miss;
            return;
          }
          const rows = apiRows.length ? apiRows : [];
          if (!rows.length) {
            // لا تستبدل رسالة الخادم دائماً بـ «مش داخل» — قد يكون فهرس/مزامنة
            let miss = live.message || 'لا يوجد صنف مطابق.';
            if (isMissingFromHesabate(live)) {
              miss = 'هذا الصنف مش داخل على حساباتي';
            } else if (
              live.products_fetch_mode === 'targeted_search_unsupported'
              || live.needs_manual_import
            ) {
              miss = live.message || 'الصنف غير موجود في الفهرس المحلي — اضغط «تحديث الكل الآن» ثم أعد الجلب.';
            }
            clearLocationTable(miss + (timingLine ? ` | ${timingLine}` : ''));
            if (stEl) stEl.textContent = miss;
            return;
          }
          const imported = !!(live.catalog_imported || live.products_fetch_mode === 'catalog_import_match');
          const n = renderLocationRowsNow(
            rows,
            `${live.message || (imported ? `تم جلب صنف جديد · ${rows.length}` : `تم جلب ${rows.length} صنف`)} | ${timingLine}`
          );
          if (stEl) stEl.textContent = live.message || `تم جلب ${n} صنف.`;
          scheduleSharedLocationAndPrintSync('location-fetch');
        } catch (err) {
          let hint = 'تعذر جلب الصنف.';
          if (err && err.name === 'AbortError') hint = 'انتهت المهلة — أعد المحاولة أو استخدم «تحديث الكل الآن».';
          else if (err && err.message) hint = String(err.message);
          setStats(hint);
          await refreshHesabateStatus();
        } finally {
          if (oneBtn) oneBtn.disabled = false;
          hesabateLookupInflight = null;
        }
      })();
      return hesabateLookupInflight;
    }

    function splitPrintQuery(raw) {
      // طباعة الباركود: باركود فقط — بدون بحث برقم الصنف/الاسم
      const t = lightSanitizeBarcodeValue(raw);
      if (!t) return { barcode: '', q: '' };
      return { barcode: t, q: '' };
    }
    function printBarcodeExactNeedle(raw) {
      // تطابق تام حرفاً بحرف بعد تطبيع الأرقام العربية فقط — بدون حذف أصفار بادئة
      const t = lightSanitizeBarcodeValue(raw);
      if (!t) return '';
      return String(t).replace(/[^A-Za-z0-9]/g, '').toUpperCase();
    }
    function printBarcodeMatchKeys(raw) {
      // نفس مفاتيح بحث المواقع (مدمج + جزء أول + أصفار) — باركود فقط
      if (typeof barcodeSearchKeysClient === 'function') {
        const keys = barcodeSearchKeysClient(raw) || [];
        if (keys.length) return keys;
      }
      const needle = printBarcodeExactNeedle(raw);
      return needle ? [needle] : [];
    }
    function locationRowMatchesPrintBarcode(row, rawBc) {
      const wantKeys = printBarcodeMatchKeys(rawBc);
      if (!wantKeys.length || !row) return false;
      const wantSet = new Set(wantKeys);
      const blobs = [row.barcode, row.unit_barcodes, row.normalized_barcode];
      for (let bi = 0; bi < blobs.length; bi++) {
        const cell = String(blobs[bi] == null ? '' : blobs[bi]).trim();
        if (!cell) continue;
        // خانة كاملة + أجزاؤها — نفس منطق فهرس المواقع
        const cellKeys = printBarcodeMatchKeys(cell);
        for (let ki = 0; ki < cellKeys.length; ki++) {
          if (wantSet.has(cellKeys[ki])) return true;
        }
        const parts = cell.split(/[\s,;|]+/).map((s) => s.trim()).filter(Boolean);
        const toks = parts.length ? parts : [cell];
        for (let i = 0; i < toks.length; i++) {
          const partKeys = printBarcodeMatchKeys(toks[i]);
          for (let pi = 0; pi < partKeys.length; pi++) {
            if (wantSet.has(partKeys[pi])) return true;
          }
        }
      }
      return false;
    }
    function filterRowsExactPrintBarcode(rows, rawBc) {
      const list = Array.isArray(rows) ? rows : [];
      if (!printBarcodeMatchKeys(rawBc).length) return [];
      return list.filter((r) => locationRowMatchesPrintBarcode(r, rawBc));
    }
    function splitOriginalTokens(raw) {
      // فواصل بين أرقام أصلية متعددة: مسافة/فاصلة/|/; فقط
      // لا نقطع على / أو + أو @ — تبقى جزءاً من الرقم كما هو
      return String(raw || '')
        .split(/[\s,|;]+/)
        .map((s) => s.trim())
        .filter(Boolean);
    }
    function isPrintOriginalMenuToken(tok) {
      // اقبل الرقم كما هو بما فيه @ و + و / — لا تحذف الرقم بسبب هذه الرموز
      const t = String(tok || '').trim();
      if (!t) return false;
      // تجاهل فواصل وحيدة بلا محتوى رقمي/حرفي
      if (!/[A-Za-z0-9\u0600-\u06FF]/.test(t)) return false;
      return true;
    }
    function sanitizePrintItemName(name) {
      // عند الطباعة: لا تطبع حرف @ داخل اسم الصنف
      return String(name == null ? '' : name).replace(/@/g, '');
    }
    function collectPrintOriginalOptions(rows) {
      const seen = new Set();
      const opts = [];
      (Array.isArray(rows) ? rows : []).forEach((r) => {
        splitOriginalTokens(r && r.original_number).forEach((tok) => {
          if (!isPrintOriginalMenuToken(tok)) return;
          const key = tok.toLowerCase();
          if (seen.has(key)) return;
          seen.add(key);
          opts.push(tok);
        });
      });
      return opts;
    }
    function pickPrintRowForOriginal(rows, original) {
      const list = Array.isArray(rows) ? rows : [];
      if (!list.length) return null;
      const want = String(original || '').trim().toLowerCase();
      if (!want) return list[0];
      const exact = list.find((r) => {
        const toks = splitOriginalTokens(r && r.original_number).map((t) => t.toLowerCase());
        return toks.includes(want) || String(r.original_number || '').trim().toLowerCase() === want;
      });
      return exact || list[0];
    }
    function setPrintFetchStatus(msg, side) {
      if (side === 'left' || side === 'right') {
        const sideEl = document.getElementById('printFetchStatus' + printSideCap(side));
        if (sideEl) sideEl.textContent = msg || '';
        return;
      }
      const el = document.getElementById('printFetchStatus');
      if (el) el.textContent = msg || '';
    }
    function printBarcodeExactValue(code) {
      // طباعة فقط: احذف \\r و \\n فقط — بلا trim/replace/ضغط فراغات.
      if (code == null) return '';
      const s = String(code);
      let out = '';
      for (let i = 0; i < s.length; i++) {
        const c = s.charCodeAt(i);
        if (c === 10 || c === 13) continue;
        out += s.charAt(i);
      }
      return out;
    }
    function printBarcodeScanValue(code) {
      // قيمة CODE128 للقراءة على الجهاز: اضغط مسافات التنسيق الطويلة إلى مسافة واحدة
      // (223112U001        HI → 223112U001 HI) حتى يبقى عرض الباركود قابلاً للقراءة
      const raw = printBarcodeExactValue(code);
      if (!raw) return '';
      let out = '';
      let prevSpace = false;
      for (let i = 0; i < raw.length; i++) {
        const ch = raw.charAt(i);
        if (ch === ' ' || ch === '\t') {
          if (!prevSpace) out += ' ';
          prevSpace = true;
        } else {
          out += ch;
          prevSpace = false;
        }
      }
      // trim يدوي بدون regex
      let a = 0;
      let b = out.length;
      while (a < b && out.charAt(a) === ' ') a++;
      while (b > a && out.charAt(b - 1) === ' ') b--;
      return out.slice(a, b);
    }
    function pickPrintBarcodeExactFromRow(row) {
      // خذ حقل barcode كما هو مع كل الفراغات؛ لا split (مسار البحث منفصل)
      const bc = printBarcodeExactValue(row && row.barcode != null ? row.barcode : '');
      if (/[A-Za-z0-9]/.test(bc)) return bc;
      const ub = printBarcodeExactValue(row && row.unit_barcodes != null ? row.unit_barcodes : '');
      if (/[A-Za-z0-9]/.test(ub)) return ub;
      return '';
    }
    function printBarcodeCompactValue(code) {
      // بدون مسافات نهائياً — أقصر CODE128 للقراءة على الطابعة الحرارية
      const raw = printBarcodeScanValue(code) || printBarcodeExactValue(code);
      if (!raw) return '';
      let out = '';
      for (let i = 0; i < raw.length; i++) {
        const ch = raw.charAt(i);
        if (ch !== ' ' && ch !== '\t') out += ch;
      }
      return out;
    }
    function printBarcodeEncodeValue(code) {
      // اطبع الباركود كما يظهر على الشاشة — مع كل الفراغات، بلا دمج ولا قص
      // مثال: "28130C5300        KA22" يبقى كما هو ولا يصبح "28130C5300KA22"
      return printBarcodeExactValue(code);
    }
    const PRINT_REAL_PX_PER_MM = 96 / 25.4;

    function renderPrintBarcodeCanvas(code, side) {
      if (typeof JsBarcode === 'undefined') return null;
      const value = printBarcodeEncodeValue(code);
      if (!value || !/[A-Za-z0-9]/.test(value)) return null;
      const cfg = PRINT_BARCODE_CFG;
      const dotsPerMm = cfg.dpi / 25.4;
      const s = getPrintLayoutSettings(side);
      const box = clampBarcodeBoxToLabelMm(side, {
        x: Number(s.bcXmm) + (Number(s.bcShiftXmm) || 0),
        y: Number(s.bcYmm) + (Number(s.bcShiftYmm) || 0),
        w: Number(s.bcWmm) > 0 ? Number(s.bcWmm) : PRINT_ZONE_CONTENT_W_MM,
        h: Number(s.bcHmm) > 0 ? Number(s.bcHmm) : PRINT_ZONE_BC_H_MM
      });
      const padMm = (cfg.fitPadMm != null) ? cfg.fitPadMm : 1.2;
      const capMm = (cfg.maxWidthMm != null) ? cfg.maxWidthMm : 48;
      const maxWmm = Math.min(capMm, Math.max(22, box.w - padMm * 2));
      const availDots = Math.floor(maxWmm * dotsPerMm);

      let probe = null;
      const c0 = document.createElement('canvas');
      ['CODE128', 'CODE128B'].forEach((fmt) => {
        if (probe) return;
        try {
          JsBarcode(c0, value, { format: fmt, width: 1, height: 8, margin: 0, displayValue: false });
          probe = { mods: c0.width, format: fmt };
        } catch (_) {}
      });
      if (!probe || !probe.mods) return null;

      const tries = [
        { d: 2, q: cfg.quietModulesEach },
        { d: 2, q: cfg.quietModulesMin },
        { d: 2, q: 4 }
      ];
      let pick = null;
      for (let i = 0; i < tries.length; i++) {
        if ((probe.mods + tries[i].q * 2) * tries[i].d <= availDots) { pick = tries[i]; break; }
      }
      let unreliable = false;
      if (!pick) { pick = { d: 2, q: 4 }; }

      const textBandMm = 3.2;
      let barHmm = Math.min(cfg.barHMaxMm, box.h - textBandMm);
      barHmm = Math.max(8, barHmm);
      const heightDots = Math.max(48, Math.round(barHmm * dotsPerMm));
      const quietPx = pick.q * pick.d;

      const canvas = document.createElement('canvas');
      try {
        JsBarcode(canvas, value, {
          format: probe.format, width: pick.d, height: heightDots, margin: 0,
          marginLeft: quietPx, marginRight: quietPx, marginTop: 0, marginBottom: 0,
          displayValue: false, background: '#ffffff', lineColor: '#000000'
        });
      } catch (_) { return null; }
      if (!canvas.width) return null;

      let wMm = canvas.width / dotsPerMm;
      let hMm = canvas.height / dotsPerMm;
      if (wMm > maxWmm + 0.08) {
        const k = maxWmm / wMm;
        wMm = maxWmm;
        hMm = hMm * k;
        if (pick.d * k < 1.5) unreliable = true;
      }
      if (wMm > box.w + 0.08) unreliable = true;
      return { canvas: canvas, value: value, moduleDots: pick.d, wMm: wMm, hMm: hMm,
               availMm: box.w, unreliable: unreliable, textBandMm: textBandMm };
    }
    function renderPrintBarcodeSvg(svgEl, code, opts) {
      if (!svgEl || typeof JsBarcode === 'undefined') return false;
      // القيمة كما في المصدر مع كل الفراغات — لا دمج/حذف مسافات
      const value = printBarcodeExactValue(code);
      if (!value || !/[A-Za-z0-9]/.test(value)) {
        svgEl.innerHTML = '';
        return false;
      }
      try {
        const conf = Object.assign({
          format: 'CODE128',
          displayValue: false,
          margin: 0,
          width: 2,
          height: 56,
          background: '#ffffff',
          lineColor: '#000000'
        }, opts || {});
        conf.format = 'CODE128';
        conf.displayValue = false;
        JsBarcode(svgEl, value, conf);
        return true;
      } catch (_) {
        try {
          const conf2 = Object.assign({
            format: 'CODE128B',
            displayValue: false,
            margin: 0,
            width: 2,
            height: 56,
            background: '#ffffff',
            lineColor: '#000000'
          }, opts || {});
          conf2.format = 'CODE128B';
          conf2.displayValue = false;
          JsBarcode(svgEl, value, conf2);
          return true;
        } catch (__) {
          svgEl.innerHTML = '';
          return false;
        }
      }
    }
    function clearPrintBarcodeWarn(wrap) {
      if (!wrap) return;
      wrap.classList.remove('is-bc-unreliable');
      wrap.querySelectorAll('.pl-bc-warn').forEach((n) => n.remove());
    }
    function showPrintBarcodeWarn(wrap, msg) {
      if (!wrap) return;
      wrap.classList.add('is-bc-unreliable');
      let w = wrap.querySelector('.pl-bc-warn');
      if (!w) {
        w = document.createElement('div');
        w.className = 'pl-bc-warn';
        wrap.appendChild(w);
      }
      w.textContent = msg || '';
    }
    async function fitPrintBarcodeDynamic(targetEl, code, opts) {
      if (!targetEl) return false;
      const raw = printBarcodeExactValue(code);
      if (!raw || !/[A-Za-z0-9]/.test(raw)) return false;
      const inLabel = !!(targetEl.closest && targetEl.closest('.print-label'));
      const wrap = targetEl.closest ? targetEl.closest('.pl-bc-wrap') : null;
      clearPrintBarcodeWarn(wrap);
      const layoutSide = printLayoutSideFromEl(targetEl);
      const labelEl = targetEl.closest && targetEl.closest('.print-label');
      try { await ensureJsBarcodeLib(); } catch (_) { return false; }

      if (!inLabel) {
        const svgEl = targetEl.tagName && targetEl.tagName.toLowerCase() === 'svg' ? targetEl : null;
        if (!svgEl) return false;
        return renderPrintBarcodeSvg(svgEl, printBarcodeEncodeValue(code),
          Object.assign({ height: 40, width: 2, margin: 10 }, opts || {}));
      }

      const r = renderPrintBarcodeCanvas(code, layoutSide);
      if (!r) return false;

      const img = document.createElement('img');
      img.className = 'pl-bc';
      img.alt = r.value;
      img.title = 'Code128 X=' + r.moduleDots + 'dot · ' + r.wMm.toFixed(2) + 'x' + r.hMm.toFixed(2) + 'mm';
      img.src = r.canvas.toDataURL('image/png');
      img.removeAttribute('width');
      img.removeAttribute('height');
      img.style.width = r.wMm.toFixed(4) + 'mm';
      img.style.height = r.hMm.toFixed(4) + 'mm';
      img.style.maxWidth = 'none';
      img.style.maxHeight = 'none';
      img.style.display = 'block';
      img.style.margin = '0 auto';
      img.style.flex = '0 0 ' + r.hMm.toFixed(4) + 'mm';
      img.style.objectFit = 'none';
      img.style.imageRendering = 'pixelated';
      img.style.direction = 'ltr';
      targetEl.replaceWith(img);

      if (wrap) {
        wrap.style.justifyContent = 'flex-start';
        const txt = wrap.querySelector('.pl-bc-text');
        if (txt) {
          txt.style.flex = '0 0 ' + r.textBandMm + 'mm';
          txt.style.height = r.textBandMm + 'mm';
          txt.style.minHeight = r.textBandMm + 'mm';
        }
      }
      if (r.unreliable) {
        if (labelEl) labelEl.dataset.bcUnreliable = '1';
        showPrintBarcodeWarn(wrap, 'الباركود طويل جداً على عرض الخانة — وسّع خانة الباركود في GUI');
        try {
          setPrintFetchStatus('تحذير: الباركود ' + r.wMm.toFixed(1) + 'مم داخل ' + r.availMm.toFixed(1) + 'مم', layoutSide);
        } catch (_) {}
      } else {
        if (labelEl) labelEl.dataset.bcUnreliable = '0';
        try {
          setPrintFetchStatus('Code128 · X=' + r.moduleDots + ' نقطة · ' + r.wMm.toFixed(1) + 'x' + r.hMm.toFixed(1) + 'مم · بدون قص', layoutSide);
        } catch (_) {}
      }
      return true;
    }
    function fitPrintBarcodeText(el, code) {
      if (!el) return;
      const display = printBarcodeEncodeValue(code);
      el.textContent = display;
      el.style.display = 'block';
      el.style.whiteSpace = 'pre';
      el.style.fontFamily = '"Cairo", "Tahoma", "Segoe UI", Arial, sans-serif';
      el.style.fontWeight = '900';
      el.style.color = '#000';
      el.style.background = '#fff';
      el.style.lineHeight = '1';
      el.style.flex = '0 0 3.2mm';
      el.style.minHeight = '3.2mm';
      el.style.height = '3.2mm';
      el.style.width = '100%';
      el.style.position = 'relative';
      el.style.zIndex = '3';
      el.style.overflow = 'hidden';
      el.style.textOverflow = 'clip';
      const n = display.length;
      el.style.letterSpacing = n > 28 ? '0' : (n > 20 ? '0.02em' : '0.04em');
      el.style.fontSize = '9px';
      el.style.fontWeight = '900';
    }
    function toEnglishDigits(raw) {
      return String(raw == null ? '' : raw)
        .replace(/[٠-٩]/g, (d) => String('٠١٢٣٤٥٦٧٨٩'.indexOf(d)))
        .replace(/[۰-۹]/g, (d) => String('۰۱۲۳۴۵۶۷۸۹'.indexOf(d)));
    }
    function normalizePrintQtyValue(raw) {
      const digits = toEnglishDigits(raw).replace(/[^\d]/g, '');
      if (!digits) return '';
      const n = parseInt(digits, 10);
      if (!Number.isFinite(n)) return '';
      return String(Math.max(1, Math.min(200, n)));
    }
    function onPrintQtyInputChange() {
      const el = document.getElementById('printQtyInput');
      if (!el) return;
      const cur = String(el.value || '');
      const norm = normalizePrintQtyValue(cur);
      // اكتب دائماً أرقاماً إنجليزية 0-9
      if (norm !== cur) {
        const start = el.selectionStart;
        el.value = norm;
        try {
          if (typeof start === 'number') {
            const pos = Math.min(norm.length, start);
            el.setSelectionRange(pos, pos);
          }
        } catch (_) {}
      }
      updatePrintDualPreviewSlots();
      scheduleSavePrintFormState();
    }
    function getPrintQty() {
      const el = document.getElementById('printQtyInput');
      const norm = normalizePrintQtyValue(el ? el.value : '1');
      const qtyRaw = Number(norm || 1);
      return Math.max(1, Math.min(200, Number.isFinite(qtyRaw) ? Math.floor(qtyRaw) : 1));
    }
    function updatePrintQtyHint() {
      const qty = getPrintQty();
      // كل «كمية» = ورقة كاملة بلزقتين معاً (يسار + يمين)
      const pages = qty;
      const hint = document.getElementById('printQtyHint');
      const qtyEn = String(qty);
      if (hint) {
        hint.textContent = qty === 1
          ? 'ورقة واحدة = لزق يسار + لزق يمين معاً'
          : (qtyEn + ' ورقات · كل ورقة لزقتين معاً (يسار + يمين)');
      }
      return { qty, pages };
    }
    function getPrintName(side) {
      const id = 'printName' + printSideCap(side);
      return sanitizePrintItemName(((document.getElementById(id) || {}).value || '').trim());
    }
    function getPrintNameLeft() { return getPrintName('left'); }
    function getPrintNameRight() { return getPrintName('right'); }
    function setPrintName(side, name) {
      const el = document.getElementById('printName' + printSideCap(side));
      if (el) {
        el.value = sanitizePrintItemName(String(name || '').trim());
        el.dataset.fromFetch = '1';
      }
      scheduleSavePrintFormState();
    }
    function collectPrintFormState() {
      const sideState = (side) => {
        const cap = printSideCap(side);
        const q = document.getElementById('printQuery' + cap);
        const n = document.getElementById('printName' + cap);
        const sel = document.getElementById('printOriginalSelect' + cap);
        return {
          query: q ? String(q.value || '') : '',
          name: n ? String(n.value || '') : '',
          fromFetch: n && n.dataset.fromFetch === '1' ? '1' : '0',
          original: sel ? String(sel.value || '') : ''
        };
      };
      const qtyEl = document.getElementById('printQtyInput');
      return {
        left: sideState('left'),
        right: sideState('right'),
        qty: qtyEl ? String(qtyEl.value || '1') : '1',
        savedAt: Date.now()
      };
    }
    function savePrintFormState() {
      try {
        localStorage.setItem(PRINT_FORM_STORAGE_KEY, JSON.stringify(collectPrintFormState()));
      } catch (_) {}
    }
    function scheduleSavePrintFormState() {
      if (printFormRestoreBusy) return;
      clearTimeout(printFormSaveTimer);
      printFormSaveTimer = setTimeout(savePrintFormState, 200);
    }
    async function restorePrintFormState() {
      if (printFormRestoreBusy) return;
      let data = null;
      try {
        const raw = localStorage.getItem(PRINT_FORM_STORAGE_KEY);
        data = raw ? JSON.parse(raw) : null;
      } catch (_) {
        data = null;
      }
      if (!data || typeof data !== 'object') return;
      printFormRestoreBusy = true;
      try {
        const qtyEl = document.getElementById('printQtyInput');
        if (qtyEl && data.qty != null && String(data.qty).trim()) {
          qtyEl.value = normalizePrintQtyValue(data.qty) || '1';
        }
        for (const side of ['left', 'right']) {
          const st = data[side] || {};
          const cap = printSideCap(side);
          const qEl = document.getElementById('printQuery' + cap);
          const nEl = document.getElementById('printName' + cap);
          if (qEl && st.query != null) qEl.value = String(st.query);
          if (nEl) {
            if (st.name != null) nEl.value = String(st.name);
            nEl.dataset.fromFetch = st.fromFetch === '1' ? '1' : '0';
          }
        }
        for (const side of ['left', 'right']) {
          const st = data[side] || {};
          const q = String(st.query || '').trim();
          if (!q) continue;
          const ok = await resolvePrintItemFromLocal(side, false);
          if (ok && st.original) {
            const sel = document.getElementById('printOriginalSelect' + printSideCap(side));
            if (sel) {
              const opts = Array.from(sel.options).map((o) => o.value);
              if (opts.indexOf(st.original) >= 0) {
                sel.value = st.original;
                updatePrintBarcodePreview(side, false);
              }
            }
          }
          if (st.name != null && String(st.name).trim()) {
            const nEl = document.getElementById('printName' + printSideCap(side));
            if (nEl) {
              nEl.value = String(st.name);
              nEl.dataset.fromFetch = st.fromFetch === '1' ? '1' : '0';
              refreshPrintNamePreview(side);
            }
          }
        }
        updatePrintDualPreviewSlots();
        savePrintFormState();
      } finally {
        printFormRestoreBusy = false;
      }
    }
    function refreshPrintNamePreview(side) {
      const sides = side ? [printSideKey(side)] : ['left', 'right'];
      sides.forEach((s) => {
        const nameEl = document.getElementById('printPvName' + printSideCap(s));
        if (nameEl) nameEl.textContent = getPrintName(s) || '—';
      });
      scheduleSavePrintFormState();
    }
    function updatePrintDualPreviewSlots() {
      updatePrintQtyHint();
      const left = document.getElementById('printPreviewLeft');
      const right = document.getElementById('printPreviewRight');
      if (left) left.classList.remove('empty-slot');
      if (right) right.classList.remove('empty-slot');
      refreshPrintNamePreview();
    }
    function clearPrintSidePreview(side) {
      const s = printSideKey(side);
      const st = printSides[s];
      st.item = null;
      st.rows = [];
      const card = document.getElementById('printPreview' + printSideCap(s));
      if (card) card.style.display = 'none';
      const mock = document.getElementById('printLabelMock' + printSideCap(s));
      if (mock) mock.innerHTML = '';
      const wrap = document.getElementById('printOriginalWrap' + printSideCap(s));
      if (wrap) wrap.style.display = 'none';
    }
    function renderPrintSideLabelMock(side) {
      const s = printSideKey(side);
      const cap = printSideCap(s);
      const card = document.getElementById('printPreview' + cap);
      const host = document.getElementById('printLabelMock' + cap);
      if (!card || !host) return;
      const payload = (typeof buildPrintPayloadForSide === 'function')
        ? buildPrintPayloadForSide(s)
        : null;
      const ok = !!(payload && !payload.error && payload.barcode && /[A-Za-z0-9]/.test(String(payload.barcode)));
      if (!ok) {
        card.style.display = 'none';
        host.innerHTML = '';
        return;
      }
      card.style.display = 'block';
      host.innerHTML = '';
      try { applyPrintGeometryCss(getPrintGeometryMm()); } catch (_) {}
      const label = buildPrintLabelEl(payload, s);
      host.appendChild(label);
      const paint = async () => {
        applyPrintLayoutToLabel(label);
        fitPrintNameDynamic(label.querySelector('.pl-name'));
        fitPrintMetaLeft(label.querySelector('.pl-orig'));
        fitPrintMetaLeft(label.querySelector('.pl-loc'));
        fitPrintBarcodeText(label.querySelector('.pl-bc-text'), payload.barcode);
        const bcEl = label.querySelector('.pl-bc');
        if (bcEl) await fitPrintBarcodeDynamic(bcEl, payload.barcode, { height: 38 });
        applyPrintLayoutToLabel(label);
      };
      ensureJsBarcodeLib().then(paint).catch(paint);
    }
    function updatePrintBarcodePreview(side, forceNameFromRow) {
      const s = printSideKey(side);
      const cap = printSideCap(s);
      const st = printSides[s];
      const card = document.getElementById('printPreview' + cap);
      const sel = document.getElementById('printOriginalSelect' + cap);
      const chosen = sel && sel.value ? sel.value : '';
      const row = pickPrintRowForOriginal(st.rows, chosen) || st.item;
      if (!row) {
        if (card) card.style.display = 'none';
        const mock = document.getElementById('printLabelMock' + cap);
        if (mock) mock.innerHTML = '';
        return;
      }
      st.item = row;
      const nameInp = document.getElementById('printName' + cap);
      const name = sanitizePrintItemName(String(row.item_name || '').trim() || '');
      if (nameInp && (forceNameFromRow || !nameInp.value.trim() || nameInp.dataset.fromFetch === '1')) {
        setPrintName(s, name);
      }
      let original = chosen || '';
      if (!original || !isPrintOriginalMenuToken(original)) {
        const rawOrig = String(row.original_number || '').trim();
        const okTok = splitOriginalTokens(rawOrig).find(isPrintOriginalMenuToken);
        original = okTok || rawOrig || '';
      }
      if (!original) original = '—';
      const location = String(row.location || '').trim() || '—';
      const barcode = pickPrintBarcodeExactFromRow(row);
      const origEl = document.getElementById('printPvOriginal' + cap);
      const locEl = document.getElementById('printPvLocation' + cap);
      const textEl = document.getElementById('printPvBarcodeText' + cap);
      const namePv = document.getElementById('printPvName' + cap);
      if (origEl) origEl.textContent = 'الأصلي: ' + original;
      if (locEl) locEl.textContent = 'الموقع: ' + location;
      if (namePv) namePv.textContent = getPrintName(s) || name || '—';
      if (textEl) {
        fitPrintBarcodeText(textEl, barcode);
        if (!barcode) textEl.textContent = '—';
      }
      updatePrintDualPreviewSlots();
      renderPrintSideLabelMock(s);
      try {
        const des = document.getElementById('printLayoutDesigner');
        if (des && des.open) refreshPrintDesignerBoard();
      } catch (_) {}
    }
    function applyPrintLookupRows(side, rows, message, forceNameFromRow) {
      const s = printSideKey(side);
      const cap = printSideCap(s);
      const st = printSides[s];
      st.rows = Array.isArray(rows) ? rows.slice() : [];
      const originals = collectPrintOriginalOptions(st.rows);
      const wrap = document.getElementById('printOriginalWrap' + cap);
      const sel = document.getElementById('printOriginalSelect' + cap);
      const prevChosen = sel && sel.value ? sel.value : '';
      if (sel) {
        sel.innerHTML = '';
        originals.forEach((o) => {
          const opt = document.createElement('option');
          opt.value = o;
          opt.textContent = o;
          sel.appendChild(opt);
        });
        if (prevChosen && originals.some((o) => o === prevChosen)) {
          sel.value = prevChosen;
        }
        sel.onchange = () => updatePrintBarcodePreview(s, false);
      }
      // اختيار الرقم الأصلي يظهر دائماً بكل لزقة (يسار/يمين) بعد الجلب
      if (wrap) wrap.style.display = st.rows.length ? 'block' : 'none';
      if (!st.rows.length) {
        st.item = null;
        const card = document.getElementById('printPreview' + cap);
        if (card) card.style.display = 'none';
        setPrintFetchStatus(message || 'غير موجود في قاعدة المواقع', s);
        return false;
      }
      if (sel && !originals.length) {
        const opt = document.createElement('option');
        opt.value = '';
        opt.textContent = '— بدون أصلي —';
        sel.appendChild(opt);
      }
      st.item = pickPrintRowForOriginal(st.rows, (sel && sel.value) || originals[0] || '');
      setPrintFetchStatus(message || `تم تجهيز الصنف · ${st.rows.length}`, s);
      updatePrintBarcodePreview(s, !!forceNameFromRow);
      scheduleSavePrintFormState();
      try {
        const des = document.getElementById('printLayoutDesigner');
        if (des && des.open) refreshPrintDesignerBoard();
      } catch (_) {}
      return true;
    }
    function rankLocationRowsForPrint(rawRows) {
      const ranked = (Array.isArray(rawRows) ? rawRows : []).slice().sort((a, b) => {
        const aApi = String(a.source_file || '') === 'حساباتي_API' ? 0 : 1;
        const bApi = String(b.source_file || '') === 'حساباتي_API' ? 0 : 1;
        if (aApi !== bApi) return aApi - bApi;
        return Number(b.id || 0) - Number(a.id || 0);
      });
      const seen = new Set();
      const out = [];
      ranked.forEach((r) => {
        const pid = String(r.hesabate_product_id || r.product_id || '').trim();
        const k = pid
          || String(r.barcode || r.original_number || r.item_name || '').trim().toLowerCase();
        if (k && seen.has(k)) return;
        if (k) seen.add(k);
        out.push(r);
      });
      return out;
    }
    async function searchLocationsLocal(paramsObj) {
      const params = new URLSearchParams(paramsObj || {});
      params.set('_', String(Date.now()));
      const res = await fetch(`/search_locations?${params.toString()}`, {
        method: 'GET',
        cache: 'no-store',
        headers: { Accept: 'application/json', 'Cache-Control': 'no-cache' }
      });
      const data = await readJsonSafe(res);
      if (!res.ok) throw new Error((data && data.detail) || `search-locations-${res.status}`);
      return Array.isArray(data && data.rows) ? data.rows : [];
    }
    async function lookupLocalLocationsForPrint(raw) {
      const t = lightSanitizeBarcodeValue(raw);
      if (!t) return [];
      const { barcode } = splitPrintQuery(t);
      if (!barcode || !printBarcodeMatchKeys(barcode).length) return [];
      // نفس /search_locations تبع تبويب المواقع — بدون barcode_exact
      // (الفلتر بعدين يبقى على حقول الباركود فقط حتى لا يطلع صنف غلط)
      const p = { q_barcode: barcode, limit: '40' };
      let rows = await searchLocationsLocal(p);
      rows = filterRowsExactPrintBarcode(rows, barcode);
      return rankLocationRowsForPrint(rows);
    }
    async function resolvePrintItemFromLocal(side, showEmptyMsg) {
      const s = printSideKey(side);
      const st = printSides[s];
      const raw = ((document.getElementById('printQuery' + printSideCap(s)) || {}).value || '').trim();
      if (!raw) {
        if (showEmptyMsg) setPrintFetchStatus('اكتب الباركود.', s);
        return false;
      }
      const gen = ++st.lookupGen;
      setPrintFetchStatus('بحث في قاعدة المواقع...', s);
      try {
        const rows = await lookupLocalLocationsForPrint(raw);
        if (gen !== st.lookupGen) return false;
        if (rows.length) {
          return applyPrintLookupRows(s, rows, `من قاعدة المواقع · ${rows.length}`);
        }
        clearPrintSidePreview(s);
        if (showEmptyMsg) {
          setPrintFetchStatus('مش موجود بقاعدة المواقع — اضغط «جلب» من حساباتي.', s);
        } else {
          setPrintFetchStatus('', s);
        }
        return false;
      } catch (err) {
        if (gen !== st.lookupGen) return false;
        if (showEmptyMsg) {
          setPrintFetchStatus((err && err.message) || 'تعذر البحث في قاعدة المواقع.', s);
        }
        return false;
      }
    }
    function schedulePrintLocalLookup(side) {
      if (printFormRestoreBusy) return;
      const s = printSideKey(side);
      const st = printSides[s];
      clearTimeout(st.debounce);
      scheduleSavePrintFormState();
      st.debounce = setTimeout(() => {
        const raw = ((document.getElementById('printQuery' + printSideCap(s)) || {}).value || '').trim();
        if (!raw || raw.length < 2) {
          clearPrintSidePreview(s);
          setPrintFetchStatus('', s);
          scheduleSavePrintFormState();
          return;
        }
        resolvePrintItemFromLocal(s, false);
      }, 280);
    }
    async function fetchItemForBarcodePrint(side) {
      const s = printSideKey(side);
      const raw = ((document.getElementById('printQuery' + printSideCap(s)) || {}).value || '').trim();
      if (!raw) {
        setPrintFetchStatus('اكتب الباركود ثم اضغط جلب.', s);
        return;
      }
      const btn = document.getElementById('printFetchBtn' + printSideCap(s));
      if (btn) btn.disabled = true;
      try {
        const { barcode } = splitPrintQuery(raw);
        const needle = printBarcodeExactNeedle(barcode);
        if (!needle) {
          clearPrintSidePreview(s);
          applyPrintLookupRows(s, [], 'الباركود غير صالح.');
          return;
        }
        const t0 = performance.now();
        const missingMsg = 'هذا الباركود مش موجود بقاعدة المواقع ولا حساباتي.';
        // 1) محلي أولاً — نفس بيانات تبويب المواقع
        setPrintFetchStatus('بحث في قاعدة المواقع...', s);
        let localRows = await lookupLocalLocationsForPrint(raw);
        if (localRows.length) {
          applyPrintLookupRows(s, localRows, `من قاعدة المواقع · ${localRows.length}`, false);
        } else {
          clearPrintSidePreview(s);
        }
        // 2) حساباتي للتحديث — بدون مسح النتيجة المحلية إن فشل
        setPrintFetchStatus(
          localRows.length ? 'تحديث من حساباتي...' : 'غير موجود محلياً — جلب من حساباتي...',
          s
        );
        const ctrl = new AbortController();
        const to = setTimeout(() => ctrl.abort(), 45000);
        let live = null;
        try {
          // نفس مفاتيح بحث المواقع (barcode_exact=false)
          live = await lookupHesabateItemOnce(barcode, '', false, ctrl.signal, true, false, false);
        } finally {
          clearTimeout(to);
        }
        let apiRows = filterRowsExactPrintBarcode(
          Array.isArray(live && live.rows) ? live.rows : [],
          barcode
        );
        const uiMs = Math.round(performance.now() - t0);
        const timingLine = formatLookupTiming(live, uiMs);
        if (!live || live.ok === false) {
          if (localRows.length) {
            applyPrintLookupRows(
              s,
              localRows,
              `من قاعدة المواقع · ${(live && live.message) || 'تعذر التحقق من حساباتي'}`
                + (timingLine ? ` | ${timingLine}` : ''),
              false
            );
            return;
          }
          clearPrintSidePreview(s);
          applyPrintLookupRows(s, [], (live && live.message) || 'تعذر جلب الصنف.');
          return;
        }
        applyHesabateUsageFromResponse(live, live.message || '');
        applyHesabateSearchVersion(live);
        if (!apiRows.length || isMissingFromHesabate(live) || live.not_found) {
          if (localRows.length) {
            applyPrintLookupRows(
              s,
              localRows,
              `من قاعدة المواقع · ${localRows.length}` + (timingLine ? ` | ${timingLine}` : ''),
              false
            );
            return;
          }
          clearPrintSidePreview(s);
          applyPrintLookupRows(s, [], missingMsg + (timingLine ? ` | ${timingLine}` : ''));
          return;
        }
        applyPrintLookupRows(
          s,
          rankLocationRowsForPrint(apiRows),
          `${live.message || ('تم · باركود ' + needle)} | ${timingLine}`,
          true
        );
        scheduleSharedLocationAndPrintSync('print-fetch');
      } catch (err) {
        let hint = 'تعذر جلب الصنف.';
        if (err && err.name === 'AbortError') hint = 'انتهت المهلة — أعد المحاولة.';
        else if (err && err.message) hint = String(err.message);
        try {
          const kept = await lookupLocalLocationsForPrint(raw);
          if (kept.length) {
            applyPrintLookupRows(s, kept, `من قاعدة المواقع · ${hint}`, false);
            return;
          }
        } catch (_) {}
        clearPrintSidePreview(s);
        applyPrintLookupRows(s, [], hint);
        try { refreshHesabateStatus(); } catch (_) {}
      } finally {
        if (btn) btn.disabled = false;
      }
    }
    function buildPrintPayloadForSide(side) {
      const s = printSideKey(side);
      const cap = printSideCap(s);
      const st = printSides[s];
      const sel = document.getElementById('printOriginalSelect' + cap);
      const chosen = sel && sel.value ? sel.value : '';
      const row = pickPrintRowForOriginal(st.rows, chosen) || st.item;
      if (!row) return null;
      const name = getPrintName(s)
        || sanitizePrintItemName(String(row.item_name || '').trim())
        || '—';
      // الأصلي على اللزقة كما هو (بما فيه @ و + و /)
      let original = chosen || '';
      if (!original || !isPrintOriginalMenuToken(original)) {
        const rawOrig = String(row.original_number || '').trim();
        const okTok = splitOriginalTokens(rawOrig).find(isPrintOriginalMenuToken);
        original = okTok || rawOrig || '';
      }
      if (!original) original = '—';
      const location = String(row.location || '').trim() || '—';
      const barcode = pickPrintBarcodeExactFromRow(row);
      if (!barcode || !/[A-Za-z0-9]/.test(barcode)) return { error: 'no-barcode', side: s };
      return { name, original, location, barcode };
    }
    const PRINT_SHEET_MM = {
      // الليبل الحامل (يمسك اللزقتين): 105×28 مم
      // هامش 1 مم من كل الجهات بين الحامل واللزقة
      // لزقتان 50×26 · فجوة 3 مم · (1+50+3+50+1 = 105) · (1+26+1 = 28)
      sheetW: 105,
      sheetH: 28,
      margin: 1,
      gap: 3,
      labelW: 50,
      labelH: 26,
      padY: 0,
      contentH: 26,
      rightContentShiftXmm: 0
    };
    /** إعدادات الباركود — Rongta 203dpi · لزقة 50×26 · بدون ضغط CSS */
    const PRINT_BARCODE_CFG = {
      dpi: 203,
      labelWmm: PRINT_SHEET_MM.labelW,
      labelHmm: PRINT_SHEET_MM.labelH,
      marginMm: 1.0,
      minModuleMm: 0.25, // 2 نقاط @203dpi — حد المسح الموثوق
      maxModuleMm: 0.375, // 3 نقاط
      minModuleDots: 2,
      absoluteMinModuleDots: 1,
      maxModuleDots: 2,
      barHMinMm: 8,
      barHMaxMm: 11.5,
      quietModulesEach: 10,
      quietModulesMin: 6,
      fitPadMm: 1.2,
      maxWidthMm: 48,
      stripSpaces: false,
      textReserveMm: 2.4
    };
    function code128SymbolModulesCount(text) {
      const s = String(text || '');
      if (!s) return 11 * 2 + 13;
      let digitsOnly = true;
      for (let i = 0; i < s.length; i++) {
        if (s.charCodeAt(i) < 48 || s.charCodeAt(i) > 57) { digitsOnly = false; break; }
      }
      if (digitsOnly) {
        const dataSymbols = Math.floor(s.length / 2) + (s.length % 2);
        return 11 * (dataSymbols + 2) + 13;
      }
      return 11 * (s.length + 2) + 13;
    }
    function code128ModulesCount(n) {
      const nn = Math.max(0, Number(n) || 0);
      return 11 * (nn + 2) + 13 + 20;
    }
    function pickModuleDotsForAvail(totalMods, availDots, cfg) {
      const maxD = (cfg && cfg.maxModuleDots) || 3;
      const absMin = (cfg && cfg.absoluteMinModuleDots) || 1;
      for (let d = maxD; d >= absMin; d--) {
        if (totalMods * d <= availDots) return d;
      }
      return 0;
    }
    function computePrintBarcodeFit(value, opts) {
      const cfg = PRINT_BARCODE_CFG;
      const lw = (opts && opts.labelWmm) || cfg.labelWmm;
      const margin = (opts && opts.marginMm != null) ? opts.marginMm : cfg.marginMm;
      const text = printBarcodeExactValue(value);
      const n = text.length;
      const quiet = cfg.quietModulesEach;
      const modules = code128SymbolModulesCount(text) + quiet * 2;
      const availW = (opts && opts.availableMm != null)
        ? opts.availableMm
        : ((typeof PRINT_ZONE_CONTENT_W_MM === 'number') ? PRINT_ZONE_CONTENT_W_MM : (lw - 2 * margin));
      const dotsPerMm = cfg.dpi / 25.4;
      const availDots = Math.floor(availW * dotsPerMm);
      if (!text || !/[A-Za-z0-9]/.test(text)) {
        return {
          ok: false, mode: 'fail', value: text, n: n, modules: modules,
          moduleMm: 0, moduleDots: 0, barHmm: 0, symbolWmm: 0, symbolHmm: 0,
          availableMm: availW, warning: 'قيمة باركود غير صالحة للطباعة', scale: 1
        };
      }
      const moduleDots = pickModuleDotsForAvail(modules, availDots, cfg);
      if (!moduleDots) {
        return {
          ok: false, mode: 'fail', value: text, n: n, modules: modules,
          moduleMm: 0, moduleDots: 0, barHmm: 0, symbolWmm: 0, symbolHmm: 0,
          availableMm: availW, scale: 1,
          warning: 'لا يتسع حتى عند X=1 نقطة — وسّع منطقة الباركود. لن يُضغط كصورة.'
        };
      }
      const moduleMm = moduleDots / dotsPerMm;
      let warning = '';
      if (moduleDots < cfg.minModuleDots) {
        warning = 'X=' + moduleDots + ' نقطة (أقل من الحد الموثوق ' + cfg.minModuleDots + ') — بدون ضغط صورة';
      }
      return {
        ok: true, mode: 'horizontal', value: text, n: n, modules: modules,
        moduleMm: moduleMm, moduleDots: moduleDots,
        barHmm: cfg.barHMinMm, symbolWmm: modules * moduleMm, symbolHmm: cfg.barHMinMm,
        availableMm: availW, warning: warning, scale: 1
      };
    }
    // مقياس شاشة — حامل 105×28 مم
    const PRINT_DESIGNER_PX_PER_MM = 6.5;
    const PRINT_CONTENT_W_MM = PRINT_SHEET_MM.labelW;
    const PRINT_CONTENT_H_MM = PRINT_SHEET_MM.labelH;
    /*
     * حامل الليبل 105×28 مم · هامش 1 مم من كل الجهات
     * كل لزقة 50×26 مم · فجوة 3 مم
     * داخل اللزقة: هامش محتوى 1 مم يمين/يسار · اسم 9 مم · أصلي+موقع 2 مم · باركود الباقي (15 مم)
     */
    const PRINT_ZONE_INSET_MM = 1;
    const PRINT_ZONE_OUTER_EDGE_MM = 1;
    const PRINT_ZONE_NAME_H_MM = 9;
    const PRINT_ZONE_META_H_MM = 2;
    const PRINT_ZONE_CONTENT_W_MM = PRINT_SHEET_MM.labelW - 2 * PRINT_ZONE_INSET_MM; // 48
    const PRINT_ZONE_BC_H_MM = PRINT_SHEET_MM.labelH - PRINT_ZONE_NAME_H_MM - PRINT_ZONE_META_H_MM; // 15
    const PRINT_ZONE_META_W_MM = PRINT_ZONE_CONTENT_W_MM / 2; // 24 + 24
    function buildFixedPrintLayoutSide(fontTweaks, side) {
      const f = fontTweaks || {};
      const inset = PRINT_ZONE_INSET_MM;
      const cw = PRINT_ZONE_CONTENT_W_MM;
      const nameH = PRINT_ZONE_NAME_H_MM;
      const metaH = PRINT_ZONE_META_H_MM;
      const bcH = PRINT_ZONE_BC_H_MM;
      const metaW = cw / 2;
      return {
        namePx: f.namePx != null ? f.namePx : 24,
        originalPx: f.originalPx != null ? f.originalPx : 7,
        locationPx: f.locationPx != null ? f.locationPx : 7,
        bcTextPx: f.bcTextPx != null ? f.bcTextPx : 9,
        nameBold: f.nameBold !== false,
        origBold: f.origBold !== false,
        locBold: f.locBold !== false,
        bcTextBold: f.bcTextBold !== false,
        nameXmm: inset,
        nameYmm: 0,
        nameWmm: cw,
        nameHmm: nameH,
        origXmm: inset,
        origYmm: nameH,
        origWmm: metaW,
        origHmm: metaH,
        locXmm: inset + metaW,
        locYmm: nameH,
        locWmm: metaW,
        locHmm: metaH,
        bcXmm: inset,
        bcYmm: nameH + metaH,
        bcWmm: cw,
        bcHmm: bcH,
        nameHeightMm: nameH,
        metaHeightMm: metaH,
        bcHeightMm: bcH,
        origWidthPct: 50,
        bcShiftXmm: 0,
        bcShiftYmm: 0
      };
    }
    const PRINT_LAYOUT_DEFAULTS_LEFT = buildFixedPrintLayoutSide({ namePx: 24, originalPx: 7, locationPx: 7, bcTextPx: 9 }, 'left');
    const PRINT_LAYOUT_DEFAULTS_RIGHT = buildFixedPrintLayoutSide({ namePx: 24, originalPx: 7, locationPx: 7, bcTextPx: 9 }, 'right');
    const PRINT_LAYOUT_DEFAULTS = PRINT_LAYOUT_DEFAULTS_LEFT;
    function defaultPrintLayoutBundle() {
      return {
        version: 11,
        left: coercePrintLayoutSide(PRINT_LAYOUT_DEFAULTS_LEFT),
        right: coercePrintLayoutSide(PRINT_LAYOUT_DEFAULTS_RIGHT)
      };
    }
    function extentPrintLayoutMm(layout) {
      const s = layout || {};
      let maxR = 0;
      let maxB = 0;
      const zones = [
        ['nameXmm', 'nameYmm', 'nameWmm', 'nameHmm'],
        ['origXmm', 'origYmm', 'origWmm', 'origHmm'],
        ['locXmm', 'locYmm', 'locWmm', 'locHmm'],
        ['bcXmm', 'bcYmm', 'bcWmm', 'bcHmm']
      ];
      for (let i = 0; i < zones.length; i++) {
        const x = Number(s[zones[i][0]]);
        const y = Number(s[zones[i][1]]);
        const w = Number(s[zones[i][2]]);
        const h = Number(s[zones[i][3]]);
        if (Number.isFinite(x) && Number.isFinite(w)) maxR = Math.max(maxR, x + w);
        if (Number.isFinite(y) && Number.isFinite(h)) maxB = Math.max(maxB, y + h);
      }
      return { w: maxR, h: maxB };
    }
    function getPrintGeometryMm() {
      // حامل 105×28 · هامش 1 مم · لزقتان 50×26 · فجوة 3 مم
      const labelW = PRINT_SHEET_MM.labelW;
      const labelH = PRINT_SHEET_MM.labelH;
      const gap = PRINT_SHEET_MM.gap;
      const margin = PRINT_SHEET_MM.margin;
      const sheetW = PRINT_SHEET_MM.sheetW;
      const sheetH = PRINT_SHEET_MM.sheetH;
      return {
        labelW: labelW,
        labelH: labelH,
        sheetW: sheetW,
        sheetH: sheetH,
        margin: margin,
        gap: gap,
        contentW: labelW,
        contentH: labelH
      };
    }
    function applyPrintGeometryCss(geo) {
      const g = geo || getPrintGeometryMm();
      let st = document.getElementById('printDynGeomStyle');
      if (!st) {
        st = document.createElement('style');
        st.id = 'printDynGeomStyle';
        document.head.appendChild(st);
      }
      // حامل 105×28 · هامش 1 · لزقة 50×26 · فجوة 3
      const m = (g.margin != null) ? g.margin : 1;
      st.textContent =
        ':root{--print-label-w:' + g.labelW + 'mm;--print-label-h:' + g.labelH + 'mm;'
        + '--print-sheet-w:' + g.sheetW + 'mm;--print-sheet-h:' + g.sheetH + 'mm;'
        + '--print-gap:' + g.gap + 'mm;--print-margin:' + m + 'mm;}'
        + '.print-label-mock-host{--print-label-w:' + g.labelW + 'mm;--print-label-h:' + g.labelH + 'mm;'
        + 'width:' + g.labelW + 'mm;height:' + g.labelH + 'mm;'
        + 'margin-bottom:calc(' + g.labelH + 'mm * (var(--mock-scale) - 1) + 8px);}'
        + '.print-label-mock-host .print-label{width:' + g.labelW + 'mm;max-width:' + g.labelW + 'mm;'
        + 'height:' + g.labelH + 'mm;max-height:' + g.labelH + 'mm;padding:0;}'
        + '.print-designer-row{gap:' + (g.gap * PRINT_DESIGNER_PX_PER_MM) + 'px;}'
        + '@media print{'
        + '@page{size:' + g.sheetW + 'mm ' + g.sheetH + 'mm;margin:0;}'
        + 'html,body{width:' + g.sheetW + 'mm !important;}'
        + '.print-label-row{width:' + g.sheetW + 'mm;height:' + g.sheetH + 'mm;max-height:' + g.sheetH + 'mm;'
        + 'gap:' + g.gap + 'mm;padding:' + m + 'mm;margin:0;box-sizing:border-box;}'
        + '.print-label,.print-label.print-label-left,.print-label.print-label-right,'
        + '.print-label-row .print-label:first-child,.print-label-row .print-label:last-child{'
        + 'width:' + g.labelW + 'mm;max-width:' + g.labelW + 'mm;'
        + 'height:' + g.labelH + 'mm;max-height:' + g.labelH + 'mm;padding:0;margin:0;}'
        + '}'
        + '.print-label-sheet.print-measure .print-label-row{width:' + g.sheetW + 'mm;height:' + g.sheetH + 'mm;'
        + 'gap:' + g.gap + 'mm;padding:' + m + 'mm;margin:0;box-sizing:border-box;}'
        + '.print-label-sheet.print-measure .print-label{width:' + g.labelW + 'mm;max-width:' + g.labelW + 'mm;'
        + 'height:' + g.labelH + 'mm;max-height:' + g.labelH + 'mm;padding:0;margin:0;}';
      return g;
    }
    function syncPrintGeometryUi() {
      const g = applyPrintGeometryCss();
      const ppm = PRINT_DESIGNER_PX_PER_MM;
      const board = document.getElementById('printDesignerBoard');
      if (board) {
        const padMm = 14;
        const padPx = padMm * ppm;
        board.style.width = (g.sheetW * ppm + padPx * 2) + 'px';
        board.style.height = (g.sheetH * ppm + padPx * 2) + 'px';
        const frame = board.querySelector('.print-designer-sheet-frame');
        if (frame) {
          frame.style.left = padPx + 'px';
          frame.style.top = padPx + 'px';
          frame.style.width = (g.sheetW * ppm) + 'px';
          frame.style.height = (g.sheetH * ppm) + 'px';
        }
        const row = board.querySelector('.print-designer-row');
        if (row) {
          const mPx = ((g.margin != null) ? g.margin : 1) * ppm;
          row.style.left = (padPx + mPx) + 'px';
          row.style.top = (padPx + mPx) + 'px';
          row.style.width = ((g.labelW * 2 + g.gap) * ppm) + 'px';
          row.style.height = (g.labelH * ppm) + 'px';
          row.style.gap = (g.gap * ppm) + 'px';
        }
        board.querySelectorAll('.print-designer-label').forEach((label) => {
          label.style.width = (g.labelW * ppm) + 'px';
          label.style.height = (g.labelH * ppm) + 'px';
        });
      }
      return g;
    }
    const PRINT_ZONE_MIN = {
      name: { w: 12, h: 4 },
      orig: { w: 6, h: 1.8 },
      loc: { w: 6, h: 1.8 },
      bc: { w: 16, h: 5 }
    };
    function clampPrintNum(n, lo, hi, fallback) {
      const v = Number(n);
      if (!Number.isFinite(v)) return fallback;
      return Math.max(lo, Math.min(hi, v));
    }
    function migrateLegacyPrintLayout(o) {
      const out = Object.assign({}, o || {});
      const hasFree = out.nameXmm != null || out.nameWmm != null || out.nameHmm != null;
      if (hasFree) return out;
      // ترحيل من نموذج الارتفاعات القديمة إلى صناديق حرة
      const nameH = clampPrintNum(out.nameHeightMm, 6, 18, PRINT_LAYOUT_DEFAULTS.nameHmm);
      const metaH = clampPrintNum(out.metaHeightMm, 2, 8, PRINT_LAYOUT_DEFAULTS.origHmm);
      const bcH = clampPrintNum(out.bcHeightMm, 5, 16, PRINT_LAYOUT_DEFAULTS.bcHmm);
      const pct = clampPrintNum(out.origWidthPct, 25, 75, 50) / 100;
      const cw = PRINT_CONTENT_W_MM;
      out.nameXmm = 0;
      out.nameYmm = 0;
      out.nameWmm = cw;
      out.nameHmm = nameH;
      out.origXmm = 0;
      out.origYmm = nameH;
      out.origWmm = Math.round(cw * pct * 100) / 100;
      out.origHmm = metaH;
      out.locXmm = out.origWmm;
      out.locYmm = nameH;
      out.locWmm = Math.round((cw - out.origWmm) * 100) / 100;
      out.locHmm = metaH;
      out.bcXmm = 0;
      out.bcYmm = nameH + metaH;
      out.bcWmm = cw;
      out.bcHmm = bcH;
      return out;
    }
    function clampPrintBox(box, minW, minH) {
      // حرّك/كبّر خارج حدود اللزقة — ما منحبس العناصر جوّا الشاشة
      const MAX_W = 90;
      const MAX_H = 60;
      const MIN_X = -45;
      const MAX_X = 90;
      const MIN_Y = -45;
      const MAX_Y = 60;
      let w = clampPrintNum(box.w, minW, MAX_W, minW);
      let h = clampPrintNum(box.h, minH, MAX_H, minH);
      let x = clampPrintNum(box.x, MIN_X, MAX_X, 0);
      let y = clampPrintNum(box.y, MIN_Y, MAX_Y, 0);
      return {
        x: Math.round(x * 100) / 100,
        y: Math.round(y * 100) / 100,
        w: Math.round(w * 100) / 100,
        h: Math.round(h * 100) / 100
      };
    }
    function normalizePrintLayoutBoxes(s) {
      const out = Object.assign({}, s);
      const name = clampPrintBox(
        { x: out.nameXmm, y: out.nameYmm, w: out.nameWmm, h: out.nameHmm },
        PRINT_ZONE_MIN.name.w, PRINT_ZONE_MIN.name.h
      );
      const orig = clampPrintBox(
        { x: out.origXmm, y: out.origYmm, w: out.origWmm, h: out.origHmm },
        PRINT_ZONE_MIN.orig.w, PRINT_ZONE_MIN.orig.h
      );
      const loc = clampPrintBox(
        { x: out.locXmm, y: out.locYmm, w: out.locWmm, h: out.locHmm },
        PRINT_ZONE_MIN.loc.w, PRINT_ZONE_MIN.loc.h
      );
      const bc = clampPrintBox(
        { x: out.bcXmm, y: out.bcYmm, w: out.bcWmm, h: out.bcHmm },
        PRINT_ZONE_MIN.bc.w, PRINT_ZONE_MIN.bc.h
      );
      out.nameXmm = name.x; out.nameYmm = name.y; out.nameWmm = name.w; out.nameHmm = name.h;
      out.origXmm = orig.x; out.origYmm = orig.y; out.origWmm = orig.w; out.origHmm = orig.h;
      out.locXmm = loc.x; out.locYmm = loc.y; out.locWmm = loc.w; out.locHmm = loc.h;
      out.bcXmm = bc.x; out.bcYmm = bc.y; out.bcWmm = bc.w; out.bcHmm = bc.h;
      // مفاتيح توافق للباركود الحراري والواجهات القديمة
      out.nameHeightMm = out.nameHmm;
      out.metaHeightMm = Math.max(out.origHmm, out.locHmm);
      out.bcHeightMm = out.bcHmm;
      out.origWidthPct = clampPrintNum((out.origWmm / Math.max(0.1, out.origWmm + out.locWmm)) * 100, 25, 75, 50);
      out.bcShiftXmm = clampPrintNum(out.bcShiftXmm, -30, 30, 0);
      out.bcShiftYmm = clampPrintNum(out.bcShiftYmm, -30, 30, 0);
      return out;
    }
    function coercePrintLayoutSide(o) {
      const raw = migrateLegacyPrintLayout(o || {});
      const base = {
        namePx: clampPrintNum(raw.namePx, 8, 72, PRINT_LAYOUT_DEFAULTS.namePx),
        originalPx: clampPrintNum(raw.originalPx, 6, 48, PRINT_LAYOUT_DEFAULTS.originalPx),
        locationPx: clampPrintNum(raw.locationPx, 6, 48, PRINT_LAYOUT_DEFAULTS.locationPx),
        bcTextPx: clampPrintNum(raw.bcTextPx, 6, 36, PRINT_LAYOUT_DEFAULTS.bcTextPx),
        nameBold: raw.nameBold !== false,
        origBold: raw.origBold !== false,
        locBold: raw.locBold !== false,
        bcTextBold: raw.bcTextBold !== false,
        nameXmm: clampPrintNum(raw.nameXmm, -45, 90, PRINT_LAYOUT_DEFAULTS.nameXmm),
        nameYmm: clampPrintNum(raw.nameYmm, -45, 60, PRINT_LAYOUT_DEFAULTS.nameYmm),
        nameWmm: clampPrintNum(raw.nameWmm, 8, 90, PRINT_LAYOUT_DEFAULTS.nameWmm),
        nameHmm: clampPrintNum(raw.nameHmm, 3, 60, PRINT_LAYOUT_DEFAULTS.nameHmm),
        origXmm: clampPrintNum(raw.origXmm, -45, 90, PRINT_LAYOUT_DEFAULTS.origXmm),
        origYmm: clampPrintNum(raw.origYmm, -45, 60, PRINT_LAYOUT_DEFAULTS.origYmm),
        origWmm: clampPrintNum(raw.origWmm, 4, 90, PRINT_LAYOUT_DEFAULTS.origWmm),
        origHmm: clampPrintNum(raw.origHmm, 1.5, 40, PRINT_LAYOUT_DEFAULTS.origHmm),
        locXmm: clampPrintNum(raw.locXmm, -45, 90, PRINT_LAYOUT_DEFAULTS.locXmm),
        locYmm: clampPrintNum(raw.locYmm, -45, 60, PRINT_LAYOUT_DEFAULTS.locYmm),
        locWmm: clampPrintNum(raw.locWmm, 4, 90, PRINT_LAYOUT_DEFAULTS.locWmm),
        locHmm: clampPrintNum(raw.locHmm, 1.5, 40, PRINT_LAYOUT_DEFAULTS.locHmm),
        bcXmm: clampPrintNum(raw.bcXmm, -45, 90, PRINT_LAYOUT_DEFAULTS.bcXmm),
        bcYmm: clampPrintNum(raw.bcYmm, -45, 60, PRINT_LAYOUT_DEFAULTS.bcYmm),
        bcWmm: clampPrintNum(raw.bcWmm, 8, 90, PRINT_LAYOUT_DEFAULTS.bcWmm),
        bcHmm: clampPrintNum(raw.bcHmm, 4, 60, PRINT_LAYOUT_DEFAULTS.bcHmm),
        bcShiftXmm: clampPrintNum(raw.bcShiftXmm, -30, 30, PRINT_LAYOUT_DEFAULTS.bcShiftXmm),
        bcShiftYmm: clampPrintNum(raw.bcShiftYmm, -30, 30, PRINT_LAYOUT_DEFAULTS.bcShiftYmm)
      };
      return normalizePrintLayoutBoxes(base);
    }
    function migratePrintLayoutSideHeightV3(side) {
      // رفع منطقة الباركود +3 مم لاستغلال ارتفاع اللزقة الجديد
      const s = coercePrintLayoutSide(side || {});
      if (Number(s.bcHmm) < 12.5) {
        s.bcHmm = Math.min(22, Number(s.bcHmm) + 3);
        s.bcHeightMm = s.bcHmm;
      }
      return s;
    }
    function getPrintLayoutBundle() {
      try {
        const raw = localStorage.getItem('printLayoutSettings');
        const parsed = raw ? JSON.parse(raw) : {};
        const ver = Number(parsed && parsed.version) || 0;
        // v11 = ملء اللزقة 50×26 بهامش 1مم من دون إزاحة خارجية زائدة
        if (ver < 11) {
          const b = defaultPrintLayoutBundle();
          try { localStorage.setItem('printLayoutSettings', JSON.stringify(b)); } catch (_) {}
          return b;
        }
        if (parsed && typeof parsed === 'object' && parsed.left && parsed.right) {
          return {
            version: 11,
            left: coercePrintLayoutSide(parsed.left),
            right: coercePrintLayoutSide(parsed.right)
          };
        }
        return defaultPrintLayoutBundle();
      } catch (_) {
        return defaultPrintLayoutBundle();
      }
    }
    function savePrintLayoutBundle(bundle) {
      const b = {
        version: 11,
        left: coercePrintLayoutSide((bundle && bundle.left) || PRINT_LAYOUT_DEFAULTS_LEFT),
        right: coercePrintLayoutSide((bundle && bundle.right) || PRINT_LAYOUT_DEFAULTS_RIGHT)
      };
      try { localStorage.setItem('printLayoutSettings', JSON.stringify(b)); } catch (_) {}
      try { scheduleUploadPrintLayoutPrefs(); } catch (_) {}
      return b;
    }
    let _printLayoutUploadTimer = null;
    async function uploadPrintLayoutPrefsToServer(showStatus) {
      let b = null;
      try { b = getPrintLayoutBundle(); } catch (_) { b = null; }
      if (!b || !b.left || !b.right) return false;
      try {
        const res = await fetch('/print_layout_prefs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(b)
        });
        const data = await res.json().catch(() => ({}));
        if (showStatus) {
          const el = document.getElementById('printDesignerReadout');
          if (el) {
            el.textContent = (data && data.ok)
              ? ('تم اعتماد قياسات GUI على السيرفر · ' + (data.updated_at || ''))
              : 'فشل رفع القياسات';
          }
        }
        return !!(data && data.ok);
      } catch (_) {
        if (showStatus) {
          const el = document.getElementById('printDesignerReadout');
          if (el) el.textContent = 'فشل رفع القياسات';
        }
        return false;
      }
    }
    function scheduleUploadPrintLayoutPrefs() {
      if (_printLayoutUploadTimer) clearTimeout(_printLayoutUploadTimer);
      _printLayoutUploadTimer = setTimeout(() => {
        uploadPrintLayoutPrefsToServer(false);
      }, 700);
    }
    function getPrintLayoutSettings(side) {
      const b = getPrintLayoutBundle();
      const s = printSideKey(side || 'left');
      return b[s] || b.left;
    }
    function savePrintLayoutSettings(sideOrSettings, maybeSettings) {
      // savePrintLayoutSettings('left', layout) أو savePrintLayoutSettings(layout) للوضع القديم
      if (maybeSettings != null || typeof sideOrSettings === 'string') {
        const side = printSideKey(sideOrSettings || 'left');
        const b = getPrintLayoutBundle();
        b[side] = coercePrintLayoutSide(maybeSettings);
        return savePrintLayoutBundle(b)[side];
      }
      const shared = coercePrintLayoutSide(sideOrSettings || PRINT_LAYOUT_DEFAULTS);
      return savePrintLayoutBundle({ left: shared, right: Object.assign({}, shared) });
    }
    function copyPrintLayoutSide(fromSide, toSide) {
      const from = printSideKey(fromSide);
      const to = printSideKey(toSide);
      if (from === to) return;
      const b = getPrintLayoutBundle();
      b[to] = Object.assign({}, b[from]);
      savePrintLayoutBundle(b);
      syncPrintLayoutSettingsUi(to);
      refreshPrintDesignerBoard();
      try {
        renderPrintSideLabelMock('left');
        renderPrintSideLabelMock('right');
      } catch (_) {}
      const el = document.getElementById('printDesignerReadout');
      if (el) {
        el.textContent = (from === 'right' ? 'تم نسخ اليمين → اليسار' : 'تم نسخ اليسار → اليمين');
      }
    }
    function updatePrintDesignerReadout(side, s) {
      const el = document.getElementById('printDesignerReadout');
      if (!el) return;
      const geo = (typeof getPrintGeometryMm === 'function') ? getPrintGeometryMm() : null;
      const geoTxt = geo
        ? (' · اللزقة ' + geo.labelW.toFixed(1) + '×' + geo.labelH.toFixed(1)
          + ' مم · الورقة ' + geo.sheetW.toFixed(1) + '×' + geo.sheetH.toFixed(1) + ' مم')
        : '';
      if (!side) {
        el.textContent = 'اليسار واليمين مستقلين — كبّر خانة فتكبر اللزقة معها' + geoTxt;
        return;
      }
      const st = s || getPrintLayoutSettings(side);
      const tag = printSideKey(side) === 'right' ? 'يمين' : 'يسار';
      el.textContent =
        tag + ': اسم @' + st.nameXmm.toFixed(1) + ',' + st.nameYmm.toFixed(1)
        + ' (' + st.nameWmm.toFixed(1) + '×' + st.nameHmm.toFixed(1) + 'مم) · '
        + 'أصلي @' + st.origXmm.toFixed(1) + ',' + st.origYmm.toFixed(1) + ' · '
        + 'موقع @' + st.locXmm.toFixed(1) + ',' + st.locYmm.toFixed(1) + ' · '
        + 'باركود @' + st.bcXmm.toFixed(1) + ',' + st.bcYmm.toFixed(1)
        + ' (' + st.bcWmm.toFixed(1) + '×' + st.bcHmm.toFixed(1) + 'مم)'
        + geoTxt;
    }
    function syncPrintLayoutSettingsUi(side) {
      const st = getPrintLayoutSettings(side || 'left');
      const set = (id, val, labelId) => {
        const el = document.getElementById(id);
        if (el) el.value = String(val);
        const lab = document.getElementById(labelId);
        if (lab) lab.textContent = String(val);
      };
      set('printSetNamePx', st.namePx, 'printSetNamePxVal');
      set('printSetOriginalPx', st.originalPx, 'printSetOriginalPxVal');
      set('printSetLocationPx', st.locationPx, 'printSetLocationPxVal');
      set('printSetBcShiftX', st.bcShiftXmm, 'printSetBcShiftXVal');
      set('printSetBcShiftY', st.bcShiftYmm, 'printSetBcShiftYVal');
      updatePrintDesignerReadout(side || null, st);
    }
    function readPrintLayoutSettingsFromUi() {
      return getPrintLayoutSettings('left');
    }
    function onPrintLayoutSettingChange() {
      syncPrintLayoutSettingsUi('left');
      refreshPrintDesignerBoard();
    }
    function resetPrintLayoutSettings() {
      savePrintLayoutBundle(defaultPrintLayoutBundle());
      syncPrintLayoutSettingsUi(null);
      refreshPrintDesignerBoard();
      try {
        renderPrintSideLabelMock('left');
        renderPrintSideLabelMock('right');
      } catch (_) {}
      const el = document.getElementById('printDesignerReadout');
      if (el) el.textContent = 'حامل 105×28 · هامش 1مم · لزقة 50×26 · اسم 9 · ميتا 2 · باركود 15';
    }
    function fitTextIntoBox(el, maxPx, minPx) {
      if (!el) return minPx;
      const hi0 = Math.max(minPx, maxPx);
      const lo0 = Math.min(minPx, hi0);
      el.style.overflow = 'hidden';
      let lo = lo0;
      let hi = hi0;
      let best = lo0;
      for (let i = 0; i < 18; i++) {
        const mid = (lo + hi) / 2;
        el.style.fontSize = mid + 'px';
        const okH = el.clientHeight <= 1 || el.scrollHeight <= el.clientHeight + 0.75;
        const okW = el.clientWidth <= 1 || el.scrollWidth <= el.clientWidth + 0.75;
        if (okH && okW) {
          best = mid;
          lo = mid;
        } else {
          hi = mid;
        }
      }
      el.style.fontSize = best + 'px';
      return best;
    }
    function designerSamplePayload(side) {
      const s = printSideKey(side);
      const p = buildPrintPayloadForSide(s);
      if (p && !p.error) return p;
      const name = getPrintName(s) || (s === 'right' ? 'اسم الصنف يمين' : 'اسم الصنف يسار');
      return {
        name: name,
        original: 'ORIGINAL',
        location: 'A-01',
        barcode: s === 'right' ? '1234567890123' : '9876543210987'
      };
    }
    function printZoneBox(layout, zone) {
      const s = layout || getPrintLayoutSettings();
      if (zone === 'name') return { x: s.nameXmm, y: s.nameYmm, w: s.nameWmm, h: s.nameHmm };
      if (zone === 'orig') return { x: s.origXmm, y: s.origYmm, w: s.origWmm, h: s.origHmm };
      if (zone === 'loc') return { x: s.locXmm, y: s.locYmm, w: s.locWmm, h: s.locHmm };
      return { x: s.bcXmm, y: s.bcYmm, w: s.bcWmm, h: s.bcHmm };
    }
    function setPrintZoneBox(layout, zone, box) {
      const s = Object.assign({}, layout);
      const min = PRINT_ZONE_MIN[zone] || { w: 6, h: 2 };
      const b = clampPrintBox(box, min.w, min.h);
      if (zone === 'name') {
        s.nameXmm = b.x; s.nameYmm = b.y; s.nameWmm = b.w; s.nameHmm = b.h;
      } else if (zone === 'orig') {
        s.origXmm = b.x; s.origYmm = b.y; s.origWmm = b.w; s.origHmm = b.h;
      } else if (zone === 'loc') {
        s.locXmm = b.x; s.locYmm = b.y; s.locWmm = b.w; s.locHmm = b.h;
      } else {
        s.bcXmm = b.x; s.bcYmm = b.y; s.bcWmm = b.w; s.bcHmm = b.h;
      }
      return s;
    }
    function fitDesignerZoneFonts(labelEl, layout, sample) {
      if (!labelEl || !layout) return;
      const k = PRINT_DESIGNER_PX_PER_MM / PRINT_REAL_PX_PER_MM;
      const side = printSideKey(labelEl.getAttribute('data-side') || 'left');
      const nameInner = labelEl.querySelector('.pdg-zone[data-zone="name"] .pdg-zone-inner');
      const origInner = labelEl.querySelector('.pdg-zone[data-zone="orig"] .pdg-zone-inner');
      const locInner = labelEl.querySelector('.pdg-zone[data-zone="loc"] .pdg-zone-inner');
      const bcInner = labelEl.querySelector('.pdg-zone[data-zone="bc"] .pdg-zone-inner');
      if (nameInner) {
        nameInner.textContent = sanitizePrintItemName(sample.name || '—') || '—';
        const minN = Math.min(14, Math.max(9, Math.round(layout.namePx * 0.32)));
        nameInner.style.lineHeight = '1.02';
        fitTextIntoBox(nameInner, layout.namePx * k, minN * k);
      }
      if (origInner) {
        origInner.textContent = sample.original || '—';
        fitTextIntoBox(origInner, layout.originalPx * k, Math.max(6, Math.round(layout.originalPx * 0.55)) * k);
      }
      if (locInner) {
        locInner.textContent = sample.location || '—';
        fitTextIntoBox(locInner, layout.locationPx * k, Math.max(6, Math.round(layout.locationPx * 0.55)) * k);
      }
      if (bcInner) {
        const ppm = PRINT_DESIGNER_PX_PER_MM;
        bcInner.innerHTML = '';
        const r = renderPrintBarcodeCanvas(sample.barcode, side);
        if (r) {
          const img = document.createElement('img');
          img.src = r.canvas.toDataURL('image/png');
          img.style.cssText = 'display:block;margin:0 auto;max-width:none;image-rendering:pixelated;'
            + 'width:' + (r.wMm * ppm) + 'px;height:' + (r.hMm * ppm) + 'px;flex:0 0 ' + (r.hMm * ppm) + 'px;';
          bcInner.appendChild(img);
        }
        const t = document.createElement('div');
        t.className = 'pdg-bc-text';
        t.textContent = printBarcodeEncodeValue(sample.barcode || '') || '—';
        t.style.cssText = 'flex:0 0 ' + (3.2 * ppm) + 'px;height:' + (3.2 * ppm) + 'px;width:100%;'
          + 'line-height:' + (3.2 * ppm) + 'px;font-size:' + ((layout.bcTextPx || 9) * k) + 'px;'
          + 'font-weight:900;letter-spacing:0.05em;text-align:center;white-space:pre;overflow:hidden;';
        bcInner.appendChild(t);
      }
    }
    function deriveMaxFontsFromZones(layout) {
      // الخط يتبع ارتفاع الخانة — كبّرت الاسم بالـ GUI → الاسم يكبر باللزقة
      return {
        namePx: clampPrintNum(Math.round(layout.nameHmm * 3.35), 14, 64, PRINT_LAYOUT_DEFAULTS.namePx),
        originalPx: clampPrintNum(Math.round(layout.origHmm * 2.95), 6, 22, PRINT_LAYOUT_DEFAULTS.originalPx),
        locationPx: clampPrintNum(Math.round(layout.locHmm * 2.95), 6, 20, PRINT_LAYOUT_DEFAULTS.locationPx),
        bcTextPx: 9
      };
    }
    let printDesignerDrag = null;
    let printDesignerSel = null;
    function printZoneFontKey(zone) {
      if (zone === 'name') return { px: 'namePx', bold: 'nameBold', defPx: 24 };
      if (zone === 'orig') return { px: 'originalPx', bold: 'origBold', defPx: 7 };
      if (zone === 'loc') return { px: 'locationPx', bold: 'locBold', defPx: 7 };
      return { px: 'bcTextPx', bold: 'bcTextBold', defPx: 9 };
    }
    function printZoneFontTitle(zone) {
      if (zone === 'name') return 'الاسم';
      if (zone === 'orig') return 'الأصلي';
      if (zone === 'loc') return 'الموقع';
      return 'نص الباركود';
    }
    function ensurePrintFontSizeOption(sel, px) {
      if (!sel) return;
      const v = String(Math.round(Number(px) || 0));
      if (!v || v === '0') return;
      for (let i = 0; i < sel.options.length; i++) {
        if (sel.options[i].value === v) return;
      }
      const opt = document.createElement('option');
      opt.value = v;
      opt.textContent = v;
      sel.appendChild(opt);
    }
    function highlightPrintDesignerSelection() {
      document.querySelectorAll('.pdg-zone.is-active').forEach((z) => z.classList.remove('is-active'));
      if (!printDesignerSel || !printDesignerSel.zone) return;
      const label = document.querySelector('.print-designer-label[data-side="' + printDesignerSel.side + '"]');
      if (!label) return;
      const zEl = label.querySelector('.pdg-zone[data-zone="' + printDesignerSel.zone + '"]');
      if (zEl) zEl.classList.add('is-active');
    }
    function syncPrintFontToolbar() {
      const sizeEl = document.getElementById('printFontSize');
      const boldEl = document.getElementById('printFontBold');
      const tgt = document.getElementById('printFontTarget');
      const sel = printDesignerSel;
      if (!sel || !sel.zone) {
        if (sizeEl) sizeEl.disabled = true;
        if (boldEl) { boldEl.disabled = true; boldEl.classList.remove('is-on'); }
        if (tgt) tgt.textContent = 'حدد عنصراً في الليبل';
        return;
      }
      const s = getPrintLayoutSettings(sel.side);
      const k = printZoneFontKey(sel.zone);
      const px = Number(s[k.px]) || k.defPx;
      const bold = s[k.bold] !== false;
      if (sizeEl) {
        sizeEl.disabled = false;
        ensurePrintFontSizeOption(sizeEl, px);
        sizeEl.value = String(Math.round(px));
      }
      if (boldEl) {
        boldEl.disabled = false;
        boldEl.classList.toggle('is-on', bold);
      }
      if (tgt) tgt.textContent = ((sel.side === 'right') ? 'يمين' : 'يسار') + ' · ' + printZoneFontTitle(sel.zone);
    }
    function applyPrintFontToSelection(patch) {
      const sel = printDesignerSel;
      if (!sel || !sel.zone) return;
      const s = Object.assign({}, getPrintLayoutSettings(sel.side), patch || {});
      savePrintLayoutSettings(sel.side, s);
      paintPrintDesignerZones(sel.side);
      highlightPrintDesignerSelection();
      syncPrintFontToolbar();
      try { renderPrintSideLabelMock(sel.side); } catch (_) {}
    }
    function onPrintFontSizeChange() {
      const sel = printDesignerSel;
      const sizeEl = document.getElementById('printFontSize');
      if (!sel || !sel.zone || !sizeEl) return;
      const k = printZoneFontKey(sel.zone);
      const patch = {};
      patch[k.px] = Number(sizeEl.value);
      applyPrintFontToSelection(patch);
    }
    function onPrintFontBoldToggle() {
      const sel = printDesignerSel;
      if (!sel || !sel.zone) return;
      const s = getPrintLayoutSettings(sel.side);
      const k = printZoneFontKey(sel.zone);
      const patch = {};
      patch[k.bold] = (s[k.bold] === false);
      applyPrintFontToSelection(patch);
    }
    function bindPrintDesignerFontUi() {
      const board = document.getElementById('printDesignerBoard');
      if (!board || board.dataset.fontUi === '1') return;
      board.dataset.fontUi = '1';
      board.addEventListener('pointerdown', (ev) => {
        if (ev.target && ev.target.closest && ev.target.closest('.pdg-zone')) return;
        printDesignerSel = null;
        highlightPrintDesignerSelection();
        syncPrintFontToolbar();
      });
    }
    function applyDesignerDrag(clientY, clientX) {
      if (!printDesignerDrag) return;
      const d = printDesignerDrag;
      const side = printSideKey(d.side || 'left');
      const ppm = PRINT_DESIGNER_PX_PER_MM;
      const dxMm = (clientX - d.startX) / ppm;
      const dyMm = (clientY - d.startY) / ppm;
      let s = Object.assign({}, getPrintLayoutSettings(side));
      const start = d.startBox;
      if (d.mode === 'move') {
        s = setPrintZoneBox(s, d.zone, {
          x: start.x + dxMm,
          y: start.y + dyMm,
          w: start.w,
          h: start.h
        });
      } else if (d.mode === 'resize') {
        const dir = d.resizeDir || 'se';
        let nw = start.w;
        let nh = start.h;
        if (dir === 'se' || dir === 'e') nw = start.w + dxMm;
        if (dir === 'se' || dir === 's') nh = start.h + dyMm;
        s = setPrintZoneBox(s, d.zone, {
          x: start.x,
          y: start.y,
          w: nw,
          h: nh
        });
      }
      const saved = savePrintLayoutSettings(side, s);
      syncPrintLayoutSettingsUi(side);
      paintPrintDesignerZones(side);
      // وسّع إطار اللزقة فوراً مع تكبير الخانة
      try { syncPrintGeometryUi(); } catch (_) {}
      updatePrintDesignerReadout(side, saved);
    }
    function endDesignerDrag() {
      if (!printDesignerDrag) return;
      const side = printSideKey(printDesignerDrag.side || 'left');
      document.querySelectorAll(
        '.pdg-zone.is-dragging, .pdg-zone.is-resizing, .pdg-zone.is-active, .pdg-resize.is-dragging'
      ).forEach((h) => {
        h.classList.remove('is-dragging', 'is-resizing', 'is-active');
      });
      printDesignerDrag = null;
      try {
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      } catch (_) {}
      try { syncPrintGeometryUi(); } catch (_) {}
      refreshPrintDesignerBoard();
      try {
        renderPrintSideLabelMock('left');
        renderPrintSideLabelMock('right');
      } catch (_) {}
    }
    function startDesignerZoneInteract(side, zone, mode, ev, resizeDir) {
      ev.preventDefault();
      ev.stopPropagation();
      const sKey = printSideKey(side || 'left');
      const s = getPrintLayoutSettings(sKey);
      const dir = resizeDir || 'se';
      printDesignerSel = { side: sKey, zone: zone };
      highlightPrintDesignerSelection();
      syncPrintFontToolbar();
      printDesignerDrag = {
        side: sKey,
        zone: zone,
        mode: mode,
        resizeDir: dir,
        startX: ev.clientX,
        startY: ev.clientY,
        startBox: printZoneBox(s, zone)
      };
      const zoneEl = ev.currentTarget && ev.currentTarget.closest
        ? ev.currentTarget.closest('.pdg-zone')
        : null;
      if (zoneEl) {
        zoneEl.classList.add(mode === 'resize' ? 'is-resizing' : 'is-dragging', 'is-active');
      }
      if (ev.currentTarget && ev.currentTarget.classList) {
        ev.currentTarget.classList.add('is-dragging');
      }
      try {
        if (ev.currentTarget && ev.currentTarget.setPointerCapture && ev.pointerId != null) {
          ev.currentTarget.setPointerCapture(ev.pointerId);
        }
      } catch (_) {}
      try {
        let cur = 'grabbing';
        if (mode === 'resize') {
          cur = dir === 'e' ? 'ew-resize' : (dir === 's' ? 'ns-resize' : 'nwse-resize');
        }
        document.body.style.cursor = cur;
        document.body.style.userSelect = 'none';
      } catch (_) {}
      updatePrintDesignerReadout(sKey, s);
    }
    function paintPrintDesignerZones(activeSide) {
      const board = document.getElementById('printDesignerBoard');
      if (!board) return;
      const ppm = PRINT_DESIGNER_PX_PER_MM;
      // أصل الإحداثيات = زاوية اللزقة (0,0) بدون padding/إزاحة إجبارية
      board.querySelectorAll('.print-designer-label').forEach((labelEl) => {
        const side = printSideKey(labelEl.getAttribute('data-side') || 'left');
        const s = getPrintLayoutSettings(side);
        ['name', 'orig', 'loc', 'bc'].forEach((zone) => {
          const zEl = labelEl.querySelector('.pdg-zone[data-zone="' + zone + '"]');
          if (!zEl) return;
          const b = printZoneBox(s, zone);
          zEl.style.left = (b.x * ppm) + 'px';
          zEl.style.top = (b.y * ppm) + 'px';
          zEl.style.width = (b.w * ppm) + 'px';
          zEl.style.height = (b.h * ppm) + 'px';
        });
        fitDesignerZoneFonts(labelEl, s, designerSamplePayload(side));
      });
      updatePrintDesignerReadout(activeSide || null);
      highlightPrintDesignerSelection();
      syncPrintFontToolbar();
    }
    function buildPrintDesignerLabelEl(side) {
      const ppm = PRINT_DESIGNER_PX_PER_MM;
      const geo = getPrintGeometryMm();
      const sKey = printSideKey(side);
      const label = document.createElement('div');
      label.className = 'print-designer-label';
      label.setAttribute('data-side', sKey);
      label.style.width = (geo.labelW * ppm) + 'px';
      label.style.height = (geo.labelH * ppm) + 'px';
      const zones = [
        { key: 'name', title: 'اسم الصنف' },
        { key: 'orig', title: 'الرقم الأصلي' },
        { key: 'loc', title: 'الموقع' },
        { key: 'bc', title: 'الباركود' }
      ];
      let html = '<span class="pdg-side-tag">' + (sKey === 'right' ? 'يمين' : 'يسار') + '</span>';
      zones.forEach((z) => {
        html += '<div class="pdg-zone" data-zone="' + z.key + '" title="اسحب للنقل · المربعات الزرقا للتكبير/التصغير">'
          + '<span class="pdg-zone-label">' + z.title + '</span>'
          + '<div class="pdg-zone-inner">'
          + (z.key === 'bc' ? '<div class="pdg-bc-bars"></div><div class="pdg-bc-text"></div>' : '')
          + '</div>'
          + '<div class="pdg-resize pdg-resize-e" data-zone="' + z.key + '" data-resize="e" title="عرض — كبّر/صغّر"></div>'
          + '<div class="pdg-resize pdg-resize-s" data-zone="' + z.key + '" data-resize="s" title="ارتفاع — كبّر/صغّر"></div>'
          + '<div class="pdg-resize pdg-resize-se" data-zone="' + z.key + '" data-resize="se" title="زاوية — كبّر/صغّر معاً"></div>'
          + '</div>';
      });
      label.innerHTML = html;
      label.querySelectorAll('.pdg-zone').forEach((zEl) => {
        const zone = zEl.getAttribute('data-zone');
        zEl.addEventListener('pointerdown', (ev) => {
          if (ev.target && ev.target.closest && ev.target.closest('.pdg-resize')) return;
          startDesignerZoneInteract(sKey, zone, 'move', ev);
        });
      });
      label.querySelectorAll('.pdg-resize').forEach((rEl) => {
        const zone = rEl.getAttribute('data-zone');
        const dir = rEl.getAttribute('data-resize') || 'se';
        rEl.addEventListener('pointerdown', (ev) => {
          startDesignerZoneInteract(sKey, zone, 'resize', ev, dir);
        });
      });
      return label;
    }
    function refreshPrintDesignerBoard() {
      const board = document.getElementById('printDesignerBoard');
      if (!board) return;
      const ppm = PRINT_DESIGNER_PX_PER_MM;
      const geo = getPrintGeometryMm();
      applyPrintGeometryCss(geo);
      // مساحة إضافية حول الورقة عشان تشوف وتسحب العناصر برا حدود اللزقة
      const padMm = 14;
      const sheetW = geo.sheetW * ppm;
      const sheetH = geo.sheetH * ppm;
      const padPx = padMm * ppm;
      board.style.width = (sheetW + padPx * 2) + 'px';
      board.style.height = (sheetH + padPx * 2) + 'px';
      board.innerHTML = '';
      // إطار يوضح حدود الورقة الفعلية
      const sheetFrame = document.createElement('div');
      sheetFrame.className = 'print-designer-sheet-frame';
      sheetFrame.style.left = padPx + 'px';
      sheetFrame.style.top = padPx + 'px';
      sheetFrame.style.width = sheetW + 'px';
      sheetFrame.style.height = sheetH + 'px';
      board.appendChild(sheetFrame);
      const row = document.createElement('div');
      row.className = 'print-designer-row';
      const mPx = ((geo.margin != null) ? geo.margin : 1) * ppm;
      row.style.left = (padPx + mPx) + 'px';
      row.style.top = (padPx + mPx) + 'px';
      row.style.width = ((geo.labelW * 2 + geo.gap) * ppm) + 'px';
      row.style.height = (geo.labelH * ppm) + 'px';
      row.style.gap = (geo.gap * ppm) + 'px';
      row.appendChild(buildPrintDesignerLabelEl('left'));
      row.appendChild(buildPrintDesignerLabelEl('right'));
      board.appendChild(row);
      paintPrintDesignerZones(null);
      highlightPrintDesignerSelection();
      syncPrintFontToolbar();
      syncPrintLayoutSettingsUi(null);
    }
    function applyPrintBoxStyle(el, box) {
      if (!el || !box) return;
      el.style.position = 'absolute';
      el.style.left = box.x + 'mm';
      el.style.top = box.y + 'mm';
      el.style.width = box.w + 'mm';
      el.style.height = box.h + 'mm';
      el.style.maxWidth = box.w + 'mm';
      el.style.maxHeight = box.h + 'mm';
      el.style.boxSizing = 'border-box';
      el.style.overflow = 'hidden';
      el.style.margin = '0';
    }
    function printLayoutSideFromEl(el) {
      if (!el || !el.closest) return 'left';
      const label = el.closest('.print-label') || el.closest('.print-designer-label');
      if (!label) return 'left';
      return printSideKey(
        label.getAttribute('data-print-side')
        || label.getAttribute('data-side')
        || 'left'
      );
    }
    function clampBarcodeBoxToLabelMm(side, box) {
      // لا يغيّر إعدادات GUI المحفوظة — يضبط الصندوق عند الرسم حتى لا يُقصّ Quiet Zone
      const labelW = PRINT_SHEET_MM.labelW;
      const labelH = PRINT_SHEET_MM.labelH;
      const sideKey = printSideKey(side || 'left');
      const padL = PRINT_ZONE_INSET_MM;
      const padR = PRINT_ZONE_INSET_MM;
      let x = Number(box && box.x);
      let y = Number(box && box.y);
      let w = Number(box && box.w);
      let h = Number(box && box.h);
      if (!Number.isFinite(x)) x = padL;
      if (!Number.isFinite(y)) y = PRINT_ZONE_NAME_H_MM + PRINT_ZONE_META_H_MM;
      if (!Number.isFinite(w) || w < 18) w = labelW - padL - padR;
      if (!Number.isFinite(h) || h < 6) h = PRINT_ZONE_BC_H_MM;
      if (x < padL) {
        w -= (padL - x);
        x = padL;
      }
      if (x + w > labelW - padR) w = labelW - padR - x;
      if (w < 18) {
        x = padL;
        w = Math.max(18, labelW - padL - padR);
      }
      if (y < 0) y = 0;
      if (y + h > labelH) h = Math.max(8, labelH - y);
      return { x: x, y: y, w: w, h: h };
    }
    function applyPrintLayoutToLabel(label) {
      if (!label) return;
      const side = printSideKey(
        label.getAttribute('data-print-side')
        || label.getAttribute('data-side')
        || 'left'
      );
      const s = getPrintLayoutSettings(side);
      const nameEl = label.querySelector('.pl-name');
      const metaEl = label.querySelector('.pl-meta');
      const origEl = label.querySelector('.pl-orig');
      const locEl = label.querySelector('.pl-loc');
      const wrap = label.querySelector('.pl-bc-wrap');
      const bcText = label.querySelector('.pl-bc-text');
      if (metaEl) metaEl.style.display = 'none';
      if (nameEl) {
        nameEl.style.fontFamily = '"Cairo", "Tahoma", "Segoe UI", Arial, sans-serif';
        nameEl.style.fontWeight = '900';
        nameEl.dataset.maxNamePx = String(s.namePx);
        applyPrintBoxStyle(nameEl, { x: s.nameXmm, y: s.nameYmm, w: s.nameWmm, h: s.nameHmm });
      }
      if (origEl) {
        origEl.style.fontSize = s.originalPx + 'px';
        origEl.style.fontWeight = '800';
        applyPrintBoxStyle(origEl, { x: s.origXmm, y: s.origYmm, w: s.origWmm, h: s.origHmm });
      }
      if (locEl) {
        locEl.style.fontSize = s.locationPx + 'px';
        locEl.style.fontWeight = '800';
        applyPrintBoxStyle(locEl, { x: s.locXmm, y: s.locYmm, w: s.locWmm, h: s.locHmm });
      }
      if (wrap) {
        const bcBox = clampBarcodeBoxToLabelMm(side, {
          x: Number(s.bcXmm) + (Number(s.bcShiftXmm) || 0),
          y: Number(s.bcYmm) + (Number(s.bcShiftYmm) || 0),
          w: Number(s.bcWmm) || 47,
          h: Number(s.bcHmm) || PRINT_ZONE_BC_H_MM
        });
        applyPrintBoxStyle(wrap, bcBox);
        wrap.style.display = 'flex';
        wrap.style.flexDirection = 'column';
        wrap.style.alignItems = 'center';
        wrap.style.justifyContent = 'flex-start';
        wrap.style.gap = '0';
        wrap.style.overflow = 'hidden';
        wrap.style.direction = 'ltr';
      }
      if (bcText) {
        bcText.dataset.maxBcTextPx = String(s.bcTextPx);
        bcText.style.flex = '0 0 3.2mm';
        bcText.style.minHeight = '3.2mm';
        bcText.style.fontSize = (s.bcTextPx || 9) + 'px';
        bcText.style.fontWeight = '900';
        bcText.style.letterSpacing = '0.05em';
        bcText.style.color = '#000';
      }
    }
    function fitPrintNameDynamic(el) {
      if (!el) return;
      const s = getPrintLayoutSettings(printLayoutSideFromEl(el));
      const maxPx = Number(el.dataset.maxNamePx) || s.namePx || 48;
      const minPx = Math.min(14, Math.max(9, Math.round(maxPx * 0.32)));
      el.style.display = 'block';
      el.style.fontFamily = '"Cairo", "Tahoma", "Segoe UI", Arial, sans-serif';
      el.style.fontWeight = '900';
      el.style.whiteSpace = 'normal';
      el.style.wordBreak = 'break-word';
      el.style.overflowWrap = 'anywhere';
      el.style.overflow = 'hidden';
      el.style.textAlign = 'center';
      el.style.lineHeight = '1.02';
      el.style.alignContent = 'center';
      el.style.webkitLineClamp = 'unset';
      el.style.lineClamp = 'unset';
      fitTextIntoBox(el, maxPx, minPx);
    }
    function fitPrintMetaLeft(el) {
      if (!el) return;
      const s = getPrintLayoutSettings(printLayoutSideFromEl(el));
      const orig = el.classList && el.classList.contains('pl-orig')
        ? el
        : el.querySelector('.pl-orig');
      const loc = el.classList && el.classList.contains('pl-loc')
        ? el
        : el.querySelector('.pl-loc');
      if (orig) fitTextIntoBox(orig, s.originalPx, Math.max(6, Math.round(s.originalPx * 0.55)));
      if (loc) fitTextIntoBox(loc, s.locationPx, Math.max(6, Math.round(s.locationPx * 0.55)));
      if (!orig && !loc) {
        let size = Math.max(s.originalPx, s.locationPx);
        el.style.fontSize = size + 'px';
        const floor = Math.max(6, Math.min(size, 7));
        while (size > floor && el.scrollWidth > el.clientWidth + 0.5) {
          size -= 0.35;
          el.style.fontSize = size + 'px';
        }
      }
    }
    function buildPrintLabelEl(data, side) {
      const sideKey = printSideKey(side || 'left');
      const label = document.createElement('div');
      label.className = 'print-label print-label-' + sideKey
        + (data && data.empty ? ' print-label-empty' : '');
      label.setAttribute('data-print-side', sideKey);
      label.innerHTML =
        `<div class="pl-name"></div>` +
        `<div class="pl-orig"></div>` +
        `<div class="pl-loc"></div>` +
        `<div class="pl-bc-wrap"><svg class="pl-bc"></svg><div class="pl-bc-text"></div></div>`;
      if (data && !data.empty) {
        label.querySelector('.pl-name').textContent = sanitizePrintItemName(data.name || '—') || '—';
        label.querySelector('.pl-orig').textContent = data.original || '—';
        label.querySelector('.pl-loc').textContent = data.location || '—';
        const bcText = label.querySelector('.pl-bc-text');
        bcText.textContent = data.barcode || '';
        fitPrintBarcodeText(bcText, data.barcode);
        // لا ترسم الباركود هنا — يُرسم بعد الإدخال في الصف حتى تُعرف جهة اليسار/اليمين
      }
      return label;
    }
    function isPrintPayloadReady(p) {
      return !!(p && !p.error && p.barcode && /[A-Za-z0-9]/.test(String(p.barcode)));
    }
    async function printBarcodeLabels() {
      const { qty, pages } = updatePrintQtyHint();
      // حاول تجهيز أي جهة فيها نص بحث — بدون إجبار الطرفين
      const leftQ = ((document.getElementById('printQueryLeft') || {}).value || '').trim();
      const rightQ = ((document.getElementById('printQueryRight') || {}).value || '').trim();
      if (!printSides.left.item && !(printSides.left.rows && printSides.left.rows.length) && leftQ) {
        await resolvePrintItemFromLocal('left', false);
      }
      if (!printSides.right.item && !(printSides.right.rows && printSides.right.rows.length) && rightQ) {
        await resolvePrintItemFromLocal('right', false);
      }
      let payloadLeft = buildPrintPayloadForSide('left');
      let payloadRight = buildPrintPayloadForSide('right');
      const leftOk = isPrintPayloadReady(payloadLeft);
      const rightOk = isPrintPayloadReady(payloadRight);
      if (!leftOk && !rightOk) {
        setPrintFetchStatus('جهّز اليسار أو اليمين على الأقل (باركود صالح) ثم اطبع.');
        return;
      }
      // طرف ناقص: انسخ الجاهز على الجهتين حتى تطلع الورقة كاملة
      if (leftOk && !rightOk) {
        payloadRight = Object.assign({}, payloadLeft);
        setPrintFetchStatus('اليمين ناقص — تُطبع نفس صنف اليسار على الجهتين.', 'right');
      } else if (rightOk && !leftOk) {
        payloadLeft = Object.assign({}, payloadRight);
        setPrintFetchStatus('اليسار ناقص — تُطبع نفس صنف اليمين على الجهتين.', 'left');
      }
      try {
        await ensureJsBarcodeLib();
      } catch (_) {
        setPrintFetchStatus('تعذر تحميل مكتبة الباركود.');
        return;
      }
      // طبّق حجم اللزقة/الورقة حسب قياسات GUI الحالية قبل الرسم
      try { applyPrintGeometryCss(getPrintGeometryMm()); } catch (_) {}
      let sheet = document.getElementById('printLabelSheet');
      if (!sheet) {
        sheet = document.createElement('div');
        sheet.id = 'printLabelSheet';
        sheet.className = 'print-label-sheet';
        document.body.appendChild(sheet);
      }
      sheet.className = 'print-label-sheet print-measure';
      sheet.innerHTML = '';
      // كل ورقة = لزق يسار + لزق يمين معاً · الكمية = عدد الأوراق
      for (let page = 0; page < pages; page++) {
        const rowEl = document.createElement('div');
        rowEl.className = 'print-label-row';
        const left = buildPrintLabelEl(payloadLeft, 'left');
        const right = buildPrintLabelEl(payloadRight, 'right');
        rowEl.appendChild(left);
        rowEl.appendChild(right);
        sheet.appendChild(rowEl);
        applyPrintLayoutToLabel(left);
        fitPrintNameDynamic(left.querySelector('.pl-name'));
        fitPrintMetaLeft(left.querySelector('.pl-orig'));
        fitPrintMetaLeft(left.querySelector('.pl-loc'));
        fitPrintBarcodeText(left.querySelector('.pl-bc-text'), payloadLeft.barcode);
        await fitPrintBarcodeDynamic(left.querySelector('.pl-bc'), payloadLeft.barcode, { height: 38 });
        applyPrintLayoutToLabel(left);

        applyPrintLayoutToLabel(right);
        fitPrintNameDynamic(right.querySelector('.pl-name'));
        fitPrintMetaLeft(right.querySelector('.pl-orig'));
        fitPrintMetaLeft(right.querySelector('.pl-loc'));
        fitPrintBarcodeText(right.querySelector('.pl-bc-text'), payloadRight.barcode);
        await fitPrintBarcodeDynamic(right.querySelector('.pl-bc'), payloadRight.barcode, { height: 38 });
        applyPrintLayoutToLabel(right);
      }
      sheet.className = 'print-label-sheet';
      const badLeft = !!(sheet.querySelector('.print-label-left[data-bc-unreliable="1"]'));
      const badRight = !!(sheet.querySelector('.print-label-right[data-bc-unreliable="1"]'));
      if (badLeft || badRight) {
        setPrintFetchStatus(
          'توقف الطباعة: باركود أطول من اللزقة بشكل موثوق (Code128 @203dpi + Quiet Zone). '
          + 'استخدم لزقة أعرض بدل طباعة باركود غير قابل للمسح.'
        );
        sheet.innerHTML = '';
        sheet.className = 'print-label-sheet';
        return;
      }
      const titleCode = (payloadLeft && payloadLeft.barcode) || (payloadRight && payloadRight.barcode) || 'labels';
      setPrintFetchStatus(
        `جاهز · ${pages} ورقة · كل ورقة لزقتين معاً · Code128 · «${RONGTA_PRINTER_NAME}» · 203dpi`
      );
      try { localStorage.setItem('preferredBarcodePrinter', RONGTA_PRINTER_NAME); } catch (_) {}
      try { savePrintFormState(); } catch (_) {}
      printJobTitleRestore = document.title;
      document.title = `Barcode-${titleCode} · ${RONGTA_PRINTER_NAME}`;
      const restoreTitle = () => {
        document.body.classList.remove('printing-labels');
        if (printJobTitleRestore != null) {
          document.title = printJobTitleRestore;
          printJobTitleRestore = null;
        }
        window.removeEventListener('afterprint', restoreTitle);
      };
      window.addEventListener('afterprint', restoreTitle);
      document.body.classList.add('printing-labels');
      setTimeout(() => {
        try {
          window.print();
        } catch (_) {
          restoreTitle();
          setPrintFetchStatus('تعذر فتح نافذة الطباعة. تأكد أن درايفر Rongta RP4xx Series مثبت.');
        }
      }, 50);
    }

    /* ===== NiceLabel-style engine: one mm model, one bitmap, 1:1 dots ===== */
    const NL_BC = { maxX: 2, quiet: [10, 7, 5], hrBandMm: 3.2, barHMaxMm: 12, barHMinMm: 5 };
    function nlDpi() { return PRINT_BARCODE_CFG.dpi || 203; }
    function nlDots(mm) { return Math.round(mm * nlDpi() / 25.4); }
    function nlHasArabic(s) {
      for (let i = 0; i < s.length; i++) {
        const c = s.charCodeAt(i);
        if (c >= 1536 && c <= 1791) return true;
      }
      return false;
    }
    function nlWrapLines(ctx, text, maxW) {
      const words = String(text).split(' ').filter(Boolean);
      const lines = [];
      let cur = '';
      words.forEach((w) => {
        const t = cur ? cur + ' ' + w : w;
        if (cur && ctx.measureText(t).width > maxW) { lines.push(cur); cur = w; }
        else cur = t;
        if (ctx.measureText(cur).width > maxW && cur.indexOf(' ') < 0) {
          let part = '';
          for (const ch of cur) {
            if (part && ctx.measureText(part + ch).width > maxW) { lines.push(part); part = ch; }
            else part += ch;
          }
          cur = part;
        }
      });
      if (cur) lines.push(cur);
      return lines;
    }
    const NL_TEXT = { ss: 3, strokeEm: 0, thresh: 160 };
    function nlDrawText(ctx, text, bx, by, bw, bh, o) {
      text = String(text == null ? '' : text);
      if (!text || bw <= 0 || bh <= 0) return;
      const S = NL_TEXT.ss;
      const tc = document.createElement('canvas');
      tc.width = Math.max(1, Math.round(bw * S));
      tc.height = Math.max(1, Math.round(bh * S));
      const t = tc.getContext('2d');
      const W = tc.width, H = tc.height;
      const fam = o.family || '"Cairo", "Tahoma", "Segoe UI", Arial, sans-serif';
      t.fillStyle = '#000';
      t.strokeStyle = '#000';
      t.lineJoin = 'round';
      t.textBaseline = 'middle';
      t.textAlign = 'center';
      t.direction = nlHasArabic(text) ? 'rtl' : 'ltr';
      const pad = 2 * S;
      let lo = o.minDot * S, hi = Math.max(o.minDot, o.maxDot) * S, best = lo;
      for (let i = 0; i < 14; i++) {
        const mid = (lo + hi) / 2;
        t.font = o.weight + ' ' + mid + 'px ' + fam;
        const lines = o.wrap ? nlWrapLines(t, text, W - pad) : [text];
        let widest = 0;
        lines.forEach((l) => { widest = Math.max(widest, t.measureText(l).width); });
        if (lines.length * mid * 1.12 <= H && widest <= W - pad) { best = mid; lo = mid; } else hi = mid;
      }
      t.font = o.weight + ' ' + best + 'px ' + fam;
      const lines = o.wrap ? nlWrapLines(t, text, W - pad) : [text];
      const lh = best * 1.12;
      let y = (H - lines.length * lh) / 2 + lh / 2;
      lines.forEach((l) => {
        t.fillText(l, W / 2, y);
        y += lh;
      });
      ctx.save();
      ctx.imageSmoothingEnabled = false;
      ctx.drawImage(tc, bx, by, bw, bh);
      ctx.restore();
    }
    function nlBinarize(c) {
      const ctx = c.getContext('2d');
      const img = ctx.getImageData(0, 0, c.width, c.height);
      const d = img.data;
      const th = NL_TEXT.thresh;
      for (let i = 0; i < d.length; i += 4) {
        const v = (d[i] * 0.299 + d[i + 1] * 0.587 + d[i + 2] * 0.114) < th ? 0 : 255;
        d[i] = v; d[i + 1] = v; d[i + 2] = v; d[i + 3] = 255;
      }
      ctx.putImageData(img, 0, 0);
    }
    function nlBarcodeBits(value) {
      if (typeof JsBarcode === 'undefined') return '';
      const c = document.createElement('canvas');
      try {
        JsBarcode(c, value, {
          format: 'CODE128', width: 1, height: 8, margin: 0, displayValue: false,
          background: '#ffffff', lineColor: '#000000'
        });
      } catch (_) {
        try {
          JsBarcode(c, value, {
            format: 'CODE128B', width: 1, height: 8, margin: 0, displayValue: false,
            background: '#ffffff', lineColor: '#000000'
          });
        } catch (__) { return ''; }
      }
      const ctx = c.getContext('2d');
      if (!ctx || !c.width) return '';
      const d = ctx.getImageData(0, 0, c.width, 1).data;
      let bits = '';
      for (let x = 0; x < c.width; x++) bits += (d[x * 4] < 128) ? '1' : '0';
      return bits;
    }
    function nlPlanBarcode(modules, boxDots) {
      // باركود قصير: نفس السلوك السابق (عرض وحدة 2 نقطة + هامش مريح)
      for (let x = NL_BC.maxX; x >= 2; x--) {
        for (let i = 0; i < NL_BC.quiet.length; i++) {
          const q = NL_BC.quiet[i];
          if ((modules + 2 * q) * x <= boxDots) return { x: x, q: q, ok: true, fitted: false };
        }
      }
      // باركود طويل: صغّر عرض الوحدة حتى يدخل كامل الرمز + هامش صغير، بدون قص أي جزء
      const minQ = 4;
      let x = boxDots / Math.max(1, modules + 2 * minQ);
      let q = minQ;
      if (x < 0.85) {
        q = 2;
        x = boxDots / Math.max(1, modules + 2 * q);
      }
      x = Math.max(0.55, Math.min(1.9, x));
      return { x: x, q: q, ok: true, fitted: true };
    }
    function nlDrawBarcode(ctx, value, ox, oy, box, s) {
      const bits = nlBarcodeBits(value);
      if (!bits) return { ok: false, msg: 'تعذر ترميز الباركود' };
      const bx = ox + nlDots(box.x), by = oy + nlDots(box.y);
      const bw = nlDots(box.w);
      const plan = nlPlanBarcode(bits.length, bw);
      const hrD = nlDots(NL_BC.hrBandMm);
      const barMm = Math.max(NL_BC.barHMinMm, Math.min(NL_BC.barHMaxMm, box.h - NL_BC.hrBandMm));
      const barH = nlDots(barMm);
      const quietPx = Math.max(1, Math.round(plan.q * plan.x));
      let dataW = Math.max(bits.length, Math.round(bits.length * plan.x));
      if (dataW + quietPx * 2 > bw) dataW = Math.max(bits.length, bw - quietPx * 2);
      const totalW = Math.min(bw, dataW + quietPx * 2);
      const startX = bx + Math.max(0, Math.floor((bw - totalW) / 2)) + quietPx;
      const maxEnd = bx + bw - 1;
      ctx.fillStyle = '#000';
      let i = 0;
      while (i < bits.length) {
        if (bits.charAt(i) === '1') {
          let j = i;
          while (j < bits.length && bits.charAt(j) === '1') j++;
          const x0 = startX + Math.round(i * (dataW / bits.length));
          const x1 = startX + Math.round(j * (dataW / bits.length));
          const w = Math.max(1, x1 - x0);
          if (x0 <= maxEnd) ctx.fillRect(x0, by, Math.min(w, maxEnd - x0 + 1), barH);
          i = j;
        } else i++;
      }
      const k = nlDpi() / 96;
      const hrPx = Number(s && s.bcTextPx) > 0 ? Number(s.bcTextPx) : 9;
      const hrWeight = (s && s.bcTextBold === false) ? '400' : '700';
      nlDrawText(ctx, value, bx, by + barH, bw, hrD, {
        maxDot: Math.max(6, hrPx * k), minDot: 5, weight: hrWeight, wrap: false,
        family: '"Cairo", "Tahoma", "Segoe UI", Arial, sans-serif'
      });
      const usedMm = (dataW + quietPx * 2) * 25.4 / nlDpi();
      return {
        ok: true, x: plan.x, fitted: !!plan.fitted,
        msg: plan.fitted
          ? ('باركود طويل · تم تصغيره ليناسب اللزقة (' + usedMm.toFixed(1) + 'مم) بدون قص')
          : ''
      };
    }
    function nlDrawLabel(ctx, ox, oy, payload, side) {
      const s = getPrintLayoutSettings(side);
      const k = nlDpi() / 96;
      const T = (txt, x, y, w, h, maxPx, wrap, weight) => nlDrawText(
        ctx, txt, ox + nlDots(x), oy + nlDots(y), nlDots(w), nlDots(h),
        { maxDot: maxPx * k, minDot: 5 * k, weight: weight, wrap: wrap });
      T(sanitizePrintItemName(payload.name || ''), s.nameXmm, s.nameYmm, s.nameWmm, s.nameHmm, s.namePx, true, (s.nameBold === false) ? '400' : '700');
      T(payload.original || '', s.origXmm, s.origYmm, s.origWmm, s.origHmm, s.originalPx, false, (s.origBold === false) ? '400' : '600');
      T(payload.location || '', s.locXmm, s.locYmm, s.locWmm, s.locHmm, s.locationPx, false, (s.locBold === false) ? '400' : '600');
      const box = clampBarcodeBoxToLabelMm(side, {
        x: Number(s.bcXmm) + (Number(s.bcShiftXmm) || 0),
        y: Number(s.bcYmm) + (Number(s.bcShiftYmm) || 0),
        w: Number(s.bcWmm) || PRINT_ZONE_CONTENT_W_MM,
        h: Number(s.bcHmm) || PRINT_ZONE_BC_H_MM
      });
      return nlDrawBarcode(ctx, printBarcodeEncodeValue(payload.barcode), ox, oy, box, s);
    }
    function nlNewCanvas(wMm, hMm) {
      const c = document.createElement('canvas');
      c.width = nlDots(wMm); c.height = nlDots(hMm);
      const ctx = c.getContext('2d');
      ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, c.width, c.height);
      return { c: c, ctx: ctx };
    }
    function nlRenderSide(payload, side) {
      const g = nlNewCanvas(PRINT_SHEET_MM.labelW, PRINT_SHEET_MM.labelH);
      const r = nlDrawLabel(g.ctx, 0, 0, payload, side);
      nlBinarize(g.c);
      return { canvas: g.c, info: r };
    }
    function nlRenderSheet(pl, pr) {
      const g = nlNewCanvas(PRINT_SHEET_MM.sheetW, PRINT_SHEET_MM.sheetH);
      const m = PRINT_SHEET_MM.margin;
      const rl = nlDrawLabel(g.ctx, nlDots(m), nlDots(m), pl, 'left');
      const rr = nlDrawLabel(g.ctx, nlDots(m + PRINT_SHEET_MM.labelW + PRINT_SHEET_MM.gap), nlDots(m), pr, 'right');
      const warns = [];
      if (!rl.ok) warns.push('يسار: ' + rl.msg);
      if (!rr.ok) warns.push('يمين: ' + rr.msg);
      nlBinarize(g.c);
      return { canvas: g.c, ok: warns.length === 0, warns: warns };
    }

    /* preview: the exact bitmap that gets printed */
    function renderPrintSideLabelMock(side) {
      const s = printSideKey(side);
      const cap = printSideCap(s);
      const card = document.getElementById('printPreview' + cap);
      const host = document.getElementById('printLabelMock' + cap);
      if (!card || !host) return;
      const payload = buildPrintPayloadForSide(s);
      if (!isPrintPayloadReady(payload)) { card.style.display = 'none'; host.innerHTML = ''; return; }
      card.style.display = 'block';
      ensureJsBarcodeLib().then(() => {
        const r = nlRenderSide(payload, s);
        const img = new Image();
        img.src = r.canvas.toDataURL('image/png');
        img.style.cssText = 'display:block;width:' + PRINT_SHEET_MM.labelW + 'mm;height:' + PRINT_SHEET_MM.labelH
          + 'mm;image-rendering:pixelated;';
        host.innerHTML = '';
        host.appendChild(img);
        setPrintFetchStatus(r.info.ok ? (r.info.msg || ('Code128 X=' + r.info.x + ' نقطة · جاهز')) : ('تحذير: ' + r.info.msg), s);
      }).catch(() => {});
    }

    /* designer: same bitmap as the label background, zones are just drag handles */
    function fitDesignerZoneFonts(labelEl, layout, sample) {
      if (!labelEl) return;
      if (typeof JsBarcode === 'undefined') {
        ensureJsBarcodeLib().then(() => paintPrintDesignerZones(null)).catch(() => {});
        return;
      }
      const side = printSideKey(labelEl.getAttribute('data-side') || 'left');
      const r = nlRenderSide(sample, side);
      labelEl.style.backgroundImage = 'url(' + r.canvas.toDataURL('image/png') + ')';
      labelEl.style.backgroundSize = '100% 100%';
      labelEl.style.backgroundRepeat = 'no-repeat';
      labelEl.style.imageRendering = 'pixelated';
    }

    /* print */
    async function printBarcodeLabels() {
      const { pages } = updatePrintQtyHint();
      const leftQ = ((document.getElementById('printQueryLeft') || {}).value || '').trim();
      const rightQ = ((document.getElementById('printQueryRight') || {}).value || '').trim();
      if (!printSides.left.item && !(printSides.left.rows && printSides.left.rows.length) && leftQ) {
        await resolvePrintItemFromLocal('left', false);
      }
      if (!printSides.right.item && !(printSides.right.rows && printSides.right.rows.length) && rightQ) {
        await resolvePrintItemFromLocal('right', false);
      }
      let pl = buildPrintPayloadForSide('left');
      let pr = buildPrintPayloadForSide('right');
      const lOk = isPrintPayloadReady(pl), rOk = isPrintPayloadReady(pr);
      if (!lOk && !rOk) { setPrintFetchStatus('جهّز اليسار أو اليمين على الأقل (باركود صالح) ثم اطبع.'); return; }
      if (lOk && !rOk) pr = Object.assign({}, pl);
      else if (rOk && !lOk) pl = Object.assign({}, pr);
      try { await ensureJsBarcodeLib(); } catch (_) { setPrintFetchStatus('تعذر تحميل مكتبة الباركود.'); return; }
      const res = nlRenderSheet(pl, pr);
      if (!res.ok) { setPrintFetchStatus('توقفت الطباعة: ' + res.warns.join(' | ')); return; }
      const url = res.canvas.toDataURL('image/png');
      let sheet = document.getElementById('printLabelSheet');
      if (!sheet) {
        sheet = document.createElement('div');
        sheet.id = 'printLabelSheet';
        document.body.appendChild(sheet);
      }
      sheet.className = 'print-label-sheet';
      sheet.innerHTML = '';
      const imgs = [];
      for (let p = 0; p < pages; p++) {
        const img = new Image();
        img.className = 'print-sheet-img';
        img.src = url;
        sheet.appendChild(img);
        imgs.push(img);
      }
      try { await Promise.all(imgs.map((i) => (i.decode ? i.decode() : Promise.resolve()))); } catch (_) {}
      setPrintFetchStatus('جاهز · ' + pages + ' ورقة · ' + nlDpi() + 'dpi · «' + RONGTA_PRINTER_NAME + '»');
      try { savePrintFormState(); } catch (_) {}
      printJobTitleRestore = document.title;
      document.title = 'Barcode-' + (pl.barcode || pr.barcode || 'labels');
      const restoreTitle = () => {
        document.body.classList.remove('printing-labels');
        if (printJobTitleRestore != null) { document.title = printJobTitleRestore; printJobTitleRestore = null; }
        window.removeEventListener('afterprint', restoreTitle);
      };
      window.addEventListener('afterprint', restoreTitle);
      document.body.classList.add('printing-labels');
      setTimeout(() => {
        try { window.print(); }
        catch (_) { restoreTitle(); setPrintFetchStatus('تعذر فتح نافذة الطباعة.'); }
      }, 50);
    }

    async function forceHesabateFullImport() {
      const stEl = document.getElementById('hesabateSyncStatus');
      if (hesabateEnsureInflight) {
        if (stEl) stEl.textContent = 'استيراد جارٍ بالفعل... انتظر حتى ينتهي.';
        return;
      }
      if (stEl) stEl.textContent = 'جاري تحديث جميع الأصناف من حساباتي (اسم/موقع/باركود/سعر)...';
      setStats('جاري تحديث الكل من حساباتي...');
      try {
        const data = await ensureHesabateLocationSyncQuiet(true);
        if (data && data.ok === false) {
          const err = data.message || 'فشل التحديث من حساباتي.';
          if (stEl) stEl.textContent = err;
          setStats( err);
          return;
        }
        const n = data && data.rows != null ? Number(data.rows) : null;
        const st = (data && data.stats) || {};
        const msg = (data && data.message)
          || (n != null
            ? `تم تحديث الكتالوج: ${n} صنف (+${st.inserted || 0} جديد، ${st.updated || 0} معدّل).`
            : 'تم التحديث من حساباتي.');
        if (stEl) stEl.textContent = msg;
        setStats( msg);
        scheduleSharedLocationAndPrintSync('full-import');
      } finally {
        /* no-op */
      }
    }

    let html5QrLocationScanner = null;
    let locationBarcodeRunning = false;
    const LOCATION_BARCODE_SCAN_MIN_LEN = 5;
    const LOCATION_BARCODE_SEARCH_KEYS_MAX_JS = 6;
    let locScanSession = { lastCanon: null, streak: 0, pendingCanon: '', pendingRaw: '', lastCbMs: 0, window: [], accepted: false };
    let scanLockScrollY = 0;
    let scanLockTimers = [];

    function getScanScreenAngle() {
      try {
        if (screen.orientation && typeof screen.orientation.angle === 'number') {
          return ((screen.orientation.angle % 360) + 360) % 360;
        }
      } catch (_) {}
      if (typeof window.orientation === 'number') {
        return ((window.orientation % 360) + 360) % 360;
      }
      return window.innerWidth > window.innerHeight ? 90 : 0;
    }
    function scanOrientationIsLandscape() {
      if (window.innerWidth > window.innerHeight) return true;
      try {
        const type = String((screen.orientation && screen.orientation.type) || '');
        if (type.indexOf('landscape') >= 0) return true;
      } catch (_) {}
      const ang = getScanScreenAngle();
      return ang === 90 || ang === 270;
    }
    function keepScanPortrait() {
      const html = document.documentElement;
      if (!html.classList.contains('barcode-scan-open')) return;
      const landscape = scanOrientationIsLandscape();
      const ang = getScanScreenAngle();
      html.classList.toggle('scan-is-landscape', landscape);
      document.querySelectorAll('.barcode-modal-overlay').forEach((modal) => {
        modal.classList.remove('force-portrait', 'force-portrait-left');
        if (!landscape || !modal.classList.contains('active')) return;
        if (ang === 270 || ang === -90) modal.classList.add('force-portrait-left');
        else modal.classList.add('force-portrait');
      });
    }
    function scheduleKeepScanPortrait() {
      keepScanPortrait();
      scanLockTimers.forEach((id) => clearTimeout(id));
      scanLockTimers = [50, 150, 350, 700].map((ms) => setTimeout(keepScanPortrait, ms));
    }
    async function tryNativePortraitLock() {
      const ori = (typeof screen !== 'undefined') ? screen.orientation : null;
      if (ori && typeof ori.lock === 'function') {
        for (const mode of ['portrait', 'portrait-primary']) {
          try {
            await ori.lock(mode);
            return true;
          } catch (_) {}
        }
      }
      const modal = document.querySelector('.barcode-modal-overlay.active') || document.getElementById('barcodeModal');
      try {
        if (modal && modal.requestFullscreen) {
          await modal.requestFullscreen();
          if (ori && typeof ori.lock === 'function') {
            await ori.lock('portrait');
            return true;
          }
        }
      } catch (_) {}
      try {
        if (document.documentElement.webkitRequestFullscreen) {
          document.documentElement.webkitRequestFullscreen();
        }
      } catch (_) {}
      return false;
    }
    async function lockScanOrientation() {
      scanLockScrollY = window.scrollY || document.documentElement.scrollTop || 0;
      document.documentElement.classList.add('barcode-scan-open');
      document.body.style.top = '-' + scanLockScrollY + 'px';
      await tryNativePortraitLock();
      window.addEventListener('orientationchange', scheduleKeepScanPortrait);
      window.addEventListener('resize', scheduleKeepScanPortrait);
      try {
        if (screen.orientation && screen.orientation.addEventListener) {
          screen.orientation.addEventListener('change', scheduleKeepScanPortrait);
        }
      } catch (_) {}
      try {
        if (window.visualViewport) {
          window.visualViewport.addEventListener('resize', scheduleKeepScanPortrait);
        }
      } catch (_) {}
      scheduleKeepScanPortrait();
    }
    function unlockScanOrientation() {
      if (!document.documentElement.classList.contains('barcode-scan-open')) return;
      scanLockTimers.forEach((id) => clearTimeout(id));
      scanLockTimers = [];
      document.documentElement.classList.remove('barcode-scan-open', 'scan-is-landscape');
      document.body.style.top = '';
      window.removeEventListener('orientationchange', scheduleKeepScanPortrait);
      window.removeEventListener('resize', scheduleKeepScanPortrait);
      try {
        if (screen.orientation && screen.orientation.removeEventListener) {
          screen.orientation.removeEventListener('change', scheduleKeepScanPortrait);
        }
      } catch (_) {}
      try {
        if (window.visualViewport) {
          window.visualViewport.removeEventListener('resize', scheduleKeepScanPortrait);
        }
      } catch (_) {}
      document.querySelectorAll('.barcode-modal-overlay').forEach((modal) => {
        modal.classList.remove('force-portrait', 'force-portrait-left');
      });
      try { if (screen.orientation && screen.orientation.unlock) screen.orientation.unlock(); } catch (_) {}
      try {
        if (document.fullscreenElement && document.exitFullscreen) document.exitFullscreen();
        else if (document.webkitFullscreenElement && document.webkitExitFullscreen) document.webkitExitFullscreen();
      } catch (_) {}
      window.scrollTo(0, scanLockScrollY || 0);
    }

    function stripBarcodeInvisibleJs(str) {
      return String(str || '').normalize('NFKC').replace(/[\u200B-\u200D\uFEFF\u2060\u00AD]/g, '');
    }
    function translateDigitsLatinJs(str) {
      let out = '';
      for (let i = 0; i < str.length; i++) {
        const c = str.charCodeAt(i);
        if (c >= 0x0660 && c <= 0x0669) out += String.fromCharCode(c - 0x0660 + 0x30);
        else if (c >= 0x06F0 && c <= 0x06F9) out += String.fromCharCode(c - 0x06F0 + 0x30);
        else out += str.charAt(i);
      }
      return out;
    }
    function normalizeBarcodeLookupKeyClient(raw) {
      if (!raw || !String(raw).trim()) return '';
      let s = stripBarcodeInvisibleJs(raw);
      s = translateDigitsLatinJs(s).trim();
      const alnum = s.replace(/[^A-Za-z0-9]/g, '');
      if (!alnum) return '';
      const up = alnum.toUpperCase();
      if (/^\\d+$/.test(up)) {
        return up;
      }
      return up;
    }
    function barcodeSearchKeysClient(raw) {
      if (!raw || !String(raw).trim()) return [];
      let s = stripBarcodeInvisibleJs(raw);
      s = translateDigitsLatinJs(s).trim();
      const alnum = s.replace(/[^A-Za-z0-9]/g, '');
      if (!alnum) return [];
      const up = alnum.toUpperCase();
      const out = [];
      if (/^\\d+$/.test(up)) {
        const full = up;
        const stripped = full.replace(/^0+/, '') || '0';
        [full, stripped].forEach((k) => { if (k && !out.includes(k)) out.push(k); });
      } else {
        out.push(up);
        // باركود بمسافات تنسيق: الجزء الأول أيضاً (223112U001 من 223112U001        HI)
        if (/\s/.test(s)) {
          const first = s.split(/[\s,/|;\\+]+/).map((p) => p.trim()).filter(Boolean)[0] || '';
          const fk = String(first).replace(/[^A-Za-z0-9]/g, '').toUpperCase();
          if (fk && fk.length >= 6 && !out.includes(fk)) out.push(fk);
        }
      }
      return out.slice(0, LOCATION_BARCODE_SEARCH_KEYS_MAX_JS);
    }
    function lightSanitizeBarcodeValue(s) {
      return stripBarcodeInvisibleJs(String(s || '')).trim();
    }
    function sanitizeScanResult(raw) {
      let t = stripBarcodeInvisibleJs(String(raw || ''));
      t = t.replace(/^[\\s\\u00A0\\-–—:_./\\\\]+|[\\s\\u00A0\\-–—:_./\\\\]+$/g, '').trim();
      return t;
    }
    function logBarcodeClientDebug() {
      try {
        if (localStorage.getItem('locationBarcodeDebug') === '1')
          console.log.apply(console, ['[barcode-ui]'].concat([].slice.call(arguments)));
      } catch (_) {}
    }
    function setBarcodeScanStatus(text) {
      const el = document.getElementById('barcodeScanStatus');
      if (el) el.innerText = text || '';
    }
    function setBarcodeScanDebug(raw, canon) {
      const el = document.getElementById('barcodeScanDebug');
      if (!el) return;
      if (!raw && !canon) {
        el.style.display = 'none';
        el.innerText = '';
        return;
      }
      el.style.display = 'block';
      const r = String(raw || '');
      const short = r.length > 52 ? r.slice(0, 50) + '…' : r;
      el.innerText = 'آخر قراءة: ' + short + ' ← بعد التطبيع: ' + String(canon || '');
    }

    async function acceptLocationScanAndSearch(canon, raw) {
      if (locScanSession.accepted) return;
      locScanSession.accepted = true;
      logBarcodeClientDebug('accept-scan', { raw, canon });
      const inp = document.getElementById('locationBarcodeQueryInput');
      if (inp) inp.value = canon;
      setBarcodeScanStatus('تم التقاط الباركود');
      try { if (navigator.vibrate) navigator.vibrate(18); } catch (_) {}
      await closeBarcodeScanner();
      fetchOneItemFromHesabate();
    }

    async function openLocationBarcodeScanner() {
      if (activeSearchTab !== 'location') {
        activeSearchTab = 'location';
        try { localStorage.setItem('activeSearchTab', 'location'); } catch (_) {}
        document.getElementById('tabMainBtn').classList.toggle('active', false);
        document.getElementById('tabSizeBtn').classList.toggle('active', false);
        document.getElementById('tabWiperBtn').classList.toggle('active', false);
        document.getElementById('tabLocBtn').classList.toggle('active', true);
        document.getElementById('mainSearchPanel').classList.toggle('active', false);
        document.getElementById('sizeSearchPanel').classList.toggle('active', false);
        document.getElementById('wiperSearchPanel').classList.toggle('active', false);
        document.getElementById('locationSearchPanel').classList.toggle('active', true);
      }
      const modal = document.getElementById('barcodeModal');
      const regionId = 'barcodeScannerRegion';
      setBarcodeScanStatus('جاري تحميل قارئ الباركود...');
      try {
        await ensureQrLib();
      } catch (_) {
        alert('تعذر تحميل مكتبة قراءة الباركود. تحقق من الاتصال.');
        return;
      }
      if (typeof Html5Qrcode === 'undefined') {
        alert('تعذر تحميل مكتبة قراءة الباركود. تحقق من الاتصال.');
        return;
      }
      await closeBarcodeScanner();
      locScanSession = { lastCanon: null, streak: 0, pendingCanon: '', pendingRaw: '', lastCbMs: 0, window: [], accepted: false };
      modal.classList.add('active');
      modal.setAttribute('aria-hidden', 'false');
      await lockScanOrientation();
      setBarcodeScanStatus('وجّه الكاميرا على الباركود');
      setBarcodeScanDebug('', '');
      const confirmBtn = document.getElementById('barcodeScanConfirmBtn');
      if (confirmBtn) {
        confirmBtn.disabled = true;
        confirmBtn.onclick = () => {
          if (locScanSession.pendingCanon) {
            acceptLocationScanAndSearch(locScanSession.pendingCanon, locScanSession.pendingRaw || '');
          }
        };
      }
      document.getElementById(regionId).innerHTML = '';
      const scanner = new Html5Qrcode(regionId);
      html5QrLocationScanner = scanner;
      const fmt = (typeof Html5QrcodeSupportedFormats !== 'undefined')
        ? [
            Html5QrcodeSupportedFormats.CODE_128,
            Html5QrcodeSupportedFormats.CODE_39,
            Html5QrcodeSupportedFormats.CODE_93,
            Html5QrcodeSupportedFormats.ITF,
            Html5QrcodeSupportedFormats.EAN_13,
            Html5QrcodeSupportedFormats.EAN_8,
            Html5QrcodeSupportedFormats.UPC_A,
            Html5QrcodeSupportedFormats.UPC_E
          ]
        : null;
      const cfg = {
        fps: 16,
        aspectRatio: 1.777,
        disableFlip: true,
        useBarCodeDetectorIfSupported: true,
        qrbox: (viewfinderWidth, viewfinderHeight) => {
          const vw = Math.max(1, viewfinderWidth);
          const vh = Math.max(1, viewfinderHeight);
          const w = Math.min(vw - 8, Math.floor(vw * 0.98));
          const h = Math.max(88, Math.min(220, Math.floor(vh * 0.4)));
          return { width: w, height: Math.min(h, vh - 16) };
        }
      };
      if (fmt) cfg.formatsToSupport = fmt;
      function scanLooksLikeQr(decodedResult) {
        if (!decodedResult || typeof decodedResult !== 'object') return false;
        const r = decodedResult.result || decodedResult;
        const f = r && (r.format != null ? r.format : r.barcodeFormat);
        if (f == null) return false;
        const s = String(f).toUpperCase();
        if (s.includes('QR') || s.includes('AZTEC') || s.includes('DATA_MATRIX')) return true;
        return false;
      }
      const onDecode = (decodedText, decodedResult) => {
        if (scanLooksLikeQr(decodedResult)) return;
        const now = Date.now();
        if (now - locScanSession.lastCbMs < 30) return;
        locScanSession.lastCbMs = now;
        const raw = sanitizeScanResult(decodedText);
        if (!raw) return;
        const canon = normalizeBarcodeLookupKeyClient(raw);
        logBarcodeClientDebug('scan-frame', { raw, canon });
        setBarcodeScanDebug(raw, canon);
        if (!canon || canon.length < LOCATION_BARCODE_SCAN_MIN_LEN) {
          setBarcodeScanStatus('قراءة قصيرة — حرّك الكاميرا قليلاً');
          return;
        }
        if (canon !== locScanSession.lastCanon) {
          locScanSession.lastCanon = canon;
          locScanSession.streak = 1;
        } else {
          locScanSession.streak += 1;
        }
        locScanSession.pendingCanon = canon;
        locScanSession.pendingRaw = raw;
        if (confirmBtn) confirmBtn.disabled = false;
        setBarcodeScanStatus('تم التقاط القراءة');
        locScanSession.window.push({ t: now, c: canon });
        locScanSession.window = locScanSession.window.filter((x) => now - x.t < 400);
        const sameInWindow = locScanSession.window.filter((x) => x.c === canon).length;
        const confident = canon.length >= 8;
        if (confident || locScanSession.streak >= 2 || sameInWindow >= 2) {
          acceptLocationScanAndSearch(canon, raw);
        }
      };
      const noopScan = () => {};
      try {
        await scanner.start({ facingMode: { exact: 'environment' } }, cfg, onDecode, noopScan);
        locationBarcodeRunning = true;
      } catch (e0) {
        try {
          await scanner.start({ facingMode: 'environment' }, cfg, onDecode, noopScan);
          locationBarcodeRunning = true;
        } catch (e1) {
          try {
            const cfgLite = Object.assign({}, cfg);
            cfgLite.fps = 12;
            await scanner.start({ facingMode: 'environment' }, cfgLite, onDecode, noopScan);
            locationBarcodeRunning = true;
          } catch (e2) {
            modal.classList.remove('active');
            modal.setAttribute('aria-hidden', 'true');
            html5QrLocationScanner = null;
            unlockScanOrientation();
            const msg = (e2 && e2.message) ? e2.message : String(e2);
            alert('تعذر فتح الكاميرا. استخدم HTTPS واسمح بالكاميرا. ' + msg);
          }
        }
      }
    }

    async function closeBarcodeScanner() {
      locScanSession.accepted = true;
      const modal = document.getElementById('barcodeModal');
      if (modal) {
        modal.classList.remove('active');
        modal.setAttribute('aria-hidden', 'true');
      }
      unlockScanOrientation();
      if (html5QrLocationScanner && locationBarcodeRunning) {
        try { await html5QrLocationScanner.stop(); } catch (_) {}
        try { await html5QrLocationScanner.clear(); } catch (_) {}
      }
      locationBarcodeRunning = false;
      html5QrLocationScanner = null;
      const el = document.getElementById('barcodeScannerRegion');
      if (el) el.innerHTML = '';
      const confirmBtn = document.getElementById('barcodeScanConfirmBtn');
      if (confirmBtn) {
        confirmBtn.disabled = true;
        confirmBtn.onclick = null;
      }
      locScanSession = { lastCanon: null, streak: 0, pendingCanon: '', pendingRaw: '', lastCbMs: 0, window: [], accepted: false };
    }

    async function uploadWiperExcelFromInput(inputId, statusId) {
      const inp = document.getElementById(inputId);
      const st = document.getElementById(statusId);
      if (!inp || !inp.files || !inp.files[0]) {
        if (st) st.innerText = 'اختَر ملف Excel أولاً.';
        return;
      }
      const f = inp.files[0];
      if (st) st.innerText = 'جاري الرفع والمعالجة...';
      try {
        let data = null;
        if (f.size > VERCEL_SAFE_UPLOAD_BYTES) {
          if (st) st.innerText = 'ملف كبير — جاري التحليل والرفع على دفعات (مناسب لـ Vercel)...';
          data = await uploadWiperSheetChunked(f);
        } else {
          const formData = new FormData();
          formData.append('files', f, f.name);
          const res = await fetch('/upload', { method: 'POST', body: formData });
          data = await readJsonSafe(res);
          if (res.status === 413 || (!res.ok && String(data.detail || '').toLowerCase().includes('too large'))) {
            if (st) st.innerText = 'الطلب كبير — إعادة المحاولة على دفعات...';
            data = await uploadWiperSheetChunked(f);
          } else if (!res.ok) {
            throw new Error(data.detail || 'upload-wiper-failed');
          }
        }
        const skipped = Number(data.skipped_files || 0) > 0;
        const wiper = Number(data.inserted_wiper_rows || 0);
        const proc = Number(data.files_count || 0);
        if (skipped && wiper === 0 && proc === 0) {
          if (st) st.innerText = 'نفس الملف مرفوع مسبقاً — لم يُعد الاستيراد.';
        } else if (wiper > 0) {
          if (st) st.innerText = `تم استيراد ${wiper} صف قشطان.`;
        } else {
          if (st) {
            st.innerText =
              'لم يُستورد كقشطان. غيّر اسم الملف ليشمل «قشط» أو «wiper» أو أضف في الصف الأول عناوين قشط/مساح، ثم أعد المحاولة.';
          }
        }
        inp.value = '';
        await loadStats();
        await searchNow('wiper');
      } catch (err) {
        if (st) st.innerText = 'فشل الرفع: ' + ((err && err.message) ? err.message : err);
      }
    }

    async function uploadWiperSheetFile() {
      await uploadWiperExcelFromInput('wiperSheetFileInput', 'wiperUploadStatus');
    }

    async function uploadLocationSheetFile() {
      const inp = document.getElementById('locationSheetFileInput');
      const st = document.getElementById('locationUploadStatus');
      if (!inp || !inp.files || !inp.files[0]) {
        if (st) st.innerText = 'اختَر ملف Excel أولاً.';
        return;
      }
      const f = inp.files[0];
      if (st) st.innerText = 'جاري الرفع والمعالجة...';
      try {
        let data = null;
        if (f.size > VERCEL_SAFE_UPLOAD_BYTES) {
          if (st) st.innerText = 'ملف كبير — جاري التحليل والرفع على دفعات (مناسب لـ Vercel)...';
          data = await uploadLocationSheetChunked(f);
        } else {
          const fd = new FormData();
          fd.append('file', f, f.name);
          const res = await fetch('/upload_location_sheet', { method: 'POST', body: fd });
          data = await readJsonSafe(res);
          if (res.status === 413 || (!res.ok && String(data.detail || '').toLowerCase().includes('too large'))) {
            if (st) st.innerText = 'الطلب كبير — إعادة المحاولة على دفعات...';
            data = await uploadLocationSheetChunked(f);
          } else if (!res.ok) {
            throw new Error(data.detail || 'upload-location-failed');
          }
        }
        if (data.skipped) {
          if (st) st.innerText = 'نفس الملف مرفوع مسبقاً — لم يُعد الاستيراد.';
        } else if (data.mode === 'merge_shelves' || data.message) {
          if (st) st.innerText = data.message || `تم تحديث مواقع الرف لـ ${Number(data.inserted_rows || 0)} صنف.`;
        } else {
          if (st) st.innerText = `تم استيراد ${Number(data.inserted_rows || 0)} صف موقع.`;
        }
        inp.value = '';
        await loadStats();
        await searchNow('location');
      } catch (err) {
        if (st) st.innerText = 'فشل الرفع: ' + ((err && err.message) ? err.message : err);
      }
    }

    function registerServiceWorker() {
      if (!('serviceWorker' in navigator)) return;
      navigator.serviceWorker.register('/sw.js').then((reg) => {
        reg.update().catch(() => {});
        reg.addEventListener('updatefound', () => {
          const newWorker = reg.installing;
          if (!newWorker) return;
          newWorker.addEventListener('statechange', () => {
            if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
              setSyncInfo('يوجد تحديث للتطبيق. أغلقه وافتحه من جديد عند رغبتك.');
            }
          });
        });
      }).catch(() => {
        // ignore service-worker registration errors
      });
    }

    async function ensureLatestClientVersion() {
      try {
        const res = await fetch(`/health?ts=${Date.now()}`, { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json();
        const serverVersion = String(data.app_version || '').trim();
        if (!serverVersion) return;

        const localVersion = localStorage.getItem('app_version') || '';
        localStorage.setItem('app_version', serverVersion);
        if (localVersion && localVersion !== serverVersion && 'serviceWorker' in navigator) {
          try {
            const regs = await navigator.serviceWorker.getRegistrations();
            await Promise.all(regs.map(r => r.update()));
          } catch (_) {}
        }
      } catch (_) {
        // ignore version-check errors
      }
    }

    function openOfflineDb() {
      return new Promise((resolve, reject) => {
        const req = indexedDB.open(OFFLINE_DB_NAME, 1);
        req.onupgradeneeded = () => {
          const db = req.result;
          if (!db.objectStoreNames.contains(OFFLINE_DB_STORE)) {
            db.createObjectStore(OFFLINE_DB_STORE);
          }
        };
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      });
    }

    async function ensureLocalSnapshotLoaded() {
      if (snapshotLoadPromise) return snapshotLoadPromise;
      snapshotLoadPromise = loadLocalSnapshot();
      try {
        await snapshotLoadPromise;
      } catch (_) {
        snapshotLoadPromise = null;
      }
    }

    async function loadLocalSnapshot() {
      try {
        const db = await openOfflineDb();
        const snapshot = await new Promise((resolve, reject) => {
          const tx = db.transaction(OFFLINE_DB_STORE, 'readonly');
          const req = tx.objectStore(OFFLINE_DB_STORE).get(OFFLINE_DB_KEY);
          req.onsuccess = () => resolve(req.result || null);
          req.onerror = () => reject(req.error);
        });
        db.close();
        if (snapshot && Array.isArray(snapshot.rows)) {
          localSnapshot = snapshot;
          setSyncInfo(`نسخة محلية متوفرة: ${snapshot.total_rows || snapshot.rows.length} صف (إصدار ${snapshot.version || '-'})`);
        } else {
          setSyncInfo('لا توجد نسخة محلية بعد. اعمل مزامنة للهاتف.');
        }
      } catch (_) {
        setSyncInfo('تعذر تحميل النسخة المحلية من الجهاز.');
      }
    }

    async function saveLocalSnapshot(snapshot) {
      const payload = {
        version: snapshot.version || '',
        updated_at: snapshot.updated_at || '',
        total_rows: Number(snapshot.total_rows || (snapshot.rows || []).length),
        rows: Array.isArray(snapshot.rows) ? snapshot.rows : []
      };
      const db = await openOfflineDb();
      await new Promise((resolve, reject) => {
        const tx = db.transaction(OFFLINE_DB_STORE, 'readwrite');
        tx.objectStore(OFFLINE_DB_STORE).put(payload, OFFLINE_DB_KEY);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
      });
      db.close();
      localSnapshot = payload;
      setSyncInfo(`تمت مزامنة النسخة المحلية: ${payload.total_rows} صف (إصدار ${payload.version || '-'})`);
    }

    function excelOnly(files) {
      return Array.from(files).filter(f => /\\.(xlsx|xlsm|xls|xlsb)$/i.test(f.name));
    }

    function sourceKeyForFile(file) {
      return String(file.webkitRelativePath || file.relativePath || file.name || '')
        .replace(/\\\\/g, '/')
        .replace(/^\\/+/, '')
        .trim() || String(file.name || 'unnamed.xlsx');
    }

    function fileKey(file) {
      // Keep every file entry unique to avoid accidental overwrite
      // when different files share same name/size/date metadata.
      return `${sourceKeyForFile(file)}::${file.size}::${file.lastModified}::${selectedFileSerial++}`;
    }

    function updateSelectionInfo() {
      const count = selectedFilesMap.size;
      document.getElementById('selectionInfo').innerText = `الملفات: ${count}`;
    }

    function appendFilesToSelection(files) {
      excelOnly(files).forEach(file => {
        selectedFilesMap.set(fileKey(file), file);
      });
      updateSelectionInfo();
    }

    function clearSelectedFiles() {
      selectedFilesMap.clear();
      updateSelectionInfo();
      setUploadProgress(0, '');
      document.getElementById('uploadResult').innerText = 'تم تفريغ الاختيارات.';
    }

    function setUploadProgress(percent, message) {
      const p = Math.max(0, Math.min(100, percent));
      document.getElementById('progressBar').style.width = `${p}%`;
      document.getElementById('progressInfo').innerText = message || '';
    }

    function readFileFromEntry(entry) {
      return new Promise((resolve) => {
        entry.file((file) => {
          try {
            file.relativePath = String(entry.fullPath || file.webkitRelativePath || file.name || '').replace(/^\\/+/, '');
          } catch (_) {}
          resolve([file]);
        }, () => resolve([]));
      });
    }

    function readEntriesOnce(reader) {
      return new Promise((resolve) => {
        reader.readEntries((entries) => resolve(entries || []), () => resolve([]));
      });
    }

    async function collectFilesFromEntry(entry) {
      if (!entry) return [];
      if (entry.isFile) {
        return await readFileFromEntry(entry);
      }
      if (!entry.isDirectory) return [];

      const reader = entry.createReader();
      const allEntries = [];
      while (true) {
        const batch = await readEntriesOnce(reader);
        if (!batch.length) break;
        allEntries.push(...batch);
      }

      const collected = [];
      for (const child of allEntries) {
        const files = await collectFilesFromEntry(child);
        collected.push(...files);
      }
      return collected;
    }

    async function collectFilesFromDirectoryHandle(handle, parentPath = '') {
      const files = [];
      for await (const [name, child] of handle.entries()) {
        const nextPath = parentPath ? `${parentPath}/${name}` : name;
        if (child.kind === 'file') {
          const file = await child.getFile();
          try { file.relativePath = nextPath; } catch (_) {}
          files.push(file);
        } else if (child.kind === 'directory') {
          const nested = await collectFilesFromDirectoryHandle(child, nextPath);
          files.push(...nested);
        }
      }
      return files;
    }

    async function pickFolderDeep() {
      if (!window.showDirectoryPicker) {
        document.getElementById('uploadResult').innerText = 'متصفحك لا يدعم اختيار المجلد العميق. استخدم السحب والإفلات.';
        return;
      }
      try {
        const root = await window.showDirectoryPicker();
        const allFiles = await collectFilesFromDirectoryHandle(root);
        appendFilesToSelection(allFiles);
        document.getElementById('uploadResult').innerText = `تمت إضافة ${allFiles.length} ملف من المجلد الرئيسي وكل المجلدات الفرعية.`;
      } catch (_) {
        // User canceled picker or permission denied.
      }
    }

    async function handleDrop(e) {
      e.preventDefault();
      const dropZone = document.getElementById('dropZone');
      if (dropZone) dropZone.classList.remove('active');

      const dt = e.dataTransfer;
      if (!dt) return;

      const filesToAdd = [];
      const items = Array.from(dt.items || []);
      if (items.length && items.some(i => typeof i.webkitGetAsEntry === 'function')) {
        for (const item of items) {
          const entry = item.webkitGetAsEntry ? item.webkitGetAsEntry() : null;
          if (!entry) continue;
          const files = await collectFilesFromEntry(entry);
          filesToAdd.push(...files);
        }
      } else {
        filesToAdd.push(...Array.from(dt.files || []));
      }

      appendFilesToSelection(filesToAdd);
      document.getElementById('uploadResult').innerText = 'تمت إضافة العناصر المسحوبة. اضغط رفع الملفات.';
    }

    function getHeaderIndexJs(headers, keywords) {
      const hs = (headers || []).map(h => normalizeTextForSearch(h));
      const ks = keywords.map(k => normalizeTextForSearch(k));
      for (let i = 0; i < hs.length; i++) {
        if (ks.includes(hs[i])) return i;
      }
      for (let i = 0; i < hs.length; i++) {
        if (ks.some(k => hs[i].includes(k))) return i;
      }
      return null;
    }

    function headerHasOriginalMarkerJs(h) {
      return h.includes('اصلي') || h.includes('أصلي') || h.includes('اصليه');
    }
    function headerBadForOriginalJs(h) {
      if (!h) return false;
      if (h.includes('متشابه') || h.includes('تشابه')) return !headerHasOriginalMarkerJs(h);
      if (h.includes('البدائل') || h.includes('بدائل')) return !headerHasOriginalMarkerJs(h);
      return false;
    }
    function headerBadForAltJs(h) {
      if (!h) return false;
      if (h.includes('رقم الشركات') || h.includes('رقم الشركة')) return true;
      if (h.includes('رقم شركة') && !h.includes('بديل') && !h.includes('متشابه') && !h.includes('بدائل') && !h.includes('تشابه')) return true;
      if (headerHasOriginalMarkerJs(h) && !h.includes('متشابه') && !h.includes('بديل') && !h.includes('بدائل') && !h.includes('تشابه')) return true;
      return false;
    }
    function headerBadForCompanyJs(h) {
      if (!h) return false;
      if (h.includes('رقم الشركات') || h.includes('رقم الشركة')) return false;
      if (h.includes('شركات') && h.includes('رقم')) return false;
      if (h.includes('شركة') && h.includes('رقم') && !h.includes('نوع')) return false;
      if (h.includes('نوع') && !h.includes('شركة') && !h.includes('شركات')) {
        if (h.includes('قطعه') || h.includes('قطعة') || h.includes('القطعه') || h.includes('القطعة') || h.includes('صنف')) return false;
        return true;
      }
      if (h === 'الصنف' || h === 'صنف' || (h.length <= 12 && h.includes('صنف') && !h.includes('رقم') && !h.includes('شرك'))) return true;
      return false;
    }

    function findHeaderColumnJs(headers, keywords, usedSet, minSubstringLen, rejectFn) {
      const hs = (headers || []).map(h => normalizeTextForSearch(h));
      const ks = [...new Set(keywords.map(k => normalizeTextForSearch(k)).filter(Boolean))]
        .sort((a, b) => b.length - a.length);
      const used = usedSet || new Set();
      for (let i = 0; i < hs.length; i++) {
        if (used.has(i) || !hs[i]) continue;
        if (rejectFn && rejectFn(hs[i])) continue;
        if (ks.includes(hs[i])) return i;
      }
      let bestIdx = null;
      let bestLen = -1;
      let bestI = 9999;
      for (let i = 0; i < hs.length; i++) {
        if (used.has(i) || !hs[i]) continue;
        const h = hs[i];
        if (rejectFn && rejectFn(h)) continue;
        for (const kw of ks) {
          if (kw.length < minSubstringLen) continue;
          if (h.includes(kw)) {
            if (kw.length > bestLen || (kw.length === bestLen && i < bestI)) {
              bestLen = kw.length;
              bestI = i;
              bestIdx = i;
            }
          }
        }
      }
      return bestIdx;
    }

    const HEADER_COMPANY_JS = ['رقم الشركات', 'رقم الشركة', 'رقم الشركه', 'رقم شركة', 'رقم شركات', 'ارقام الشركات', 'أرقام الشركات', 'ارقام شركات', 'أرقام شركات', 'ارقام الشركه', 'أرقام الشركه', 'company number', 'رقم الشركات الموحد', 'رقم المرجع', 'المرجع', 'مرجع الشركة', 'كود الشركة', 'كود الصنف', 'رقم الصنف التجاري', 'الصنف التجاري', 'الكود التجاري', 'كود المنتج', 'رقم المنتج التجاري', 'رقم التاجر', 'رقم المورد', 'كود التخزين', 'رقم التعريف', 'رقم مرجع', 'part number', 'reference', 'stock code', 'sku'];
    const HEADER_ORIGINAL_JS = ['الرقم الاصلي', 'الرقم الأصلي', 'رقم اصلي', 'رقم أصلي', 'ارقام اصلية', 'أرقام أصلية', 'ارقام أصلية', 'أرقام اصلية', 'original number', 'الرقم الاصلي للقطعة', 'الرقم الأصلي للقطعة'];
    const HEADER_ALT_JS = ['متشابهات', 'متشابهاب', 'المتشابهات', 'التشابهات', 'المشابهات', 'المشابهين', 'البدائل', 'بدائل', 'البديلة', 'السيارات البديلة', 'السيارة البديلة', 'سيارات مشابهة', 'سيارة مشابهة', 'alternatives', 'alternative', 'سيارات بديلة', 'السيارات المعادلة'];
    const HEADER_ITEM_NAME_JS = ['اسم السيارة', 'اسم الصنف', 'اسم السيارة/الصنف', 'النوع', 'نوع', 'نوع السيارة', 'نوع المركبة', 'نوع المركبه', 'الصنف', 'item name', 'name'];
    const HEADER_ITEM_NUMBER_JS = ['رقم الصنف', 'رقم القطعة', 'رقم الصنف الدولي', 'item number', 'part number'];
    const HEADER_NOTES_JS = ['ملاحظات', 'ملاحظة', 'notes'];

    function forwardFillHeaderRowJs(rawRow) {
      const cleaned = (rawRow || []).map(v => String(v || '').trim());
      const out = [];
      let carry = '';
      for (const s of cleaned) {
        if (s) {
          carry = s;
          out.push(s);
        } else {
          out.push(carry);
        }
      }
      return out;
    }

    function sanitizeAlternativesVsOriginalJs(alternatives, originalNumbers) {
      const a = String(alternatives || '').trim();
      if (!a) return '';
      const o = String(originalNumbers || '').trim();
      if (!o) return a;
      if (normalizeTextForSearch(a) === normalizeTextForSearch(o)) return '';
      if (normalizeTextCompactJs(a) === normalizeTextCompactJs(o)) return '';
      return a;
    }

    function normalizeHeaderCellForMatchJs(raw) {
      let t = normalizeTextForSearch(String(raw || ''));
      t = t.replace(/\u200c|\u200b|\ufeff|\xa0/g, '');
      return t.replace(/\\s+/g, ' ').trim();
    }

    function findCompanyColumnByExactHeaderJs(header) {
      const canon = new Set([
        normalizeTextForSearch('رقم الشركات'),
        normalizeTextForSearch('رقم الشركة'),
        normalizeTextForSearch('رقم شركات'),
        normalizeTextForSearch('ارقام الشركات'),
        normalizeTextForSearch('أرقام الشركات'),
        normalizeTextForSearch('ارقام الشركة'),
        normalizeTextForSearch('أرقام الشركة')
      ]);
      for (let i = 0; i < header.length; i++) {
        const h = normalizeHeaderCellForMatchJs(header[i]);
        if (!h) continue;
        if (canon.has(h)) return i;
        if (h.length <= 32 && h.includes('رقم') && (h.includes('شركات') || h.endsWith('شركه')) && !headerBadForCompanyJs(h)) return i;
      }
      return null;
    }

    function resolveProductColumnIndicesJs(header) {
      const used = new Set();
      function assign(kws, minSub, rejectFn) {
        const idx = findHeaderColumnJs(header, kws, used, minSub, rejectFn);
        if (idx !== null && idx !== undefined) used.add(idx);
        return idx;
      }
      let companyIdx = findCompanyColumnByExactHeaderJs(header);
      if (companyIdx !== null && companyIdx !== undefined) {
        used.add(companyIdx);
      } else {
        companyIdx = assign(HEADER_COMPANY_JS, 6, headerBadForCompanyJs);
      }
      const originalIdx = assign(HEADER_ORIGINAL_JS, 6, headerBadForOriginalJs);
      let altIdx = assign(HEADER_ALT_JS, 5, headerBadForAltJs);
      const itemNameIdx = assign(HEADER_ITEM_NAME_JS, 4);
      const notesIdx = assign(HEADER_NOTES_JS, 4);
      let itemNumIdx = findHeaderColumnJs(header, HEADER_ITEM_NUMBER_JS, used, 6);
      if (itemNumIdx !== null && itemNumIdx !== undefined) used.add(itemNumIdx);
      else if (companyIdx !== null && companyIdx !== undefined) itemNumIdx = companyIdx;
      else itemNumIdx = 4;
      if (originalIdx != null && originalIdx !== undefined && altIdx === originalIdx) altIdx = null;
      if (companyIdx === null || companyIdx === undefined) {
        const skipLoose = new Set([originalIdx, altIdx, notesIdx].filter((x) => x !== null && x !== undefined));
        for (let i = 0; i < header.length; i++) {
          if (skipLoose.has(i)) continue;
          const h = normalizeTextForSearch(String(header[i] || ''));
          if (!h) continue;
          if ((h.includes('رقم') || h.includes('كود') || h.includes('ارقام')) && (h.includes('شركات') || h.includes('شركه'))) {
            if (headerBadForCompanyJs(h)) continue;
            companyIdx = i;
            break;
          }
        }
      }
      if ((companyIdx === null || companyIdx === undefined) && originalIdx != null && originalIdx !== undefined) {
        const blocked = new Set([itemNameIdx, altIdx, originalIdx, notesIdx].filter((x) => x !== null && x !== undefined));
        for (const delta of [1, -1, 2, -2]) {
          const fb = originalIdx + delta;
          if (fb < 0 || blocked.has(fb) || fb >= header.length) continue;
          companyIdx = fb;
          break;
        }
      }
      return { companyIdx, originalIdx, altIdx, itemNameIdx, itemNumIdx, notesIdx };
    }

    function pickItemNameJs(row, preferredIdx) {
      const val = (i) => (i !== null && i !== undefined && i >= 0 ? String(row[i] || '').trim() : '');
      const primary = val(preferredIdx);
      if (primary) return primary;

      for (const i of [1, 0, 2, 3, 4, 5]) {
        const v = val(i);
        if (!v) continue;
        if (/[A-Za-z\u0600-\u06FF]/.test(v)) return v;
      }
      for (let i = 0; i < Math.min(10, row.length || 0); i++) {
        const v = val(i);
        if (v) return v;
      }
      return '';
    }

    async function sha256Hex(arrayBuffer) {
      const hash = await crypto.subtle.digest('SHA-256', arrayBuffer);
      const bytes = Array.from(new Uint8Array(hash));
      return bytes.map(b => b.toString(16).padStart(2, '0')).join('');
    }

    function cleanCellUploadJs(v) {
      if (v == null || v === undefined) return '';
      const t = String(v).trim();
      return t.toLowerCase() === 'none' ? '' : t;
    }

    function locationRowLooksLikeHeaderJs(cells) {
      const samples = cells.slice(0, 11).map(cleanCellUploadJs);
      const joined = normalizeTextForSearch(samples.join(' '));
      let hits = 0;
      const kws = ['صنف', 'اسم', 'موقع', 'باركود', 'شركة', 'منتج', 'مكان', 'علامه', 'ملاحظ', 'سعر', 'كميه', 'كمية', 'location', 'barcode', 'company', 'brand', 'price'];
      for (const kw of kws) {
        if (joined.includes(normalizeTextForSearch(kw))) hits++;
      }
      return hits >= 2;
    }

    function wiperRowLooksLikeHeaderJs(cells) {
      const samples = cells.slice(0, 5).map(cleanCellUploadJs);
      if (samples[0] && samples[0].length > 48) return false;
      const joined = normalizeTextForSearch(samples.join(' '));
      let hits = 0;
      for (const kw of ['اسم', 'سياره', 'سيارة', 'موديل', 'ماتور', 'محرك', 'موقع', 'قشاط', 'قشط', 'رقم']) {
        if (joined.includes(normalizeTextForSearch(kw))) hits++;
      }
      if (hits >= 3) return true;
      if (joined.includes(normalizeTextForSearch('اسم')) && joined.includes(normalizeTextForSearch('موديل'))) return true;
      const hasW = ['قشاط', 'قشط', 'مساح', 'ماسحه'].some((k) => joined.includes(normalizeTextForSearch(k)));
      const hasLoc = joined.includes(normalizeTextForSearch('موقع')) || joined.includes(normalizeTextForSearch('رقم'));
      if (hasW && hasLoc) return true;
      return false;
    }

    async function parseLocationRowsForChunkUpload(file) {
      await ensureXlsxLib();
      const arrayBuffer = await file.arrayBuffer();
      const wb = XLSX.read(arrayBuffer, { type: 'array' });
      const rows = [];
      for (const sheetName of wb.SheetNames) {
        const ws = wb.Sheets[sheetName];
        const grid = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
        if (!grid.length) continue;
        const firstRow = (grid[0] || []).map((x) => cleanCellUploadJs(x));
        const dataStart = locationRowLooksLikeHeaderJs(firstRow) ? 1 : 0;
        for (let ri = dataStart; ri < grid.length; ri++) {
          const r = grid[ri] || [];
          const vals = [];
          for (let c = 0; c < 12; c++) vals.push(cleanCellUploadJs(r[c]));
          const original_number = vals[1];
          const item_name = vals[2];
          const barcode = vals[3];
          const brand = vals[4];
          const company = vals[5];
          const quantity = vals[6];
          const location = vals[8];
          const notes = vals[9];
          const unit_barcodes = vals[10];
          const price = vals[11];
          if (!item_name && !location && !company && !original_number && !barcode && !unit_barcodes && !brand && !notes && !quantity && !price) continue;
          rows.push({
            item_name,
            location,
            company,
            original_number,
            barcode,
            unit_barcodes,
            brand,
            notes,
            quantity,
            price,
            source_sheet: String(sheetName)
          });
        }
      }
      return {
        file_size: arrayBuffer.byteLength,
        content_hash: await sha256Hex(arrayBuffer),
        rows
      };
    }

    async function parseWiperRowsForChunkUpload(file) {
      await ensureXlsxLib();
      const arrayBuffer = await file.arrayBuffer();
      const wb = XLSX.read(arrayBuffer, { type: 'array' });
      const sourceKey = sourceKeyForFile(file);
      const rows = [];
      for (const sheetName of wb.SheetNames) {
        const ws = wb.Sheets[sheetName];
        const grid = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
        if (!grid.length) continue;
        const firstRow = (grid[0] || []).map((x) => cleanCellUploadJs(x));
        const dataStart = wiperRowLooksLikeHeaderJs(firstRow) ? 1 : 0;
        for (let ri = dataStart; ri < grid.length; ri++) {
          const r = grid[ri] || [];
          const vals = [];
          for (let c = 0; c < 5; c++) vals.push(cleanCellUploadJs(r[c]));
          const car_name = vals[0];
          const car_model = vals[1];
          const engine = vals[2];
          const wiper_location = vals[3];
          const wiper_number = vals[4];
          if (!car_name && !car_model && !engine && !wiper_location && !wiper_number) continue;
          rows.push({
            car_name,
            car_model,
            engine,
            wiper_location,
            wiper_number,
            source_sheet: String(sheetName)
          });
        }
      }
      return {
        file_name: sourceKey,
        file_size: arrayBuffer.byteLength,
        content_hash: await sha256Hex(arrayBuffer),
        rows
      };
    }

    async function uploadLocationSheetChunked(file) {
      const parsed = await parseLocationRowsForChunkUpload(file);
      const sourceKey = LOCATION_TAB_SOURCE_FILE;
      try {
        const startRes = await fetch('/upload_location_rows/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            file_name: sourceKey,
            content_hash: parsed.content_hash,
            file_size: parsed.file_size,
            replace_entire_table: true
          })
        });
        const startData = await readJsonSafe(startRes);
        if (!startRes.ok) throw new Error(startData.detail || 'location-start-failed');
        if (startData.skip) return { ok: true, skipped: true, inserted_rows: 0 };

        const chunkSize = 500;
        for (let i = 0; i < parsed.rows.length; i += chunkSize) {
          const chunk = parsed.rows.slice(i, i + chunkSize);
          const chunkRes = await fetch('/upload_location_rows/chunk', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: sourceKey, rows: chunk })
          });
          const chunkData = await readJsonSafe(chunkRes);
          if (!chunkRes.ok) throw new Error(chunkData.detail || 'location-chunk-failed');
        }

        const finishRes = await fetch('/upload_location_rows/finish', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            file_name: sourceKey,
            content_hash: parsed.content_hash,
            file_size: parsed.file_size
          })
        });
        const finishData = await readJsonSafe(finishRes);
        if (!finishRes.ok) throw new Error(finishData.detail || 'location-finish-failed');

        return { ok: true, skipped: false, inserted_rows: Number(finishData.rows_count || 0) };
      } catch (err) {
        try {
          await fetch('/upload_location_rows/abort', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: sourceKey })
          });
        } catch (_) {}
        throw err;
      }
    }

    async function uploadWiperSheetChunked(file) {
      const parsed = await parseWiperRowsForChunkUpload(file);
      const sourceKey = parsed.file_name;
      try {
        const startRes = await fetch('/upload_wiper_rows/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            file_name: sourceKey,
            content_hash: parsed.content_hash,
            file_size: parsed.file_size
          })
        });
        const startData = await readJsonSafe(startRes);
        if (!startRes.ok) throw new Error(startData.detail || 'wiper-start-failed');
        if (startData.skip) {
          return { files_count: 0, skipped_files: 1, inserted_wiper_rows: 0 };
        }

        const chunkSize = 800;
        for (let i = 0; i < parsed.rows.length; i += chunkSize) {
          const chunk = parsed.rows.slice(i, i + chunkSize);
          const chunkRes = await fetch('/upload_wiper_rows/chunk', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: sourceKey, rows: chunk })
          });
          const chunkData = await readJsonSafe(chunkRes);
          if (!chunkRes.ok) throw new Error(chunkData.detail || 'wiper-chunk-failed');
        }

        const finishRes = await fetch('/upload_wiper_rows/finish', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            file_name: sourceKey,
            content_hash: parsed.content_hash,
            file_size: parsed.file_size
          })
        });
        const finishData = await readJsonSafe(finishRes);
        if (!finishRes.ok) throw new Error(finishData.detail || 'wiper-finish-failed');

        return {
          files_count: 1,
          skipped_files: 0,
          inserted_wiper_rows: Number(finishData.rows_count || 0)
        };
      } catch (err) {
        try {
          await fetch('/upload_wiper_rows/abort', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: sourceKey })
          });
        } catch (_) {}
        throw err;
      }
    }

    async function readJsonSafe(res) {
      const raw = await res.text();
      let data = {};
      try {
        data = raw ? JSON.parse(raw) : {};
      } catch (_) {
        data = { detail: raw || `HTTP ${res.status}` };
      }
      return data;
    }

    async function parseExcelRowsForChunkUpload(file) {
      await ensureXlsxLib();
      const arrayBuffer = await file.arrayBuffer();
      const wb = XLSX.read(arrayBuffer, { type: 'array' });
      const sourceKey = sourceKeyForFile(file);
      const rows = [];
      const itemNamesSet = new Set();

      for (const sheetName of wb.SheetNames) {
        const ws = wb.Sheets[sheetName];
        const grid = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
        const headerCandidates = ['اسم الصنف', 'اسم السيارة', 'رقم الصنف', 'رقم الشركة', 'رقم الشركات', 'الرقم الاصلي', 'البدائل', 'متشابهات'];
        function previewRowIsLikelyHeaderJs(cells) {
          const nonempty = cells.map(s => String(s || '').trim()).filter(Boolean);
          if (nonempty.length < 2) return true;
          let dataLike = 0;
          for (const v of nonempty.slice(0, 14)) {
            if (v.length < 5) continue;
            const dig = [...v].filter(ch => /\d/.test(ch)).length;
            if (v.length >= 10 && dig / v.length > 0.4) dataLike++;
            else if (v.length >= 6 && dig / v.length > 0.55) dataLike++;
          }
          return dataLike < Math.max(2, Math.floor(nonempty.length / 3));
        }
        let headerRowIndex = 0;
        let bestScore = -10;
        for (let i = 0; i < Math.min(30, grid.length); i++) {
          const rowCells = (grid[i] || []).map(v => String(v || '').trim());
          let score = 0;
          for (const k of headerCandidates) {
            if (getHeaderIndexJs(rowCells, [k]) !== null) score += 1;
          }
          if (!previewRowIsLikelyHeaderJs(rowCells)) score -= 12;
          if (score > bestScore) {
            bestScore = score;
            headerRowIndex = i;
          }
        }
        const header = forwardFillHeaderRowJs(grid[headerRowIndex] || []);
        const colMap = resolveProductColumnIndicesJs(header);
        const itemNameIdx = colMap.itemNameIdx != null ? colMap.itemNameIdx : 0;
        const itemNumberIdx = colMap.itemNumIdx != null ? colMap.itemNumIdx : 4;
        const companyIdx = colMap.companyIdx;
        const originalIdx = colMap.originalIdx;
        const notesIdx = colMap.notesIdx;
        const altIdx = colMap.altIdx != null && colMap.altIdx !== undefined ? colMap.altIdx : null;

        for (const r of grid.slice(headerRowIndex + 1)) {
          const itemName = pickItemNameJs(r, itemNameIdx);
          const itemNumber = String(r[itemNumberIdx] || '').trim();
          let companyNumber = String((companyIdx != null && companyIdx >= 0) ? r[companyIdx] || '' : '').trim();
          const originalNumbers = String((originalIdx != null && originalIdx >= 0) ? r[originalIdx] || '' : '').trim();
          if (!companyNumber && originalNumbers && originalIdx != null && originalIdx !== undefined) {
            const oC = normalizeTextCompactJs(originalNumbers);
            const skipAdj = new Set([companyIdx, altIdx, notesIdx, itemNameIdx, itemNumberIdx].filter((x) => x != null && x !== undefined));
            for (const delta of [1, -1]) {
              const j = originalIdx + delta;
              if (skipAdj.has(j) || j < 0) continue;
              const cand = String(r[j] || '').trim();
              if (!cand || normalizeTextCompactJs(cand) === oC) continue;
              companyNumber = cand;
              break;
            }
          }
          const notes = String((notesIdx != null && notesIdx >= 0) ? r[notesIdx] || '' : '').trim();
          let alternatives = String((altIdx != null && altIdx >= 0) ? r[altIdx] || '' : '').trim();
          alternatives = sanitizeAlternativesVsOriginalJs(alternatives, originalNumbers);
          const sizeWidth = String(r[1] || '').trim();
          const sizeDiameter = String(r[2] || '').trim();
          const sizeHeight = String(r[3] || '').trim();

          if (!itemName && !itemNumber && !companyNumber && !originalNumbers && !notes && !alternatives && !sizeWidth && !sizeDiameter && !sizeHeight) continue;

          rows.push({
            item_name: itemName,
            item_number: itemNumber,
            company_number: companyNumber,
            original_numbers: originalNumbers,
            notes: notes,
            alternatives: alternatives,
            size_width: sizeWidth,
            size_diameter: sizeDiameter,
            size_height: sizeHeight,
            source_sheet: String(sheetName)
          });
          if (itemName) itemNamesSet.add(itemName);
        }
      }

      return {
        file_name: sourceKey,
        file_size: arrayBuffer.byteLength,
        content_hash: await sha256Hex(arrayBuffer),
        rows,
        item_names_count: itemNamesSet.size
      };
    }

    async function uploadLargeFileChunked(file) {
      const parsed = await parseExcelRowsForChunkUpload(file);
      try {
        const startRes = await fetch('/upload_rows/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            file_name: parsed.file_name,
            content_hash: parsed.content_hash,
            file_size: parsed.file_size
          })
        });
        const startData = await readJsonSafe(startRes);
        if (!startRes.ok) throw new Error(startData.detail || 'start-failed');
        if (startData.skip) return { files_count: 0, skipped_files: 1, inserted_rows: 0, inserted_items: 0 };

        const chunkSize = 1200;
        let inserted = 0;
        for (let i = 0; i < parsed.rows.length; i += chunkSize) {
          const chunk = parsed.rows.slice(i, i + chunkSize);
          const chunkRes = await fetch('/upload_rows/chunk', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: parsed.file_name, rows: chunk })
          });
          const chunkData = await readJsonSafe(chunkRes);
          if (!chunkRes.ok) throw new Error(chunkData.detail || 'chunk-failed');
          inserted += Number(chunkData.inserted_rows || 0);
        }

        const finishRes = await fetch('/upload_rows/finish', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            file_name: parsed.file_name,
            content_hash: parsed.content_hash,
            file_size: parsed.file_size
          })
        });
        const finishData = await readJsonSafe(finishRes);
        if (!finishRes.ok) throw new Error(finishData.detail || 'finish-failed');

        return {
          files_count: 1,
          skipped_files: 0,
          inserted_rows: Number(finishData.rows_count || inserted),
          inserted_items: Number(parsed.item_names_count || 0)
        };
      } catch (err) {
        // Prevent partial file data if a chunk fails mid-upload.
        try {
          await fetch('/upload_rows/abort', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: parsed.file_name })
          });
        } catch (_) {}
        throw err;
      }
    }

    const SEARCH_SPELLING_CANON_JS = { foucs: 'focus', toucson: 'tucson' };
    function normalizeTextForSearch(text) {
      return String(text || '')
        .replace(/[٠-٩]/g, d => String('٠١٢٣٤٥٦٧٨٩'.indexOf(d)))
        .replace(/[۰-۹]/g, d => String('۰۱۲۳۴۵۶۷۸۹'.indexOf(d)))
        .replace(/[أإآٱ]/g, 'ا')
        .replace(/ى/g, 'ي')
        .replace(/ؤ/g, 'و')
        .replace(/ئ/g, 'ي')
        .replace(/ة/g, 'ه')
        .replace(/ء/g, '')
        .replace(/ـ/g, '')
        .replace(/[\u064B-\u065F\u0670\u06D6-\u06ED]/g, '')
        .trim()
        .toLowerCase()
        .replace(/[\\s\\-_]+/g, ' ')
        .replace(/[a-z]+/g, w => SEARCH_SPELLING_CANON_JS[w] || w);
    }

    function normalizeTextCompactJs(text) {
      return normalizeTextForSearch(text).replace(/[\\s\\-_/]+/g, '');
    }

    function tokenizeQueryForSearch(text) {
      let raw = normalizeTextForSearch(text).replace(/[،,;/|]+/g, ' ');
      raw = raw.replace(/[()]+/g, ' ');
      raw = raw.replace(/(\d{1,2})\s*[.,،]\s*(\d{1,3})(?!\d)/g, '$1.$2');
      raw = raw.replace(/\\s+/g, ' ').trim();
      return raw.split(' ').filter(Boolean).filter(t => !['+', '-', '–', '—'].includes(t));
    }

    function splitQueryDisplayWordsJs(text) {
      let raw = String(text || '')
        .replace(/[٠-٩]/g, d => String('٠١٢٣٤٥٦٧٨٩'.indexOf(d)))
        .replace(/[۰-۹]/g, d => String('۰۱۲۳۴۵۶۷۸۹'.indexOf(d)))
        .trim();
      raw = raw.replace(/[،,;/|]+/g, ' ');
      raw = raw.replace(/[()]+/g, ' ');
      raw = raw.replace(/(\d{1,2})\s*[.,،]\s*(\d{1,3})(?!\d)/g, '$1.$2');
      raw = raw.replace(/\\s+/g, ' ').trim();
      return raw.split(' ').filter(Boolean).filter(t => !['+', '-', '–', '—'].includes(t));
    }

    function displayFormsForTextTokensJs(textTokens, rawQuery) {
      const words = splitQueryDisplayWordsJs(rawQuery);
      const used = new Set();
      return textTokens.map(t => {
        const nt = normalizeTextForSearch(t);
        for (let i = 0; i < words.length; i++) {
          if (used.has(i)) continue;
          if (normalizeTextForSearch(words[i]) === nt) {
            used.add(i);
            return words[i];
          }
        }
        return t;
      });
    }

    function formatSizeLabelDisplayJs(sourceFile, width, diameter, height) {
      const sw = String(width || '').trim();
      const sd = String(diameter || '').trim();
      const sh = String(height || '').trim();
      if (!isSizeFileNameJs(sourceFile)) return '';
      const sf = normalizeTextForSearch(sourceFile || '');
      if (normalizedPathMatchesDriveshaftJs(sf) || (isSizeFileNameJs(sourceFile) && !sh)) {
        const p = [sw, sd].filter(Boolean);
        return p.length ? p.join(' · ') : '';
      }
      const p2 = [sw, sd, sh].filter(Boolean);
      return p2.length ? p2.join(' · ') : '';
    }

    function normalizedPathMatchesDriveshaftJs(sf) {
      if (!sf) return false;
      const t = normalizeTextForSearch;
      if (sf.includes(t('مصلبة دراي شفط')) || sf.includes(t('مصلبات دراي شفط')) || sf.includes(t('مصلب دراي شفط'))) return true;
      if (!sf.includes('مصلب')) return false;
      const hasShaft = sf.includes('شفط') || sf.includes('shaft');
      const hasDrive = sf.includes('دراي') || sf.includes('دري') || sf.includes('dry') || sf.includes('drive');
      return hasShaft || hasDrive;
    }

    function isDriveshaftSizeTypeJs(st) {
      if (!st) return false;
      const t = normalizeTextForSearch;
      const keys = [t('مصلبات دراي شفط'), t('مصلبه دراي شفط'), t('مصلبات'), t('مصلب دراي شفط'), t('مصلب دراي')];
      if (keys.includes(st)) return true;
      return st.includes('مصلب') && (st.includes('شفط') || st.includes('دراي') || st.includes('دري') || st.includes('dry') || st.includes('drive') || st.includes('shaft'));
    }

    function isSizeFileNameJs(sourceFile) {
      const sf = normalizeTextForSearch(sourceFile || '');
      return sf.includes('لبادات')
        || sf.includes('بيل')
        || sf.includes('بيلية')
        || sf.includes('نابات')
        || normalizedPathMatchesDriveshaftJs(sf);
    }

    function fileMatchesSizeTypeJs(sourceFile, sizeType) {
      const st = normalizeTextForSearch(sizeType || '');
      if (!st) return true;
      const sf = normalizeTextForSearch(sourceFile || '');
      if (st === 'لبادات') return sf.includes('لبادات');
      if (['بيلية', 'بيل', 'بلي'].includes(st)) {
        return sf.includes(normalizeTextForSearch('نابات + بيل عجل'))
          || sf.includes('بيل مشكلة')
          || sf.includes(normalizeTextForSearch('بيـل مشكلة'));
      }
      if (isDriveshaftSizeTypeJs(st)) return normalizedPathMatchesDriveshaftJs(sf);
      return true;
    }

    function fileMatchesSizeTypeWithFallbackJs(r, sizeType, sw, sd) {
      const st = normalizeTextForSearch(sizeType || '');
      if (!st) return true;
      if (fileMatchesSizeTypeJs(r.source_file || '', sizeType)) return true;
      if (!isDriveshaftSizeTypeJs(st)) return false;
      const sf = normalizeTextForSearch(r.source_file || '');
      if (sf.includes('لبادات')) return false;
      if (sf.includes('بيل مشكلة') || sf.includes(normalizeTextForSearch('بيـل مشكلة')) || sf.includes(normalizeTextForSearch('نابات + بيل عجل'))) return false;
      const swq = normalizeTextForSearch(sw || '').trim();
      const sdq = normalizeTextForSearch(sd || '').trim();
      if (!swq || !sdq) return false;
      const rw = String(r.size_width || '').trim();
      const rd = String(r.size_diameter || '').trim();
      if (!rw || !rd) return false;
      return (cellMatchesSizeQueryDriveshaftJs(rw, sw) && cellMatchesSizeQueryDriveshaftJs(rd, sd))
        || (cellMatchesSizeQueryDriveshaftJs(rw, sd) && cellMatchesSizeQueryDriveshaftJs(rd, sw));
    }

    function rowMatchesSizeFiltersJs(r, sizeType, sw, sd, sh) {
      const st = normalizeTextForSearch(sizeType || '');
      const wv = r.size_width || '';
      const dv = r.size_diameter || '';
      const hv = r.size_height || '';
      const swq = normalizeTextForSearch(sw || '').trim();
      const sdq = normalizeTextForSearch(sd || '').trim();
      if (isDriveshaftSizeTypeJs(st)) {
        let dimOk;
        if (swq && sdq) {
          dimOk = (cellMatchesSizeQueryDriveshaftJs(wv, sw) && cellMatchesSizeQueryDriveshaftJs(dv, sd))
            || (cellMatchesSizeQueryDriveshaftJs(wv, sd) && cellMatchesSizeQueryDriveshaftJs(dv, sw));
        } else if (swq) {
          dimOk = cellMatchesSizeQueryDriveshaftJs(wv, sw) || cellMatchesSizeQueryDriveshaftJs(dv, sw);
        } else if (sdq) {
          dimOk = cellMatchesSizeQueryDriveshaftJs(wv, sd) || cellMatchesSizeQueryDriveshaftJs(dv, sd);
        } else {
          dimOk = true;
        }
        return dimOk && cellMatchesSizeQueryJs(hv, sh);
      }
      return cellMatchesSizeQueryJs(wv, sw) && cellMatchesSizeQueryJs(dv, sd) && cellMatchesSizeQueryJs(hv, sh);
    }

    function cellMatchesSizeQueryJs(cellVal, queryVal) {
      if (!String(queryVal || '').trim()) return true;
      const q = normalizeTextForSearch(queryVal).replace(',', '.').trim();
      const c = normalizeTextForSearch(cellVal).replace(',', '.').trim();
      if (c === q) return true;
      const nq = Number(q);
      const nc = Number(c);
      if (!Number.isNaN(nq) && !Number.isNaN(nc) && nq === nc) return true;
      if (Number.isNaN(nq)) return c.includes(q);
      const nums = [];
      const re = /\\d+(?:\\.\\d+)?/g;
      let m;
      while ((m = re.exec(c)) !== null) {
        const v = Number(m[0]);
        if (!Number.isNaN(v)) nums.push(v);
      }
      return nums.some(v => v === nq);
    }

    function cellMatchesSizeQueryDriveshaftJs(cellVal, queryVal) {
      if (cellMatchesSizeQueryJs(cellVal, queryVal)) return true;
      const q = normalizeTextForSearch(queryVal || '').replace(',', '.').trim();
      if (!/^\\d$/.test(q)) return false;
      const t = normalizeTextForSearch(cellVal || '').replace(',', '.');
      const re = /\\d+(?:\\.\\d+)?/g;
      let m;
      while ((m = re.exec(t)) !== null) {
        const s = m[0];
        const sn = s.replace(/^0+/, '') || '0';
        if (s.startsWith(q) || sn.startsWith(q)) return true;
      }
      return false;
    }

    function updateSizeInputsLayout() {
      const type = (document.getElementById('sizeTypeInput').value || '').trim();
      const width = document.getElementById('sizeWidthInput');
      const diameter = document.getElementById('sizeDiameterInput');
      const height = document.getElementById('sizeHeightInput');
      if (isDriveshaftSizeTypeJs(normalizeTextForSearch(type))) {
        width.placeholder = 'الراسية (B)';
        diameter.placeholder = 'طول المصلبة (C)';
        height.style.display = 'none';
        height.value = '';
      } else {
        width.placeholder = 'العرض (B)';
        diameter.placeholder = 'القطر (C)';
        height.style.display = '';
        height.placeholder = 'الارتفاع (D)';
      }
    }

    function extractFileSearchHintsJs(query, tokens) {
      const qn = normalizeTextForSearch(query);
      const hints = [];
      const consumed = new Set();
      const consume = (...parts) => parts.forEach(p => consumed.add(normalizeTextForSearch(p)));

      if ((qn.includes('فلتر') || qn.includes('فلاتر')) && qn.includes('هواء')) {
        hints.push('فلتر هواء', 'فلاتر هواء');
        consume('فلتر', 'فلاتر', 'هواء');
      }
      if ((qn.includes('فلتر') || qn.includes('فلاتر')) && qn.includes('زيت')) {
        hints.push('فلتر زيت', 'فلاتر زيت');
        consume('فلتر', 'فلاتر', 'زيت');
      }
      if ((qn.includes('فلتر') || qn.includes('فلاتر')) && (qn.includes('سولار') || qn.includes('ديزل'))) {
        hints.push('فلتر سولار', 'فلاتر السولار', 'فلاتر سولار', 'فلتر ديزل', 'فلاتر ديزل');
        consume('فلتر', 'فلاتر', 'سولار', 'ديزل', 'السولار');
      }

      const cleanedTokens = (tokens || []).filter(t => !consumed.has(normalizeTextForSearch(t)));
      const uniqueHints = Array.from(new Set(hints.map(normalizeTextForSearch))).filter(Boolean);
      return { fileHints: uniqueHints, textTokens: cleanedTokens };
    }

    function fileSearchHintMatchesPathJs(pathNorm, hintNorm) {
      const pn = String(pathNorm || '').trim();
      const hn = String(hintNorm || '').trim();
      if (!hn) return true;
      if (pn.includes(hn)) return true;
      const significant = hn.split(/\\s+/).filter(w => w.length >= 2);
      if (significant.length < 2) return pn.includes(hn);
      function wordOk(w) {
        if (pn.includes(w)) return true;
        if (w.startsWith('ال') && w.length > 2 && pn.includes(w.slice(2))) return true;
        return false;
      }
      return significant.every(wordOk);
    }

    function parseYearTokenJs(token) {
      const cleaned = String(token || '').replace(/[^\\d]/g, '');
      if (!(cleaned.length === 2 || cleaned.length === 4)) return null;
      const year = Number(cleaned);
      if (cleaned.length === 2) return year <= 30 ? 2000 + year : 1900 + year;
      return year;
    }

    function parseQueryYearTokenJs(token) {
      const raw = normalizeTextForSearch(token);
      const currentYear = new Date().getFullYear();

      if (/^\d{1,2}\.\d{1,3}$/.test(raw)) return null;

      if (/^(?:\+\d{2,4}|\d{2,4}\+)$/.test(raw)) {
        const start = parseYearTokenJs(raw.startsWith('+') ? raw.slice(1) : raw.slice(0, -1));
        if (!start) return null;
        return Array.from({ length: currentYear - start + 1 }, (_, i) => start + i);
      }
      const rangeMatch = raw.match(/^(\d{2,4})\s*[-–—]\s*(\d{2,4})$/);
      if (rangeMatch) {
        const start = parseYearTokenJs(rangeMatch[1]);
        const end = parseYearTokenJs(rangeMatch[2]);
        if (!start || !end) return null;
        const low = Math.min(start, end);
        const high = Math.max(start, end);
        return Array.from({ length: high - low + 1 }, (_, i) => low + i);
      }
      if (/^(?:-\d{2,4}|\d{2,4}-)$/.test(raw)) {
        const end = parseYearTokenJs(raw.startsWith('-') ? raw.slice(1) : raw.slice(0, -1));
        if (!end) return null;
        return Array.from({ length: end - 1900 + 1 }, (_, i) => 1900 + i);
      }
      if (/^\d{2,4}$/.test(raw)) {
        const y = parseYearTokenJs(raw);
        return y ? [y] : null;
      }
      return null;
    }

    function isExplicitYearOperatorTokenJs(token) {
      const raw = normalizeTextForSearch(token);
      return /^(?:\+\d{2,4}|\d{2,4}\+)$/.test(raw)
        || /^(\d{2,4})\s*[-–—]\s*(\d{2,4})$/.test(raw)
        || /^(?:-\d{2,4}|\d{2,4}-)$/.test(raw);
    }

    function yearInRangeTextJs(year, text) {
      const normalized = normalizeTextForSearch(text);
      if (!normalized) return false;

      const currentYear = new Date().getFullYear();
      for (const m of normalized.matchAll(/(?:^|[^\d])(\d{4})(?!\d)/g)) {
        if (Number(m[1]) === year) return true;
      }
      for (const m of normalized.matchAll(/(?:^|[^\d])(\d{2})(?!\d)/g)) {
        if (parseYearTokenJs(m[1]) === year) return true;
      }

      // Chained plus markers: +03,+08 => 03-07 and +08.
      const plusMarks = [];
      for (const m of normalized.matchAll(/(?:\+\s*(\d{2,4})(?!\d)|(?:^|[^\d])(\d{2,4})\s*\+(?!\d))/g)) {
        const token = m[1] || m[2];
        const start = parseYearTokenJs(token);
        if (start) plusMarks.push({ idx: m.index ?? 0, start });
      }
      if (plusMarks.length) {
        plusMarks.sort((a, b) => a.idx - b.idx);
        for (let i = 0; i < plusMarks.length; i++) {
          const start = plusMarks[i].start;
          const end = i + 1 < plusMarks.length ? plusMarks[i + 1].start - 1 : currentYear;
          if (start <= year && year <= end) return true;
        }
      }

      for (const m of normalized.matchAll(/\+\s*(\d{2,4})(?!\d)/g)) {
        const start = parseYearTokenJs(m[1]);
        if (start && start <= year && year <= currentYear) return true;
      }
      for (const m of normalized.matchAll(/(?:^|[^\d])(\d{2,4})\s*\+(?!\d)/g)) {
        const start = parseYearTokenJs(m[1]);
        if (start && start <= year && year <= currentYear) return true;
      }
      for (const m of normalized.matchAll(/(?:^|[^\d])(\d{2,4})\s*[-–—]\s*(\d{2,4})(?!\d)/g)) {
        const start = parseYearTokenJs(m[1]);
        const end = parseYearTokenJs(m[2]);
        if (!start || !end) continue;
        const low = Math.min(start, end);
        const high = Math.max(start, end);
        if (low <= year && year <= high) return true;
      }

      // Chained minus markers: -03,-08 => <=03 and 04-08.
      const minusMarks = [];
      for (const m of normalized.matchAll(/(?:[-–—]\s*(\d{2,4})(?!\d)|(?:^|[^\d])(\d{2,4})\s*[-–—](?!\s*\d))/g)) {
        const token = m[1] || m[2];
        const end = parseYearTokenJs(token);
        if (end) minusMarks.push({ idx: m.index ?? 0, end });
      }
      if (minusMarks.length) {
        minusMarks.sort((a, b) => a.idx - b.idx);
        if (year <= minusMarks[0].end) return true;
        for (let i = 1; i < minusMarks.length; i++) {
          const low = minusMarks[i - 1].end + 1;
          const high = minusMarks[i].end;
          if (low <= high && low <= year && year <= high) return true;
        }
      }

      for (const m of normalized.matchAll(/[-–—]\s*(\d{2,4})(?!\d)/g)) {
        const end = parseYearTokenJs(m[1]);
        if (end && year <= end) return true;
      }
      for (const m of normalized.matchAll(/(?:^|[^\d])(\d{2,4})\s*[-–—](?!\s*\d)/g)) {
        const end = parseYearTokenJs(m[1]);
        if (end && year <= end) return true;
      }
      return false;
    }

    function atMtProtectedRangesJs(s) {
      const str = String(s || '');
      const ranges = [];
      const re = /(?<![A-Za-z0-9/])(?:at\s*\/\s*mt|mt\s*\/\s*at)(?![A-Za-z0-9/])/gi;
      let m;
      while ((m = re.exec(str)) !== null) {
        ranges.push([m.index, m.index + m[0].length]);
      }
      return ranges;
    }

    function charInAtMtRangeJs(idx, ranges) {
      return ranges.some(([a, b]) => idx >= a && idx < b);
    }

    function nextSegmentSlashJs(str, start) {
      const ranges = atMtProtectedRangesJs(str);
      for (let i = start; i < str.length; i++) {
        if (str[i] !== '/') continue;
        if (charInAtMtRangeJs(i, ranges)) continue;
        return i;
      }
      return -1;
    }

    function splitAlternativeSegmentsJs(alternatives) {
      const s = String(alternatives || '');
      const ranges = atMtProtectedRangesJs(s);
      const parts = [];
      let curStart = 0;
      for (let i = 0; i < s.length; i++) {
        const ch = s[i];
        const delim = '|,;'.includes(ch) || ch.charCodeAt(0) === 10 || (ch === '/' && !charInAtMtRangeJs(i, ranges));
        if (delim) {
          const p = s.slice(curStart, i).trim();
          if (p) parts.push(p);
          curStart = i + 1;
        }
      }
      const t = s.slice(curStart).trim();
      if (t) parts.push(t);
      return parts;
    }

    function normSegmentWordSetJs(nseg) {
      const edges = '.,|;:()[]';
      const out = new Set();
      for (const w of String(nseg || '').split(/\\s+/)) {
        let t = w;
        while (t.length && edges.includes(t[0])) t = t.slice(1);
        while (t.length && edges.includes(t[t.length - 1])) t = t.slice(0, -1);
        if (t) out.add(t);
      }
      return out;
    }

    function tokenInNormSegmentJs(ntok, nseg) {
      if (!ntok) return false;
      const wset = normSegmentWordSetJs(nseg);
      if (ntok.length <= 2) return wset.has(ntok);
      if (wset.has(ntok)) return true;
      return nseg.includes(ntok);
    }

    function tokenAppearsInAlternativesColumnJs(ntok, alternatives) {
      const nAlt = normalizeTextForSearch(alternatives);
      if (!ntok) return false;
      const wset = normSegmentWordSetJs(nAlt);
      if (ntok.length <= 2) return wset.has(ntok);
      if (wset.has(ntok)) return true;
      return nAlt.includes(ntok);
    }

    function tokensMatchingAlternativesColumnJs(textTokens, alternatives) {
      const seen = new Set();
      const out = [];
      for (const t of textTokens) {
        const nt = normalizeTextForSearch(t);
        if (!nt || seen.has(nt)) continue;
        if (!tokenAppearsInAlternativesColumnJs(nt, alternatives)) continue;
        seen.add(nt);
        out.push(nt);
      }
      return out;
    }

    function isWordCharAltJs(ch) {
      if (!ch) return false;
      const c = ch[0];
      if (c >= '0' && c <= '9') return true;
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) return true;
      if (c >= '\u0600' && c <= '\u06FF') return true;
      return c === '+' || c === '.';
    }

    function findWholeWordSpanJs(haystack, needle) {
      if (!needle || !haystack) return null;
      const h = haystack.toLowerCase();
      const n = needle.toLowerCase();
      let pos = 0;
      while (true) {
        const idx = h.indexOf(n, pos);
        if (idx < 0) return null;
        const end = idx + n.length;
        const leftOk = idx === 0 || !isWordCharAltJs(haystack[idx - 1]);
        const rightOk = end >= haystack.length || !isWordCharAltJs(haystack[end]);
        if (leftOk && rightOk) return [idx, end];
        pos = idx + 1;
      }
    }

    function anchorPriorityForSliceTokenJs(t) {
      const nt = normalizeTextForSearch(t);
      if (!nt) return [99, 0];
      if (/^\d{1,2}\.\d{1,3}$/.test(nt)) return [4, -nt.length];
      if (/^\d{1,2}$/.test(nt)) return [3, -nt.length];
      if (/[a-z\u0600-\u06ff]/i.test(nt)) return [0, -nt.length];
      return [2, -nt.length];
    }

    function bestAnchorSpanForSliceJs(alternatives, tokens) {
      if (!tokens || !tokens.length) return null;
      const ordered = tokens.slice().sort((a, b) => {
        const pa = anchorPriorityForSliceTokenJs(a);
        const pb = anchorPriorityForSliceTokenJs(b);
        if (pa[0] !== pb[0]) return pa[0] - pb[0];
        return pa[1] - pb[1];
      });
      for (const tok of ordered) {
        const sp = findWholeWordSpanJs(alternatives, tok);
        if (sp) return sp;
      }
      return null;
    }

    function sliceAlternativesFirstMatchToSlashJs(alternatives, tokens) {
      const s = String(alternatives || '');
      const sp = bestAnchorSpanForSliceJs(s, tokens);
      if (!sp) return null;
      const st = sp[0];
      const slashAt = nextSegmentSlashJs(s, st);
      if (slashAt < 0) return s.slice(st).trim();
      return s.slice(st, slashAt).trim();
    }

    function extractMatchedAlternativeJs(alternatives, textTokens, years, rawQuery = '') {
      void rawQuery;
      if (!String(alternatives || '').trim()) return '';
      const segments = splitAlternativeSegmentsJs(alternatives);
      if (!segments.length) return '';

      const altToks = tokensMatchingAlternativesColumnJs(textTokens, alternatives);
      let tokensUse;
      if (altToks.length) tokensUse = altToks;
      else if (years.length) tokensUse = [];
      else tokensUse = textTokens.map(normalizeTextForSearch).filter(Boolean);

      if (tokensUse.length) {
        const sliced = sliceAlternativesFirstMatchToSlashJs(String(alternatives), tokensUse);
        if (sliced) {
          if (!years.length || years.some(y => yearInRangeTextJs(y, sliced))) {
            return sliced;
          }
        }
      }

      function segMatchesTokens(n) {
        if (!tokensUse.length) return true;
        return tokensUse.every(t => tokenInNormSegmentJs(t, n));
      }

      const candidates = [];
      for (const segment of segments) {
        const n = normalizeTextForSearch(segment);
        if (!segMatchesTokens(n)) continue;
        const hasYear = !years.length || years.some(y => yearInRangeTextJs(y, segment));
        if (hasYear) candidates.push(segment);
      }
      if (candidates.length) {
        return candidates.reduce((a, b) => (a.length >= b.length ? a : b)).trim();
      }

      let pool = segments.slice();
      if (years.length) {
        const yp = segments.filter(s => years.some(y => yearInRangeTextJs(y, s)));
        if (yp.length) pool = yp;
      }

      if (tokensUse.length) {
        let bestSeg = '';
        let bestScore = -1;
        for (const segment of pool) {
          const n = normalizeTextForSearch(segment);
          const sc = tokensUse.filter(t => tokenInNormSegmentJs(t, n)).length;
          if (sc > bestScore || (sc === bestScore && segment.length > bestSeg.length)) {
            bestScore = sc;
            bestSeg = segment;
          }
        }
        if (bestScore > 0) return bestSeg.trim();
      }

      if (years.length) {
        const yearHits = segments.filter(s => years.some(y => yearInRangeTextJs(y, s)));
        if (yearHits.length) {
          return yearHits.reduce((a, b) => (a.length >= b.length ? a : b)).trim();
        }
        return '';
      }
      return '';
    }

    function yearMatchScoreJs(segment, years) {
      if (!segment || !years.length) return 0;
      const n = normalizeTextForSearch(segment);
      let best = 0;
      for (const y of years) {
        if ((new RegExp(`(^|\\\\D)${y}(\\\\D|$)`)).test(n)) best = Math.max(best, 6000);
      }
      for (const m of n.matchAll(/\+\s*(\d{2,4})(?!\d)/g)) {
        const start = parseYearTokenJs(m[1]);
        if (start && years.every(y => start <= y)) best = Math.max(best, 4000 + start);
      }
      for (const m of n.matchAll(/(?:^|[^\d])(\d{2,4})\s*\+(?!\d)/g)) {
        const start = parseYearTokenJs(m[1]);
        if (start && years.every(y => start <= y)) best = Math.max(best, 4000 + start);
      }
      for (const m of n.matchAll(/(?:^|[^\d])(\d{2,4})\s*[-–—]\s*(\d{2,4})(?!\d)/g)) {
        const a = parseYearTokenJs(m[1]); const b = parseYearTokenJs(m[2]);
        if (!a || !b) continue;
        const low = Math.min(a, b), high = Math.max(a, b);
        if (years.every(y => low <= y && y <= high)) best = Math.max(best, 5000 - (high - low));
      }
      return best;
    }

    function rowMatchesQueryJs(row, textTokens, years) {
      // كل توكن يجب أن يظهر في الصف (اسم/بدائل/ملف) — ترتيب الكلمات في الاستعلام لا يؤثر.
      const itemName = row.item_name || '';
      const alternatives = row.alternatives || '';
      const sourceFile = row.source_file || '';
      const itemNorm = normalizeTextForSearch(itemName);
      const altNorm = normalizeTextForSearch(alternatives);
      const fileNorm = normalizeTextForSearch(sourceFile);
      const combined = `${itemNorm} ${altNorm} ${fileNorm}`;
      const combinedCompact = normalizeTextCompactJs(`${itemName} ${alternatives} ${sourceFile}`);
      for (const token of textTokens) {
        const nt = normalizeTextForSearch(token);
        const nct = normalizeTextCompactJs(token);
        if (!combined.includes(nt) && !(nct && combinedCompact.includes(nct))) return false;
      }
      if (years.length) {
        const itemTokens = textTokens.map(normalizeTextForSearch).filter(t => itemNorm.includes(t));
        const altTokens = textTokens.map(normalizeTextForSearch).filter(t => altNorm.includes(t));
        const fileTokens = textTokens.map(normalizeTextForSearch).filter(t => fileNorm.includes(t));
        const itemYearOk = itemTokens.length > 0 && years.some(y => yearInRangeTextJs(y, itemName));
        let altYearOk = false;
        for (const seg of splitAlternativeSegmentsJs(alternatives)) {
          const n = normalizeTextForSearch(seg);
          const hasAltTokens = !altTokens.length || altTokens.every(t => n.includes(t));
          const hasYear = years.some(y => yearInRangeTextJs(y, seg));
          if (hasAltTokens && hasYear) { altYearOk = true; break; }
        }
        if (altTokens.length ? !altYearOk : !(itemYearOk || altYearOk)) return false;
        if (!altTokens.length && fileTokens.length && !itemTokens.length) {
          // File-context token match: don't require year context in row text.
        } else if (altTokens.length ? !altYearOk : !(itemYearOk || altYearOk)) {
          return false;
        }
      }
      return true;
    }

    function rowSearchRelevanceScoreJs(row, textTokens, years) {
      const item = String(row.item_name || '');
      const alt = String(row.alternatives || '');
      const bag = normalizeTextForSearch(`${item} ${alt}`);
      const bagC = normalizeTextCompactJs(`${item} ${alt}`);
      const altN = normalizeTextForSearch(alt);
      const itemN = normalizeTextForSearch(item);
      const altC = normalizeTextCompactJs(alt);
      const itemC = normalizeTextCompactJs(item);
      let score = 0;
      for (const t of textTokens) {
        const nt = normalizeTextForSearch(t);
        if (!nt) continue;
        const nct = normalizeTextCompactJs(nt);
        const inBag = bag.includes(nt) || (nct && bagC.includes(nct));
        if (!inBag) continue;
        const inAlt = altN.includes(nt) || (nct && altC.includes(nct));
        const inItem = itemN.includes(nt) || (nct && itemC.includes(nct));
        if (inAlt) score += 5;
        else if (inItem) score += 2;
      }
      let yb = 0;
      if (years.length) {
        if (years.some(y => yearInRangeTextJs(y, alt))) yb += 3;
        else if (years.some(y => yearInRangeTextJs(y, item))) yb += 1;
      }
      const detail = Math.min(alt.length, 5000);
      return [score + yb, yb, detail, item];
    }

    function scoreTupleBetterJs(a, b) {
      for (let i = 0; i < 4; i++) {
        const x = a[i];
        const y = b[i];
        if (x !== y) return x > y;
      }
      return false;
    }

    function dedupeRowsByNormalizedOriginalForSearchJs(rowsIn, textTokens, years) {
      const tt = textTokens || [];
      const yy = years || [];
      const winners = {};
      for (const r of rowsIn) {
        const oc = normalizeTextCompactJs(String(r.original_numbers || ''));
        if (!oc) continue;
        if (!winners[oc]) winners[oc] = r;
        else {
          const sa = rowSearchRelevanceScoreJs(r, tt, yy);
          const sb = rowSearchRelevanceScoreJs(winners[oc], tt, yy);
          if (scoreTupleBetterJs(sa, sb)) winners[oc] = r;
        }
      }
      const out = [];
      const emitted = new Set();
      for (const r of rowsIn) {
        const oc = normalizeTextCompactJs(String(r.original_numbers || ''));
        if (!oc) {
          out.push(r);
          continue;
        }
        if (emitted.has(oc)) continue;
        emitted.add(oc);
        out.push(winners[oc]);
      }
      return out;
    }

    function searchOfflineSnapshot(query, numberQuery = '', sizeType = '', sizeWidth = '', sizeDiameter = '', sizeHeight = '') {
      const tokens = tokenizeQueryForSearch(query);
      const numberTokens = tokenizeQueryForSearch(numberQuery);
      const hintData = extractFileSearchHintsJs(query, tokens);
      const fileHints = hintData.fileHints;
      if (!hintData.textTokens.length && !numberTokens.length && !sizeType && !sizeWidth && !sizeDiameter && !sizeHeight && !fileHints.length) {
        return { total_rows: 0, matching_item_names: [], matching_items: [], rows: [] };
      }
      const parsedYears = hintData.textTokens.map(parseQueryYearTokenJs);
      const years = Array.from(new Set(parsedYears.filter(Boolean).flat()));
      const explicitYearOps = hintData.textTokens.some(isExplicitYearOperatorTokenJs);
      const textTokens = hintData.textTokens.filter((_, idx) => !parsedYears[idx]);

      let rows = (localSnapshot.rows || []).filter(r => {
        if (!rowMatchesQueryJs(r, textTokens, years)) return false;
        if (fileHints.length) {
          const sf = normalizeTextForSearch(r.source_file || '');
          if (!fileHints.some(h => fileSearchHintMatchesPathJs(sf, h))) return false;
        }
        const numberSpace = normalizeTextForSearch(`${r.company_number || ''} ${r.original_numbers || ''} ${r.notes || ''}`);
        if (!numberTokens.every(t => numberSpace.includes(normalizeTextForSearch(t)))) return false;
        if (!fileMatchesSizeTypeWithFallbackJs(r, sizeType, sizeWidth, sizeDiameter)) return false;
        if (!rowMatchesSizeFiltersJs(r, sizeType, sizeWidth, sizeDiameter, sizeHeight)) return false;
        return true;
      });
      rows = dedupeRowsByNormalizedOriginalForSearchJs(rows, textTokens, years);
      rows = rows.slice(0, 300);
      rows = rows.map((r, i) => {
        const altRaw = String(r.alternatives || '').trim();
        let matched = extractMatchedAlternativeJs(altRaw, textTokens, years, query);
        if (!altRaw) matched = '';
        return {
          ...r,
          item_number: String(r.company_number || r.item_number || ''),
          alternatives: altRaw,
          size_label: formatSizeLabelDisplayJs(r.source_file, r.size_width, r.size_diameter, r.size_height),
          matched_alternative: matched,
          match_score: yearMatchScoreJs(matched, years),
          row_key: `${r.source_file || ''}|${r.source_sheet || ''}|${r.company_number || r.item_number || ''}|${i}`
        };
      });
      if (explicitYearOps && years.length && rows.length) {
        const maxScore = Math.max(...rows.map(r => Number(r.match_score || 0)));
        if (maxScore > 0) rows = rows.filter(r => Number(r.match_score || 0) === maxScore);
      }
      const names = Array.from(new Set(rows.map(r => r.item_name).filter(Boolean))).slice(0, 100);
      const matchingItems = [];
      const seen = new Set();
      for (const r of rows) {
        const key = `${r.item_name}|${r.matched_alternative || ''}`;
        if (!r.item_name || seen.has(key)) continue;
        seen.add(key);
        matchingItems.push({
          item_name: r.item_name,
          matched_alternative: r.matched_alternative || '',
          size_label: r.size_label || '',
          row_key: r.row_key
        });
        if (matchingItems.length >= 100) break;
      }
      return { total_rows: rows.length, matching_item_names: names, matching_items: matchingItems, rows };
    }

    async function syncSnapshotFromServer() {
      try {
        setSyncInfo('جاري تنزيل نسخة البيانات للهاتف...');
        const res = await fetch('/sync/data', { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        await saveLocalSnapshot(data);
      } catch (err) {
        const m = (err && err.message) ? String(err.message) : '';
        setSyncInfo(`فشل تنزيل نسخة الهاتف. ${m ? '(' + m + ') ' : ''}تحقق من الإنترنت أو أن السيرفر غير مشغول ثم أعد المحاولة.`);
      }
    }

    async function syncFromGoogleDrive() {
      const uploadResult = document.getElementById('uploadResult');
      try {
        uploadResult.innerText = 'جاري المزامنة من Google Drive...';
        const res = await fetch('/google_drive/sync', { method: 'POST' });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'google-drive-sync-failed');

        const unsupportedCount = Array.isArray(data.unsupported_files) ? data.unsupported_files.length : 0;
        const failedCount = Array.isArray(data.failed_files) ? data.failed_files.length : 0;
        uploadResult.innerText =
          `اكتملت مزامنة Google Drive: تم فحص ${Number(data.scanned_files || 0)} ملف، معالجة ${Number(data.files_count || 0)}، تخطي ${Number(data.skipped_files || 0)}، إدخال ${Number(data.inserted_rows || 0)} صف، غير مدعوم ${unsupportedCount}، فشل ${failedCount}.`;

        await loadStats();
        await searchNow();
      } catch (err) {
        uploadResult.innerText = `فشلت مزامنة Google Drive: ${(err && err.message) ? err.message : 'error'}`;
      }
    }

    async function autoSyncGoogleDriveIfServerEmpty() {
      try {
        const doneKey = 'gdrive_auto_sync_done';
        if (sessionStorage.getItem(doneKey) === '1') return;
        const res = await fetch('/stats', { cache: 'no-store' });
        if (!res.ok) return;
        const stats = await res.json();
        if (Number(stats.total_rows || 0) > 0) {
          sessionStorage.setItem(doneKey, '1');
          return;
        }
        // If server is empty, try to restore from Drive automatically.
        await syncFromGoogleDrive();
        sessionStorage.setItem(doneKey, '1');
      } catch (_) {}
    }

    function groupRowsBySourceFile(rows) {
      const groups = new Map();
      for (const row of rows || []) {
        const fileName = String(row.source_file || 'restored-data.xlsx').trim() || 'restored-data.xlsx';
        if (!groups.has(fileName)) groups.set(fileName, []);
        groups.get(fileName).push({
          item_name: String(row.item_name || ''),
          item_number: String(row.item_number || ''),
          company_number: String(row.company_number || ''),
          original_numbers: String(row.original_numbers || ''),
          notes: String(row.notes || ''),
          alternatives: String(row.alternatives || ''),
          size_width: String(row.size_width || ''),
          size_diameter: String(row.size_diameter || ''),
          size_height: String(row.size_height || ''),
          source_sheet: String(row.source_sheet || 'Sheet1')
        });
      }
      return groups;
    }

    async function restoreServerFromLocalSnapshotIfEmpty() {
      try {
        await ensureLocalSnapshotLoaded();
        if (!localSnapshot || !Array.isArray(localSnapshot.rows) || !localSnapshot.rows.length) return;

        const statsRes = await fetch('/stats', { cache: 'no-store' });
        if (!statsRes.ok) return;
        const stats = await statsRes.json();
        if (Number(stats.total_rows || 0) > 0) return;

        setSyncInfo('تم اكتشاف سيرفر بدون بيانات. جاري استرجاع الأصناف من النسخة المحلية...');
        const groups = groupRowsBySourceFile(localSnapshot.rows);
        let restoredRows = 0;

        for (const [fileName, rows] of groups.entries()) {
          const pseudoHash = await sha256Hex(new TextEncoder().encode(`${localSnapshot.version || ''}:${fileName}:${rows.length}`));
          const startRes = await fetch('/upload_rows/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: fileName, content_hash: pseudoHash, file_size: rows.length })
          });
          if (!startRes.ok) continue;
          const startData = await startRes.json();
          if (startData.skip) continue;

          const chunkSize = 1200;
          for (let i = 0; i < rows.length; i += chunkSize) {
            const chunk = rows.slice(i, i + chunkSize);
            const cRes = await fetch('/upload_rows/chunk', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ file_name: fileName, rows: chunk })
            });
            if (!cRes.ok) continue;
          }

          const finishRes = await fetch('/upload_rows/finish', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_name: fileName, content_hash: pseudoHash, file_size: rows.length })
          });
          if (finishRes.ok) restoredRows += rows.length;
        }

        setSyncInfo(`اكتمل استرجاع البيانات من النسخة المحلية: ${restoredRows} صف.`);
        await loadStats();
      } catch (_) {
        setSyncInfo('تعذر الاسترجاع التلقائي من النسخة المحلية.');
      }
    }

    async function checkSnapshotUpdate(showNoUpdate, askUser = false) {
      try {
        await ensureLocalSnapshotLoaded();
        const res = await fetch('/sync/meta', { cache: 'no-store' });
        if (!res.ok) throw new Error('sync-meta-failed');
        const meta = await res.json();  
        if (!localSnapshot.version) {
          setSyncInfo(`متاح تحميل نسخة بيانات (${meta.total_rows} صف).`);
          if (askUser) {
            await syncSnapshotFromServer();
          }
          return;
        }
        if (meta.version !== localSnapshot.version) {
          setSyncInfo('يوجد تحديث جديد للبيانات. اضغط "فحص تحديث البيانات" أو "مزامنة للهاتف".');
          if (askUser && meta.version !== lastNotifiedVersion) {
            await syncSnapshotFromServer();
            lastNotifiedVersion = '';
          } else if (askUser) {
            lastNotifiedVersion = meta.version;
          }
        } else if (showNoUpdate) {
          lastNotifiedVersion = '';
          setSyncInfo('النسخة المحلية محدثة.');
        }
      } catch (_) {
        if (showNoUpdate) setSyncInfo('تعذر فحص التحديث الآن. قد تكون غير متصل بالسيرفر.');
      }
    }

    async function uploadFiles() {
      const selectedFiles = Array.from(selectedFilesMap.values());
      if (!selectedFiles.length) {
        document.getElementById('uploadResult').innerText = 'اختار ملفات Excel أولاً';
        return;
      }

      const uploadBtn = document.getElementById('uploadBtn');
      uploadBtn.disabled = true;
      let done = 0;
      let processed = 0;
      let skipped = 0;
      let inserted = 0;
      let insertedWiper = 0;
      let insertedLocation = 0;
      let insertedItems = 0;
      const failed = [];
      const tooLarge = [];

      document.getElementById('uploadResult').innerText = 'جاري الرفع والمعالجة...';
      setUploadProgress(0, `بدء الرفع... 0/${selectedFiles.length}`);

      for (const file of selectedFiles) {
        try {
          let data = null;
          const ext = String(file.name || '').toLowerCase().split('.').pop() || '';
          const forceChunked = (ext === 'xlsb' || ext === 'xls');
          if (file.size > VERCEL_SAFE_UPLOAD_BYTES || forceChunked) {
            tooLarge.push(file.name);
            setUploadProgress(
              Math.round((done / selectedFiles.length) * 100),
              forceChunked ? `جاري معالجة ملف بصيغة قديمة: ${file.name}` : `جاري معالجة ملف كبير: ${file.name}`
            );
            data = await uploadLargeFileChunked(file);
          } else {
            const formData = new FormData();
            formData.append('files', file, sourceKeyForFile(file));
            const res = await fetch('/upload', { method: 'POST', body: formData });
            data = await readJsonSafe(res);
            if (!res.ok) {
              if (res.status === 413 || ext === 'xls' || ext === 'xlsb') {
                // fallback to chunked mode automatically
                data = await uploadLargeFileChunked(file);
              } else {
                throw new Error(data.detail || 'upload-failed');
              }
            }
          }
          processed += Number(data.files_count || 0);
          skipped += Number(data.skipped_files || 0);
          inserted += Number(data.inserted_rows || 0);
          insertedWiper += Number(data.inserted_wiper_rows || 0);
          insertedLocation += Number(data.inserted_location_rows || 0);
          insertedItems += Number(data.inserted_items || 0);
        } catch (err) {
          failed.push(`${file.name} (${(err && err.message) ? err.message : 'upload-failed'})`);
        }

        done += 1;
        const percent = Math.round((done / selectedFiles.length) * 100);
        setUploadProgress(percent, `جاري الرفع: ${done}/${selectedFiles.length} - الملف الحالي: ${file.name}`);
      }

      uploadBtn.disabled = false;
      const failText = failed.length ? ` | فشل ${failed.length} ملف` : '';
      const largeText = tooLarge.length ? ` | ملفات تمت معالجتها بنمط مجزأ: ${tooLarge.length}` : '';
      const wiperPart = insertedWiper ? `، و${insertedWiper} صف قشطان` : '';
      const locPart = insertedLocation ? `، و${insertedLocation} صف مواقع` : '';
      document.getElementById('uploadResult').innerText =
        `اكتمل الرفع ${Math.round((done / selectedFiles.length) * 100)}%: تم معالجة ${processed} ملفات، تخطي ${skipped} ملفات غير معدلة، وإدخال ${inserted} صف منتجات${wiperPart}${locPart}، وإضافة ${insertedItems} صنف${largeText}${failText}${failed.length ? ` | أمثلة فشل: ${failed.slice(0, 3).join(' | ')}` : ''}.`;
      await loadStats();
      await searchNow();
    }

    async function searchNow(mode = 'auto') {
      const effectiveMode = mode === 'auto' ? activeSearchTab : mode;

      const wiperWrap = document.getElementById('wipersTableWrap');
      const wipersTable = document.getElementById('wipersTable');
      const locWrap = document.getElementById('locationsTableWrap');
      const locTable = document.getElementById('locationsTable');

      if (effectiveMode === 'print') {
        document.getElementById('names').innerHTML = '';
        document.getElementById('resultsTable').style.display = 'none';
        wiperWrap.style.display = 'none';
        wipersTable.style.display = 'none';
        locWrap.style.display = 'none';
        locTable.style.display = 'none';
        return;
      }

      if (effectiveMode === 'location') {
        const lqText = document.getElementById('locationTextQueryInput').value.trim();
        let lqBc = lightSanitizeBarcodeValue(document.getElementById('locationBarcodeQueryInput').value);
        const lqCanon = normalizeBarcodeLookupKeyClient(lqBc);
        const barcodeDebug = localStorage.getItem('locationBarcodeDebug') === '1';
        const gen = ++locationSearchGen;
        const tLocal0 = performance.now();
        logBarcodeClientDebug('search-location', { raw: lqBc, canonical: lqCanon, debug: barcodeDebug, gen });
        localStorage.setItem('lastLocationTextQuery', lqText);
        localStorage.setItem('lastLocationBarcodeQuery', lqBc);
        document.getElementById('locationBarcodeQueryInput').value = lqBc;
        document.getElementById('names').innerHTML = '';
        document.getElementById('resultsTable').style.display = 'none';
        wiperWrap.style.display = 'none';
        wipersTable.style.display = 'none';
        if (!lqText && !lqBc) {
          locWrap.style.display = 'none';
          locTable.style.display = 'none';
          setStats('اكتب اسم/رقم أو باركود، أو استخدم الكاميرا.');
          return;
        }
        setStats('جاري البحث...');
        let data = null;
        try {
          const params = lqBc ? new URLSearchParams() : new URLSearchParams({ q: lqText });
          if (lqBc) {
            params.set('q_barcode', lqBc);
            if (lqCanon) params.set('q_barcode_canonical', lqCanon);
            if (barcodeDebug) params.set('barcode_debug', '1');
          }
          params.set('_', String(Date.now()));
          const ctrl = new AbortController();
          const to = setTimeout(() => ctrl.abort(), 45000);
          const res = await fetch(`/search_locations?${params.toString()}`, {
            method: 'GET',
            cache: 'no-store',
            signal: ctrl.signal,
            headers: { Accept: 'application/json', 'Cache-Control': 'no-cache' }
          });
          clearTimeout(to);
          const parsed = await readJsonSafe(res);
          if (!res.ok) throw new Error(parsed.detail || `search-locations-${res.status}`);
          data = parsed;
          if (data && data.search_version != null) {
            lastHesabateSearchVersion = Number(data.search_version);
          }
        } catch (err) {
          if (gen !== locationSearchGen) return;
          let hint = 'تحقق من الإنترنت ثم أعد المحاولة.';
          if (err && err.name === 'AbortError') hint = 'انتهت مهلة الانتظار. أعد المحاولة.';
          else if (err && err.message) {
            const m = String(err.message);
            if (m.includes('search-locations-')) hint = `ردّ الخادم (${m.replace('search-locations-', 'رمز ')}) — قد يكون السيرفر مشغولاً؛ أعد المحاولة بعد قليل.`;
            else if (m !== 'Failed to fetch') hint = m;
          }
          setStats( `تعذر البحث في المواقع: ${hint}`);
          locWrap.style.display = 'none';
          locTable.style.display = 'none';
          return;
        }
        if (gen !== locationSearchGen) return;
        const localMs = Math.round(performance.now() - tLocal0);
        const rawRows = Array.isArray(data && data.rows) ? data.rows : [];
        // فضّل صف حساباتي_API على Excel عند التكرار
        const ranked = rawRows.slice().sort((a, b) => {
          const aApi = String(a.source_file || '') === 'حساباتي_API' ? 0 : 1;
          const bApi = String(b.source_file || '') === 'حساباتي_API' ? 0 : 1;
          if (aApi !== bApi) return aApi - bApi;
          return Number(b.id || 0) - Number(a.id || 0);
        });
        const seenKeys = new Set();
        const safeRows = [];
        ranked.forEach((r) => {
          const pid = String(r.hesabate_product_id || '').trim();
          const k = pid
            ? ('pid:' + pid)
            : String(r.barcode || r.original_number || r.item_name || '').trim().toLowerCase();
          if (k && seenKeys.has(k)) return;
          if (k) seenKeys.add(k);
          safeRows.push(r);
        });
        // زر بحث = قاعدة البيانات المحلية فقط — بدون أي طلب حساباتي
        if (safeRows.length) {
          renderLocationRowsNow(
            safeRows,
            `بحث محلي · ${safeRows.length} · ${localMs}ms · بدون API`
          );
        } else {
          clearLocationTable(lqBc ? 'لا يوجد صنف بهذا الباركود في قاعدة البيانات.' : 'لا توجد نتيجة في قاعدة البيانات.');
        }
        return;
      }

      locWrap.style.display = 'none';
      locTable.style.display = 'none';

      if (effectiveMode === 'wiper') {
        const wCar = document.getElementById('wiperCarInput').value.trim();
        const wModel = document.getElementById('wiperModelInput').value.trim();
        const wEngine = document.getElementById('wiperEngineInput').value.trim();
        localStorage.setItem('lastWiperCar', wCar);
        localStorage.setItem('lastWiperModel', wModel);
        localStorage.setItem('lastWiperEngine', wEngine);

        document.getElementById('names').innerHTML = '';
        document.getElementById('resultsTable').style.display = 'none';

        if (!wCar && !wModel && !wEngine) {
          wiperWrap.style.display = 'none';
          wipersTable.style.display = 'none';
          setStats( 'لا توجد نتائج. اكتب اسم السيارة أو الموديل أو الماتور.');
          return;
        }

        let data = null;
        try {
          const params = new URLSearchParams({ q_car: wCar, q_model: wModel, q_engine: wEngine });
          const ctrl = new AbortController();
          const to = setTimeout(() => ctrl.abort(), 90000);
          const res = await fetch(`/search_wipers?${params.toString()}`, {
            method: 'GET',
            cache: 'no-store',
            signal: ctrl.signal,
            headers: { Accept: 'application/json' }
          });
          clearTimeout(to);
          const parsed = await readJsonSafe(res);
          if (!res.ok) throw new Error(parsed.detail || `search-wipers-${res.status}`);
          data = parsed;
        } catch (err) {
          let hint = 'تحقق من الإنترنت ثم أعد المحاولة.';
          if (err && err.name === 'AbortError') hint = 'انتهت مهلة الانتظار. أعد المحاولة.';
          else if (err && err.message) {
            const m = String(err.message);
            if (m.includes('search-wipers-')) hint = `ردّ الخادم (${m.replace('search-wipers-', 'رمز ')}) — أعد المحاولة لاحقاً.`;
            else if (m !== 'Failed to fetch') hint = m;
          }
          setStats( `تعذر البحث في القشطان: ${hint}`);
          wiperWrap.style.display = 'none';
          wipersTable.style.display = 'none';
          return;
        }

        const safeRows = Array.isArray(data && data.rows) ? data.rows : [];
        const safeTotal = Number((data && data.total_rows) || safeRows.length || 0);
        setStats( `عدد نتائج القشطان: ${safeTotal} | المصدر: الخادم`);

        const wBody = wipersTable.querySelector('tbody');
        wBody.innerHTML = '';
        safeRows.forEach((r) => {
          const tr = document.createElement('tr');
          tr.innerHTML = `<td>${escapeHtml(String(r.source_file || '').trim())}</td>
            <td>${escapeHtml(String(r.car_name ?? ''))}</td>
            <td>${escapeHtml(String(r.car_model ?? ''))}</td>
            <td>${escapeHtml(String(r.engine ?? ''))}</td>
            <td>${escapeHtml(String(r.wiper_location ?? ''))}</td>
            <td>${escapeHtml(String(r.wiper_number ?? ''))}</td>`;
          wBody.appendChild(tr);
        });
        const showT = safeRows.length > 0;
        wiperWrap.style.display = showT ? 'block' : 'none';
        wipersTable.style.display = showT ? 'table' : 'none';
        if (!showT) {
          setStats( 'لا توجد نتائج للقشطان لهذا البحث.');
        }
        return;
      }

      wiperWrap.style.display = 'none';
      wipersTable.style.display = 'none';

      const qRaw = document.getElementById('queryInput').value.trim();
      const qNumbersRaw = document.getElementById('numberQueryInput').value.trim();
      const sizeTypeRaw = document.getElementById('sizeTypeInput').value.trim();
      const sizeWidthRaw = document.getElementById('sizeWidthInput').value.trim();
      const sizeDiameterRaw = document.getElementById('sizeDiameterInput').value.trim();
      const sizeHeightRaw = document.getElementById('sizeHeightInput').value.trim();

      const q = effectiveMode === 'main' ? qRaw : '';
      const qNumbers = effectiveMode === 'main' ? qNumbersRaw : '';
      const sizeType = effectiveMode === 'size' ? sizeTypeRaw : '';
      const sizeWidth = effectiveMode === 'size' ? sizeWidthRaw : '';
      const sizeDiameter = effectiveMode === 'size' ? sizeDiameterRaw : '';
      const sizeHeight = effectiveMode === 'size' ? sizeHeightRaw : '';

      localStorage.setItem('lastSearchQuery', q);
      localStorage.setItem('lastNumberQuery', qNumbers);
      localStorage.setItem('lastSizeType', sizeType);
      localStorage.setItem('lastSizeWidth', sizeWidth);
      localStorage.setItem('lastSizeDiameter', sizeDiameter);
      localStorage.setItem('lastSizeHeight', sizeHeight);

      if (!q && !qNumbers && !sizeType && !sizeWidth && !sizeDiameter && !sizeHeight) {
        const namesDiv = document.getElementById('names');
        namesDiv.innerHTML = '';
        const table = document.getElementById('resultsTable');
        table.style.display = 'none';
        setStats(effectiveMode === 'main' ? 'اكتب كلمة للبحث.' : 'أدخل القياسات أو اختر النوع.');
        return;
      }

      const params = new URLSearchParams({
        q: q,
        q_numbers: qNumbers,
        size_type: sizeType,
        size_width: sizeWidth,
        size_diameter: sizeDiameter,
        size_height: sizeHeight
      });

      let data = null;
      let sourceLabel = 'الخادم';
      try {
        const ctrl = new AbortController();
        const to = setTimeout(() => ctrl.abort(), 90000);
        const res = await fetch(`/search?${params.toString()}`, {
          method: 'GET',
          cache: 'no-store',
          signal: ctrl.signal,
          headers: { Accept: 'application/json' }
        });
        clearTimeout(to);
        const parsed = await readJsonSafe(res);
        if (!res.ok) throw new Error(parsed.detail || `search-http-${res.status}`);
        if (!parsed || typeof parsed !== 'object') throw new Error('search-bad-json');
        data = parsed;
      } catch (_) {
        await ensureLocalSnapshotLoaded();
        data = searchOfflineSnapshot(q, qNumbers, sizeType, sizeWidth, sizeDiameter, sizeHeight);
        sourceLabel = 'النسخة المحلية';

        // Mobile fallback: if local data is empty but internet exists,
        // pull a snapshot once and retry to avoid "no data" confusion.
        if (!(localSnapshot.rows || []).length && navigator.onLine) {
          try {
            await syncSnapshotFromServer();
            data = searchOfflineSnapshot(q, qNumbers, sizeType, sizeWidth, sizeDiameter, sizeHeight);
            if (data.total_rows > 0) {
              sourceLabel = 'النسخة المحلية (بعد تنزيل البيانات)';
            } else {
              const ctrl2 = new AbortController();
              const to2 = setTimeout(() => ctrl2.abort(), 90000);
              const retry = await fetch(`/search?${params.toString()}`, {
                method: 'GET',
                cache: 'no-store',
                signal: ctrl2.signal,
                headers: { Accept: 'application/json' }
              });
              clearTimeout(to2);
              const parsed2 = await readJsonSafe(retry);
              if (retry.ok && parsed2 && typeof parsed2 === 'object') {
                data = parsed2;
                sourceLabel = 'الخادم';
              }
            }
          } catch (_) {}
        }
      }

      const safeRows = Array.isArray(data && data.rows) ? data.rows : [];
      const safeTotal = Number((data && data.total_rows) || safeRows.length || 0);
      setStats( `عدد النتائج: ${safeTotal} | المصدر: ${sourceLabel}`);
      if (!safeTotal && sourceLabel.includes('النسخة المحلية') && !(localSnapshot.rows || []).length) {
        setSyncInfo('لا توجد بيانات محفوظة محلياً بعد. استخدم "مزامنة للهاتف (Offline)" مرة واحدة وأعد البحث.');
      }

      const namesDiv = document.getElementById('names');
      namesDiv.innerHTML = '';
      const quickItemsRaw = Array.isArray(data && data.matching_items)
        ? data.matching_items
        : (Array.isArray(data && data.matching_item_names) ? data.matching_item_names.map(name => ({
            item_name: name,
            matched_alternative: '',
            size_label: '',
            row_key: name
          })) : []);
      const quickItems = quickItemsRaw.map((entry) => {
        if (entry && typeof entry === 'object') return entry;
        return {
          item_name: String(entry || ''),
          matched_alternative: '',
          size_label: '',
          row_key: String(entry || '')
        };
      }).filter((entry) => String(entry.item_name || '').trim());

      /** إزالة لاحقة مثل « بربيش ... ماء.xlsx» إذا وُسِم اسم الصنف باسم ملف في البيانات */
      function stripTrailingSpreadsheetFileFromDisplayName(label) {
        const s = String(label || '').trim();
        if (!s) return s;
        const re = /\\s+([^.]+)\\.(xlsx|xlsm|xlsb|xls)\\s*$/i;
        const m = re.exec(s);
        if (!m || m.index === undefined) return s;
        const body = String(m[1] || '').trim();
        if (!/[\u0600-\u06FF]/.test(body)) return s;
        return s.slice(0, m.index).trim();
      }

      quickItems.forEach(entry => {
        const btn = document.createElement('button');
        btn.className = 'pill pill-btn';
        btn.type = 'button';
        const quickName = document.createElement('span');
        quickName.className = 'quick-name';
        quickName.textContent = stripTrailingSpreadsheetFileFromDisplayName(entry.item_name || '');
        btn.appendChild(quickName);
        if (entry.matched_alternative) {
          const quickAlt = document.createElement('span');
          quickAlt.className = 'quick-alt';
          quickAlt.textContent = entry.matched_alternative;
          btn.appendChild(quickAlt);
        }
        if (entry.size_label) {
          const quickSize = document.createElement('span');
          quickSize.className = 'quick-size';
          quickSize.textContent = entry.size_label;
          btn.appendChild(quickSize);
        }
        btn.onclick = () => focusRowByKey(entry.row_key);
        namesDiv.appendChild(btn);
      });

      const table = document.getElementById('resultsTable');
      const body = table.querySelector('tbody');
      body.innerHTML = '';
      safeRows.forEach(r => {
        const sizeLabel = String(r.size_label || '');
        const nameForUi = stripTrailingSpreadsheetFileFromDisplayName(r.item_name || '');
        const originalNumbers = String(r.original_numbers || '');
        const copyIcon =
          '<button type="button" title="نسخ الرقم الأصلي + اكتشاف الموقع" aria-label="نسخ الرقم الأصلي" data-copy="1" ' +
          'style="display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;padding:0;margin-left:8px;' +
          'border:0;background:transparent;color:#1a2744;cursor:pointer;line-height:1;opacity:.9;" ' +
          'onmouseenter="this.style.opacity=1" onmouseleave="this.style.opacity=.9">' +
          '<svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true" focusable="false" style="display:block">' +
          '<rect x="8" y="3" width="12" height="14" rx="2" ry="2" fill="none" stroke="currentColor" stroke-width="2"/>' +
          '<rect x="4" y="7" width="12" height="14" rx="2" ry="2" fill="none" stroke="currentColor" stroke-width="2"/>' +
          '</svg>' +
          '</button>';
        const nameCell = `${copyIcon}<span>${escapeHtml(nameForUi)}</span>${sizeLabel ? `<div class="quick-size">${escapeHtml(sizeLabel)}</div>` : ''}`;
        const itemNumberDisplay = String(r.company_number || r.item_number || '');
        const fileName = String(r.source_file || '').trim();
        const tr = document.createElement('tr');
        tr.dataset.rowKey = String(r.row_key || '');
        tr.innerHTML = `<td>${escapeHtml(fileName)}</td>
                        <td>${nameCell}</td>
                        <td>${escapeHtml(itemNumberDisplay)}</td>
                        <td>${escapeHtml(String(r.alternatives ?? ''))}</td>`;
        const copyBtn = tr.querySelector('button[data-copy="1"]');
        if (copyBtn) {
          copyBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            copyOriginalAndGoToLocation(originalNumbers, itemNumberDisplay);
          });
        }
        body.appendChild(tr);
      });
      table.style.display = safeRows.length ? 'table' : 'none';
    }

    function initAutoSearch() {
      const queryInput = document.getElementById('queryInput');
      const savedQuery = localStorage.getItem('lastSearchQuery') || '';
      queryInput.value = savedQuery;
      document.getElementById('numberQueryInput').value = localStorage.getItem('lastNumberQuery') || '';
      document.getElementById('sizeTypeInput').value = localStorage.getItem('lastSizeType') || '';
      document.getElementById('sizeWidthInput').value = localStorage.getItem('lastSizeWidth') || '';
      document.getElementById('sizeDiameterInput').value = localStorage.getItem('lastSizeDiameter') || '';
      document.getElementById('sizeHeightInput').value = localStorage.getItem('lastSizeHeight') || '';
      document.getElementById('wiperCarInput').value = localStorage.getItem('lastWiperCar') || '';
      document.getElementById('wiperModelInput').value = localStorage.getItem('lastWiperModel') || '';
      document.getElementById('wiperEngineInput').value = localStorage.getItem('lastWiperEngine') || '';
      const savedTab = localStorage.getItem('activeSearchTab') || 'main';
      activeSearchTab = (savedTab === 'size' || savedTab === 'wiper' || savedTab === 'location' || savedTab === 'print')
        ? savedTab
        : 'main';
      setBodySearchTabClass(activeSearchTab);
      document.getElementById('tabMainBtn').classList.toggle('active', activeSearchTab === 'main');
      document.getElementById('tabSizeBtn').classList.toggle('active', activeSearchTab === 'size');
      document.getElementById('tabWiperBtn').classList.toggle('active', activeSearchTab === 'wiper');
      document.getElementById('tabLocBtn').classList.toggle('active', activeSearchTab === 'location');
      const printBtnInit = document.getElementById('tabPrintBtn');
      if (printBtnInit) printBtnInit.classList.toggle('active', activeSearchTab === 'print');
      document.getElementById('mainSearchPanel').classList.toggle('active', activeSearchTab === 'main');
      document.getElementById('sizeSearchPanel').classList.toggle('active', activeSearchTab === 'size');
      document.getElementById('wiperSearchPanel').classList.toggle('active', activeSearchTab === 'wiper');
      document.getElementById('locationSearchPanel').classList.toggle('active', activeSearchTab === 'location');
      const printPanelInit = document.getElementById('printSearchPanel');
      if (printPanelInit) printPanelInit.classList.toggle('active', activeSearchTab === 'print');
      {
        let lt = localStorage.getItem('lastLocationTextQuery');
        if (lt == null || lt === '') {
          const legacy = localStorage.getItem('lastLocationQuery');
          if (legacy) {
            lt = legacy;
            try { localStorage.setItem('lastLocationTextQuery', legacy); } catch (_) {}
          }
        }
        document.getElementById('locationTextQueryInput').value = lt || '';
        document.getElementById('locationBarcodeQueryInput').value = localStorage.getItem('lastLocationBarcodeQuery') || '';
      }
      updateSizeInputsLayout();
      if (activeSearchTab === 'location') {
        ensureQrLib().catch(() => {});
        startHesabateLocationPoller();
      } else {
        try { clearLocationTable(null); } catch (_) {}
      }
      if (activeSearchTab === 'print') {
        ensureJsBarcodeLib().catch(() => {});
      }
      // ارفع قياسات GUI الحالية من المتصفح للسيرفر (عشان نصير نعتمدها كافتراضي)
      try {
        if (localStorage.getItem('printLayoutSettings')) {
          setTimeout(() => { uploadPrintLayoutPrefsToServer(false); }, 1200);
        }
      } catch (_) {}
      refreshHesabateStatus();

      const printQueryLeft = document.getElementById('printQueryLeft');
      const printQueryRight = document.getElementById('printQueryRight');
      if (printQueryLeft) {
        printQueryLeft.addEventListener('input', () => schedulePrintLocalLookup('left'));
      }
      if (printQueryRight) {
        printQueryRight.addEventListener('input', () => schedulePrintLocalLookup('right'));
      }
      const printNameLeft = document.getElementById('printNameLeft');
      const printNameRight = document.getElementById('printNameRight');
      if (printNameLeft) {
        printNameLeft.addEventListener('input', () => {
          printNameLeft.dataset.fromFetch = '0';
          refreshPrintNamePreview('left');
        });
      }
      if (printNameRight) {
        printNameRight.addEventListener('input', () => {
          printNameRight.dataset.fromFetch = '0';
          refreshPrintNamePreview('right');
        });
      }
      const printQtyInput = document.getElementById('printQtyInput');
      if (printQtyInput) {
        printQtyInput.addEventListener('focus', function () {
          const el = this;
          setTimeout(function () { try { el.select(); } catch (_) {} }, 0);
        });
        printQtyInput.addEventListener('mouseup', function (e) {
          // أول كبسة تحدد الرقم كامل بدون ما يلغى التحديد
          e.preventDefault();
        });
        printQtyInput.addEventListener('change', () => {
          const el = document.getElementById('printQtyInput');
          if (el) {
            const n = normalizePrintQtyValue(el.value);
            el.value = n || '1';
          }
          onPrintQtyInputChange();
        });
        // تأكيد القيمة إنجليزية عند الإقلاع
        printQtyInput.value = normalizePrintQtyValue(printQtyInput.value) || '1';
      }
      // استرجاع آخر طباعة/قيم مكتوبة بعد تحديث الصفحة
      restorePrintFormState().catch(() => {});

      queryInput.addEventListener('input', () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => {
          searchNow('main');
        }, 250);
      });
      document.getElementById('numberQueryInput').addEventListener('input', () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => {
          searchNow('main');
        }, 250);
      });
      document.getElementById('sizeTypeInput').addEventListener('change', () => {
        updateSizeInputsLayout();
        searchNow('size');
      });
      document.getElementById('sizeWidthInput').addEventListener('input', () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => searchNow('size'), 250);
      });
      document.getElementById('sizeDiameterInput').addEventListener('input', () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => searchNow('size'), 250);
      });
      document.getElementById('sizeHeightInput').addEventListener('input', () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => searchNow('size'), 250);
      });
      ['wiperCarInput', 'wiperModelInput', 'wiperEngineInput'].forEach((id) => {
        document.getElementById(id).addEventListener('input', () => {
          clearTimeout(searchDebounceTimer);
          searchDebounceTimer = setTimeout(() => {
            if (activeSearchTab === 'wiper') searchNow('wiper');
          }, 250);
        });
      });
      const locTextEl = document.getElementById('locationTextQueryInput');
      if (locTextEl) {
        locTextEl.addEventListener('input', () => {
          clearTimeout(searchDebounceTimer);
          searchDebounceTimer = setTimeout(() => {
            if (activeSearchTab === 'location') searchNow('location');
          }, 300);
        });
        locTextEl.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            clearTimeout(searchDebounceTimer);
            if (activeSearchTab === 'location') searchNow('location');
          }
        });
      }
      const locBcEl = document.getElementById('locationBarcodeQueryInput');
      if (locBcEl) {
        locBcEl.addEventListener('input', () => {
          clearTimeout(searchDebounceTimer);
          searchDebounceTimer = setTimeout(() => {
            if (activeSearchTab === 'location') searchNow('location');
          }, 400);
        });
        locBcEl.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            clearTimeout(searchDebounceTimer);
            if (activeSearchTab === 'location') searchNow('location');
          }
        });
        locBcEl.addEventListener('paste', () => {
          setTimeout(() => {
            locBcEl.value = lightSanitizeBarcodeValue(locBcEl.value);
            clearTimeout(searchDebounceTimer);
            searchDebounceTimer = setTimeout(() => {
              if (activeSearchTab === 'location') searchNow('location');
            }, 150);
          }, 0);
        });
      }

      clearInterval(autoRefreshTimer);
      autoRefreshTimer = setInterval(() => {
        loadStats();
      }, 10000);
    }

    function focusRowByKey(rowKey) {
      const rows = Array.from(document.querySelectorAll('#resultsTable tbody tr'));
      const target = rows.find(r => r.dataset.rowKey === String(rowKey || ''));
      if (!target) return;

      rows.forEach(r => r.classList.remove('row-focus'));
      target.classList.add('row-focus');
      target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }

    function escapeHtml(str) {
      return String(str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    function looksLikeOriginalNumber(token) {
      const t = String(token || '').trim();
      if (!t || t.length < 2) return false;
      if (!/\\d|[\\u0660-\\u0669\\u06F0-\\u06F9]/.test(t)) return false;
      const noDigits = t.replace(/[\\u0660-\\u0669\\u06F0-\\u06F9\\d]/g, '');
      if (/[\\u0600-\\u06FF]/.test(noDigits)) return false;
      const cleaned = t.replace(/[\\s\\-_.\\/\\\\|:،؛,]+/g, '');
      if (!cleaned || !/^[\\dA-Za-z\\u0660-\\u0669\\u06F0-\\u06F9]+$/u.test(cleaned)) return false;
      return /\\d|[\\u0660-\\u0669\\u06F0-\\u06F9]/.test(cleaned);
    }

    function pickFirstOriginalNumber(raw) {
      const s = String(raw || '').trim();
      if (!s) return '';
      const parts = s.split(/[\\s,،;؛|/\\\\]+/).map(p => String(p || '').trim()).filter(Boolean);
      for (const p of parts) {
        if (looksLikeOriginalNumber(p)) return p;
      }
      const scanRe = /[A-Za-z0-9][A-Za-z0-9\\-_.\\/\\\\]{0,48}\\d[A-Za-z0-9\\-_.\\/\\\\]{0,48}|\\d[A-Za-z0-9\\-_.\\/\\\\]{1,49}/g;
      let m;
      while ((m = scanRe.exec(s)) !== null) {
        if (looksLikeOriginalNumber(m[0])) return m[0];
      }
      return '';
    }

    async function copyTextToClipboard(text) {
      const t = String(text || '').trim();
      if (!t) return false;
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(t);
          return true;
        }
      } catch (_) {}
      try {
        const ta = document.createElement('textarea');
        ta.value = t;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        ta.style.pointerEvents = 'none';
        document.body.appendChild(ta);
        ta.focus();
        ta.select();
        const ok = document.execCommand('copy');
        document.body.removeChild(ta);
        return !!ok;
      } catch (_) {
        return false;
      }
    }

    async function copyOriginalAndGoToLocation(originalCandidate, fallbackCandidate = '') {
      const picked = pickFirstOriginalNumber(originalCandidate) || pickFirstOriginalNumber(fallbackCandidate);
      if (!picked) return;

      await copyTextToClipboard(picked);

      setSearchTab('location');
      const textEl = document.getElementById('locationTextQueryInput');
      const bcEl = document.getElementById('locationBarcodeQueryInput');
      if (bcEl) bcEl.value = '';
      if (textEl) {
        textEl.value = picked;
        try { localStorage.setItem('lastLocationTextQuery', picked); } catch (_) {}
        textEl.focus();
        textEl.setSelectionRange(textEl.value.length, textEl.value.length);
      }
      await searchNow('location');
    }

    function formatLastUploadLabel(ts) {
      const t = String(ts || '').trim();
      return t ? `آخر تحديث: ${t}` : 'آخر تحديث: —';
    }
    async function loadStats() {
      try {
        const res = await fetch('/stats', { cache: 'no-store' });
        const data = await res.json();
        const wn = Number(data.total_wiper_rows || 0);
        const locn = Number(data.total_location_rows || 0);
        setStats(`صفوف: ${data.total_rows} · قشطان: ${wn} · مواقع: ${locn} · أصناف: ${data.total_items || 0}`);
        const mainEl = document.getElementById('mainLastUpdate');
        const wiperEl = document.getElementById('wiperLastUpdate');
        if (mainEl) mainEl.textContent = formatLastUploadLabel(data.last_products_upload_at);
        if (wiperEl) wiperEl.textContent = formatLastUploadLabel(data.last_wiper_upload_at);
      } catch (_) {
        setStats(`صفوف محلية: ${localSnapshot.total_rows || (localSnapshot.rows || []).length}`);
      }
    }
    const dropZone = document.getElementById('dropZone');
    if (dropZone) {
      dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('active');
      });
      dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('active');
      });
      dropZone.addEventListener('drop', (e) => {
        handleDrop(e);
      });
    }
    try {
      updateSelectionInfo();
      setUploadProgress(0, '');
      registerServiceWorker();
      initAutoSearch();
      loadStats();
      searchNow();
      ensureLatestClientVersion();
      // تحديث مواقع حساباتي تلقائياً بدون انتظار فتح تبويب معيّن
      startHesabateAutoEnsure();
      try {
        syncPrintLayoutSettingsUi(null);
        // المحرر مغلق افتراضياً — يُبنى المحتوى عند الفتح اليدوي
        const desInit = document.getElementById('printLayoutDesigner');
        if (desInit) {
          desInit.open = false;
          desInit.addEventListener('toggle', () => {
            if (desInit.open) {
              try { bindPrintDesignerFontUi(); refreshPrintDesignerBoard(); } catch (_) {}
            }
          });
        }
      } catch (_) {}
      try {
        window.addEventListener('pointermove', (ev) => {
          if (!printDesignerDrag) return;
          applyDesignerDrag(ev.clientY, ev.clientX);
        });
        window.addEventListener('pointerup', () => endDesignerDrag());
        window.addEventListener('pointercancel', () => endDesignerDrag());
      } catch (_) {}
      setTimeout(() => {
        try {
          restoreServerFromLocalSnapshotIfEmpty();
          autoSyncGoogleDriveIfServerEmpty();
        } catch (_) {}
      }, 12000);
    } catch (bootErr) {
      try {
        console.error('boot-failed', bootErr);
        const st = document.getElementById('hesabateSyncStatus') || document.getElementById('stats');
        if (st) st.textContent = 'تعذّر تشغيل الواجهة بالكامل — حدّث الصفحة (Ctrl+F5).';
      } catch (_) {}
    }
  </script>
</body>
</html>
"""
    ).replace('href="/app-icon.png"', f'href="/app-icon.png?v={_icon_q}"').replace(
        'href="/apple-touch-icon.png"', f'href="/apple-touch-icon.png?v={_at_q}"', 1
    ).replace("{APP_VERSION}", APP_VERSION)
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "public, max-age=60, stale-while-revalidate=86400"},
    )


@app.post("/upload")
async def upload(files: list[UploadFile] = File(...)) -> dict:
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    inserted_total = 0
    inserted_wiper_total = 0
    inserted_location_total = 0
    inserted_items_total = 0
    processed_files = 0
    skipped_files = 0

    for file in files:
        lower = file.filename.lower() if file.filename else ""
        if not lower.endswith((".xlsx", ".xls", ".xlsm")):
            continue

        content = await file.read()
        content_hash, file_size = file_fingerprint(content)
        fname = file.filename or ""
        if should_process_as_wiper_spreadsheet(fname, content):
            if is_same_wiper_upload(fname, content_hash, file_size):
                skipped_files += 1
                continue
            try:
                inserted_wiper_total += process_wiper_excel_file(content, fname, content_hash, file_size)
                processed_files += 1
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Could not process wiper file '{file.filename}': {exc}",
                ) from exc
            continue

        if should_process_as_location_spreadsheet(fname, content):
            if is_same_location_upload(fname, content_hash, file_size):
                skipped_files += 1
                continue
            try:
                inserted_location_total += process_location_excel_file(content, fname, content_hash, file_size)
                processed_files += 1
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Could not process location file '{file.filename}': {exc}",
                ) from exc
            continue

        if is_same_uploaded_file(file.filename, content_hash, file_size):
            skipped_files += 1
            continue
        try:
            inserted_rows, inserted_items = process_excel_file(content, file.filename, content_hash, file_size)
            inserted_total += inserted_rows
            inserted_items_total += inserted_items
            processed_files += 1
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Could not process file '{file.filename}': {exc}",
            ) from exc

    return {
        "files_count": processed_files,
        "inserted_rows": inserted_total,
        "inserted_wiper_rows": inserted_wiper_total,
        "inserted_location_rows": inserted_location_total,
        "inserted_items": inserted_items_total,
        "skipped_files": skipped_files,
    }


@app.post("/upload_location_sheet")
async def upload_location_sheet(file: UploadFile = File(...)) -> dict:
    """
    رفع ملف المواقع: يحدّث عمود موقع الرف (I) حسب الباركود.
    إذا كان الكتالوج من حساباتي API موجوداً: دمج المواقع فقط بدون مسح الأصناف.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="لم يُختَر ملف")
    lower = str(file.filename).lower()
    if not lower.endswith((".xlsx", ".xls", ".xlsm")):
        raise HTTPException(status_code=400, detail="يُقبل Excel فقط (.xlsx / .xlsm / .xls)")
    content = await file.read()
    content_hash, file_size = file_fingerprint(content)
    fixed_name = LOCATION_TAB_IMPORT_SOURCE_FILE

    conn = get_db()
    try:
        api_rows = conn.execute(
            "SELECT COUNT(*) AS c FROM location_rows WHERE source_file = ?",
            (LOCATION_API_SOURCE_FILE,),
        ).fetchone()
        has_api_catalog = int((api_rows["c"] if api_rows else 0) or 0) > 0
    finally:
        conn.close()

    try:
        if has_api_catalog or hesabate_configured():
            merged = merge_shelf_locations_from_excel_bytes(content)
            # احفظ بصمة الملف حتى لا يُعاد نفس الدمج بلا داعي
            upsert_location_file_meta(fixed_name, int(merged.get("mapped_barcodes") or 0), content_hash, file_size)
            return {
                "ok": True,
                "skipped": False,
                "inserted_rows": int(merged.get("updated_rows") or 0),
                "mapped_barcodes": int(merged.get("mapped_barcodes") or 0),
                "mode": "merge_shelves",
                "message": (
                    f"تم تحديث مواقع الرف لـ {int(merged.get('updated_rows') or 0)} صنف "
                    f"من {int(merged.get('mapped_barcodes') or 0)} باركود في الملف "
                    "(بدون مسح بيانات حساباتي)."
                ),
            }
        if is_same_location_upload(fixed_name, content_hash, file_size):
            return {"ok": True, "skipped": True, "inserted_rows": 0}
        inserted = process_location_excel_file(
            content, fixed_name, content_hash, file_size, replace_entire_table=True
        )
        # خزّن المواقع في الجدول الدائم أيضاً
        conn2 = get_db()
        try:
            migrate_current_locations_into_shelf_map(conn2)
            conn2.commit()
        finally:
            conn2.close()
        return {"ok": True, "skipped": False, "inserted_rows": inserted, "mode": "replace"}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"تعذر معالجة الملف: {exc}") from exc


@app.post("/google_drive/sync")
def google_drive_sync() -> dict:
    folder_id = (os.getenv("GDRIVE_FOLDER_ID") or "").strip()
    if not folder_id:
        raise HTTPException(status_code=400, detail="Missing GDRIVE_FOLDER_ID")

    try:
        service = get_gdrive_service()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Google Drive setup error: {exc}") from exc

    files = list_gdrive_excel_files(service, folder_id)
    processed = 0
    skipped = 0
    inserted_rows_total = 0
    inserted_wiper_total = 0
    inserted_location_total = 0
    inserted_items_total = 0
    failed: list[str] = []
    unsupported: list[str] = []

    for f in files:
        file_name = normalize_source_file_name(f.get("relative_name") or f.get("name") or "")
        if not file_name:
            continue
        if not is_openpyxl_supported_file_name(file_name):
            unsupported.append(file_name)
            continue

        try:
            content = download_gdrive_file_bytes(service, f["id"])
            content_hash, file_size = file_fingerprint(content)
            if should_process_as_wiper_spreadsheet(file_name, content):
                if is_same_wiper_upload(file_name, content_hash, file_size):
                    skipped += 1
                    continue
                inserted_wiper_total += process_wiper_excel_file(content, file_name, content_hash, file_size)
                processed += 1
                continue
            if should_process_as_location_spreadsheet(file_name, content):
                if is_same_location_upload(file_name, content_hash, file_size):
                    skipped += 1
                    continue
                inserted_location_total += process_location_excel_file(
                    content, file_name, content_hash, file_size
                )
                processed += 1
                continue
            if is_same_uploaded_file(file_name, content_hash, file_size):
                skipped += 1
                continue
            rows, items = process_excel_file(content, file_name, content_hash, file_size)
            processed += 1
            inserted_rows_total += rows
            inserted_items_total += items
        except Exception as exc:
            failed.append(f"{file_name}: {exc}")

    return {
        "scanned_files": len(files),
        "files_count": processed,
        "skipped_files": skipped,
        "inserted_rows": inserted_rows_total,
        "inserted_wiper_rows": inserted_wiper_total,
        "inserted_location_rows": inserted_location_total,
        "inserted_items": inserted_items_total,
        "unsupported_files": unsupported[:100],
        "failed_files": failed[:100],
    }


@app.post("/upload_rows/start")
def upload_rows_start(payload: UploadRowsStartIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    if is_same_uploaded_file(source_key, payload.content_hash, payload.file_size):
        return {"skip": True}

    conn = get_db()
    try:
        conn.execute("DELETE FROM products WHERE source_file = ?", (source_key,))
        conn.commit()
    finally:
        conn.close()
    return {"skip": False}


@app.post("/upload_rows/chunk")
def upload_rows_chunk(payload: UploadRowsChunkIn) -> dict:
    if not payload.rows:
        return {"inserted_rows": 0}

    source_key = normalize_source_file_name(payload.file_name)
    rows_to_insert = []
    for row in payload.rows:
        if not any(
            [
                row.item_name,
                row.item_number,
                row.company_number,
                row.original_numbers,
                row.notes,
                row.alternatives,
                row.size_width,
                row.size_diameter,
                row.size_height,
            ]
        ):
            continue
        rows_to_insert.append(
            (
                row.item_name.strip(),
                row.item_number.strip(),
                row.company_number.strip(),
                row.original_numbers.strip(),
                row.notes.strip(),
                sanitize_alternatives_vs_original(row.alternatives, row.original_numbers),
                row.size_width.strip(),
                row.size_diameter.strip(),
                row.size_height.strip(),
                source_key,
                (row.source_sheet or "Sheet1").strip(),
            )
        )

    if not rows_to_insert:
        return {"inserted_rows": 0}

    conn = get_db()
    try:
        conn.executemany(
            """
            INSERT INTO products (
                item_name, item_number, company_number, original_numbers, notes, alternatives,
                size_width, size_diameter, size_height, source_file, source_sheet
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        conn.commit()
    finally:
        conn.close()
    return {"inserted_rows": len(rows_to_insert)}


@app.post("/upload_rows/finish")
def upload_rows_finish(payload: UploadRowsFinishIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    conn = get_db()
    try:
        rows_count = conn.execute(
            "SELECT COUNT(*) AS c FROM products WHERE source_file = ?",
            (source_key,),
        ).fetchone()["c"]
    finally:
        conn.close()

    upsert_uploaded_file_meta(source_key, rows_count, payload.content_hash, payload.file_size)
    return {"rows_count": rows_count}


@app.post("/upload_rows/abort")
def upload_rows_abort(payload: UploadRowsAbortIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    conn = get_db()
    try:
        conn.execute("DELETE FROM products WHERE source_file = ?", (source_key,))
        conn.execute("DELETE FROM uploaded_files WHERE file_name = ?", (source_key,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.post("/upload_location_rows/start")
def upload_location_rows_start(payload: LocationRowsStartIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    if is_same_location_upload(source_key, payload.content_hash, payload.file_size):
        return {"skip": True}
    conn = get_db()
    try:
        ensure_location_rows_import_schema(conn)
        if payload.replace_entire_table:
            conn.execute("DELETE FROM location_rows")
        else:
            conn.execute("DELETE FROM location_rows WHERE source_file = ?", (source_key,))
        conn.commit()
    finally:
        conn.close()
    return {"skip": False}


@app.post("/upload_location_rows/chunk")
def upload_location_rows_chunk(payload: LocationRowsChunkIn) -> dict:
    if not payload.rows:
        return {"inserted_rows": 0}
    source_key = normalize_source_file_name(payload.file_name)
    rows_to_insert: list[tuple[str, ...]] = []
    for row in payload.rows:
        if not any(
            [
                row.item_name,
                row.location,
                row.company,
                row.original_number,
                row.barcode,
                row.unit_barcodes,
                row.brand,
                row.notes,
                row.quantity,
                row.price,
            ]
        ):
            continue
        bc = row.barcode.strip()
        ub = (row.unit_barcodes or "").strip()
        bc_store = " | ".join(x for x in [bc, ub] if x)
        in_item = row.item_name.strip()
        in_co = row.company.strip()
        in_br = row.brand.strip()
        in_no = row.notes.strip()
        in_orig = row.original_number.strip()
        in_qty = (row.quantity or "").strip()
        nb = location_row_normalized_barcode_value(bc, ub)
        nt = location_row_normalized_text_value(in_item, in_co, in_br, in_no, in_orig)
        rows_to_insert.append(
            (
                in_item,
                row.location.strip(),
                in_co,
                in_orig,
                bc_store,
                in_br,
                in_no,
                in_qty,
                row.price.strip(),
                source_key,
                (row.source_sheet or "Sheet1").strip(),
                nb,
                nt,
            )
        )
    if not rows_to_insert:
        return {"inserted_rows": 0}
    conn = get_db()
    try:
        ensure_location_rows_import_schema(conn)
        try:
            conn.executemany(
                """
                INSERT INTO location_rows (
                    item_name, location, company, original_number, barcode,
                    brand, notes, quantity, price, source_file, source_sheet,
                    normalized_barcode, normalized_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows_to_insert,
            )
            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            detail = f"{type(exc).__name__}: {exc!s}"
            if len(detail) > 900:
                detail = detail[:900] + "…"
            raise HTTPException(status_code=400, detail=f"تعذر حفظ دفعة المواقع: {detail}") from exc
    finally:
        conn.close()
    return {"inserted_rows": len(rows_to_insert)}


@app.post("/upload_location_rows/finish")
def upload_location_rows_finish(payload: LocationRowsFinishIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    conn = get_db()
    try:
        rows_count = conn.execute(
            "SELECT COUNT(*) AS c FROM location_rows WHERE source_file = ?",
            (source_key,),
        ).fetchone()["c"]
    finally:
        conn.close()
    upsert_location_file_meta(source_key, rows_count, payload.content_hash, payload.file_size)
    return {"rows_count": rows_count}


@app.post("/upload_location_rows/abort")
def upload_location_rows_abort(payload: LocationRowsAbortIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    conn = get_db()
    try:
        conn.execute("DELETE FROM location_rows WHERE source_file = ?", (source_key,))
        conn.execute("DELETE FROM uploaded_files WHERE file_name = ?", (source_key,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.post("/upload_wiper_rows/start")
def upload_wiper_rows_start(payload: WiperRowsStartIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    if is_same_wiper_upload(source_key, payload.content_hash, payload.file_size):
        return {"skip": True}
    conn = get_db()
    try:
        conn.execute("DELETE FROM wiper_rows WHERE source_file = ?", (source_key,))
        conn.commit()
    finally:
        conn.close()
    return {"skip": False}


@app.post("/upload_wiper_rows/chunk")
def upload_wiper_rows_chunk(payload: WiperRowsChunkIn) -> dict:
    if not payload.rows:
        return {"inserted_rows": 0}
    source_key = normalize_source_file_name(payload.file_name)
    rows_to_insert: list[tuple[str, str, str, str, str, str, str]] = []
    for row in payload.rows:
        if not any(
            [
                row.car_name,
                row.car_model,
                row.engine,
                row.wiper_location,
                row.wiper_number,
            ]
        ):
            continue
        rows_to_insert.append(
            (
                row.car_name.strip(),
                row.car_model.strip(),
                row.engine.strip(),
                row.wiper_location.strip(),
                row.wiper_number.strip(),
                source_key,
                (row.source_sheet or "Sheet1").strip(),
            )
        )
    if not rows_to_insert:
        return {"inserted_rows": 0}
    conn = get_db()
    try:
        conn.executemany(
            """
            INSERT INTO wiper_rows (
                car_name, car_model, engine, wiper_location, wiper_number, source_file, source_sheet
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows_to_insert,
        )
        conn.commit()
    finally:
        conn.close()
    return {"inserted_rows": len(rows_to_insert)}


@app.post("/upload_wiper_rows/finish")
def upload_wiper_rows_finish(payload: WiperRowsFinishIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    conn = get_db()
    try:
        rows_count = conn.execute(
            "SELECT COUNT(*) AS c FROM wiper_rows WHERE source_file = ?",
            (source_key,),
        ).fetchone()["c"]
    finally:
        conn.close()
    upsert_wiper_file_meta(source_key, rows_count, payload.content_hash, payload.file_size)
    return {"rows_count": rows_count}


@app.post("/upload_wiper_rows/abort")
def upload_wiper_rows_abort(payload: WiperRowsAbortIn) -> dict:
    source_key = normalize_source_file_name(payload.file_name)
    conn = get_db()
    try:
        conn.execute("DELETE FROM wiper_rows WHERE source_file = ?", (source_key,))
        conn.execute("DELETE FROM uploaded_files WHERE file_name = ?", (source_key,))
        conn.commit()
    finally:
        conn.close()
    return {"ok": True}


@app.get("/search")
def search(
    q: str = "",
    q_numbers: str = "",
    size_type: str = "",
    size_width: str = "",
    size_diameter: str = "",
    size_height: str = "",
) -> dict:
    tokens = tokenize_query(q)
    number_tokens = tokenize_query(q_numbers)
    file_hints, tokens = extract_file_search_hints(q, tokens)
    size_active = any([size_type.strip(), size_width.strip(), size_diameter.strip(), size_height.strip()])
    if not tokens and not number_tokens and not size_active and not file_hints:
        return {"total_rows": 0, "matching_item_names": [], "matching_items": [], "rows": []}

    parsed_year_tokens = [parse_query_year_token(t) for t in tokens]
    query_years = sorted({y for ys in parsed_year_tokens if ys for y in ys})
    explicit_year_ops = any(is_explicit_year_operator_token(t) for t in tokens)
    text_tokens = [t for t, ys in zip(tokens, parsed_year_tokens) if ys is None]

    conn = get_db()
    try:
        where_parts = []
        params: list[str] = []

        for token in text_tokens:
            # Digits can appear in Arabic/Persian forms inside Excel text.
            # Keep final matching in Python (normalized) to avoid false misses.
            if any(ch.isdigit() for ch in token):
                continue
            var_ors: list[str] = []
            for v in spelling_query_variants(token):
                like = f"%{v}%"
                var_ors.append(
                    "(lower(item_name) LIKE ? OR lower(alternatives) LIKE ? OR lower(source_file) LIKE ?)"
                )
                params.extend([like, like, like])
            if var_ors:
                where_parts.append("(" + " OR ".join(var_ors) + ")")

        for token in number_tokens:
            if any(ch.isdigit() for ch in token):
                continue
            var_ors = []
            for v in spelling_query_variants(token):
                like = f"%{v}%"
                var_ors.append(
                    "(lower(company_number) LIKE ? OR lower(original_numbers) LIKE ? OR lower(notes) LIKE ?)"
                )
                params.extend([like, like, like])
            if var_ors:
                where_parts.append("(" + " OR ".join(var_ors) + ")")

        if file_hints:
            fh_sql, fh_params = file_hints_sql_clause(file_hints)
            where_parts.append(fh_sql)
            params.extend(fh_params)

        # تصفية أولية بالقياسات (أرقام فقط) لتقليل الصفوف قبل فلترة بايثون
        if size_active:
            for raw in (size_width, size_diameter, size_height):
                v = normalize_text(raw or "").replace(",", ".")
                if v and re.fullmatch(r"[\d.]+", v):
                    likev = f"%{v.lower()}%"
                    where_parts.append(
                        "(lower(COALESCE(size_width,'')) LIKE ? OR "
                        "lower(COALESCE(size_diameter,'')) LIKE ? OR "
                        "lower(COALESCE(size_height,'')) LIKE ?)"
                    )
                    params.extend([likev, likev, likev])

        # Size/type filtering is applied in Python for better tolerance.

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        candidate_rows = conn.execute(
            f"""
            SELECT item_name, item_number, company_number, original_numbers, notes, alternatives,
                   size_width, size_diameter, size_height, source_file, source_sheet
            FROM products
            WHERE {where_clause}
            ORDER BY item_name
            LIMIT 40000
            """,
            params,
        ).fetchall()

        filtered_rows = [
            r
            for r in candidate_rows
            if row_matches_query(r, text_tokens, query_years)
            and row_matches_number_query(r, number_tokens)
            and (
                not file_hints
                or any(file_search_hint_matches_path(r["source_file"] or "", h) for h in file_hints)
            )
            and file_matches_size_type_with_fallback(r, r["source_file"] or "", size_type, size_width, size_diameter)
            and row_matches_size_filters(size_type, r, size_width, size_diameter, size_height)
        ]
        filtered_rows = dedupe_rows_by_normalized_original_for_search(
            filtered_rows, text_tokens, query_years
        )
        rows = filtered_rows[:500]
    finally:
        conn.close()

    prepared_rows = []
    quick_items = []
    seen_quick = set()
    for index, row in enumerate(rows):
        row_dict = sql_row_to_dict(row)
        matched_alt = extract_matched_alternative(
            row_dict.get("alternatives", ""),
            text_tokens,
            query_years,
            q,
        )
        if not (row_dict.get("alternatives") or "").strip():
            matched_alt = ""
        row_key = f"{row_dict.get('source_file', '')}|{row_dict.get('source_sheet', '')}|{row_dict.get('company_number') or row_dict.get('item_number', '')}|{index}"
        size_label = build_size_label_display(
            row_dict.get("source_file", "") or "",
            row_dict.get("size_width") or "",
            row_dict.get("size_diameter") or "",
            row_dict.get("size_height") or "",
        )
        item_number_display = (row_dict.get("company_number") or row_dict.get("item_number") or "").strip()
        row_dict["item_number"] = item_number_display
        row_dict["size_label"] = size_label
        row_dict["matched_alternative"] = matched_alt
        row_dict["alternatives"] = (row_dict.get("alternatives") or "").strip()
        row_dict["match_score"] = year_match_score(matched_alt, query_years)
        row_dict["row_key"] = row_key
        prepared_rows.append(row_dict)

        quick_key = (row_dict.get("item_name", ""), matched_alt)
        if quick_key not in seen_quick and row_dict.get("item_name", ""):
            seen_quick.add(quick_key)
            quick_items.append(
                {
                    "item_name": row_dict["item_name"],
                    "matched_alternative": matched_alt,
                    "size_label": row_dict.get("size_label", ""),
                    "row_key": row_key,
                }
            )
        if len(quick_items) >= 100:
            break

    # Keep "most precise only" behavior for explicit year operators (+YY, YY-, YY-YY),
    # but keep all valid matches for plain year tokens like "20".
    if explicit_year_ops and query_years and prepared_rows:
        max_score = max(r.get("match_score", 0) for r in prepared_rows)
        if max_score > 0:
            prepared_rows = [r for r in prepared_rows if r.get("match_score", 0) == max_score]

            # Rebuild quick items after precision filtering.
            quick_items = []
            seen_quick = set()
            for r in prepared_rows:
                quick_key = (r.get("item_name", ""), r.get("matched_alternative", ""))
                if quick_key in seen_quick or not r.get("item_name"):
                    continue
                seen_quick.add(quick_key)
                quick_items.append(
                    {
                        "item_name": r["item_name"],
                        "matched_alternative": r.get("matched_alternative", ""),
                        "size_label": r.get("size_label", ""),
                        "row_key": r.get("row_key", ""),
                    }
                )
                if len(quick_items) >= 100:
                    break

    names = sorted({r["item_name"] for r in prepared_rows if r["item_name"]})[:100]
    return {
        "total_rows": len(prepared_rows),
        "matching_item_names": names,
        "matching_items": quick_items,
        "rows": prepared_rows,
    }


@app.get("/search_wipers")
def search_wipers(q_car: str = "", q_model: str = "", q_engine: str = "") -> dict:
    car_tt, car_years = wiper_tokens_and_years(q_car)
    model_tt, model_years = wiper_tokens_and_years(q_model)
    engine_tt, engine_years = wiper_tokens_and_years(q_engine)
    if not (car_tt or car_years or model_tt or model_years or engine_tt or engine_years):
        return {"total_rows": 0, "rows": []}

    conn = get_db()
    try:
        where_parts: list[str] = []
        params: list[str] = []

        def add_like_tokens(field_sql: str, toks: list[str]) -> None:
            for token in toks:
                if any(ch.isdigit() for ch in token):
                    continue
                variants = spelling_query_variants(token)
                if not variants:
                    continue
                if len(variants) == 1:
                    where_parts.append(f"lower(COALESCE({field_sql},'')) LIKE ?")
                    params.append(f"%{variants[0]}%")
                else:
                    where_parts.append(
                        "("
                        + " OR ".join([f"lower(COALESCE({field_sql},'')) LIKE ?" for _ in variants])
                        + ")"
                    )
                    params.extend([f"%{v}%" for v in variants])

        add_like_tokens("car_name", car_tt)
        add_like_tokens("car_model", model_tt)
        add_like_tokens("engine", engine_tt)

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"
        candidate_rows = conn.execute(
            f"""
            SELECT car_name, car_model, engine, wiper_location, wiper_number, source_file, source_sheet
            FROM wiper_rows
            WHERE {where_clause}
            ORDER BY car_name, car_model, source_file
            LIMIT 40000
            """,
            params,
        ).fetchall()

        filtered = [
            r
            for r in candidate_rows
            if wiper_field_matches_tokens_and_years(r["car_name"] or "", car_tt, car_years)
            and wiper_field_matches_tokens_and_years(r["car_model"] or "", model_tt, model_years)
            and wiper_field_matches_tokens_and_years(r["engine"] or "", engine_tt, engine_years)
        ]
        rows_out = filtered[:500]
    finally:
        conn.close()

    prepared: list[dict[str, Any]] = []
    for index, row in enumerate(rows_out):
        d = sql_row_to_dict(row)
        d["row_key"] = f"{d.get('source_file', '')}|{d.get('source_sheet', '')}|{index}"
        d["engine"] = reverse_wiper_engine_display(d.get("engine"))
        prepared.append(d)
    return {"total_rows": len(prepared), "rows": prepared}


@app.get("/search_locations")
def search_locations(
    q: str = "",
    q_barcode: str = "",
    q_barcode_canonical: str = "",
    barcode_exact: int = Query(default=0, ge=0, le=1),
    barcode_debug: int = Query(default=0, ge=0, le=1),
    limit: int = Query(default=LOCATION_SEARCH_LIMIT_DEFAULT, ge=1, le=LOCATION_SEARCH_LIMIT_MAX),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """
    q: بحث نصي (normalized_text إن وُجد، وإلا مطابقة على أعمدة الصنف/الشركة/العلامة/الرقم/الملاحظات).
    q_barcode: الباركود كما أدخله المستخدم (خام) — يُعاد تطبيعه على الخادم؛ إن وُجد يُتجاهل q.
    q_barcode_canonical: اختياري من الواجهة للمطابقة والتتبع؛ لا يُستخدم للاستعلام دون إعادة تطبيع الخادم.
    barcode_exact=1: تطابق تام للمفتاح الكامل بدون حذف أصفار بادئة وبدون fallback واسع.
    barcode_debug=1: إخراج تتبع خفيف في استجابة الـ JSON وفي سجلات الخادم (مع BARCODE_SEARCH_DEBUG أو الطلب).
    """
    t_search0 = time.perf_counter()
    lim = int(limit)
    off = int(offset)
    meta: dict[str, Any] = {"has_more": False, "limit": lim, "offset": off}
    debug_req = bool(int(barcode_debug)) or BARCODE_SEARCH_DEBUG
    exact_bc = bool(int(barcode_exact))

    # البحث دائماً من النسخة المحلية فقط (فوري). لا تنزيل API هنا.
    # جلب صنف واحد: /hesabate/lookup — استيراد الكل: زر الاستيراد.
    qb_raw = (q_barcode or "").strip()

    conn = get_db()
    try:
        try:
            ensure_location_normalized_schema(conn)
            try:
                need_norm = conn.execute(
                    """
                    SELECT 1 AS x FROM location_rows
                    WHERE normalized_text IS NULL
                       OR normalized_barcode IS NULL
                       OR (
                         COALESCE(TRIM(barcode), '') <> ''
                         AND TRIM(COALESCE(normalized_barcode, '')) = ''
                       )
                    LIMIT 1
                    """
                ).fetchone()
            except Exception:
                need_norm = None
            if need_norm:
                try:
                    backfill_location_normalized_columns(conn, max_per_boot=5000, batch_size=1500)
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            if qb_raw:
                b_keys = barcode_search_keys_exact(qb_raw) if exact_bc else barcode_search_keys(qb_raw)
                if not b_keys:
                    _barcode_search_debug_log(
                        debug_req, "no-keys-after-normalize", raw=qb_raw, keys=b_keys, kind="none"
                    )
                    out = {"total_rows": 0, "rows": [], **meta}
                    if debug_req:
                        out["barcode_meta"] = {
                            "keys_tried": [],
                            "match_kind": "none",
                            "raw_input": qb_raw,
                            "canonical_primary": "",
                            "client_canonical_hint": (q_barcode_canonical or "").strip(),
                        }
                    return out

                key_set = set(b_keys)
                bc_primary = b_keys[0]
                match_kind = ""

                rows, has_more = _search_locations_sql_only(
                    conn,
                    mode="barcode",
                    canonical_barcode=bc_primary,
                    barcode_keys=b_keys,
                    norm_tokens=[],
                    limit=lim,
                    offset=off,
                    barcode_exact=exact_bc,
                )
                if rows:
                    match_kind = "exact_index"
                # باركود بمسافات تنسيق (223112U001        HI) قبل إعادة الفهرسة
                if not rows:
                    rows, has_more = _search_locations_barcode_spaced_compact_fallback(
                        conn, key_set, lim, off, barcode_exact=exact_bc
                    )
                    if rows:
                        match_kind = "spaced_compact_barcode"
                # barcode_exact: بدون LIKE واسع وبدون مرادفات أصفار
                if not rows and not exact_bc:
                    rows, has_more = _search_locations_barcode_python_fallback(
                        conn, key_set, lim, off, barcode_exact=False
                    )
                    if rows:
                        match_kind = "legacy_barcode_fallback"
                if not rows and not exact_bc:
                    rows, has_more = _search_locations_barcode_like_fallback(
                        conn, key_set, lim, off, barcode_exact=False
                    )
                    if rows:
                        match_kind = "like_barcode_fallback"
                if not rows:
                    try:
                        backfill_location_normalized_columns(conn, max_per_boot=8000, batch_size=2000)
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    rows, has_more = _search_locations_sql_only(
                        conn,
                        mode="barcode",
                        canonical_barcode=bc_primary,
                        barcode_keys=b_keys,
                        norm_tokens=[],
                        limit=lim,
                        offset=off,
                        barcode_exact=exact_bc,
                    )
                    if rows:
                        match_kind = "exact_index_after_backfill"
                if not rows:
                    rows, has_more = _search_locations_barcode_spaced_compact_fallback(
                        conn, key_set, lim, off, barcode_exact=exact_bc
                    )
                    if rows:
                        match_kind = "spaced_compact_barcode_after_backfill"
                if not rows and not exact_bc:
                    rows, has_more = _search_locations_barcode_python_fallback(
                        conn, key_set, lim, off, barcode_exact=False
                    )
                    if rows:
                        match_kind = "legacy_barcode_fallback_after_backfill"
                if not rows and not exact_bc:
                    rows, has_more = _search_locations_barcode_like_fallback(
                        conn, key_set, lim, off, barcode_exact=False
                    )
                    if rows:
                        match_kind = "like_barcode_fallback_after_backfill"
                # لا نُحوّل بحث الباركود إلى بحث نصي — كان يُرجع أصنافاً كثيرة بالخطأ

                _barcode_search_debug_log(
                    debug_req,
                    "barcode-search-done",
                    raw=qb_raw,
                    keys=b_keys,
                    kind=match_kind or "none",
                )

                prepared = _prepare_location_search_rows(rows)
                search_ms = int((time.perf_counter() - t_search0) * 1000)
                logger.info(
                    "location_search ms=%s rows=%s mode=barcode version=%s",
                    search_ms,
                    len(prepared),
                    _hesabate_search_data_version(),
                )
                out = {
                    "total_rows": len(prepared),
                    "rows": prepared,
                    **meta,
                    "has_more": has_more,
                    "search_ms": search_ms,
                    "search_version": _hesabate_search_data_version(),
                    # الواجهة تستدعي /hesabate/lookup عند الفراغ (بدون حجز اتصال DB أثناء سحب API)
                    "needs_live_refresh": bool((not prepared) and hesabate_configured()),
                }
                if debug_req:
                    out["barcode_meta"] = {
                        "keys_tried": list(b_keys),
                        "match_kind": match_kind or "none",
                        "raw_input": qb_raw,
                        "canonical_primary": bc_primary,
                        "client_canonical_hint": (q_barcode_canonical or "").strip(),
                        "server_canonical": normalize_barcode_lookup_key(qb_raw),
                    }
                return out

            qt = (q or "").strip()
            if not qt:
                return {"total_rows": 0, "rows": [], **meta, "search_version": _hesabate_search_data_version()}

            norm_tokens = location_text_search_norm_tokens(qt)
            if not norm_tokens:
                return {"total_rows": 0, "rows": [], **meta, "search_version": _hesabate_search_data_version()}
            rows, has_more = _search_locations_sql_only(
                conn,
                mode="text",
                canonical_barcode="",
                barcode_keys=None,
                norm_tokens=norm_tokens,
                limit=lim,
                offset=off,
            )
            prepared = _prepare_location_search_rows(rows)
            search_ms = int((time.perf_counter() - t_search0) * 1000)
            logger.info(
                "location_search ms=%s rows=%s mode=text version=%s",
                search_ms,
                len(prepared),
                _hesabate_search_data_version(),
            )
            return {
                "total_rows": len(prepared),
                "rows": prepared,
                **meta,
                "has_more": has_more,
                "search_ms": search_ms,
                "search_version": _hesabate_search_data_version(),
                "needs_live_refresh": bool(
                    (not prepared) and hesabate_configured() and looks_like_item_code(qt)
                ),
            }
        except HTTPException:
            raise
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            detail = f"{type(exc).__name__}: {exc!s}"
            if len(detail) > 800:
                detail = detail[:800] + "…"
            raise HTTPException(status_code=500, detail=detail) from exc
    finally:
        conn.close()


@app.get("/hesabate/status")
def hesabate_status() -> dict:
    cache_n = hesabate_product_cache_count()
    usage = hesabate_usage_summary()
    usage["product_cache_rows"] = str(cache_n)
    usage["product_cache_at"] = app_kv_get("hesabate_product_cache_at", "")
    last_err = app_kv_get("hesabate_last_error", "")
    age_sec = int(_hesabate_last_sync_age_sec())
    return {
        "configured": hesabate_configured(),
        "usage": usage,
        "product_cache_rows": cache_n,
        "search_version": _hesabate_search_data_version(),
        "last_sync_at": app_kv_get("hesabate_last_sync_at", ""),
        "last_error": last_err,
        "sync_min_interval_sec": HESABATE_SYNC_MIN_INTERVAL_SEC,
        "age_sec": age_sec,
        "auto_sync_count": usage.get("auto_sync_count", 0),
        "auto_sync_last_at": usage.get("auto_sync_last_at", ""),
        "cron_last_ping_at": usage.get("cron_last_ping_at", ""),
        "cron_last_ping_reason": usage.get("cron_last_ping_reason", ""),
        "catalog_mode": "full_download_compare",
        "api_note": (
            "Store API: Auth + download (products/company/trades/model). "
            "صنف واحد فعّال فقط عبر pro_id+all=1. حقل barcode لا يفلتر (all=1 يعيد الكتالوج كاملاً). "
            "لا جلب تغييرات/Webhooks — اكتشاف صنف جديد بالباركود يحتاج مزامنة الكتالوج."
        ),
        "docs": "https://documenter.getpostman.com/view/5117721/2sAYdcrXsJ",
    }


@app.post("/hesabate/warm_cache")
def hesabate_warm_product_cache(force: int = Query(default=0, ge=0, le=1)) -> dict:
    """
    توافق خلفي: نفس مسار المزامنة الموحّد (location_rows + product_cache).
    force=1 يتجاوز مهلة الحد الأدنى فقط.
    """
    result = ensure_hesabate_locations_synced(force=bool(force))
    return {
        "ok": bool(result.get("ok")),
        "skipped": bool(result.get("skipped")),
        "warmed": bool(result.get("ok")) and not bool(result.get("skipped")),
        "inflight": result.get("reason") == "inflight",
        "rows": result.get("rows"),
        "stats": result.get("stats"),
        "timing_ms": result.get("timing_ms"),
        "search_version": result.get("search_version", _hesabate_search_data_version()),
        "last_sync_at": result.get("last_sync_at") or app_kv_get("hesabate_last_sync_at", ""),
        "last_error": result.get("last_error") or app_kv_get("hesabate_last_error", ""),
        "message": result.get("message") or "",
        "catalog_mode": result.get("catalog_mode") or "full_download_compare",
        "usage": result.get("usage") or hesabate_usage_summary(),
    }


@app.post("/hesabate/ensure")
def hesabate_ensure() -> dict:
    """مزامنة تلقائية إن لزم (تحترم الحد الأدنى + القفل المشترك بين الأجهزة)."""
    result = ensure_hesabate_locations_synced(force=False)
    reason = str(result.get("reason") or "")
    # سجّل كـ«تحديث تلقائي» عند تشغيل فعلي (ليس fresh/inflight)
    if result.get("ok") and reason not in ("inflight", "not_configured", "fresh"):
        result["auto_sync_count"] = bump_hesabate_auto_sync_count()
        result["auto_sync_last_at"] = app_kv_get("hesabate_auto_sync_last_at", "")
    else:
        result.update(hesabate_auto_sync_stats())
    if "usage" not in result or not result.get("usage"):
        result["usage"] = hesabate_usage_summary()
    return result


@app.post("/hesabate/sync")
def hesabate_sync_force() -> dict:
    """مزامنة فورية (تستخدم عند الحاجة فقط — تستهلك Requests)."""
    return ensure_hesabate_locations_synced(force=True)


@app.api_route("/hesabate/cron_sync", methods=["GET", "POST"])
def hesabate_cron_sync(request: Request) -> dict:
    """
    مزامنة مجدولة كل 4 ساعات (GitHub Actions) — تعمل والتطبيق مغلق.
    صمت تلقائي 21:00–08:00 بتوقيت فلسطين. الزر اليدوي لا يتأثر.
    احمِها بـ CRON_SECRET أو HESABATE_CRON_SECRET إن وُجد.
    """
    secret = (os.getenv("CRON_SECRET") or os.getenv("HESABATE_CRON_SECRET") or "").strip()
    if secret:
        auth = (request.headers.get("authorization") or "").strip()
        cron_header = (request.headers.get("x-vercel-cron") or "").strip()
        ok_bearer = auth == f"Bearer {secret}"
        ok_vercel = bool(cron_header)
        if not ok_bearer and not ok_vercel:
            raise HTTPException(status_code=401, detail="Unauthorized cron")
    allowed, schedule_meta = hesabate_auto_sync_allowed_now()
    interval_min = max(1, int(HESABATE_SYNC_MIN_INTERVAL_SEC // 60))
    force_q = str(request.query_params.get("force") or "").strip().lower()
    force_catchup = force_q in ("1", "true", "yes")
    if not allowed:
        touch_hesabate_cron_ping(reason="quiet_hours_palestine")
        return {
            "ok": True,
            "skipped": True,
            "reason": "quiet_hours_palestine",
            "scheduled": True,
            "interval_hint_min": interval_min,
            "message": (
                "توقفت المزامنة التلقائية ليلاً (9 مساءً – 8 صباحاً بتوقيت فلسطين). "
                f"الوقت المحلي: {schedule_meta.get('local_time')}"
            ),
            "schedule": schedule_meta,
            "search_version": _hesabate_search_data_version(),
            **hesabate_auto_sync_stats(),
            "usage": hesabate_usage_summary(),
        }
    # المجدول قد يطرق كل ~30د — نفرض الفاصل الأدنى بين مزامنة فعلية
    age_sec = _hesabate_last_sync_age_sec()
    if (not force_catchup) and age_sec < HESABATE_SYNC_MIN_INTERVAL_SEC:
        touch_hesabate_cron_ping(reason="fresh")
        return {
            "ok": True,
            "skipped": True,
            "reason": "fresh",
            "scheduled": True,
            "interval_hint_min": interval_min,
            "age_sec": int(age_sec),
            "message": (
                f"التحديث التلقائي كل {interval_min} دقيقة — "
                f"آخر مزامنة قبل {int(age_sec)} ث (لم يحن الموعد)."
            ),
            "schedule": schedule_meta,
            "search_version": _hesabate_search_data_version(),
            "last_sync_at": app_kv_get("hesabate_last_sync_at", ""),
            **hesabate_auto_sync_stats(),
            "usage": hesabate_usage_summary(),
        }
    # force=True: تنزيل + إعادة كتابة حساباتي_API كمصدر رئيسي للعرض
    result = ensure_hesabate_locations_synced(force=True)
    result["scheduled"] = True
    result["interval_hint_min"] = interval_min
    result["schedule"] = schedule_meta
    reason = str(result.get("reason") or "")
    if result.get("ok") and reason not in ("inflight", "not_configured", "fresh"):
        result["auto_sync_count"] = bump_hesabate_auto_sync_count()
        result["auto_sync_last_at"] = app_kv_get("hesabate_auto_sync_last_at", "")
        touch_hesabate_cron_ping(reason=reason or "synced")
    else:
        touch_hesabate_cron_ping(reason=reason or "skipped")
        result.update(hesabate_auto_sync_stats())
    result["usage"] = hesabate_usage_summary()
    return result


@app.post("/hesabate/lookup")
def hesabate_lookup_item(payload: HesabateLookupIn) -> dict:
    """
    جلب/تحديث موجّه من حساباتي.
    refresh=True يتجاوز كاش DB/الذاكرة ويجلب عبر pro_id+all=1.
    discover_new=True (زر جلب الصنف): إن غاب pro_id يستورد الكتالوج ثم يعيد مطابقة الصنف.
    """
    qb = str(payload.barcode or "").strip()
    qt = str(payload.q or "").strip()
    if not qb and not qt:
        return {
            "ok": False,
            "live": True,
            "total_rows": 0,
            "rows": [],
            "message": "أدخل باركود أو رقم أصلي.",
            "usage": hesabate_usage_summary(),
        }
    if not hesabate_configured():
        return {
            "ok": False,
            "live": True,
            "total_rows": 0,
            "rows": [],
            "message": "حساباتي غير مُعدّ على السيرفر.",
            "usage": hesabate_usage_summary(),
        }
    try:
        exact_bc = bool(payload.barcode_exact)
        if exact_bc:
            qt = ""
            b_keys = barcode_search_keys_exact(qb) if qb else None
        else:
            b_keys = barcode_search_keys(qb) if qb else None
        live = live_lookup_locations_from_hesabate(
            q_barcode=qb,
            q_text=qt,
            barcode_keys=b_keys,
            cache_only=bool(payload.cache_only),
            refresh=bool(payload.refresh),
            discover_new=False if exact_bc else bool(payload.discover_new),
            barcode_exact=exact_bc,
        )
    except Exception as exc:
        return {
            "ok": False,
            "live": True,
            "total_rows": 0,
            "rows": [],
            "message": f"فشل الجلب: {type(exc).__name__}: {exc}",
            "usage": hesabate_usage_summary(),
        }
    return {
        **live,
        "refreshed_on_miss": True,
        "has_more": False,
        "limit": int(live.get("total_rows") or 0),
        "offset": 0,
    }


def _format_upload_timestamp(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    s = str(value).strip().replace("T", " ")
    if "+" in s:
        s = s.split("+", 1)[0].strip()
    if "." in s and " " in s:
        s = s.split(".", 1)[0].strip()
    return s[:19]


@app.get("/stats")
def stats() -> dict:
    conn = get_db()
    try:
        total_rows = conn.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"]
        total_wiper_rows = conn.execute("SELECT COUNT(*) AS c FROM wiper_rows").fetchone()["c"]
        total_location_rows = conn.execute("SELECT COUNT(*) AS c FROM location_rows").fetchone()["c"]
        total_files = conn.execute("SELECT COUNT(*) AS c FROM uploaded_files").fetchone()["c"]
        total_items = conn.execute(
            "SELECT COUNT(DISTINCT item_name) AS c FROM products WHERE COALESCE(TRIM(item_name), '') <> ''"
        ).fetchone()["c"]
        # رئيسي = ملفات المنتجات (كل ما ليس قشطان/مواقع)
        products_ts = conn.execute(
            """
            SELECT MAX(uploaded_at) AS ts
            FROM uploaded_files
            WHERE COALESCE(parser_version, '') NOT IN (?, ?)
            """,
            (WIPER_PARSER_VERSION, LOCATION_PARSER_VERSION),
        ).fetchone()
        wiper_ts = conn.execute(
            """
            SELECT MAX(uploaded_at) AS ts
            FROM uploaded_files
            WHERE COALESCE(parser_version, '') = ?
            """,
            (WIPER_PARSER_VERSION,),
        ).fetchone()
        last_products_upload_at = _format_upload_timestamp(
            sql_row_to_dict(products_ts).get("ts") if products_ts is not None else None
        )
        last_wiper_upload_at = _format_upload_timestamp(
            sql_row_to_dict(wiper_ts).get("ts") if wiper_ts is not None else None
        )
    finally:
        conn.close()
    return {
        "total_rows": total_rows,
        "total_wiper_rows": total_wiper_rows,
        "total_location_rows": total_location_rows,
        "total_files": total_files,
        "total_items": total_items,
        "last_products_upload_at": last_products_upload_at,
        "last_wiper_upload_at": last_wiper_upload_at,
        "db_backend": DB_BACKEND,
    }


@app.get("/sync/meta")
def sync_meta() -> dict:
    return get_sync_meta()


@app.get("/sync/data")
def sync_data() -> dict:
    meta = get_sync_meta()
    return {
        "version": meta["version"],
        "updated_at": meta["updated_at"],
        "total_rows": meta["total_rows"],
        "rows": get_sync_rows(),
    }


@app.get("/manifest.webmanifest")
def manifest() -> HTMLResponse:
    _iq = app_icon_cache_query()
    _icon_url = f"/app-icon.png?v={_iq}"
    payload = {
        "name": "تبديل الأصناف",
        "short_name": "تبديل الأصناف",
        "start_url": "/",
        "display": "standalone",
        "orientation": "portrait",
        "background_color": "#ffffff",
        "theme_color": "#1a2744",
        "lang": "ar",
        "icons": [
            {
                "src": _icon_url,
                "sizes": "96x96",
                "type": "image/png",
                "purpose": "any",
            },
            {
                "src": _icon_url,
                "sizes": "96x96",
                "type": "image/png",
                "purpose": "maskable",
            },
        ],
    }
    return HTMLResponse(content=json.dumps(payload, ensure_ascii=False), media_type="application/manifest+json")


@app.get("/sw.js")
def service_worker() -> HTMLResponse:
    _iq = app_icon_cache_query()
    _atq = apple_touch_icon_cache_query()
    js = f"""
const CACHE_NAME = 'badail-shell-{APP_VERSION}';
const APP_SHELL = ['/', '/manifest.webmanifest', '/app-icon.png?v={_iq}', '/apple-touch-icon.png?v={_atq}'];

self.addEventListener('install', (event) => {{
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)));
  self.skipWaiting();
}});

self.addEventListener('activate', (event) => {{
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => k.startsWith('badail-shell-') && k !== CACHE_NAME).map((k) => caches.delete(k))))
  );
  self.clients.claim();
}});

self.addEventListener('fetch', (event) => {{
  const req = event.request;
  if (req.method !== 'GET') return;

  let path = '';
  try {{
    path = new URL(req.url).pathname || '';
  }} catch (_) {{
    path = '';
  }}
  // دائماً من الشبكة: فحص الإصدار، المزامنة، البحث، الرفع — لا كاش قديم يعطل التحديث على الهاتف
  const networkOnly =
    path === '/health' ||
    path.startsWith('/sync/') ||
    path.startsWith('/search') ||
    path.startsWith('/upload') ||
    path.startsWith('/hesabate') ||
    path === '/stats';
  if (networkOnly) {{
    event.respondWith(fetch(req));
    return;
  }}

  if (req.mode === 'navigate') {{
    event.respondWith((async () => {{
      const cached = await caches.match('/');
      const net = fetch(req).then((res) => {{
        if (res && res.ok) {{
          const copy = res.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put('/', copy));
        }}
        return res;
      }});
      // فضّل الشبكة؛ عند فشل/500 استخدم الكاش الصالح فقط
      return net.catch(async () => cached || Response.error());
    }})());
    return;
  }}

  event.respondWith(
    fetch(req).then((res) => {{
      if (res && res.ok) {{
        const copy = res.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(req, copy));
      }}
      return res;
    }}).catch(() => caches.match(req).then((cached) => cached || caches.match('/')))
  );
}});
"""
    return HTMLResponse(
        content=js,
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/health")
def health() -> dict:
    db_ok = False
    db_error = ""
    try:
        conn = get_db()
        try:
            conn.execute("SELECT 1")
            db_ok = True
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        db_error = f"{type(exc).__name__}: {exc}"
    return {
        "status": "ok" if db_ok else "degraded",
        "app_version": APP_VERSION,
        "db_backend": DB_BACKEND,
        "db_ok": db_ok,
        "db_error": db_error[:500],
    }


@app.get("/print_layout_prefs")
def get_print_layout_prefs() -> dict:
    """قياسات GUI المحفوظة من المتصفح (لمزامنة الافتراضي مع تعديلات المستخدم)."""
    raw = app_kv_get("print_layout_prefs_json", "")
    if not raw:
        return {"ok": True, "layout": None, "updated_at": ""}
    try:
        layout = json.loads(raw)
    except Exception:
        return {"ok": False, "layout": None, "updated_at": app_kv_get("print_layout_prefs_at", "")}
    return {
        "ok": True,
        "layout": layout,
        "updated_at": app_kv_get("print_layout_prefs_at", ""),
    }


@app.post("/print_layout_prefs")
async def set_print_layout_prefs(payload: dict) -> dict:
    """حفظ قياسات اليسار/اليمين كما عدّلها المستخدم في GUI."""
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    left = payload.get("left")
    right = payload.get("right")
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise HTTPException(status_code=400, detail="left/right required")
    body = {
        "version": int(payload.get("version") or 3),
        "left": left,
        "right": right,
    }
    app_kv_set("print_layout_prefs_json", json.dumps(body, ensure_ascii=False))
    app_kv_set("print_layout_prefs_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return {"ok": True, "updated_at": app_kv_get("print_layout_prefs_at", "")}
