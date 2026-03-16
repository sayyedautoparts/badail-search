import io
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from openpyxl import load_workbook


DB_PATH = Path(os.getenv("APP_DB_PATH", str(Path(__file__).with_name("search_data.db"))))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
CURRENT_YEAR = datetime.now().year


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS uploaded_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT UNIQUE NOT NULL,
                uploaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                rows_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_name TEXT,
                item_number TEXT,
                original_numbers TEXT,
                alternatives TEXT,
                source_file TEXT NOT NULL,
                source_sheet TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_item_name ON products(item_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_products_alternatives ON products(alternatives)")
        conn.commit()
    finally:
        conn.close()


def normalize_text(text: str) -> str:
    text = str(text).strip().lower()
    text = re.sub(r"[\s\-_]+", " ", text)
    return text


def is_brakes_file(file_name: str) -> bool:
    normalized_name = normalize_text(file_name)
    brake_markers = ["بريك", "بريكات", "brake", "brakes"]
    return any(marker in normalized_name for marker in brake_markers)


def parse_year_token(token: str) -> int | None:
    cleaned = re.sub(r"[^\d]", "", token)
    if len(cleaned) not in (2, 4):
        return None
    year = int(cleaned)
    if len(cleaned) == 2:
        return 2000 + year if year <= 30 else 1900 + year
    return year


def parse_query_year_token(token: str) -> int | None:
    raw = str(token).strip().lower()
    # Query-side years should be explicit to avoid misreading engine values like "1.6" as years.
    if not re.fullmatch(r"\+?\d{2,4}", raw):
        return None
    return parse_year_token(raw)


def year_in_range_text(year: int, text: str) -> bool:
    if not text:
        return False

    normalized = normalize_text(text)

    # Exact standalone year tokens (e.g. 2011, 11).
    for match in re.finditer(r"(?<!\d)(\d{4})(?!\d)", normalized):
        if int(match.group(1)) == year:
            return True
    for match in re.finditer(r"(?<!\d)(\d{2})(?!\d)", normalized):
        token_year = parse_year_token(match.group(1))
        if token_year == year:
            return True

    # Supports formats like: "lancer +04" => 2004 .. current year.
    for match in re.finditer(r"\+\s*(\d{2,4})(?!\d)", normalized):
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

    # Supports open-ended formats like: "04-" => 2004 .. current year.
    for match in re.finditer(r"(?<!\d)(\d{2,4})\s*[-–—](?!\s*\d)", normalized):
        start = parse_year_token(match.group(1))
        if start is not None and start <= year <= CURRENT_YEAR:
            return True
    return False


def split_alternative_segments(alternatives: str) -> list[str]:
    if not alternatives:
        return []
    parts = re.split(r"[\/|,\n;]+", alternatives)
    return [p.strip() for p in parts if p and p.strip()]


def split_year_chunks(text: str) -> list[str]:
    if not text:
        return []
    chunks = [
        c.strip()
        for c in re.findall(r"[^/|,\n;]*?(?:\+\s*\d{2,4}|\d{2,4}\s*[-–—]\s*\d{2,4}|\d{2,4}\s*[-–—])", text)
        if c and c.strip()
    ]
    return chunks or [text.strip()]


def extract_matched_alternative(alternatives: str, text_tokens: list[str], years: list[int]) -> str:
    segments = split_alternative_segments(alternatives)
    if not segments:
        return ""

    normalized_tokens = [normalize_text(t) for t in text_tokens if normalize_text(t)]
    for segment in segments:
        for chunk in split_year_chunks(segment):
            nseg = normalize_text(chunk)
            has_text = (not normalized_tokens) or all(tok in nseg for tok in normalized_tokens)
            has_year = (not years) or any(year_in_range_text(y, chunk) for y in years)
            if has_text and has_year:
                return chunk

    # Fallback: return the first segment that contains at least one text token.
    if normalized_tokens:
        for segment in segments:
            for chunk in split_year_chunks(segment):
                nseg = normalize_text(chunk)
                if any(tok in nseg for tok in normalized_tokens):
                    return chunk
    return segments[0]


def row_matches_query(row: sqlite3.Row, text_tokens: list[str], years: list[int]) -> bool:
    item_name = row["item_name"] or ""
    alternatives = row["alternatives"] or ""
    combined = normalize_text(f"{item_name} {alternatives}")

    for token in text_tokens:
        if normalize_text(token) not in combined:
            return False

    if years:
        segments = split_alternative_segments(alternatives)
        if segments:
            alt_tokens = [normalize_text(t) for t in text_tokens if normalize_text(t) in normalize_text(alternatives)]
            segment_ok = False
            for segment in segments:
                for chunk in split_year_chunks(segment):
                    nseg = normalize_text(chunk)
                    has_alt_tokens = (not alt_tokens) or all(t in nseg for t in alt_tokens)
                    has_year = any(year_in_range_text(y, chunk) for y in years)
                    if has_alt_tokens and has_year:
                        segment_ok = True
                        break
                if segment_ok:
                    break
            if not segment_ok:
                return False
        else:
            # Fallback if alternatives empty: check item name only.
            if not any(year_in_range_text(y, item_name) for y in years):
                return False

    return True


def process_excel_file(file_bytes: bytes, file_name: str) -> int:
    conn = get_db()
    inserted_rows = 0

    try:
        conn.execute("DELETE FROM products WHERE source_file = ?", (file_name,))
        wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
        use_h_for_alternatives = is_brakes_file(file_name)
        try:
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                rows_to_insert = []

                for row in ws.iter_rows(min_col=1, max_col=8, values_only=True):
                    # Fixed mapping by Excel column letters:
                    # A => item_name, E => item_number, alternatives => G (default) or H for brakes files
                    item_name = str(row[0]).strip() if row[0] is not None else ""
                    item_number = str(row[4]).strip() if row[4] is not None else ""
                    original_numbers = ""
                    alt_index = 7 if use_h_for_alternatives else 6
                    alternatives = str(row[alt_index]).strip() if row[alt_index] is not None else ""

                    if not any([item_name, item_number, original_numbers, alternatives]):
                        continue

                    # Skip typical header row.
                    joined = normalize_text(" ".join([item_name, item_number, original_numbers, alternatives]))
                    if "اسم الصنف" in joined and "رقم الصنف" in joined:
                        continue

                    rows_to_insert.append(
                        (
                            item_name,
                            item_number,
                            original_numbers,
                            alternatives,
                            file_name,
                            str(sheet_name),
                        )
                    )

                if rows_to_insert:
                    conn.executemany(
                        """
                        INSERT INTO products (
                            item_name, item_number, original_numbers, alternatives, source_file, source_sheet
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        rows_to_insert,
                    )
                    inserted_rows += len(rows_to_insert)
        finally:
            wb.close()

        conn.execute(
            """
            INSERT INTO uploaded_files (file_name, rows_count)
            VALUES (?, ?)
            ON CONFLICT(file_name) DO UPDATE SET
                uploaded_at = CURRENT_TIMESTAMP,
                rows_count = excluded.rows_count
            """,
            (file_name, inserted_rows),
        )
        conn.commit()
        return inserted_rows
    finally:
        conn.close()


app = FastAPI(title="Advanced Excel Search")
init_db()


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return """
<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>بحث البدائل</title>
  <style>
    body { font-family: Arial, sans-serif; background: #f6f7fb; margin: 0; }
    .wrap { max-width: 760px; margin: 18px auto; padding: 0 12px; }
    .card { background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 2px 10px rgba(0,0,0,.06); margin-bottom: 16px; }
    input, button { width: 100%; padding: 10px; margin: 6px 0; border: 1px solid #ddd; border-radius: 8px; box-sizing: border-box; }
    button { background: #0b66ff; color: #fff; border: none; cursor: pointer; }
    button:hover { opacity: 0.95; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; table-layout: fixed; }
    th, td { border: 1px solid #eee; padding: 6px; text-align: right; vertical-align: top; word-wrap: break-word; }
    th { background: #fafafa; }
    .muted { color: #666; font-size: 13px; }
    .pill { display: inline-block; background: #eef4ff; color: #2d5fff; padding: 7px 10px; border-radius: 10px; margin: 4px 4px 0 0; font-size: 12px; text-align: right; }
    .pill-btn { border: none; cursor: pointer; min-width: 180px; }
    .quick-name { display: block; font-weight: 700; }
    .quick-alt { display: block; margin-top: 3px; color: #4e5f8f; }
    .row-focus { background: #fff7d6; transition: background-color 0.25s ease; }
    .upload-label { display: block; margin-top: 8px; font-size: 12px; color: #334; font-weight: 700; }
    .upload-actions { display: flex; gap: 8px; margin-top: 8px; }
    .upload-actions button { flex: 1; }
    .btn-secondary { background: #edf0f8; color: #223; }
    .drop-zone { margin-top: 8px; border: 2px dashed #b8c4e6; border-radius: 10px; padding: 12px; text-align: center; color: #445; background: #f8faff; }
    .drop-zone.active { border-color: #2d5fff; background: #eef3ff; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h2>رفع ملفات Excel</h2>
      <p class="muted">اختَر ملفات Excel و/أو فولدرات تحتوي Excel بنفس الوقت، ثم ارفع دفعة واحدة.</p>
      <label class="upload-label" for="filesInput">اختيار ملفات Excel (ملفات فقط)</label>
      <input id="filesInput" type="file" multiple accept=".xls,.xlsx,.xlsm" />
      <label class="upload-label" for="folderInput">اختيار فولدرات فيها ملفات Excel</label>
      <input id="folderInput" type="file" webkitdirectory directory multiple />
      <div id="dropZone" class="drop-zone">اسحب كل الفولدرات/الملفات هون دفعة واحدة ثم ارفع</div>
      <div class="upload-actions">
        <button type="button" class="btn-secondary" onclick="clearSelectedFiles()">تفريغ الاختيار</button>
        <button type="button" onclick="uploadFiles()">رفع الملفات</button>
      </div>
      <div id="selectionInfo" class="muted">الملفات المحددة: 0</div>
      <div id="uploadResult" class="muted"></div>
    </div>

    <div class="card">
      <h2>البحث</h2>
      <p class="muted">مثال: اكتب <b>رينج liana</b> أو <b>liana</b></p>
      <input id="queryInput" placeholder="اكتب كلمة البحث..." />
      <button onclick="searchNow()">بحث</button>
      <div id="stats" class="muted"></div>
      <div id="names"></div>
      <div style="overflow:auto;">
        <table id="resultsTable" style="display:none;">
          <thead>
            <tr>
              <th>الملف</th>
              <th>اسم الصنف</th>
              <th>رقم الصنف</th>
              <th>التعريف المطابق</th>
              <th>البدائل</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    let autoRefreshTimer = null;
    let searchDebounceTimer = null;
    const selectedFilesMap = new Map();

    function excelOnly(files) {
      return Array.from(files).filter(f => /\\.(xlsx|xlsm|xls)$/i.test(f.name));
    }

    function fileKey(file) {
      return `${file.webkitRelativePath || file.name}::${file.size}::${file.lastModified}`;
    }

    function updateSelectionInfo() {
      const count = selectedFilesMap.size;
      document.getElementById('selectionInfo').innerText = `الملفات المحددة: ${count}`;
    }

    function appendFilesToSelection(files) {
      excelOnly(files).forEach(file => {
        selectedFilesMap.set(fileKey(file), file);
      });
      updateSelectionInfo();
    }

    function clearSelectedFiles() {
      selectedFilesMap.clear();
      document.getElementById('filesInput').value = '';
      document.getElementById('folderInput').value = '';
      updateSelectionInfo();
      document.getElementById('uploadResult').innerText = 'تم تفريغ الاختيارات.';
    }

    function readFileFromEntry(entry) {
      return new Promise((resolve) => {
        entry.file((file) => resolve([file]), () => resolve([]));
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

    async function handleDrop(e) {
      e.preventDefault();
      const dropZone = document.getElementById('dropZone');
      dropZone.classList.remove('active');

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

    async function uploadFiles() {
      const selectedFiles = Array.from(selectedFilesMap.values());
      if (!selectedFiles.length) {
        document.getElementById('uploadResult').innerText = 'اختار ملفات Excel أولاً';
        return;
      }

      const formData = new FormData();
      selectedFiles.forEach(file => formData.append('files', file, file.name));
      document.getElementById('uploadResult').innerText = 'جاري الرفع والمعالجة...';

      const res = await fetch('/upload', { method: 'POST', body: formData });
      const data = await res.json();
      if (!res.ok) {
        document.getElementById('uploadResult').innerText = data.detail || 'حصل خطأ';
        return;
      }
      document.getElementById('uploadResult').innerText =
        `تم رفع ${data.files_count} ملفات، وإدخال ${data.inserted_rows} صف.`;
      await loadStats();
      await searchNow();
    }

    async function searchNow() {
      const q = document.getElementById('queryInput').value.trim();
      localStorage.setItem('lastSearchQuery', q);
      const res = await fetch(`/search?q=${encodeURIComponent(q)}`);
      const data = await res.json();

      document.getElementById('stats').innerText = `عدد النتائج: ${data.total_rows}`;

      const namesDiv = document.getElementById('names');
      namesDiv.innerHTML = '';
      const quickItems = data.matching_items || (data.matching_item_names || []).map(name => ({
        item_name: name,
        matched_alternative: '',
        row_key: name
      }));

      quickItems.forEach(entry => {
        const btn = document.createElement('button');
        btn.className = 'pill pill-btn';
        btn.type = 'button';
        const quickName = document.createElement('span');
        quickName.className = 'quick-name';
        quickName.textContent = entry.item_name || '';
        btn.appendChild(quickName);
        if (entry.matched_alternative) {
          const quickAlt = document.createElement('span');
          quickAlt.className = 'quick-alt';
          quickAlt.textContent = entry.matched_alternative;
          btn.appendChild(quickAlt);
        }
        btn.onclick = () => focusRowByKey(entry.row_key);
        namesDiv.appendChild(btn);
      });

      const table = document.getElementById('resultsTable');
      const body = table.querySelector('tbody');
      body.innerHTML = '';
      data.rows.forEach(r => {
        const tr = document.createElement('tr');
        tr.dataset.rowKey = String(r.row_key || '');
        tr.innerHTML = `<td>${escapeHtml(r.source_file)}</td>
                        <td>${escapeHtml(r.item_name)}</td>
                        <td>${escapeHtml(r.item_number)}</td>
                        <td>${escapeHtml(r.matched_alternative || '')}</td>
                        <td>${escapeHtml(r.alternatives)}</td>`;
        body.appendChild(tr);
      });
      table.style.display = data.rows.length ? 'table' : 'none';
    }

    function initAutoSearch() {
      const queryInput = document.getElementById('queryInput');
      const savedQuery = localStorage.getItem('lastSearchQuery') || '';
      queryInput.value = savedQuery;

      queryInput.addEventListener('input', () => {
        clearTimeout(searchDebounceTimer);
        searchDebounceTimer = setTimeout(() => {
          searchNow();
        }, 250);
      });

      clearInterval(autoRefreshTimer);
      autoRefreshTimer = setInterval(() => {
        searchNow();
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

    async function loadStats() {
      const res = await fetch('/stats');
      const data = await res.json();
      document.getElementById('stats').innerText =
        `إجمالي الصفوف: ${data.total_rows} | عدد الملفات: ${data.total_files}`;
    }
    document.getElementById('filesInput').addEventListener('change', (e) => {
      appendFilesToSelection(e.target.files);
      e.target.value = '';
    });
    document.getElementById('folderInput').addEventListener('change', (e) => {
      appendFilesToSelection(e.target.files);
      e.target.value = '';
    });
    const dropZone = document.getElementById('dropZone');
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
    updateSelectionInfo();
    initAutoSearch();
    loadStats();
    searchNow();
  </script>
</body>
</html>
"""


@app.post("/upload")
async def upload(files: list[UploadFile] = File(...)) -> dict:
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    inserted_total = 0
    processed_files = 0

    for file in files:
        lower = file.filename.lower() if file.filename else ""
        if not lower.endswith((".xlsx", ".xls", ".xlsm")):
            continue

        content = await file.read()
        try:
            inserted_total += process_excel_file(content, file.filename)
            processed_files += 1
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Could not process file '{file.filename}': {exc}",
            ) from exc

    return {"files_count": processed_files, "inserted_rows": inserted_total}


@app.get("/search")
def search(q: str = "") -> dict:
    tokens = [t for t in normalize_text(q).split(" ") if t]
    if not tokens:
        return {"total_rows": 0, "matching_item_names": [], "matching_items": [], "rows": []}

    query_years = [y for y in (parse_query_year_token(t) for t in tokens) if y is not None]
    text_tokens = [t for t in tokens if parse_query_year_token(t) is None]

    conn = get_db()
    try:
        if text_tokens:
            where_parts = []
            params: list[str] = []
            for token in text_tokens:
                like = f"%{token}%"
                where_parts.append(
                    """
                    (
                        lower(item_name) LIKE ?
                        OR lower(alternatives) LIKE ?
                    )
                    """
                )
                params.extend([like, like])
            where_clause = " OR ".join(where_parts)
            candidate_rows = conn.execute(
                f"""
                SELECT item_name, item_number, alternatives, source_file, source_sheet
                FROM products
                WHERE {where_clause}
                ORDER BY item_name
                LIMIT 4000
                """,
                params,
            ).fetchall()
        else:
            candidate_rows = conn.execute(
                """
                SELECT item_name, item_number, alternatives, source_file, source_sheet
                FROM products
                ORDER BY id DESC
                LIMIT 4000
                """
            ).fetchall()

        filtered_rows = [r for r in candidate_rows if row_matches_query(r, text_tokens, query_years)]
        rows = filtered_rows[:300]
    finally:
        conn.close()

    prepared_rows = []
    quick_items = []
    seen_quick = set()
    for index, row in enumerate(rows):
        row_dict = dict(row)
        matched_alt = extract_matched_alternative(row_dict.get("alternatives", ""), text_tokens, query_years)
        row_key = f"{row_dict.get('source_file', '')}|{row_dict.get('source_sheet', '')}|{row_dict.get('item_number', '')}|{index}"
        row_dict["matched_alternative"] = matched_alt
        row_dict["row_key"] = row_key
        prepared_rows.append(row_dict)

        quick_key = (row_dict.get("item_name", ""), matched_alt)
        if quick_key not in seen_quick and row_dict.get("item_name", ""):
            seen_quick.add(quick_key)
            quick_items.append(
                {
                    "item_name": row_dict["item_name"],
                    "matched_alternative": matched_alt,
                    "row_key": row_key,
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


@app.get("/stats")
def stats() -> dict:
    conn = get_db()
    try:
        total_rows = conn.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"]
        total_files = conn.execute("SELECT COUNT(*) AS c FROM uploaded_files").fetchone()["c"]
    finally:
        conn.close()
    return {"total_rows": total_rows, "total_files": total_files}


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
