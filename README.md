# Advanced Excel Search (Arabic-friendly)

برنامج بحث متقدم لملفات Excel:
- رفع ملفات كثيرة دفعة واحدة.
- استخراج البيانات من الأعمدة الأساسية:
  - اسم الصنف
  - رقم الصنف
  - البدائل
- البحث السريع المتقدم (مع دعم مطابقة سنة ضمن مدى مثل `09-15`).
- نفس البيانات متاحة من الكمبيوتر أو iPhone طالما الاثنين على نفس الشبكة.

## 1) تثبيت

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 2) تشغيل السيرفر

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

## 3) فتح التطبيق

- على الكمبيوتر:
  - `http://127.0.0.1:8000`
- على iPhone (نفس شبكة الواي فاي):
  - `http://YOUR_COMPUTER_IP:8000`
  - مثال: `http://192.168.1.20:8000`

## 4) طريقة العمل

1. من صفحة الويب اضغط رفع الملفات.
2. اختر عدة ملفات Excel.
3. بعد الرفع، اكتب أي كلمة بحث مثل `liana` أو `رينج liana`.
4. سترى:
   - أسماء الأصناف المطابقة
   - كل الصفوف المطابقة مع اسم الملف والشيت.

## ملاحظات

- عند رفع نفس الملف مرة ثانية، يتم تحديث بياناته.
- التطبيق يخزن البيانات في ملف قاعدة محلية: `search_data.db`.
- إذا أردت الوصول من خارج الشبكة المنزلية، تحتاج استضافة أو إعداد Port Forwarding/Cloud.

## تشغيل 24/7 على Render (الأفضل)

تم تجهيز ملف `render.yaml` تلقائياً.

### الخطوات:

1. ارفع المشروع إلى GitHub.
2. ادخل إلى [Render](https://render.com) وسجل دخول.
3. اختر **New +** ثم **Blueprint**.
4. اختر نفس الـ repository.
5. Render سيقرأ `render.yaml` وينشئ الخدمة تلقائياً مع:
   - Web Service (Python)
   - Persistent Disk لحفظ قاعدة البيانات
   - متغير `APP_DB_PATH=/var/data/search_data.db`
6. بعد اكتمال النشر، سيعطيك رابط عام مثل:
   - `https://badail-search.onrender.com`

### ملاحظات مهمة:

- طالما الخدمة شغالة على Render، التطبيق يبقى 24/7 حتى لو كمبيوترك مطفي.
- أي رفع ملفات من واجهة الموقع ينحفظ على القرص الدائم في Render.
- إذا غيّرت الكود لاحقاً، فقط اعمل Push وسيتم إعادة النشر تلقائياً.

## تشغيل 24/7 على Railway

تم تجهيز ملفات Railway:
- `Procfile`
- `railway.toml`

### الخطوات:

1. ادخل إلى [Railway](https://railway.app) وسجل دخول بحساب GitHub.
2. اضغط **New Project** ثم **Deploy from GitHub repo**.
3. اختر الريبو: `sayyedautoparts/badail-search`.
4. بعد إنشاء المشروع:
   - افتح **Variables** وأضف:
     - `APP_DB_PATH=/data/search_data.db`
5. افتح **Settings** > **Volumes**:
   - أنشئ Volume جديد
   - Mount Path: `/data`
6. اعمل **Redeploy** للخدمة.

### النتيجة:

- التطبيق يطلع على رابط عام من Railway.
- يبقى شغال 24/7 (حسب الخطة).
- البيانات تبقى محفوظة لأن قاعدة SQLite موجودة داخل Volume.

### مهم:

- إذا ما أضفت Volume، ممكن تضيع البيانات بعد إعادة النشر.
- بعد أي تعديل جديد في الكود: اعمل Push على GitHub وRailway يعيد النشر تلقائياً.

## تشغيل على Vercel

تم تجهيز ملفات Vercel:
- `vercel.json`
- `api/index.py`

### الخطوات:

1. ادخل إلى [Vercel](https://vercel.com) وسجل دخول بحساب GitHub.
2. اختر **Add New...** ثم **Project**.
3. اختر الريبو: `sayyedautoparts/badail-search`.
4. اضغط **Deploy**.

### ملاحظة مهمة جداً:

- على Vercel، SQLite المحلي ليس تخزين دائم (يكون مؤقت).
- يعني البيانات المرفوعة قد تختفي بعد إعادة تشغيل/نشر.
- إذا بدك حفظ دائم 100%، استخدم Railway/Render مع Volume أو قاعدة بيانات خارجية.
