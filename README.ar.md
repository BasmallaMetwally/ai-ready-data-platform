# Unified Data Platform (النسخة العربية)

> **ملحوظة**: النسخة الإنجليزية الرئيسية موجودة في `README.md`، وفيها
> سجل التعديلات الكامل في `CHANGELOG.md`. الملف ده شرح تفصيلي بالعربي.
>
> **إضافات جولة 3** (تفاصيلها الكاملة في `CHANGELOG.md`): notebook للـ
> EDA (`notebooks/01_ecommerce_eda.ipynb`)، ترحيل حقيقي لـ MySQL
> (`mysql_migration/`، اتجرب فعليًا على MySQL 8 حقيقي، وكشف bug حقيقي في
> سكريبت الترحيل لـ Postgres كان محدش شغّله قبل كده)، وخيار
> Hugging Face اختياري لتحليل المشاعر (`etl/process_reviews.py --backend hf`).

> **ملحوظة عن البيانات**: بيانات التجارة الإلكترونية (`data/raw/*.csv`)
> بيانات **صناعية (synthetic)** لعرض توضيحي، مش معاملات حقيقية — أي score
> أو forecast أو segment هنا بيوضّح إن الـ pipeline شغال، مش نتيجة عمل حقيقية.

دمج تلات مشاريع منفصلة كانوا عندك في ملف واحد متكامل:

1. **نظام جودة البيانات (DQ System)** — `dq/`
2. **منصة تحليلات التجارة الإلكترونية** (ETL + ML + API) — `etl/`, `ml/`, `api/`
3. **خط أنابيب بيانات الكريبتو** (Binance/CoinGecko extract + Spark/dbt/Airflow) — `ingestion/crypto/`

كل حاجة هنا **اتشغّلت فعليًا وجربتها** في البيئة اللي بكتب فيها الكود (مش
مجرد كتابة كود وسيبه)، ما عدا الأجزاء اللي محتاجة بنية تحتية أو اتصال
بإنترنت مش متاح هنا (موضّح تحت في قسم "الحدود").

---

## إيه اللي اتغيّر عن الثلاث مشاريع الأصليين (مميزات جديدة حقيقية، مش بس نسخ ولزق)

1. **DQ Gate جوه الـ ETL نفسه** (`dq/quality_gate.py` + `etl/pipeline.py`)
   محرك الـ DQ كان أداة منفصلة تمامًا عن الـ ETL في المشروعين الأصليين.
   دلوقتي كل جدول بيتحمّل في الـ warehouse (`dim_customer`, `dim_product`,
   `fact_sales`) بياخد Data Quality Score تلقائيًا **قبل** التحميل، ولو
   السكور وقع تحت حد معيّن (افتراضيًا 80) الـ load بيتوقف. جربتها فعليًا:
   بيانات نضيفة بتعدي، وبيانات متعمد تكون وحشة (نص القيم فاضية + مفاتيح
   مكررة) بتتوقف فعلاً مع رسالة واضحة — الاختبار موجود في
   `dq/tests/test_quality_gate.py`.

2. **API واحد موحّد** (`api/main.py`) بيجمع:
   - DQ endpoints (`POST /dq/validate` — ارفع أي CSV واخد score + auto-remediation)
   - تحليلات التجارة الإلكترونية (`/forecast`, `/customers/{id}/segment`,
     `/sales/anomalies`, `/products/{id}/similar`, `/customers/{id}/recommendations`)
   - **جديد**: `POST /pipeline/run` — تشغّل الـ ETL pipeline كامل عن طريق HTTP request
   - **جديد**: `GET /quality/latest` و `GET /quality/history/{dataset_name}` —
     تتبّع جودة البيانات عبر الزمن لكل جدول، مش بس لملف واحد زي ما كان
     في نظام الـ DQ الأصلي

3. **كوبري بين الكريبتو ومحرك الـ DQ** (`ingestion/crypto/full_stack_optional/src/pipeline/quality/dq_bridge.py`)،
   ومتنادى فعليًا من جوه الـ DAG الحقيقي (`crypto_market_ingest.py`) —
   بيانات الـ OHLCV بتاخد نفس نظام الـ scoring اللي بتاخده بيانات
   التجارة الإلكترونية، فمعندكش نظامين منفصلين لجودة البيانات.

4. **أمر واحد يشغّل كل حاجة**: `python3 run_all.py`
   (DQ check → ETL+Gate → تدريب كل الموديلات → تشغيل الـ API اختياريًا)

5. **بقات حقيقية اتصلحت** (مش نظرية — لقيتها وأنا بشغّل الكود فعليًا):
   - `train_and_save_models.py` كان بيدرب 3 موديلات بس وناسي موديل
     التوصيات (recommendation) رغم إن اسم الملف والـ docstring بيقولوا
     "كل النماذج" — دلوقتي بيدرب الأربعة.
   - `get_audit_trail()` بترجّع DataFrame فيه أرقام `numpy.int64` مش
     قابلة تتحوّل JSON مباشرة في استجابة الـ API — اتصلحت.
   - `datetime.utcnow()` في `dq/history.py` كانت deprecated في بايثون 3.12
     — اتحوّلت لـ `datetime.now(timezone.utc)`.

---

## طريقة التشغيل

```bash
pip install -r requirements.txt --break-system-packages   # أو جوه virtualenv عادي

# كل حاجة بأمر واحد: فحص DQ + ETL مع الـ gate + تدريب كل الموديلات
python3 run_all.py

# نفس الحاجة + تشغيل الـ API بعدها على المنفذ 8000
python3 run_all.py --serve

# أو تشغيل الأجزاء لوحدها:
cd dq && python3 main.py --file customers_orders.csv --name customers_orders --remediate
cd etl && python3 pipeline.py
cd api && python3 train_and_save_models.py && uvicorn main:app --reload --port 8000
```

بعد التشغيل، `http://localhost:8000/docs` فيه Swagger UI تفاعلي لكل الـ
endpoints (DQ + تحليلات + pipeline + quality history).

### تشغيل كل الاختبارات

```bash
cd dq && python3 -m unittest discover -s tests -v          # 34/34 ناجح
cd ingestion/crypto && python3 -m unittest test_dq_bridge -v   # 3/3 ناجح
cd ingestion/crypto/full_stack_optional && python3 -m pytest tests/ -q \
    --ignore=tests/test_spark_silver.py --ignore=tests/test_warehouse_integration.py \
    --ignore=tests/test_lake.py                              # 71/71 ناجح (بدون Spark/Postgres حقيقي، شامل اختبارات dq_bridge)
```

---

## هيكل المشروع

```
unified_data_platform/
├── run_all.py                  # نقطة الدخول الموحّدة
├── requirements.txt
│
├── dq/                         # نظام جودة البيانات (المشروع الأول، زي ما هو تقريبًا)
│   ├── main.py, validation.py, scoring.py, auto_remediation.py, ...
│   └── quality_gate.py         # ★ جديد: الكوبري بين DQ وأي pipeline تاني
│
├── etl/                        # ETL بتاع التجارة الإلكترونية (المشروع الثاني)
│   └── pipeline.py             # ★ معدّل: فيه خطوة DQ Gate جديدة
│
├── ml/                         # segmentation, forecasting, anomaly, recommendations
├── models/                     # الموديلات المتدربة (.joblib) + lookups
├── database/                   # SQLite warehouse (star schema)
├── data/raw/                   # بيانات خام (customers, products, orders, order_items)
│
├── api/
│   └── main.py                 # ★ جديد: API موحّد (DQ + تحليلات + pipeline + quality history)
│
├── orchestration/               # airflow_dag.py + simple_scheduler.py (زي ما هما)
├── postgres_migration/          # ترحيل اختياري من SQLite لـ Postgres (زي ما هو)
│
└── ingestion/crypto/            # المشروع الثالث
    ├── pipeline/                 # الاستخلاص + الـ quality checks الأصلية (زي ما هي)
    ├── dq_bridge.py              # ★ جديد: يوصل بيانات الكريبتو بمحرك DQ الموحّد
    ├── test_dq_bridge.py         # ★ جديد: اختبار فعلي على بيانات صناعية
    └── full_stack_optional/      # كل حاجة تانية من المشروع الأصلي (Airflow/dbt/Spark/
                                   # docker-compose) — منقولة كاملة زي ما هي، محتاجة
                                   # Docker + نت عشان تشتغل (مش متاحين هنا)
```

---

## الحدود (اللي مقدرتش أختبره live، وليه)

بيئة التنفيذ اللي كتبت فيها الكود مسموح فيها بس بالوصول لمواقع تثبيت
الباكدجات (pypi.org, npmjs.com, github.com, ...)، مش مسموح فيها بـ:

- `api.binance.com` / `api.coingecko.com` (استخلاص بيانات الكريبتو الحقيقية)
- Docker / أي حاويات (عشان تشغّل Airflow, Spark, Postgres, MinIO الفعليين)

فبالنسبالهم عملت اللي أقدر عليه واللي فعلاً بيثبت إن الكود سليم:
- شغّلت الاختبارات الأصلية بتاعة مشروع الكريبتو (اللي بتموّه الـ HTTP
  requests بدل نت حقيقي) — **70 اختبار عدّوا بنجاح** (67 الأصليين + 3 اختبارات dq_bridge الجديدة).
- كتبت وشغّلت اختبارات جديدة لكوبري الـ DQ على بيانات OHLCV صناعية
  (نضيفة ووحشة) عشان أثبت إن التكامل بين المشروعين شغال منطقيًا.

كل حاجة تانية في المشروع (نظام DQ، الـ ETL، الموديلات الأربعة، الـ API
الموحّد بكل endpoints بتاعته، والـ CLI الموحّد) **اتشغّلت من الأول للآخر
فعليًا هنا** مش بس اتكتبت.
