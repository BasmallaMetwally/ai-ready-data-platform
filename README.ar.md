# AI-Ready Data Platform

منصة بيانات end-to-end بتحوّل بيانات متجر إلكتروني صناعية وJSON reviews إلى
warehouse موثوق ومجهز للتحليلات والـ ML. المشروع يجمع ETL محكوم بجودة البيانات،
star schema، FastAPI، وأجزاء اختيارية لخط كريبتو باستخدام Airflow وSpark وdbt.

**التقنيات:** Python، Pandas، SQL، SQLite، PostgreSQL، MySQL، FastAPI،
scikit-learn، Airflow، dbt، Spark، Docker، وGitHub Actions.

للتفاصيل الكاملة والأوامر: [README الإنجليزي](README.md). معلومات التحقق
والقيود البيئية: [docs/VERIFICATION.md](docs/VERIFICATION.md).

> بيانات e-commerce في `data/raw/` صناعية وموجودة لإتاحة تشغيل المشروع بشكل
> قابل للتكرار؛ نتائجها أمثلة هندسية وليست ادعاءات أعمال حقيقية.

## في دقيقة

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 run_all.py --skip-ml

cd api && uvicorn main:app --reload --port 8000
```

بعد التشغيل افتح `http://localhost:8000/docs` لتجربة الـ API. الأمر يشغّل
فحص DQ، وETL، والـ quality gate، وSQLite warehouse، ومعالجة reviews.

## المعمارية

```mermaid
flowchart LR
    A[CSV sources] --> B[ETL]
    R[JSON reviews] --> S[Sentiment]
    B --> Q{DQ gate}
    Q -->|Pass| W[(Star-schema warehouse)]
    Q -->|Fail| H[History & remediation]
    S --> W
    W --> M[EDA · SQL · ML · FastAPI]
    C[Crypto APIs] --> X[Airflow · Spark · dbt]
    X --> Q
```

## أهم ما يوضحه المشروع

- DQ scoring وload blocking وremediation وaudit history.
- star schema: `dim_customer` و`dim_product` و`dim_date` و`fact_sales`.
- تحميل حي إلى SQLite أو PostgreSQL أو MySQL.
- معالجة بيانات structured CSV وunstructured JSON reviews.
- forecasting وsegmentation وanomaly detection وrecommendations.
- FastAPI وGitHub Actions ومشروع crypto اختياري متكامل.

## قواعد البيانات

SQLite هو الافتراضي ولا يحتاج server. لاستخدام PostgreSQL أو MySQL ثبّت
drivers الاختيارية وحدد قاعدة بيانات تجريبية؛ كل تحميل يعيد إنشاء schema.

```bash
pip install -r etl/requirements-db.txt

python3 run_all.py --skip-ml --target postgres \
  --database-url 'postgresql://user:password@localhost:5432/ecommerce_dw'

python3 run_all.py --skip-ml --target mysql \
  --database-url 'mysql://user:password@localhost:3306/ecommerce_dw'
```

## الاختبارات

```bash
python3 -m unittest discover -s dq/tests -v
python3 -m unittest discover -s etl/tests -v
```

لا تستخدمي رقم tests ثابت في الـ CV؛ اعتبري GitHub Actions هو المرجع للأرقام
المحدّثة. تنفيذ Airflow/Docker وlive crypto APIs وHugging Face model يحتاج
بيئة خارجية مناسبة، ومش موصوف هنا كتنفيذ مكتمل قبل توفرها.
