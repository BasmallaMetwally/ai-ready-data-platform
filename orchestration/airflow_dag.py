"""
airflow_dag.py
--------------
DAG لجدولة الـ ETL Pipeline يومياً باستخدام Apache Airflow.

ملحوظة: الملف ده كود جاهز للنشر في بيئة فيها Airflow متثبت
(مش مُشغّل في هذا المشروع لأن Airflow محتاج سيرفر وقاعدة بيانات خاصة بيه).
لتشغيله: انسخه في مجلد $AIRFLOW_HOME/dags/ وشغّل الـ Airflow scheduler.

الجدولة: يومياً الساعة 2 صباحاً (بعد انتهاء يوم العمليات، وقبل بداية
تقارير الصباح)، مع 3 محاولات عند الفشل وتنبيه بالإيميل عند الفشل النهائي.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

import sys
import os

# إضافة مجلد الـ etl لمسار البحث عشان نقدر نستورد الموديولز بتاعتنا
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "etl"))


default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _run_extract(**context):
    from extract import extract_all
    raw = extract_all()
    # بنحفظ عدد الصفوف في XCom عشان نراقبه من واجهة Airflow
    context["ti"].xcom_push(key="row_counts", value={k: len(v) for k, v in raw.items()})
    return "extract_done"


def _run_transform(**context):
    from extract import extract_all
    from transform import transform_all
    raw = extract_all()
    transformed = transform_all(raw)
    # هنا في بيئة إنتاجية أفضل نحفظ النتيجة في parquet مؤقت بدل ما نعيد
    # الاستخراج، لكن مبسّطة هنا للوضوح
    return "transform_done"


def _run_validate(**context):
    from extract import extract_all
    from transform import transform_all
    from validate import validate_all
    raw = extract_all()
    transformed = transform_all(raw)
    validate_all(transformed)
    return "validate_done"


def _run_load(**context):
    from extract import extract_all
    from transform import transform_all
    from load import load_all
    raw = extract_all()
    transformed = transform_all(raw)
    load_all(transformed)
    return "load_done"


with DAG(
    dag_id="ecommerce_etl_daily",
    description="ETL يومي لبيانات المتجر الإلكتروني: Extract -> Transform -> Validate -> Load",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # يومياً الساعة 2 صباحاً
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl", "daily"],
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=_run_extract)
    transform_task = PythonOperator(task_id="transform", python_callable=_run_transform)
    validate_task = PythonOperator(task_id="validate", python_callable=_run_validate)
    load_task = PythonOperator(task_id="load", python_callable=_run_load)

    failure_alert = EmailOperator(
        task_id="notify_failure",
        to="data-alerts@example.com",
        subject="🚨 فشل ETL Pipeline - ecommerce_etl_daily",
        html_content="فشل تشغيل الـ pipeline اليومي. راجع Airflow logs فوراً.",
        trigger_rule="one_failed",
    )

    extract_task >> transform_task >> validate_task >> load_task
    [extract_task, transform_task, validate_task, load_task] >> failure_alert
