"""
simple_scheduler.py
---------------------
بديل بسيط وخفيف لـ Airflow لما مايكونش متاح — شغّال بمكتبة Python
القياسية بس (sched + time)، وبيشغّل الـ pipeline كل يوم في معاد ثابت.

الاستخدام:
    python3 simple_scheduler.py --daily-at 02:00

أو في بيئة إنتاجية حقيقية، الأفضل تستخدم crontab بدل ما تسيب سكريبت
شغال طول الوقت. مثال على crontab entry بيشغل الـ pipeline يومياً الساعة 2 صباحاً:

    0 2 * * * cd /path/to/etl && /usr/bin/python3 pipeline.py >> /path/to/logs/cron.log 2>&1
"""

import argparse
import sched
import sys
import time
import os
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "etl"))


def run_pipeline_job():
    from pipeline import run_pipeline
    from exceptions import PipelineError

    print(f"\n[{datetime.now()}] بدء تشغيل الـ ETL Pipeline المجدول...")
    try:
        run_pipeline()
        print(f"[{datetime.now()}] الـ Pipeline اشتغل بنجاح.")
    except PipelineError as e:
        print(f"[{datetime.now()}] فشل الـ Pipeline: {e}")
        # في بيئة إنتاجية: هنا نبعت تنبيه (Slack/Email) بدل ما نكتفي بالطباعة


def seconds_until_next_run(target_time: str) -> float:
    hour, minute = map(int, target_time.split(":"))
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def main():
    parser = argparse.ArgumentParser(description="جدولة تشغيل الـ ETL Pipeline يومياً")
    parser.add_argument("--daily-at", default="02:00", help="المعاد اليومي بصيغة HH:MM (24 ساعة)")
    parser.add_argument("--run-once-now", action="store_true",
                         help="يشغّل الـ pipeline فوراً مرة واحدة (للاختبار) بدل الانتظار")
    args = parser.parse_args()

    if args.run_once_now:
        run_pipeline_job()
        return

    scheduler = sched.scheduler(time.time, time.sleep)
    print(f"تم تفعيل الجدولة: هيشتغل الـ pipeline يومياً الساعة {args.daily_at}")

    def schedule_next():
        delay = seconds_until_next_run(args.daily_at)
        print(f"التشغيل الجاي بعد {delay/3600:.1f} ساعة ({datetime.now() + timedelta(seconds=delay)})")
        scheduler.enter(delay, 1, run_and_reschedule)

    def run_and_reschedule():
        run_pipeline_job()
        schedule_next()

    schedule_next()
    scheduler.run()


if __name__ == "__main__":
    main()
