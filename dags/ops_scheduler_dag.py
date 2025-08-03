# 요약이 아닌 스케줄러 작업용 dag

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import re

from dotenv import load_dotenv
import os
# 반드시 명시적으로 상위 경로의 .env 지정
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)


# 매월 1일 새벽 1시 쿠폰 템플릿 사용기간 업데이트 (선착순 쿠폰 제외)
def update_coupon_template_dates():
    try:
        conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
        cur = conn.cursor()

        cur.execute("""
            UPDATE coupon_templates
            SET
                coupon_start = date_trunc('month', current_date),
                coupon_end = date_trunc('month', current_date + interval '1 month')
            WHERE
                discount_code != 'COUPON_FCFS'
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] update_coupon_template_dates failed: {e}")
        raise



default_args = {
    'owner': 'unear',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ops_scheduler_dag',
    default_args=default_args,
    schedule='0 1 1 * *',  # 매월 1일 01:00
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    update_coupon_task = PythonOperator(
        task_id='update_coupon_template_dates',
        python_callable=update_coupon_template_dates
    )
