# dags/user_summary_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import re

from dotenv import load_dotenv
import os
# 반드시 명시적으로 상위 경로의 .env 지정
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)


def summarize_user_actions():
    import json
    from collections import defaultdict
    from datetime import date
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    # summary_date = date.today()
    summary_date = date.today() - timedelta(days=1)

    cursor.execute("""
        SELECT user_id, action_type, screen, metadata, create_at
        FROM user_action_logs
        WHERE create_at::date = %s
    """, (summary_date,))
    rows = cursor.fetchall()

    summary = defaultdict(lambda: {"user_set": set(), "count": 0})

    config = {
        'BENEFIT_DETAIL': ['franchiseName'],
        'BENEFIT_KEYWORD': ['keyword'],
        'BENEFIT_CATEGORY': ['category'],
        'PLACE_KEYWORD': ['keyword'],
        'PLACE_FILTER': ['category', 'benefit'],
        'VIEW_PLACE_DETAIL': ['category', 'benefit'],
        'FAVORITE_ON': ['category', 'benefit'],
        'DOWNLOAD_COUPON': ['benefit', 'grade'],
        'DOWNLOAD_FCFS_COUPON': ['benefit', 'grade'],
    }

    for user_id, action_type, screen, metadata ,create_at in rows:
        if not metadata:
            continue
        try:
            metadata_dict = json.loads(metadata)
        except json.JSONDecodeError:
            continue


        #  성별 + 연령대 : 관심있는 카테고리
        if action_type in ('BENEFIT_CATEGORY', 'VIEW_PLACE_DETAIL', 'PLACE_FILTER', 'FAVORITE_ON'):
            gender = metadata_dict.get('gender')
            age_group = metadata_dict.get('ageGroup')
            category = metadata_dict.get('category')
            
            if gender and age_group and category:
                key = ('AGE_GENDER_CATEGORY', f"{age_group}_{gender}", category.strip().lower())
                summary[key]["user_set"].add(user_id)
                summary[key]["count"] += 1
        

        # PLACE_KEYWORD 처리
        if action_type == "PLACE_KEYWORD":
            keyword = metadata_dict.get("keyword")
            if keyword:
                normalized_keyword = keyword.strip().lower()

                # 성별/연령대별
                gender = metadata_dict.get("gender")
                age_group = metadata_dict.get("ageGroup")
                if gender and age_group:
                    key2 = ("AGE_GENDER_KEYWORD_PLACE", f"{age_group}_{gender}", normalized_keyword)
                    summary[key2]["user_set"].add(user_id)
                    summary[key2]["count"] += 1

        # BENEFIT_KEYWORD 처리
        elif action_type == "BENEFIT_KEYWORD":
            keyword = metadata_dict.get("keyword")
            if keyword:
                normalized_keyword = keyword.strip().lower()

                gender = metadata_dict.get("gender")
                age_group = metadata_dict.get("ageGroup")
                if gender and age_group:
                    key2 = ("AGE_GENDER_KEYWORD_BENEFIT", f"{age_group}_{gender}", normalized_keyword)
                    summary[key2]["user_set"].add(user_id)
                    summary[key2]["count"] += 1

        # 시간대별 연령/성별 활성도 분석
        gender = metadata_dict.get('gender')
        age_group = metadata_dict.get('ageGroup')
        if gender and age_group and create_at:
            hour_str = create_at.strftime("%H")  # '00' ~ '23'
            group = f"{age_group}_{gender}"     # 예: '20s_F'
            key = ('AGE_GENDER_ACTIVATE_TIME', group, hour_str)
            summary[key]["user_set"].add(user_id)
            summary[key]["count"] += 1



    # insert
    for (action_type, group_by_field, group_by_text), data in summary.items():
        distinct_user_count = len(data["user_set"])
        count = data["count"]

        cursor.execute("""
            INSERT INTO user_action_summary (
                action_type, group_by_field, group_by_text,
                distinct_user_count, count, summary_date
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (action_type, group_by_field, group_by_text, summary_date)
            DO UPDATE SET
                distinct_user_count = EXCLUDED.distinct_user_count,
                count = EXCLUDED.count
        """, (
            action_type, group_by_field, group_by_text,
            distinct_user_count, count, summary_date
        ))

    conn.commit()
    cursor.close()
    conn.close()


def summarize_event_actions():
    import psycopg2
    from collections import defaultdict
    from datetime import date

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    # summary_date = date.today()
    summary_date = date.today() - timedelta(days=1)

    # EVENT 참여/완료/이탈 집계용
    summary = defaultdict(lambda: {"user_set": set(), "count": 0})

    # 참여 + 유저 메타 정보
    cursor.execute("""
        SELECT rr.user_id,
            rr.unear_event_id,
            rr.reward IS NOT NULL AS completed,
            CASE 
                WHEN EXTRACT(YEAR FROM AGE(current_date, u.birthdate)) < 20 THEN '10s'
                WHEN EXTRACT(YEAR FROM AGE(current_date, u.birthdate)) < 30 THEN '20s'
                WHEN EXTRACT(YEAR FROM AGE(current_date, u.birthdate)) < 40 THEN '30s'
                WHEN EXTRACT(YEAR FROM AGE(current_date, u.birthdate)) < 50 THEN '40s'
                ELSE '50s+'
            END AS age_group,
            u.gender
        FROM roulette_results rr
        JOIN users u ON rr.user_id = u.user_id
        WHERE rr.participated = true
        AND u.birthdate IS NOT NULL
        AND u.gender IS NOT NULL
    """)
    for user_id, event_id, completed, age_group, gender in cursor.fetchall():
        group = f"{age_group}_{gender}"
        summary_key = ("EVENT_JOIN", "age_gender", group)
        summary[summary_key]["user_set"].add(user_id)
        summary[summary_key]["count"] += 1

        if completed:
            done_key = ("EVENT_DONE", "age_gender", group)
            summary[done_key]["user_set"].add(user_id)
            summary[done_key]["count"] += 1
        else:
            drop_key = ("EVENT_DROP", "age_gender", group)
            summary[drop_key]["user_set"].add(user_id)
            summary[drop_key]["count"] += 1

    # 결과 저장
    for (action_type, group_by_field, group_by_text), data in summary.items():
        distinct_user_count = len(data["user_set"])
        count = data["count"]
        cursor.execute("""
            INSERT INTO user_action_summary (
                action_type, group_by_field, group_by_text,
                distinct_user_count, count, summary_date
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (action_type, group_by_field, group_by_text, summary_date)
            DO UPDATE SET
                distinct_user_count = EXCLUDED.distinct_user_count,
                count = EXCLUDED.count
        """, (
            action_type, group_by_field, group_by_text,
            distinct_user_count, count, summary_date
        ))

    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    'owner': 'unear',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='user_action_summary_dag',
    default_args=default_args,
    schedule='0 3 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    summarize_task = PythonOperator(
        task_id='summarize_user_logs',
        python_callable=summarize_user_actions
    )


if __name__ == "__main__":
    summarize_user_actions()
    summarize_event_actions()