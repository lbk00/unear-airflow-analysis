# dags/user_summary_dag.py

from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import re

from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)


def extract_list_or_single(value):
    if isinstance(value, list):
        return value
    elif isinstance(value, str):
        return [value]
    return []

# 사용자 행동 기반 집계
def summarize_user_actions():
    try:
        import json
        from collections import defaultdict
        from datetime import date
        import psycopg2

        # summary_date = date.today() - timedelta(days=1)
        summary_date = date.today()

        with psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT user_id, action_type, screen, metadata, create_at
                    FROM user_action_logs
                    WHERE create_at::date = %s
                """, (summary_date,))
                rows = cursor.fetchall()

                summary = defaultdict(lambda: {"user_set": set(), "count": 0})

                for user_id, action_type, screen, metadata ,create_at in rows:
                    if not metadata:
                        continue
                    try:
                        metadata_dict = json.loads(metadata)
                    except json.JSONDecodeError:
                        continue

                    gender = metadata_dict.get('gender')
                    age_group = metadata_dict.get('ageGroup')

                    # 1. 연령대/성별에따른 인기 검색어
                    if action_type in {"PLACE_KEYWORD", "BENEFIT_KEYWORD"}:
                        keyword = metadata_dict.get("keyword")
                        if keyword and keyword.strip():
                            normalized_keyword = keyword.strip().lower()
                            if gender and age_group:
                                key2 = ("AGE_GENDER_KEYWORD", f"{age_group}_{gender}", normalized_keyword)
                                summary[key2]["user_set"].add(user_id)
                                summary[key2]["count"] += 1

                    # 2. 연령대/성별에 따른 관심 카테고리
                    if action_type in ('BENEFIT_DETAIL', 'VIEW_PLACE_DETAIL', 'PLACE_FILTER', 'FAVORITE_ON'):
                        categories = extract_list_or_single(metadata_dict.get('category'))
                        for category in categories:
                            if gender and age_group and category:
                                key = ('AGE_GENDER_CATEGORY', f"{age_group}_{gender}", category.strip().lower())
                                summary[key]["user_set"].add(user_id)
                                summary[key]["count"] += 1

                    # 3. 시간대별 성별/연령대 활성도
                    if gender and age_group and create_at:
                        hour_str = create_at.strftime("%H")
                        group = f"{age_group}_{gender}"
                        key = ('AGE_GENDER_ACTIVATE_TIME', group, hour_str)
                        summary[key]["user_set"].add(user_id)
                        summary[key]["count"] += 1

                # 4. Insert 또는 Update
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

    except Exception as e:
        print(f"[ERROR] summarize_user_actions failed: {e}")
        raise


#이벤트 참여 통계
def summarize_event_actions():
    import psycopg2
    from collections import defaultdict
    from datetime import date

    try:
        summary_date = date.today()

        with psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        ) as conn:
            with conn.cursor() as cursor:

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
                    summary_key = ("EVENT_JOIN", group, "age_gender")
                    summary[summary_key]["user_set"].add(user_id)
                    summary[summary_key]["count"] += 1

                    if completed:
                        done_key = ("EVENT_DONE", group, "age_gender")
                        summary[done_key]["user_set"].add(user_id)
                        summary[done_key]["count"] += 1
                    else:
                        drop_key = ("EVENT_DROP", group, "age_gender")
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

            conn.commit()  # with conn 블록 안에서 명시적 commit

    except Exception as e:
        print(f"[ERROR] summarize_event_actions failed: {e}")
        raise


# 인기 이벤트 장소 요약
def summarize_event_place_popularity():
    import psycopg2
    from collections import defaultdict
    from datetime import date
    import json
    import os
    import math

    try:
        summary_date = date.today()

        with psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        ) as conn:
            with conn.cursor() as cursor:

                # 1. 클릭 집계
                cursor.execute("""
                    SELECT metadata, COUNT(*) AS click_count, COUNT(DISTINCT user_id)
                    FROM user_action_logs
                    WHERE action_type = 'VIEW_PLACE_DETAIL'
                      AND screen = 'eventPage'
                      AND create_at::date = %s
                    GROUP BY metadata
                """, (summary_date,))

                click_stats = {}
                total_clicks = 0

                for metadata_json, count, distinct_users in cursor.fetchall():
                    try:
                        metadata = json.loads(metadata_json)
                        place_name = metadata.get("placeName")
                        if place_name:
                            key = place_name.strip()
                            click_stats[key] = {
                                "click_count": count,
                                "click_users": distinct_users
                            }
                            total_clicks += count
                    except json.JSONDecodeError:
                        continue

                # 2. 스탬프 집계
                cursor.execute("""
                    SELECT place_name, COUNT(*) AS stamp_count, COUNT(DISTINCT user_id)
                    FROM stamps
                    WHERE stamped_at::date = %s
                    GROUP BY place_name
                """, (summary_date,))

                stamp_stats = {}
                total_stamps = 0

                for place_name, count, distinct_users in cursor.fetchall():
                    if place_name:
                        key = place_name.strip()
                        stamp_stats[key] = {
                            "stamp_count": count,
                            "stamp_users": distinct_users
                        }
                        total_stamps += count

                # 3. 통합 저장
                all_place_names = set(click_stats.keys()) | set(stamp_stats.keys())

                for place_name in all_place_names:
                    click_info = click_stats.get(place_name, {})
                    stamp_info = stamp_stats.get(place_name, {})

                    click_count = click_info.get("click_count", 0)
                    click_users = click_info.get("click_users", 0)
                    stamp_count = stamp_info.get("stamp_count", 0)
                    stamp_users = stamp_info.get("stamp_users", 0)

                    normalized_stamp = stamp_count / total_stamps if total_stamps > 0 else 0
                    click_score = math.log(click_count + 1)
                    stamp_score = 5 * normalized_stamp
                    popularity_score = click_score + stamp_score
                    distinct_user_count = max(click_users, stamp_users)

                    if popularity_score > 0:
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
                            "EVENT_PLACE_POPULARITY",
                            place_name,
                            "event",
                            distinct_user_count,
                            popularity_score,
                            summary_date
                        ))

            conn.commit()

        print(f"[INFO] 이벤트 매장 {len(all_place_names)}곳 인기 순위 저장 완료.")

    except Exception as e:
        print(f"[ERROR] summarize_event_place_popularity failed: {e}")
        raise


default_args = {
    'owner': 'unear',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

from pendulum import datetime, timezone
kst = timezone("Asia/Seoul")

# DAG 정의
with DAG(
    dag_id='user_action_summary_dag',
    default_args=default_args,
    schedule='0 2 * * *',  # 매일 새벽 2시
    start_date=datetime(2025, 1, 1, tz=kst),
    catchup=False
) as dag:

    # Task 1: 유저 로그 요약
    summarize_user_logs_task = PythonOperator(
        task_id='summarize_user_logs',
        python_callable=summarize_user_actions
    )

    # Task 2: 이벤트 참여 요약
    summarize_event_actions_task = PythonOperator(
        task_id='summarize_event_actions',
        python_callable=summarize_event_actions
    )

    # Task 3: 이벤트 장소 인기 요약
    summarize_event_place_popularity_task = PythonOperator(
        task_id='summarize_event_place_popularity',
        python_callable=summarize_event_place_popularity
    )

    # 실행 순서 지정
    summarize_user_logs_task >> summarize_event_actions_task >> summarize_event_place_popularity_task


#로컬 테스트용
if __name__ == "__main__":
    summarize_user_actions()
    summarize_event_actions()
    summarize_event_place_popularity()