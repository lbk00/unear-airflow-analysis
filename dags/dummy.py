import os
import random
from datetime import date, timedelta
from dotenv import load_dotenv
import numpy as np

# .env 로드
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# # 월별 키워드 가중치
# monthly_keyword_weights = {
#     3: [("포토존", 0.15), ("피크닉", 0.12), ("브런치", 0.1), ("카페", 0.1), ("감성카페", 0.08),
#         ("팝업", 0.07), ("플리마켓", 0.07), ("성수동", 0.06), ("핫플", 0.06),
#         ("갤러리", 0.04), ("디저트", 0.03), ("러닝", 0.03)],
#     4: [("공원", 0.13), ("피크닉", 0.12), ("카페", 0.1), ("브런치", 0.09), ("감성카페", 0.08),
#         ("전시", 0.08), ("팝업", 0.07), ("디저트", 0.06), ("핫플", 0.06), 
#        ("포토존", 0.03)],
#     5: [("맛집", 0.12), ("카페", 0.1), ("한강", 0.09), ("전시", 0.09), ("핫플", 0.08),
#         ("페스티벌", 0.07), ("브런치", 0.06), ("피크닉", 0.06), ("팝업", 0.05),
#         ("루프탑", 0.05), ("야경", 0.05), ("전동킥보드", 0.04)],
#     6: [("갤러리", 0.12), ("감성카페", 0.1), ("카페", 0.1), ("커피", 0.09),
#         ("핫플", 0.07), ("성수동", 0.06), ("공원", 0.06), ("맛집", 0.05), ("디저트", 0.04)],
#     7: [("카페", 0.12), ("커피", 0.1), ("디저트", 0.1), ("야경", 0.08),
#         ("페스티벌", 0.08), ("전시", 0.07), ("한강", 0.06), ("핫플", 0.06), 
#         ("포토존", 0.03)],
#     8: [("팝업", 0.12), ("핫플", 0.12), ("카페", 0.1), ("맛집", 0.09), ("전시", 0.08),
#      ("성수동", 0.05), ("감성카페", 0.04),
#       ("한강", 0.03), ("피크닉", 0.03)],
# }


# # 그룹 정의
# age_gender_groups = ['10s_M', '10s_F', '20s_M', '20s_F', '30s_M', '30s_F', '40s_M', '40s_F']

# # 샘플링 함수
# def weighted_sample(choices, k):
#     items, weights = zip(*choices)
#     return list(np.random.choice(items, size=k, replace=False, p=np.array(weights) / sum(weights)))

# # 날짜 범위 설정
# start = date(2025, 3, 1)
# end = date(2025, 8, 8)
# current = start

# sql_lines = []

# while current <= end:
#     month = current.month
#     keyword_weights = monthly_keyword_weights[month]

#     for group in age_gender_groups:
#         # 해당 그룹에서 월별로 선호 키워드 샘플링 (4~6개 뽑기)
#         dynamic_preference = weighted_sample(keyword_weights, k=random.choice([4, 5, 6]))

#         is_trendy = current.day in [3, 10, 17, 25] or current.weekday() == 5
#         keyword_count = 4 if is_trendy else 3
#         selected_keywords = random.sample(dynamic_preference, k=min(keyword_count, len(dynamic_preference)))

#         for keyword in selected_keywords:
#             count = random.randint(40, 80) if is_trendy else random.randint(5, 30)
#             distinct = max(1, count // random.randint(2, 4))

#             sql = f"""INSERT INTO user_action_summary (
#                 action_type, group_by_field, group_by_text,
#                 distinct_user_count, count, summary_date
#             ) VALUES (
#                 'AGE_GENDER_KEYWORD', '{group}', '{keyword}',
#                 {distinct}, {count}, '{current}'
#             ) ON CONFLICT (action_type, group_by_field, group_by_text, summary_date) DO NOTHING;"""

#             sql_lines.append(sql)
#     current += timedelta(days=1)

# # 저장
# output_path = "./insert_keyword_summary.sql"
# with open(output_path, "w", encoding="utf-8") as f:
#     f.write("\n".join(sql_lines))

# print(f"✅ SQL 파일 생성 완료: {output_path}")








# import os
# import random
# from datetime import date, timedelta
# import numpy as np

# # 전체 카테고리
# categories = ['food', 'activity', 'education', 'bakery', 'life', 'shopping', 'cafe', 'beauty', 'culture']

# # 그룹별 선호 카테고리 (남성, 여성 차별화)
# group_category_preferences = {
#     '10s_M': ['activity', 'food', 'shopping'],
#     '10s_F': ['beauty', 'cafe', 'shopping'],
#     '20s_M': ['cafe', 'food', 'life'],
#     '20s_F': ['beauty', 'cafe', 'bakery'],
#     '30s_M': ['life', 'education', 'food'],
#     '30s_F': ['beauty', 'life', 'culture'],
#     '40s_M': ['education', 'food', 'culture'],
#     '40s_F': ['life', 'beauty', 'education'],
# }

# monthly_trends = {
#     3: ['beauty', 'bakery', 'culture'],
#     4: ['cafe', 'shopping', 'culture'],
#     5: ['activity', 'food', 'life'],
#     6: ['education', 'life', 'food'],
#     7: ['activity', 'cafe', 'shopping'],
#     8: ['life', 'culture', 'food'],
# }

# # 가중치 적용 함수
# def get_weighted_categories(group, month, all_categories):
#     group_pref = group_category_preferences.get(group, [])
#     trend_pref = monthly_trends.get(month, [])

#     weights = []
#     for cat in all_categories:
#         score = 1.0  # 기본값
#         if cat in group_pref:
#             score += 1.2  # 그룹 선호
#         if cat in trend_pref:
#             score += 0.8  # 월별 트렌드
#         weights.append(score)

#     # 정규화
#     weight_sum = sum(weights)
#     return [(cat, w / weight_sum) for cat, w in zip(all_categories, weights)]


# # 가중치 기반 샘플링 함수
# def weighted_sample(choices, k):
#     items, weights = zip(*choices)
#     return list(np.random.choice(items, size=k, replace=False, p=np.array(weights)))

# # 날짜 범위
# start = date(2025, 3, 1)
# end = date(2025, 8, 8)
# current = start

# sql_statements = []

# while current <= end:
#     month = current.month
#     for group in group_category_preferences.keys():
#         is_trendy = current.day in [1, 7, 14, 21, 28] or current.weekday() == 6

#         k = random.randint(5, 8) if is_trendy else random.randint(2, 4)
#         weight_list = get_weighted_categories(group, month, categories)
#         selected = weighted_sample(weight_list, k=k)

#         for category in selected:
#             count = random.randint(30, 70) if is_trendy else random.randint(5, 25)
#             distinct = max(1, count // random.randint(2, 4))

#             sql = f"""INSERT INTO user_action_summary (
#     action_type, group_by_field, group_by_text,
#     distinct_user_count, count, summary_date
# ) VALUES (
#     'AGE_GENDER_CATEGORY', '{group}', '{category}', {distinct}, {count}, '{current}'
# ) ON CONFLICT (action_type, group_by_field, group_by_text, summary_date) DO NOTHING;"""
#             sql_statements.append(sql)

#     current += timedelta(days=1)


# # 파일로 저장
# output_path = "./insert_category_summary.sql"
# with open(output_path, "w", encoding="utf-8") as f:
#     f.write("\n".join(sql_statements))

# print(f"✅ 카테고리 SQL 파일 생성 완료: {output_path}")






# import os
# import random
# from datetime import date, timedelta

# age_gender_groups = ['10s_M', '10s_F', '20s_M', '20s_F', '30s_M', '30s_F', '40s_M', '40s_F']

# def extract_age_sex(group):
#     return int(group[:2]), group[-1]

# def get_month_weights(month, age, sex):
#     if sex == 'F':
#         if age == 10:
#             completion_rate = random.uniform(0.60, 0.68)
#             drop_rate = random.uniform(0.28, 0.32)
#         elif age == 20:
#             completion_rate = random.uniform(0.65, 0.75)
#             drop_rate = random.uniform(0.22, 0.30)
#         elif age == 30:
#             completion_rate = random.uniform(0.70, 0.80)
#             drop_rate = random.uniform(0.18, 0.28)
#         elif age == 40:
#             completion_rate = random.uniform(0.72, 0.82)
#             drop_rate = random.uniform(0.15, 0.25)
#         else:
#             completion_rate = 0.65
#             drop_rate = 0.30
#     else:  # sex == 'M'
#         if age == 10:
#             completion_rate = random.uniform(0.45, 0.52)
#             drop_rate = random.uniform(0.43, 0.48)
#         elif age == 20:
#             completion_rate = random.uniform(0.50, 0.60)
#             drop_rate = random.uniform(0.35, 0.45)
#         elif age == 30:
#             completion_rate = random.uniform(0.55, 0.65)
#             drop_rate = random.uniform(0.30, 0.40)
#         elif age == 40:
#             completion_rate = random.uniform(0.55, 0.65)
#             drop_rate = random.uniform(0.30, 0.40)
#         else:
#             completion_rate = 0.5
#             drop_rate = 0.45

#     # 안전 보정
#     if completion_rate + drop_rate > 0.97:
#         scale = 0.97 / (completion_rate + drop_rate)
#         completion_rate *= scale
#         drop_rate *= scale

#     return round(completion_rate, 3), round(drop_rate, 3)




# start = date(2025, 3, 1)
# end = date(2025, 8, 8)
# current = start

# sql_lines = []

# while current <= end:
#     month = current.month
#     for group in age_gender_groups:
#         age, sex = extract_age_sex(group)

#         # 참여 인원은 5~50명
#         join_count = random.randint(5, 50)

#         # 완료율 / 이탈률 결정
#         completion_rate, drop_rate = get_month_weights(month, age, sex)

#         done_count = int(join_count * completion_rate)
#         drop_count = int(join_count * drop_rate)

#         # 중복 제거용
#         def insert(action, count):
#             distinct = max(1, count // random.randint(2, 3)) if count > 0 else 0
#             sql = f"""INSERT INTO user_action_summary (
#     action_type, group_by_field, group_by_text,
#     distinct_user_count, count, summary_date
# ) VALUES (
#     '{action}', '{group}', 'age_gender', {distinct}, {count}, '{current}'
# ) ON CONFLICT (action_type, group_by_field, group_by_text, summary_date) DO NOTHING;"""
#             sql_lines.append(sql)

#         insert("EVENT_JOIN", join_count)
#         insert("EVENT_DONE", done_count)
#         insert("EVENT_DROP", drop_count)

#     current += timedelta(days=1)

# # 저장
# output_path = "./insert_event_summary.sql"
# with open(output_path, "w", encoding="utf-8") as f:
#     f.write("\n".join(sql_lines))

# print(f"✅ 이벤트 요약 SQL 생성 완료: {output_path}")



# import random
# from datetime import date, timedelta

# # 인기 매장 목록
# place_names = [
#     "자그마치 성수", "디커피", "롱브르378", "언더프레셔 성수점",
#     "시크릿 커피", "성수동 팝업스토어", "뚜레쥬르 성수역점",
#     "나이스웨더", "땡스오트", "페인트커피", "어니언 성수",
#     "밀로커피로스터스", "오르에르 성수"
# ]

# start_date = date(2025, 8, 1)
# end_date = date(2025, 8, 8)

# sql_lines = []

# current = start_date
# while current <= end_date:
#     for place in place_names:
#         # 유저 수 및 점수 랜덤 생성
#         click_count = random.randint(3, 50)
#         stamp_count = random.randint(0, 30)

#         distinct_user_count = max(1, int((click_count + stamp_count) * random.uniform(0.4, 0.8)))

#         # 점수 공식: log(클릭수+1) + 5 * (스탬프 / 전체합) → 단순화 버전
#         click_score = round(random.uniform(1.0, 2.5), 2)
#         stamp_score = round(random.uniform(0.0, 3.0), 2)
#         popularity_score = round(click_score + stamp_score, 2)

#         sql = f"""INSERT INTO user_action_summary (
#     action_type, group_by_field, group_by_text,
#     distinct_user_count, count, summary_date
# ) VALUES (
#     'EVENT_PLACE_POPULARITY', '{place}', 'event', {distinct_user_count}, {popularity_score}, '{current}'
# ) ON CONFLICT (action_type, group_by_field, group_by_text, summary_date) DO NOTHING;"""
#         sql_lines.append(sql)

#     current += timedelta(days=1)

# # 저장
# output_path = "./insert_event_place_popularity.sql"
# with open(output_path, "w", encoding="utf-8") as f:
#     f.write("\n".join(sql_lines))

# print(f"✅ 이벤트 인기 매장 SQL 생성 완료: {output_path}")
