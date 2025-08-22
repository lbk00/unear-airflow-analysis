# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import os
# import psycopg2
# import pandas as pd
# import json
# import openai
# import numpy as np
# from collections import defaultdict

# # 설정
# OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# default_args = {
#     'owner': 'unear_1seyoung',
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def get_db_connection():
#     return psycopg2.connect(
#         host=os.environ['DB_HOST'],
#         user=os.environ['DB_USER'],
#         password=os.environ['DB_PASSWORD'],
#         database=os.environ['DB_NAME'],
#     )

# def get_age_group(birthdate):
#     """생년월일로 연령대 계산"""
#     if not birthdate:
#         return "UNKNOWN"
    
#     birth_year = birthdate.year
#     current_year = datetime.now().year
#     age = current_year - birth_year
    
#     if age < 20:
#         return "10s"
#     elif age < 30:
#         return "20s"
#     elif age < 40:
#         return "30s"
#     elif age < 50:
#         return "40s"
#     elif age < 60:
#         return "50s"
#     else:
#         return "60s+"

# def load_persona_embeddings(model="text-embedding-3-small", prefix="사용자 페르소나 설명: "):
#     """페르소나 설명을 임베딩으로 변환"""
#     conn = get_db_connection()
#     try:
#         personas_df = pd.read_sql("""
#             SELECT payment_type_tag_id, name, description
#             FROM payment_type_tags
#         """, conn)
        
#         client = openai.OpenAI(api_key=OPENAI_API_KEY)
#         persona_embeddings = {}
        
#         for _, persona in personas_df.iterrows():
#             embedding_input = f"{prefix}{persona['description']}"
            
#             for attempt in range(3):
#                 try:
#                     response = client.embeddings.create(
#                         model=model,
#                         input=embedding_input
#                     )
#                     break
#                 except Exception as e:
#                     print(f"[ERROR] Failed embedding {persona['name']} (attempt {attempt+1}): {e}")
#                     time.sleep(1)
#             else:
#                 continue  # 3회 실패 시 skip
            
#             persona_embeddings[persona['payment_type_tag_id']] = {
#                 'name': persona['name'],
#                 'embedding': response.data[0].embedding
#             }

#         return persona_embeddings
#     finally:
#         conn.close()

# def validate_prerequisites(**context):
#     """전제 조건 검증"""
#     conn = get_db_connection()
#     try:
#         cur = conn.cursor()
        
#         # 필수 테이블 존재 확인
#         cur.execute("""
#             SELECT table_name FROM information_schema.tables 
#             WHERE table_name IN ('users', 'user_histories', 'payment_type_tags', 'user_action_summary')
#         """)
#         tables = [row[0] for row in cur.fetchall()]
        
#         required_tables = ['users', 'user_histories', 'payment_type_tags', 'user_action_summary']
#         missing_tables = set(required_tables) - set(tables)
        
#         if missing_tables:
#             raise Exception(f"필수 테이블 누락: {missing_tables}")
        
#         # user_embedding 컬럼 존재 확인
#         cur.execute("""
#             SELECT column_name FROM information_schema.columns 
#             WHERE table_name = 'user_recommendation_profile' 
#             AND column_name = 'user_embedding'
#         """)
        
#         if not cur.fetchone():
#             raise Exception("user_recommendation_profile.user_embedding 컬럼이 필요합니다")
        
#         print("✅ 전제 조건 검증 완료")
        
#     finally:
#         conn.close()

# def load_base_data(**context):
#     """기초 데이터 로드 및 검증 (테스트 모드)"""
    
#     # 테스트할 유저 ID들 (여기서 수정!)
#     TEST_USER_IDS = [10000]
    
#     conn = get_db_connection()
#     try:
#         # 테스트 유저들만 확인
#         users_df = pd.read_sql(f"""
#             SELECT COUNT(*) as user_count 
#             FROM users 
#             WHERE is_profile_complete = true 
#             AND user_id IN ({','.join(map(str, TEST_USER_IDS))})
#         """, conn)
        
#         # 테스트 유저들의 결제 내역 확인
#         payments_df = pd.read_sql(f"""
#             SELECT COUNT(*) as payment_count 
#             FROM user_histories 
#             WHERE paid_at >= NOW() - INTERVAL '4 months'
#             AND user_id IN ({','.join(map(str, TEST_USER_IDS))})
#         """, conn)
        
#         user_count = users_df.iloc[0]['user_count']
#         payment_count = payments_df.iloc[0]['payment_count']
        
#         print(f"🎯 테스트 모드 - 분석 대상: 사용자 {user_count}명, 결제내역 {payment_count}건")
#         print(f"🎯 대상 유저 IDs: {TEST_USER_IDS}")
        
#         if user_count == 0:
#             raise Exception("테스트 유저가 없습니다")
        
#         # 다음 태스크로 전달
#         context['task_instance'].xcom_push(key='user_count', value=user_count)
#         context['task_instance'].xcom_push(key='payment_count', value=payment_count)
#         context['task_instance'].xcom_push(key='test_user_ids', value=TEST_USER_IDS)
        
#     finally:
#         conn.close()

# def prepare_persona_embeddings(**context):
#     """페르소나 임베딩 준비"""
#     embeddings = load_persona_embeddings()
    
#     print(f"🧠 페르소나 임베딩 준비 완료: {len(embeddings)}개")
    
#     # 다음 태스크로 전달
#     context['task_instance'].xcom_push(key='persona_embeddings', value=embeddings)

# def get_top3_from_summary(target_group: str, conn) -> pd.DataFrame:
#     """연령대+성별 그룹의 상위 3개 카테고리 조회"""
#     try:
#         cur = conn.cursor()
#         three_months_ago = datetime.now().date() - timedelta(days=90)
        
#         cur.execute("""
#             SELECT group_by_field, group_by_text, SUM(distinct_user_count) AS user_count
#             FROM user_action_summary
#             WHERE action_type = 'AGE_GENDER_CATEGORY'
#             AND group_by_field = %s
#             AND summary_date >= %s
#             GROUP BY group_by_field, group_by_text
#             ORDER BY user_count DESC
#             LIMIT 3
#         """, (target_group, three_months_ago))
        
#         rows = cur.fetchall()
#         return pd.DataFrame(rows, columns=['group_by_field', 'group_by_text', 'distinct_user_count'])
#     except Exception as e:
#         print(f"[DB ERROR] {e}")
#         return pd.DataFrame()

# def create_user_pattern_text(target_group, category_scores, total_amount, popular_categories):
#     """사용자 소비 패턴을 텍스트로 변환 (임베딩 생성용)"""
#     pattern_text = f"사용자 소비 패턴 ({target_group} 그룹): "
    
#     # 개인 소비 패턴
#     for category, score in category_scores.items():
#         ratio = score / total_amount
#         if ratio > 0.1:
#             pattern_text += f"{category} {ratio:.1%}, "
    
#     # 동일 그룹의 인기 카테고리 정보 추가
#     if not popular_categories.empty:
#         pattern_text += f"동일 그룹 인기 카테고리: "
#         for _, row in popular_categories.iterrows():
#             pattern_text += f"{row['group_by_text']}, "
    
#     # 추가 컨텍스트 (더 풍부한 임베딩을 위해)
#     top_category = max(category_scores.keys(), key=category_scores.get)
#     top_ratio = category_scores[top_category] / total_amount
#     pattern_text += f"주요 소비: {top_category} ({top_ratio:.1%})"
    
#     return pattern_text.strip(', ')

# def cosine_similarity(vec1, vec2):
#     """코사인 유사도 계산"""
#     vec1 = np.array(vec1)
#     vec2 = np.array(vec2)
#     return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

# def get_hashtag_for_persona(persona_id):
#     """페르소나별 해시태그 매핑 (hashtags 테이블 기준)"""
#     # payment_type_tag_id에 따른 hashtag_id 매핑
#     persona_to_hashtag = {
#         1: 2,   # 카페인 뱀파이어 → 카페인 러버 (hashtag_id=2)
#         2: 3,   # 빵지순례 → 빵덕후 (hashtag_id=3)  
#         3: 7,   # 버거왕 → 버거킹 (hashtag_id=7)
#         4: 5,   # 쩝쩝박사 → 맛있으면0칼로리 (hashtag_id=5)
#         5: 14,  # 핫플헌터 → 핫플정복 (hashtag_id=14)
#         6: 15,  # 와라편의점 → 편돌이 (hashtag_id=15)
#         7: 16,  # 감성수집가 → 감성충전 (hashtag_id=16)
#         8: 18   # 소비요정 → 득템 (hashtag_id=18)
#     }
    
#     hashtag_id = persona_to_hashtag.get(persona_id, 20)
    
#     hashtag_mapping = {
#         2: "카페인 러버", 
#         3: "빵덕후",
#         5: "맛있으면0칼로리",
#         7: "버거킹", 
#         14: "핫플정복",
#         15: "편돌이",
#         16: "감성충전",
#         18: "득템",
#         20: "MZ"
#     }
    
#     return hashtag_mapping.get(hashtag_id, "MZ")

# def analyze_user_behavior_profile(user_payments):
#     """사용자 세밀한 행동 패턴 분석"""
    
#     profile = {}
    
#     if user_payments.empty:
#         print("❌ 유저 결제 내역이 비어있음 - 기본 프로필 반환")
#         return profile
    
#     print(f"📊 행동 분석 시작 - 총 {len(user_payments)}건의 결제 내역")
    
#     # 1. 할인 민감도 분석
#     total_transactions = len(user_payments)
    
#     # 컬럼 존재 여부 확인
#     has_coupon = 'coupon_discount_amount' in user_payments.columns
#     has_membership = 'membership_discount_amount' in user_payments.columns
    
#     print(f"🔍 컬럼 확인 - coupon_discount_amount: {has_coupon}, membership_discount_amount: {has_membership}")
    
#     if has_coupon:
#         coupon_used = len(user_payments[user_payments['coupon_discount_amount'] > 0])
#         profile['coupon_usage_rate'] = coupon_used / total_transactions if total_transactions > 0 else 0
#         profile['avg_coupon_discount'] = user_payments['coupon_discount_amount'].mean()
#     else:
#         profile['coupon_usage_rate'] = 0
#         profile['avg_coupon_discount'] = 0
        
#     if has_membership:
#         membership_used = len(user_payments[user_payments['membership_discount_amount'] > 0])
#         profile['membership_usage_rate'] = membership_used / total_transactions if total_transactions > 0 else 0
#         profile['avg_membership_discount'] = user_payments['membership_discount_amount'].mean()
#     else:
#         profile['membership_usage_rate'] = 0
#         profile['avg_membership_discount'] = 0

#     # 2. 소비 시간대 분석
#     user_payments['hour'] = pd.to_datetime(user_payments['paid_at']).dt.hour
#     hour_counts = user_payments['hour'].value_counts()
    
#     if not hour_counts.empty:
#         peak_hour = hour_counts.index[0]
#         if 6 <= peak_hour <= 11:
#             profile['time_preference'] = '아침형'
#         elif 12 <= peak_hour <= 17:
#             profile['time_preference'] = '오후형' 
#         elif 18 <= peak_hour <= 22:
#             profile['time_preference'] = '저녁형'
#         else:
#             profile['time_preference'] = '야행성'
#     else:
#         profile['time_preference'] = '일반형'
    
#     # 3. 소비 규모 분석
#     avg_amount = user_payments['total_payment_amount'].mean()
#     high_amount_count = len(user_payments[user_payments['total_payment_amount'] > 50000])
    
#     profile['avg_payment'] = avg_amount
#     profile['high_amount_ratio'] = high_amount_count / total_transactions if total_transactions > 0 else 0
    
#     if avg_amount < 15000:
#         profile['spending_style'] = '가성비형'
#     elif avg_amount < 30000:
#         profile['spending_style'] = '적당형'
#     else:
#         profile['spending_style'] = '프리미엄형'
    
#     # 4. 멤버십 레벨 분석
#     if 'membership_code' in user_payments.columns:
#         membership_codes = user_payments['membership_code'].value_counts()
#         if not membership_codes.empty:
#             main_membership = membership_codes.index[0]
#             profile['membership_level'] = main_membership
#         else:
#             profile['membership_level'] = 'BASIC'
#     else:
#         profile['membership_level'] = 'BASIC'
    
#     # 5. 할인 추구 성향
#     total_discount = 0
#     total_original = 0
    
#     if has_coupon:
#         total_discount += user_payments['coupon_discount_amount'].sum()
#     if has_membership:
#         total_discount += user_payments['membership_discount_amount'].sum()
        
#     if 'original_amount' in user_payments.columns:
#         total_original = user_payments['original_amount'].sum()
#     else:
#         total_original = user_payments['total_payment_amount'].sum()  # 대체값 사용
    
#     if total_original > 0:
#         discount_rate = total_discount / total_original
#         if discount_rate > 0.15:
#             profile['discount_seeker'] = '할인헌터'
#         elif discount_rate > 0.05:
#             profile['discount_seeker'] = '혜택추구형'
#         else:
#             profile['discount_seeker'] = '무던형'
#     else:
#         profile['discount_seeker'] = '무던형'
    
#     print(f"✅ 행동 프로필 생성 완료: {profile}")
#     return profile

# def get_default_message(persona_name):
#     """LLM 실패시 기본 메시지"""
#     default_messages = {
#         "카페인 뱀파이어": "카페를 사랑하는 당신, 오늘도 좋은 하루 보내세요 ☕",
#         "빵지순례": "맛있는 빵을 좋아하는 당신, 달콤한 하루 되세요 🥐",
#         "버거왕": "든든한 식사를 즐기는 당신, 맛있는 하루 보내세요 🍔",
#         "쩝쩝박사": "맛집 탐험을 좋아하는 당신, 새로운 맛의 발견이 기다려요 🍽️",
#         "핫플헌터": "트렌드를 선도하는 당신, 멋진 하루 보내세요 🔥",
#         "와라편의점": "편리함을 추구하는 당신, 효율적인 하루 되세요 🏪",
#         "감성수집가": "문화를 사랑하는 당신, 감성 충전하는 하루 보내세요 🎨",
#         "소비요정": "라이프스타일을 중시하는 당신, 특별한 하루 되세요 ✨"
#     }
#     return default_messages.get(persona_name, "당신만의 특별함을 응원해요 🌟")

# def generate_personalized_message_with_llm(persona_name, age_group, gender, category_scores, total_amount, user_payments=None):
#     """LLM으로 개인화된 격려 메시지 생성 (강화 버전)"""

#     print(f"🤖 LLM 메시지 생성 시작 - 페르소나: {persona_name}, 연별대: {age_group}, 성별: {gender}")

#     try:
#         api_key = os.environ.get("OPENAI_API_KEY")
#         if not api_key:
#             print("❌ OPENAI_API_KEY 환경변수가 설정되지 않음")
#             return get_default_message(persona_name)

#         print(f"✅ OpenAI API 키 확인됨 (길이: {len(api_key)})")

#         consumption_pattern = ""
#         if category_scores and total_amount > 0:
#             top_category = max(category_scores.keys(), key=category_scores.get)
#             top_ratio = (category_scores[top_category] / total_amount * 100)

#             category_korean = {
#                 "CAFE": "카페",
#                 "BAKERY": "밥집",
#                 "FOOD": "음식점",
#                 "SHOPPING": "상점",
#                 "BEAUTY": "뷰티",
#                 "ACTIVITY": "애티비티",
#                 "LIFE": "해당 사항",
#                 "CULTURE": "민민한 문화"
#             }

#             main_category = category_korean.get(top_category, top_category)
#             consumption_pattern = f"{main_category} 소비 중심"
#             print(f"📊 주요 소비 패턴: {main_category} ({top_ratio:.1f}%)")

#         behavior_profile = analyze_user_behavior_profile(user_payments) if user_payments is not None else {}

#         trait_descriptions = []
#         if behavior_profile.get('discount_seeker'):
#             trait_descriptions.append("할인보다는 만족을 중요하게 생각하는")
#         if behavior_profile.get('time_preference') == '야행성':
#             trait_descriptions.append("밤 시간대에 소비가 집중되는")
#         if behavior_profile.get('spending_style') == '적당형':
#             trait_descriptions.append("균형 잡힌 소비 성향을 보이는")

#         behavior_text = " / ".join(trait_descriptions[:3]) or "뚜렷한 소비 성향 정보 없음"

#         print(f"🎯 행동 특성 내용: {behavior_text}")
#         print(f"🔍 기본 메시지: {get_default_message(persona_name)}")

#         # 최근 결제 이력 요약 추가 (최대 3개)
#         payment_summaries = []
#         if user_payments is not None:
#             recent_payments = user_payments.sort_values(by='paid_at', ascending=False).head(3)
#             for _, row in recent_payments.iterrows():
#                 date_str = pd.to_datetime(row['paid_at']).strftime('%m/%d')
#                 payment_summaries.append(f"- {row['place_category']} {int(row['total_payment_amount'])}원 ({date_str})")
#         payment_summary_text = "\n".join(payment_summaries)

#         prompt = f"""
# 다음은 사용자 분석 결과입니다. 이 정보를 참고하여 따뜻하고 감성적인 한 줄 메시지를 생성해주세요.

# 페르소나: {persona_name}
# 연령대: {age_group}
# 성별: {gender}
# 주요소비: {consumption_pattern}
# 행동 특성: {behavior_text}

# 최근 결제 이력:
# {payment_summary_text}

# 요구사항:
# 1. 50자 이내의 한 문장
# 2. 이모지 1개 포함
# 3. 마케팅 문구는 제외
# 4. 사용자의 소비 성향이 드러나야 함
# 5. 자연스럽고 격려하는 말투로 작성

# 메시지만 출력해주세요:
# """

#         print(f"📝 GPT 프롬프트 준비 완료 (길이: {len(prompt)})")

#         try:
#             client = openai.OpenAI(api_key=api_key)
#             print("✅ OpenAI 클라이언트 생성 성공")
#         except Exception as client_error:
#             print(f"❌ OpenAI 클라이언트 생성 실패: {client_error}")
#             return get_default_message(persona_name)

#         print("🚀 GPT API 호출 시작...")
#         response = client.chat.completions.create(
#             model="gpt-4o-mini",
#             messages=[{"role": "user", "content": prompt}],
#             temperature=0.8,
#             max_tokens=150
#         )

#         print("✅ GPT API 호출 성공")
#         generated_message = response.choices[0].message.content.strip()
#         print(f"✨ LLM 생성 메시지: {generated_message}")

#         return generated_message

#     except Exception as e:
#         print(f"❌ LLM 메시지 생성 실패: {e}")
#         import traceback
#         traceback.print_exc()
#         return get_default_message(persona_name)

# def get_recommend_message(persona_name, age_group, gender, category_scores, total_amount, user_payments=None):
#     print(f"💬 메시지 생성 시작 - 페르소나: {persona_name}")

#     if persona_name == "신규가입자":
#         return "아직 충분한 데이터가 없어 정확한 분석이 어려워요"

#     return generate_personalized_message_with_llm(
#         persona_name, age_group, gender,
#         category_scores, total_amount, user_payments
#     )

# def assign_default_persona(user):
#     """결제 내역 없는 사용자 기본 페르소나 + 기본 임베딩"""
#     age_group = get_age_group(user['birthdate'])
#     gender = user['gender'] or 'U'
    
#     print(f"🆕 신규 사용자 기본 페르소나 할당 - {age_group}_{gender}")
    
#     # 신규 사용자용 기본 임베딩 생성
#     default_text = f"신규 가입자 ({age_group}_{gender}). 아직 소비 패턴이 없는 사용자."
    
#     try:
#         client = openai.OpenAI(api_key=OPENAI_API_KEY)
#         response = client.embeddings.create(
#             model="text-embedding-3-small",
#             input=default_text
#         )
#         default_embedding = response.data[0].embedding
#         print("✅ 신규 사용자 임베딩 생성 성공")
#     except Exception as e:
#         print(f"❌ 신규 사용자 임베딩 생성 실패: {e}")
#         # 기본 임베딩 (영벡터)
#         default_embedding = [0.0] * 1536
    
#     default_message = '아직 결제 내역이 없어 분석할 수 없어요. 첫 결제 후 당신만의 소비 패턴을 분석해드릴게요!'
#     print(f"📝 기본 메시지 설정: '{default_message}'")
    
#     return {
#         'user_id': user['user_id'],
#         'age_group': age_group,
#         'gender': gender,
#         'payment_type_tag': '신규가입자',
#         'hashtag': 'MZ',
#         'recommend_message': default_message,
#         'vector_similarity': None,
#         'user_embedding': default_embedding
#     }

# def analyze_payment_with_rag(user, payments, analysis_date, month_weights, persona_embeddings):
#     """RAG 기반 페르소나 매칭 + 사용자 임베딩 생성 (hybrid scoring 개선 포함)"""

#     print(f"🔍 결제 분석 시작 - User {user['user_id']}")

#     # 결제 패턴 분석
#     category_scores = defaultdict(float)
#     total_amount = 0

#     for _, payment in payments.iterrows():
#         paid_date = pd.to_datetime(payment['paid_at'])
#         months_ago = (analysis_date.year - paid_date.year) * 12 + (analysis_date.month - paid_date.month)

#         if months_ago in month_weights:
#             weight = month_weights[months_ago]
#             amount = payment['total_payment_amount']
#             category = payment['place_category']

#             category_scores[category] += amount * weight
#             total_amount += amount * weight

#     print(f"💰 총 가중 결제액: {total_amount:,.0f}원")
#     print(f"📊 카테고리별 점수: {dict(category_scores)}")

#     if total_amount == 0:
#         print("❌ 가중 결제액이 0 - 기본 페르소나 할당")
#         return assign_default_persona(user)

#     # 연령대+성별 그룹 생성
#     age_group = get_age_group(user['birthdate'])
#     gender = user['gender'] or 'U'
#     target_group = f"{age_group}_{gender}"

#     print(f"👤 타겟 그룹: {target_group}")

#     # 동일 연령대+성별 그룹의 인기 카테고리 조회
#     conn = get_db_connection()
#     popular_categories = get_top3_from_summary(target_group, conn)
#     conn.close()

#     print(f"🔥 인기 카테고리: {len(popular_categories)}개")

#     # 사용자 소비 패턴 텍스트 생성 (임베딩용)
#     pattern_text = create_user_pattern_text(target_group, category_scores, total_amount, popular_categories)

#     print(f"📝 패턴 텍스트 생성 완료 (길이: {len(pattern_text)})")
#     print(f"🧾 패턴 텍스트 내용:\n{pattern_text}")

#     # 사용자 패턴을 임베딩으로 변환
#     try:
#         client = openai.OpenAI(api_key=OPENAI_API_KEY)
#         print("🔗 임베딩 API 호출 시작...")
#         response = client.embeddings.create(
#             model="text-embedding-3-small",
#             input=pattern_text
#         )
#         user_embedding = response.data[0].embedding
#         print("✅ 사용자 임베딩 생성 성공")
#     except Exception as e:
#         print(f"❌ 임베딩 생성 실패: {e}")
#         return assign_default_persona(user)

#     # 유사도 계산 + hybrid scoring
#     print(f"🎯 페르소나 매칭 시작 - {len(persona_embeddings)}개 페르소나 대상")
#     similarity_scores = []
#     user_top_category = max(category_scores, key=category_scores.get)

#     for persona_id, persona_data in persona_embeddings.items():
#         similarity = cosine_similarity(user_embedding, persona_data['embedding'])
#         persona_categories = persona_data.get("categories", [])  # e.g., ["CAFE", "LIFE"]
#         category_bonus = 1.0 if user_top_category in persona_categories else 0.0
#         final_score = 0.7 * similarity + 0.3 * category_bonus

#         similarity_scores.append((persona_id, final_score, similarity))
#         print(f"  - {persona_data['name']}: similarity={similarity:.4f}, final_score={final_score:.4f}")

#     # 상위 후보 출력
#     top_matches = sorted(similarity_scores, key=lambda x: -x[1])[:3]
#     print("🔥 유사도 상위 후보:")
#     for pid, final, sim in top_matches:
#         print(f"   - {persona_embeddings[pid]['name']}: score={final:.4f}, raw={sim:.4f}")

#     # 최종 선택
#     best_persona_id, best_score, best_similarity = max(similarity_scores, key=lambda x: x[1])
#     print(f"🏆 최적 매칭: {persona_embeddings[best_persona_id]['name']} (유사도: {best_similarity:.4f}, 종합점수: {best_score:.4f})")

#     # 메시지 생성
#     recommend_message = get_recommend_message(
#         persona_embeddings[best_persona_id]['name'], 
#         age_group, 
#         gender, 
#         category_scores, 
#         total_amount, 
#         payments
#     )

#     print(f"📨 최종 생성된 메시지: '{recommend_message}'")

#     result = {
#         'user_id': user['user_id'],
#         'age_group': age_group,
#         'gender': gender,
#         'payment_type_tag': persona_embeddings[best_persona_id]['name'],
#         'hashtag': get_hashtag_for_persona(best_persona_id),
#         'recommend_message': recommend_message,
#         'vector_similarity': best_similarity,
#         'user_embedding': user_embedding
#     }

#     print(f"🎁 analyze_payment_with_rag 최종 반환 결과:")
#     print(f"   - payment_type_tag: {result['payment_type_tag']}")
#     print(f"   - recommend_message: '{result['recommend_message']}'")

#     return result


# def process_user_embeddings(**context):
#     """사용자별 임베딩 생성 및 페르소나 매칭 (테스트 모드)"""
    
#     # 이전 태스크에서 데이터 가져오기
#     user_count = context['task_instance'].xcom_pull(key='user_count')
#     persona_embeddings = context['task_instance'].xcom_pull(key='persona_embeddings')
#     test_user_ids = context['task_instance'].xcom_pull(key='test_user_ids')
    
#     # 분석 기준월 설정
#     analysis_date = datetime.now().replace(day=1) - timedelta(days=1)
#     month_weights = {0: 1.0, 1: 0.7, 2: 0.5, 3: 0.3}
    
#     conn = get_db_connection()
#     try:
#         # 테스트 유저들만 로드
#         users_df = pd.read_sql(f"""
#             SELECT user_id, birthdate, gender, membership_code, created_at
#             FROM users
#             WHERE is_profile_complete = true
#             AND user_id IN ({','.join(map(str, test_user_ids))})
#         """, conn)
        
#         # 결제 내역 로드 (테스트 유저들만)
#         start_date = analysis_date - timedelta(days=120)
#         payments_df = pd.read_sql(f"""
#             SELECT user_id, place_category, total_payment_amount, paid_at
#             FROM user_histories
#             WHERE paid_at >= %s AND paid_at <= %s
#             AND user_id IN ({','.join(map(str, test_user_ids))})
#         """, conn, params=[start_date, analysis_date])
        
#         print(f"🎯 실제 처리할 유저: {len(users_df)}명")
#         print(f"🎯 유저별 결제 내역: {len(payments_df)}건")
        
#         # 사용자별 임베딩 생성
#         user_personas = []
        
#         for _, user in users_df.iterrows():
#             user_id = user['user_id']
#             user_payments = payments_df[payments_df['user_id'] == user_id]
            
#             print(f"\n👤 처리 중: User {user_id} (결제내역: {len(user_payments)}건)")
            
#             if user_payments.empty:
#                 print("📭 결제 내역 없음 - 기본 페르소나 할당")
#                 persona = assign_default_persona(user)
#             else:
#                 print("📊 결제 내역 있음 - RAG 분석 시작")
#                 persona = analyze_payment_with_rag(user, user_payments, analysis_date, month_weights, persona_embeddings)
            
#             print(f"✅ User {user_id} 처리 완료:")
#             print(f"   - 페르소나: {persona['payment_type_tag']}")
#             print(f"   - 메시지: '{persona['recommend_message']}'")
            
#             user_personas.append(persona)
        
#         # DB에 저장
#         save_personas_to_db(conn, user_personas)
        
#         print(f"\n✅ 테스트 모드 완료: {len(user_personas)}명 처리")
#         context['task_instance'].xcom_push(key='processed_count', value=len(user_personas))
        
#     finally:
#         conn.close()

# def save_personas_to_db(conn, personas):
#     """DB에 페르소나 결과 + 임베딩 저장 (UPSERT)"""
    
#     print(f"💾 DB 저장 시작 - {len(personas)}건")
    
#     cur = conn.cursor()
    
#     for i, persona in enumerate(personas):
#         print(f"💾 저장 중 [{i+1}/{len(personas)}]: User {persona['user_id']}")
#         print(f"   - 저장할 메시지: '{persona['recommend_message']}'")
        
#         cur.execute("""
#             INSERT INTO user_recommendation_profile 
#             (user_id, age_group, gender, payment_type_tag, hashtag, recommend_message, vector_similarity, user_embedding)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#             ON CONFLICT (user_id) DO UPDATE SET
#                 age_group = EXCLUDED.age_group,
#                 gender = EXCLUDED.gender,
#                 payment_type_tag = EXCLUDED.payment_type_tag,
#                 hashtag = EXCLUDED.hashtag,
#                 recommend_message = EXCLUDED.recommend_message,
#                 vector_similarity = EXCLUDED.vector_similarity,
#                 user_embedding = EXCLUDED.user_embedding,
#                 updated_at = NOW()
#         """, (
#             persona['user_id'],
#             persona['age_group'], 
#             persona['gender'],
#             persona['payment_type_tag'],
#             persona['hashtag'],
#             persona['recommend_message'],
#             persona['vector_similarity'],
#             persona['user_embedding']
#         ))
        
#         # 저장 후 즉시 확인
#         cur.execute("""
#             SELECT recommend_message FROM user_recommendation_profile 
#             WHERE user_id = %s
#         """, (persona['user_id'],))
        
#         saved_message = cur.fetchone()
#         if saved_message:
#             print(f"   ✅ DB에 저장된 메시지: '{saved_message[0]}'")
#             if saved_message[0] != persona['recommend_message']:
#                 print(f"   ⚠️  메시지 불일치 발견!")
#                 print(f"       원본: '{persona['recommend_message']}'")
#                 print(f"       저장: '{saved_message[0]}'")
#         else:
#             print(f"   ❌ 저장 확인 실패")
    
#     conn.commit()
#     print(f"💾 DB 저장 완료: {len(personas)}건 (임베딩 포함)")

# def validate_results(**context):
#     """결과 검증 및 품질 체크 (테스트 모드)"""
#     processed_count = context['task_instance'].xcom_pull(key='processed_count')
#     test_user_ids = context['task_instance'].xcom_pull(key='test_user_ids')
    
#     conn = get_db_connection()
#     try:
#         cur = conn.cursor()
        
#         # 테스트 유저들의 임베딩 저장 확인
#         cur.execute(f"""
#             SELECT 
#                 user_id,
#                 payment_type_tag,
#                 vector_similarity,
#                 CASE WHEN user_embedding IS NOT NULL THEN 'YES' ELSE 'NO' END as has_embedding
#             FROM user_recommendation_profile
#             WHERE user_id IN ({','.join(map(str, test_user_ids))})
#             ORDER BY user_id
#         """)
        
#         results = cur.fetchall()
        
#         print(f"🔍 테스트 결과 검증:")
#         print(f"  - 처리 요청: {len(test_user_ids)}명")
#         print(f"  - 실제 처리: {processed_count}명")
#         print(f"  - DB 저장됨: {len(results)}명")
        
#         print(f"\n📊 개별 결과:")
#         for user_id, persona, similarity, has_embedding in results:
#             similarity_str = f"{similarity:.3f}" if similarity else "N/A"
#             print(f"  User {user_id}: {persona} (유사도: {similarity_str}, 임베딩: {has_embedding})")
        
#         embedding_count = sum(1 for _, _, _, has_emb in results if has_emb == 'YES')
#         print(f"\n✅ 임베딩 생성률: {embedding_count}/{len(results)} ({embedding_count/len(results)*100:.1f}%)")
        
#     finally:
#         conn.close()

# def generate_persona_summary(**context):
#     """페르소나 분석 결과 요약 (전체 + 테스트)"""
#     test_user_ids = context['task_instance'].xcom_pull(key='test_user_ids')
    
#     conn = get_db_connection()
#     try:
#         # 전체 통계
#         summary_df = pd.read_sql("""
#             SELECT payment_type_tag, COUNT(*) as user_count,
#                    AVG(vector_similarity) as avg_similarity,
#                    COUNT(CASE WHEN user_embedding IS NOT NULL THEN 1 END) as embedding_count
#             FROM user_recommendation_profile
#             GROUP BY payment_type_tag
#             ORDER BY user_count DESC
#         """, conn)
        
#         print("📈 전체 페르소나 분포:")
#         for _, row in summary_df.iterrows():
#             similarity = f" (유사도: {row['avg_similarity']:.3f})" if row['avg_similarity'] else ""
#             embedding_info = f" [임베딩: {row['embedding_count']}개]"
#             print(f"  {row['payment_type_tag']}: {row['user_count']}명{similarity}{embedding_info}")
        
#         # 테스트 유저별 상세 정보
#         test_detail_df = pd.read_sql(f"""
#             SELECT user_id, payment_type_tag, hashtag, vector_similarity,
#                    CASE WHEN user_embedding IS NOT NULL THEN 'YES' ELSE 'NO' END as has_embedding
#             FROM user_recommendation_profile
#             WHERE user_id IN ({','.join(map(str, test_user_ids))})
#             ORDER BY user_id
#         """, conn)
        
#         print(f"\n🎯 테스트 유저 상세:")
#         for _, row in test_detail_df.iterrows():
#             similarity = f"{row['vector_similarity']:.3f}" if row['vector_similarity'] else "N/A"
#             print(f"  User {row['user_id']}: {row['payment_type_tag']} (#{row['hashtag']}, 유사도: {similarity}, 임베딩: {row['has_embedding']})")
            
#     finally:
#         conn.close()

# # DAG 정의 (테스트 모드 - 8명 전용)
# with DAG(
#     dag_id="analyze_user_personas_test_mode",
#     default_args=default_args,
#     description="RAG 기반 사용자 페르소나 분석 + 임베딩 저장 (테스트 8명 전용)",
#     start_date=datetime(2025, 8, 1),
#     schedule_interval=None,  # 수동 실행
#     catchup=False,
#     tags=["step3", "user", "persona", "embedding", "test", "8users"]
# ) as dag:
    
#     # 1단계: 전제 조건 검증
#     validate_task = PythonOperator(
#         task_id="validate_prerequisites",
#         python_callable=validate_prerequisites
#     )
    
#     # 2단계: 기초 데이터 로드 (테스트 모드)
#     load_data_task = PythonOperator(
#         task_id="load_test_data",
#         python_callable=load_base_data
#     )
    
#     # 3단계: 페르소나 임베딩 준비
#     prepare_embeddings_task = PythonOperator(
#         task_id="prepare_persona_embeddings",
#         python_callable=prepare_persona_embeddings
#     )
    
#     # 4단계: 테스트 유저 임베딩 처리
#     process_users_task = PythonOperator(
#         task_id="process_test_users",
#         python_callable=process_user_embeddings
#     )
    
#     # 5단계: 결과 검증 (테스트 모드)
#     validate_results_task = PythonOperator(
#         task_id="validate_test_results",
#         python_callable=validate_results
#     )
    
#     # 6단계: 요약 보고서
#     summary_task = PythonOperator(
#         task_id="generate_test_summary", 
#         python_callable=generate_persona_summary
#     )
    
#     # 태스크 의존성 정의
#     validate_task >> [load_data_task, prepare_embeddings_task]
#     [load_data_task, prepare_embeddings_task] >> process_users_task
#     process_users_task >> validate_results_task >> summary_task