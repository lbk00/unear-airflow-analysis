from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import psycopg2
import pandas as pd
import json
import boto3
import openai

# 설정
OUTPUT_PATH = "/opt/airflow/output/partner_metadata_llm.jsonl"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
S3_BUCKET = os.environ["S3_BUCKET_NAME"]

default_args = {
    'owner': 'unear_1seyoung',  # 본인 이름으로 변경
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DB 연결 함수
def get_db_connection():
    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_NAME'],
    )

# 해시태그 매핑 로드
def load_hashtags_mapping():
    """DB에서 해시태그 매핑 로드"""
    conn = get_db_connection()
    try:
        hashtags_df = pd.read_sql("SELECT hashtag_id, name FROM hashtags", conn)
        return {row['name']: row['hashtag_id'] for _, row in hashtags_df.iterrows()}
    finally:
        conn.close()

# 마지막 ID 관리
def get_last_processed_id():
    return 0  # 항상 처음부터 전체 처리

def save_last_processed_id(new_id):
    os.makedirs(os.path.dirname(DEFAULT_LAST_ID_PATH), exist_ok=True)
    with open(DEFAULT_LAST_ID_PATH, "w") as f:
        f.write(str(new_id))

# 규칙 기반 태그 매칭
def match_tags_by_rules(place_info: dict, hashtag_mapping: dict) -> list:
    """카테고리와 키워드 기반 태그 매칭"""
    matched_tags = []
    
    category = str(place_info['category_code']).lower()
    place_name = str(place_info['place_name']).lower()
    franchise_name = str(place_info.get('franchise_name', '') or '').lower()
    desc = str(place_info['place_desc'] or '').lower()
    benefit = str(place_info['benefit_category'] or '').lower()
    
    # 프랜차이즈별 특별 매핑
    if 'starbucks' in franchise_name or '스타벅스' in franchise_name:
        matched_tags.extend(['카페인 러버', '감성한잔'])
    elif any(name in franchise_name for name in ['이디야', '메가커피', '컴포즈']):
        matched_tags.extend(['카페인 러버', '감성한잔'])
    elif any(name in franchise_name for name in ['파리바게뜨', '뚜레쥬르', '브레댄코']):
        matched_tags.extend(['빵덕후', '밥대신빵'])
    elif any(name in franchise_name for name in ['맥도날드', '굽네치킨']):
        matched_tags.extend(['햄최몇', '완전식품'])
    elif any(name in franchise_name for name in ['cgv', '롯데시네마', '메가박스']):
        matched_tags.extend(['문화소비', '데이트코스'])
    elif any(name in franchise_name for name in ['cu', 'gs25', 'gs 25']):
        matched_tags.append('편돌이')
    elif any(name in franchise_name for name in ['gs the fresh']):
        matched_tags.extend(['편돌이', 'MZ'])
    
    # 카테고리별 매핑 규칙
    if category == 'cafe':
        matched_tags.extend(['감성한잔', '카페인 러버'])
    elif category == 'bakery':
        matched_tags.extend(['빵덕후', '밥대신빵'])
    elif category == 'food':
        matched_tags.extend(['맛있으면0칼로리', '식후아아'])
    elif category == 'life':
        matched_tags.append('편돌이')
    elif category == 'activity':
        matched_tags.extend(['일단나와', '데이트코스'])
    elif category == 'culture':
        matched_tags.extend(['문화소비', '감성충전'])
    elif category == 'shopping':
        matched_tags.extend(['득템', '지름신'])
    elif category == 'beauty':
        matched_tags.extend(['득템', 'MZ'])
    
    # 중복 제거하고 해시태그 매핑에 있는 것만 반환
    unique_tags = list(dict.fromkeys(matched_tags))
    valid_tags = [tag for tag in unique_tags if tag in hashtag_mapping]
    
    return valid_tags[:5]

# GPT 백업 태깅 (규칙 매칭이 실패했을 때만)
def generate_tags_with_gpt(place_info: dict, hashtag_mapping: dict) -> list:
    """GPT로 태그 생성 (fallback)"""
    available_tags = list(hashtag_mapping.keys())
    
    prompt = f"""
다음 장소 정보를 분석하여 어울리는 해시태그를 선택해주세요.

사용 가능한 해시태그: {available_tags}

[예시]
장소: 스타벅스 강남점, 카테고리: CAFE
→ ["카페인 러버", "감성한잔"]

장소: 파리바게뜨 홍대점, 카테고리: BAKERY
→ ["빵덕후", "밥대신빵"]

장소: CGV 용산점, 카테고리: CULTURE
→ ["문화소비", "데이트코스"]

[분석할 장소]
장소명: {place_info['place_name']}
카테고리: {place_info['category_code']}
설명: {place_info['place_desc']}

위 예시처럼 JSON 배열 형태로만 반환:
"""
    
    try:
        print(f"[🤖] GPT 호출 시작: {place_info['place_name']}")
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=300
        )
        result = response.choices[0].message.content.strip()
        print(f"[🤖] GPT 응답: {result}")
        
        # JSON 파싱 시도
        try:
            tags = json.loads(result)
            if isinstance(tags, list):
                print(f"[✅] GPT 태그 파싱 성공: {tags}")
                return [tag for tag in tags if tag in hashtag_mapping][:5]
        except json.JSONDecodeError:
            print(f"[⚠️] JSON 파싱 실패: {result}")
            
        # 파싱 실패시 패턴 매칭으로 태그 추출
        import re
        found_tags = []
        for tag in hashtag_mapping.keys():
            if tag in result:
                found_tags.append(tag)
        return found_tags[:5] if found_tags else []
        
    except Exception as e:
        print(f"[❌] GPT 태깅 실패: {e}")
        return []

# 메인 처리 함수
def create_metadata_and_upload(**context):
    """메타데이터 생성 및 S3 업로드"""
    
    # 초기화
    last_id = get_last_processed_id()
    hashtag_mapping = load_hashtags_mapping()
    
    # 데이터 추출
    conn = get_db_connection()
    try:
        query = f"""
            SELECT p.place_id, p.place_name, p.place_desc, p.category_code, 
                   p.address, p.benefit_category, p.latitude, p.longitude,
                   p.franchise_id, f.name as franchise_name
            FROM places p
            LEFT JOIN franchises f ON p.franchise_id = f.franchise_id
            WHERE p.is_deleted = false AND p.place_id > {last_id}
            ORDER BY p.place_id ASC
        """
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    
    if df.empty:
        print("🚫 신규 장소 없음")
        return
    
    # 메타데이터 생성
    records = []
    gpt_usage_count = 0
    
    for _, row in df.iterrows():
        place_info = row.to_dict()
        
        # 프랜차이즈 vs 로컬 구분 처리
        if row['franchise_id']:  # 프랜차이즈
            tags = match_tags_by_rules(place_info, hashtag_mapping)
        else:  # 로컬 매장 - GPT 사용
            tags = generate_tags_with_gpt(place_info, hashtag_mapping)
            gpt_usage_count += 1
        
        # 여전히 없으면 카테고리 기반 기본값
        if not tags:
            if row['category_code'] == 'CAFE':
                tags = ["카페인 러버"]
            elif row['category_code'] == 'FOOD':
                tags = ["맛있으면0칼로리"]
            elif row['category_code'] == 'SHOPPING':
                tags = ["득템"]
            else:
                tags = ["MZ"]
        
        # 메타데이터 구성
        metadata = {
            "id": f"place_{row['place_id']}",
            "place_id": int(row['place_id']),
            "place_name": row['place_name'],
            "franchise_id": int(row['franchise_id']) if pd.notna(row['franchise_id']) else None,
            "franchise_name": row['franchise_name'],
            "description": row['place_desc'],
            "category": row['category_code'],
            "benefit": row['benefit_category'],
            "address": row['address'],
            "latitude": float(row['latitude']) if row['latitude'] else None,
            "longitude": float(row['longitude']) if row['longitude'] else None,
            "tags": tags,
            "tag_count": len(tags),
            "processing_method": "gpt",
            "is_franchise": bool(row['franchise_id'])
        }
        records.append(metadata)
    
    # 파일 저장
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    
    # S3 업로드
    s3 = boto3.client('s3')
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"rag/partner_metadata_{timestamp}.jsonl"
    s3.upload_file(OUTPUT_PATH, S3_BUCKET, s3_key)
    
    # 상태 업데이트
    max_id = df["place_id"].max()
    save_last_processed_id(max_id)
    
    # 결과 출력
    print(f"✅ 처리 완료: {len(records)}건")
    print(f"📊 GPT 사용: {gpt_usage_count}건")
    print(f"💾 S3 업로드: {s3_key}")
    print(f"🆔 최신 place_id: {max_id}")

# DAG 정의
with DAG(
    dag_id="generate_partner_metadata_optimized",
    default_args=default_args,
    description="제휴처 메타데이터 생성 (규칙+GPT 혼합)",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["step1", "metadata", "partner", "optimized"]
) as dag:
    
    generate_metadata = PythonOperator(
        task_id="generate_partner_metadata",
        python_callable=create_metadata_and_upload
    )
