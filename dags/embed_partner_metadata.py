# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import os
# import psycopg2
# import json
# import boto3
# import openai
# import numpy as np

# # 설정
# OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
# S3_BUCKET = os.environ["S3_BUCKET_NAME"]

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

# def create_embedding_table():
#     """임베딩 테이블 생성"""
#     conn = get_db_connection()
#     try:
#         cur = conn.cursor()
        
#         # pgvector 확장 생성
#         cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        
#         # 테이블 생성
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS place_embeddings (
#                 place_id INTEGER PRIMARY KEY,
#                 embedding vector(1536),
#                 created_at TIMESTAMP DEFAULT NOW(),
#                 FOREIGN KEY (place_id) REFERENCES places(place_id)
#             );
#         """)
        
#         conn.commit()
#         print("✅ place_embeddings 테이블 준비 완료")
        
#     finally:
#         conn.close()

# def get_latest_s3_metadata():
#     """S3에서 최신 메타데이터 파일 가져오기"""
#     s3 = boto3.client('s3')
    
#     response = s3.list_objects_v2(
#         Bucket=S3_BUCKET,
#         Prefix='rag/partner_metadata_'
#     )
    
#     if 'Contents' not in response:
#         raise Exception("S3에 메타데이터 파일이 없습니다")
    
#     # 최신 파일 선택
#     latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
#     s3_key = latest_file['Key']
    
#     print(f"📥 S3 파일: {s3_key}")
    
#     # 파일 다운로드
#     local_path = f"/tmp/{s3_key.split('/')[-1]}"
#     s3.download_file(S3_BUCKET, s3_key, local_path)
    
#     return local_path

# def generate_embeddings(**context):
#     """메타데이터에서 임베딩 생성 및 저장"""
    
#     # S3에서 메타데이터 다운로드
#     metadata_file = get_latest_s3_metadata()
    
#     # OpenAI 클라이언트 초기화
#     client = openai.OpenAI(api_key=OPENAI_API_KEY)
    
#     # 메타데이터 읽기
#     metadata_list = []
#     with open(metadata_file, 'r', encoding='utf-8') as f:
#         for line in f:
#             metadata_list.append(json.loads(line.strip()))
    
#     print(f"📊 처리할 메타데이터: {len(metadata_list)}개")
    
#     # DB 연결
#     conn = get_db_connection()
#     try:
#         cur = conn.cursor()
        
#         # 기존 임베딩 삭제 (전체 재생성)
#         cur.execute("DELETE FROM place_embeddings")
        
#         processed_count = 0
#         batch_size = 10  # 배치 처리
        
#         for i in range(0, len(metadata_list), batch_size):
#             batch = metadata_list[i:i+batch_size]
            
#             # 임베딩할 텍스트 준비
#             texts = []
#             place_ids = []
            
#             for metadata in batch:
#                 # 임베딩용 텍스트 생성
#                 text = f"{metadata['place_name']}. {metadata['category']} 카테고리. "
#                 if metadata['franchise_name']:
#                     text += f"{metadata['franchise_name']} 프랜차이즈. "
#                 if metadata['description']:
#                     text += f"{metadata['description']}. "
#                 if metadata['tags']:
#                     text += f"특징: {', '.join(metadata['tags'])}"
                
#                 texts.append(text)
#                 place_ids.append(metadata['place_id'])
            
#             # OpenAI 임베딩 생성
#             response = client.embeddings.create(
#                 model="text-embedding-3-small",
#                 input=texts
#             )
            
#             # 임베딩 결과 저장
#             for j, embedding_data in enumerate(response.data):
#                 place_id = place_ids[j]
#                 embedding = embedding_data.embedding
                
#                 # PostgreSQL에 저장
#                 cur.execute(
#                     "INSERT INTO place_embeddings (place_id, embedding) VALUES (%s, %s)",
#                     (place_id, embedding)
#                 )
                
#                 processed_count += 1
            
#             conn.commit()
#             print(f"✅ 배치 {i//batch_size + 1} 완료 ({len(batch)}개)")
        
#         print(f"🎯 임베딩 완료: {processed_count}개")
        
#         # 통계 확인
#         cur.execute("SELECT COUNT(*) FROM place_embeddings")
#         total_count = cur.fetchone()[0]
#         print(f"📈 DB 저장된 임베딩: {total_count}개")
        
#     finally:
#         conn.close()

# def test_vector_search(**context):
#     """벡터 검색 테스트"""
#     conn = get_db_connection()
#     try:
#         cur = conn.cursor()
        
#         # 샘플 검색
#         cur.execute("""
#             SELECT p.place_name, p.category_code, 
#                    pe.embedding <-> '[0.1,0.2,0.3,...]'::vector AS distance
#             FROM place_embeddings pe
#             JOIN places p ON pe.place_id = p.place_id
#             ORDER BY distance
#             LIMIT 5
#         """)
        
#         results = cur.fetchall()
#         print("🔍 벡터 검색 테스트 완료")
#         for result in results:
#             print(f"  - {result[0]} ({result[1]})")
            
#     except Exception as e:
#         print(f"⚠️ 벡터 검색 테스트 실패: {e}")
#     finally:
#         conn.close()

# # DAG 정의
# with DAG(
#     dag_id="embed_partner_metadata",
#     default_args=default_args,
#     description="제휴처 메타데이터 벡터 임베딩",
#     start_date=datetime(2025, 8, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["step2", "embedding", "vector", "partner"]
# ) as dag:
    
#     create_table_task = PythonOperator(
#         task_id="create_embedding_table",
#         python_callable=create_embedding_table
#     )
    
#     generate_embeddings_task = PythonOperator(
#         task_id="generate_embeddings",
#         python_callable=generate_embeddings
#     )
    
#     test_search_task = PythonOperator(
#         task_id="test_vector_search",
#         python_callable=test_vector_search
#     )
    
#     create_table_task >> generate_embeddings_task >> test_search_task
