"""
데이터 저장 함수
담당: 김호재
"""

import pandas as pd
import os
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)

def save_to_gcs(df: pd.DataFrame, table_name: str, dataset: str = 'processed') -> dict:
    """전처리된 데이터를 GCS에 parquet으로 저장 (개선 버전)"""
    
    if df.empty:
        raise ValueError(f"빈 DataFrame을 저장할 수 없습니다: {table_name}")

    # 전처리된 데이터임을 명확히 표시
    processed_table_name = f"{table_name}_processed"
    bucket_name = 'sprintda05_final_project'
    blob_path = f"{dataset}/{processed_table_name}.parquet"
    gcs_path = f"gs://{bucket_name}/{blob_path}"
    
    try:
        logger.info(f"💾 {processed_table_name} 저장 시작...")
        logger.info(f"   저장할 데이터: {len(df):,}행, {df.shape[1]}열")
        
        # 로컬에 임시 저장 - 압축 옵션 추가
        local_path = f"/tmp/{processed_table_name}_{os.getpid()}.parquet"
        df.to_parquet(
            local_path, 
            engine='pyarrow', 
            index=False,
            compression='snappy'  # 압축으로 파일 크기 최적화
        )
        
        # 파일 크기 확인
        file_size_mb = os.path.getsize(local_path) / 1024**2
        logger.info(f"   파일 크기: {file_size_mb:.1f}MB")
        
        # GCS에 업로드
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        blob.upload_from_filename(local_path, timeout=300)  # 5분 타임아웃
        
        # 업로드 검증
        blob.reload()
        if not blob.exists():
            raise RuntimeError("GCS 업로드 검증 실패")
        
        result = {
            'table_name': processed_table_name,
            'rows': len(df),
            'columns': df.shape[1],
            'file_size_mb': round(file_size_mb, 1),
            'gcs_path': gcs_path
        }
        
        logger.info(f"✅ {processed_table_name} 저장 완료")
        logger.info(f"   저장 경로: {gcs_path}")
        
        return result

    except MemoryError:
        raise MemoryError(f"메모리 부족으로 {table_name} 저장 실패")
    except Exception as e:
        logger.error(f"❌ {processed_table_name} 저장 실패: {str(e)}")
        raise
    finally:
        # 안전한 임시 파일 정리
        if 'local_path' in locals() and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.debug(f"임시 파일 정리: {local_path}")
            except OSError as e:
                logger.warning(f"임시 파일 정리 실패: {e}")

# 사용 예시
if __name__ == "__main__":
    # 예시 데이터프레임 저장
    df = pd.DataFrame({'test': [1, 2, 3]})
    try:
        result = save_to_gcs(df, 'test_table', 'processed')
        print(f"저장 완료: {result}")
    except Exception as e:
        print(f"테스트 실패: {e}")