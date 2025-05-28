"""
데이터 로드 함수
담당: 김재문
"""

import pandas as pd
import os
import logging
from google.cloud import storage
from google.oauth2 import service_account

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_gcs_auth():
    """GCS 인증 설정"""
    try:
        # 환경변수에서 서비스 계정 키 경로 확인
        cred_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if cred_path and os.path.exists(cred_path):
            logger.info("✅ 환경변수에서 GCS 인증 정보 확인")
            return
        
        # Airflow Connection에서 인증 정보 찾기 (선택사항)
        # 실제 배포 시에는 환경변수 설정 필요
        logger.warning("⚠️ GOOGLE_APPLICATION_CREDENTIALS 환경변수가 설정되지 않음")
        logger.info("💡 다음 명령어로 인증 설정:")
        logger.info("   export GOOGLE_APPLICATION_CREDENTIALS='path/to/service-account-key.json'")
        
    except Exception as e:
        logger.error(f"❌ GCS 인증 설정 실패: {e}")
        raise

def load_table(table_name: str, dataset: str = 'votes') -> pd.DataFrame:
    """GCS에서 테이블 로드 (개선 버전)"""
    
    setup_gcs_auth()
    
    # GCS 경로
    bucket_name = 'sprintda05_final_project'
    blob_path = f"{dataset}/{table_name}.parquet"
    gcs_path = f"gs://{bucket_name}/{blob_path}"
    
    try:
        logger.info(f"📥 {table_name} 로드 시작...")
        logger.info(f"   경로: {gcs_path}")
        
        # 메모리 사용량 체크
        import psutil
        memory_before = psutil.virtual_memory().percent
        logger.info(f"   메모리 사용률 (로드 전): {memory_before:.1f}%")
        
        if memory_before > 85:
            raise MemoryError(f"메모리 부족 위험: {memory_before:.1f}% 사용 중")
        
        # 파일 존재 확인
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        if not blob.exists():
            raise FileNotFoundError(f"GCS 파일을 찾을 수 없습니다: {gcs_path}")
        
        # 파일 크기 확인
        blob.reload()
        file_size_mb = blob.size / 1024**2
        logger.info(f"   파일 크기: {file_size_mb:.1f}MB")
        
        if file_size_mb > 1000:  # 1GB 이상이면 경고
            logger.warning(f"⚠️ 큰 파일 감지: {file_size_mb:.1f}MB - 로딩에 시간이 걸릴 수 있습니다")
        
        # pandas로 직접 로드 (gcsfs 사용)
        try:
            df = pd.read_parquet(gcs_path, engine='pyarrow')
        except ImportError:
            # gcsfs가 없으면 로컬 다운로드 후 로드
            logger.info("   gcsfs 미설치 - 로컬 다운로드 방식 사용")
            local_path = f"/tmp/{table_name}_{dataset}_{os.getpid()}.parquet"
            
            try:
                blob.download_to_filename(local_path)
                df = pd.read_parquet(local_path, engine='pyarrow')
            finally:
                if os.path.exists(local_path):
                    os.remove(local_path)
        
        memory_after = psutil.virtual_memory().percent
        logger.info(f"✅ {table_name} 로드 완료: {df.shape[0]:,}행, {df.shape[1]}열")
        logger.info(f"   메모리 사용률 (로드 후): {memory_after:.1f}%")
        logger.info(f"   메모리 증가: +{memory_after - memory_before:.1f}%")
        
        return df
        
    except FileNotFoundError as e:
        logger.error(f"❌ 파일 없음: {e}")
        raise
    except MemoryError as e:
        logger.error(f"❌ 메모리 부족: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ {table_name} 로드 실패: {e}")
        raise

# 사용 예시
if __name__ == "__main__":
    # 테스트 실행
    try:
        df = load_table('accounts_user', 'votes')
        print(f"테스트 성공: {len(df):,}행 로드됨")
    except Exception as e:
        print(f"테스트 실패: {e}")