"""
hackle_events 전처리
담당: 조수진
"""

import pandas as pd
import logging
import gc

logger = logging.getLogger(__name__)

def preprocess_hackle_events(df: pd.DataFrame) -> pd.DataFrame:
    """hackle_events 전처리: 중복 이벤트 제거 (개선 버전)"""

    logger.info("🔧 조수진: hackle_events 전처리 시작...")
    
    try:
        original_count = len(df)
        
        # 데이터 검증
        required_columns = ['session_id', 'event_datetime', 'event_key']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"필수 컬럼 누락: {missing_columns}")

        # 중복 제거 전 분석
        logger.info(f"   중복 검사 기준: {required_columns}")
        duplicate_check = df.duplicated(subset=required_columns)
        duplicate_count = duplicate_check.sum()
        logger.info(f"   발견된 중복: {duplicate_count:,}건")

        # 중복 제거 (session_id, event_datetime, event_key 기준)
        before_count = len(df)
        df_clean = df.drop_duplicates(subset=required_columns, keep='first')
        after_count = len(df_clean)

        removed = before_count - after_count
        removal_rate = removed / before_count * 100
        
        logger.info(f"✅ 조수진: 중복 제거 완료")
        logger.info(f"   제거된 데이터: {removed:,}건 ({removal_rate:.2f}%)")
        logger.info(f"   최종 데이터: {after_count:,}건")
        
        # 결과 검증
        if len(df_clean) == 0:
            raise ValueError("전처리 후 데이터가 비어있습니다")
        
        # 메모리 정리
        del df
        gc.collect()

        return df_clean
        
    except Exception as e:
        logger.error(f"❌ 조수진: hackle_events 전처리 실패 - {str(e)}")
        gc.collect()
        raise

# 사용 예시
if __name__ == "__main__":
    from load_data import load_table
    
    try:
        df = load_table('hackle_events', 'hackle')
        df_processed = preprocess_hackle_events(df)
        print(f"전처리 완료: {len(df_processed):,}행")
    except Exception as e:
        print(f"테스트 실패: {e}")