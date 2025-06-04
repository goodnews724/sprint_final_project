"""
hackle_events 전처리
담당: 조수진
"""

import pandas as pd
import logging
import gc

logger = logging.getLogger(__name__)

def preprocess_hackle_events(df: pd.DataFrame) -> pd.DataFrame:
    """hackle_events 전처리: 중복 이벤트 제거 + 불필요 이벤트 삭제 (개선 버전)"""

    logger.info("🔧 조수진: hackle_events 전처리 시작...")
    
    try:
        original_count = len(df)
        
        # 데이터 검증
        required_columns = ['session_id', 'event_datetime', 'event_key']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"필수 컬럼 누락: {missing_columns}")

        # 1. 불필요한 이벤트 삭제
        exclude_events = ['button', 'click_appbar_setting']
        before_filter_count = len(df)
        
        # 삭제할 이벤트가 몇 개나 있는지 확인
        excluded_counts = {}
        for event in exclude_events:
            count = (df['event_key'] == event).sum()
            excluded_counts[event] = count
            logger.info(f"   삭제 대상 '{event}': {count:,}건")
        
        # 필터링 실행
        df_filtered = df[~df['event_key'].isin(exclude_events)].copy()
        after_filter_count = len(df_filtered)
        
        filtered_out = before_filter_count - after_filter_count
        logger.info(f"   이벤트 필터링: {filtered_out:,}건 삭제 ({filtered_out/before_filter_count*100:.2f}%)")

        # 2. 중복 제거 전 분석
        logger.info(f"   중복 검사 기준: {required_columns}")
        duplicate_check = df_filtered.duplicated(subset=required_columns)
        duplicate_count = duplicate_check.sum()
        logger.info(f"   발견된 중복: {duplicate_count:,}건")

        # 3. 중복 제거 (session_id, event_datetime, event_key 기준)
        before_count = len(df_filtered)
        df_clean = df_filtered.drop_duplicates(subset=required_columns, keep='first')
        after_count = len(df_clean)

        removed = before_count - after_count
        removal_rate = removed / before_count * 100 if before_count > 0 else 0
        
        logger.info(f"✅ 조수진: 전처리 완료")
        logger.info(f"   원본 데이터: {original_count:,}건")
        logger.info(f"   이벤트 필터링 후: {after_filter_count:,}건")
        logger.info(f"   중복 제거 후: {after_count:,}건")
        logger.info(f"   총 제거율: {(original_count-after_count)/original_count*100:.2f}%")
        
        # 결과 검증
        if len(df_clean) == 0:
            raise ValueError("전처리 후 데이터가 비어있습니다")
        
        # 최종 이벤트 종류 확인
        final_events = df_clean['event_key'].value_counts()
        logger.info(f"   최종 이벤트 종류: {len(final_events)}개")
        logger.info(f"   상위 5개 이벤트: {final_events.head().to_dict()}")
        
        # 메모리 정리
        del df, df_filtered
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
        
        # 이벤트 종류 확인
        print("\n이벤트 종류별 개수:")
        print(df_processed['event_key'].value_counts().head(10))
        
    except Exception as e:
        print(f"테스트 실패: {e}")