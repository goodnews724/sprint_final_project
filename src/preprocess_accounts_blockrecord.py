"""
accounts_blockrecord 전처리
담당: 이준희
"""

import pandas as pd
import logging
import gc

logger = logging.getLogger(__name__)

def preprocess_blockrecord(df: pd.DataFrame) -> pd.DataFrame:
    """blockrecord 전처리: 자기 자신 차단 제거 (개선 버전)"""

    logger.info("🔧 이준희: accounts_blockrecord 전처리 시작...")
    
    try:
        original_count = len(df)
        
        # 데이터 검증
        required_columns = ['user_id', 'block_user_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"필수 컬럼 누락: {missing_columns}")

        # 자기 자신 차단 식별 및 제거
        self_blocks = df['user_id'] == df['block_user_id']
        self_block_count = self_blocks.sum()

        df_clean = df[~self_blocks].copy()
        
        removal_rate = self_block_count / original_count * 100 if original_count > 0 else 0

        logger.info(f"✅ 이준희: 자기 자신 차단 제거 완료")
        logger.info(f"   제거된 데이터: {self_block_count:,}건 ({removal_rate:.2f}%)")
        logger.info(f"   최종 데이터: {len(df_clean):,}건")
        
        # 결과 검증
        if len(df_clean) == 0 and original_count > 0:
            raise ValueError("전처리 후 데이터가 비어있습니다")
        
        # 메모리 정리
        del df
        gc.collect()

        return df_clean
        
    except Exception as e:
        logger.error(f"❌ 이준희: blockrecord 전처리 실패 - {str(e)}")
        gc.collect()
        raise

# 사용 예시
if __name__ == "__main__":
    from load_data import load_table
    
    try:
        df = load_table('accounts_blockrecord', 'votes')
        df_processed = preprocess_blockrecord(df)
        print(f"전처리 완료: {len(df_processed):,}행")
    except Exception as e:
        print(f"테스트 실패: {e}")