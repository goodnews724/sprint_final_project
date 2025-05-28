"""
accounts_userquestionrecord 전처리
담당: 진우형
"""

import pandas as pd
import logging
import gc

logger = logging.getLogger(__name__)

def preprocess_userquestionrecord(df: pd.DataFrame) -> pd.DataFrame:
    """userquestionrecord 전처리: 자기 투표를 '자기 사랑' 플래그로 처리 (개선 버전)"""

    logger.info("🔧 진우형: accounts_userquestionrecord 전처리 시작...")
    
    try:
        original_count = len(df)
        
        # 데이터 검증
        required_columns = ['user_id', 'chosen_user_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"필수 컬럼 누락: {missing_columns}")

        # 자기 자신 투표 플래그 생성
        df['is_self_love'] = df['user_id'] == df['chosen_user_id']

        self_vote_count = df['is_self_love'].sum()
        self_vote_rate = self_vote_count / len(df) * 100
        
        logger.info(f"✅ 진우형: 자기 사랑 플래그 생성 완료")
        logger.info(f"   자기 투표: {self_vote_count:,}건 ({self_vote_rate:.3f}%)")
        logger.info(f"   총 투표 데이터: {len(df):,}건")
        
        # 결과 검증
        if len(df) == 0:
            raise ValueError("전처리 후 데이터가 비어있습니다")
        
        # 메모리 정리
        gc.collect()

        return df
        
    except Exception as e:
        logger.error(f"❌ 진우형: userquestionrecord 전처리 실패 - {str(e)}")
        gc.collect()
        raise

# 사용 예시
if __name__ == "__main__":
    from load_data import load_table
    
    try:
        df = load_table('accounts_userquestionrecord', 'votes')
        df_processed = preprocess_userquestionrecord(df)
        print(f"전처리 완료: {len(df_processed):,}행")
    except Exception as e:
        print(f"테스트 실패: {e}")