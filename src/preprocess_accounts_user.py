"""
accounts_user 전처리
담당: 천지현
"""

import pandas as pd
import ast
import logging
import gc

logger = logging.getLogger(__name__)

def preprocess_accounts_user(df: pd.DataFrame) -> pd.DataFrame:
    """accounts_user 전처리: 포인트/친구수 기반 specialist 분류 (개선 버전)"""

    logger.info("🔧 천지현: accounts_user 전처리 시작...")
    
    try:
        original_count = len(df)
        
        # 데이터 검증
        required_columns = ['friend_id_list', 'point']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"필수 컬럼 누락: {missing_columns}")
        
        # 친구 리스트 파싱 - 에러 핸들링 강화
        def parse_list(x):
            if pd.isna(x):
                return []
            try:
                if isinstance(x, str):
                    if x.strip() == '' or x.strip() == '[]':
                        return []
                    return ast.literal_eval(x)
                elif isinstance(x, list):
                    return x
                else:
                    return []
            except (ValueError, SyntaxError):
                logger.warning(f"친구 리스트 파싱 실패: {x} -> 빈 리스트로 처리")
                return []

        logger.info("   친구 수 계산 중...")
        df['friend_count'] = df['friend_id_list'].apply(parse_list).apply(len)
        
        # 기본 통계 정보
        logger.info(f"   포인트 통계: min={df['point'].min()}, max={df['point'].max()}, mean={df['point'].mean():.1f}")
        logger.info(f"   친구수 통계: min={df['friend_count'].min()}, max={df['friend_count'].max()}, mean={df['friend_count'].mean():.1f}")

        # Specialist 분류 기준 설정
        # 1. 포인트 기준: Q3 + 3*IQR 초과하는 사용자
        Q1_point = df['point'].quantile(0.25)
        Q3_point = df['point'].quantile(0.75)
        IQR_point = Q3_point - Q1_point
        point_specialist_threshold = Q3_point + 3 * IQR_point

        # 2. 친구수 기준: 상위 1% 사용자
        friend_specialist_threshold = df['friend_count'].quantile(0.99)
        
        logger.info(f"   Specialist 임계값:")
        logger.info(f"      포인트 >= {point_specialist_threshold:.1f}")
        logger.info(f"      친구수 >= {friend_specialist_threshold:.1f}")

        # Specialist 분류 컬럼 생성
        df['is_point_specialist'] = df['point'] >= point_specialist_threshold
        df['is_friend_specialist'] = df['friend_count'] >= friend_specialist_threshold
        
        # 종합 specialist 여부 (포인트 또는 친구수 중 하나라도 specialist면 True)
        df['is_specialist'] = df['is_point_specialist'] | df['is_friend_specialist']
        
        # Specialist 유형 분류
        def classify_specialist_type(row):
            if row['is_point_specialist'] and row['is_friend_specialist']:
                return 'both'  # 포인트와 친구수 모두 높음
            elif row['is_point_specialist']:
                return 'point'  # 포인트만 높음
            elif row['is_friend_specialist']:
                return 'friend'  # 친구수만 높음
            else:
                return 'normal'  # 일반 사용자
        
        df['specialist_type'] = df.apply(classify_specialist_type, axis=1)

        # 통계 정보 출력
        total_specialists = df['is_specialist'].sum()
        point_specialists = df['is_point_specialist'].sum()
        friend_specialists = df['is_friend_specialist'].sum()
        both_specialists = (df['specialist_type'] == 'both').sum()
        
        specialist_rate = total_specialists / len(df) * 100
        
        logger.info(f"✅ 천지현: Specialist 분류 완료")
        logger.info(f"   총 Specialist: {total_specialists:,}명 ({specialist_rate:.2f}%)")
        logger.info(f"   포인트 Specialist: {point_specialists:,}명")
        logger.info(f"   친구수 Specialist: {friend_specialists:,}명")
        logger.info(f"   복합 Specialist: {both_specialists:,}명")
        logger.info(f"   일반 사용자: {len(df) - total_specialists:,}명")
        
        # 유형별 통계 출력
        type_counts = df['specialist_type'].value_counts()
        logger.info(f"   유형별 분포:")
        for spec_type, count in type_counts.items():
            percentage = count / len(df) * 100
            logger.info(f"      {spec_type}: {count:,}명 ({percentage:.2f}%)")
        
        # 결과 검증
        if len(df) == 0:
            raise ValueError("전처리 후 데이터가 비어있습니다")
        
        # Specialist들의 평균 통계
        if total_specialists > 0:
            specialist_df = df[df['is_specialist']]
            normal_df = df[~df['is_specialist']]
            
            logger.info(f"   Specialist vs 일반 사용자 비교:")
            logger.info(f"      Specialist 평균 포인트: {specialist_df['point'].mean():.1f}")
            logger.info(f"      일반 사용자 평균 포인트: {normal_df['point'].mean():.1f}")
            logger.info(f"      Specialist 평균 친구수: {specialist_df['friend_count'].mean():.1f}")
            logger.info(f"      일반 사용자 평균 친구수: {normal_df['friend_count'].mean():.1f}")
        
        logger.info(f"   최종 데이터: {len(df):,}명 (데이터 제거 없음)")
        
        # 메모리 정리 (원본 데이터는 제거하지 않고 임시 변수만 정리)
        gc.collect()

        return df
        
    except Exception as e:
        logger.error(f"❌ 천지현: accounts_user 전처리 실패 - {str(e)}")
        gc.collect()
        raise

# 사용 예시
if __name__ == "__main__":
    from load_data import load_table
    
    try:
        df = load_table('accounts_user', 'votes')
        df_processed = preprocess_accounts_user(df)
        
        print(f"전처리 완료: {len(df_processed):,}행")
        print(f"추가된 컬럼: {[col for col in df_processed.columns if 'specialist' in col or 'friend_count' in col]}")
        
        # Specialist 분포 확인
        print("\n=== Specialist 분포 ===")
        print(df_processed['specialist_type'].value_counts())
        
        # 상위 5명의 Specialist 정보 출력
        print("\n=== 상위 Specialist 예시 ===")
        specialists = df_processed[df_processed['is_specialist']].nlargest(5, 'point')
        for _, row in specialists.iterrows():
            print(f"포인트: {row['point']:,}, 친구수: {row['friend_count']}, 유형: {row['specialist_type']}")
            
    except Exception as e:
        print(f"테스트 실패: {e}")