"""
전체 전처리 파이프라인 실행
팀원별 담당 업무 통합 실행
"""

import sys
import os
import time
import logging
from datetime import datetime
import gc
import psutil

# 현재 디렉토리를 파이썬 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from load_data import load_table
from preprocess_accounts_user import preprocess_accounts_user
from preprocess_hackle_events import preprocess_hackle_events
from preprocess_accounts_userquestionrecord import preprocess_userquestionrecord
from preprocess_accounts_blockrecord import preprocess_blockrecord
from save_data import save_to_gcs

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'preprocessing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

def check_system_resources():
    """시스템 리소스 체크"""
    memory_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=1)
    disk_percent = psutil.disk_usage('/tmp').percent
    
    logger.info(f"📊 시스템 리소스 체크:")
    logger.info(f"   메모리: {memory_percent:.1f}%")
    logger.info(f"   CPU: {cpu_percent:.1f}%")
    logger.info(f"   디스크(/tmp): {disk_percent:.1f}%")
    
    if memory_percent > 90:
        raise MemoryError(f"메모리 사용률 위험 수준: {memory_percent:.1f}%")
    if disk_percent > 90:
        raise RuntimeError(f"디스크 공간 부족: {disk_percent:.1f}%")
    
    return {
        'memory': memory_percent,
        'cpu': cpu_percent,
        'disk': disk_percent
    }

def run_single_preprocessing(processor_name: str, table_name: str, dataset: str, preprocess_func):
    """개별 전처리 실행"""
    start_time = time.time()
    
    try:
        logger.info(f"\n🔄 {processor_name}: {table_name} 처리 시작")
        
        # 리소스 체크
        resources_before = check_system_resources()
        
        # 1. 데이터 로드
        df = load_table(table_name, dataset)
        original_count = len(df)
        
        # 2. 전처리
        df_clean = preprocess_func(df)
        processed_count = len(df_clean)
        
        # 3. 저장
        result = save_to_gcs(df_clean, table_name, 'processed')
        
        # 4. 메모리 정리
        del df_clean
        gc.collect()
        
        # 처리 시간 계산
        elapsed_time = time.time() - start_time
        
        # 결과 요약
        summary = {
            'processor': processor_name,
            'table_name': table_name,
            'original_rows': original_count,
            'processed_rows': processed_count,
            'processing_time_seconds': round(elapsed_time, 2),
            'status': 'SUCCESS',
            'gcs_info': result
        }
        
        logger.info(f"✅ {processor_name}: {table_name} 완료")
        logger.info(f"   처리 시간: {elapsed_time:.1f}초")
        logger.info(f"   데이터: {original_count:,}행 → {processed_count:,}행")
        
        return summary
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"❌ {processor_name}: {table_name} 실패 - {str(e)}")
        
        return {
            'processor': processor_name,
            'table_name': table_name,
            'processing_time_seconds': round(elapsed_time, 2),
            'status': 'FAILED',
            'error': str(e)
        }

def run_all_preprocessing(parallel: bool = False):
    """모든 테이블 전처리 실행"""
    
    pipeline_start_time = time.time()
    
    logger.info("🚀 전체 전처리 파이프라인 시작")
    logger.info("=" * 80)
    logger.info(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 초기 리소스 체크
    try:
        initial_resources = check_system_resources()
    except Exception as e:
        logger.error(f"❌ 시스템 리소스 부족으로 실행 중단: {e}")
        return
    
    # 전처리 작업 정의
    preprocessing_tasks = [
        {
            'processor': '천지현',
            'table_name': 'accounts_user',
            'dataset': 'votes',
            'function': preprocess_accounts_user
        },
        {
            'processor': '조수진',
            'table_name': 'hackle_events',
            'dataset': 'hackle',
            'function': preprocess_hackle_events
        },
        {
            'processor': '진우형',
            'table_name': 'accounts_userquestionrecord',
            'dataset': 'votes',
            'function': preprocess_userquestionrecord
        },
        {
            'processor': '이준희',
            'table_name': 'accounts_blockrecord',
            'dataset': 'votes',
            'function': preprocess_blockrecord
        }
    ]
    
    results = []
    
    if parallel:
        # 병렬 처리 (선택사항) - 메모리가 충분할 때만
        logger.info("⚡ 병렬 처리 모드")
        from concurrent.futures import ThreadPoolExecutor
        
        with ThreadPoolExecutor(max_workers=2) as executor:  # 메모리 절약을 위해 2개만
            futures = []
            for task in preprocessing_tasks:
                future = executor.submit(
                    run_single_preprocessing,
                    task['processor'],
                    task['table_name'],
                    task['dataset'],
                    task['function']
                )
                futures.append(future)
            
            for future in futures:
                result = future.result()
                results.append(result)
    else:
        # 순차 처리 (기본) - 메모리 안전
        logger.info("🔄 순차 처리 모드")
        for i, task in enumerate(preprocessing_tasks, 1):
            logger.info(f"\n📍 진행률: {i}/{len(preprocessing_tasks)}")
            
            result = run_single_preprocessing(
                task['processor'],
                task['table_name'],
                task['dataset'],
                task['function']
            )
            results.append(result)
            
            # 중간 리소스 체크 및 정리
            gc.collect()
            time.sleep(1)  # 시스템 안정화를 위한 짧은 대기
    
    # 전체 파이프라인 완료
    pipeline_elapsed_time = time.time() - pipeline_start_time
    
    logger.info("\n" + "=" * 80)
    logger.info("🎉 전체 데이터 전처리 파이프라인 완료!")
    logger.info("=" * 80)
    
    # 결과 요약
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed_count = len(results) - success_count
    
    logger.info(f"📊 처리 결과 요약:")
    logger.info(f"   ✅ 성공: {success_count}개")
    logger.info(f"   ❌ 실패: {failed_count}개")
    logger.info(f"   ⏱️ 총 처리 시간: {pipeline_elapsed_time:.1f}초")
    
    # 개별 결과 상세 출력
    for result in results:
        if result['status'] == 'SUCCESS':
            logger.info(f"   ✅ {result['table_name']} ({result['processor']})")
            logger.info(f"      {result['original_rows']:,}행 → {result['processed_rows']:,}행")
            logger.info(f"      처리 시간: {result['processing_time_seconds']}초")
        else:
            logger.info(f"   ❌ {result['table_name']} ({result['processor']})")
            logger.info(f"      오류: {result['error']}")
    
    if success_count > 0:
        logger.info(f"💾 성공한 데이터는 gs://sprintda05_final_project/processed/ 에 저장됨")
    
    # 최종 리소스 상태
    try:
        final_resources = check_system_resources()
        logger.info(f"🔧 리소스 변화: 메모리 {initial_resources['memory']:.1f}% → {final_resources['memory']:.1f}%")
    except:
        pass
    
    logger.info(f"완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='데이터 전처리 파이프라인 실행')
    parser.add_argument('--parallel', action='store_true', help='병렬 처리 모드 (메모리 충분할 때만)')
    parser.add_argument('--table', type=str, help='특정 테이블만 처리 (accounts_user, hackle_events, accounts_userquestionrecord, accounts_blockrecord)')
    
    args = parser.parse_args()
    
    if args.table:
        # 특정 테이블만 처리
        table_map = {
            'accounts_user': ('천지현', 'votes', preprocess_accounts_user),
            'hackle_events': ('조수진', 'hackle', preprocess_hackle_events),
            'accounts_userquestionrecord': ('진우형', 'votes', preprocess_userquestionrecord),
            'accounts_blockrecord': ('이준희', 'votes', preprocess_blockrecord)
        }
        
        if args.table in table_map:
            processor, dataset, func = table_map[args.table]
            result = run_single_preprocessing(processor, args.table, dataset, func)
            print(f"\n결과: {result}")
        else:
            print(f"❌ 지원하지 않는 테이블: {args.table}")
            print(f"지원 테이블: {', '.join(table_map.keys())}")
    else:
        # 전체 파이프라인 실행
        results = run_all_preprocessing(parallel=args.parallel)