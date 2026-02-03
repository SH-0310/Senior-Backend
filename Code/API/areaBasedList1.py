import os
import sys
import requests
import json
import csv
import time
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 현재 파일의 부모 폴더(Code)를 시스템 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from utils import get_db_connection

# 로깅 설정 (실시간 출력을 위해 StreamHandler 확인)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("area_collection.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_last_month_str():
    """현재 날짜 기준 2개월 전을 YYYYMM 형식으로 반환 (가장 안정적인 데이터)"""
    last_month = datetime.now() - relativedelta(months=2)
    return last_month.strftime("%Y%m")

def load_api_config():
    try:
        with open('api_config.json', 'r', encoding='utf-8') as f:
            configs = json.load(f)
            return configs[0]
    except Exception as e:
        logging.error(f"❌ 설정 파일 로드 실패: {e}")
        return None

def run_full_collection():
    base_ym = get_last_month_str()
    logging.info(f"🚀 수집 시작! 기준연월: {base_ym}")

    config = load_api_config()
    if not config: return

    url = "http://apis.data.go.kr/B551011/LocgoHubTarService1/areaBasedList1"
    
    # ✅ 브라우저처럼 보이게 하여 차단 방지
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        # 전체 행 개수 파악 (진행률 표시용)
        with open('province_code.csv', 'r', encoding='utf-8') as f:
            total_rows = sum(1 for line in f) - 1 # 헤더 제외

        conn = get_db_connection()
        conn.autocommit(True)
        total_saved = 0
        current_idx = 0
        
        with open('province_code.csv', mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t') 
            
            for row in reader:
                current_idx += 1
                area_cd = row['areaCd']
                sigungu_cd = row['sigunguCd']
                sigungu_nm = row['sigunguNm']
                area_nm = row['areaNm']
                
                # 실시간 진행률 출력
                progress = (current_idx / total_rows) * 100
                logging.info(f"[{current_idx}/{total_rows}] {progress:.1f}% | 📍 {area_nm} {sigungu_nm} 시도 중...")

                params = {
                    "serviceKey": config["SERVICE_KEY"],
                    "MobileApp": config["MOBILE_APP"],
                    "MobileOS": "ETC",
                    "numOfRows": 50,
                    "pageNo": 1,
                    "baseYm": base_ym,
                    "areaCd": area_cd,
                    "signguCd": sigungu_cd,
                    "_type": "json"
                }

                max_retries = 3
                success = False
                
                for attempt in range(max_retries):
                    try:
                        res = requests.get(url, params=params, headers=headers, timeout=30)
                        res.raise_for_status()
                        res_json = res.json()
                        
                        header = res_json.get("response", {}).get("header", {})
                        if header.get("resultCode") != "0000":
                            logging.warning(f"  ⚠️ API 응답 지연/오류 ({sigungu_nm}): {header.get('resultMsg')}")
                            time.sleep(3)
                            continue

                        items = res_json.get("response", {}).get("body", {}).get("items", {}).get("item", [])
                        
                        if items:
                            with conn.cursor() as cursor:
                                for item in items:
                                    cursor.execute("""
                                        INSERT INTO areaBasedList1 (
                                            base_ym, area_cd, area_nm, signgu_cd, signgu_nm,
                                            hub_tats_cd, hub_tats_nm, hub_ctgry_lcls_nm, hub_ctgry_mcls_nm, 
                                            hub_rank, map_x, map_y
                                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON DUPLICATE KEY UPDATE 
                                            hub_rank=VALUES(hub_rank), 
                                            map_x=VALUES(map_x), 
                                            map_y=VALUES(map_y)
                                    """, (
                                        item.get("baseYm"), item.get("areaCd"), item.get("areaNm"),
                                        item.get("signguCd"), item.get("signguNm"),
                                        item.get("hubTatsCd"), item.get("hubTatsNm"),
                                        item.get("hubCtgryLclsNm"), item.get("hubCtgryMclsNm"),
                                        item.get("hubRank"), item.get("mapX"), item.get("mapY")
                                    ))
                            logging.info(f"  ✅ {sigungu_nm} 저장 완료 ({len(items)}건)")
                            total_saved += len(items)
                        else:
                            logging.info(f"  ℹ️ {sigungu_nm} 데이터 없음")
                        
                        success = True
                        break 

                    except Exception as e:
                        if attempt < max_retries - 1:
                            logging.warning(f"  ⚠️ {sigungu_nm} 재시도 중... ({attempt + 1}/{max_retries})")
                            time.sleep(5)
                        else:
                            logging.error(f"  ❌ {sigungu_nm} 최종 실패: {str(e)[:100]}")

                time.sleep(0.3) # API 서버 예우

        logging.info(f"🏁 수집 완료! 총 {total_saved}건의 관광지가 저장되었습니다.")

    except Exception as e:
        logging.error(f"❌ 치명적 오류: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    run_full_collection()