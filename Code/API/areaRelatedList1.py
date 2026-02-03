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

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("area_related_collection.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_safe_month_str():
    """안전하게 2개월 전 연월(YYYYMM)을 반환합니다."""
    safe_month = datetime.now() - relativedelta(months=2)
    return safe_month.strftime("%Y%m")

def load_api_config():
    try:
        with open('api_config.json', 'r', encoding='utf-8') as f:
            configs = json.load(f)
            return configs[0]
    except Exception as e:
        logging.error(f"❌ 설정 파일 로드 실패: {e}")
        return None

def run_full_collection():
    base_ym = get_safe_month_str()
    logging.info(f"🚀 연관 관광지 수집 시작! 기준연월: {base_ym}")

    config = load_api_config()
    if not config: return

    url = "http://apis.data.go.kr/B551011/TarRlteTarService1/areaBasedList1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        with open('province_code.csv', 'r', encoding='utf-8') as f:
            total_rows = sum(1 for line in f) - 1

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
                
                progress = (current_idx / total_rows) * 100
                logging.info(f"[{current_idx}/{total_rows}] {progress:.1f}% | 📍 {sigungu_nm} 연관 데이터 시도 중...")

                params = {
                    "serviceKey": config["SERVICE_KEY"],
                    "MobileApp": config["MOBILE_APP"],
                    "MobileOS": "ETC",
                    "numOfRows": 100, # 연관 관광지는 목록이 길 수 있으므로 100개 요청
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
                            logging.warning(f"  ⚠️ 응답 지연 ({sigungu_nm}): {header.get('resultMsg')}")
                            time.sleep(3)
                            continue

                        items = res_json.get("response", {}).get("body", {}).get("items", {}).get("item", [])
                        
                        if items:
                            with conn.cursor() as cursor:
                                for item in items:
                                    cursor.execute("""
                                        INSERT INTO areaRelatedList1 (
                                            base_ym, t_ats_cd, t_ats_nm, area_cd, area_nm, signgu_cd, signgu_nm,
                                            rlte_tats_cd, rlte_tats_nm, rlte_regn_cd, rlte_regn_nm,
                                            rlte_signgu_cd, rlte_signgu_nm, rlte_ctgry_lcls_nm,
                                            rlte_ctgry_mcls_nm, rlte_ctgry_scls_nm, rlte_rank
                                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        ON DUPLICATE KEY UPDATE 
                                            rlte_rank=VALUES(rlte_rank), 
                                            rlte_tats_nm=VALUES(rlte_tats_nm)
                                    """, (
                                        item.get("baseYm"), item.get("tAtsCd"), item.get("tAtsNm"),
                                        item.get("areaCd"), item.get("areaNm"), item.get("signguCd"), item.get("signguNm"),
                                        item.get("rlteTatsCd"), item.get("rlteTatsNm"), item.get("rlteRegnCd"),
                                        item.get("rlteRegnNm"), item.get("rlteSignguCd"), item.get("rlteSignguNm"),
                                        item.get("rlteCtgryLclsNm"), item.get("rlteCtgryMclsNm"),
                                        item.get("rlteCtgrySclsNm"), item.get("rlteRank")
                                    ))
                            logging.info(f"  ✅ {sigungu_nm} 연관 데이터 {len(items)}건 저장")
                            total_saved += len(items)
                        else:
                            logging.info(f"  ℹ️ {sigungu_nm} 연관 데이터 없음")
                        
                        success = True
                        break 

                    except Exception as e:
                        if attempt < max_retries - 1:
                            time.sleep(5)
                        else:
                            logging.error(f"  ❌ {sigungu_nm} 최종 실패: {str(e)[:50]}")

                time.sleep(0.3)

        logging.info(f"🏁 수집 완료! 총 {total_saved}건의 연관 정보가 저장되었습니다.")

    except Exception as e:
        logging.error(f"❌ 치명적 오류: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    run_full_collection()