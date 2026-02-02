import requests
import pymysql
from urllib.parse import unquote
import time
import json
import os
import logging
from datetime import datetime

# --- [로깅 설정: 실행 결과가 파일에 저장됩니다] ---
log_file = '/home/ubuntu/Senior/Code/API/festival_sync.log'
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def load_api_configs():
    config_path = '/home/ubuntu/Senior/Code/API/api_config.json'
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_db_connection():
    return pymysql.connect(
        host='localhost', user='shmoon', password='Tjdgursla87!',
        db='senior_travel', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def clean_int(value):
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try: return int(value)
    except: return None

def sync_festivals_periodic():
    API_ACCOUNTS = load_api_configs()
    current_key_idx = 0
    conn = get_db_connection()
    BASE_URL = "http://apis.data.go.kr/B551011/KorService2/searchFestival2"
    
    # ✅ [자동화 핵심] 실행하는 시점의 오늘 날짜를 자동으로 가져옵니다.
    today_str = datetime.now().strftime('%Y%m%d')
    logging.info(f"🚀 {today_str} 기준 정기 수집을 시작합니다.")
    
    try:
        page = 1
        while True:
            success_page = False
            while current_key_idx < len(API_ACCOUNTS):
                acc = API_ACCOUNTS[current_key_idx]
                params = {
                    'serviceKey': unquote(acc['SERVICE_KEY']),
                    'numOfRows': 100, 'pageNo': page,
                    'MobileOS': 'AND', 'MobileApp': acc['MOBILE_APP'],
                    '_type': 'json', 'arrange': 'C',
                    'eventStartDate': today_str
                }
                
                try:
                    response = requests.get(BASE_URL, params=params, timeout=60)
                    if response.status_code != 200 or "LIMITED" in response.text:
                        current_key_idx += 1
                        continue
                        
                    data = response.json()
                    body = data.get('response', {}).get('body', {})
                    if not body or 'items' not in body or body['items'] == "":
                        logging.info("✅ 수집할 새로운 데이터가 없습니다. 종료합니다.")
                        return

                    items = body['items']['item']
                    with conn.cursor() as cursor:
                        sql = """
                            INSERT INTO festivals (
                                contentid, contenttypeid, title, addr1, addr2, zipcode, areacode, sigungucode,
                                mapx, mapy, mlevel, tel, firstimage, firstimage2, 
                                eventstartdate, eventenddate, cat1, cat2, cat3,
                                lDongRegnCd, lDongSignguCd, lclsSystm1, lclsSystm2, lclsSystm3,
                                progresstype, festivaltype, createdtime, modifiedtime
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                modifiedtime = VALUES(modifiedtime), title = VALUES(title), 
                                eventstartdate = VALUES(eventstartdate), eventenddate = VALUES(eventenddate),
                                addr1 = VALUES(addr1), firstimage = VALUES(firstimage)
                        """
                        for item in items:
                            cursor.execute(sql, (
                                clean_int(item.get('contentid')), clean_int(item.get('contenttypeid')),
                                item.get('title'), item.get('addr1', ''), item.get('addr2', ''), item.get('zipcode', ''),
                                clean_int(item.get('areacode')), clean_int(item.get('sigungucode')),
                                item.get('mapx'), item.get('mapy'), clean_int(item.get('mlevel')),
                                item.get('tel', ''), item.get('firstimage', ''), item.get('firstimage2', ''),
                                item.get('eventstartdate'), item.get('eventenddate'),
                                item.get('cat1'), item.get('cat2'), item.get('cat3'),
                                item.get('lDongRegnCd'), item.get('lDongSignguCd'),
                                item.get('lclsSystm1'), item.get('lclsSystm2'), item.get('lclsSystm3'),
                                item.get('progresstype'), item.get('festivaltype'),
                                item.get('createdtime'), item.get('modifiedtime')
                            ))
                        conn.commit()
                    logging.info(f"📦 {page}페이지 저장 완료 ({len(items)}건)")
                    success_page = True
                    break
                
                except requests.exceptions.Timeout:
                    logging.warning("⏳ 서버 지연 발생. 5초 후 재시도합니다.")
                    time.sleep(5); continue
                except Exception as e:
                    logging.error(f"⚠️ 에러 발생: {e}")
                    current_key_idx += 1; break

            if current_key_idx >= len(API_ACCOUNTS): break
            if success_page: page += 1; time.sleep(0.5)
            else: break
    finally:
        conn.close()
        logging.info("🏁 동기화 프로세스 종료.")

if __name__ == "__main__":
    sync_festivals_periodic()