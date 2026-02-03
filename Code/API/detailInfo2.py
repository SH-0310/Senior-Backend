import requests
import pymysql
from urllib.parse import unquote
import time
import json

# --- [기본 설정 및 DB 연결] ---
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

def sync_all_info_master():
    API_ACCOUNTS = load_api_configs()
    current_key_idx = 0
    BASE_URL = "http://apis.data.go.kr/B551011/KorService2/detailInfo2"

    while True: # 🔄 무한 루프 시작: 데이터가 없을 때까지 반복
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 500개씩 끊어서 가져오기 (NOT IN 덕분에 자연스럽게 다음 데이터를 가져옵니다)
                sql_targets = """
                    SELECT contentid, contenttypeid FROM spot_commons
                    WHERE contenttypeid IN (12, 14, 15, 25, 28)
                    AND contentid NOT IN (SELECT DISTINCT contentid FROM spot_info)
                    LIMIT 500
                """
                cursor.execute(sql_targets)
                targets = cursor.fetchall()

            # 🛑 탈출 조건 1: 더 이상 수집할 데이터가 없음
            if not targets:
                print("✨ 모든 데이터를 수집했습니다! 작업을 종료합니다.")
                break

            print(f"🚀 이번 회차: {len(targets)}건의 상세 정보 수집을 시작합니다. (현재 API 키 인덱스: {current_key_idx})")

            for row in targets:
                # 🚨 탈출 조건 2: 모든 API 키 소진 시 즉시 중단
                if current_key_idx >= len(API_ACCOUNTS):
                    print("🚨 모든 API 키가 소진되었습니다. 루프를 종료합니다.")
                    return # 함수 전체 종료

                cid, tid = row['contentid'], row['contenttypeid']
                item_list = []

                # API 호출 로직
                while current_key_idx < len(API_ACCOUNTS):
                    acc = API_ACCOUNTS[current_key_idx]
                    params = {
                        'serviceKey': unquote(acc['SERVICE_KEY']),
                        'MobileOS': 'ETC', 'MobileApp': 'AppTest',
                        '_type': 'json', 'contentId': cid, 'contentTypeId': tid
                    }

                    try:
                        res = requests.get(BASE_URL, params=params, timeout=30)
                        data = res.json()
                        items_container = data.get('response', {}).get('body', {}).get('items', '')
                        
                        if items_container and 'item' in items_container:
                            item_list = items_container['item']
                            if isinstance(item_list, dict): item_list = [item_list]
                        break # 성공 시 while 키 루프 탈출
                    except:
                        print(f"⚠️ 키 {acc['MOBILE_APP']} 교체 시도...")
                        current_key_idx += 1
                        continue

                # DB 저장 로직
                if item_list:
                    with conn.cursor() as cursor:
                        for item in item_list:
                            sql = """
                                INSERT IGNORE INTO spot_info (
                                    contentid, contenttypeid, serialnum, 
                                    infoname, infotext, fldgubun,
                                    subcontentid, subname, subdetailoverview, subdetailimg, subdetailalt
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(sql, (
                                cid, tid, item.get('serialnum') or item.get('subnum'),
                                item.get('infoname'), item.get('infotext'), item.get('fldgubun'),
                                item.get('subcontentid'), item.get('subname'),
                                item.get('subdetailoverview'), item.get('subdetailimg'), item.get('subdetailalt')
                            ))
                    conn.commit()
                    # print(f"✅ ID {cid} 저장 완료") # 로그가 너무 많으면 이 줄을 주석처리하세요

                time.sleep(0.1) # 서버 부하 방지

        finally:
            conn.close() # 500개 주기가 끝날 때마다 연결을 닫아 안정성 확보
            print(f"📦 500개 배치 완료. 다음 배치를 준비합니다...")

if __name__ == "__main__":
    sync_all_info_master()