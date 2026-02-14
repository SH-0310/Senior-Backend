import requests
import pymysql
from urllib.parse import unquote
import time
import json
import os

# --- [기본 설정 함수 동일] ---
def load_api_configs():
    config_path = '/home/ubuntu/Senior/Code/API/api_config.json'
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def clean_int(value):
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try: return int(value)
    except: return None

def get_db_connection():
    return pymysql.connect(
        host='localhost', user='shmoon', password='Tjdgursla87!',
        db='senior_travel', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def sync_all_common_master():
    API_ACCOUNTS = load_api_configs()
    current_key_idx = 0
    conn = get_db_connection()
    BASE_URL = "http://apis.data.go.kr/B551011/KorService2/detailCommon2"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        with conn.cursor() as cursor:
            # ✅ 1. 현재 보유 중인 데이터 개수 (spot_commons에 이미 있는 것)
            cursor.execute("SELECT COUNT(*) as cnt FROM spot_commons")
            current_owned = cursor.fetchone()['cnt']

            # ✅ 2. 수집해야 할 잔여 수량 파악 (중복 제거 기준)
            sql_count = """
                SELECT COUNT(*) as total FROM (
                    SELECT contentid FROM picnic_spots
                    UNION
                    SELECT contentid FROM festivals
                ) AS all_ids
                WHERE contentid NOT IN (SELECT contentid FROM spot_commons)
            """
            cursor.execute(sql_count)
            total_pending = cursor.fetchone()['total']

            # ✅ 3. 전체 목표 (보유 + 잔여)
            total_goal = current_owned + total_pending

            if total_pending == 0:
                print(f"✨ [완료] 모든 데이터가 수집되었습니다! (총 {total_goal}건 보유)")
                return

            # ✅ 4. 이번 회차에 가져올 리스트 조회 (LIMIT 1000)
            sql_targets = """
                SELECT contentid FROM (
                    SELECT contentid FROM picnic_spots
                    UNION
                    SELECT contentid FROM festivals
                ) AS all_ids
                WHERE contentid NOT IN (SELECT contentid FROM spot_commons)
                LIMIT 1000
            """
            cursor.execute(sql_targets)
            targets = cursor.fetchall()
            current_batch_size = len(targets)

        print(f"\n📊 [수집 통계] 총 목표: {total_goal}건 | 보유: {current_owned}건 | 잔여: {total_pending}건")
        print(f"🚀 이번 배치({current_batch_size}건) 수집 시작 (API 키 인덱스: {current_key_idx})")

        # 5. 데이터 수집 루프
        for index, row in enumerate(targets, 1):
            cid = row['contentid']
            item_data = None
            retry_limit = 3
            
            while current_key_idx < len(API_ACCOUNTS):
                acc = API_ACCOUNTS[current_key_idx]
                params = {
                    'serviceKey': unquote(acc['SERVICE_KEY']),
                    'MobileOS': 'ETC',
                    'MobileApp': acc['MOBILE_APP'],
                    '_type': 'json',
                    'contentId': cid
                }

                success = False
                for attempt in range(retry_limit):
                    try:
                        res = requests.get(BASE_URL, params=params, headers=headers, timeout=40)
                        
                        # 429 에러 대응
                        if res.status_code == 429:
                            print(f"\n🚦 [429 Error] 과부하! 15초 대기 후 재시도... ({attempt+1}/{retry_limit})")
                            time.sleep(15)
                            continue

                        if res.status_code == 200:
                            if "LIMITED" in res.text:
                                print(f"\n🚫 키 한도 초과: {acc['MOBILE_APP']}")
                                current_key_idx += 1
                                break

                            data = res.json()
                            body = data.get('response', {}).get('body', {})
                            if body and 'items' in body and body['items']:
                                item_data = body['items']['item'][0]
                                success = True
                                break
                            else:
                                item_data = "SKIP"
                                success = True
                                break
                        else:
                            print(f"\n⚠️ 서버 응답 에러 ({res.status_code}). 재시도 {attempt+1}/{retry_limit}")
                            time.sleep(2)

                    except Exception as e:
                        print(f"\n❌ 기타 에러: {e}")
                        break

                if success: break
                if not success and attempt == retry_limit - 1:
                    current_key_idx += 1

            if current_key_idx >= len(API_ACCOUNTS):
                print("\n🚨 모든 API 키 소진. 작업을 중단합니다."); break

            # DB 저장
            if item_data and item_data != "SKIP":
                with conn.cursor() as cursor:
                    sql = """
                        INSERT INTO spot_commons (
                            contentid, contenttypeid, title, createdtime, modifiedtime,
                            tel, telname, homepage, firstimage, firstimage2, cpyrhtDivCd,
                            areacode, sigungucode, cat1, cat2, cat3, addr1, addr2, zipcode,
                            mapx, mapy, mlevel, overview, lDongRegnCd, lDongSignguCd,
                            lclsSystm1, lclsSystm2, lclsSystm3
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE modifiedtime = VALUES(modifiedtime)
                    """
                    cursor.execute(sql, (
                        clean_int(item_data.get('contentid')), clean_int(item_data.get('contenttypeid')),
                        item_data.get('title'), item_data.get('createdtime'), item_data.get('modifiedtime'),
                        item_data.get('tel'), item_data.get('telname'), item_data.get('homepage'),
                        item_data.get('firstimage'), item_data.get('firstimage2'), item_data.get('cpyrhtDivCd'),
                        clean_int(item_data.get('areacode')), clean_int(item_data.get('sigungucode')),
                        item_data.get('cat1'), item_data.get('cat2'), item_data.get('cat3'),
                        item_data.get('addr1'), item_data.get('addr2'), item_data.get('zipcode'),
                        item_data.get('mapx'), item_data.get('mapy'), clean_int(item_data.get('mlevel')),
                        item_data.get('overview'), item_data.get('lDongRegnCd'), item_data.get('lDongSignguCd'),
                        item_data.get('lclsSystm1'), item_data.get('lclsSystm2'), item_data.get('lclsSystm3')
                    ))
                conn.commit()
            
            # ✅ 실시간 진행률 로그 출력
            if index % 10 == 0 or index == current_batch_size:
                realtime_owned = current_owned + index
                progress_percent = round((realtime_owned / total_goal) * 100, 1)
                print(f"\r📈 실시간 현황: [{progress_percent}%] 보유 {realtime_owned} / 총량 {total_goal} (잔여 {total_pending - index}건)", end="")

            time.sleep(0.5) # 429 방지를 위해 조금 더 넉넉히 쉼

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("\n🏁 DB 연결 종료 및 프로세스 완료.")

if __name__ == "__main__":
    sync_all_common_master()