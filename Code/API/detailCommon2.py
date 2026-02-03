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
    
    # ✅ 브라우저인 척 하기 위한 헤더 설정
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        with conn.cursor() as cursor:
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

        if not targets:
            print("✨ 수집할 새로운 데이터가 없습니다.")
            return

        print(f"🚀 {len(targets)}건 수집 시작 (타임아웃 재시도 로직 강화)")

        for row in targets:
            cid = row['contentid']
            item_data = None
            
            # ✅ 아이템 하나당 최대 3번까지 재시도
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
                        # ✅ 타임아웃을 40초로 더 넉넉하게 잡습니다.
                        res = requests.get(BASE_URL, params=params, headers=headers, timeout=40)
                        
                        if res.status_code == 200:
                            if "LIMITED" in res.text:
                                print(f"🚫 키 한도 초과: {acc['MOBILE_APP']}")
                                current_key_idx += 1
                                break # 다음 키로 이동

                            data = res.json()
                            body = data.get('response', {}).get('body', {})
                            if body and 'items' in body and body['items']:
                                item_data = body['items']['item'][0]
                                success = True
                                break # 수집 성공!
                            else:
                                print(f"ℹ️ ID {cid}: 데이터 없음. 패스.")
                                item_data = "SKIP"
                                success = True
                                break
                        else:
                            print(f"⚠️ 서버 응답 에러 ({res.status_code}). 재시도 {attempt+1}/{retry_limit}")
                            time.sleep(2)

                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                        print(f"⏳ ID {cid} 타임아웃 발생. 재시도 {attempt+1}/{retry_limit}...")
                        time.sleep(3) # 잠시 쉬었다가 다시 시도
                    except Exception as e:
                        print(f"❌ 기타 에러: {e}")
                        break

                if success:
                    break # while 루프 탈출 (다음 아이템으로)
                
                if not success and attempt == retry_limit - 1:
                    print(f"🚨 ID {cid}는 이 키로 도저히 안 됩니다. 키를 교체합니다.")
                    current_key_idx += 1

            if current_key_idx >= len(API_ACCOUNTS):
                print("🚨 모든 API 키 소진."); break

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
                print(f"✅ ID {cid} 저장 완료")
            
            time.sleep(0.1)

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("🏁 DB 연결 종료 및 프로세스 완료.")

if __name__ == "__main__":
    sync_all_common_master()