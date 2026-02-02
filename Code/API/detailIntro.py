import requests
import pymysql
from urllib.parse import unquote
import time
import json
import os

# --- [1. 설정 파일 로드 로직] ---
def load_api_configs():
    config_path = 'api_config.json'
    if not os.path.exists(config_path):
        print(f"🚨 설정 파일({config_path})이 없습니다. 파일을 먼저 생성해주세요.")
        exit()
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# 여러 개의 계정 정보 로드
API_ACCOUNTS = load_api_configs()
current_key_idx = 0  # 현재 사용 중인 키 번호 (0부터 시작)

# --- [DB 연결 설정] ---
def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# --- [필드 매핑 로직] ---
def map_standard_fields(item):
    return {
        'contentid': item.get('contentid'),
        'infocenter': item.get('infocenter') or item.get('infocenterculture') or item.get('infocenterfood') or item.get('infocenterleports') or item.get('infocentershopping') or "",
        'restdate': item.get('restdate') or item.get('restdateculture') or item.get('restdatefood') or item.get('restdateleports') or item.get('restdateshopping') or "",
        'usetime': item.get('usetime') or item.get('usetimeculture') or item.get('opentimefood') or item.get('opentime') or item.get('usetimeleports') or "",
        'parking': item.get('parking') or item.get('parkingculture') or item.get('parkingfood') or item.get('parkingleports') or item.get('parkingshopping') or "",
        'parkingfee': item.get('parkingfee') or item.get('parkingfeeleports') or "",
        'chkbabycarriage': item.get('chkbabycarriage') or item.get('chkbabycarriageculture') or item.get('chkbabycarriageleports') or item.get('chkbabycarriageshopping') or "",
        'chkpet': item.get('chkpet') or item.get('chkpetculture') or item.get('chkpetleports') or item.get('chkpetshopping') or "",
        'chkcreditcard': item.get('chkcreditcard') or item.get('chkcreditcardculture') or item.get('chkcreditcardfood') or item.get('chkcreditcardleports') or item.get('chkcreditcardshopping') or "",
        'eventstartdate': item.get('eventstartdate') or "",
        'eventenddate': item.get('eventenddate') or "",
        'playtime': item.get('playtime') or "",
        'firstmenu': item.get('firstmenu') or "",
        'treatmenu': item.get('treatmenu') or "",
        'usefee': item.get('usefee') or item.get('usetimefestival') or item.get('usefeeleports') or "",
        'expagerange': item.get('expagerange') or item.get('agelimit') or "",
        'opendate': item.get('opendate') or item.get('opendatefood') or "",
        'raw_data': json.dumps(item, ensure_ascii=False)
    }

def sync_details():
    global current_key_idx
    conn = get_db_connection()
    DETAIL_URL = "http://apis.data.go.kr/B551011/KorService2/detailIntro2"

    try:
        with conn.cursor() as cursor:
            sql_targets = """
                SELECT p.contentid, p.contenttypeid 
                FROM picnic_spots p
                LEFT JOIN spot_details d ON p.contentid = d.contentid
                WHERE d.contentid IS NULL
            """
            cursor.execute(sql_targets)
            targets = cursor.fetchall()

        total = len(targets)
        print(f"🚀 총 {total}건의 상세 정보 수집을 시작합니다.")

        for i, row in enumerate(targets):
            cid, tid = row['contentid'], row['contenttypeid']
            item_data = None

            # --- [강화된 키 로테이션 루프] ---
            while current_key_idx < len(API_ACCOUNTS):
                acc = API_ACCOUNTS[current_key_idx]
                params = {
                    'serviceKey': unquote(acc['SERVICE_KEY']),
                    'MobileOS': 'AND',
                    'MobileApp': acc['MOBILE_APP'],
                    '_type': 'json',
                    'contentId': cid,
                    'contentTypeId': tid
                }

                try:
                    response = requests.get(DETAIL_URL, params=params, timeout=15)
                    
                    # 1. 상태 코드가 200이 아니거나, 'LIMITED' 문구가 있거나, 응답 내용이 너무 짧으면 키 교체
                    if response.status_code != 200 or "LIMITED" in response.text or len(response.text) < 100:
                        print(f"⚠️ 계정 [{acc['MOBILE_APP']}] 이상 감지 (한도초과 의심). 키를 교체합니다.")
                        current_key_idx += 1
                        continue # 즉시 다음 키로 재시도

                    # 2. JSON 파싱 시도
                    data = response.json()
                    body = data.get('response', {}).get('body', {})
                    
                    # 3. 정상 데이터 확인
                    if body and 'items' in body and body['items'] != "":
                        item_data = body['items']['item'][0]
                    
                    break # 성공했으므로 while 루프 탈출
                
                except (json.JSONDecodeError, requests.exceptions.RequestException) as e:
                    # JSON 해석 실패 시(line 1 column 1 등) 높은 확률로 한도 초과임
                    print(f"⚠️ 계정 [{acc['MOBILE_APP']}] 응답 해석 불가. 다음 키로 전환합니다.")
                    current_key_idx += 1
                    continue # 즉시 다음 키로 재시도

            # 모든 키 소진 여부 확인
            if current_key_idx >= len(API_ACCOUNTS):
                print("🚨 [중단] 사용 가능한 모든 API 키가 소진되었습니다.")
                break

            # --- [DB 저장 로직] ---
            if item_data:
                clean = map_standard_fields(item_data)
                with conn.cursor() as cursor:
                    sql_insert = """
                        INSERT INTO spot_details (
                            contentid, infocenter, restdate, usetime, parking, parkingfee, 
                            chkbabycarriage, chkpet, chkcreditcard, eventstartdate, 
                            eventenddate, playtime, firstmenu, treatmenu, usefee, 
                            expagerange, opendate, raw_data
                        ) VALUES (
                            %(contentid)s, %(infocenter)s, %(restdate)s, %(usetime)s, %(parking)s, %(parkingfee)s, 
                            %(chkbabycarriage)s, %(chkpet)s, %(chkcreditcard)s, %(eventstartdate)s, 
                            %(eventenddate)s, %(playtime)s, %(firstmenu)s, %(treatmenu)s, %(usefee)s, 
                            %(expagerange)s, %(opendate)s, %(raw_data)s
                        )
                    """
                    cursor.execute(sql_insert, clean)
                conn.commit()
                print(f"✅ [{i+1}/{total}] {cid} 저장 완료 (Key: {API_ACCOUNTS[current_key_idx]['MOBILE_APP']})")
            else:
                print(f"❓ [{i+1}/{total}] {cid} 데이터 없음 (Skip)")

            time.sleep(0.2)

    except Exception as e:
        print(f"❗ 치명적 오류 발생: {e}")
    finally:
        conn.close()
        print("🏁 수집 프로세스가 종료되었습니다.")

if __name__ == "__main__":
    sync_details()