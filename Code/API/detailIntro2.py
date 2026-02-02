import requests
import pymysql
from urllib.parse import unquote
import time
import json

# --- [DB 연결 설정: 유저님의 정보 유지] ---
def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# --- [TourAPI 설정] ---
# 상세 정보 조회를 위한 URL로 변경됨
DETAIL_URL = "http://apis.data.go.kr/B551011/KorService2/detailIntro2"
SERVICE_KEY = "eb08c1ad2a7c050ba576b0d3669ecb5d82c5484660c0ec6df85fae3b563a1c2a"

def map_standard_fields(item):
    """
    TourAPI의 타입별로 다른 필드명들을 DB 컬럼에 맞게 통합 매핑합니다.
    명세서의 각 항목구분(관광지, 음식점 등)에 따른 필드명을 모두 체크합니다.
    """
    return {
        'contentid': item.get('contentid'),
        # 공통 정보 (타입별 필드명 우선순위 매핑)
        'infocenter': item.get('infocenter') or item.get('infocenterculture') or item.get('infocenterfood') or item.get('infocenterleports') or item.get('infocentershopping') or "",
        'restdate': item.get('restdate') or item.get('restdateculture') or item.get('restdatefood') or item.get('restdateleports') or item.get('restdateshopping') or "",
        'usetime': item.get('usetime') or item.get('usetimeculture') or item.get('opentimefood') or item.get('opentime') or item.get('usetimeleports') or "",
        'parking': item.get('parking') or item.get('parkingculture') or item.get('parkingfood') or item.get('parkingleports') or item.get('parkingshopping') or "",
        'parkingfee': item.get('parkingfee') or item.get('parkingfeeleports') or "",
        'chkbabycarriage': item.get('chkbabycarriage') or item.get('chkbabycarriageculture') or item.get('chkbabycarriageleports') or item.get('chkbabycarriageshopping') or "",
        'chkpet': item.get('chkpet') or item.get('chkpetculture') or item.get('chkpetleports') or item.get('chkpetshopping') or "",
        'chkcreditcard': item.get('chkcreditcard') or item.get('chkcreditcardculture') or item.get('chkcreditcardfood') or item.get('chkcreditcardleports') or item.get('chkcreditcardshopping') or "",
        
        # 특화 정보 (축제, 음식점 등)
        'eventstartdate': item.get('eventstartdate') or "",
        'eventenddate': item.get('eventenddate') or "",
        'playtime': item.get('playtime') or "",
        'firstmenu': item.get('firstmenu') or "",
        'treatmenu': item.get('treatmenu') or "",
        'usefee': item.get('usefee') or item.get('usetimefestival') or item.get('usefeeleports') or "",
        'expagerange': item.get('expagerange') or item.get('agelimit') or "",
        'opendate': item.get('opendate') or item.get('opendatefood') or "",
        
        # 원본 전체 데이터 백업 (JSON)
        'raw_data': json.dumps(item, ensure_ascii=False)
    }

def sync_details():
    conn = get_db_connection()
    try:
        # 1. 수집 대상 선정 (기본 정보는 있지만 상세 정보는 없는 contentid 추출)
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
            cid = row['contentid']
            tid = row['contenttypeid']
            
            params = {
                'serviceKey': unquote(SERVICE_KEY),
                'MobileOS': 'AND',
                'MobileApp': 'OneDayPicnic',
                '_type': 'json',
                'contentId': cid,
                'contentTypeId': tid
            }

            # --- [재시도 로직 포함된 요청] ---
            max_retries = 3
            item_data = None
            
            for attempt in range(max_retries):
                try:
                    response = requests.get(DETAIL_URL, params=params, timeout=20)
                    data = response.json()
                    body = data.get('response', {}).get('body', {})
                    if body and 'items' in body and body['items'] != "":
                        item_data = body['items']['item'][0]
                    break
                except Exception as e:
                    print(f"⚠️ {cid} ({attempt+1}차 시도 실패): {e}")
                    time.sleep(3)
            
            if item_data:
                # 데이터 표준화 매핑
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
                print(f"✅ [{i+1}/{total}] {cid} 저장 완료")
            else:
                print(f"❓ [{i+1}/{total}] {cid} 데이터 없음 (Skip)")

            # 과부하 방지용 짧은 휴식
            time.sleep(0.2)

    except Exception as e:
        print(f"❗ 치명적 오류 발생: {e}")
    finally:
        conn.close()
        print("🏁 모든 수집 작업이 종료되었습니다.")

if __name__ == "__main__":
    sync_details()