import requests
import pymysql
from urllib.parse import unquote
import time

# --- [DB 연결 설정: 유저님의 app.py 정보 반영] ---
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
BASE_URL = "http://apis.data.go.kr/B551011/KorService2/areaBasedList2"
SERVICE_KEY = "eb08c1ad2a7c050ba576b0d3669ecb5d82c5484660c0ec6df85fae3b563a1c2a"
TARGET_CONTENT_TYPES = [12, 14, 15]

def sync_data():
    conn = get_db_connection()
    try:
        for content_type in TARGET_CONTENT_TYPES:
            print(f"🚀 카테고리 {content_type} 수집 시작...")
            if content_type == 12:
                page = 11
            else:
                page = 1
            
            while True:
                params = {
                    'serviceKey': unquote(SERVICE_KEY),
                    'numOfRows': 100,
                    'pageNo': page,
                    'MobileOS': 'AND',
                    'MobileApp': 'OneDayPicnic',
                    '_type': 'json',
                    'contentTypeId': content_type,
                    'arrange': 'O'
                }
                
                # --- [수정된 요청 로직: 재시도 포함] ---
                max_retries = 3
                success = False
                
                for attempt in range(max_retries):
                    try:
                        # timeout을 30초로 늘림
                        response = requests.get(BASE_URL, params=params, timeout=30)
                        data = response.json()
                        success = True
                        break # 성공하면 재시도 루프 탈출
                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                        print(f"⚠️ {page}페이지 {attempt+1}차 시도 실패 (타임아웃)... 5초 후 재시도")
                        time.sleep(5)
                
                if not success:
                    print(f"❌ {page}페이지 수집 포기. 다음 카테고리로 넘어가거나 수동 확인이 필요합니다.")
                    break
                # ---------------------------------------

                body = data.get('response', {}).get('body', {})
                if not body or 'items' not in body or body['items'] == "":
                    print(f"✅ {content_type} 수집 완료.")
                    break
                
                items = body['items']['item']
                
                with conn.cursor() as cursor:
                    # 25개 필드를 한 번에 저장/업데이트하는 SQL (ON DUPLICATE KEY UPDATE)
                    sql = """
                        INSERT INTO picnic_spots (
                            contentid, contenttypeid, title, addr1, addr2, zipcode, areacode, sigungucode,
                            mapx, mapy, mlevel, cat1, cat2, cat3, firstimage, firstimage2, cpyrhtDivCd,
                            tel, createdtime, modifiedtime, lDongRegnCd, lDongSignguCd, lclsSystm1, 
                            lclsSystm2, lclsSystm3
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON DUPLICATE KEY UPDATE
                            modifiedtime = VALUES(modifiedtime),
                            title = VALUES(title),
                            addr1 = VALUES(addr1),
                            firstimage = VALUES(firstimage),
                            firstimage2 = VALUES(firstimage2),
                            mapx = VALUES(mapx),
                            mapy = VALUES(mapy)
                    """
                    
                    for item in items:
                        # 좌표 데이터 정제 (float 변환)
                        mx = float(item['mapx']) if item.get('mapx') else None
                        my = float(item['mapy']) if item.get('mapy') else None
                        
                        cursor.execute(sql, (
                            item.get('contentid'), item.get('contenttypeid'), item.get('title'),
                            item.get('addr1', ''), item.get('addr2', ''), item.get('zipcode', ''),
                            item.get('areacode', ''), item.get('sigungucode', ''),
                            mx, my, item.get('mlevel', ''),
                            item.get('cat1', ''), item.get('cat2', ''), item.get('cat3', ''),
                            item.get('firstimage', ''), item.get('firstimage2', ''),
                            item.get('cpyrhtDivCd', ''), item.get('tel', ''),
                            item.get('createdtime', ''), item.get('modifiedtime', ''),
                            item.get('lDongRegnCd', ''), item.get('lDongSignguCd', ''),
                            item.get('lclsSystm1', ''), item.get('lclsSystm2', ''), item.get('lclsSystm3', '')
                        ))
                
                conn.commit() # 페이지 단위 커밋
                print(f"📦 {page}페이지 저장 완료 (ID: {items[0]['contentid']} 등 {len(items)}개)")
                
                if page * 100 >= body.get('totalCount', 0):
                    break
                page += 1
                time.sleep(0.3)
                
    except Exception as e:
        print(f"❗ 오류 발생: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    sync_data()