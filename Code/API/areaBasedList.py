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

# 계정 리스트 로드 및 인덱스 초기화
API_ACCOUNTS = load_api_configs()
current_key_idx = 0 

# --- [2. DB 연결 설정] ---
def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# --- [3. 메인 수집 함수] ---
def sync_data():
    global current_key_idx
    conn = get_db_connection()
    BASE_URL = "http://apis.data.go.kr/B551011/KorService2/areaBasedList2"
    TARGET_CONTENT_TYPES = [12, 14, 15]

    try:
        for content_type in TARGET_CONTENT_TYPES:
            print(f"🚀 카테고리 {content_type} 수집 시작...")
            page = 11 if content_type == 12 else 1
            
            while True:
                success_page = False  # 해당 페이지 수집 성공 여부
                
                # --- [키 로테이션 루프] ---
                while current_key_idx < len(API_ACCOUNTS):
                    acc = API_ACCOUNTS[current_key_idx]
                    params = {
                        'serviceKey': unquote(acc['SERVICE_KEY']),
                        'numOfRows': 100,
                        'pageNo': page,
                        'MobileOS': 'AND',
                        'MobileApp': acc['MOBILE_APP'],
                        '_type': 'json',
                        'contentTypeId': content_type,
                        'arrange': 'O'
                    }
                    
                    try:
                        # 타임아웃을 넉넉히 30초 설정
                        response = requests.get(BASE_URL, params=params, timeout=30)
                        
                        # A. 한도 초과 및 비정상 응답 체크 (지난번 배운 로직 적용)
                        if response.status_code != 200 or "LIMITED" in response.text or len(response.text) < 150:
                            reason = "한도초과 의심" if "LIMITED" in response.text else "비정상 응답"
                            print(f"⚠️ 계정 [{acc['MOBILE_APP']}] {reason}. 키를 교체합니다.")
                            current_key_idx += 1
                            continue # 다음 키로 같은 페이지 재시도
                        
                        # B. 정상 데이터 파싱
                        data = response.json()
                        body = data.get('response', {}).get('body', {})
                        
                        # 데이터가 없는 경우 (수집 완료)
                        if not body or 'items' not in body or body['items'] == "":
                            print(f"✅ {content_type} 카테고리 전체 수집 완료.")
                            success_page = "FINISH"
                            break
                        
                        items = body['items']['item']
                        
                        # DB 저장 로직
                        with conn.cursor() as cursor:
                            sql = """
                                INSERT INTO picnic_spots (
                                    contentid, contenttypeid, title, addr1, addr2, zipcode, areacode, sigungucode,
                                    mapx, mapy, mlevel, cat1, cat2, cat3, firstimage, firstimage2, cpyrhtDivCd,
                                    tel, createdtime, modifiedtime, lDongRegnCd, lDongSignguCd, lclsSystm1, 
                                    lclsSystm2, lclsSystm3
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    modifiedtime = VALUES(modifiedtime), title = VALUES(title), 
                                    addr1 = VALUES(addr1), firstimage = VALUES(firstimage),
                                    mapx = VALUES(mapx), mapy = VALUES(mapy)
                            """
                            for item in items:
                                mx = float(item['mapx']) if item.get('mapx') else None
                                my = float(item['mapy']) if item.get('mapy') else None
                                cursor.execute(sql, (
                                    item.get('contentid'), item.get('contenttypeid'), item.get('title'),
                                    item.get('addr1', ''), item.get('addr2', ''), item.get('zipcode', ''),
                                    item.get('areacode', ''), item.get('sigungucode', ''), mx, my, 
                                    item.get('mlevel', ''), item.get('cat1', ''), item.get('cat2', ''), 
                                    item.get('cat3', ''), item.get('firstimage', ''), item.get('firstimage2', ''),
                                    item.get('cpyrhtDivCd', ''), item.get('tel', ''), item.get('createdtime', ''),
                                    item.get('modifiedtime', ''), item.get('lDongRegnCd', ''), item.get('lDongSignguCd', ''),
                                    item.get('lclsSystm1', ''), item.get('lclsSystm2', ''), item.get('lclsSystm3', '')
                                ))
                            conn.commit()
                        
                        print(f"📦 {page}페이지 저장 완료 (ID: {items[0]['contentid']} 등 {len(items)}개) - Key: {acc['MOBILE_APP']}")
                        success_page = True
                        break # 성공했으므로 키 로테이션 루프 탈출
                        
                    except (json.JSONDecodeError, requests.exceptions.RequestException) as e:
                        print(f"⚠️ {page}페이지 요청 중 기술적 오류 발생: {e}. 키 교체를 시도합니다.")
                        current_key_idx += 1
                        continue

                # 모든 키를 다 썼다면 종료
                if current_key_idx >= len(API_ACCOUNTS):
                    print("🚨 [중단] 모든 API 키를 소진했습니다. 내일 다시 실행하세요.")
                    return

                # 다음 페이지로 이동 혹은 카테고리 종료
                if success_page == "FINISH":
                    break
                elif success_page:
                    page += 1
                    time.sleep(0.3)
                else:
                    print(f"❌ {page}페이지 수집 실패. 다음 단계로 넘어갑니다.")
                    break

    except Exception as e:
        print(f"❗ 치명적 오류 발생: {e}")
    finally:
        conn.close()
        print("🏁 수집 프로세스가 종료되었습니다.")

if __name__ == "__main__":
    sync_data()