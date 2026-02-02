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
    conn = get_db_connection()
    BASE_URL = "http://apis.data.go.kr/B551011/KorService2/detailInfo2"

    try:
        with conn.cursor() as cursor:
            # ✅ 타겟: 관광지(12), 문화시설(14), 축제(15), 여행코스(25), 레포츠(28)
            # spot_info에 아직 데이터가 없는 contentid들만 추출
            sql_targets = """
                SELECT contentid, contenttypeid FROM spot_commons
                WHERE contenttypeid IN (12, 14, 15, 25, 28)
                AND contentid NOT IN (SELECT DISTINCT contentid FROM spot_info)
                LIMIT 500
            """
            cursor.execute(sql_targets)
            targets = cursor.fetchall()

        if not targets:
            print("✨ 수집할 새로운 상세 정보가 없습니다.")
            return

        print(f"🚀 총 {len(targets)}건의 상세 정보(spot_info) 수집을 시작합니다.")

        for row in targets:
            cid, tid = row['contentid'], row['contenttypeid']
            item_list = []

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
                        # 아이템이 하나일 때도 리스트로 변환하여 처리
                        item_list = items_container['item']
                        if isinstance(item_list, dict): item_list = [item_list]
                    break
                except:
                    current_key_idx += 1; continue

            if item_list:
                with conn.cursor() as cursor:
                    for item in item_list:
                        # ✅ 테이블 이름 spot_info 적용
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
                print(f"✅ ID {cid} 상세 정보 {len(item_list)}건 저장 완료")
            
            time.sleep(0.15)

    finally:
        conn.close()
        print("🏁 spot_info 테이블 수집 작업이 완료되었습니다.")

if __name__ == "__main__":
    sync_all_info_master()