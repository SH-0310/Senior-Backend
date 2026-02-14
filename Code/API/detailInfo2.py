import requests
import pymysql
from urllib.parse import unquote
import time
import json
import os

# --- [기본 설정 함수] ---
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
    
    BATCH_SIZE = 2000 

    while True:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 1. 현재 보유 중인 '장소' 개수 (중복 제거)
                cursor.execute("SELECT COUNT(DISTINCT contentid) as cnt FROM spot_info")
                current_owned_spots = cursor.fetchone()['cnt']
                
                # 2. 현재 테이블에 쌓인 전체 '행(Row)' 개수
                cursor.execute("SELECT COUNT(*) as row_cnt FROM spot_info")
                current_rows = cursor.fetchone()['row_cnt']

                # 3. 수집해야 할 잔여 장소 수량
                sql_pending_count = """
                    SELECT COUNT(*) as cnt FROM spot_commons
                    WHERE contenttypeid IN (12, 14, 15, 25, 28)
                    AND contentid NOT IN (SELECT DISTINCT contentid FROM spot_info)
                """
                cursor.execute(sql_pending_count)
                total_pending = cursor.fetchone()['cnt']

                # 4. 전체 목표 장소 수 (보유 장소 + 잔여 장소)
                total_goal = current_owned_spots + total_pending

                if total_pending == 0:
                    print(f"\n✨ [완료] 모든 장소 수집 완료! (총 {current_owned_spots}개 장소, {current_rows}개 행 보유)")
                    break

                # ✅ 5. 실제 수집할 대상(targets) 가져오기 (이 부분이 누락되면 에러납니다)
                sql_targets = f"""
                    SELECT contentid, contenttypeid FROM spot_commons
                    WHERE contenttypeid IN (12, 14, 15, 25, 28)
                    AND contentid NOT IN (SELECT DISTINCT contentid FROM spot_info)
                    LIMIT {BATCH_SIZE}
                """
                cursor.execute(sql_targets)
                targets = cursor.fetchall()
                total_targets = len(targets)

            print(f"\n📊 [수집 통계] 총 목표: {total_goal}개 장소")
            print(f"✅ 현재 보유: {current_owned_spots}개 장소 (총 {current_rows}개 데이터 행 저장됨)")
            print(f"⏳ 남은 수집: {total_pending}개 장소")
            print(f"🚀 이번 배치({total_targets}개) 수집 시작...")

            for index, row in enumerate(targets, 1):
                if current_key_idx >= len(API_ACCOUNTS):
                    print("\n🚨 [중단] 모든 API 키 소진.")
                    return

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
                        res = requests.get(BASE_URL, params=params, timeout=20)
                        
                        if res.status_code == 429:
                            print(f"\n🚦 [429] 과부하! 15초 대기...")
                            time.sleep(15)
                            continue

                        data = res.json()
                        body = data.get('response', {}).get('body', {})
                        items_container = body.get('items', '')
                        
                        if items_container and 'item' in items_container:
                            item_list = items_container['item']
                            if isinstance(item_list, dict): item_list = [item_list]
                        break 
                    except Exception:
                        current_key_idx += 1
                        continue

                # DB 저장
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

                # ✅ 진행률 실시간 업데이트 (보유 장소 수 기준)
                if index % 10 == 0 or index == total_targets:
                    realtime_owned = current_owned_spots + index
                    progress_percent = round((realtime_owned / total_goal) * 100, 1)
                    print(f"\r📈 실시간 현황: [{progress_percent}%] 장소 {realtime_owned} / {total_goal} (잔여 {total_pending - index}건)", end="")

                time.sleep(0.1)

        finally:
            conn.close()
            print(f"\n✅ {BATCH_SIZE}개 배치 완료. 다음 배치를 준비합니다.")

if __name__ == "__main__":
    sync_all_info_master()