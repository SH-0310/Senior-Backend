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
    
    # ✅ 배치 사이즈 조절 (원하시는 만큼 숫자를 키우세요)
    BATCH_SIZE = 2000 

    while True:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # ✅ 대량 수집을 위한 쿼리
                sql_targets = f"""
                    SELECT contentid, contenttypeid FROM spot_commons
                    WHERE contenttypeid IN (12, 14, 15, 25, 28)
                    AND contentid NOT IN (SELECT DISTINCT contentid FROM spot_info)
                    LIMIT {BATCH_SIZE}
                """
                cursor.execute(sql_targets)
                targets = cursor.fetchall()

            if not targets:
                print("✨ [완료] 수집할 새로운 데이터가 더 이상 없습니다!")
                break

            total_targets = len(targets)
            print(f"\n🚀 {total_targets}건 수집 시작 (현재 API 키 인덱스: {current_key_idx})")

            for index, row in enumerate(targets, 1):
                # 모든 키 소진 시 종료
                if current_key_idx >= len(API_ACCOUNTS):
                    print("\n🚨 [중단] 사용 가능한 모든 API 키를 소진했습니다.")
                    return

                cid, tid = row['contentid'], row['contenttypeid']
                item_list = []

                # API 키를 바꿔가며 호출
                while current_key_idx < len(API_ACCOUNTS):
                    acc = API_ACCOUNTS[current_key_idx]
                    params = {
                        'serviceKey': unquote(acc['SERVICE_KEY']),
                        'MobileOS': 'ETC', 'MobileApp': 'AppTest',
                        '_type': 'json', 'contentId': cid, 'contentTypeId': tid
                    }

                    try:
                        res = requests.get(BASE_URL, params=params, timeout=20)
                        data = res.json()
                        body = data.get('response', {}).get('body', {})
                        items_container = body.get('items', '')
                        
                        if items_container and 'item' in items_container:
                            item_list = items_container['item']
                            if isinstance(item_list, dict): item_list = [item_list]
                        
                        # 성공 시(데이터가 없어도 응답은 받은 것이므로) 루프 탈출
                        break 
                    except Exception:
                        # 타임아웃이나 한도초과 발생 시 키 교체
                        print(f"\n⚠️ 키 [{acc['MOBILE_APP']}] 문제 발생. 다음 키로 전환...")
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

                # ✅ 진행률 표시 (10건마다 출력)
                if index % 10 == 0 or index == total_targets:
                    print(f"\r📦 진행도: {index}/{total_targets} ({round(index/total_targets*100, 1)}%)", end="")

                time.sleep(0.05) # 간격을 약간 줄여 속도 향상

        finally:
            conn.close()
            print(f"\n✅ {BATCH_SIZE}개 배치 완료 및 DB 저장 성공. 다음 세션을 시작합니다.")

if __name__ == "__main__":
    sync_all_info_master()