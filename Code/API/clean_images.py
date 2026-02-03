import os
import sys
import requests
import json
import time
import logging
import re

# 경로 설정
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utils import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

def check_url_alive(url):
    if not url: return None
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    
    # ✅ 팁: 최근 공공데이터 사진은 https에서만 열리는 경우가 많습니다.
    # http 주소를 https로 우선 변환하여 테스트합니다.
    target_url = url.replace("http://", "https://")
    
    try:
        # 주소 확인 타임아웃 15초로 연장
        res = requests.head(target_url, timeout=15, allow_redirects=True, headers=headers, verify=False)
        if res.status_code == 200:
            return target_url
        
        # https 실패 시 원본 http로 재시도
        res = requests.head(url, timeout=10, allow_redirects=True, headers=headers)
        if res.status_code == 200:
            return url
    except:
        pass
    return None

def run_recent_cleanup():
    conn = get_db_connection()
    conn.autocommit(True)
    
    with open('api_config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)[0]

    api_url = "http://apis.data.go.kr/B551011/PhotoGalleryService1/gallerySyncDetailList1"

    try:
        with conn.cursor() as cursor:
            # 최근 2시간 이내 작업분 (범위 확장)
            cursor.execute("""
                SELECT contentid, title, firstimage 
                FROM picnic_spots 
                WHERE last_sync_at >= NOW() - INTERVAL 2 HOUR
            """)
            targets = cursor.fetchall()
            
            logging.info(f"🔎 총 {len(targets)}개의 장소 정밀 재검토 (초강력 버전)")

            for row in targets:
                c_id = row['contentid'] if isinstance(row, dict) else row[0]
                title = row['title'] if isinstance(row, dict) else row[1]
                current_img = row['firstimage'] if isinstance(row, dict) else row[2]

                # 1. 기존 주소 검증 (이미 비워졌다면 수집 모드로 바로 진입)
                if current_img:
                    alive_url = check_url_alive(current_img)
                    if alive_url:
                        logging.info(f"  ✅ [{title}] 이미 정상")
                        continue

                # 2. 404거나 비어있는 경우 재수집
                logging.warning(f"  🚨 [{title}] 유효한 사진 찾는 중...")
                search_title = re.sub(r'\(.*\)|\[.*\]', '', title).strip()
                
                params = {
                    "serviceKey": config["SERVICE_KEY"],
                    "MobileApp": config["MOBILE_APP"], "MobileOS": "ETC",
                    "numOfRows": 15, "pageNo": 1, "_type": "json", "title": search_title
                }

                try:
                    # ✅ 타임아웃을 60초로 대폭 연장 (서버가 느려도 끝까지 기다림)
                    res = requests.get(api_url, params=params, timeout=60)
                    res_json = res.json()
                    items_node = res_json.get("response", {}).get("body", {}).get("items", "")
                    
                    if not isinstance(items_node, dict):
                        logging.info(f"  ℹ️ [{title}] API 데이터 없음")
                        continue

                    items = items_node.get("item", [])
                    if not isinstance(items, list): items = [items]

                    new_img = None
                    for item in items:
                        val_url = check_url_alive(item.get("galWebImageUrl"))
                        if val_url:
                            new_img = val_url
                            break
                    
                    if new_img:
                        cursor.execute("UPDATE picnic_spots SET firstimage = %s WHERE contentid = %s", (new_img, c_id))
                        logging.info(f"  ✨ [{title}] 사진 복구 완료!")
                    else:
                        # ⚠️ 404가 확실할 때만 비워둠
                        logging.error(f"  ❌ [{title}] 모든 사진 만료 확인")

                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                    logging.error(f"  ⏱️ [{title}] 서버 응답 없음 (건너뜀)")
                except Exception as e:
                    logging.error(f"  ⚠️ [{title}] 에러: {str(e)[:50]}")
                
                time.sleep(0.5)

    finally:
        conn.close()

if __name__ == "__main__":
    run_recent_cleanup()