import os
import sys
import requests
import json
import time
import logging
import re
import urllib3

# ✅ SSL 경고 제거 및 로깅 설정
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("full_collection_v2.log", encoding='utf-8'), logging.StreamHandler()]
)

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utils import get_db_connection

def check_url_alive(url):
    if not url: return None
    headers = {"User-Agent": "Mozilla/5.0"}
    test_urls = [url.replace("http://", "https://"), url]
    for t_url in test_urls:
        try:
            res = requests.head(t_url, timeout=5, allow_redirects=True, headers=headers, verify=False)
            if res.status_code == 200: return t_url
        except: continue
    return None

def clean_title(title):
    if not title: return ""
    return re.sub(r'\(.*\)|\[.*\]', '', title).strip()

def update_all_missing_images():
    try:
        with open('api_config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)[0]
    except Exception as e:
        logging.error(f"❌ 설정 로드 실패: {e}")
        return

    api_url = "http://apis.data.go.kr/B551011/PhotoGalleryService1/gallerySyncDetailList1"
    conn = get_db_connection()
    conn.autocommit(True)
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT contentid, title, addr1 FROM picnic_spots WHERE firstimage IS NULL OR firstimage = ''")
            targets = cursor.fetchall()
            total = len(targets)
            logging.info(f"🏁 정밀 수집 재시작! 대상: 총 {total}개")

            success_count = 0
            skip_count = 0

            for idx, row in enumerate(targets, 1):
                c_id, raw_title, addr1 = (row['contentid'], row['title'], row.get('addr1', '')) if isinstance(row, dict) else (row[0], row[1], row[2])
                search_title = clean_title(raw_title)
                
                params = {
                    "serviceKey": config["SERVICE_KEY"],
                    "MobileApp": config["MOBILE_APP"], "MobileOS": "ETC",
                    "numOfRows": 15, "pageNo": 1, "_type": "json", "title": search_title
                }

                try:
                    res = requests.get(api_url, params=params, timeout=20)
                    data = res.json()
                    
                    # ✅ [핵심수정] 단계별 안전한 데이터 추출 로직
                    response = data.get("response", {})
                    body = response.get("body", {}) if isinstance(response, dict) else {}
                    items_node = body.get("items", {}) if isinstance(body, dict) else {}
                    
                    # items가 문자열("")로 오면 검색 결과가 없는 것이므로 스킵
                    if not isinstance(items_node, dict):
                        logging.info(f"[{idx}/{total}] ℹ️ {raw_title}: 검색 결과 없음 (items is string)")
                        skip_count += 1
                        continue

                    items = items_node.get("item", [])
                    if not isinstance(items, list): items = [items] if items else []

                    final_img = None
                    addr_parts = addr1.split()[:2]
                    
                    for item in items:
                        if not isinstance(item, dict): continue # 방어 코드
                        photo_loc = item.get("galPhotographyLocation", "")
                        candidate_url = item.get("galWebImageUrl")
                        
                        if any(part in photo_loc for part in addr_parts if len(part) > 1):
                            alive_url = check_url_alive(candidate_url)
                            if alive_url:
                                final_img = alive_url; break

                    if not final_img: # 지역 매칭 실패 시 1번 사진 생존 확인
                        for item in items:
                            if not isinstance(item, dict): continue
                            alive_url = check_url_alive(item.get("galWebImageUrl"))
                            if alive_url:
                                final_img = alive_url; break

                    if final_img:
                        cursor.execute("UPDATE picnic_spots SET firstimage = %s, last_sync_at = NOW() WHERE contentid = %s", (final_img, c_id))
                        logging.info(f"[{idx}/{total}] ✅ {raw_title}: 업데이트 성공")
                        success_count += 1
                    else:
                        logging.info(f"[{idx}/{total}] ❌ {raw_title}: 유효한 사진 없음")
                        skip_count += 1

                except Exception as e:
                    logging.error(f"[{idx}/{total}] ⚠️ {raw_title} 에러: {str(e)}")
                
                time.sleep(0.3)

            logging.info(f"✨ 최종 결과 - 성공: {success_count}, 실패/스킵: {skip_count}")

    finally:
        conn.close()

if __name__ == "__main__":
    update_all_missing_images()