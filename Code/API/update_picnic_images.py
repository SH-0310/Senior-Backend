import os
import sys
import requests
import json
import time
import logging
import re  # 괄호 제거용
from urllib.parse import unquote

# ✅ 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utils import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]
)

def clean_title(title):
    """검색 성공률을 높이기 위해 제목에서 괄호와 특수문자를 제거합니다."""
    if not title: return ""
    # 예: "경복궁 (사적)" -> "경복궁"
    cleaned = re.sub(r'\(.*\)', '', title) # 괄호와 그 안의 내용 삭제
    cleaned = re.sub(r'\[.*\]', '', cleaned) # 대괄호 삭제
    return cleaned.strip()

def load_api_config():
    try:
        with open('api_config.json', 'r', encoding='utf-8') as f:
            return json.load(f)[0]
    except Exception as e:
        logging.error(f"❌ 설정 로드 실패: {e}")
        return None

def update_missing_images():
    config = load_api_config()
    if not config: return

    url = "http://apis.data.go.kr/B551011/PhotoGalleryService1/gallerySyncDetailList1"
    
    try:
        conn = get_db_connection()
        conn.autocommit(True)
        
        with conn.cursor() as cursor:
            # 1431개를 다 돌리기 위해 LIMIT을 넉넉히 잡거나 제거하세요.
            cursor.execute("SELECT contentid, title FROM picnic_spots WHERE firstimage IS NULL OR firstimage = '' LIMIT 100")
            targets = cursor.fetchall()
            logging.info(f"🔎 진짜 데이터로 수집 시작! 대상: {len(targets)}개")

            for row in targets:
                # ✅ [핵심수정] 데이터 타입에 따라 안전하게 값 추출
                if isinstance(row, dict):
                    contentid = row['contentid']
                    raw_title = row['title']
                else:
                    contentid = row[0]
                    raw_title = row[1]

                # 아까처럼 'title'이라는 글자가 들어오는 걸 방지
                if raw_title.lower() == 'title': continue

                # 검색어 최적화 (사천아이(관람차) -> 사천아이)
                search_title = clean_title(raw_title)

                params = {
                    "serviceKey": config["SERVICE_KEY"],
                    "MobileApp": config["MOBILE_APP"], "MobileOS": "ETC",
                    "numOfRows": 1, "pageNo": 1, "_type": "json", 
                    "title": search_title
                }

                try:
                    res = requests.get(url, params=params, timeout=15)
                    if res.status_code != 200: continue

                    data = res.json()
                    # 안전하게 경로 타기
                    items_node = data.get("response", {}).get("body", {}).get("items", "")
                    
                    if isinstance(items_node, dict):
                        items = items_node.get("item", [])
                        if items:
                            img = items[0].get("galWebImageUrl")
                            cursor.execute("UPDATE picnic_spots SET firstimage = %s WHERE contentid = %s", (img, contentid))
                            logging.info(f"✅ [{raw_title}] -> 이미지 자동 매칭 성공!")
                        else:
                            logging.info(f"ℹ️ [{search_title}] 사진첩에 데이터 없음")
                    else:
                        logging.info(f"ℹ️ [{search_title}] 결과 없음")

                except Exception as e:
                    logging.error(f"⚠️ [{raw_title}] 처리 중 오류: {e}")
                
                time.sleep(0.2) # 속도를 조금 높였습니다.

        conn.close()
        logging.info("🏁 수집 작업이 종료되었습니다.")
    except Exception as e:
        logging.error(f"❌ DB 오류: {e}")

if __name__ == "__main__":
    update_missing_images()