import time, requests, logging, json
from datetime import datetime, timedelta
from utils import extract_all_keywords, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "노랑풍선"
YB_PHONE = "1544-2288"
# 상세 페이지 도메인
BASE_URL = "https://prdt.ybtour.co.kr" 
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/home/ubuntu/Senior/Code/yb_crawler.log", encoding='utf-8'), 
        logging.StreamHandler()
    ]
)

def send_telegram_msg(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try: requests.post(url, data=payload, timeout=10)
    except: pass

def run_collection():
    start_time = datetime.now()
    this_month = start_time.strftime("%Y%m")
    next_month = (start_time.replace(day=28) + timedelta(days=5)).strftime("%Y%m")
    
    # API 비교용 8자리 문자열
    today_str_api = start_time.strftime("%Y%m%d")
    limit_str_api = (start_time + timedelta(days=30)).strftime("%Y%m%d")
    
    logging.info(f"🚀 {AGENCY_NAME} 전수 수집 시작 (범위: {today_str_api} ~ {limit_str_api})")
    
    conn = get_db_connection()
    conn.autocommit(True)
    stats = {"total_rprs": 0, "saved_schedules": 0, "deleted_tours": 0}

    try:
        with conn.cursor() as cursor:
            # 1. 부모 리스트 API 호출
            parent_api = "https://prdt.ybtour.co.kr/prdt/api/goods/by-menu/ADBJ000"
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.ybtour.co.kr/'
            }
            
            res = requests.get(parent_api, headers=headers, timeout=15)
            products = res.json().get("body", [])

            for p_doc in products:
                if p_doc.get("evDayCntList") != "1": continue

                parent_code = p_doc.get("goodsCd")
                parent_title = p_doc.get("goodsNm")
                main_img_url = p_doc.get("imageThum3", "")
                location = parent_title.split(' ')[0].replace('#', '') if ' ' in parent_title else "국내"
                # ✅ [추가] 태그 추출 (부모/자식 공통 사용)
                tags = extract_all_keywords(parent_title)
                description = p_doc.get("goodsTourInfo") or parent_title

                # 💾 1) 부모 DB 저장 (tours) - category 대신 tags 기반으로 업데이트 가능
                cursor.execute("""
                    INSERT INTO tours (product_code, reference_code, title, description, main_image_url, location, collected_at, agency, category, phone, is_priority)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
                    ON DUPLICATE KEY UPDATE 
                        title=%s, description=%s, main_image_url=%s, location=%s, category=%s, collected_at=%s
                """, (
                    # INSERT용 데이터 10개
                    parent_code, parent_code, parent_title, description, main_img_url, 
                    location, start_time, AGENCY_NAME, tags, YB_PHONE,
                    # UPDATE용 데이터 6개
                    parent_title, description, main_img_url, location, tags, start_time
                ))
                stats["total_rprs"] += 1
                
                logging.info(f"📦 [{stats['total_rprs']}] {parent_title[:20]}...")

                # 💾 2) 자식 일정 수집
                for target_month in [this_month, next_month]:
                    event_api = f"https://prdt.ybtour.co.kr/prdt/api/event/by-goods/{parent_code}/ADBJ000/{target_month}"
                    try:
                        e_res = requests.get(event_api, headers=headers, timeout=10)
                        events = e_res.json().get("body", []) or []

                        for event in events:
                            dep_date_raw = event.get("outStartDt") # 예: "20260128"
                            if not dep_date_raw or not (today_str_api <= dep_date_raw <= limit_str_api):
                                continue
                            
                            # ✅ [수정 핵심 1] 날짜 형식 변환 (20260128 -> 2026-01-28)
                            dep_date_db = f"{dep_date_raw[:4]}-{dep_date_raw[4:6]}-{dep_date_raw[6:]}"
                            
                            price = event.get("adtPrice", 0)
                            status_nm = event.get("evProgDiviNm")
                            error_msg = None if status_nm in ["예약진행", "예약가능", "출발확정"] else status_nm
                            
                            relative_link = event.get("weblinkurl")
                            booking_url = f"{BASE_URL}{relative_link}" if relative_link else f"{BASE_URL}/product/goods.do?goodsCd={parent_code}"

                            # 💾 [수정 핵심 2] 자식 DB 저장 (tags 컬럼 및 하이픈 날짜 반영)
                            cursor.execute("""
                                INSERT INTO tour_schedules (
                                    product_code, title, departure_date, price_text, 
                                    booking_url, updated_at, last_verified_at, error_msg, tags
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE 
                                    price_text=%s, updated_at=%s, last_verified_at=%s, error_msg=%s, tags=%s,
                                    departure_date=%s
                            """, (parent_code, parent_title, dep_date_db, str(price), 
                                  booking_url, start_time, start_time, error_msg, tags,
                                  str(price), start_time, start_time, error_msg, tags, dep_date_db))
                            stats["saved_schedules"] += 1

                    except Exception as e:
                        logging.error(f"   ⚠️ 일정 파싱 실패 ({parent_code}): {e}")
                    
                    time.sleep(0.05)

            # 🧹 3) Cleanup & Logging
            cleanup_limit = start_time - timedelta(minutes=30)
            cursor.execute("DELETE FROM tours WHERE agency = %s AND collected_at < %s", (AGENCY_NAME, cleanup_limit))
            stats["deleted_tours"] = cursor.rowcount

            finish_time = datetime.now()
            cursor.execute("""
                INSERT INTO crawler_logs (agency_name, status, collected_count, crawled_at, message)
                VALUES (%s, %s, %s, %s, %s)
            """, (AGENCY_NAME, "SUCCESS", stats["saved_schedules"], finish_time, f"부모 {stats['total_rprs']}종 수집 완료"))

        send_telegram_msg(f"🤖 [{AGENCY_NAME} 완료]\n📦 부모: {stats['total_rprs']}종\n🔹 일정: {stats['saved_schedules']}건\n🧹 삭제: {stats['deleted_tours']}종")

    except Exception as e:
        logging.error(f"❌ 치명적 오류: {e}")
        send_telegram_msg(f"❌ {AGENCY_NAME} 오류: {str(e)[:100]}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    run_collection()
