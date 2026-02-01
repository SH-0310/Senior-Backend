import time, requests, logging, json
from datetime import datetime, timedelta
from utils import classify_categories, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "인터파크투어"
IP_PHONE = "1588-3443"
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/home/ubuntu/Senior/Code/interpark_crawler.log", encoding='utf-8'), 
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
    today_str = start_time.strftime("%Y%m%d")
    limit_str = (start_time + timedelta(days=30)).strftime("%Y%m%d")
    
    logging.info(f"🚀 {AGENCY_NAME} 전수 수집 시작 (필터: {today_str} ~ {limit_str})")
    
    api_url = "https://travel.interpark.com/api-package/search"
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://travel.interpark.com',
        'Referer': 'https://travel.interpark.com/tour/search'
    }

    stats = {"total_rprs": 0, "saved_schedules": 0, "deleted_tours": 0}
    conn = get_db_connection()
    conn.autocommit(True)

    try:
        with conn.cursor() as cursor:
            # 1. API 호출
            payload = {
                "q": "당일여행", "domain": "t", "resveCours": "p",
                "start": 0, "rows": 100, "sort": "score desc", "filter": []
            }
            res = requests.post(api_url, headers=headers, json=payload, timeout=15)
            data = res.json()

            docs = data.get("data", {}).get("docs", [])
            logging.info(f"🔎 총 {len(docs)}개의 검색 결과 분석 시작")

            for idx, p_doc in enumerate(docs, 1):
                parent_title = p_doc.get("goodsNm", "제목없음")
                parent_code = p_doc.get("baseGoodsCode") or p_doc.get("goodsCode")
                
                tour_day = p_doc.get("tourDay") or ""
                if "0박1일" not in tour_day:
                    continue

                region_list = p_doc.get("stdRegionNm") or []
                location = region_list[0] if region_list else "국내"
                categories = classify_categories(parent_title)
                description = p_doc.get("productDescription") or parent_title

                # 1) 부모 저장
                cursor.execute("""
                    INSERT INTO tours (product_code, reference_code, title, description, location, collected_at, agency, category, phone)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE title=%s, collected_at=%s
                """, (parent_code, parent_code, parent_title, description, location, start_time, AGENCY_NAME, categories, IP_PHONE,
                      parent_title, start_time))
                stats["total_rprs"] += 1
                
                logging.info(f"📦 [{idx}] 부모 상품: {parent_title}")

                # 2) 일정 병합 (부모 + 자식)
                all_raw_docs = []
                all_raw_docs.append(p_doc)
                sub_docs_container = p_doc.get("subDocs") or {}
                sub_list = sub_docs_container.get("docs") or []
                all_raw_docs.extend(sub_list)

                # 3) 일정 중복 제거 및 DB 저장
                seen_dates = set()
                valid_count_for_this_tour = 0
                
                for c_doc in all_raw_docs:
                    dep_date = c_doc.get("departureDay")
                    
                    if not dep_date or dep_date in seen_dates or not (today_str <= dep_date <= limit_str):
                        continue
                    
                    seen_dates.add(dep_date)
                    
                    child_code = c_doc.get("goodsCode") or parent_code
                    price = c_doc.get("salesPrice") or c_doc.get("price") or 0
                    
                    # ✨ [수정된 부분] 상태값 처리 로직
                    raw_status = c_doc.get("bookingCode")
                    if raw_status in ["예약가능", "출발확정"] or not raw_status:
                        status = None  # DB에 NULL로 저장됨
                    else:
                        status = raw_status # "출발확정", "대기예약" 등은 그대로 유지
                    
                    booking_url = f"https://travel.interpark.com/tour/goods?goodsCd={child_code}"

                    cursor.execute("""
                        INSERT INTO tour_schedules (
                            product_code, title, departure_date, price_text, 
                            booking_url, updated_at, last_verified_at, error_msg
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            price_text=%s, updated_at=%s, last_verified_at=%s, error_msg=%s
                    """, (parent_code, parent_title, dep_date, str(price), 
                          booking_url, start_time, start_time, status,
                          str(price), start_time, start_time, status))
                    
                    valid_count_for_this_tour += 1
                    # 로그에는 가독성을 위해 NULL 대신 "정상"으로 표시
                    status_log = status if status else "정상(NULL)"
                    logging.info(f"   ∟ 📅 {dep_date} | 💰 {price}원 | 🏷 {status_log} | 🔗 {booking_url}")

                stats["saved_schedules"] += valid_count_for_this_tour

            # 🛠 [Cleanup]
            cleanup_limit_time = start_time - timedelta(hours=1)
            cursor.execute("DELETE FROM tours WHERE agency = %s AND collected_at < %s", (AGENCY_NAME, cleanup_limit_time))
            stats["deleted_tours"] = cursor.rowcount

        duration = datetime.now() - start_time
        report = (
            f"🤖 [{AGENCY_NAME} 수집 완료]\n"
            f"📦 부모 상품: {stats['total_rprs']}종\n"
            f"🔹 자식 일정: {stats['saved_schedules']}건\n"
            f"🧹 삭제: {stats['deleted_tours']}종"
        )
        send_telegram_msg(report)
        logging.info(f"🏁 최종 저장된 총 일정: {stats['saved_schedules']}건")

    except Exception as e:
        logging.error(f"❌ 오류: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    run_collection()