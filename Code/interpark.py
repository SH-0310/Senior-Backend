import time, requests, logging, json
from datetime import datetime, timedelta
from utils import extract_all_keywords, get_db_connection

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
    
    logging.info(f"🚀 {AGENCY_NAME} 수집 시작 (검색어: '당일여행', 범위: {today_str} ~ {limit_str})")
    
    api_url = "https://travel.interpark.com/api-package/search"
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://travel.interpark.com',
        'Referer': 'https://travel.interpark.com/tour/search'
    }

    stats = {"total_rprs": 0, "saved_schedules": 0, "deleted_tours": 0, "filtered_by_day": 0}
    conn = get_db_connection()
    conn.autocommit(True)

    try:
        with conn.cursor() as cursor:
            # 1. API 호출 로그 강화
            payload = {
                "q": "당일여행", "domain": "t", "resveCours": "p",
                "start": 0, "rows": 100, "sort": "score desc", "filter": []
            }
            
            logging.info(f"📡 API 요청 중... URL: {api_url}")
            res = requests.post(api_url, headers=headers, json=payload, timeout=15)
            
            if res.status_code != 200:
                logging.error(f"❌ API 호출 실패! 상태 코드: {res.status_code}")
                logging.error(f"📄 응답 내용: {res.text[:500]}")
                return

            data = res.json()
            docs = data.get("data", {}).get("docs", [])
            total_count = data.get("data", {}).get("totalCount", 0)
            
            logging.info(f"✅ API 응답 수신: 검색결과 총 {total_count}개 중 {len(docs)}개 수집됨")

            if not docs:
                logging.warning("⚠️ 검색 결과(docs)가 비어 있습니다. 검색어 '당일여행'을 '당일'로 변경하거나 페이로드를 확인하세요.")

            for idx, p_doc in enumerate(docs, 1):
                parent_title = p_doc.get("goodsNm", "제목없음")
                parent_code = p_doc.get("baseGoodsCode") or p_doc.get("goodsCode")
                tour_day = p_doc.get("tourDay") or ""
                
                # 로그: 모든 상품의 제목과 여행 일수 노출
                logging.info(f"🔍 [{idx}/{len(docs)}] 상품 분석: {parent_title} (코드: {parent_code}, 기간: {tour_day})")

                # 필터링 로그
                if "0박1일" not in tour_day:
                    logging.debug(f"   ⏩ 패스: '0박1일' 아님 ({tour_day})")
                    stats["filtered_by_day"] += 1
                    continue

                main_img_url = p_doc.get("mainImgUrl", "")
                region_list = p_doc.get("stdRegionNm") or []
                location = region_list[0] if region_list else "국내"
                categories = extract_all_keywords(parent_title)
                description = p_doc.get("productDescription") or parent_title

                # 1) 부모 저장
                cursor.execute("""
                    INSERT INTO tours (product_code, reference_code, title, description, main_image_url, location, collected_at, agency, category, phone)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE title=%s, main_image_url=%s, collected_at=%s
                """, (parent_code, parent_code, parent_title, description, main_img_url, location, start_time, AGENCY_NAME, categories, IP_PHONE,
                    parent_title, main_img_url, start_time))
                stats["total_rprs"] += 1
                
                # 2) 일정 병합 로그
                sub_docs_container = p_doc.get("subDocs") or {}
                sub_list = sub_docs_container.get("docs") or []
                all_raw_docs = [p_doc] + sub_list
                
                logging.info(f"   📦 부모 상품 저장 완료. 연결된 일정 후보: {len(all_raw_docs)}개")

                seen_dates = set()
                valid_count_for_this_tour = 0
                
                for c_idx, c_doc in enumerate(all_raw_docs):
                    dep_date_raw = c_doc.get("departureDay")
                    
                    if not dep_date_raw:
                        continue
                    
                    # 날짜 필터링 상세 로그
                    if not (today_str <= dep_date_raw <= limit_str):
                        logging.debug(f"      📅 날짜 제외: {dep_date_raw} (범위 밖)")
                        continue
                        
                    if dep_date_raw in seen_dates:
                        continue

                    seen_dates.add(dep_date_raw)
                    dep_date_db = f"{dep_date_raw[:4]}-{dep_date_raw[4:6]}-{dep_date_raw[6:]}"
                    child_code = c_doc.get("goodsCode") or parent_code
                    price = c_doc.get("salesPrice") or c_doc.get("price") or 0
                    tags = extract_all_keywords(parent_title)
                    
                    raw_status = c_doc.get("bookingCode")
                    status = raw_status if raw_status not in ["예약가능", "출발확정", None] else None
                    booking_url = f"https://travel.interpark.com/tour/goods?goodsCd={child_code}"

                    cursor.execute("""
                        INSERT INTO tour_schedules (product_code, title, departure_date, price_text, booking_url, updated_at, last_verified_at, error_msg, tags)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE price_text=%s, updated_at=%s, last_verified_at=%s, error_msg=%s, tags=%s, departure_date=%s
                    """, (parent_code, parent_title, dep_date_db, str(price), booking_url, start_time, start_time, status, tags,
                          str(price), start_time, start_time, status, tags, dep_date_db))
                    
                    valid_count_for_this_tour += 1
                    logging.info(f"      ∟ 📅 {dep_date_db} | 💰 {price}원 | 🏷 {status or '정상'}")

                stats["saved_schedules"] += valid_count_for_this_tour

            # 🛠 [Cleanup]
            cleanup_limit_time = start_time - timedelta(hours=1)
            cursor.execute("DELETE FROM tours WHERE agency = %s AND collected_at < %s", (AGENCY_NAME, cleanup_limit_time))
            stats["deleted_tours"] = cursor.rowcount

            # 📊 로그 기록
            finish_time = datetime.now()
            log_sql = "INSERT INTO crawler_logs (agency_name, status, collected_count, crawled_at, message) VALUES (%s, %s, %s, %s, %s)"
            log_message = f"부모 {stats['total_rprs']}종(비당일패스 {stats['filtered_by_day']}종), 삭제 {stats['deleted_tours']}종"
            cursor.execute(log_sql, (AGENCY_NAME, "SUCCESS", stats["saved_schedules"], finish_time, log_message))

        duration = datetime.now() - start_time
        report = (
            f"🤖 [{AGENCY_NAME} 수집 리포트]\n"
            f"검색결과: {total_count}개\n"
            f"당일부모: {stats['total_rprs']}종\n"
            f"일정저장: {stats['saved_schedules']}건\n"
            f"비당일제외: {stats['filtered_by_day']}건"
        )
        send_telegram_msg(report)
        logging.info(f"🏁 수집 종료. 저장된 일정: {stats['saved_schedules']}건. 소요시간: {duration}")

    except Exception as e:
        logging.error(f"💥 치명적 오류: {e}", exc_info=True)
        try:
            with get_db_connection() as err_conn:
                with err_conn.cursor() as err_cursor:
                    err_cursor.execute("INSERT INTO crawler_logs (agency_name, status, crawled_at, message) VALUES (%s, %s, %s, %s)", 
                                     (AGENCY_NAME, "FAIL", datetime.now(), str(e)[:200]))
                    err_conn.commit()
        except: pass
        send_telegram_msg(f"❌ {AGENCY_NAME} 수집 실패: {str(e)[:100]}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    run_collection()