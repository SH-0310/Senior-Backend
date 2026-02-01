import time, requests, logging, pymysql
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ✅ 공통 모듈 임포트
from utils import extract_all_keywords, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "하나투어"
HANA_PHONE = "1577-1233"
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/home/ubuntu/Senior/Code/hanatour_crawler.log", encoding='utf-8'), 
        logging.StreamHandler()
    ]
)

def get_automated_cookies():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.page_load_strategy = 'eager' 
    service = Service("/usr/bin/chromedriver")
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.get("https://www.hanatour.com/package/major-products?pkgServiceCd=DP&trvlDayCnt=1")
        time.sleep(7)
        cookies = driver.get_cookies()
        return "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    finally:
        driver.quit()

def fetch_sale_products_by_day(rprs_code, dep_day, cookie):
    """해당 날짜에 존재하는 모든 판매 상품(saleProdCd)의 prefix/suffix/가격 정보를 추출"""
    if not dep_day: return []
    url = "https://gw.hanatour.com/front/package/products?_siteId=hanatour"
    
    headers = {
        "Content-Type": "application/json",
        "Cookie": cookie,
        "Referer": "https://www.hanatour.com/",
        "Origin": "https://www.hanatour.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        "prgmid": "major-products",
        "accept": "application/json, text/plain, */*"
    }
    
    payload = {
        "header": {
            "timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
            "lang": "ko", "browserEngine": "Edge", "browserVersion": "143.0.0.0",
            "osName": "Windows", "osVersion": "10", "pathCd": "DCP", "siteId": "hanatour", "userDevice": "PC"
        },
        "domain": "https://www.hanatour.com",
        "scods": "B1,B2,B3,B4,B5,B6,B7,B8,A8,A9",
        "areaCd": "AK", "pkgServiceCd": "DP", "trvlDayCnt": "1",
        "strtDepDay": dep_day, "endDepDay": dep_day, "rprsProdCds": rprs_code,
        "page": 1, "pageSize": 20, "sort": "PROD_SORT5", "os": "pc"
    }
    
    results = []
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        products = res.json().get('data', {}).get('products', [])
        for p in products:
            sale_cd = p.get('saleProdCd')
            if sale_cd and len(sale_cd) >= 9:
                results.append({
                    'prefix': sale_cd[:6],
                    'suffix': sale_cd[-3:],
                    'title': p.get('saleProdNm'),
                    'price': p.get('adtAmt'), # 날짜 API의 가격보다 실제 상품 리스트의 가격이 더 정확함
                    'sale_cd': sale_cd
                })
        return results
    except Exception as e: 
        logging.error(f"상세 상품 리스트 추출 중 예외 발생: {e}")
    return []

def fetch_calendar(rprs_code, month_str, cookie):
    url = "https://gw.hanatour.com/front/package/calendar/departure-dates?_siteId=hanatour"
    headers = {"Content-Type": "application/json", "Cookie": cookie, "Referer": "https://www.hanatour.com/"}
    payload = {
        "header": {"timestamp": datetime.now().strftime("%Y%m%d%H%M%S"), "lang": "ko", "pathCd": "DCP", "siteId": "hanatour"},
        "domain": "https://www.hanatour.com", "rprsProdCds": rprs_code, "depDay": month_str, 
        "areaCd": "AK", "pkgServiceCd": "DP", "trvlDayCnt": "1", "os": "pc", "scods": "B1,B2,B3,B4,B5,B6,B7,B8,A8,A9"
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        return res.json().get('data', {}).get(month_str, [])
    except: return []

def send_telegram_msg(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try: requests.post(url, data=payload, timeout=10)
    except Exception as e: logging.error(f"텔레그램 전송 실패: {e}")

def run_collection():
    start_time = datetime.now()
    logging.info(f"🚀 {AGENCY_NAME} 정밀 수집 프로세스를 시작합니다. (기준 시간: {start_time})")
    
    stats = {"total_rprs": 0, "saved_schedules": 0, "failed_codes": 0, "deleted_tours": 0}
    cookie = get_automated_cookies()
    if not cookie: return

    main_url = "https://gw.hanatour.com/front/package/major-products?_siteId=hanatour"
    main_payload = {
        "header": {"timestamp": datetime.now().strftime("%Y%m%d%H%M%S"), "lang": "ko"},
        "domain": "https://www.hanatour.com", "areaCd": "AK", "pkgServiceCd": "DP", "trvlDayCnt": "1",
        "pageSize": 100, "sort": "RPRS_SORT2", "strtDepDay": datetime.now().strftime("%Y%m%d"), "os": "pc"
    }

    try:
        response = requests.post(main_url, json=main_payload, headers={"Content-Type": "application/json", "Cookie": cookie})
        products = response.json().get('data', {}).get('products', [])
        
        conn = get_db_connection()
        conn.autocommit(True) 
        
        now_dt = datetime.now()
        one_month_later = now_dt + relativedelta(months=1)
        today_str, limit_day_str = now_dt.strftime("%Y%m%d"), one_month_later.strftime("%Y%m%d")

        with conn.cursor() as cursor:
            for p in products:
                rprs_code = p.get('rprsProdCd')
                title = p.get('rprsProdNm')
                
                # 1️⃣ [날짜 확인 먼저] 1개월 내 출발 가능한 날짜가 있는지 체크
                all_schedules = []
                months = [now_dt.strftime("%Y%m"), one_month_later.strftime("%Y%m")]
                for m in months:
                    all_schedules.extend(fetch_calendar(rprs_code, m, cookie))
                
                all_schedules = [s for s in all_schedules if today_str <= s.get('depDay') <= limit_day_str]

                # 2️⃣ [조건부 저장] 날짜가 없으면 tours 테이블에도 넣지 않고 즉시 건너뜀
                if not all_schedules:
                    # 이전에 저장되어 있던 상품이었다면, 마지막의 유령 상품 정리 로직에서 자동 삭제됩니다.
                    continue

                # 3️⃣ [부모 테이블 저장] 날짜가 있는 경우에만 tours 정보를 입력/갱신
                location = p.get('trstCityNmLstCont')
                categories = extract_all_keywords(title)
                sql_tours = """
                    INSERT INTO tours (product_code, title, description, location, collected_at, agency, category, phone, is_priority)
                    VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, 0)
                    ON DUPLICATE KEY UPDATE title=%s, location=%s, category=%s, collected_at=NOW()
                """
                cursor.execute(sql_tours, (rprs_code, title, title, location, AGENCY_NAME, categories, HANA_PHONE, title, location, categories))
                stats["total_rprs"] += 1

                # 4️⃣ [자식 테이블 저장] 세부 일정 정보 입력
                # --- [자식 테이블 저장] 세부 일정 정보 입력 루프 내부 ---
                for s in all_schedules:
                    raw_dep_date = s.get('depDay') # 예: "20260131"
                    
                    # ✅ 여기서 하이픈이 들어간 형식으로 변환합니다 (20260131 -> 2026-01-31)
                    dep_date = f"{raw_dep_date[:4]}-{raw_dep_date[4:6]}-{raw_dep_date[6:]}"
                    
                    # 상세 상품 리스트 호출
                    sale_list = fetch_sale_products_by_day(rprs_code, raw_dep_date, cookie) 
                    # 주의: API 호출 시에는 원래의 8자리(raw_dep_date)를 사용해야 합니다.

                    if not sale_list: continue

                    for sp in sale_list:
                        # 예약 URL 생성 (URL에도 8자리 날짜 형식이 필요할 수 있으니 확인 필요)
                        # 하나투어 URL 규칙에 따라 raw_dep_date[2:] 등을 적절히 사용
                        booking_url = f"https://www.hanatour.com/trp/pkg/CHPC0PKG0200M200?pkgCd={sp['prefix']}{raw_dep_date[2:]}{sp['suffix']}&prePage=major-products"
                        
                        cursor.execute("""
                            INSERT INTO tour_schedules (product_code, title, departure_date, price_text, booking_url, updated_at)
                            VALUES (%s, %s, %s, %s, %s, NOW()) 
                            ON DUPLICATE KEY UPDATE 
                                title = %s, 
                                price_text = %s, 
                                booking_url = %s, 
                                updated_at = NOW(), 
                                departure_date = %s  -- ✅ 업데이트 시에도 날짜 형식을 유지
                        """, (rprs_code, sp['title'], dep_date, sp['price'], booking_url, sp['title'], sp['price'], booking_url, dep_date))
                        stats["saved_schedules"] += 1
                
                logging.info(f" ✅ 동기화 완료: {title} ({len(all_schedules)}개 날짜 확인)")

            # 5️⃣ [정리 로직] 이번 실행에서 날짜가 확인되지 않은 모든 하나투어 상품 제거
            # tours에서 삭제되면 tour_schedules 데이터도 CASCADE에 의해 함께 지워집니다.
            cleanup_sql = "DELETE FROM tours WHERE agency = %s AND collected_at < %s"
            cursor.execute(cleanup_sql, (AGENCY_NAME, start_time))
            stats["deleted_tours"] = cursor.rowcount

            # ✅ [추가] 5-1️⃣ 크롤링 로그 기록 (DB 저장)
            finish_time = datetime.now()
            log_sql = """
                INSERT INTO crawler_logs (agency_name, status, collected_count, crawled_at, message)
                VALUES (%s, %s, %s, %s, %s)
            """
            log_message = f"부모 {stats['total_rprs']}종, 삭제 {stats['deleted_tours']}종"
            cursor.execute(log_sql, (
                AGENCY_NAME, 
                "SUCCESS", 
                stats["saved_schedules"], 
                finish_time, 
                log_message
            ))

        # 6️⃣ 리포트 발송
        duration = datetime.now() - start_time
        report = (
            f"🤖 [{AGENCY_NAME} 데이터 정제 완료]\n"
            f"⏱ 소요시간: {str(duration).split('.')[0]}\n"
            f"📦 유효 상품: {stats['total_rprs']}종 (날짜 존재)\n"
            f"🔹 상세 일정: {stats['saved_schedules']}건\n"
            f"🧹 삭제된 무효상품: {stats['deleted_tours']}종 (날짜 없음)\n"
            f"--------------------------\n"
            f"※ 출발 불가능한 상품이 DB에서 완전히 제거되었습니다."
        )
        send_telegram_msg(report)

    except Exception as e:
        logging.error(f"❌ 오류: {e}")
        # ✅ [추가] 에러 발생 시에도 'FAIL' 상태로 로그를 남깁니다.
        try:
            with get_db_connection() as err_conn:
                with err_conn.cursor() as err_cursor:
                    err_cursor.execute("""
                        INSERT INTO crawler_logs (agency_name, status, crawled_at, message)
                        VALUES (%s, %s, %s, %s)
                    """, (AGENCY_NAME, "FAIL", datetime.now(), str(e)[:200]))
                    err_conn.commit()
        except: pass
        
        send_telegram_msg(f"❌ {AGENCY_NAME} 수집 중 오류: {str(e)[:100]}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    run_collection()