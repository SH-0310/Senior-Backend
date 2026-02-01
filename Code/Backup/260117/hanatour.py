import time, requests, logging, pymysql
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ✅ 공통 모듈 임포트
from utils import classify_categories, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "하나투어"
HANA_PHONE = "1577-1233"

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

def fetch_prefix_dynamically(rprs_code, dep_day, cookie):
    """saleProdCd에서 prefix(앞 6자리)와 suffix(뒤 3자리)를 모두 추출"""
    if not dep_day: return None, None
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
    
    # ⭐ 잘 되던 코드의 상세 헤더 구조 유지
    payload = {
        "header": {
            "timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
            "lang": "ko",
            "browserEngine": "Edge",
            "browserVersion": "143.0.0.0",
            "osName": "Windows",
            "osVersion": "10",
            "pathCd": "DCP",
            "siteId": "hanatour",
            "userDevice": "PC"
        },
        "domain": "https://www.hanatour.com",
        "scods": "B1,B2,B3,B4,B5,B6,B7,B8,A8,A9",
        "areaCd": "AK",
        "pkgServiceCd": "DP",
        "trvlDayCnt": "1",
        "strtDepDay": dep_day,
        "endDepDay": dep_day,
        "rprsProdCds": rprs_code,
        "page": 1,
        "pageSize": 20,
        "sort": "PROD_SORT5",
        "paymentTypeYn": "Y",
        "resPathCd": "DCP",
        "os": "pc"
    }
    
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        res_json = res.json()
        products = res_json.get('data', {}).get('products', [])
        if products:
            sale_cd = products[0].get('saleProdCd') # 예: AKP861260123004
            if sale_cd and len(sale_cd) >= 9:
                prefix = sale_cd[:6]  # AKP861
                suffix = sale_cd[-3:] # 004
                return prefix, suffix
    except: 
        return None, None
    return None, None

def fetch_calendar(rprs_code, month_str, cookie):
    url = "https://gw.hanatour.com/front/package/calendar/departure-dates?_siteId=hanatour"
    headers = {
        "Content-Type": "application/json",
        "Cookie": cookie,
        "Referer": "https://www.hanatour.com/",
        "prgmid": "major-products"
    }
    payload = {
        "header": {"timestamp": datetime.now().strftime("%Y%m%d%H%M%S"), "lang": "ko", "pathCd": "DCP", "siteId": "hanatour"},
        "domain": "https://www.hanatour.com", "rprsProdCds": rprs_code, "depDay": month_str, 
        "areaCd": "AK", "pkgServiceCd": "DP", "trvlDayCnt": "1", "os": "pc", "scods": "B1,B2,B3,B4,B5,B6,B7,B8,A8,A9"
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        return res.json().get('data', {}).get(month_str, [])
    except: return []

def run_collection():
    logging.info(f"🚀 {AGENCY_NAME} 수집 프로세스를 시작합니다.")
    cookie = get_automated_cookies()
    if not cookie: return

    main_url = "https://gw.hanatour.com/front/package/major-products?_siteId=hanatour"
    main_payload = {
        "header": {"timestamp": datetime.now().strftime("%Y%m%d%H%M%S"), "lang": "ko", "prevPage": "NO-REFERRER"},
        "domain": "https://www.hanatour.com", "areaCd": "AK", "pkgServiceCd": "DP", "trvlDayCnt": "1",
        "pageSize": 50, "sort": "RPRS_SORT2", "strtDepDay": datetime.now().strftime("%Y%m%d"),
        "scods": "B1,B2,B3,B4,B5,B6,B7,B8,A8,A9", "os": "pc"
    }

    try:
        response = requests.post(main_url, json=main_payload, headers={"Content-Type": "application/json", "Cookie": cookie})
        products = response.json().get('data', {}).get('products', [])
        
        # ✅ DB 자동 커밋 설정
        conn = get_db_connection()
        conn.autocommit(True) 
        
        with conn.cursor() as cursor:
            for p in products:
                rprs_code = p.get('rprsProdCd')
                title = p.get('rprsProdNm')
                location = p.get('trstCityNmLstCont')
                categories = classify_categories(title)

                # ✅ 1. tours 테이블 저장 (image_url 컬럼 제거)
                sql_tours = """
                    INSERT INTO tours (
                        product_code, title, description, location, 
                        collected_at, agency, category, phone, is_priority
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        title=%s, location=%s, category=%s
                """
                cursor.execute(sql_tours, (
                    rprs_code, title, title, location, 
                    datetime.now(), AGENCY_NAME, categories, HANA_PHONE, 0,
                    title, location, categories
                ))

                # 2. 달력 수집 (1월, 2월)
                all_schedules = []
                now = datetime.now()
                # 다음 달 계산
                next_month_dt = now + relativedelta(months=1)
                
                # ["202601", "202602"] 형태의 리스트 생성
                months_to_fetch = [now.strftime("%Y%m"), next_month_dt.strftime("%Y%m")]
                for m in months_to_fetch:
                    all_schedules.extend(fetch_calendar(rprs_code, m, cookie))

                today_str = datetime.now().strftime("%Y%m%d")
                all_schedules = [s for s in all_schedules if s.get('depDay') >= today_str]

                if not all_schedules: continue

                # 3. 정밀 Prefix 및 Suffix 획득
                prefix, suffix = fetch_prefix_dynamically(rprs_code, all_schedules[0].get('depDay'), cookie)                
                
                if not prefix or not suffix: 
                    logging.warning(f"   ⏩ {title} : 코드 추출 실패로 일정 건너뜜")
                    continue
                
                for s in all_schedules:
                    dep_date_full = s.get('depDay')
                    price = s.get('adtAmt')
                    booking_url = f"https://www.hanatour.com/trp/pkg/CHPC0PKG0200M200?pkgCd={prefix}{dep_date_full[2:]}{suffix}&prePage=major-products"
                    cursor.execute("""
                        INSERT INTO tour_schedules (product_code, departure_date, price_text, booking_url)
                        VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE price_text=%s, booking_url=%s
                    """, (rprs_code, dep_date_full, price, booking_url, price, booking_url))
                
                logging.info(f"   ✅ DB 저장 완료: {title} (Prefix: {prefix}) (Suffix: {suffix})")
                time.sleep(0.5)

    except Exception as e:
        logging.error(f"❌ 오류: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()
        logging.info("🏁 수집 프로세스 종료")

if __name__ == "__main__":
    run_collection()