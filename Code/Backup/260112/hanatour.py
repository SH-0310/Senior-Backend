import time, requests, pymysql, json, logging, os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# --- 로그 설정 ---
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
    options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
    options.page_load_strategy = 'eager' 
    service = Service("/usr/bin/chromedriver")
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(180)
        driver.get("https://www.hanatour.com/package/major-products?pkgServiceCd=DP&trvlDayCnt=1")
        time.sleep(7)
        cookies = driver.get_cookies()
        return "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    finally:
        driver.quit()

def fetch_prefix_dynamically(rprs_code, dep_day, cookie):
    """제공해주신 Payload와 Headers를 100% 반영한 Prefix 획득 함수"""
    if not dep_day: return None
    url = "https://gw.hanatour.com/front/package/products?_siteId=hanatour"
    
    # 🚨 [수정] 제공해주신 Request Headers 반영
    headers = {
        "Content-Type": "application/json",
        "Cookie": cookie,
        "Referer": "https://www.hanatour.com/",
        "Origin": "https://www.hanatour.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        "prgmid": "major-products", # ⭐ 필수 헤더
        "accept": "application/json, text/plain, */*"
    }
    
    # 🚨 [수정] 제공해주신 Payload 구조와 100% 일치시킴
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
        "endDepDay": dep_day, # ⭐ 시작일과 동일하게 설정
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
            sale_cd = products[0].get('saleProdCd')
            prefix = sale_cd[:6] if sale_cd else None
            return prefix
        else:
            logging.warning(f"   [Prefix 획득실패] {rprs_code} - 응답: {res_json.get('message')}")
            return None
    except Exception as e:
        logging.error(f"   [Prefix 에러] {rprs_code}: {e}")
        return None

def fetch_calendar(rprs_code, month_str, cookie):
    """달력 API에도 동일한 보안 헤더 적용"""
    url = "https://gw.hanatour.com/front/package/calendar/departure-dates?_siteId=hanatour"
    headers = {
        "Content-Type": "application/json",
        "Cookie": cookie,
        "Referer": "https://www.hanatour.com/",
        "prgmid": "major-products"
    }
    payload = {
        "header": {
            "timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
            "lang": "ko",
            "pathCd": "DCP",
            "siteId": "hanatour"
        },
        "domain": "https://www.hanatour.com",
        "rprsProdCds": rprs_code, 
        "depDay": month_str, 
        "areaCd": "AK", 
        "pkgServiceCd": "DP", 
        "trvlDayCnt": "1", 
        "os": "pc",
        "scods": "B1,B2,B3,B4,B5,B6,B7,B8,A8,A9"
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        data = res.json().get('data', {})
        return data.get(month_str, [])
    except:
        return []

def run_collection():
    logging.info("🚀 수집 프로세스를 시작합니다.")
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
        
        conn = pymysql.connect(host='localhost', user='shmoon', password='Tjdgursla87!', db='senior_travel', charset='utf8mb4')
        with conn.cursor() as cursor:
            for p in products:
                rprs_code = p.get('rprsProdCd')
                title = p.get('rprsProdNm')
                list_min_dep_day = p.get('minDepDay')

                logging.info(f"🔎 상품 분석 중: {title} ({rprs_code})")

                # 1. 상품 정보 저장
                cursor.execute("INSERT INTO tours (product_code, title, location) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE title=%s", 
                               (rprs_code, title, p.get('trstCityNmLstCont'), title))

                # 2. 달력 수집
                all_schedules = []
                for m in ["202601", "202602"]:
                    all_schedules.extend(fetch_calendar(rprs_code, m, cookie))

                if not all_schedules:
                    logging.warning(f"   ⏩ 일정 없음")
                    continue

                # 3. 정밀 Prefix 획득 (실제 첫 날짜 기준)
                prefix = fetch_prefix_dynamically(rprs_code, all_schedules[0].get('depDay'), cookie)

                if not prefix:
                    logging.error(f"   ❌ Prefix 획득 최종 실패 (보안 정책 위반 가능성)")
                    continue

                # 4. 일정 DB 저장
                for s in all_schedules:
                    dep_date_full = s.get('depDay')
                    dep_date_short = dep_date_full[2:]
                    price = s.get('adtAmt')
                    booking_url = f"https://www.hanatour.com/trp/pkg/CHPC0PKG0200M200?pkgCd={prefix}{dep_date_short}001&prePage=major-products"

                    cursor.execute("""
                        INSERT INTO tour_schedules (product_code, departure_date, price_text, booking_url)
                        VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE price_text=%s, booking_url=%s
                    """, (rprs_code, dep_date_full, price, booking_url, price, booking_url))
                
                logging.info(f"   ✅ {len(all_schedules)}개 일정 저장 완료 (Prefix: {prefix})")
                time.sleep(0.5)

        conn.commit()
    except Exception as e:
        logging.error(f"❌ 치명적 오류: {e}")
    finally:
        if 'conn' in locals() and conn: conn.close()
        logging.info("🏁 수집 프로세스 종료")

if __name__ == "__main__":
    run_collection()