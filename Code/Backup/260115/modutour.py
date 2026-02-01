import time, requests, logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ✅ 공통 모듈 임포트
from utils import classify_categories, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "모두투어"
MODE_PHONE = "1544-5252"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/home/ubuntu/Senior/Code/modutour_crawler.log", encoding='utf-8'), 
        logging.StreamHandler()
    ]
)

def get_automated_cookies():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service("/usr/bin/chromedriver") 
    try:
        driver = webdriver.Chrome(service=service, options=options)
        # 쿠키 생성을 위해 국내 카테고리 페이지 접속
        driver.get("https://www.modetour.com/category/domestic") 
        time.sleep(7)
        cookies = driver.get_cookies()
        return "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    except Exception as e:
        logging.error(f"❌ 쿠키 획득 실패: {e}")
        return None
    finally:
        driver.quit()

def fetch_modetour_pnums(asis_code, cookie):
    """상세 날짜별 pnum 추출 함수"""
    url = "https://b2c-api.modetour.com/Package/SearchProductDates"
    headers = {
        "Content-Type": "application/json",
        "Cookie": cookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    # 날짜별 조회 시에도 해외(Overseas) 필터 규칙이 적용될 수 있음
    payload = {
        "groupCls": "P",
        "itemCode": [asis_code],
        "startDate": datetime.now().strftime("%Y-%m-%d"),
        "filter": { "typeFilter": "PGTOverseasTravel" } 
    }
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        master_detail = res.json().get('result', {}).get('productMasterDetail', [])
        return master_detail[0].get('productDate', []) if master_detail else []
    except: return []

def run_collection():
    logging.info(f"🚀 {AGENCY_NAME} 수집 시작 (정밀 페이로드 모드)")
    
    cookie = get_automated_cookies()
    if not cookie: return

    master_url = "https://b2c-api.modetour.com/Package/SearchProductMaster"
    
    # ✅ 사용자가 캡처한 Payload와 100% 동일하게 구성
    master_payload = {
        "themeId": "A0A0616C-B2FC-436E-8F0E-08DD04715021",
        "areaId": "A0A0616C-B2FC-436E-8F0E-08DD04715021",
        "areaKeyWordId": [],
        "deviceType": "DVTPC",
        "filter": {
            "typeFilter": "PGTOverseasTravel", # 사용자가 캡처한 대로 유지
            "minPrice": 0,
            "maxPrice": 0,
            "startingPoint": None,
            "destination": None,
            "isViewAllAvailableSeat": True,
            "sort": "Recommend"
        },
        "page": 1,
        "pageSize": 100,
        "searchFrom": "2026-01-20", # 캡처된 시작 날짜
        "searchTo": "2026-12-20",   # 캡처된 종료 날짜
        "travelType": "GNBOverseasTravel" # 사용자가 캡처한 핵심 키워드
    }

    headers = {
        "Content-Type": "application/json",
        "Cookie": cookie,
        "Origin": "https://www.modetour.com",
        "Referer": "https://www.modetour.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.post(master_url, json=master_payload, headers=headers)
        res_json = response.json()
        
        # 디버깅을 위해 결과 출력 추가
        products = res_json.get('result', {}).get('productMaster', [])
        logging.info(f"🔎 검색 결과 travelType: {res_json.get('result', {}).get('travelType')}")
        logging.info(f"🔎 검색된 마스터 상품 수: {len(products)}개")

        if not products:
            logging.warning("⚠️ 여전히 결과가 0개입니다. 응답 본문을 확인해보세요.")
            return

        conn = get_db_connection()
        conn.autocommit(True) 
        with conn.cursor() as cursor:
            for p in products:
                master_code = p.get('masterCode')
                title = p.get('masterProductName')
                location = p.get('areas', [{}])[0].get('name', '국내')
                categories = classify_categories(title)
                
                # asisProductNo 추출
                product_codes = p.get('productCodes', [])
                if not product_codes: continue
                asis_code = product_codes[0].get('asisProductNo')

                # 1. tours 테이블 저장
                cursor.execute("""
                    INSERT INTO tours (product_code, title, description, location, collected_at, agency, category, phone, is_priority)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE title=%s, location=%s, category=%s
                """, (master_code, title, title, location, datetime.now(), AGENCY_NAME, categories, MODE_PHONE, 0, title, location, categories))

                # 2. 상세 일정 수집 (pnum 기반)
                date_details = fetch_modetour_pnums(asis_code, cookie)
                for detail in date_details:
                    pnum = detail.get('pnum')
                    sdate = detail.get('date', {}).get('sdate').replace('-', '')
                    price = detail.get('price', {}).get('adult')
                    booking_url = f"https://www.modetour.com/package/{pnum}"
                    
                    cursor.execute("""
                        INSERT INTO tour_schedules (product_code, departure_date, price_text, booking_url)
                        VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE price_text=%s, booking_url=%s
                    """, (master_code, sdate, price, booking_url, price, booking_url))
                
                logging.info(f"   ✅ 수집 완료: {title} ({len(date_details)}개 일정)")
                time.sleep(0.3)

        conn.close()
        logging.info("🏁 모두투어 수집 프로세스 종료")

    except Exception as e:
        logging.error(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    run_collection()