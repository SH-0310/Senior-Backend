import time, requests, logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ✅ 공통 모듈 임포트
from utils import extract_all_keywords, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "모두투어"
MODE_PHONE = "1544-5252"
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/home/ubuntu/Senior/Code/modutour_crawler.log", encoding='utf-8'), 
        logging.StreamHandler()
    ]
)

def send_telegram_msg(text):
    """텔레그램 알림 전송"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        logging.error(f"텔레그램 전송 실패: {e}")

def get_automated_cookies():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service("/usr/bin/chromedriver") 
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.get("https://www.modetour.com/category/domestic") 
        time.sleep(7)
        cookies = driver.get_cookies()
        return "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    except Exception as e:
        logging.error(f"❌ 쿠키 획득 실패: {e}")
        return None
    finally:
        driver.quit()

def fetch_modetour_pnums(asis_code, cookie, start_date, end_date):
    """자녀 상품 상세 추출 (성공했던 정밀 Payload 구조 적용)"""
    url = "https://b2c-api.modetour.com/Package/SearchProductDates"
    
    # API가 거부하지 않도록 필수 tourCondition 구조를 포함합니다.
    payload = {
        "groupCls": "P",
        "itemCode": [asis_code],
        "startDate": start_date,
        "endDate": end_date,
        "pnums": [None],
        "filter": {
            "typeFilter": "PGTOverseasTravel",
            "isViewAllAvaiableSeat": False,
            "sort": "Hightest",
            "tourCondition": {
                "airSeatClass": None, "airPortTax": None, "localTraffic": None, "mealFee": None, "dolomites": None,
                "entranceFee": None, "freeSchedule": None, "guideYn": None, "localGuide": None,
                "neccessaryLocalExpenses": None, "optionalTour": None, "roomCharge": None,
                "shopping": None, "transport": None, "transportation": None, "travelConcept": None,
                "travelPeriod": None, "travelType": []
            }
        }
    }
    try:
        res = requests.post(url, json=payload, headers={"Content-Type": "application/json", "Cookie": cookie}, timeout=10)
        master_detail = res.json().get('result', {}).get('productMasterDetail', [])
        return master_detail[0].get('productDate', []) if master_detail else []
    except: return []

def run_collection():
    start_time = datetime.now() # 🕒 Cleanup 기준점
    logging.info(f"🚀 {AGENCY_NAME} 1개월 정밀 통합 수집 시작")
    
    stats = {"total_rprs": 0, "saved_schedules": 0, "deleted_tours": 0}
    cookie = get_automated_cookies()
    if not cookie: return

    today_str = start_time.strftime("%Y-%m-%d")
    limit_day_str = (start_time + relativedelta(months=1)).strftime("%Y-%m-%d")

    master_url = "https://b2c-api.modetour.com/Package/SearchProductMaster"
    
    # ✅ 수집이 잘 되던 코드의 Payload 구성 요소를 모두 포함 (masterCodeIds 등 필수 필드)
    master_payload = {
        "themeId": "A0A0616C-B2FC-436E-8F0E-08DD04715021",
        "areaId": "A0A0616C-B2FC-436E-8F0E-08DD04715021",
        "areaKeyWordId": [],
        "deviceType": "DVTPC",
        "filter": {
            "typeFilter": "PGTOverseasTravel",
            "depatureDay": None, "depatureTime": None, "destination": None, "endLocation": None,
            "isViewAllAvailableSeat": True, "lodgment": None, "maxPrice": 0, "minPrice": 0,
            "productBrand": None, "promotion": None, "promotions": None, "sort": "Recommend",
            "startingPoint": None, "transport": None, "transportation": None, "travelConcept": None,
            "travelPeriod": None, "travelType": None,
            "tourCondition": {
                "airSeatClass": None, "airPortTax": None, "localTraffic": None, "mealFee": None, "dolomites": None,
                "entranceFee": None, "freeSchedule": None, "guideYn": None, "localGuide": None,
                "neccessaryLocalExpenses": None, "optionalTour": None, "roomCharge": None,
                "shopping": None, "transport": None, "transportation": None, "travelConcept": None,
                "travelPeriod": None, "travelType": []
            }
        },
        "masterCodeIds": [],
        "masterCodes": [],
        "page": 1,
        "pageSize": 100,
        "searchFrom": today_str,
        "searchTo": limit_day_str,
        "travelType": "GNBOverseasTravel"
    }

    try:
        headers = {"Content-Type": "application/json", "Cookie": cookie}
        response = requests.post(master_url, json=master_payload, headers=headers)
        res_json = response.json()
        products = res_json.get('result', {}).get('productMaster', [])
        
        logging.info(f"🔎 검색 결과: {len(products)}개 마스터 상품 발견")
        if not products: return

        conn = get_db_connection()
        conn.autocommit(True) 
        
        with conn.cursor() as cursor:
            for p in products:
                master_code = p.get('masterCode')
                master_title = p.get('masterProductName')
                master_desc = p.get('descriptions') or master_title 
                master_img = p.get('image', '')
                asis_code = p.get('productCodes', [{}])[0].get('asisProductNo')
                if not asis_code: continue

                # 1. 자녀 정보 수집
                date_details = fetch_modetour_pnums(asis_code, cookie, today_str, limit_day_str)
                # sdate가 YYYY-MM-DD 형식인지 확인하며 필터링
                valid_details = [d for d in date_details if today_str <= d.get('date', {}).get('sdate', '') <= limit_day_str]
                
                if not valid_details:
                    continue 

                # ✅ [추가] 태그 추출 (마스터 제목 기반)
                tags = extract_all_keywords(master_title)
                location = p.get('areas', [{}])[0].get('name', '국내')

                # 2. 부모 정보 저장
                cursor.execute("""
                    INSERT INTO tours (product_code, reference_code, title, description, main_image_url, location, collected_at, agency, category, phone, is_priority)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, 0)
                    ON DUPLICATE KEY UPDATE 
                        title=%s, description=%s, main_image_url=%s, location=%s, category=%s, collected_at=NOW()
                """, (
                    # INSERT (10개)
                    master_code, asis_code, master_title, master_desc, master_img, 
                    location, AGENCY_NAME, tags, MODE_PHONE,
                    # UPDATE (6개)
                    master_title, master_desc, master_img, location, tags
                ))
                stats["total_rprs"] += 1

                # 3. 자식 정보 저장 (tour_schedules)
                for detail in valid_details:
                    pnum = detail.get('pnum')
                    child_title = detail.get('pName') or master_title
                    
                    # ✅ [수정 핵심 1] 날짜 형식 유지 (2026-01-28 형식 그대로 사용)
                    # 기존의 .replace('-', '')를 제거했습니다.
                    sdate = detail.get('date', {}).get('sdate', '') 
                    
                    price = detail.get('price', {}).get('adult')
                    booking_url = f"https://www.modetour.com/package/{pnum}"

                    # ✅ [수정 핵심 2] tags 컬럼 추가 및 departure_date 업데이트
                    cursor.execute("""
                        INSERT INTO tour_schedules (
                            product_code, title, departure_date, price_text, 
                            booking_url, error_msg, updated_at, tags
                        )
                        VALUES (%s, %s, %s, %s, %s, NULL, NOW(), %s)
                        ON DUPLICATE KEY UPDATE 
                            title=%s, price_text=%s, booking_url=%s, 
                            error_msg=NULL, updated_at=NOW(), tags=%s,
                            departure_date=%s
                    """, (
                        master_code, child_title, sdate, str(price), booking_url, tags,
                        child_title, str(price), booking_url, tags, sdate
                    ))
                    stats["saved_schedules"] += 1
                
                logging.info(f"   ✅ {master_title}: {len(valid_details)}개 일정 동기화 완료")
                time.sleep(0.5)

            # 4. 무효 상품 Cleanup (이번에 업데이트 안 된 과거 데이터 삭제)
            cleanup_sql = "DELETE FROM tours WHERE agency = %s AND collected_at < %s"
            cursor.execute(cleanup_sql, (AGENCY_NAME, start_time))
            stats["deleted_tours"] = cursor.rowcount

            # ✅ [추가] 4-1️⃣ 크롤링 성공 로그 기록 (DB 저장)
            finish_time = datetime.now()
            log_sql = """
                INSERT INTO crawler_logs (agency_name, status, collected_count, crawled_at, message)
                VALUES (%s, %s, %s, %s, %s)
            """
            log_message = f"부모 {stats['total_rprs']}종, 삭제 {stats['deleted_tours']}종 수집 완료"
            cursor.execute(log_sql, (
                AGENCY_NAME, 
                "SUCCESS", 
                stats["saved_schedules"], 
                finish_time, 
                log_message
            ))

        # 5. 리포트 발송
        duration = datetime.now() - start_time
        report = (
            f"🤖 [{AGENCY_NAME} 1개월 수집 완료]\n"
            f"⏱ 소요시간: {str(duration).split('.')[0]}\n"
            f"📦 유효 부모상품: {stats['total_rprs']}종\n"
            f"🔹 상세 일정(자식): {stats['saved_schedules']}건\n"
            f"🧹 삭제된 무효상품: {stats['deleted_tours']}종"
        )
        send_telegram_msg(report)

    except Exception as e:
        logging.error(f"❌ 오류 발생: {e}")
        # ✅ [추가] 에러 발생 시 FAIL 상태 로그 기록
        try:
            with get_db_connection() as err_conn:
                with err_conn.cursor() as err_cursor:
                    err_cursor.execute("""
                        INSERT INTO crawler_logs (agency_name, status, crawled_at, message)
                        VALUES (%s, %s, %s, %s)
                    """, (AGENCY_NAME, "FAIL", datetime.now(), str(e)[:200]))
                    err_conn.commit()
        except: pass
        
        send_telegram_msg(f"❌ {AGENCY_NAME} 수집 중 오류 발생: {str(e)[:100]}")
    finally:
        if 'conn' in locals() and conn: conn.close()
        logging.info("🏁 수집 프로세스 종료")

if __name__ == "__main__":
    run_collection()