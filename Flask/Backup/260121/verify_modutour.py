import pymysql, requests, json, re, time, logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# ==========================================
# 1. 설정 정보
# ==========================================
DB_CONFIG = {
    'host': 'localhost', 'user': 'shmoon', 'password': 'Tjdgursla87!',
    'db': 'senior_travel', 'charset': 'utf8mb4', 'cursorclass': pymysql.cursors.DictCursor
}
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

class ModetourVerifier:
    def __init__(self, cookie):
        self.api_url = "https://b2c-api.modetour.com/Package/SearchProductDates"
        self.cookie = cookie
        self.headers = {
            "Content-Type": "application/json",
            "Cookie": self.cookie,
            "Origin": "https://www.modetour.com",
            "Referer": "https://www.modetour.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
        }

    def clean_text(self, text):
        return re.sub(r'[^가-힣a-zA-Z0-9]', '', text)

    def check_status(self, tour):
        db_id = tour.get('id')
        db_title = tour.get('title', '').strip()
        db_dep_date = str(tour.get('departure_date', '')).replace('-', '').strip()
        # 부모로부터 JOIN해서 가져온 reference_code 사용
        ref_code = tour.get('reference_code') 
        
        payload = {
            "groupCls": "P",
            "itemCode": [ref_code], # 👈 자식 정보를 얻기 위한 참조 키로 조회
            "startDate": datetime.now().strftime("%Y-%m-%d"),
            "endDate": (datetime.now() + relativedelta(months=3)).strftime("%Y-%m-%d"),
            "pnums": [None],
            "filter": { "typeFilter": "PGTOverseasTravel", "isViewAllAvaiableSeat": False, "sort": "Hightest", "tourCondition": {"travelType": []} }
        }
        
        try:
            res = requests.post(self.api_url, json=payload, headers=self.headers, timeout=10)
            res_json = res.json()
            
            # 1단계: API 응답 결과 존재 확인
            master_details = res_json.get('result', {}).get('productMasterDetail', [])
            if not master_details:
                print(f"  [결과: 예약마감] 사유: API 응답에 해당 상품 마스터 정보가 없음.")
                return (db_id, "예약마감", None)

            data_list = master_details[0].get('productDate', [])
            print(f"  [진행] API 응답에서 {len(data_list)}개의 날짜 데이터를 발견했습니다.")
            
            # 2단계: 날짜 매칭 수행 (API는 2026-01-31 형식)
            target_info = None
            for d in data_list:
                api_sdate = d.get('date', {}).get('sdate', '').replace('-', '').strip()
                if api_sdate == db_dep_date:
                    target_info = d
                    break
            
            if not target_info:
                print(f"  [결과: 예약마감] 사유: API 결과 중 DB 날짜({db_dep_date})와 일치하는 일정이 없음.")
                return (db_id, "예약마감", None)

            # 3단계: 상태 변수 분석
            state_sort = target_info.get('stateSort')  # 1: 예약가능
            state_code = target_info.get('booking', {}).get('state')  # 2: 확인필요 등
            prefix = target_info.get('prefixPName', '')
            api_title = target_info.get('pName', '').strip()

            print(f"  [진행] 날짜 매칭 성공. 변수 확인 -> stateSort: {state_sort}, booking.state: {state_code}, prefix: '{prefix}'")

            # 제목 유사도 체크
            new_title = api_title if SequenceMatcher(None, self.clean_text(db_title), self.clean_text(api_title)).ratio() > 0.5 and db_title != api_title else None

            # 4단계: 최종 판정 로직
            if prefix == "출발확정" or state_code == 4 or state_sort == 0:
                print(f"  [결과: 정상(NULL)] 사유: 출발확정 조건(prefix/state_code) 충족.")
                return (db_id, None, new_title) 
            elif state_sort == 1:
                print(f"  [결과: 정상(NULL)] 사유: stateSort가 1(예약가능)임.")
                return (db_id, None, new_title)
            elif state_sort == 2:
                print(f"  [결과: 대기예약] 사유: stateSort가 2(대기예약)임.")
                return (db_id, "대기예약", new_title)
            else:
                print(f"  [결과: 예약마감] 사유: 판매 가능 코드(1, 2)를 벗어남. (현재: {state_sort})")
                return (db_id, "예약마감", new_title)

        except Exception as e:
            print(f"  [결과: 데이터에러] 사유: 통신/파싱 중 예외 발생 -> {str(e)}")
            return (db_id, "에러(통신장애)", None)

# --- 이하 get_automated_cookies 및 main 함수는 기존과 동일 ---

def get_automated_cookies():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    service = Service("/usr/bin/chromedriver") 
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.get("https://www.modetour.com/category/domestic") 
        time.sleep(7)
        cookies = driver.get_cookies()
        return "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    except: return ""
    finally: driver.quit()

def main():
    print("🍪 검증용 세션 쿠키 획득 중...")
    cookie = get_automated_cookies()
    conn = pymysql.connect(**DB_CONFIG)
    verifier = ModetourVerifier(cookie)
    
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT s.id, s.title, s.departure_date, t.reference_code 
                FROM tour_schedules s
                JOIN tours t ON s.product_code = t.product_code
                WHERE t.agency = '모두투어' AND s.departure_date >= CURDATE()
                AND (s.error_msg IS NULL OR s.error_msg != '예약마감')
            """
            cursor.execute(sql)
            tours = cursor.fetchall()
            
            if not tours:
                print("검증할 상품이 없습니다.")
                return

            print(f"🔎 {len(tours)}개 일정 정밀 검증 시작...")
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(verifier.check_status, tours))

            stats = {"정상": 0, "대기예약": 0, "예약마감": 0, "데이터에러": 0}
            for db_id, reason, new_title in results:
                if new_title:
                    cursor.execute("UPDATE tour_schedules SET error_msg=%s, title=%s, last_verified_at=NOW() WHERE id=%s", (reason, new_title, db_id))
                else:
                    cursor.execute("UPDATE tour_schedules SET error_msg=%s, last_verified_at=NOW() WHERE id=%s", (reason, db_id))
                
                if reason is None: stats["정상"] += 1
                elif "에러" in reason: stats["데이터에러"] += 1
                elif reason == "대기예약": stats["대기예약"] += 1
                else: stats["예약마감"] += 1
            
            conn.commit()

            report = (
                f"📝 [모두투어 정밀 검증 리포트]\n"
                f"✅ 총 검사: {len(tours)}건\n"
                f"🟢 예약가능: {stats['정상']}건\n"
                f"🟡 대기예약: {stats['대기예약']}건\n"
                f"🔴 예약마감: {stats['예약마감']}건\n"
                f"⚠️ 에러: {stats['데이터에러']}건"
            )
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", data={"chat_id": CHAT_ID, "text": report})
            print("🏁 검증 프로세스 종료.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()