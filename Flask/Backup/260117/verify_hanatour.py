import pymysql
import requests
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs

# ==========================================
# 1. 설정 정보
# ==========================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'shmoon',
    'password': 'Tjdgursla87!',
    'db': 'senior_travel',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

# ==========================================
# 2. 하나투어 검증 클래스
# ==========================================
class HanaTourVerifier:
    def __init__(self):
        self.api_url = "https://gw.hanatour.com/package/pkg/api/common/pkgcomprod/getPkgProdInfo/v1.00?_siteId=hanatour"
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"

    def extract_pkg_code(self, url):
        """URL에서 상품코드(pkgCd)를 추출합니다."""
        try:
            parsed_url = urlparse(url)
            params = parse_qs(parsed_url.query)
            return params.get('pkgCd', [None])[0]
        except Exception:
            return None

    def check_status(self, tour):
        """
        API를 분석하여 (DB_ID, 최종사유) 튜플을 반환합니다.
        사유가 None이면 예약 가능(Y/2)입니다.
        """
        db_id = tour.get('id')
        db_title = tour.get('title', '').strip()
        # DB 날짜 포맷을 API와 대조하기 위해 YYYYMMDD로 변환
        db_dep_date = str(tour.get('departure_date', '')).replace('-', '')
        url = tour.get('booking_url', '')
        
        # [에러 카테고리 3: 설정 에러]
        if not url: 
            return (db_id, "에러(URL없음)")
        pkg_api_code = self.extract_pkg_code(url)
        if not pkg_api_code: 
            return (db_id, "에러(코드추출실패)")

        headers = {
            "Content-Type": "application/json",
            "Origin": "https://www.hanatour.com",
            "Referer": url,
            "User-Agent": self.user_agent,
            "prgmid": "CHPC0PKG0200M200",
            "Accept": "application/json, text/plain, */*"
        }
        payload = {
            "pkgCd": pkg_api_code,
            "inpPathCd": "DCP",
            "smplYn": "N",
            "coopYn": "N",
            "partnerYn": "N",
            "resAcceptPtn": {}
        }

        try:
            response = requests.post(self.api_url, json=payload, headers=headers, timeout=10)
            
            # [에러 카테고리 4: 통신 에러]
            if response.status_code != 200: 
                return (db_id, f"에러(통신장애:상태코드 {response.status_code})")

            res_data = response.json().get('data', {})
            if not res_data: 
                return (db_id, "에러(데이터부재)")

            # API 실시간 데이터 추출
            api_title = res_data.get('saleProdNm', '').strip()
            api_dep_day = res_data.get('depDay', '')
            res_psbl = res_data.get('resAddPsblYn')  # Y/N
            bkng_stat = res_data.get('bkngStatCd')   # 2:정상, 1:대기, 0:마감
            today = datetime.now().strftime('%Y%m%d')

            # [에러 카테고리 2: 데이터 무결성 검증]
            mismatch = []
            if db_dep_date != api_dep_day: 
                mismatch.append("날짜불일치")
            if db_title.replace(" ", "") != api_title.replace(" ", ""): 
                mismatch.append("상품명불일치")
            
            if mismatch: 
                return (db_id, f"에러({', '.join(mismatch)})")

            # [카테고리 1: 정상 및 예약 상태 판정]
            # 출발일 당일/과거 체크
            if api_dep_day and api_dep_day <= today: 
                return (db_id, "예약마감")
            
            # 최종 상태 매핑
            if res_psbl == "Y" and bkng_stat == "2":
                return (db_id, None)        # 정상 예약 가능
            elif res_psbl == "Y" and bkng_stat == "1":
                return (db_id, "대기예약")   # 대기 예약
            else:
                return (db_id, "예약마감")   # 그 외(N/0 등)

        except Exception as e:
            # [에러 카테고리 4: 시스템 예외]
            return (db_id, f"에러(통신장애:{str(e)[:15]})")

# ==========================================
# 3. 메인 실행부 (DB 업데이트 및 리포트)
# ==========================================
def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def send_telegram_msg(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")

def main():
    start_time = datetime.now()
    print(f"[{start_time}] 하나투어 정밀 무결성 검증 및 리포트 시작...")
    conn = get_db_connection()
    verifier = HanaTourVerifier()
    
    try:
        with conn.cursor() as cursor:
            # 검사 대상 추출: NULL이거나 가변적인 '대기예약' 상품 포함 (36시간 룰 제거됨)
            sql = """
                SELECT s.id, t.title, s.departure_date, s.booking_url 
                FROM tour_schedules s
                JOIN tours t ON s.product_code = t.product_code
                WHERE t.agency = '하나투어'
                AND s.departure_date >= CURDATE()
                AND (s.error_msg IS NULL OR s.error_msg = '대기예약')
                ORDER BY s.departure_date ASC
                LIMIT 300
            """
            cursor.execute(sql)
            tours = cursor.fetchall()
            
            if not tours:
                print("검사 대상이 없습니다.")
                return

            print(f"총 {len(tours)}개 상품 분석 중...")
            with ThreadPoolExecutor(max_workers=15) as executor:
                results = list(executor.map(verifier.check_status, tours))

            # DB 업데이트 및 결과 집계
            update_sql = "UPDATE tour_schedules SET last_verified_at = NOW(), error_msg = %s WHERE id = %s"
            
            stats = {"정상": 0, "대기예약": 0, "예약마감": 0, "데이터에러": 0}
            
            with conn.cursor() as update_cursor:
                for db_id, reason in results:
                    update_cursor.execute(update_sql, (reason, db_id))
                    
                    if reason is None:
                        stats["정상"] += 1
                    elif "에러" in reason:
                        stats["데이터에러"] += 1
                    elif reason == "대기예약":
                        stats["대기예약"] += 1
                    else:
                        stats["예약마감"] += 1
            
            conn.commit()

            # -------------------------------------------------------
            # 📊 텔레그램 종합 보고서 발송
            # -------------------------------------------------------
            report = (
                f"📝 [하나투어 정밀 검증 리포트]\n"
                f"📅 일시: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"--------------------------\n"
                f"✅ 총 검사 상품: {len(tours)}건\n"
                f"🟢 예약 가능: {stats['정상']}건\n"
                f"🟡 대기 예약: {stats['대기예약']}건\n"
                f"🔴 예약 마감: {stats['예약마감']}건\n"
                f"⚠️ 데이터 에러: {stats['데이터에러']}건\n"
                f"--------------------------\n"
                f"※ '에러' 항목은 관리자 확인이 필요합니다."
            )
            
            send_telegram_msg(report)
            print(f"✅ 검증 완료 및 리포트 발송 성공 (에러 {stats['데이터에러']}건 발견).")

    finally:
        conn.close()

if __name__ == "__main__":
    main()