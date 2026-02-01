import pymysql
import requests
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs
from difflib import SequenceMatcher

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

    def clean_text(self, text):
        """유사도 측정을 위해 불필요한 공백 및 특수문자 제거"""
        text = re.sub(r'[^가-힣a-zA-Z0-9]', '', text)
        ignore_words = ['대표명소', '연합상품', '특가', '당일']
        for word in ignore_words:
            text = text.replace(word, '')
        return text

    def check_status(self, tour):
        """
        API를 분석하여 (DB_ID, product_code, 최종사유, 업데이트할제목) 튜플을 반환합니다.
        """
        db_id = tour.get('id')
        db_product_code = tour.get('product_code')
        db_title = tour.get('title', '').strip()
        db_dep_date = str(tour.get('departure_date', '')).replace('-', '')
        url = tour.get('booking_url', '')
        
        if not url: return (db_id, db_product_code, "에러(URL없음)", None)
        pkg_api_code = self.extract_pkg_code(url)
        if not pkg_api_code: return (db_id, db_product_code, "에러(코드추출실패)", None)

        headers = {
            "Content-Type": "application/json",
            "Origin": "https://www.hanatour.com",
            "Referer": url,
            "User-Agent": self.user_agent,
            "prgmid": "CHPC0PKG0200M200",
            "Accept": "application/json, text/plain, */*"
        }
        payload = {"pkgCd": pkg_api_code, "inpPathCd": "DCP", "smplYn": "N", "coopYn": "N", "partnerYn": "N", "resAcceptPtn": {}}

        try:
            response = requests.post(self.api_url, json=payload, headers=headers, timeout=10)
            if response.status_code != 200: 
                return (db_id, db_product_code, f"에러(통신장애:{response.status_code})", None)

            res_data = response.json().get('data', {})
            if not res_data: return (db_id, db_product_code, "에러(데이터부재)", None)

            api_title = res_data.get('saleProdNm', '').strip()
            api_dep_day = res_data.get('depDay', '')
            res_psbl = res_data.get('resAddPsblYn')
            bkng_stat = res_data.get('bkngStatCd')
            today = datetime.now().strftime('%Y%m%d')

            # [무결성 검증: 날짜 및 상품명 유사도]
            mismatch = []
            new_title_to_update = None
            
            if db_dep_date != api_dep_day: 
                mismatch.append("날짜불일치")
            
            db_clean = self.clean_text(db_title)
            api_clean = self.clean_text(api_title)
            similarity = SequenceMatcher(None, db_clean, api_clean).ratio()
            similarity_percent = similarity * 100
            
            if similarity_percent < 50:
                mismatch.append(f"상품명불일치({similarity_percent:.1f}%)")
            elif db_title != api_title: # 유사도는 50%를 넘으나 문자열이 완벽히 같지 않을 때만 업데이트 예약
                new_title_to_update = api_title

            if mismatch: 
                return (db_id, db_product_code, f"에러({', '.join(mismatch)})", None)

            # [예약 상태 판정]
            if api_dep_day and api_dep_day <= today: 
                return (db_id, db_product_code, "예약마감", new_title_to_update)
            
            if res_psbl == "Y" and bkng_stat == "2":
                return (db_id, db_product_code, None, new_title_to_update)
            elif res_psbl == "Y" and bkng_stat == "1":
                return (db_id, db_product_code, "대기예약", new_title_to_update)
            else:
                return (db_id, db_product_code, "예약마감", new_title_to_update)

        except Exception as e:
            return (db_id, db_product_code, f"에러(통신장애:{str(e)[:15]})", None)

# ==========================================
# 3. 메인 실행부
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
    print(f"[{start_time}] 하나투어 정밀 무결성 검증 시작...")
    
    conn = get_db_connection()
    verifier = HanaTourVerifier()
    
    try:
        with conn.cursor() as cursor:
            # 🎯 자식 테이블(tour_schedules)의 개별 id와 제목을 가져옵니다.
            sql = """
                SELECT s.id, s.product_code, s.title, s.departure_date, s.booking_url 
                FROM tour_schedules s
                JOIN tours t ON s.product_code = t.product_code
                WHERE t.agency = '하나투어'
                AND s.departure_date >= CURDATE()
                AND (s.error_msg IS NULL OR s.error_msg = '대기예약')
                ORDER BY s.departure_date ASC
                LIMIT 1000
            """
            cursor.execute(sql)
            tours = cursor.fetchall()
            
            if not tours:
                print("검사 대상이 없습니다.")
                return

            print(f"총 {len(tours)}개 상품 분석 중...")
            with ThreadPoolExecutor(max_workers=15) as executor:
                results = list(executor.map(verifier.check_status, tours))

            # 업데이트 쿼리 준비
            sql_with_title = "UPDATE tour_schedules SET last_verified_at = NOW(), error_msg = %s, title = %s WHERE id = %s"
            sql_without_title = "UPDATE tour_schedules SET last_verified_at = NOW(), error_msg = %s WHERE id = %s"

            stats = {"정상": 0, "대기예약": 0, "예약마감": 0, "데이터에러": 0, "제목갱신": 0}

            with conn.cursor() as update_cursor:
                for db_id, p_code, reason, new_title in results:
                    if new_title:
                        # 1. 새 제목이 있는 경우: 제목과 상태를 모두 업데이트
                        update_cursor.execute(sql_with_title, (reason, new_title, db_id))
                        stats["제목갱신"] += 1
                    else:
                        # 2. 새 제목이 없는 경우: 기존 제목 유지하고 상태만 업데이트
                        update_cursor.execute(sql_without_title, (reason, db_id))

                    # 통계 집계
                    if reason is None: stats["정상"] += 1
                    elif "에러" in reason: stats["데이터에러"] += 1
                    elif reason == "대기예약": stats["대기예약"] += 1
                    else: stats["예약마감"] += 1

                conn.commit()

            # 리포트 발송
            report = (
                f"📝 [하나투어 정밀 검증 리포트]\n"
                f"📅 일시: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"--------------------------\n"
                f"✅ 총 검사 상품: {len(tours)}건\n"
                f"🟢 예약 가능: {stats['정상']}건\n"
                f"🟡 대기 예약: {stats['대기예약']}건\n"
                f"🔴 예약 마감: {stats['예약마감']}건\n"
                f"⚠️ 데이터 에러: {stats['데이터에러']}건\n"
                f"✨ 제목 자동갱신: {stats['제목갱신']}건\n"
                f"--------------------------\n"
                f"※ 1일소풍 앱 데이터 동기화 완료"
            )
            send_telegram_msg(report)
            print(f"✅ 검증 완료.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()