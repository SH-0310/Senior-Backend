import pymysql
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time

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

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
}

INVALID_KEYWORDS = ["판매 종료", "존재하지 않는 상품", "마감되었습니다", "잘못된 접근", "상품 정보를 찾을 수"]
MIN_CONTENT_LENGTH = 500

# ==========================================
# 2. 핵심 기능 함수들
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

def check_single_link(tour):
    # 1. 숙박 상품(1박/2박) 정책 체크
    title = tour.get('title', '')
    if "1박" in title or "2박" in title:
        tour['reason'] = "숙박 상품(1박/2박) 감지"
        return tour

    url = tour['booking_url']
    if not url or not url.startswith("http"):
        return None

    try:
        response = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        
        if response.status_code != 200:
            tour['reason'] = f"접속불가({response.status_code})"
            return tour

        content = response.text
        if len(content) < MIN_CONTENT_LENGTH:
            tour['reason'] = f"본문 부족({len(content)}자)"
            return tour

        for keyword in INVALID_KEYWORDS:
            if keyword in content:
                tour['reason'] = f"키워드 감지({keyword})"
                return tour

    except Exception as e:
        tour['reason'] = f"에러({str(e)[:15]})"
        return tour
    
    return None

# ==========================================
# 3. 메인 실행 프로세스
# ==========================================

def main():
    print(f"[{datetime.now()}] 정밀 검증 시작 (출발임박순 & DB기록)...")
    
    conn = get_db_connection()
    broken_links = []

    try:
        with conn.cursor() as cursor:
            # ✅ 쿼리: 출발일 빠른 순(1순위), 검사 오래된 순(2순위)으로 1,000개 추출
            sql = """
                SELECT s.product_code, t.title, t.agency, s.booking_url 
                FROM tour_schedules s
                JOIN tours t ON s.product_code = t.product_code
                WHERE s.departure_date >= CURDATE() 
                  AND s.departure_date <= DATE_ADD(CURDATE(), INTERVAL 1 MONTH)
                ORDER BY s.departure_date ASC, s.last_verified_at ASC
                LIMIT 1000
            """
            cursor.execute(sql)
            tours = cursor.fetchall()
            
            if not tours:
                print("검사할 상품이 없습니다.")
                return

            print(f"총 {len(tours)}개 상품 검사 시작...")

            # 멀티스레딩 검증
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(check_single_link, tours))
                # None이 아닌 결과(이상 발견)만 추림
                broken_links = [r for r in results if r is not None]

            # ✅ 3. DB 업데이트 (검증 시간 + 에러 사유 기록)
            print("DB 업데이트 중 (사유 저장)...")
            update_sql = "UPDATE tour_schedules SET last_verified_at = NOW(), error_msg = %s WHERE product_code = %s"
            
            with conn.cursor() as update_cursor:
                # 검사한 1,000개 전체에 대해 개별 업데이트
                for i, tour in enumerate(tours):
                    # 결과 리스트(results)에서 해당 상품의 이상 여부 확인
                    problem = results[i]
                    reason = problem['reason'] if problem else None # 이상 없으면 NULL
                    
                    update_cursor.execute(update_sql, (reason, tour['product_code']))
            conn.commit()

        # 4. 결과 처리 (텔레그램)
        if broken_links:
            report = f"🚨 [1일소풍] 이상 상품 {len(broken_links)}건 발견\n"
            for i, link in enumerate(broken_links[:10]):
                report += f"\n{i+1}. {link['agency']} | {link['title'][:15]}...\n🔗 {link['reason']}"
            
            if len(broken_links) > 10:
                report += f"\n\n외 {len(broken_links)-10}건 더 있음. (전체 목록은 DB 확인)"
            
            send_telegram_msg(report)
            print(f"검증 완료: {len(broken_links)}건 발견 보고.")
        else:
            send_telegram_msg(f"✅ [1일소풍] 오늘자 {len(tours)}개 검증 완료! 모두 정상입니다.")
            print("모든 링크 정상.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()