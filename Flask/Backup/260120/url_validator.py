import pymysql
import requests
from bs4 import BeautifulSoup
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
    # 1. 기존 정책 체크 (숙박 1박/2박)
    title = tour.get('title', '')
    if "1박" in title or "2박" in title:
        tour['reason'] = "숙박 상품(1박/2박) 감지"
        return tour

    url = tour.get('booking_url', '')
    if not url or not url.startswith("http"):
        return None

    try:
        # URL 기반 여행사 판별
        low_url = url.lower()
        target_agency = ""
        if "hanatour" in low_url:
            target_agency = "하나투어"
        elif "modetour" in low_url:
            target_agency = "모두투어"

        response = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        if response.status_code != 200:
            tour['reason'] = f"접속불가({response.status_code})"
            return tour

        soup = BeautifulSoup(response.text, 'html.parser')

        # ---------------------------------------------------------
        # 2. 여행사별 맞춤형 예약 버튼 정밀 검증 (수정된 부분)
        # ---------------------------------------------------------
        
        # [A] 하나투어: gray 클래스 버튼의 텍스트 유연하게 검사
        if target_agency == "하나투어":
            # 클래스에 'gray'가 포함된 모든 span 탐색
            soldout_btns = soup.find_all('span', class_='gray')
            for btn in soldout_btns:
                btn_text = btn.get_text(strip=True)
                # "마감", "불가", "종료" 등 예약 불가능 키워드 포함 여부 확인
                if any(k in btn_text for k in ["마감", "불가", "종료", "매진"]):
                    tour['reason'] = f"하나투어: {btn_text} 버튼 감지"
                    return tour

        # [B] 모두투어: p 태그 내 텍스트 유연하게 검사
        elif target_agency == "모두투어":
            buttons = soup.find_all('button')
            for btn in buttons:
                p_tag = btn.find('p')
                if p_tag:
                    p_text = p_tag.get_text(strip=True)
                    # "마감", "불가", "종료", "매진" 포함 여부 확인
                    if any(k in p_text for k in ["마감", "불가", "종료", "매진"]):
                        tour['reason'] = f"모두투어: {p_text} 버튼 감지"
                        return tour

        # ---------------------------------------------------------
        # 3. 범용 검증 (키워드 및 본문 길이)
        # ---------------------------------------------------------
        content = response.text
        for keyword in INVALID_KEYWORDS:
            if keyword in content:
                tour['reason'] = f"키워드 감지({keyword})"
                return tour

        if len(content) < MIN_CONTENT_LENGTH:
            tour['reason'] = f"본문 부족({len(content)}자)"
            return tour

    except Exception as e:
        tour['reason'] = f"에러({str(e)[:15]})"
        return tour
    
    return None

# ==========================================
# 3. 메인 실행 프로세스
# ==========================================

def main():
    print(f"[{datetime.now()}] 정밀 검증 시작 (격일제 & 정상 상품 우선)...")    
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
                AND s.error_msg IS NULL  -- ✅ 1. 에러가 이미 발견된 상품은 제외
                -- ✅ 2. 검사한 지 36시간이 지난 상품만 (어제 검사한 건 오늘 통과)
                AND (s.last_verified_at < DATE_SUB(NOW(), INTERVAL 36 HOUR) OR s.last_verified_at IS NULL)
                ORDER BY s.departure_date ASC, s.last_verified_at ASC
                LIMIT 1000 -- ✅ 3. 한 번에 검사할 양 제한
            """
            cursor.execute(sql)
            tours = cursor.fetchall()
            
            if not tours:
                print("검사 대상이 없습니다. (모든 임박 상품이 36시간 내 검사되었거나 에러 상태임)")
                return

            print(f"총 {len(tours)}개 상품 검사 시작...")

            # 멀티스레딩 검증
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(check_single_link, tours))
                # None이 아닌 결과(이상 발견)만 추림
                broken_links = [r for r in results if r is not None]

            # ✅ 3. DB 업데이트 (검증 시간 + 에러 사유 기록)
            print("DB 업데이트 중 (last_verified_at 갱신)...")
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