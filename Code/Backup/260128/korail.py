import time, requests, logging, re, json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ✅ 공통 모듈 임포트
from utils import classify_categories, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "코레일관광개발"
KORAIL_PHONE = "1544-7755"
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/home/ubuntu/Senior/Code/korail_crawler.log", encoding='utf-8'), 
        logging.StreamHandler()
    ]
)

def send_telegram_msg(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try: requests.post(url, data=payload, timeout=10)
    except: pass

def get_detailed_data(parent_url, headers):
    """
    [개선] 상세 페이지의 JS를 분석하여 {날짜: 고유번호} 매핑 리스트를 가져옵니다.
    """
    results = {"price": "0", "schedules": []}
    try:
        res = requests.get(parent_url, headers=headers, timeout=10)
        res.encoding = 'euc-kr'
        html = res.text
        soup = BeautifulSoup(html, 'html.parser')

        # 1. 성인 가격 추출
        price_tag = soup.select_one('#adult_amt')
        if price_tag:
            results["price"] = price_tag.get_text(strip=True).replace(',', '')

        # 2. JS 날짜 및 고유번호(selNum) 추출
        # 예: {"2026-01-28":"353130", ...}
        match = re.search(r'const\s+select_info\s*=\s*(\{.*?\});', html, re.DOTALL)
        if match:
            try:
                date_map = json.loads(match.group(1))
                for raw_date, sel_num in date_map.items():
                    clean_date = raw_date.replace('-', '') # DB 저장용 (20260128)
                    results["schedules"].append({
                        "clean_date": clean_date,
                        "raw_date": raw_date,      # URL 파라미터용 (2026-01-28)
                        "sel_num": sel_num         # URL 파라미터용 (353130)
                    })
            except: pass
        
        # 3. JS 데이터가 없는 경우의 방어 로직 (배너 날짜)
        if not results["schedules"]:
            rep_date_tag = soup.select_one('.Banner_txt strong')
            if rep_date_tag:
                nums = re.findall(r'\d+', rep_date_tag.get_text())
                if len(nums) >= 3:
                    date_val = f"{nums[0]}-{nums[1].zfill(2)}-{nums[2].zfill(2)}"
                    results["schedules"].append({
                        "clean_date": date_val.replace('-', ''),
                        "raw_date": date_val,
                        "sel_num": "" # 번호를 알 수 없으므로 비움
                    })
        return results
    except Exception as e:
        logging.error(f" 상세 페이지 분석 에러: {e}")
        return results

def run_collection():
    start_time = datetime.now()
    today_str = start_time.strftime("%Y%m%d")
    limit_str = (start_time + timedelta(days=30)).strftime("%Y%m%d")
    
    logging.info(f"🚀 {AGENCY_NAME} '1일소풍' 고유 링크 수집 시작 ({today_str} ~ {limit_str})")
    
    conn = get_db_connection()
    conn.autocommit(True)
    stats = {"total_rprs": 0, "saved_schedules": 0, "deleted_tours": 0}

    try:
        page = 1
        with conn.cursor() as cursor:
            while True:
                list_url = f"https://www.korailtravel.com/web/goods_view/index.asp?page_nm=goods_list&gotopage={page}&strEpart=11"
                res = requests.get(list_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                res.encoding = 'euc-kr'
                soup = BeautifulSoup(res.text, 'html.parser')
                
                items = [item for item in soup.select('.tourBox') if item.select_one('#tourBox_Title_b')]
                if not items: break

                for item in items:
                    title_main = item.select_one('#tourBox_Title_b').get_text(strip=True)
                    title_sub = item.select_one('#tourBox_Title_s').get_text(strip=True)
                    
                    btn = item.select_one('.tourBox_Btn')
                    match = re.search(r'setList\(\d+,\s*(\d+)\)', btn.get('onclick', '')) if btn else None
                    if not match: continue
                    
                    goods_num = match.group(1)
                    # 상세 정보를 얻기 위한 기본 부모 URL
                    parent_url = f"https://www.korailtravel.com/web/goods_view/index.asp?page_nm=goods_day&goodsNum={goods_num}"

                    detail_data = get_detailed_data(parent_url, {'User-Agent': 'Mozilla/5.0'})
                    
                    # 1개월 이내 필터링된 일정만 추출
                    valid_items = [s for s in detail_data["schedules"] if today_str <= s["clean_date"] <= limit_str]
                    if not valid_items: continue

                    # 1. 부모 저장
                    categories = classify_categories(title_main)
                    loc_match = re.search(r'\[(.*?)\]', title_sub)
                    location = loc_match.group(1) if loc_match else "국내"

                    cursor.execute("""
                        INSERT INTO tours (product_code, reference_code, title, description, location, collected_at, agency, category, phone, is_priority)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
                        ON DUPLICATE KEY UPDATE title=%s, description=%s, location=%s, category=%s, collected_at=%s
                    """, (goods_num, goods_num, title_main, title_sub, location, start_time, AGENCY_NAME, categories, KORAIL_PHONE,
                          title_main, title_sub, location, categories, start_time))
                    stats["total_rprs"] += 1

                    # 2. 자식 저장 (출발일별 고유 URL 생성)
                    for s in valid_items:
                        # 🎯 [핵심] 출발일과 고유번호를 결합한 전용 예약 링크 생성
                        child_booking_url = f"https://www.korailtravel.com/web/goods_view/index.asp?page_nm=goods_day&selDate={s['raw_date']}&selNum={s['sel_num']}"
                        
                        cursor.execute("""
                            INSERT INTO tour_schedules (product_code, title, departure_date, price_text, booking_url, updated_at, last_verified_at, error_msg)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
                            ON DUPLICATE KEY UPDATE title=%s, price_text=%s, booking_url=%s, updated_at=%s, last_verified_at=%s, error_msg=NULL
                        """, (goods_num, title_main, s['clean_date'], detail_data["price"], child_booking_url, start_time, start_time,
                              title_main, detail_data["price"], child_booking_url, start_time, start_time))
                        stats["saved_schedules"] += 1

                    logging.info(f"   ✅ {goods_num} 동기화: {len(valid_items)}개 일정 (고유링크 생성 완료)")
                    time.sleep(0.4)

                if len(items) < 10: break
                page += 1

            # 3. Cleanup: 1시간 이전 데이터 삭제
            cleanup_limit_time = start_time - timedelta(hours=1)
            cursor.execute("DELETE FROM tours WHERE agency = %s AND collected_at < %s", (AGENCY_NAME, cleanup_limit_time))
            stats["deleted_tours"] = cursor.rowcount

            # ✅ [추가] 3-1. 크롤링 성공 로그 기록 (DB 저장)
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

        duration = datetime.now() - start_time
        report = f"🤖 [{AGENCY_NAME} '1일소풍' 동기화 완료]\n⏱ 소요시간: {str(duration).split('.')[0]}\n📦 유효 상품: {stats['total_rprs']}종\n🔹 1개월 내 일정: {stats['saved_schedules']}건\n🧹 삭제: {stats['deleted_tours']}종"
        send_telegram_msg(report)

    except Exception as e:
        logging.error(f"❌ 치명적 오류: {e}")
        # ✅ [추가] 에러 발생 시 FAIL 상태로 로그 남기기
        try:
            with get_db_connection() as err_conn:
                with err_conn.cursor() as err_cursor:
                    err_cursor.execute("""
                        INSERT INTO crawler_logs (agency_name, status, crawled_at, message)
                        VALUES (%s, %s, %s, %s)
                    """, (AGENCY_NAME, "FAIL", datetime.now(), str(e)[:200]))
                    err_conn.commit()
        except: pass
        
        send_telegram_msg(f"❌ {AGENCY_NAME} 치명적 오류: {str(e)[:100]}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    run_collection()