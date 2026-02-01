import time, requests, logging, re, json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ✅ 공통 모듈 임포트
from utils import extract_all_keywords, get_db_connection

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
    results = {"price": "0", "schedules": []}
    try:
        res = requests.get(parent_url, headers=headers, timeout=10)
        res.encoding = 'euc-kr'
        html = res.text
        soup = BeautifulSoup(html, 'html.parser')

        price_tag = soup.select_one('#adult_amt')
        if price_tag:
            results["price"] = price_tag.get_text(strip=True).replace(',', '')

        match = re.search(r'const\s+select_info\s*=\s*(\{.*?\});', html, re.DOTALL)
        if match:
            try:
                date_map = json.loads(match.group(1))
                for raw_date, sel_num in date_map.items():
                    results["schedules"].append({
                        "db_date": raw_date,
                        "sel_num": sel_num
                    })
            except: pass
        
        if not results["schedules"]:
            rep_date_tag = soup.select_one('.Banner_txt strong')
            if rep_date_tag:
                nums = re.findall(r'\d+', rep_date_tag.get_text())
                if len(nums) >= 3:
                    date_val = f"{nums[0]}-{nums[1].zfill(2)}-{nums[2].zfill(2)}"
                    results["schedules"].append({
                        "db_date": date_val,
                        "sel_num": "" 
                    })
        return results
    except Exception as e:
        logging.error(f" 상세 페이지 분석 에러: {e}")
        return results

def run_collection():
    start_time = datetime.now()
    today_str = start_time.strftime("%Y-%m-%d")
    limit_str = (start_time + timedelta(days=30)).strftime("%Y-%m-%d")
    
    logging.info(f"🚀 {AGENCY_NAME} 수집 시작 ({today_str} ~ {limit_str})")
    
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
                
                items = soup.select('.tourBox')
                if not items: break

                for item in items:
                    title_main_tag = item.select_one('#tourBox_Title_b')
                    title_sub_tag = item.select_one('#tourBox_Title_s')
                    if not title_main_tag: continue
                    
                    title_main = title_main_tag.get_text(strip=True)
                    title_sub = title_sub_tag.get_text(strip=True) if title_sub_tag else ""

                    # ✅ [추가] 이미지 URL 추출 (tourBoxImg 클래스 활용)
                    img_tag = item.select_one('.tourBoxImg img')
                    raw_img_url = img_tag.get('src') if img_tag else ""
                    
                    # http 주소를 https로 보정 (앱 보안 연결 대응)
                    main_img_url = raw_img_url.replace("http://", "https://") if raw_img_url else ""

                    btn = item.select_one('.tourBox_Btn')
                    match = re.search(r'setList\(\d+,\s*(\d+)\)', btn.get('onclick', '')) if btn else None
                    if not match: continue
                    
                    goods_num = match.group(1)
                    parent_url = f"https://www.korailtravel.com/web/goods_view/index.asp?page_nm=goods_day&goodsNum={goods_num}"

                    detail_data = get_detailed_data(parent_url, {'User-Agent': 'Mozilla/5.0'})
                    valid_items = [s for s in detail_data["schedules"] if today_str <= s["db_date"] <= limit_str]
                    if not valid_items: continue

                    tags = extract_all_keywords(title_main)
                    loc_match = re.search(r'\[(.*?)\]', title_sub)
                    location = loc_match.group(1) if loc_match else "국내"

                    # 1. 부모 저장 (main_image_url 추가)
                    # 1. 부모 저장 (INSERT 부분에 %s 하나 추가)
                    cursor.execute("""
                        INSERT INTO tours (product_code, reference_code, title, description, main_image_url, location, collected_at, agency, category, phone, is_priority)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
                        ON DUPLICATE KEY UPDATE 
                            title=%s, 
                            description=%s, 
                            main_image_url=%s, 
                            location=%s, 
                            category=%s, 
                            collected_at=%s
                    """, (
                        # INSERT를 위한 10개 변수
                        goods_num, goods_num, title_main, title_sub, main_img_url, 
                        location, start_time, AGENCY_NAME, tags, KORAIL_PHONE,
                        # UPDATE를 위한 6개 변수
                        title_main, title_sub, main_img_url, location, tags, start_time
                    ))
                    stats["total_rprs"] += 1

                    # 2. 자식 저장
                    for s in valid_items:
                        child_booking_url = f"https://www.korailtravel.com/web/goods_view/index.asp?page_nm=goods_day&selDate={s['db_date']}&selNum={s['sel_num']}"
                        
                        cursor.execute("""
                            INSERT INTO tour_schedules (product_code, title, departure_date, price_text, booking_url, updated_at, last_verified_at, tags)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                                title=%s, price_text=%s, booking_url=%s, updated_at=%s, last_verified_at=%s, tags=%s,
                                departure_date=%s
                        """, (goods_num, title_main, s['db_date'], detail_data["price"], child_booking_url, start_time, start_time, tags,
                              title_main, detail_data["price"], child_booking_url, start_time, start_time, tags, s['db_date']))
                        stats["saved_schedules"] += 1

                    logging.info(f" 📸 이미지 확인: {main_img_url[:50]}...")
                    logging.info(f" ✅ {goods_num} 동기화 완료 ({len(valid_items)}개 일정)")
                    time.sleep(0.4)

                if len(items) < 10: break
                page += 1

            cleanup_limit_time = start_time - timedelta(hours=1)
            cursor.execute("DELETE FROM tours WHERE agency = %s AND collected_at < %s", (AGENCY_NAME, cleanup_limit_time))
            stats["deleted_tours"] = cursor.rowcount

            finish_time = datetime.now()
            cursor.execute("""
                INSERT INTO crawler_logs (agency_name, status, collected_count, crawled_at, message)
                VALUES (%s, %s, %s, %s, %s)
            """, (AGENCY_NAME, "SUCCESS", stats["saved_schedules"], finish_time, f"부모 {stats['total_rprs']}종 수집"))

        duration = datetime.now() - start_time
        send_telegram_msg(f"🤖 [{AGENCY_NAME} 완료]\n📦 유효 상품: {stats['total_rprs']}종\n🔹 자식 일정: {stats['saved_schedules']}건\n⏱ 소요시간: {str(duration).split('.')[0]}")

    except Exception as e:
        logging.error(f"❌ 치명적 오류: {e}")
        send_telegram_msg(f"❌ {AGENCY_NAME} 오류: {str(e)[:100]}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    run_collection()