import requests
from bs4 import BeautifulSoup
import time, logging
from datetime import datetime, timedelta
from utils import extract_all_keywords, get_db_connection

# --- 설정 및 상수 ---
AGENCY_NAME = "롯데관광"
LOTTE_PHONE = "1577-3000"
BASE_URL = "https://www.lottetour.com"
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("/home/ubuntu/Senior/Code/lotte_crawler.log", encoding='utf-8'), 
        logging.StreamHandler()
    ]
)

def send_telegram_msg(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try: requests.post(url, data=payload, timeout=10)
    except: pass

# ✅ [수정] tags 인자를 추가로 받도록 변경
def get_child_schedules(cursor, god_id, m1, m2, m3, m4, parent_title, start_time, tags):
    now = start_time
    months = [now.strftime("%Y%m"), (now.replace(day=28) + timedelta(days=5)).strftime("%Y%m")]
    
    today_str = now.strftime("%Y-%m-%d")
    limit_str = (now + timedelta(days=30)).strftime("%Y-%m-%d")
    
    child_api_url = "https://www.lottetour.com/evtlist/evtListAjax"
    headers = {"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"}

    saved_count = 0
    for dep_dt in months:
        params = {
            "menuNo1": m1, "menuNo2": m2, "menuNo3": m3, "menuNo4": m4,
            "evtOrderBy": "DT", "maxEvtCnt": "100", "godId": god_id, "depDt": dep_dt, "template": "pdtList"
        }

        try:
            res = requests.get(child_api_url, params=params, headers=headers, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            rows = soup.select("tbody > tr")

            for row in rows:
                date_raw = row.select("td")[1].get_text(strip=True).split('(')[0] # "01/30"
                mm, dd = date_raw.split('/')
                formatted_date = f"{dep_dt[:4]}-{mm.zfill(2)}-{dd.zfill(2)}"

                if not (today_str <= formatted_date <= limit_str):
                    continue

                price_tag = row.select_one(".price")
                price = ''.join(filter(str.isdigit, price_tag.get_text())) if price_tag else "0"

                status_td = row.select("td")[-1]
                status_nm = status_td.get_text(strip=True).split('잔여석')[0].strip()
                error_msg = None if status_nm in ["예약진행", "출발확정", "예약가능"] else status_nm

                link_tag = row.select_one(".goods_list_table_evtnm a")
                booking_url = f"{BASE_URL}{link_tag['href']}" if link_tag else ""

                # ✅ [수정] tags 컬럼 추가
                cursor.execute("""
                    INSERT INTO tour_schedules (
                        product_code, title, departure_date, price_text, 
                        booking_url, updated_at, last_verified_at, error_msg, tags
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        price_text=%s, updated_at=%s, last_verified_at=%s, error_msg=%s, tags=%s,
                        departure_date=%s
                """, (god_id, parent_title, formatted_date, price, 
                      booking_url, now, now, error_msg, tags,
                      price, now, now, error_msg, tags, formatted_date))
                saved_count += 1

        except Exception as e:
            logging.error(f"   ⚠️ 자식 일정 파싱 오류 ({god_id}): {e}")
    
    return saved_count

def run_collection():
    start_time = datetime.now()
    logging.info(f"🚀 {AGENCY_NAME} 전수 수집 시작")
    
    conn = get_db_connection()
    conn.autocommit(True)
    stats = {"total_rprs": 0, "saved_schedules": 0, "deleted_tours": 0}

    try:
        with conn.cursor() as cursor:
            parent_api_url = "https://www.lottetour.com/pdtListAjax"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "User-Agent": "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest"
            }

            page_index = 1
            while True:
                payload = {
                    "pageIndex": page_index, "menuNo1": "851", "menuNo2": "940", "menuNo3": "958",
                    "menuNo4": "0", "prdCnt": "36", "pageSize": "15", "orderType": "orderBy", "bType": "menuDept4"
                }

                res = requests.post(parent_api_url, headers=headers, data=payload, timeout=15)
                soup = BeautifulSoup(res.text, 'html.parser')
                items = soup.select("ul.list > li")
                
                if not items:
                    break

                for item in items:
                    god_id = item.select_one(".devCuponView")['godid'].strip()
                    title = item.select_one(".txt strong").get_text(strip=True)
                    desc_tag = item.select_one(".txt p")
                    description = desc_tag.get_text(strip=True) if desc_tag else title
                    
                    link_tag = item.select_one(".txt a")
                    href = link_tag['href']
                    path_parts = href.split('?')[0].split('/')
                    m1, m2, m3, m4 = path_parts[2], path_parts[3], path_parts[4], path_parts[5]

                    location = title.split(' ')[0].replace('◈', '').replace('[', '').replace(']', '').replace('《', '').replace('》', '')
                    
                    # ✅ [수정] 태그 추출 및 하위 함수 전달
                    tags = extract_all_keywords(title)

                    cursor.execute("""
                        INSERT INTO tours (product_code, reference_code, title, description, location, collected_at, agency, category, phone)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE title=%s, description=%s, location=%s, category=%s, collected_at=%s
                    """, (god_id, god_id, title, description, location, start_time, AGENCY_NAME, tags, LOTTE_PHONE,
                          title, description, location, tags, start_time))
                    stats["total_rprs"] += 1

                    logging.info(f"📦 [{stats['total_rprs']}] {title[:20]}...")
                    
                    # ✅ [수정] tags 인자 추가 전달
                    child_count = get_child_schedules(cursor, god_id, m1, m2, m3, m4, title, start_time, tags)
                    stats["saved_schedules"] += child_count

                if len(items) < 15:
                    break
                page_index += 1
                time.sleep(0.5)

            cleanup_limit = start_time - timedelta(minutes=30)
            cursor.execute("DELETE FROM tours WHERE agency = %s AND collected_at < %s", (AGENCY_NAME, cleanup_limit))
            stats["deleted_tours"] = cursor.rowcount

            # 로그 기록
            finish_time = datetime.now()
            cursor.execute("""
                INSERT INTO crawler_logs (agency_name, status, collected_count, crawled_at, message)
                VALUES (%s, %s, %s, %s, %s)
            """, (AGENCY_NAME, "SUCCESS", stats["saved_schedules"], finish_time, f"부모 {stats['total_rprs']}종 수집"))

        send_telegram_msg(f"🤖 [{AGENCY_NAME} 완료]\n📦 부모: {stats['total_rprs']}종\n🔹 일정: {stats['saved_schedules']}건\n🧹 삭제: {stats['deleted_tours']}종")

    except Exception as e:
        logging.error(f"❌ 치명적 오류: {e}")
        send_telegram_msg(f"❌ {AGENCY_NAME} 오류: {str(e)[:100]}")
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    run_collection()