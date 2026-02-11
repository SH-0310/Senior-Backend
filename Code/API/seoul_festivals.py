import requests
import xml.etree.ElementTree as ET
import re
from datetime import datetime
import pymysql

API_KEY = "57704857666d73683738494b526d72"

def get_db_connection():
    return pymysql.connect(
        host='localhost', user='shmoon', password='Tjdgursla87!',
        db='senior_travel', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# ✅ 추가: 데이터 오염(예: '37.123~2')을 방지하는 안전한 숫자 변환 함수
def safe_float(value):
    if not value:
        return 0.0
    try:
        # '~' 문자가 포함되어 있다면 앞쪽 숫자만 취함
        clean_value = str(value).split('~')[0].strip()
        return float(clean_value)
    except (ValueError, TypeError):
        return 0.0

def get_cult_code(url):
    """상세 URL에서 고유 번호(cultcode) 추출"""
    if not url: return None
    match = re.search(r'cultcode=(\d+)', url)
    return match.group(1) if match else None

def sync_seoul_culture_data():
    conn = get_db_connection()
    conn.autocommit(True)
    cursor = conn.cursor()

    try:
        # 1. 전체 데이터 개수 파악하기
        base_url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/culturalEventInfo/1/1/"
        response = requests.get(base_url)
        root = ET.fromstring(response.text)
        total_count = int(root.findtext('list_total_count') or 0)
        
        print(f"📅 [수집 시작] 총 {total_count}건의 데이터를 확인했습니다.")

        # 2. 1000건씩 끊어서 전체 데이터 수집 (페이지네이션)
        for start in range(1, total_count + 1, 1000):
            end = start + 999
            url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/culturalEventInfo/{start}/{end}/"
            
            res = requests.get(url)
            row_root = ET.fromstring(res.text)
            
            for row in row_root.findall('row'):
                hmpg_addr = row.findtext('HMPG_ADDR')
                cult_code = get_cult_code(hmpg_addr)
                
                # 날짜 전처리
                raw_start = row.findtext('STRTDATE')
                start_date = raw_start.split(' ')[0] if raw_start else None
                raw_end = row.findtext('END_DATE')
                end_date = raw_end.split(' ')[0] if raw_end else None

                # DB 저장 (중복 시 업데이트 - ON DUPLICATE KEY UPDATE)
                sql = """
                    INSERT INTO seoul_events (
                        cult_code, title, codename, guname, date_range, place, 
                        use_target, use_fee, is_free, main_img, hmpg_url, lat, lng, start_date, end_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title=VALUES(title),
                        date_range=VALUES(date_range),
                        place=VALUES(place),
                        use_fee=VALUES(use_fee),
                        main_img=VALUES(main_img),
                        end_date=VALUES(end_date)
                """
                
                # ✅ safe_float 함수를 사용하여 LAT/LOT 값을 안전하게 추출
                lat_val = safe_float(row.findtext('LAT'))
                lng_val = safe_float(row.findtext('LOT'))
                
                params = (
                    cult_code, row.findtext('TITLE'), row.findtext('CODENAME'), 
                    row.findtext('GUNAME'), row.findtext('DATE'), row.findtext('PLACE'),
                    row.findtext('USE_TRGT'), row.findtext('USE_FEE'), row.findtext('IS_FREE'),
                    row.findtext('MAIN_IMG'), hmpg_addr, 
                    lat_val, lng_val,
                    start_date, end_date
                )
                
                cursor.execute(sql, params)
            
            print(f"⌛ {start} ~ {min(end, total_count)}번 데이터 처리 완료...")

        # 3. 종료된 행사 정리
        cursor.execute("DELETE FROM seoul_events WHERE end_date < CURDATE()")
        print("🧹 기간이 지난 행사 데이터를 정리했습니다.")

    except Exception as e:
        print(f"🚨 크롤링 중 에러 발생: {e}")
    finally:
        conn.close()
        print("✅ 수집 작업이 종료되었습니다.")

if __name__ == "__main__":
    sync_seoul_culture_data()