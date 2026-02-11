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
        # 1. 전체 데이터 개수 파악
        base_url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/culturalEventInfo/1/1/"
        response = requests.get(base_url)
        root = ET.fromstring(response.text)
        total_count = int(root.findtext('list_total_count') or 0)
        
        print(f"📅 [수집 시작] 총 {total_count}건의 데이터를 확인했습니다.")

        for start in range(1, total_count + 1, 1000):
            end = start + 999
            url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/culturalEventInfo/{start}/{end}/"
            
            res = requests.get(url)
            row_root = ET.fromstring(res.text)
            
            for row in row_root.findall('row'):
                hmpg_addr = row.findtext('HMPG_ADDR')
                cult_code = get_cult_code(hmpg_addr)
                
                # 날짜 전처리 (YYYY-MM-DD 형식 추출)
                def clean_date(d): return d.split(' ')[0] if d else None
                
                start_date = clean_date(row.findtext('STRTDATE'))
                end_date = clean_date(row.findtext('END_DATE'))
                rgst_date = clean_date(row.findtext('RGSTDATE'))

                # 2. 모든 필드를 포함한 SQL 쿼리
                sql = """
                    INSERT INTO seoul_events (
                        cult_code, title, codename, guname, date_range, place, 
                        org_name, use_target, use_fee, inquiry, player, program, 
                        etc_desc, is_free, main_img, hmpg_url, org_link, lat, lng, 
                        start_date, end_date, rgstdate, ticket, themecode, pro_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        title=VALUES(title),
                        date_range=VALUES(date_range),
                        place=VALUES(place),
                        use_fee=VALUES(use_fee),
                        inquiry=VALUES(inquiry),
                        player=VALUES(player),
                        program=VALUES(program),
                        main_img=VALUES(main_img),
                        hmpg_url=VALUES(hmpg_url),
                        end_date=VALUES(end_date),
                        pro_time=VALUES(pro_time)
                """
                
                # 위도/경도 안전 변환
                lat_val = safe_float(row.findtext('LAT'))
                lng_val = safe_float(row.findtext('LOT'))
                
                # 3. 데이터 매핑 (API 순서에 맞춤)
                params = (
                    cult_code, row.findtext('TITLE'), row.findtext('CODENAME'), 
                    row.findtext('GUNAME'), row.findtext('DATE'), row.findtext('PLACE'),
                    row.findtext('ORG_NAME'), row.findtext('USE_TRGT'), row.findtext('USE_FEE'),
                    row.findtext('INQUIRY'), row.findtext('PLAYER'), row.findtext('PROGRAM'),
                    row.findtext('ETC_DESC'), row.findtext('IS_FREE'), row.findtext('MAIN_IMG'),
                    hmpg_addr, row.findtext('ORG_LINK'), lat_val, lng_val,
                    start_date, end_date, rgst_date, row.findtext('TICKET'),
                    row.findtext('THEMECODE'), row.findtext('PRO_TIME')
                )
                
                cursor.execute(sql, params)
            
            print(f"⌛ {start} ~ {min(end, total_count)}번 데이터 처리 완료...")

        # 4. 종료된 행사 정리
        cursor.execute("DELETE FROM seoul_events WHERE end_date < CURDATE()")
        print("🧹 기간이 지난 행사 데이터를 정리했습니다.")

    except Exception as e:
        print(f"🚨 크롤링 중 에러 발생: {e}")
    finally:
        conn.close()
        print("✅ 모든 데이터 수집 및 정리가 종료되었습니다.")

if __name__ == "__main__":
    sync_seoul_culture_data()