import requests
import pymysql
import datetime

# 1. 설정 정보
SERVICE_KEY = "eb08c1ad2a7c050ba576b0d3669ecb5d82c5484660c0ec6df85fae3b563a1c2a"
BASE_URL = "http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService"
DB_CONFIG = {
    "host": "localhost",
    "user": "shmoon",
    "password": "Tjdgursla87!",
    "db": "senior_travel",
    "charset": "utf8mb4"
}

# dateKind 한글 매핑 딕셔너리 
DATE_KIND_MAP = {
    "01": "국경일",
    "02": "기념일",
    "03": "24절기",
    "04": "잡절"
}

def fetch_and_save(operation, year):
    url = f"{BASE_URL}/{operation}"
    params = {
        'serviceKey': SERVICE_KEY,
        'solYear': year,
        '_type': 'json',
        'numOfRows': '100'
    }
    
    try:
        response = requests.get(url, params=params)
        res_json = response.json()
        
        if res_json['response']['header']['resultCode'] != '00':
            print(f"API 에러 ({operation}): {res_json['response']['header']['resultMsg']}")
            return

        items = res_json['response']['body']['items'].get('item', [])
        if isinstance(items, dict):
            items = [items]

        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        sql = """
            INSERT INTO holiday_info (locdate, date_name, is_holiday, date_kind)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                date_name = VALUES(date_name),
                is_holiday = VALUES(is_holiday),
                date_kind = VALUES(date_kind),
                updated_at = CURRENT_TIMESTAMP
        """

        for item in items:
            # API에서 받은 코드(01, 02 등)를 한글 명칭으로 변환 
            raw_kind = str(item['dateKind'])
            korean_kind = DATE_KIND_MAP.get(raw_kind, raw_kind) # 매핑 없으면 원래 코드 유지

            cursor.execute(sql, (
                str(item['locdate']),
                item['dateName'],
                item.get('isHoliday', 'N'),
                korean_kind # 한글화된 명칭 저장
            ))

        conn.commit()
        print(f"[{operation}] {year}년 데이터 {len(items)}건 동기화 성공.")

    except Exception as e:
        print(f"오류 발생 ({operation}): {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    current_year = datetime.datetime.now().strftime("%Y")
    
    operations = [
        "getHoliDeInfo",       # 국경일 [cite: 34]
        "getRestDeInfo",       # 공휴일 [cite: 34]
        "getAnniversaryInfo",  # 기념일 [cite: 34]
        "get24DivisionsInfo",  # 24절기 [cite: 34]
        "getSundryDayInfo"     # 잡절 [cite: 34]
    ]
    
    for op in operations:
        fetch_and_save(op, current_year)