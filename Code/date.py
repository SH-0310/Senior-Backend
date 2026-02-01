import requests
import pymysql
import datetime
import time

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

DATE_KIND_MAP = {
    "01": "국경일",
    "02": "기념일",
    "03": "24절기",
    "04": "잡절"
}

def fetch_and_save(operation, year):
    url = f"{BASE_URL}/{operation}"
    params = {
        'serviceKey': requests.utils.unquote(SERVICE_KEY), # 키 인코딩 문제 방지
        'solYear': year,
        '_type': 'json',
        'numOfRows': '200' # 기념일은 100건이 넘을 수 있어 늘림
    }
    
    print(f"🔄 [{operation}] 요청 중...", end=" ", flush=True)
    
    try:
        # ✅ timeout(연결 대기 5초, 데이터 수신 15초) 추가
        response = requests.get(url, params=params, timeout=(5, 15))
        res_json = response.json()
        
        # 응답 구조 확인
        header = res_json.get('response', {}).get('header', {})
        if header.get('resultCode') != '00':
            print(f"\n❌ API 에러: {header.get('resultMsg')}")
            return

        body = res_json.get('response', {}).get('body', {})
        if not body or 'items' not in body or not body['items']:
            print("데이터 없음 (Skipped)")
            return

        items = body['items'].get('item', [])
        if isinstance(items, dict): # 데이터가 1개일 경우 dict로 오므로 list로 변환
            items = [items]

        if not items:
            print("데이터 없음 (Skipped)")
            return

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
            raw_kind = str(item.get('dateKind', ''))
            korean_kind = DATE_KIND_MAP.get(raw_kind, raw_kind)

            cursor.execute(sql, (
                str(item['locdate']),
                item['dateName'],
                item.get('isHoliday', 'N'),
                korean_kind
            ))

        conn.commit()
        print(f"✅ {len(items)}건 동기화 완료.")

    except requests.exceptions.Timeout:
        print("\n⚠️ 타임아웃 발생 (서버 응답 지연)")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    current_year = datetime.datetime.now().strftime("%Y")
    # 2026년 데이터를 수집하려면 직접 지정하거나 내년도까지 반복하도록 수정 가능
    target_year = "2026" 
    
    operations = [
        "getHoliDeInfo",
        "getRestDeInfo",
        "getAnniversaryInfo",
        "get24DivisionsInfo",
        "getSundryDayInfo"
    ]
    
    for op in operations:
        fetch_and_save(op, target_year)
        time.sleep(1) # API 서버 부하 방지를 위해 1초씩 휴식