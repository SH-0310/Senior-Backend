import pandas as pd
import requests
import logging
import pymysql
import time
from datetime import datetime, timedelta

# --- 설정 ---
SERVICE_KEY = "eb08c1ad2a7c050ba576b0d3669ecb5d82c5484660c0ec6df85fae3b563a1c2a"
CSV_PATH = "/home/ubuntu/Senior/Code/API/weather_coords.csv" 

# 모든 중기예보 구역 코드를 포함한 통합 매핑 표
MID_TERM_REG_MAP = {
    # 수도권 (서울, 인천, 경기)
    "서울": "11B00000", "인천": "11B00000", "경기": "11B00000", "수도권": "11B00000",
    # 강원권 (영서/영동 분리 필수)
    "강릉": "11D20000", "속초": "11D20000", "양양": "11D20000", "고성": "11D20000", "동해": "11D20000", "삼척": "11D20000",
    "춘천": "11D10000", "원주": "11D10000", "철원": "11D10000", "횡성": "11D10000", "홍천": "11D10000", "평창": "11D10000",
    # 충청권
    "대전": "11C20000", "세종": "11C20000", "충남": "11C20000", "천안": "11C20000", "보령": "11C20000", "부여": "11C20000",
    "청주": "11C10000", "충북": "11C10000", "제천": "11C10000", "충주": "11C10000",
    # 호남권
    "광주": "11F20000", "전남": "11F20000", "여수": "11F20000", "순천": "11F20000", "목포": "11F20000", "광양": "11F20000", "신안": "11F20000",
    "전주": "11F10000", "전북": "11F10000", "군산": "11F10000", "무주": "11F10000", "익산": "11F10000",
    # 영남권
    "대구": "11H10000", "경북": "11H10000", "안동": "11H10000", "포항": "11H10000", "영주": "11H10000",
    "부산": "11H20000", "울산": "11H20000", "경남": "11H20000", "창원": "11H20000", "거제": "11H20000", "통영": "11H20000", "남해": "11H20000",
    # 제주권
    "제주": "11G00000"
}


def get_db_connection():
    return pymysql.connect(
        host='localhost', user='shmoon', password='Tjdgursla87!',
        db='senior_travel', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def fetch_mid_term_weather(city):
    """중기육상예보(4~10일) API 호출 및 저장"""
    # 1. regId 매핑
    reg_id = next((code for name, code in MID_TERM_REG_MAP.items() if name in city), None)
    if not reg_id: return

    url = "http://apis.data.go.kr/1360000/MidFcstInfoService/getMidLandFcst"
    now = datetime.now()
    
    # 2. 발표시각(tmFc) 계산: 06시, 18시 기준
    if now.hour < 6:
        base_dt = (now - timedelta(days=1)).strftime("%Y%m%d") + "1800"
    elif now.hour < 18:
        base_dt = now.strftime("%Y%m%d") + "0600"
    else:
        base_dt = now.strftime("%Y%m%d") + "1800"

    params = {
        'serviceKey': SERVICE_KEY,
        'dataType': 'JSON',
        'regId': reg_id,
        'tmFc': base_dt
    }

    try:
        res = requests.get(url, params=params, timeout=30).json()
        item = res['response']['body']['items']['item'][0]
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 3일(또는 4일)부터 10일까지 순회하며 저장
            start_day = 4 if base_dt.endswith("0600") else 5
            for i in range(start_day, 11):
                fcst_date = (datetime.strptime(base_dt[:8], "%Y%m%d") + timedelta(days=i)).strftime("%Y%m%d")
                
                # 7일차까지는 오전/오후 존재, 8일부터는 하루 단위
                times = ['Am', 'Pm'] if i <= 7 else ['']
                for t in times:
                    wf_key = f'wf{i}{t}'
                    if wf_key in item:
                        weather_text = item[wf_key]
                        ampm = t.upper() if t else 'AM' # 8일 이후는 AM으로 통합 저장하거나 별도 처리
                        
                        sql = """
                            INSERT INTO weather_forecasts (location, forecast_date, ampm, weather_status)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE weather_status = %s, collected_at = NOW()
                        """
                        cursor.execute(sql, (city, fcst_date, ampm, weather_text, weather_text))
            conn.commit()
            logging.info(f"📅 {city} 중기예보 저장 완료 (기준구역: {reg_id})")
    except Exception as e:
        logging.error(f"❌ {city} 중기예보 실패: {e}")
    finally:
        if 'conn' in locals(): conn.close()


def get_mapping_info(cities):
    """Excel 바이너리 형식 파일에서 도시별 nx, ny 추출 및 매칭 로그 출력"""
    logging.info(f"📂 Excel 로드 시작 (대상 도시 수: {len(cities)})")
    try:
        df = pd.read_excel(CSV_PATH, engine='openpyxl')
        logging.info("✅ Excel 데이터 로드 성공")
    except Exception as e:
        logging.error(f"❌ Excel 로드 실패: {e}")
        return {}

    df.columns = [col.strip() for col in df.columns]
    mapping = {}
    
    for city in cities:
        if not city or city.strip() == "" or city in ["강원전체", "기타"]:
            continue
            
        # '수도권' 포함 시 '서울'로 검색어 변경 예외처리
        search_city = "서울" if "수도권" in city else city
        
        try:
            # 1순위: 2단계(시/군/구) 매칭
            match = df[df['2단계'].str.contains(search_city, na=False)]
            # 2순위: 1단계(시/도) 매칭
            if match.empty:
                match = df[df['1단계'].str.contains(search_city, na=False)]
            
            if not match.empty:
                row = match.iloc[0]
                nx, ny = int(row['격자 X']), int(row['격자 Y'])
                # 엑셀의 실제 지명(1단계 + 2단계) 조합
                excel_name = f"{row['1단계']} {row['2단계'] if pd.notna(row['2단계']) else ''}".strip()
                
                # 🔗 상세 매칭 로그 추가
                logging.info(f"🔗 매칭 성공: DB[{city}] -> Excel[{excel_name}] (좌표: {nx}, {ny})")
                
                mapping[city] = {'nx': nx, 'ny': ny, 'excel_name': excel_name}
            else:
                logging.warning(f"❓ 매칭 실패: DB[{city}]에 해당하는 좌표를 찾을 수 없음")
                
        except Exception as e:
            logging.error(f"❌ 매칭 중 오류 발생 ({city}): {e}")
            
    logging.info(f"✅ 최종 매칭 완료: {len(mapping)}개 도시")
    return mapping

def fetch_and_save_weather(city, info):
    """기상청 단기예보 API 호출 및 DB 저장 (발표 시간 자동 최적화)"""
    nx, ny = info['nx'], info['ny']
    excel_name = info['excel_name']
    
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
    
    # --- [시간 로직 추가] 현재 시각 기준으로 가장 최근 발표 시간 계산 ---
    now = datetime.now()
    # 단기예보 발표 시간 리스트 (02:00부터 3시간 간격)
    available_times = [2, 5, 8, 11, 14, 17, 20, 23]
    
    # 현재 시간보다 작거나 같은 마지막 발표 시간 찾기
    latest_time = 23 # 기본값 (어제 밤 11시)
    for t in available_times:
        if now.hour >= t:
            latest_time = t
        else:
            break
            
    # 만약 현재 시각이 새벽 2시 이전이라면 어제 날짜의 23시 데이터를 가져와야 함
    if now.hour < 2:
        base_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        base_time = "2300"
    else:
        base_date = now.strftime("%Y%m%d")
        base_time = f"{latest_time:02d}00"
    
    logging.info(f"📡 {city} 요청 시각: {base_date} / {base_time}")
    # -------------------------------------------------------------

    params = {
        'serviceKey': SERVICE_KEY,
        'numOfRows': '1000',
        'dataType': 'JSON',
        'base_date': base_date,
        'base_time': base_time,
        'nx': nx, 'ny': ny
    }

    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code != 200 or not response.text.strip() or response.text.startswith('<'):
                time.sleep(2)
                continue

            res_json = response.json()
            items = res_json.get('response', {}).get('body', {}).get('items', {}).get('item', [])
            
            if not items:
                logging.warning(f"⚠️ {city}({excel_name}) 예보 데이터가 없습니다.")
                return

            conn = get_db_connection()
            with conn.cursor() as cursor:
                save_count = 0
                for item in items:
                    if item['fcstTime'] in ['0900', '1500'] and item['category'] == 'SKY':
                        fcst_date = item['fcstDate']
                        ampm = 'AM' if item['fcstTime'] == '0900' else 'PM'
                        val = item['fcstValue']
                        weather_text = "맑음" if val == '1' else "구름많음" if val == '3' else "흐림"
                        
                        sql = """
                            INSERT INTO weather_forecasts (location, forecast_date, ampm, weather_status, nx, ny)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE 
                                weather_status = %s, collected_at = NOW()
                        """
                        cursor.execute(sql, (city, fcst_date, ampm, weather_text, nx, ny, weather_text))
                        save_count += 1
                conn.commit()
                logging.info(f"💾 {city} DB 저장 완료: {save_count}건 (기준: {excel_name})")
                return 

        except Exception as e:
            logging.warning(f"⚠️ {city} 수집 시도 {attempt+1}/3 실패: {e}")
            time.sleep(2)
    
    logging.error(f"❌ {city} 최종 수집 실패")

def run_weather_update():
    logging.info("🚀 날씨 업데이트 프로세스 시작")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # tours 테이블의 city 컬럼에서 고유 지명 추출
            cursor.execute("SELECT DISTINCT city FROM tours WHERE city IS NOT NULL")
            rows = cursor.fetchall()
            cities = [row['city'] for row in rows]
            logging.info(f"🔍 DB 조회 성공 (대상 지명: {len(cities)}개)")
    finally:
        conn.close()

    mapping = get_mapping_info(cities)
    
    for city, info in mapping.items():
        logging.info(f"⛅ {city} 날씨 정보 수집 시작...")
        
        # 1. 단기예보 (1~3일치)
        fetch_and_save_weather(city, info)
        
        # 2. 중기예보 (4~10일치) - 추가된 부분
        fetch_mid_term_weather(city)
        
        time.sleep(0.5)

    logging.info("🏁 모든 작업 종료")

if __name__ == "__main__":
    run_weather_update()