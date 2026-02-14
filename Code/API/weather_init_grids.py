import os
import logging
import pandas as pd
import pymysql

# =========================
# 설정
# =========================
EXCEL_PATH = "/home/ubuntu/Senior/Code/API/weather_coords.csv"  # 실제는 xlsx 포맷
DB_HOST = "localhost"
DB_USER = "shmoon"
DB_PASS = os.getenv("DB_PASS", "")  # export DB_PASS="..." 로 설정 권장
DB_NAME = "senior_travel"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# =========================
# 대표 격자 (<50개) 정의
# (원하면 여기만 조정하면 됨)
# =========================
REP_GRIDS = [
    # --- 수도권(예시 18: 서울10 + 인천2 + 경기6) ---
    ("seoul_jongno", "서울 종로", "종로구"),
    ("seoul_jung", "서울 중구", "중구"),
    ("seoul_gangnam", "서울 강남", "강남구"),
    ("seoul_songpa", "서울 송파", "송파구"),
    ("seoul_mapo", "서울 마포", "마포구"),
    ("seoul_yeongdeungpo", "서울 영등포", "영등포구"),
    ("seoul_gangseo", "서울 강서", "강서구"),
    ("seoul_nowon", "서울 노원", "노원구"),
    ("seoul_seocho", "서울 서초", "서초구"),
    ("seoul_seongdong", "서울 성동", "성동구"),

    ("incheon_bupyeong", "인천 부평", "부평구"),
    ("incheon_yeonsu", "인천 연수(송도)", "연수구"),

    ("gyeonggi_suwon", "경기 수원", "수원시"),
    ("gyeonggi_seongnam", "경기 성남", "성남시"),
    ("gyeonggi_yongin", "경기 용인", "용인시"),
    ("gyeonggi_goyang", "경기 고양", "고양시"),
    ("gyeonggi_bucheon", "경기 부천", "부천시"),
    ("gyeonggi_uijeongbu", "경기 의정부", "의정부시"),

    # --- 강원(4) ---
    ("gangwon_chuncheon", "강원 춘천", "춘천시"),
    ("gangwon_wonju", "강원 원주", "원주시"),
    ("gangwon_gangneung", "강원 강릉", "강릉시"),
    ("gangwon_sokcho", "강원 속초", "속초시"),

    # --- 충청(6) ---
    ("chung_daejeon", "대전", "대전광역시"),
    ("chung_sejong", "세종", "세종특별자치시"),
    ("chung_cheonan", "충남 천안", "천안시"),
    ("chung_cheongju", "충북 청주", "청주시"),
    ("chung_chungju", "충북 충주", "충주시"),
    ("chung_boryeong", "충남 보령", "보령시"),

    # --- 전라(6) ---
    ("jeolla_gwangju", "광주", "광주광역시"),
    ("jeolla_jeonju", "전북 전주", "전주시"),
    ("jeolla_gunsan", "전북 군산", "군산시"),
    ("jeolla_mokpo", "전남 목포", "목포시"),
    ("jeolla_yeosu", "전남 여수", "여수시"),
    ("jeolla_suncheon", "전남 순천", "순천시"),

    # --- 경상(9) ---
    ("gyeong_daegu", "대구", "대구광역시"),
    ("gyeong_pohang", "경북 포항", "포항시"),
    ("gyeong_andong", "경북 안동", "안동시"),
    ("gyeong_busan", "부산", "부산광역시"),
    ("gyeong_ulsan", "울산", "울산광역시"),
    ("gyeong_changwon", "경남 창원", "창원시"),
    ("gyeong_jinju", "경남 진주", "진주시"),
    ("gyeong_tongyeong", "경남 통영", "통영시"),
    ("gyeong_geoje", "경남 거제", "거제시"),

    # --- 제주(2) ---
    ("jeju_jeju", "제주 제주시", "제주시"),
    ("jeju_seogwipo", "제주 서귀포", "서귀포시"),
]


def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS weather_grids (
      grid_id   VARCHAR(50)  NOT NULL PRIMARY KEY,
      grid_name VARCHAR(100) NOT NULL,
      nx INT NOT NULL,
      ny INT NOT NULL,
      lat DOUBLE NOT NULL,
      lng DOUBLE NOT NULL,
      keyword VARCHAR(50) NULL,
      created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(ddl)
        conn.commit()
        logging.info("✅ weather_grids 테이블 준비 완료")
    finally:
        conn.close()


def load_excel_df():
    # 확장자는 csv지만 실제는 xlsx라 openpyxl로 읽음
    df = pd.read_excel(EXCEL_PATH, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]
    return df


def pick_row(df, keyword: str):
    # 1) 2단계(시군구) 우선
    m = df[df["2단계"].astype(str).str.contains(keyword, na=False)]
    if m.empty:
        # 2) 1단계(시/도) fallback
        m = df[df["1단계"].astype(str).str.contains(keyword, na=False)]
    if m.empty:
        return None
    return m.iloc[0]


def upsert_grids():
    df = load_excel_df()

    required_cols = ["격자 X", "격자 Y", "위도(초/100)", "경도(초/100)", "1단계", "2단계"]
    for col in required_cols:
        if col not in df.columns:
            raise RuntimeError(f"엑셀 컬럼이 없습니다: {col} / 현재 컬럼: {list(df.columns)}")

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            ok = 0
            fail = 0

            for grid_id, grid_name, keyword in REP_GRIDS:
                row = pick_row(df, keyword)
                if row is None:
                    logging.warning(f"❓ 매칭 실패: {grid_id} / {grid_name} / keyword={keyword}")
                    fail += 1
                    continue

                nx = int(row["격자 X"])
                ny = int(row["격자 Y"])
                lat = float(row["위도(초/100)"])
                lng = float(row["경도(초/100)"])

                cursor.execute(
                    """
                    INSERT INTO weather_grids (grid_id, grid_name, nx, ny, lat, lng, keyword)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                      grid_name=VALUES(grid_name),
                      nx=VALUES(nx), ny=VALUES(ny),
                      lat=VALUES(lat), lng=VALUES(lng),
                      keyword=VALUES(keyword)
                    """,
                    (grid_id, grid_name, nx, ny, lat, lng, keyword),
                )
                ok += 1

            conn.commit()
            logging.info(f"✅ 대표 격자 upsert 완료: 성공 {ok} / 실패 {fail}")

    finally:
        conn.close()


if __name__ == "__main__":
    ensure_table()
    upsert_grids()
