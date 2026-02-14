import time
import logging
import requests
import pymysql
from datetime import datetime, timedelta

DB_PASS = 'Tjdgursla87!'

DB_HOST = "localhost"
DB_USER = "shmoon"
DB_NAME = "senior_travel"
SERVICE_KEY = "eb08c1ad2a7c050ba576b0d3669ecb5d82c5484660c0ec6df85fae3b563a1c2a"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
session = requests.Session()

def get_db_connection():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS,
        db=DB_NAME, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

def get_latest_base_datetime(now: datetime):
    available = [2, 5, 8, 11, 14, 17, 20, 23]
    if now.hour < 2:
        base_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        base_time = "2300"
        return base_date, base_time
    latest = max([h for h in available if now.hour >= h])
    return now.strftime("%Y%m%d"), f"{latest:02d}00"

def load_weather_grids(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT grid_id, nx, ny FROM weather_grids")
        return cursor.fetchall()

def fetch_vilage_fcst(nx: int, ny: int, base_date: str, base_time: str, retries: int = 3):
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
    params = {
        "serviceKey": SERVICE_KEY,
        "numOfRows": "1000",
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, params=params, timeout=(5, 25))

            if r.status_code != 200:
                last_err = f"HTTP {r.status_code}"
                time.sleep(1.5 * attempt)
                continue

            txt = r.text.strip()
            if not txt or txt.startswith("<"):
                last_err = "EMPTY_OR_XML"
                time.sleep(1.5 * attempt)
                continue

            return r.json()

        except Exception as e:
            last_err = str(e)
            time.sleep(1.5 * attempt)

    raise RuntimeError(last_err or "UNKNOWN_ERROR")

def upsert_forecasts(conn, grid_id, nx, ny, base_date, base_time, items):
    bucket = {}
    for it in items:
        cat = it.get("category")
        if cat not in ("SKY", "PTY", "POP", "TMP"):
            continue
        key = (it["fcstDate"], it["fcstTime"])
        bucket.setdefault(key, {})[cat] = it["fcstValue"]

    with conn.cursor() as cursor:
        save_count = 0
        for (fcst_date, fcst_time), vals in bucket.items():
            sky = int(vals["SKY"]) if "SKY" in vals else None
            pty = int(vals["PTY"]) if "PTY" in vals else None
            pop = int(vals["POP"]) if "POP" in vals else None
            tmp = int(float(vals["TMP"])) if "TMP" in vals else None

            cursor.execute("""
                INSERT INTO weather_grid_forecasts
                  (grid_id, nx, ny, base_date, base_time, fcst_date, fcst_time, sky, pty, pop, tmp, collected_at)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                  sky=VALUES(sky), pty=VALUES(pty), pop=VALUES(pop), tmp=VALUES(tmp),
                  collected_at=NOW()
            """, (grid_id, nx, ny, base_date, base_time, fcst_date, fcst_time, sky, pty, pop, tmp))
            save_count += 1
        return save_count

def run():
    if not SERVICE_KEY:
        logging.error("❌ SERVICE_KEY가 비었습니다.")
        return

    conn = get_db_connection()
    try:
        grids = load_weather_grids(conn)
        now = datetime.now()
        base_date, base_time = get_latest_base_datetime(now)

        logging.info(f"🚀 대표 격자 단기예보 수집 시작 (base {base_date}/{base_time}, grids={len(grids)})")

        ok = 0
        fail = 0

        for g in grids:
            grid_id, nx, ny = g["grid_id"], g["nx"], g["ny"]
            try:
                data = fetch_vilage_fcst(nx, ny, base_date, base_time)

                items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
                if not items:
                    logging.warning(f"⚠️ {grid_id} 데이터 없음")
                    fail += 1
                    continue

                saved = upsert_forecasts(conn, grid_id, nx, ny, base_date, base_time, items)
                conn.commit()

                logging.info(f"✅ {grid_id} 저장 {saved}건")
                ok += 1

            except Exception as e:
                logging.warning(f"⚠️ {grid_id} 실패: {e}")
                fail += 1

            time.sleep(0.2)

        logging.info(f"🏁 완료: 성공 {ok}, 실패 {fail}")

    finally:
        conn.close()

if __name__ == "__main__":
    run()
