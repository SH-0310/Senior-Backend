import pymysql
import re

# (extract_clean_url 함수는 이전과 동일)
def extract_clean_url(html):
    if not html or html.strip() == "":
        return ""
    match = re.search(r'href=["\'](https?://[^"\']+)["\']', html)
    if match: return match.group(1).strip()
    match = re.search(r'href=["\']([^"\']+\.[^"\']+)["\']', html)
    if match:
        url = match.group(1).strip()
        return url if url.startswith('http') else f"http://{url}"
    match = re.search(r'(https?://[^\s<>]+|www\.[^\s<>]+)', html)
    if match:
        url = match.group(1).strip()
        return url if url.startswith('http') else f"http://{url}"
    return ""

def update_commons_homepage():
    conn = pymysql.connect(
        host='localhost', user='shmoon', password='Tjdgursla87!',
        db='senior_travel', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with conn.cursor() as cursor:
            # ✅ 1. 컬럼 타입 변경: VARCHAR(500) -> TEXT (용량 문제 해결)
            # 이미 컬럼이 있다면 타입을 TEXT로 확장합니다.
            try:
                cursor.execute("ALTER TABLE spot_commons MODIFY COLUMN homepage_url TEXT")
                conn.commit()
                print("✅ 'homepage_url' 컬럼 타입을 TEXT로 확장했습니다.")
            except Exception:
                # 컬럼이 아예 없다면 새로 생성 (TEXT 타입으로)
                cursor.execute("ALTER TABLE spot_commons ADD COLUMN homepage_url TEXT")
                conn.commit()
                print("✅ 'homepage_url' 컬럼을 TEXT 타입으로 생성했습니다.")

            # 2. 데이터 조회
            cursor.execute("SELECT contentid, homepage FROM spot_commons WHERE homepage IS NOT NULL AND homepage != ''")
            rows = cursor.fetchall()

            print(f"🔄 총 {len(rows)}개의 주소를 처리합니다...")
            
            update_data = []
            for row in rows:
                clean_url = extract_clean_url(row['homepage'])
                if clean_url:
                    update_data.append((clean_url, row['contentid']))

            # 3. 일괄 업데이트
            if update_data:
                update_sql = "UPDATE spot_commons SET homepage_url = %s WHERE contentid = %s"
                cursor.executemany(update_sql, update_data)
                conn.commit()
                print(f"🏁 작업 완료! 총 {len(update_data)}개의 레코드를 업데이트했습니다.")

    except Exception as e:
        print(f"🚨 에러 발생: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_commons_homepage()