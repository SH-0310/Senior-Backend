import pymysql


# ==========================================
# DB 연결 함수 (이미 있는 것 그대로 사용)
# ==========================================
def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


# ==========================================
# spot_rank_tags 동기화 함수
# ==========================================
def sync_spot_rank_tags():
    print("======================================")
    print("📊 spot_rank_tags 동기화 시작")

    conn = None

    try:
        conn = get_db_connection()

        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT IGNORE INTO spot_rank_tags (contentid)
                SELECT contentid FROM picnic_spots
            """)

            inserted = cursor.rowcount

        conn.commit()

        print(f"✅ 신규 {inserted}건 추가 완료")
        print("📊 spot_rank_tags 동기화 완료")

    except Exception as e:
        print("❌ spot_rank_tags 동기화 실패")
        print("에러:", e)

    finally:
        if conn:
            conn.close()

    print("======================================")


# ==========================================
# 기존 classifier 작업 끝난 뒤 호출
# ==========================================
if __name__ == "__main__":
    # 여기 위에 기존 classifier 로직 실행됨

    sync_spot_rank_tags()
