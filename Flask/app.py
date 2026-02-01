# /home/ubuntu/Senior/Code/app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
import pymysql
from datetime import datetime

app = Flask(__name__)
CORS(app)
app.json.ensure_ascii = False

def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# ✅ 1. 기존 API: 검색 및 전체 리스트용 (tags 추가됨)
@app.route('/api/tours', methods=['GET'])
def get_tours():
    target_date = request.args.get('date')
    category = request.args.get('category')
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT 
                    t.province, t.city,
                    t.title as parent_title, s.title as title,
                    s.price_text as price, t.agency, t.category, 
                    s.departure_date as date, t.phone, t.is_priority, 
                    t.location, s.booking_url, s.error_msg,
                    s.tags,
                    t.main_image_url
                FROM tours t
                JOIN tour_schedules s ON t.product_code = s.product_code
                WHERE REPLACE(s.departure_date, '-', '') >= DATE_FORMAT(CURDATE(), '%%Y%%m%%d')
            """
            params = []
            if target_date and target_date != 'null':
                sql += " AND REPLACE(s.departure_date, '-', '') = %s"
                params.append(target_date.replace('-', ''))
            
            # 카테고리 혹은 태그 기반 검색 지원 (앱 필터 연동)
            if category and category != '전체':
                sql += " AND (t.category LIKE %s OR s.tags LIKE %s)"
                params.extend([f"%{category}%", f"%{category}%"])

            sql += " ORDER BY t.is_priority DESC, s.departure_date ASC"
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            # 날짜 형식 통일 (YYYY-MM-DD)
            for row in results:
                if row['date']:
                    raw_val = str(row['date']).replace('-', '')
                    if len(raw_val) == 8:
                        row['date'] = f"{raw_val[:4]}-{raw_val[4:6]}-{raw_val[6:]}"
            
            return jsonify(results)
    finally:
        conn.close()

# ✅ 2. 지역별 그룹화 API: 메인 계층형 UX용 (tags 추가됨)
@app.route('/api/tours/grouped', methods=['GET'])
def get_grouped_tours():
    target_date = request.args.get('date')
    
    if not target_date:
        return jsonify({"error": "날짜 정보가 필요합니다."}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT 
                    t.province, t.city, t.title as parent_title, 
                    t.agency, s.price_text as price, s.booking_url, 
                    s.error_msg, t.phone,
                    s.tags,
                    t.main_image_url
                FROM tours t
                JOIN tour_schedules s ON t.product_code = s.product_code
                WHERE REPLACE(s.departure_date, '-', '') = %s
                ORDER BY t.province, t.city, t.is_priority DESC
            """
            cursor.execute(sql, (target_date.replace('-', ''),))
            rows = cursor.fetchall()

            grouped = {}
            for row in rows:
                p = row['province'] or "기타"
                c = row['city'] or "기타"
                
                if p not in grouped: grouped[p] = {}
                if c not in grouped[p]: grouped[p][c] = []
                
                grouped[p][c].append(row)
            
            return jsonify(grouped)
    finally:
        conn.close()

@app.route('/api/date', methods=['GET'])
def get_all_special_days():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # holiday_info 테이블의 모든 데이터와 감성 메시지를 합쳐서 가져옵니다.
            # 날짜(locdate) 순으로 정렬하여 앱에서 처리하기 쉽게 합니다.
            sql = """
                SELECT 
                    h.locdate, 
                    h.date_name, 
                    h.is_holiday, 
                    h.date_kind, 
                    m.message
                FROM holiday_info h
                LEFT JOIN holiday_messages m ON h.date_name = m.target_name
                ORDER BY h.locdate ASC
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            # 데이터가 없을 경우 빈 리스트 반환
            if not results:
                return jsonify([])
            
            return jsonify(results)
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/promotions', methods=['GET'])
def get_promotions():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # ✅ 수정된 SQL: priority 숫자가 낮은 순(ASC)으로 정렬합니다.
            sql = """
                SELECT 
                    icon, 
                    title, 
                    description, 
                    target_url 
                FROM promotions 
                WHERE is_active = 1 
                ORDER BY priority ASC, id DESC
            """
            # (설명: 우선순위(priority)가 같으면 최신순(id DESC)으로 보여줍니다.)
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            if not results:
                return jsonify([])
                
            return jsonify(results)
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ✅ 3. 테마 2: 내 주변 나들이 장소 API (거리순 정렬 + 페이징)
@app.route('/api/spots/nearby', methods=['GET'])
def get_nearby_spots():
    lat = request.args.get('lat')   # 사용자 현재 위도
    lng = request.args.get('lng')   # 사용자 현재 경도
    radius = request.args.get('radius', 20) # 검색 반경 (기본 20km)
    category = request.args.get('category') # 12, 14, 15 등 필터
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))

    if not lat or not lng:
        return jsonify({"error": "위치 정보(lat, lng)가 필요합니다."}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 하버사인(Haversine) 공식을 이용한 MySQL 거리 계산 쿼리
            # 6371은 지구의 반지름(km)입니다.
            sql = """
                SELECT *, (
                    6371 * acos(
                        cos(radians(%s)) * cos(radians(mapy)) 
                        * cos(radians(mapx) - radians(%s)) 
                        + sin(radians(%s)) * sin(radians(mapy))
                    )
                ) AS distance 
                FROM picnic_spots
            """
            params = [lat, lng, lat]

            # 필터 조건 추가
            where_clauses = []
            if category:
                where_clauses.append("contenttypeid = %s")
                params.append(category)
            
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            # 반경 제한 및 정렬
            sql += " HAVING distance <= %s ORDER BY distance ASC LIMIT %s OFFSET %s"
            params.extend([float(radius), limit, offset])

            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ✅ 4. 장소 상세 정보 API (contentid 기반)
@app.route('/api/spots/<contentid>', methods=['GET'])
def get_spot_detail(contentid):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT * FROM picnic_spots WHERE contentid = %s"
            cursor.execute(sql, (contentid,))
            result = cursor.fetchone()
            
            if not result:
                return jsonify({"error": "해당 장소를 찾을 수 없습니다."}), 404
                
            return jsonify(result)
    finally:
        conn.close()



# ✅ 헬스 체크용 API: 로드 밸런서 상태 확인용
@app.route('/health', methods=['GET'])
def health_check():
    # 서버가 정상 작동 중임을 알리는 가장 가벼운 응답
    return jsonify({"status": "working", "timestamp": datetime.now().isoformat()}), 200

if __name__ == '__main__':
    # 서버 재시작 시 포트 충돌이 나면 sudo fuser -k 5000/tcp 명령어를 잊지 마세요!
    app.run(host='0.0.0.0', port=5000, debug=True)