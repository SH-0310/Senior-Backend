# /home/ubuntu/Senior/Code/app.py
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import pymysql
import json
from datetime import datetime, date
from decimal import Decimal
import re
from werkzeug.middleware.proxy_fix import ProxyFix
import logging

app = Flask(__name__)
CORS(app)
app.json.ensure_ascii = False

# --- [추가된 부분] 로드밸런서 및 실제 IP 로깅 설정 ---

# 1. 로드밸런서의 X-Forwarded-For 헤더 신뢰 설정 (LB 1대 기준)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)

# 2. 로그 설정 (에러 방지를 위해 표준 포맷 사용)
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
)

# 3. 모든 요청마다 실제 IP를 로그로 남김
@app.before_request
def log_request_info():
    # ProxyFix 덕분에 request.remote_addr에 실제 사용자 IP가 담깁니다.
    client_ip = request.remote_addr
    method = request.method
    path = request.path
    app.logger.info(f"CONNECTED IP: {client_ip} - {method} {path}")

# --- 설정 끝 ---

# --- 공통 도우미 함수 ---
def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def clean_html(text):
    """HTML 태그 제거 및 줄바꿈 변환"""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


# --- API 경로 시작 ---

# ✅ 1. 검색 및 전체 리스트용 (에러 핸들링 추가)
@app.route('/api/tours', methods=['GET'])
def get_tours():
    target_date = request.args.get('date')
    category = request.args.get('category')
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT t.province, t.city, t.title as parent_title, s.title as title,
                       s.price_text as price, t.agency, t.category, s.departure_date as date, 
                       t.phone, t.is_priority, t.location, s.booking_url, s.error_msg,
                       s.tags, t.main_image_url
                FROM tours t
                JOIN tour_schedules s ON t.product_code = s.product_code
                WHERE REPLACE(s.departure_date, '-', '') >= DATE_FORMAT(CURDATE(), '%%Y%%m%%d')
            """
            params = []
            if target_date and target_date != 'null':
                sql += " AND REPLACE(s.departure_date, '-', '') = %s"
                params.append(target_date.replace('-', ''))
            
            if category and category != '전체':
                sql += " AND (t.category LIKE %s OR s.tags LIKE %s)"
                params.extend([f"%{category}%", f"%{category}%"])

            sql += " ORDER BY t.is_priority DESC, s.departure_date ASC"
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            for row in results:
                row['title'] = clean_html(row['title'])
                row['parent_title'] = clean_html(row['parent_title'])
                row['tags'] = clean_html(row['tags'])
                if row.get('date'):
                    raw_val = str(row['date']).replace('-', '')
                    if len(raw_val) == 8:
                        row['date'] = f"{raw_val[:4]}-{raw_val[4:6]}-{raw_val[6:]}"
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ✅ 2. 지역별 그룹화 API (에러 핸들링 추가)
@app.route('/api/tours/grouped', methods=['GET'])
def get_grouped_tours():
    target_date = request.args.get('date')
    if not target_date:
        return jsonify({"error": "날짜 정보가 필요합니다."}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT t.province, t.city, t.title as parent_title, t.agency, 
                       s.price_text as price, s.booking_url, s.error_msg, 
                       t.phone, s.tags, t.main_image_url
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
                row['parent_title'] = clean_html(row['parent_title'])
                row['tags'] = clean_html(row['tags'])
                if p not in grouped: grouped[p] = {}
                if c not in grouped[p]: grouped[p][c] = []
                grouped[p][c].append(row)
            return jsonify(grouped)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ✅ 3. 공휴일 API (기존 유지)
@app.route('/api/date', methods=['GET'])
def get_all_special_days():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT h.locdate, h.date_name, h.is_holiday, h.date_kind, m.message FROM holiday_info h LEFT JOIN holiday_messages m ON h.date_name = m.target_name ORDER BY h.locdate ASC"
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results: row['message'] = clean_html(row['message'])
            return jsonify(results)
    except Exception as e: return jsonify({"error": str(e)}), 500
    finally: conn.close()

# ✅ 4. 프로모션 API (기존 유지)
@app.route('/api/promotions', methods=['GET'])
def get_promotions():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT icon, title, description, target_url FROM promotions WHERE is_active = 1 ORDER BY priority ASC, id DESC"
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results: row['description'] = clean_html(row['description'])
            return jsonify(results)
    except Exception as e: return jsonify({"error": str(e)}), 500
    finally: conn.close()

# ✅ 내 주변 소풍지 API (페이지네이션 유지 + 일일 고정 랜덤 추가)
@app.route('/api/spots/nearby', methods=['GET'])
def get_nearby_spots():
    lat = request.args.get('lat', default=37.5665, type=float)
    lng = request.args.get('lng', default=126.9780, type=float)
    min_dist = request.args.get('min_radius', default=0.0, type=float)
    max_dist = request.args.get('max_radius', default=10.0, type=float)
    
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    
    # ✅ 매일 다른 랜덤 순서를 위해 오늘 날짜를 Seed로 사용 (예: 20260212)
    # 이렇게 하면 사용자가 페이지를 넘겨도(Offset) 하루 동안은 순서가 유지됩니다.
    seed = datetime.now().strftime('%Y%m%d')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # ✅ 정렬 로직 설명:
            # 1. 2km 단위(FLOOR(distance / 2))로 그룹화하여 가까운 곳을 우선순위로 둡니다.
            # 2. 같은 2km 반경 내에서는 RAND(seed)를 사용하여 매일 다른 순서로 섞어줍니다.
            sql = """
                SELECT P.*, C.overview, 
                       (6371 * acos(cos(radians(%s)) * cos(radians(P.mapy)) * cos(radians(P.mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(P.mapy)))) AS distance 
                FROM picnic_spots P 
                JOIN spot_commons C ON CAST(P.contentid AS CHAR) = CAST(C.contentid AS CHAR)
                WHERE P.firstimage IS NOT NULL AND P.firstimage != '' 
                  AND C.overview IS NOT NULL AND C.overview != ''
                HAVING distance > %s AND distance <= %s 
                ORDER BY FLOOR(distance / 2) ASC, RAND(%s) 
                LIMIT %s OFFSET %s
            """
            
            cursor.execute(sql, [lat, lng, lat, min_dist, max_dist, seed, limit, offset])
            results = cursor.fetchall()
            
            for row in results: 
                row['overview'] = clean_html(row['overview'])
                row['distance'] = round(row['distance'], 1) # 거리값 반올림 추가
                
            return jsonify(results)
    except Exception as e: 
        return jsonify({"error": str(e)}), 500
    finally: 
        conn.close()

# ✅ 6. 통합 검색 API (띄어쓰기 무시 및 중복 보충형)
@app.route('/api/search/global', methods=['GET'])
def global_search():
    # 1. 검색어 전처리: 앞뒤 공백 제거 및 모든 공백 제거 버전 생성
    query = request.args.get('q', '').strip()
    lat = float(request.args.get('lat', 37.5665))
    lng = float(request.args.get('lng', 126.9780))
    
    if not query: 
        return jsonify({"packages": [], "spots_by_title": [], "spots_by_addr": []})

    # 공백을 제거한 검색어 (예: "대관령 양떼" -> "대관령양떼")
    clean_query = query.replace(" ", "")
    search_param = f"%{clean_query}%"

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 🚀 [1] 여행 패키지 검색: 제목과 카테고리에서 공백 제거 후 비교
            sql_tours = """
                SELECT ANY_VALUE(s.title) as title, MIN(s.departure_date) as date, ANY_VALUE(s.price_text) as price, ANY_VALUE(s.booking_url) as booking_url,
                       ANY_VALUE(t.province) as province, ANY_VALUE(t.city) as city, ANY_VALUE(t.agency) as agency, ANY_VALUE(s.tags) as tags, ANY_VALUE(t.main_image_url) as main_image_url
                FROM tour_schedules s JOIN tours t ON s.product_code = t.product_code
                WHERE (REPLACE(s.title, ' ', '') LIKE %s OR REPLACE(t.category, ' ', '') LIKE %s) 
                  AND REPLACE(s.departure_date, '-', '') >= DATE_FORMAT(CURDATE(), '%%Y%%m%%d')
                GROUP BY s.product_code ORDER BY date ASC LIMIT 5
            """
            cursor.execute(sql_tours, (search_param, search_param))
            packages = cursor.fetchall()
            for p in packages:
                p['title'] = clean_html(p['title'])
                p['tags'] = clean_html(p['tags'])

            # 🚀 [2] 소풍지 (A): 이름 기반 검색 (공백 제거 적용)
            sql_spots_title = """
                SELECT *, (6371 * acos(cos(radians(%s)) * cos(radians(mapy)) * cos(radians(mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(mapy)))) AS distance 
                FROM picnic_spots 
                WHERE REPLACE(title, ' ', '') LIKE %s 
                  AND contenttypeid != 15
                ORDER BY (firstimage IS NOT NULL AND firstimage != '') DESC, RAND() 
                LIMIT 8
            """
            cursor.execute(sql_spots_title, (lat, lng, lat, search_param))
            spots_title = cursor.fetchall()
            
            title_ids = [s['contentid'] for s in spots_title]

            # 🚀 [3] 소풍지 (B): 장소(주소) 기반 검색 (공백 제거 및 중복 제외)
            if title_ids:
                format_strings = ','.join(['%s'] * len(title_ids))
                sql_spots_addr = f"""
                    SELECT *, (6371 * acos(cos(radians(%s)) * cos(radians(mapy)) * cos(radians(mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(mapy)))) AS distance 
                    FROM picnic_spots 
                    WHERE REPLACE(addr1, ' ', '') LIKE %s 
                      AND contenttypeid != 15
                      AND contentid NOT IN ({format_strings})
                    ORDER BY (firstimage IS NOT NULL AND firstimage != '') DESC, RAND() 
                    LIMIT 8
                """
                params = [lat, lng, lat, search_param] + title_ids
                cursor.execute(sql_spots_addr, params)
            else:
                sql_spots_addr = """
                    SELECT *, (6371 * acos(cos(radians(%s)) * cos(radians(mapy)) * cos(radians(mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(mapy)))) AS distance 
                    FROM picnic_spots 
                    WHERE REPLACE(addr1, ' ', '') LIKE %s 
                      AND contenttypeid != 15
                    ORDER BY (firstimage IS NOT NULL AND firstimage != '') DESC, RAND() 
                    LIMIT 8
                """
                cursor.execute(sql_spots_addr, (lat, lng, lat, search_param))
            
            spots_addr = cursor.fetchall()
            for s in spots_addr: 
                s['title'] = clean_html(s['title'])

            return jsonify({
                "packages": packages,
                "spots_by_title": spots_title,
                "spots_by_addr": spots_addr
            })

    except Exception as e:
        print(f"🚨 통합 검색 에러: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/spots/<int:contentid>', methods=['GET'])
def get_spot_detail(contentid):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # ✅ C.homepage 대신 C.homepage_url을 조회하도록 변경
            sql_main = """
                SELECT P.contentid, P.title, P.addr1, P.addr2, P.mapx, P.mapy, P.firstimage, P.tel, 
                       C.overview, C.homepage_url, 
                       D.parking, D.restdate, D.usetime, D.infocenter, 
                       D.chkbabycarriage, D.chkpet, D.chkcreditcard, D.usefee, D.expagerange
                FROM picnic_spots P 
                LEFT JOIN spot_commons C ON CAST(P.contentid AS CHAR) = CAST(C.contentid AS CHAR) 
                LEFT JOIN spot_details D ON CAST(P.contentid AS CHAR) = CAST(D.contentid AS CHAR)
                WHERE P.contentid = %s
            """
            cursor.execute(sql_main, (contentid,))
            m = cursor.fetchone()
            if not m: return jsonify({"error": "데이터 없음"}), 404

            # 추가 정보(spot_info) 조회
            sql_sub = "SELECT infoname, infotext FROM spot_info WHERE CAST(contentid AS CHAR) = %s ORDER BY serialnum ASC"
            cursor.execute(sql_sub, (str(contentid),))
            sub_info = cursor.fetchall()

            result = {
                "basic": {
                    "title": clean_html(m.get('title')),
                    "address": f"{m.get('addr1', '')} {m.get('addr2', '')}".strip(),
                    "lat": m.get('mapy'),
                    "lng": m.get('mapx'),
                    "image": m.get('firstimage', ''),
                    "tel": clean_html(m.get('tel', '') or m.get('infocenter', '')),
                    "overview": clean_html(m.get('overview')) or "설명 준비 중",
                    # ✅ 정제된 컬럼값을 그대로 사용 (extract_url 호출 불필요)
                    "homepage": m.get('homepage_url') or ""
                },
                "facility": {
                    "parking": clean_html(m.get('parking')) or "정보 없음",
                    "restdate": clean_html(m.get('restdate')) or "정보 없음",
                    "usetime": clean_html(m.get('usetime')) or "상시 개방",
                    "baby_carriage": clean_html(m.get('chkbabycarriage')) or "확인 필요",
                    "pet": clean_html(m.get('chkpet')) or "정보 없음",
                    "credit_card": clean_html(m.get('chkcreditcard')) or "정보 없음",
                    "fee": clean_html(m.get('usefee')) or "무료 또는 정보 없음",
                    "age_range": clean_html(m.get('expagerange')) or "전연령 가능"
                },
                "extra_details": [{"infoname": i['infoname'], "infotext": clean_html(i['infotext'])} for i in sub_info]
            }
            return jsonify(result)
    except Exception as e: return jsonify({"error": str(e)}), 500
    finally: conn.close()


# ✅ 8. 축제 정보 API (기존 유지)
@app.route('/api/festivals', methods=['GET'])
def get_festivals():
    area_code = request.args.get('areaCode')
    today = datetime.now().strftime('%Y%m%d')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT F.*, C.overview FROM festivals F LEFT JOIN spot_commons C ON F.contentid = C.contentid WHERE F.eventenddate >= %s"
            params = [today]
            if area_code: sql += " AND F.areacode = %s"; params.append(area_code)
            sql += " ORDER BY F.eventstartdate ASC"
            cursor.execute(sql, params)
            results = cursor.fetchall()
            for row in results:
                row['overview'] = clean_html(row['overview'])
                start = row['eventstartdate']
                row['status'] = "진행 중" if start <= today else f"예정 ({start[4:6]}/{start[6:8]} 시작)"
            return jsonify(results)
    except Exception as e: return jsonify({"error": str(e)}), 500
    finally: conn.close()

# ✅ 12. 서울시 문화행사 전체 리스트 API
@app.route('/api/festivals/seoul', methods=['GET'])
def get_seoul_festivals():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 1. 모든 컬럼 선택 (updated_at 제외 또는 포함 선택 가능)
            sql = """
                SELECT 
                    cult_code, title, codename, guname, date_range, place, 
                    org_name, use_target, use_fee, inquiry, player, program, 
                    etc_desc, is_free, main_img, hmpg_url, org_link, lat, lng, 
                    start_date, end_date, rgstdate, ticket, themecode, pro_time
                FROM seoul_events
                WHERE end_date >= CURDATE()
                ORDER BY start_date ASC
            """
            cursor.execute(sql)
            results = cursor.fetchall()

            # 2. 데이터 가공 (날짜 변환 및 HTML 태그 제거)
            for row in results:
                # 텍스트 필드 HTML 클리닝
                text_fields = ['title', 'place', 'org_name', 'use_target', 'use_fee', 
                               'inquiry', 'player', 'program', 'etc_desc', 'pro_time']
                for field in text_fields:
                    if row.get(field):
                        row[field] = clean_html(row[field])
                
                # 날짜 객체 처리 (JSON 에러 방지)
                date_fields = ['start_date', 'end_date', 'rgstdate']
                for d_field in date_fields:
                    if isinstance(row.get(d_field), (date, datetime)):
                        row[d_field] = row[d_field].strftime('%Y-%m-%d')

            return jsonify(results)
            
    except Exception as e:
        app.logger.error(f"🚨 서울 문화행사 API 에러: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/config/splash', methods=['GET'])
def get_splash_config():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 활성화된 스플래시 정보를 우선순위 순으로 가져옴
            sql = """
                SELECT image_url, message 
                FROM splash_screens 
                WHERE is_active = TRUE 
                ORDER BY priority DESC, id DESC
            """
            cursor.execute(sql)
            results = cursor.fetchall()
            
            # 문구 HTML 태그 제거
            for row in results:
                row['message'] = clean_html(row['message'])
                
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ✅ 10. AI 풍경 컨텐츠 API (리스트 및 상세 조회 통합 - 최종 업그레이드)
@app.route('/api/ai-landscapes', methods=['GET'])
def get_ai_landscapes():
    content_id = request.args.get('id')
    limit = request.args.get('limit', default=10, type=int)

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if content_id:
                # 1. 상세 조회: SELECT * 이므로 thumbnail_text가 자동으로 포함됩니다.
                sql_content = "SELECT * FROM ai_landscapes WHERE id = %s"
                cursor.execute(sql_content, (content_id,))
                result = cursor.fetchone()
                
                if result:
                    # 2. 이미지 리스트 가져오기
                    sql_images = "SELECT image_url FROM ai_landscape_images WHERE landscape_id = %s ORDER BY priority ASC"
                    cursor.execute(sql_images, (content_id,))
                    images_rows = cursor.fetchall()
                    image_urls = [row['image_url'] for row in images_rows]

                    # 3. 치환자 기반 블록 가공 로직
                    raw_content = clean_html(result['content'])
                    parts = re.split(r'(\[\[IMG_\d+\]\])', raw_content)
                    
                    content_blocks = []
                    for part in parts:
                        part = part.strip()
                        if not part: continue
                        
                        img_match = re.match(r'\[\[IMG_(\d+)\]\]', part)
                        if img_match:
                            img_idx = int(img_match.group(1)) - 1
                            if img_idx < len(image_urls):
                                content_blocks.append({
                                    "type": "image",
                                    "value": image_urls[img_idx]
                                })
                        else:
                            content_blocks.append({
                                "type": "text",
                                "value": part
                            })

                    result['blocks'] = content_blocks
                    result['images'] = image_urls
                    return jsonify(result)
            else:
                # 4. 리스트 조회: thumbnail_text 컬럼을 추가했습니다! 🚀
                sql = """
                    SELECT id, title, thumbnail_text, thumbnail_url, detail_image_url, 
                           card_description, category, author 
                    FROM ai_landscapes 
                    ORDER BY created_at DESC 
                    LIMIT %s
                """
                cursor.execute(sql, (limit,))
                results = cursor.fetchall()
                
                for row in results:
                    row['card_description'] = clean_html(row['card_description'])
                
                return jsonify(results)
                
    except Exception as e:
        print(f"🚨 API 에러: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ✅ 11. 전체 지역 날씨 예보 API (전체 데이터 출력)
@app.route('/api/weather/all', methods=['GET'])
def get_all_weather():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 모든 지역의 오늘 이후 데이터를 날짜/오전오후 순으로 정렬
            sql = """
                SELECT location, forecast_date, ampm, weather_status
                FROM weather_forecasts
                WHERE forecast_date >= CURDATE()
                ORDER BY location ASC, forecast_date ASC, ampm ASC
            """
            cursor.execute(sql)
            results = cursor.fetchall()

            # 날짜 객체를 JSON 전송이 가능한 문자열로 변환
            for row in results:
                if isinstance(row['forecast_date'], (date, datetime)):
                    row['forecast_date'] = row['forecast_date'].strftime('%Y-%m-%d')

            return jsonify(results)
            
    except Exception as e:
        app.logger.error(f"🚨 전체 날씨 API 에러: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# ✅ 헬스 체크
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "working", "timestamp": datetime.now().isoformat()}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)