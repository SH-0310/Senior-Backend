# /home/ubuntu/Senior/Code/app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
import pymysql
from datetime import datetime
import re

app = Flask(__name__)
CORS(app)
app.json.ensure_ascii = False

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

# ✅ 5. 내 주변 소풍지 API (기존 유지)
@app.route('/api/spots/nearby', methods=['GET'])
def get_nearby_spots():
    lat = request.args.get('lat', default=37.5665, type=float)
    lng = request.args.get('lng', default=126.9780, type=float)
    min_dist = request.args.get('min_radius', default=0.0, type=float)
    max_dist = request.args.get('max_radius', default=10.0, type=float)
    limit = int(request.args.get('limit', 20))
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT P.*, C.overview, (6371 * acos(cos(radians(%s)) * cos(radians(P.mapy)) * cos(radians(P.mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(P.mapy)))) AS distance 
                FROM picnic_spots P JOIN spot_commons C ON CAST(P.contentid AS CHAR) = CAST(C.contentid AS CHAR)
                WHERE P.firstimage IS NOT NULL AND P.firstimage != '' AND C.overview IS NOT NULL AND C.overview != ''
                HAVING distance > %s AND distance <= %s ORDER BY distance ASC LIMIT %s
            """
            cursor.execute(sql, [lat, lng, lat, min_dist, max_dist, limit])
            results = cursor.fetchall()
            for row in results: row['overview'] = clean_html(row['overview'])
            return jsonify(results)
    except Exception as e: return jsonify({"error": "오류 발생"}), 500
    finally: conn.close()

# ✅ 6. 통합 검색 API (이름 우선순위 및 장소 기반 중복 보충형)
@app.route('/api/search/global', methods=['GET'])
def global_search():
    query = request.args.get('q', '')
    lat = float(request.args.get('lat', 37.5665))
    lng = float(request.args.get('lng', 126.9780))
    
    if not query: 
        return jsonify({"packages": [], "spots_by_title": [], "spots_by_addr": []})

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 1. 여행 패키지 검색 (기존 유지)
            sql_tours = """
                SELECT ANY_VALUE(s.title) as title, MIN(s.departure_date) as date, ANY_VALUE(s.price_text) as price, ANY_VALUE(s.booking_url) as booking_url,
                       ANY_VALUE(t.province) as province, ANY_VALUE(t.city) as city, ANY_VALUE(t.agency) as agency, ANY_VALUE(s.tags) as tags, ANY_VALUE(t.main_image_url) as main_image_url
                FROM tour_schedules s JOIN tours t ON s.product_code = t.product_code
                WHERE (s.title LIKE %s OR t.category LIKE %s) AND REPLACE(s.departure_date, '-', '') >= DATE_FORMAT(CURDATE(), '%%Y%%m%%d')
                GROUP BY s.product_code ORDER BY date ASC LIMIT 5
            """
            cursor.execute(sql_tours, (f"%{query}%", f"%{query}%"))
            packages = cursor.fetchall()
            for p in packages:
                p['title'] = clean_html(p['title'])
                p['tags'] = clean_html(p['tags'])

            # ✅ 2. 소풍지 (A): 이름 기반 검색 (축제 제외: contentTypeId != 15)
            sql_spots_title = """
                SELECT *, (6371 * acos(cos(radians(%s)) * cos(radians(mapy)) * cos(radians(mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(mapy)))) AS distance 
                FROM picnic_spots 
                WHERE title LIKE %s 
                  AND contenttypeid != 15  -- ⬅️ 축제/행사 데이터 제외
                ORDER BY (firstimage IS NOT NULL AND firstimage != '') DESC, RAND() 
                LIMIT 8
            """
            cursor.execute(sql_spots_title, (lat, lng, lat, f"%{query}%"))
            spots_title = cursor.fetchall()
            
            title_ids = [s['contentid'] for s in spots_title]

            # ✅ 3. 소풍지 (B): 장소 기반 검색 (중복 및 축제 제외)
            if title_ids:
                format_strings = ','.join(['%s'] * len(title_ids))
                sql_spots_addr = f"""
                    SELECT *, (6371 * acos(cos(radians(%s)) * cos(radians(mapy)) * cos(radians(mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(mapy)))) AS distance 
                    FROM picnic_spots 
                    WHERE addr1 LIKE %s 
                      AND contenttypeid != 15
                      AND contentid NOT IN ({format_strings})
                    ORDER BY (firstimage IS NOT NULL AND firstimage != '') DESC, RAND() 
                    LIMIT 8
                """
                params = [lat, lng, lat, f"%{query}%"] + title_ids
                cursor.execute(sql_spots_addr, params)
            else:
                sql_spots_addr = """
                    SELECT *, (6371 * acos(cos(radians(%s)) * cos(radians(mapy)) * cos(radians(mapx) - radians(%s)) + sin(radians(%s)) * sin(radians(mapy)))) AS distance 
                    FROM picnic_spots 
                    WHERE addr1 LIKE %s 
                      AND contenttypeid != 15
                    ORDER BY (firstimage IS NOT NULL AND firstimage != '') DESC, RAND() 
                    LIMIT 8
                """
                cursor.execute(sql_spots_addr, (lat, lng, lat, f"%{query}%"))
            
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

# ✅ 7. 장소 상세 API (기존 유지)
@app.route('/api/spots/<int:contentid>', methods=['GET'])
def get_spot_detail(contentid):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql_main = """
                SELECT P.contentid, P.title, P.addr1, P.addr2, P.mapx, P.mapy, P.firstimage, P.tel, C.overview, D.parking, D.restdate, D.usetime, D.chkbabycarriage
                FROM picnic_spots P LEFT JOIN spot_commons C ON CAST(P.contentid AS CHAR) = CAST(C.contentid AS CHAR) LEFT JOIN spot_details D ON CAST(P.contentid AS CHAR) = CAST(D.contentid AS CHAR)
                WHERE P.contentid = %s
            """
            cursor.execute(sql_main, (contentid,))
            m = cursor.fetchone()
            if not m: return jsonify({"error": "데이터 없음"}), 404
            sql_sub = "SELECT infoname, infotext FROM spot_info WHERE CAST(contentid AS CHAR) = %s ORDER BY serialnum ASC"
            cursor.execute(sql_sub, (str(contentid),))
            sub_info = cursor.fetchall()
            result = {
                "basic": {"title": clean_html(m.get('title')), "address": f"{m.get('addr1', '')} {m.get('addr2', '')}".strip(), "lat": m.get('mapy'), "lng": m.get('mapx'), "image": m.get('firstimage', ''), "tel": clean_html(m.get('tel', '')), "overview": clean_html(m.get('overview')) or "설명 준비 중"},
                "facility": {"parking": clean_html(m.get('parking')) or "정보 없음", "restdate": clean_html(m.get('restdate')) or "정보 없음", "usetime": clean_html(m.get('usetime')) or "상시 개방", "wheelchair": clean_html(m.get('chkbabycarriage')) or "확인 필요"},
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

# ✅ 9. 지역별 랜덤 API (기존 유지)
@app.route('/api/spots/region/random', methods=['GET'])
def get_random_spots_by_region():
    region = request.args.get('query')
    if not region: return jsonify({"error": "지역 정보 필요"}), 400
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT P.contentid, P.title, P.addr1, P.firstimage, P.mapx, P.mapy, C.overview FROM picnic_spots P LEFT JOIN spot_commons C ON CAST(P.contentid AS CHAR) = CAST(C.contentid AS CHAR) WHERE P.addr1 LIKE %s AND P.firstimage != '' ORDER BY RAND() LIMIT 20"
            cursor.execute(sql, (f"{region}%",))
            results = cursor.fetchall()
            if len(results) < 5:
                sql_fb = "SELECT P.contentid, P.title, P.addr1, P.firstimage, P.mapx, P.mapy, C.overview FROM picnic_spots P LEFT JOIN spot_commons C ON CAST(P.contentid AS CHAR) = CAST(C.contentid AS CHAR) WHERE P.addr1 LIKE %s ORDER BY RAND() LIMIT 20"
                cursor.execute(sql_fb, (f"{region}%",))
                results = cursor.fetchall()
            for row in results:
                row['overview'] = clean_html(row['overview'])
                row['title'] = clean_html(row['title'])
            return jsonify(results)
    except Exception as e: return jsonify({"error": str(e)}), 500
    finally: conn.close()

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

# ✅ 10. AI 풍경 컨텐츠 API (리스트 및 상세 조회 통합 - 업그레이드 버전)
@app.route('/api/ai-landscapes', methods=['GET'])
def get_ai_landscapes():
    content_id = request.args.get('id')
    limit = request.args.get('limit', default=10, type=int)

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if content_id:
                # 1. 글 정보 가져오기
                sql_content = "SELECT * FROM ai_landscapes WHERE id = %s"
                cursor.execute(sql_content, (content_id,))
                result = cursor.fetchone()
                
                if result:
                    # 2. 이미지 리스트 가져오기
                    sql_images = "SELECT image_url FROM ai_landscape_images WHERE landscape_id = %s ORDER BY priority ASC"
                    cursor.execute(sql_images, (content_id,))
                    images_rows = cursor.fetchall()
                    image_urls = [row['image_url'] for row in images_rows]

                    # 3. [핵심 업그레이드] 치환자 기반 블록 가공 로직 🚀
                    raw_content = clean_html(result['content'])
                    
                    # [[IMG_N]]을 기준으로 텍스트를 쪼갭니다.
                    # 예: ["도입부", "[[IMG_1]]", "중간글", "[[IMG_2]]", "마무리"]
                    parts = re.split(r'(\[\[IMG_\d+\]\])', raw_content)
                    
                    content_blocks = []
                    for part in parts:
                        part = part.strip()
                        if not part: continue
                        
                        # 만약 쪼개진 조각이 [[IMG_n]] 형태라면 사진 블록 추가
                        img_match = re.match(r'\[\[IMG_(\d+)\]\]', part)
                        if img_match:
                            img_idx = int(img_match.group(1)) - 1 # [[IMG_1]]은 0번 인덱스
                            if img_idx < len(image_urls):
                                content_blocks.append({
                                    "type": "image",
                                    "value": image_urls[img_idx]
                                })
                        else:
                            # 그게 아니라면 텍스트 블록 추가
                            content_blocks.append({
                                "type": "text",
                                "value": part
                            })

                    # 최종 결과 구성
                    result['content'] = raw_content # 원본도 혹시 모르니 유지
                    result['blocks'] = content_blocks # 앱에서 바로 쓸 "황금 리스트"
                    result['images'] = image_urls
                    return jsonify(result)
            else:
                # 리스트 조회 로직 (기존과 동일)
                sql = "SELECT id, title, thumbnail_url, detail_image_url, card_description, category, author FROM ai_landscapes ORDER BY created_at DESC LIMIT %s"
                cursor.execute(sql, (limit,))
                results = cursor.fetchall()
                for row in results:
                    row['card_description'] = clean_html(row['card_description'])
                return jsonify(results)
                
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ✅ 헬스 체크
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "working", "timestamp": datetime.now().isoformat()}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)