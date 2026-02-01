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

@app.route('/api/tours', methods=['GET'])
def get_tours():
    target_date = request.args.get('date')
    category = request.args.get('category')
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # ✅ t.title은 'parent_title'로, s.title은 'title'로 가져옵니다.
            # 앱 코드 수정을 최소화하기 위해 s.title을 기본 'title'로 보냅니다.
            sql = """
                SELECT 
                    t.title as parent_title, 
                    s.title as title,
                    s.price_text as price, 
                    t.agency, 
                    t.category, 
                    s.departure_date as date, 
                    t.phone, 
                    t.is_priority, 
                    t.location, 
                    s.booking_url,
                    s.error_msg
                FROM tours t
                JOIN tour_schedules s ON t.product_code = s.product_code
                WHERE s.departure_date >= CURDATE()
                    AND (s.error_msg IS NULL OR s.error_msg = '대기예약' OR s.error_msg = '예약마감')           
            """
            params = []

            # 필터 조건 추가 (날짜)
            if target_date and target_date != 'null':
                sql += " AND s.departure_date = %s"
                params.append(target_date)
            
            # 필터 조건 추가 (카테고리)
            if category and category != '전체':
                sql += " AND t.category LIKE %s"
                params.append(f"%{category}%")

            # 정렬 (우선순위 높은 순, 날짜 빠른 순)
            sql += " ORDER BY t.is_priority DESC, s.departure_date ASC"
            
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            # ✅ 데이터 가공 루프
            for row in results:
                # 1. 날짜 형식 변환
                if row['date'] and hasattr(row['date'], 'strftime'):
                    row['date'] = row['date'].strftime('%Y-%m-%d')
                            
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# /home/ubuntu/Senior/Code/app.py 수정

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

if __name__ == '__main__':
    # 서버 재시작 시 포트 충돌이 나면 sudo fuser -k 5000/tcp 명령어를 잊지 마세요!
    app.run(host='0.0.0.0', port=5000, debug=True)