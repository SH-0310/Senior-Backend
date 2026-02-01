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
            # ✅ s.booking_url을 추가하여 각 일정별 예약 링크를 직접 가져옵니다.
            sql = """
                SELECT 
                    t.title, 
                    s.price_text as price, 
                    t.agency, 
                    t.category, 
                    s.departure_date as date, 
                    t.phone,
                    t.is_priority,
                    t.location,
                    s.booking_url
                FROM tours t
                JOIN tour_schedules s ON t.product_code = s.product_code
                WHERE s.departure_date >= CURDATE()
            """
            params = []

            # 필터 조건 추가
            if target_date and target_date != 'null':
                sql += " AND s.departure_date = %s"
                params.append(target_date)
            
            if category and category != '전체':
                sql += " AND t.category LIKE %s"
                params.append(f"%{category}%")

            # 정렬 (필터 이후 맨 마지막에 추가)
            sql += " ORDER BY t.is_priority DESC, s.departure_date ASC"
            
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            # ✅ 데이터 가공 루프
            for row in results:
                # 1. 날짜 형식 변환
                if row['date'] and hasattr(row['date'], 'strftime'):
                    row['date'] = row['date'].strftime('%Y-%m-%d')
                
                # 2. 가격 텍스트 뒤에 " 원 이상" 추가
                if row['price']:
                    row['price'] = f"{row['price']} 원 이상"
            
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    # 서버 재시작 시 포트 충돌이 나면 sudo fuser -k 5000/tcp 명령어를 잊지 마세요!
    app.run(host='0.0.0.0', port=5000, debug=True)