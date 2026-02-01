# /home/ubuntu/Senior/Code/utils.py
import pymysql

def classify_categories(title):
    """제목을 분석하여 해당하는 모든 테마를 쉼표로 구분하여 반환합니다."""
    categories = []
    
    # 테마별 키워드 매핑
    mapping = {
        "지역축제": ["축제", "장터", "엑스포", "박람회", "시장", "체험"],
        "기차/추억": ["열차", "KTX", "기차", "철도", "ITX", "V-train", "S-train"],
        "섬/바다": ["섬", "유람선", "해상", "선상", "바다", "해변", "항구", "여객선"],
        "온천/건강": ["온천", "힐링", "수목원", "숲", "산책", "스파", "명상", "산림욕"]
    }
    
    for cat, keywords in mapping.items():
        if any(k in title for k in keywords):
            categories.append(cat)
            
    # 해당되는 테마가 하나도 없다면 기본값 부여
    if not categories:
        categories.append("자연/명소")
        
    return ",".join(categories)

def get_db_connection():
    """데이터베이스 연결 객체를 반환합니다."""
    return pymysql.connect(
        host='localhost',
        user='shmoon',
        password='Tjdgursla87!',
        db='senior_travel',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )