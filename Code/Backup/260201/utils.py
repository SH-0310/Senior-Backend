# /home/ubuntu/Senior/Code/utils.py
import pymysql

def extract_all_keywords(title):
    """제목을 분석하여 필터에 쓰일 핵심 키워드들을 추출합니다."""
    keywords = []
    
    # 1. 혜택/조건 키워드
    if any(k in title for k in ["중식", "석식", "조식", "식사제공", "도시락", "식사포함"]): 
        keywords.append("식사포함")
    if "출발확정" in title: 
        keywords.append("출발확정")
        
    # 2. 이동수단/테마 키워드
    if any(k in title for k in ["KTX", "열차", "기차", "철도"]): 
        keywords.append("기차여행")
    if any(k in title for k in ["버스", "리무진"]): 
        keywords.append("전용버스")
        
    # 3. 목적/감성 키워드
    if any(k in title for k in ["축제", "장터", "박람회", "꽃", "구경"]): 
        keywords.append("축제/구경")
    if any(k in title for k in ["섬", "바다", "해변", "항구", "유람선"]): 
        keywords.append("바다/섬")
    if any(k in title for k in ["온천", "스파", "힐링", "수목원", "명상"]): 
        keywords.append("온천/힐링")

    return ",".join(keywords)

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