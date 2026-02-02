import pymysql
import logging
import requests
import re
from utils import get_db_connection

# --- 텔레그램 및 LOCATION_MAP 설정은 기존과 동일 ---
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"

LOCATION_MAP = {
    "서울 / 인천": {
        "서울": ["서울", "청와대", "경복궁", "남산", "인사동", "광장시장"],
        "인천": ["인천", "월미도", "개항장", "무의도", "차이나타운"],
        "강화": ["강화", "석모도", "교동도"]
    },
    "경기도": {
        "양주": ["양주", "나리농원"],
        "가평": ["가평", "어비계곡", "아침고요", "쁘띠프랑스", "스위스마을"],
        "여주": ["여주", "신륵사", "세종대왕", "파사성"],
        "오산": ["오산", "물향기"],
        "수원": ["수원", "화성", "융건릉"],
        "안성": ["안성", "팜랜드"],
        "포천": ["포천", "아트밸리", "산정호수"],
        "파주": ["파주", "마장호수", "임진각", "헤이리"],
        "용인": ["용인", "에버랜드", "민속촌"],
        "안산": ["제부도", "대부도", "선재도"]
    },
    "강원도": {
        "태백": ["태백", "눈축제", "구문소", "철암", "통리", "운탄고도", "황지연못", "함백산", "만항재", "당골", "雪왕雪래"],
        "인제": ["인제", "원대리", "자작나무"],
        "강릉": ["강릉", "정동진", "주문진", "경포대", "바다열차"],
        "속초": ["속초", "설악산", "대포항"],
        "춘천": ["춘천", "남이섬", "소양강"],
        "평창": ["평창", "대관령", "양떼농장", "월정사"],
        "원주": ["원주", "소금산", "출렁다리", "반계리"],
        "철원": ["철원", "고석정", "주상절리"],
        "화천": ["화천", "산천어", "평화의댐"],
        "영월": ["영월", "청령포", "선돌", "눈꽃열차"],
        "고성": ["고성", "화진포", "아야진", "해파랑길"],
        "삼척": ["삼척", "환선굴", "추암"],
        "정선": ["정선", "화암동굴", "아라리촌", "민둥산"],
        "양구": ["양구", "두타연"],
        "강원전체": ["강원도", "강원권"]
    },
    "충청도": {
        "대전": ["대전", "장태산", "성심당", "얼음동산", "상소동", "우암사적", "명상정원"],
        "공주": ["공주", "군밤축제", "무령왕릉", "공산성"],
        "부여": ["부여", "백제", "궁남지", "낙화암", "부소산성", "서동", "연꽃축제"],
        "영동": ["영동", "곶감축제", "과일나라"],
        "진천": ["진천", "농다리", "배티성지"],
        "홍성": ["홍성", "남당항", "새조개", "속동", "홍주성", "천북"],
        "예산": ["예산", "수덕사", "스카이타워"],
        "청양": ["청양", "얼음분수", "칠갑"],
        "금산": ["금산", "인삼관", "하늘물빛정원"],
        "서천": ["서천", "춘장대", "마량포구", "동백나무숲"],
        "단양": ["단양", "만천하", "고수동굴"],
        "제천": ["제천", "청풍호", "의림지"],
        "태안": ["태안", "안면도", "꽃지"],
        "보령": ["보령", "대천"]
    },
    "전라도 (전주/여수)": {
        "무주": ["무주", "덕유산", "향적봉", "구천동"],
        "정읍": ["정읍", "내장산", "쌍화차", "미술관"],
        "순창": ["순창", "강천사", "금성산성"],
        "남원": ["남원", "광한루", "지리산", "바래봉", "서도역"],
        "군산": ["군산", "선유도", "장자도"],
        "전주": ["전주", "한옥마을"],
        "부안": ["부안", "채석강"],
        "고창": ["고창", "선운사", "청보리"],
        "광양": ["광양", "옥룡사", "매화"],
        "담양": ["담양", "죽녹원", "메타세콰이어"],
        "순천": ["순천", "국가정원", "선암사", "낙안읍성"],
        "여수": ["여수", "오동도", "향일암"],
        "보성": ["보성", "녹차밭"],
        "신안": ["신안", "퍼플섬"],
        "목포": ["목포", "케이블카"],
        "구례": ["구례", "화엄사", "산수유"]
    },
    "경북 (대구/경주)": {
        "대구": ["대구", "팔공산", "김광석", "서문시장"],
        "경주": ["경주", "불국사", "황리단길"],
        "안동": ["안동", "하회마을"],
        "청송": ["청송", "주왕산", "얼음폭포", "얼음골", "협곡비경"],
        "봉화": ["봉화", "백두대간수목원", "분천", "산타마을", "협곡열차"],
        "포항": ["포항", "호미곶", "스페이스워크"],
        "울릉도": ["울릉도", "독도"],
        "문경": ["문경", "새재"],
        "영주": ["영주", "부석사"]
    },
    "부산 / 경남 (남해)": {
        "부산": ["부산", "태종대", "송도", "해운대", "자갈치", "엘시티", "해변열차"],
        "남해": ["남해", "독일마을", "보리암"],
        "통영": ["통영", "동피랑"],
        "거제": ["거제", "외도", "바람의언덕"],
        "진해": ["진해", "여좌천", "경화역"],
        "하동": ["하동", "최참판댁"]
    },
    "제주도": {
        "제주": ["제주", "성산일출봉", "우도", "에코랜드"]
    }
}

def send_telegram_msg(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    try: requests.post(url, data=payload, timeout=10)
    except: pass

def classify_location(title):
    for province_display, cities in LOCATION_MAP.items():
        for city_name, keywords in cities.items():
            if any(keyword in title for keyword in [city_name] + keywords):
                return province_display, city_name
    return "기타", "기타"

def extract_tags_only(raw_title):
    tag_list = []
    
    # 🍱 식사 관련
    if any(k in raw_title for k in ["중식", "석식", "조식", "식사제공", "도시락", "뷔페"]): 
        tag_list.append("식사포함")
    
    # ✅ 예약 상태
    if "출발확정" in raw_title or "확정" in raw_title: 
        tag_list.append("출발확정")
    
    # 🚄 기차 세분화 로직 (수정됨)
    if "KTX" in raw_title: tag_list.append("KTX")
    if "SRT" in raw_title: tag_list.append("SRT")
    if "새마을" in raw_title: tag_list.append("새마을호")
    if "무궁화" in raw_title: tag_list.append("무궁화호")
    
    # 특정 브랜드 없이 '열차'나 '기차' 단어만 있는 경우를 위해 체크
    # (단, KTX나 SRT 등 구체적인 이름이 이미 있다면 '기차' 태그는 중복이라 판단하여 제외할 수 있지만, 
    #  명확한 분류를 위해 키워드 존재 시 추가합니다.)
    if any(k in raw_title for k in ["열차", "기차"]):
        # 구체적인 기차 브랜드가 없을 때만 '기차' 태그를 붙이고 싶다면 아래와 같이 조건 추가 가능
        if not any(x in tag_list for x in ["KTX", "SRT", "새마을호", "무궁화호"]):
            tag_list.append("기차")

    # 🚌 버스 관련
    if any(k in raw_title for k in ["버스", "리무진", "셔틀"]): 
        tag_list.append("전용버스")
    
    # 🎡 테마 관련
    if any(k in raw_title for k in ["축제", "장터", "박람회", "체험", "만들기", "따기"]): 
        tag_list.append("축제/체험")
    
    if any(k in raw_title for k in ["섬", "바다", "해변", "항구", "유람선", "크루즈"]): 
        tag_list.append("바다/섬")
    
    if any(k in raw_title for k in ["온천", "스파", "수목원", "숲", "산책", "힐링", "명상"]): 
        tag_list.append("힐링/온천")

    return ",".join(tag_list)

def run_full_refinement():
    conn = get_db_connection()
    conn.autocommit(True)
    
    try:
        with conn.cursor() as cursor:
            # --- [1단계] 부모 테이블(tours) 지역 분류 ---
            cursor.execute("SELECT product_code, title, province, city FROM tours")
            parents = cursor.fetchall()
            
            logging.info(f"🚀 [1단계] 부모 테이블 지역 분류 중...")
            for p in parents:
                new_prov, new_city = classify_location(p['title'])
                if p['province'] != new_prov or p['city'] != new_city:
                    cursor.execute("UPDATE tours SET province=%s, city=%s WHERE product_code=%s", 
                                   (new_prov, new_city, p['product_code']))

            # --- [2단계] 자식 테이블(tour_schedules) 태그 업데이트 ---
            cursor.execute("SELECT id, title FROM tour_schedules")
            schedules = cursor.fetchall()
            
            logging.info(f"🚀 [2단계] 자식 테이블 태그 추출 시작 ({len(schedules)}건)...")
            child_update_count = 0
            for s in schedules:
                tags = extract_tags_only(s['title'])
                
                # ✅ 수정: title_main, title_sub를 제외하고 tags만 업데이트
                cursor.execute("""
                    UPDATE tour_schedules 
                    SET tags = %s 
                    WHERE id = %s
                """, (tags, s['id']))
                child_update_count += 1
                
                if child_update_count % 500 == 0:
                    logging.info(f"⌛ {child_update_count}건 처리 완료...")

            msg = f"✅ [정제 완료] 태그 추출 및 부모 분류 완료 (총 {child_update_count}건)"
            logging.info(msg)
            send_telegram_msg(msg)

    finally:
        conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run_full_refinement()