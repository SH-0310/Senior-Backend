import pymysql
import logging
import requests
import re
import json
from datetime import datetime  # ✅ 추가: datetime.now() 사용을 위해 필요
from utils import get_db_connection

# --- 텔레그램 및 LOCATION_MAP 설정은 기존과 동일 ---
TELEGRAM_TOKEN = "8543857876:AAFs2kEURQEihK6_j6mw2PPaKQO4gYoBoSM"
CHAT_ID = "8305877092"
FAILED_PARENTS_FILE = "failed_parents.log"
FAILED_CHILDREN_FILE = "failed_children.log"

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
        "논산": ["논산", "탑정호"],
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
        "영주": ["영주", "부석사"],
        "울진": ["울진", "백암온천"]
    },
    "부산 / 경남 (남해)": {
        "부산": ["부산", "태종대", "송도", "해운대", "자갈치", "엘시티", "해변열차"],
        "남해": ["남해", "독일마을", "보리암"],
        "통영": ["통영", "동피랑"],
        "거제": ["거제", "외도", "바람의언덕"],
        "진해": ["진해", "여좌천", "경화역"],
        "창녕": ["창녕", "우포늪", "부곡"],
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
    
    # 🚄 기차 브랜드 세분화 (신규 추가 및 업데이트)
    # 1. 고속열차
    if "KTX" in raw_title: tag_list.append("KTX")
    if "SRT" in raw_title: tag_list.append("SRT")
    
    # 2. 테마 관광열차 (S-train 등)
    if any(k in raw_title for k in ["S-train", "S트레인", "남도해양"]): tag_list.append("S-train")
    if any(k in raw_title for k in ["V-train", "V트레인", "협곡열차", "백두대간열차"]): tag_list.append("V-train")
    if any(k in raw_title for k in ["서해금빛", "G-train", "금빛열차"]): tag_list.append("서해금빛열차")
    if any(k in raw_title for k in ["바다열차", "Sea Train"]): tag_list.append("바다열차")
    if any(k in raw_title for k in ["정선아리랑", "A-train"]): tag_list.append("아리랑열차")
    if "눈꽃열차" in raw_title: tag_list.append("눈꽃열차")

    # 3. 일반열차
    if any(k in raw_title for k in ["새마을", "ITX"]): tag_list.append("새마을호")
    if "무궁화" in raw_title: tag_list.append("무궁화호")
    
    # 4. 포괄적 기차 태그 (위의 구체적 이름들이 하나도 없을 때만 적용)
    train_specific_tags = ["KTX", "SRT", "S-train", "V-train", "서해금빛열차", "바다열차", "아리랑열차", "눈꽃열차", "새마을호", "무궁화호"]
    if any(k in raw_title for k in ["열차", "기차"]):
        if not any(x in tag_list for x in train_specific_tags):
            tag_list.append("기차")

    # 🚌 버스 관련
    if any(k in raw_title for k in ["버스", "리무진", "셔틀"]): 
        tag_list.append("전용버스")
    
    # 🎡 테마 관련
    if any(k in raw_title for k in ["자유여행", "자유일정", "레일텔", "에어텔", "카텔"]): 
        tag_list.append("자유여행")
        
    # 💰 지자체 지원 관련 태그 추가
    if any(k in raw_title for k in ["지자체지원", "지자체협업", "지원특가", "OO군지원", "인센티브"]): 
        tag_list.append("지자체지원특가")

    if any(k in raw_title for k in ["시장", "장터", "오일장", "5일장", "수산물센터", "풍물시장"]): 
        tag_list.append("전통시장")

    # 🥾 트레킹/걷기 (기존 로직)
    if any(k in raw_title for k in ["트레킹", "둘레길", "올레길", "등산", "산행", "걷기"]): 
        tag_list.append("트레킹")

    if any(k in raw_title for k in ["박물관", "미술관", "기념관", "전시관", "역사관", "전시회", "비엔날레"]): 
        tag_list.append("문화/역사") # 또는 "전시/관람"

    if any(k in raw_title for k in ["양떼", "목장", "동물", "사파리", "아쿠아", "알파카"]): 
        tag_list.append("동물")

    if any(k in raw_title for k in ["축제", "장터", "박람회", "체험", "만들기", "따기"]): 
        tag_list.append("축제/체험")
    
    if any(k in raw_title for k in ["섬", "바다", "해변", "항구", "유람선", "크루즈"]): 
        tag_list.append("바다/섬")
    
    if any(k in raw_title for k in ["온천", "스파", "수목원", "숲", "산책", "힐링", "명상"]): 
        tag_list.append("힐링/온천")

    if not tag_list:
        tag_list.append("일반여행")
    
    return ",".join(tag_list)

def save_to_fail_log(file_path, data):
    """실패한 상품 정보를 JSON 형태로 파일에 기록"""
    with open(file_path, "a", encoding="utf-8") as f:
        log_entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": data
        }
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

def run_full_refinement():
    conn = get_db_connection()
    conn.autocommit(True)
    
    # 카운터 초기화
    stats = {
        "parent": {"total": 0, "success": 0, "fail": 0, "skipped": 0},
        "child": {"total": 0, "success": 0, "fail": 0}
    }

    try:
        with conn.cursor() as cursor:
            # --- [1단계] 부모 테이블(tours) 정제 ---
            cursor.execute("SELECT product_code, title, province, city FROM tours")
            parents = cursor.fetchall()
            stats["parent"]["total"] = len(parents)
            
            logging.info(f"🚀 [1단계] 부모 테이블 정제 시작 (총 {stats['parent']['total']}건)")
            
            for p in parents:
                try:
                    new_prov, new_city = classify_location(p['title'])
                    
                    # 분류 결과가 '기타'인 경우 (로직 개선 필요 대상)
                    if new_prov == "기타":
                        save_to_fail_log(FAILED_PARENTS_FILE, {"product_code": p['product_code'], "title": p['title'], "reason": "분류 키워드 없음"})
                        stats["parent"]["skipped"] += 1
                    
                    # DB 업데이트
                    cursor.execute("UPDATE tours SET province=%s, city=%s WHERE product_code=%s", 
                                   (new_prov, new_city, p['product_code']))
                    stats["parent"]["success"] += 1
                    
                except Exception as e:
                    stats["parent"]["fail"] += 1
                    save_to_fail_log(FAILED_PARENTS_FILE, {"product_code": p['product_code'], "error": str(e)})

            # --- [2단계] 자식 테이블(tour_schedules) 정제 ---
            cursor.execute("SELECT id, title FROM tour_schedules")
            schedules = cursor.fetchall()
            stats["child"]["total"] = len(schedules)
            
            logging.info(f"🚀 [2단계] 자식 테이블 태그 정제 시작 (총 {stats['child']['total']}건)")
            
            for s in schedules:
                try:
                    tags = extract_tags_only(s['title'])
                    cursor.execute("UPDATE tour_schedules SET tags = %s WHERE id = %s", (tags, s['id']))
                    stats["child"]["success"] += 1
                    
                    if stats["child"]["success"] % 1000 == 0:
                        logging.info(f"⌛ 자식 상품 {stats['child']['success']}건 처리 중...")
                        
                except Exception as e:
                    stats["child"]["fail"] += 1
                    save_to_fail_log(FAILED_CHILDREN_FILE, {"id": s['id'], "title": s['title'], "error": str(e)})

            # --- [3단계] 최종 리포트 생성 ---
            report = (
                f"📊 **[정제 완료 리포트]**\n\n"
                f"🏠 **부모 상품 (tours)**\n"
                f"- 전체: {stats['parent']['total']}건\n"
                f"- 성공: {stats['parent']['success']}건\n"
                f"- 미분류(기타): {stats['parent']['skipped']}건 (로그 확인 필요)\n"
                f"- 에러실패: {stats['parent']['fail']}건\n\n"
                f"👶 **자식 상품 (schedules)**\n"
                f"- 전체: {stats['child']['total']}건\n"
                f"- 성공: {stats['child']['success']}건\n"
                f"- 에러실패: {stats['child']['fail']}건\n\n"
                f"📁 실패 상세는 `{FAILED_PARENTS_FILE}` 및 `{FAILED_CHILDREN_FILE}`을 확인하세요."
            )
            
            logging.info(report)
            send_telegram_msg(report)

    finally:
        conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run_full_refinement()