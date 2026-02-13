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
        "의정부": ["의정부", "부대찌개거리"],
        "양평": ["양평", "두물머리", "세미원", "용문사"],
        "남양주": ["남양주", "다산생가", "물의정원"],
        "파주": ["파주", "마장호수", "임진각", "헤이리", "도라산", "통일촌", "제3땅굴", "DMZ", "판문점", "임진각"],
        "양주": ["양주", "나리농원"],
        "가평": ["가평", "어비계곡", "아침고요", "쁘띠프랑스", "스위스마을"],
        "여주": ["여주", "신륵사", "세종대왕", "파사성"],
        "오산": ["오산", "물향기"],
        "수원": ["수원", "화성", "융건릉"],
        "안성": ["안성", "팜랜드", "금광호수"],
        "포천": ["포천", "아트밸리", "산정호수"],
        "용인": ["용인", "에버랜드", "민속촌"],
        "안산": ["제부도", "대부도", "선재도"]
    },
    "강원도": {
        "횡성": ["횡성"],
        "태백": ["태백", "눈축제", "구문소", "철암", "통리", "운탄고도", "황지연못", "함백산", "만항재", "당골"],
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
        "천안": ["천안", "각원사", "독립기념관", "뚜쥬루"],
        "당진": ["당진", "삼선산", "솔뫼성지", "면천읍성", "삽교호"],
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
        "보령": ["보령", "대천"],
        "서산": ["서산", "유기방가옥", "개심사", "해미읍성"],
        "괴산": ["괴산", "산막이", "수옥폭포", "문광호", "연하협"]
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
        "예천": ["예천", "회룡포", "용궁마을", "초간정", "용문사"],
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

# ===============================
# spot_rank_tags 파이프라인 추가
# ===============================
def sync_spot_rank_tags():
    """picnic_spots 기준으로 spot_rank_tags contentid 동기화"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT IGNORE INTO spot_rank_tags (contentid)
                SELECT contentid FROM picnic_spots
            """)
            logging.info(f"spot_rank_tags 신규 {cursor.rowcount}건 추가")
        conn.commit()
    finally:
        conn.close()


def update_spot_rank_flags():
    """
    detailIntro2(spot_details) 기반 편의 플래그 업데이트
    ✅ 현재 DB는 utf8mb4_unicode_ci로 통일되어 있으므로 별도 COLLATE 없이 JOIN
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE spot_rank_tags t
                LEFT JOIN spot_details d
                  ON d.contentid = t.contentid
                SET
                  t.babycarriage_ok = CASE
                    WHEN IFNULL(d.chkbabycarriage,'') REGEXP '가능|가능함|Y|예|있음' THEN 1 ELSE 0 END,
                  t.has_parking = CASE
                    WHEN IFNULL(d.parking,'') REGEXP '있음|가능|주차' THEN 1 ELSE 0 END,
                  t.pet_ok = CASE
                    WHEN IFNULL(d.chkpet,'') REGEXP '가능|가능함|Y|예' THEN 1 ELSE 0 END,
                  t.card_ok = CASE
                    WHEN IFNULL(d.chkcreditcard,'') REGEXP '가능|가능함|Y|예' THEN 1 ELSE 0 END,
                  t.has_usetime = IF(IFNULL(d.usetime,'')='', 0, 1),
                  t.has_restdate = IF(IFNULL(d.restdate,'')='', 0, 1),
                  t.has_infocenter = IF(IFNULL(d.infocenter,'')='', 0, 1)
            """)
        conn.commit()
        logging.info("spot_rank_tags 편의 플래그 업데이트 완료")
    finally:
        conn.close()


def update_spot_rank_scores():
    """
    rule_v5: spot_info(infotext) 통합 검색 반영 및 4개 그룹 점수 최적화
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION group_concat_max_len = 1048576;")
            cursor.execute("""
                UPDATE spot_rank_tags t
                JOIN picnic_spots p ON p.contentid = t.contentid
                LEFT JOIN spot_commons c ON c.contentid = t.contentid
                -- ✅ spot_info 데이터를 하나로 합쳐 JOIN (1:N 해결)
                LEFT JOIN (
                    SELECT contentid, 
                           GROUP_CONCAT(infotext SEPARATOR ' ') as combined_info
                    FROM spot_info
                    GROUP BY contentid
                ) i ON i.contentid = t.contentid
                SET
                  t.tagged_at = NOW(),
                  t.version = 'rule_v5',

                  -- 1️⃣ 유아동반(toddler): 유모차/주차/편의시설/통합 키워드
                  t.score_toddler = LEAST(100,
                    (CASE p.contenttypeid
                      WHEN '15' THEN 10 WHEN '14' THEN 9 WHEN '12' THEN 8 ELSE 3 END)
                    + (COALESCE(t.babycarriage_ok,0) * 25)
                    + (COALESCE(t.has_parking,0) * 10)
                    + (COALESCE(t.has_usetime,0) * 2)
                    + (COALESCE(t.has_infocenter,0) * 2)
                    + (CASE WHEN (IFNULL(c.title,'') REGEXP '키즈|어린이|유아|놀이|체험|아쿠아|동물|목장|농장|테마파크'
                               OR IFNULL(c.overview,'') REGEXP '키즈|어린이|유아|놀이|체험|아쿠아|동물|목장|농장|테마파크'
                               OR IFNULL(i.combined_info,'') REGEXP '키즈|어린이|유아|놀이|체험|아쿠아|동물|목장|농장|테마파크|수유실|기저귀|대여')
                            THEN 20 ELSE 0 END)
                    + (CASE WHEN (IFNULL(c.title,'') REGEXP '실내|체험관|박물관|미술관|전시관|과학관'
                               OR IFNULL(c.overview,'') REGEXP '실내|체험관|박물관|미술관|전시관|과학관'
                               OR IFNULL(i.combined_info,'') REGEXP '실내|전시실|교육|수업')
                            THEN 10 ELSE 0 END)
                  ),

                  -- 2️⃣ 초등동반(elementary): 체험/교육/자연/액티비티
                  t.score_elementary = LEAST(100,
                    (CASE p.contenttypeid
                      WHEN '15' THEN 12 WHEN '14' THEN 15 WHEN '28' THEN 18 WHEN '12' THEN 10 ELSE 3 END)
                    + (COALESCE(t.has_parking,0) * 8)
                    + (COALESCE(t.babycarriage_ok,0) * 4)
                    + (CASE WHEN (IFNULL(c.title,'') REGEXP '과학관|초등|천문|박물관|역사|체험|만들기|수목원|동물|아쿠아'
                               OR IFNULL(c.overview,'') REGEXP '과학관|초등|천문|박물관|역사|체험|만들기|수목원|동물|아쿠아'
                               OR IFNULL(i.combined_info,'') REGEXP '과학관|초등|천문|박물관|역사|체험|만들기|수목원|동물|아쿠아|교육|해설|도슨트|만들기|수업')
                            THEN 18 ELSE 0 END)
                    + (CASE WHEN (IFNULL(c.title,'') REGEXP '공원|호수|숲|산책|해변|바다'
                               OR IFNULL(c.overview,'') REGEXP '공원|호수|숲|산책|해변|바다')
                            THEN 12 ELSE 0 END)
                    - (CASE WHEN IFNULL(c.title,'') REGEXP '둘레길|등산|트레킹|경기옛길' THEN 15 ELSE 0 END)
                  ),

                  -- 3️⃣ 청소년(teen): 포토/감성/야경/액티비티
                  t.score_teen = LEAST(100,
                    -- [1] 기본 점수: 카테고리별 격차를 최소화 (키워드로 승부하게 함)
                    (CASE p.contenttypeid
                    WHEN '28' THEN 10 -- 레포츠
                    WHEN '38' THEN 10 -- 쇼핑/번화가
                    WHEN '12' THEN 8  -- 관광지
                    WHEN '14' THEN 7  -- 문화시설
                    WHEN '15' THEN 8  -- 축제
                    ELSE 3 END)
                    
                    -- [2] 비주얼 & 감성 가점: 12, 14번이 28번을 추월할 수 있는 핵심 동력
                    + (CASE WHEN (
                        IFNULL(c.title,'') REGEXP '인생샷|포토존|사진찍기|인스타그램|야경|불빛|루프탑|핫플|카페거리|벽화마을|감성|뷰|전망대'
                    OR IFNULL(c.overview,'') REGEXP '인생샷|포토존|사진찍기|인스타그램|야경|불빛|루프탑|핫플|카페거리|벽화마을|감성|뷰|전망대'
                    OR IFNULL(i.combined_info,'') REGEXP '인생샷|포토존|사진찍기|인스타그램|야경|불빛|루프탑|핫플|카페거리|벽화마을|감성|뷰|전망대'
                    ) THEN 40 ELSE 0 END) -- 가점을 40점으로 대폭 상향
                    
                    -- [3] 액티비티 가점: 실제 즐길거리가 있는 경우 (중복 방지를 위해 키워드 정교화)
                    + (CASE WHEN (
                        IFNULL(c.title,'') REGEXP '짚라인|루지|레일바이크|카약|서핑|패러글라이딩|액티비티|테마파크|놀이동산|체험|서바이벌'
                    OR IFNULL(c.overview,'') REGEXP '짚라인|루지|레일바이크|카약|서핑|패러글라이딩|액티비티|테마파크|놀이동산|체험|서바이벌'
                    ) THEN 35 ELSE 0 END)
                    
                    -- [4] 트렌드 가점: 청소년/중학생/고등학생 직접 언급 시 추가 보너스
                    + (CASE WHEN (
                        IFNULL(c.title,'') REGEXP '청소년|중학생|고등학생|학생'
                    OR IFNULL(c.overview,'') REGEXP '청소년|중학생|고등학생|학생'
                    ) THEN 10 ELSE 0 END)

                    - (CASE WHEN IFNULL(c.title,'') REGEXP '둘레길|산책길|산책로|등산|트레킹|해파랑길|경기옛길|모실길' 
                    THEN 25 ELSE 0 END)
                ),

                  -- 4️⃣ 혼자여행(solo): 산책/전시/조용한 여행
                  -- 4️⃣ 혼자여행(solo): 사색/전시/풍경/조용한 여행 최적화
                t.score_solo = LEAST(100,
                    -- [1] 기본 점수 (카테고리 간 격차 최소화)
                    (CASE p.contenttypeid
                    WHEN '12' THEN 10 -- 관광지
                    WHEN '14' THEN 10 -- 문화시설
                    WHEN '25' THEN 8  -- 여행코스
                    WHEN '38' THEN 7  -- 쇼핑/거리
                    ELSE 4 END)
                    
                    -- [2] 자연 & 사색 가점 (둘레길, 호수 등 혼자 걷기 좋은 곳)
                    + (CASE WHEN (
                        IFNULL(c.title,'') REGEXP '산책|둘레길|트레킹|길|공원|정원|호수|숲|풍경|사색|혼자|조용한|명소'
                    OR IFNULL(c.overview,'') REGEXP '산책|둘레길|트레킹|길|공원|정원|호수|숲|풍경|사색|혼자|조용한|명소'
                    OR IFNULL(i.combined_info,'') REGEXP '산책|둘레길|트레킹|길|공원|정원|호수|숲|풍경|사색|혼자|조용한|명소'
                    ) THEN 40 ELSE 0 END) -- 가점을 40점으로 상향하여 변별력 확보
                    
                    -- [3] 문화 & 예술 가점 (박물관, 미술관 등 혼자 관람하기 좋은 곳)
                    + (CASE WHEN (
                        IFNULL(c.title,'') REGEXP '박물관|미술관|전시|갤러리|역사|문화|한옥|서점|북카페'
                    OR IFNULL(c.overview,'') REGEXP '박물관|미술관|전시|갤러리|역사|문화|한옥|서점|북카페'
                    ) THEN 30 ELSE 0 END)

                    -- [4] 비주얼 가점 (혼자서도 가기 좋은 전망/야경 명소)
                    + (CASE WHEN (
                        IFNULL(c.title,'') REGEXP '전망|야경|뷰|감성|포토존'
                    OR IFNULL(c.overview,'') REGEXP '전망|야경|뷰|감성|포토존'
                    ) THEN 15 ELSE 0 END)

                    -- ✅ [5] 혼자 가기 어색하거나 시끄러운 장소 페널티 (감점)
                    - (CASE WHEN (
                        IFNULL(c.title,'') REGEXP '테마파크|워터파크|놀이동산|캠핑장|키즈'
                    OR p.contenttypeid = '15' -- 축제는 혼자 가기 난이도가 높으므로 감점
                    ) THEN 20 ELSE 0 END)
                )

                WHERE p.contenttypeid <> '39'
            """)
        conn.commit()
        logging.info("🚀 spot_rank_tags v5 통합 업데이트 완료 (infotext 반영)")
    except Exception as e:
        logging.error(f"❌ 점수 업데이트 실패: {e}")
    finally:
        conn.close()



def run_spot_rank_pipeline():
    logging.info("🚀 spot_rank_tags 파이프라인 시작")
    sync_spot_rank_tags()
    update_spot_rank_flags()
    update_spot_rank_scores()
    logging.info("✅ spot_rank_tags 파이프라인 완료")



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

    try:
        run_full_refinement()
    except Exception as e:
        logging.exception("❌ run_full_refinement 실패 (그래도 spot_rank는 계속 진행)")

    try:
        run_spot_rank_pipeline()
    except Exception as e:
        logging.exception("❌ run_spot_rank_pipeline 실패")

