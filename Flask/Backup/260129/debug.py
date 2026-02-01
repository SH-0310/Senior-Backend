import requests
import json
from datetime import datetime

def check_hanatour_status_detailed(pkg_code):
    # 1. API 정보 설정 (제공해주신 정보 반영)
    url = "https://gw.hanatour.com/package/pkg/api/common/pkgcomprod/getPkgProdInfo/v1.00?_siteId=hanatour"
    
    # 2. 헤더 설정 (브라우저 환경 모사)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.hanatour.com",
        "Referer": f"https://www.hanatour.com/trp/pkg/CHPC0PKG0200M200?pkgCd={pkg_code}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0",
        "prgmid": "CHPC0PKG0200M200"
    }
    
    # 3. 페이로드 설정 (제공해주신 Payload 반영)
    payload = {
        "pkgCd": pkg_code,
        "inpPathCd": "DCP",
        "smplYn": "N",
        "coopYn": "N",
        "partnerYn": "N",
        "resAcceptPtn": {}
    }

    print(f"\n🚀 [검증 시작] 상품코드: {pkg_code}")
    
    try:
        # POST 요청 실행
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        # 상세 통신 로그 출력
        print(f"📡 [통신 로그] Status Code: {response.status_code} | 응답 크기: {len(response.text)} bytes")

        if response.status_code == 200:
            res_json = response.json()
            data = res_json.get('data', {})
            
            if not data:
                print("❌ [오류] API 응답에 상품 데이터가 비어 있습니다.")
                return "데이터 없음"

            # 핵심 필드 추출
            res_psbl = data.get('resAddPsblYn')  # 예약 추가 가능 여부
            bkng_stat = data.get('bkngStatCd')   # 예약 상태 코드
            prod_name = data.get('saleProdNm', '상품명 없음')
            dep_day = data.get('depDay', '날짜 미상')
            
            print(f"📦 [상품 정보] {prod_name}")
            print(f"📅 [출발 일자] {dep_day}")
            print(f"📊 [상태 분석] resAddPsblYn: {res_psbl} | bkngStatCd: {bkng_stat}")

            # -------------------------------------------------------
            # 🎯 최종 마감 판정 로직
            # -------------------------------------------------------
            # 예약 가능 조건: resAddPsblYn이 'Y' 이고 bkngStatCd가 '2'인 경우
            if res_psbl == "Y" and bkng_stat == "2":
                print("✅ [결과] 현재 예약 가능한 상품입니다.")
                return None  # 정상
            else:
                # 마감 사유 세분화
                reason = ""
                if res_psbl == "N":
                    reason = "예약추가불가(N)"
                if bkng_stat == "0":
                    reason += " | 예약정지상태(0)"
                
                error_msg = f"하나투어 마감: {reason}"
                print(f"🚨 [결과] {error_msg}")
                return error_msg

        else:
            print(f"❌ [오류] API 서버 연결 실패 (HTTP {response.status_code})")
            return f"API 에러({response.status_code})"

    except Exception as e:
        print(f"❌ [오류] 예외 발생: {str(e)}")
        return f"통신 장애({str(e)[:15]})"

# --- 테스트 실행 영역 ---
print("="*50)
print("하나투어 실시간 API 정밀 검증 테스트")
print("="*50)

# 1. 예약 마감 상품 테스트
check_hanatour_status_detailed("AKP612260117001")

print("\n" + "-"*50)

# 2. 예약 가능 상품 테스트 (기존 데이터 기준)
check_hanatour_status_detailed("AKP612260123001")