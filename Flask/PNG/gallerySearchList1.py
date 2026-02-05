import requests
import json
import sys
import time
from datetime import datetime

def log(message):
    """시간과 함께 표준 에러(stderr)로 로그 출력"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", file=sys.stderr)

def get_all_photos(keyword, service_key):
    url = 'http://apis.data.go.kr/B551011/PhotoGalleryService1/gallerySearchList1'
    all_items = []
    page_no = 1
    num_of_rows = 100 

    log(f"🚀 '{keyword}' 검색 시작 (단위: {num_of_rows}개씩)")

    while True:
        start_time = time.time()
        log(f"📡 페이지 {page_no} 요청 중...")
        
        params = {
            'serviceKey': service_key,
            'numOfRows': str(num_of_rows),
            'pageNo': str(page_no),
            'MobileOS': 'ETC',
            'MobileApp': 'AppTest',
            'keyword': keyword,
            '_type': 'json'
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            elapsed_time = time.time() - start_time
            
            log(f"📥 응답 수신: HTTP {response.status_code} ({elapsed_time:.2f}초)")

            if response.status_code != 200:
                log(f"❌ 에러 발생: {response.text}")
                break

            data = response.json()
            body = data.get('response', {}).get('body', {})
            items_container = body.get('items')

            # 데이터가 아예 없는 경우 처리
            if not items_container or items_container == "":
                log("🏁 더 이상 가져올 데이터가 없습니다.")
                break

            items = items_container.get('item', [])
            
            # 검색 결과가 1개일 때 dict로 오는 경우 처리
            if isinstance(items, dict):
                items = [items]
            
            if not items:
                log("🏁 빈 아이템 리스트 수신. 수집을 종료합니다.")
                break

            all_items.extend(items)
            total_count = body.get('totalCount', 0)
            
            log(f"✅ 수집 성공: 이번 페이지 {len(items)}개 (누적: {len(all_items)} / 전체: {total_count})")

            if len(all_items) >= total_count:
                log("🎊 모든 데이터를 수집했습니다.")
                break
                
            page_no += 1
            
        except Exception as e:
            log(f"❗ 네트워크 오류 발생: {e}")
            break

    return all_items

# --- 실행부 ---
MY_KEY = 'eb08c1ad2a7c050ba576b0d3669ecb5d82c5484660c0ec6df85fae3b563a1c2a'

# 1. 데이터 수집
results = get_all_photos('대관령 설경', MY_KEY)

# 2. 최종 결과 요약
log(f"✨ 최종 완료: 총 {len(results)}개의 데이터를 추출했습니다.")

# 3. 데이터 출력 (이 부분만 a.txt로 들어감)
for item in results:
    print(json.dumps(item, ensure_ascii=False))