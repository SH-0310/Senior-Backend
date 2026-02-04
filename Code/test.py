import requests
from collections import Counter

url = "http://129.80.118.34:5000/api/tours"
data = requests.get(url).json()

# 1. 모든 booking_url만 추출
urls = [item['booking_url'] for item in data]

# 2. URL별 개수 세기
counts = Counter(urls)

# 3. 중복된(개수가 1보다 큰) URL만 골라내기
duplicates = {url: count for url, count in counts.items() if count > 1}

print(f"✅ 전체 데이터 개수: {len(data)}개")
print(f"⚠️ 중복된 URL 종류: {len(duplicates)}개")

# 상세 중복 내역 출력 (상위 5개)
print("\n--- [상세 중복 내역 (상위 5개)] ---")
for url, count in sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f"회수: {count}회 | URL: {url}")