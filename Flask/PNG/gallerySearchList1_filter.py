import requests
import json
import sys

def filter_valid_images(input_file):
    # 브라우저인 척 속이기 위한 헤더 (관광공사 서버 필수)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    valid_items = []
    
    # 1. 파일 읽기
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ {input_file} 파일을 찾을 수 없습니다.", file=sys.stderr)
        return

    total = len(lines)
    print(f"🚀 총 {total}개의 데이터를 검증합니다...", file=sys.stderr)

    # 2. 하나씩 접속해서 확인
    for i, line in enumerate(lines, 1):
        item = json.loads(line.strip())
        url = item.get('galWebImageUrl')
        title = item.get('galTitle')

        try:
            # stream=True를 써서 이미지를 다 다운로드하지 않고 연결 상태만 확인
            response = requests.get(url, headers=headers, stream=True, timeout=5)
            
            if response.status_code == 200:
                valid_items.append(item)
                # 표준 출력(stdout)으로 유효한 JSON 출력 (나중에 > 결과파일.txt 용도)
                print(json.dumps(item, ensure_ascii=False))
                print(f"✅ [{i}/{total}] 성공: {title}", file=sys.stderr)
            else:
                print(f"❌ [{i}/{total}] 실패 (Status {response.status_code}): {title}", file=sys.stderr)
        
        except Exception as e:
            print(f"⚠️ [{i}/{total}] 에러 ({e}): {title}", file=sys.stderr)

    print(f"\n✨ 검증 완료! 유효한 이미지: {len(valid_items)} / {total}", file=sys.stderr)

if __name__ == "__main__":
    # a.txt 파일을 입력으로 사용
    filter_valid_images('a.txt')