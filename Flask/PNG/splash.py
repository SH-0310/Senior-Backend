import os
import subprocess

# 1. 대상 확장자 설정 (.jpg, .jpeg 모두 포함)
target_extensions = ('.jpg', '.jpeg', '.png') # 여러 개를 넣고 싶을 때 튜플로 지정

# 현재 폴더에서 해당 확장자로 끝나는 파일들만 리스트업
files = [f for f in os.listdir('.') if f.lower().endswith(target_extensions)]

if not files:
    print("❌ 변환할 이미지 파일(.jpg, .jpeg, .png)이 없습니다!")
else:
    print(f"🚀 {len(files)}개의 이미지 변환을 시작합니다...")
    for f in files:
        # 파일명에서 확장자를 제거하고 .webp 붙이기
        output_name = os.path.splitext(f)[0] + ".webp"
        
        # cwebp 명령어 설정
        # -q 80: 품질(0~100)
        # -resize 128 0: 가로 128px로 맞추고 세로는 비율대로 (0)
        # ⚠️ 만약 고화질 수상작이라면 -resize 옵션은 빼거나 숫자를 키우는 게 좋습니다!
        cmd = [
            "cwebp", 
            "-q", "82",           # 품질 80~85 사이 추천
            "-resize", "1080", "0", # 가로 1080px로 리사이징 (세로는 자동 비율)
            "-m", "6",             # 최상의 압축 기술 사용
            "-metadata", "none",   # 불필요한 정보 삭제
            f, 
            "-o", output_name
        ]
        
        try:
            subprocess.run(cmd, check=True)
            print(f"✅ 최적화 완료: {f} -> {output_name}")
        except Exception as e:
            print(f"❌ {f} 변환 실패: {e}")

    print("\n✨ 모든 변환 작업이 성공적으로 끝났습니다!") # else 블록 마지막에 위치