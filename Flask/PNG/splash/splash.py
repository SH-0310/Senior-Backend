import os
import subprocess

# 1. 대상 확장자 설정 (.jpg, .jpeg, .png 포함)
target_extensions = ('.jpg', '.jpeg', '.png')

# 현재 폴더 파일 리스트업
files = [f for f in os.listdir('.') if f.lower().endswith(target_extensions)]

if not files:
    print("❌ 변환할 이미지 파일이 없습니다!")
else:
    print(f"🚀 {len(files)}개의 스플래시 이미지 변환을 시작합니다...")
    for f in files:
        output_name = os.path.splitext(f)[0] + ".webp"
        
        # 🎨 스플래시 전용 고화질 설정
        cmd = [
            "cwebp", 
            "-q", "82",           # 품질 82 (용량 대비 화질 최적)
            "-resize", "1080", "0", # 가로 1080px로 리사이징 (현대 스마트폰 표준)
            "-m", "6",             # 압축 효율 최대 (시간은 좀 더 걸리지만 용량 최소화)
            "-metadata", "none",   # 불필요한 촬영 정보 삭제
            f, 
            "-o", output_name
        ]
        
        try:
            subprocess.run(cmd, check=True)
            print(f"✅ 최적화 완료: {f} -> {output_name}")
        except Exception as e:
            print(f"❌ {f} 변환 실패: {e}")

    print("\n✨ 모든 작업이 끝났습니다! 이제 프로젝트의 drawable 폴더에 넣으세요.")