import os
import subprocess

# 1. 대상 확장자 설정
target_extensions = ('.jpg', '.jpeg', '.png')

# 현재 폴더 파일 리스트업
files = [f for f in os.listdir('.') if f.lower().endswith(target_extensions)]

if not files:
    print("❌ 변환할 이미지 파일이 없습니다!")
else:
    print(f"🚀 {len(files)}개의 가로형 고화질 이미지 변환을 시작합니다...")
    for f in files:
        output_name = os.path.splitext(f)[0] + ".webp"
        
        # 🎨 가로형(Landscape) 고화질 설정
        cmd = [
            "cwebp", 
            "-q", "85",             # 품질 85 (가로형은 디테일이 중요하므로 살짝 높임)
            "-resize", "1920", "0", # 가로를 1920px(FHD)로 고정, 세로는 비율 유지
            "-m", "6",              # 압축 효율 최대
            "-sharpness", "2",      # 풍경 사진의 선명도를 위해 샤프니스 약간 추가
            "-metadata", "none",    # 정보 삭제
            f, 
            "-o", output_name
        ]
        
        try:
            subprocess.run(cmd, check=True)
            print(f"✅ 최적화 완료: {f} -> {output_name}")
        except Exception as e:
            print(f"❌ {f} 변환 실패: {e}")

    print("\n✨ 모든 작업이 끝났습니다! AI 상세 페이지용 static 폴더에 넣으세요.")