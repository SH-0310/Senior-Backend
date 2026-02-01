import os
import subprocess

# 현재 폴더의 모든 파일을 확인
files = [f for f in os.listdir('.') if f.endswith('.png')]

if not files:
    print("❌ 현재 폴더에 .png 파일이 하나도 없습니다!")
else:
    print(f"🚀 {len(files)}개의 이미지 변환을 시작합니다...")
    for f in files:
        output_name = f.rsplit('.', 1)[0] + ".webp"
        # 리눅스 명령어(cwebp)를 실행
        cmd = ["cwebp", "-q", "80", "-resize", "1024", "0", f, "-o", output_name]
        
        try:
            subprocess.run(cmd, check=True)
            print(f"✅ 변환 완료: {output_name}")
        except Exception as e:
            print(f"❌ {f} 변환 실패: {e}")

    print("\n✨ 모든 작업이 끝났습니다. 'ls -lh'로 확인해보세요!")