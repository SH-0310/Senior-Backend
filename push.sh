#!/bin/bash

# 1. 커밋 메시지 받기 (입력 안 하면 기본 메시지 사용)
MESSAGE=${1:-"Update: $(date +'%Y-%m-%d %H:%M:%S')"}

echo "🚀 GitHub로 코드 업로드를 시작합니다..."

# 2. Git 작업
git add .
git commit -m "$MESSAGE"
git push origin main

echo "✅ 업로드 완료! 슬레이브 서버에서 'sh pull.sh'를 실행하세요."
