#!/bin/bash

# 1. 작업 디렉토리 이동
cd /home/ubuntu/Senior/Code

# 로그에 시작 시각 기록
echo "======================================" >> daily_process.log
echo "🚀 전체 통합 작업 시작: $(date)" >> daily_process.log

# 2. 수집 스크립트 순차 실행 (하나가 끝나야 다음이 실행됨)
echo "1. 인터파크 수집 시작..." >> daily_process.log
/usr/bin/python3 interpark.py >> interpark_collect.log 2>&1

echo "2. 하나투어 수집 시작..." >> daily_process.log
/usr/bin/python3 hanatour.py >> hanatour_collect.log 2>&1

echo "3. 모두투어 수집 시작..." >> daily_process.log
/usr/bin/python3 modutour.py >> modutour_collect.log 2>&1

echo "4. 코레일 수집 시작..." >> daily_process.log
/usr/bin/python3 korail.py >> korail_collect.log 2>&1

echo "5. 노랑풍선 수집 시작..." >> daily_process.log
/usr/bin/python3 yellow.py >> yellow_collect.log 2>&1

echo "6. 롯데관광 수집 시작..." >> daily_process.log
/usr/bin/python3 lotte.py >> lotte_collect.log 2>&1

echo "7. 공휴일 정보 업데이트 시작..." >> daily_process.log
/usr/bin/python3 date.py >> date_collect.log 2>&1

# 3. 모든 수집 완료 후 분류기(Classifier) 실행 ⭐️ 핵심!
echo "🔮 [전수조사] 지역 분류 및 매핑 시작..." >> daily_process.log
/usr/bin/python3 classifier.py >> classifier.log 2>&1

echo "✅ 전체 통합 작업 완료: $(date)" >> daily_process.log
echo "======================================" >> daily_process.log