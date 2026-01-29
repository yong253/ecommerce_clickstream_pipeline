#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
설정 파일
Ecommerce Clickstream Pipeline 프로젝트에서 사용되는 설정 값들을 정의합니다.
"""

import os
from dotenv import load_dotenv

# .env222 파일 로드 (존재하는 경우)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env222')
if os.path.exists(env_path):
    load_dotenv(env_path)

# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:29092,kafka2:29092,kafka3:29092')
ECOMMERCE_TOPIC = os.getenv('ECOMMERCE_TOPIC', 'user_clickstream')


# 이메일 알림 설정
EMAIL_SENDER = os.getenv('EMAIL_SENDER', '')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')
EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT', '')
EMAIL_SMTP_SERVER = os.getenv('EMAIL_SMTP_SERVER', 'smtp.gmail.com')
EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', 587))

# Slack 알림 설정
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', '#alert-test')

# S3 경로 설정
# S3 데이터 경로
REFINED_PATH = os.getenv("S3_REFINED_EVENT_PATH")
USER_CAT_SCORE_PATH = os.getenv("S3_AGG_USER_CATEGORY_SCORE_PATH")
TOP_PRODUCTS_PATH = os.getenv("S3_AGG_CATEGORY_TOP_PRODUCTS_PATH")

# 체크포인트 경로
CHK_REFINED = os.getenv("S3_CHK_REFINED_EVENTS_PATH")
CHK_USER_CAT = os.getenv("S3_CHK_AGG_USER_CATEGORY_PATH")
CHK_CAT_PROD = os.getenv("S3_CHK_AGG_CATEGORY_PRODUCT_PATH")

# AWS KEY
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")