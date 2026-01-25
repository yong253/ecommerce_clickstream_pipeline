from kafka import KafkaProducer
import boto3
import gzip
import io
import json
import csv
import time
import argparse
import os
import sys
import logging
import snappy

# 1. 로그 디렉토리 및 파일 설정
log_dir = "logs"
log_file = os.path.join(log_dir, "ecommerce_collector.log")

# 로그 디렉토리가 없으면 생성
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 2. 로깅 설정 수정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),           # 터미널 콘솔 출력용
        logging.FileHandler(log_file)     # 파일 저장용 (logs/Ecommerce_collector.log)
    ]
)
logging.getLogger('kafka').setLevel(logging.ERROR)
logger = logging.getLogger("EcommerceCollector")

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.config import *

'''
1. configuration 설정
2. producer 생성
3. 메시지 수 제한 로직
4. 메시지 전송
'''
def create_producer(args):
    producer = KafkaProducer(
        bootstrap_servers = args.bootstrap_servers,
        batch_size = args.batch_size,
        linger_ms = args.linger_ms,
        acks=1,
        compression_type = 'snappy',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def data_loader(file_path):
    with open(file_path, mode='r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def s3_data_loader(bucket, key):
    """S3에서 압축 파일을 스트리밍으로 읽어 한 줄씩 반환"""
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    logger.info(f"S3 객체 가져오는 중: s3://{bucket}/{key}")
    response = s3.get_object(Bucket=bucket, Key=key)

    # response['Body'](StreamingBody)를 gzip으로 감싸 디스크 없이 메모리에서 해제
    with gzip.GzipFile(fileobj=response['Body']) as gzip_file:
        # csv.DictReader를 위해 텍스트 스트림으로 변환
        text_file = io.TextIOWrapper(gzip_file, encoding='utf-8')
        reader = csv.DictReader(text_file)
        for row in reader:
            yield row

def kafka_send(producer, topic, bucket, key):
    target_tps = 10 # 초당 500건
    batch_group_size = 50 # 한번에 묶어서 쏠 단위
    # 전송 멈추는 시간. 50/500 = 0.1초
    sleep_interval = batch_group_size / target_tps
    logger.info(f"전송 시작: 초당 {target_tps}건 목표 (간격: {sleep_interval}초)")

    count = 0
    for data in s3_data_loader(bucket, key):
        producer.send(topic, value=data)
        count += 1

        if count % batch_group_size == 0: # 배치 size만큼 보냈으면 잠시 휴식
            logger.info(f"전송 완료: {count}건, 휴식: {sleep_interval}초")
            time.sleep(sleep_interval)
    producer.flush()

def main():
    # 명령줄 인자 전달
    parser = argparse.ArgumentParser(description='이커머스 클릭스트림 수집기')

    parser.add_argument('--bootstrap_servers', type=str, nargs='+',
                        default=['kafka1:29092', 'kafka2:29092', 'kafka3:29092'],
                        help='Kafka 서버 주소(기본값: localhost)')
    parser.add_argument('--kafka_topic', type=str, default='user_clickstream', help='kafka topic 지정(기본값: user_clickstream)')
    parser.add_argument('--batch_size', type=int, default=32678)
    parser.add_argument('--linger_ms', type=int, default=10)
    parser.add_argument('--bucket', type=str, default='ecommerce-datalake-yong')
    parser.add_argument('--key', type=str, default='dataset/2019-Oct.gz')
    args = parser.parse_args()

    producer = create_producer(args)

    kafka_send(producer, args.kafka_topic, args.bucket, args.key)


if __name__ == "__main__":
    main()
