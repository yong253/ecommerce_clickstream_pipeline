from kafka import KafkaProducer
import json
import csv
import time
import argparse
import logging

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
logger = logging.getLogger("EcommerceCollector")

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

def kafka_send(producer, topic):
    target_tps = 500 # 초당 500건
    batch_group_size = 50 # 한번에 묶어서 쏠 단위
    file_path = r'C:\Users\leeyongseok\ecommerce_clickstream_pipeline\dataset\2019-Oct.csv' # 임시 파일 경로
    # 전송 멈추는 시간. 50/500 = 0.1초
    sleep_interval = batch_group_size / target_tps
    # print(f"전송 시작: 초당 {target_tps}건 목표 (간격: {sleep_interval}초)")
    logger.info(f"전송 시작: 초당 {target_tps}건 목표 (간격: {sleep_interval}초)")

    count = 0
    for data in data_loader(file_path):
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
                        default=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                        help='Kafka 서버 주소(기본값: localhost)')
    parser.add_argument('--kafka_topic', type=str, default='user_clickstream', help='kafka topic 지정(기본값: user_clickstream)')
    parser.add_argument('--batch_size', type=int, default=32678)
    parser.add_argument('--linger_ms', type=int, default=10)
    args = parser.parse_args()

    producer = create_producer(args)

    kafka_send(producer, args.kafka_topic)


if __name__ == "__main__":
    main()
