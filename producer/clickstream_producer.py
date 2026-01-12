from kafka import KafkaProducer
import json
import argparse

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

def kafka_send(producer):
    print(1)


def main():
    # 명령줄 인자 전달
    parser = argparse.ArgumentParser(description='이커머스 클릭스트림 수집기')

    parser.add_argument('--bootstrap_servers', type=str, nargs='+',
                        default=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                        help='Kafka 서버 주소(기본값: localhost)')
    parser.add_argument('--kafka_topic', type=str, default='user_clickstream')
    parser.add_argument('--batch_size', type=int, default=32678)
    parser.add_argument('--linger_ms', type=int, default=10)
    args = parser.parse_args()

    producer = create_producer(args)

    kafka_send(producer)


if __name__ == "__main__":
    main()
