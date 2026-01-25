import redis
import boto3
import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import logging

LOG_FILE_PATH = "/app/logs/clickstream_analyzer.log"
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE_PATH)
    ]
)
logger = logging.getLogger("ClickstreamAnalyzer")

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.config import *

class ClickstreamAnalyzer:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.redis_host = "redis"
        self.redis_port = 6379
        # S3 데이터 저장 경로 설정
        self.S3_REFINED_PATH = f"s3a://{REFINED_PATH}" # 1차 변환 데이터
        self.S3_USER_CAT_SCORE_PATH = f"s3a://{USER_CAT_SCORE_PATH}" # 유저별 카테고리 선호도 집계 데이터 저장 경로
        self.S3_TOP_PRODUCTS_PATH = f"s3a://{TOP_PRODUCTS_PATH}" # 카테고리별 인기 상품 집계 데이터 저장 경로

        # S3 체크포인트 저장 경로
        self.S3_CHK_REFINED = f"s3a://{CHK_REFINED}" # 1차 변환 데이터
        self.S3_CHK_USER_CAT = f"s3a://{CHK_USER_CAT}" # 유저별 카테고리 선호도
        self.S3_CHK_CAT_PROD = f"s3a://{CHK_CAT_PROD}" # 카테고리별 인기 상품

        self.raw_schema = StructType([
            StructField("event_time", StringType(), False), # 이벤트 발생 시간
            StructField("event_type", StringType(), False), # 이벤트 유형(가중치)
            StructField("product_id", IntegerType(), False), # 상품 ID
            StructField("category_id", LongType(), False), # 카테고리 ID
            StructField("category_code", StringType(), True), # 카테고리 code
            StructField("user_id", LongType(), False),
            StructField("user_session", StringType(), True)
        ])
        logger.info("ClickstreamAnalyzer 생성기")

    def create_spark_session(self):
        """Spark 세션 및 Delta Lake/Kafka 커넥터 설정"""
        return SparkSession.builder \
            .appName("ClickstreamRealtimeEngine") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.shuffle.partitions", "12") \
            .config("spark.jars.packages", (
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4")) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()

    # --- [1. 입력: 데이터 로드] ---
    def read_kafka_stream(self):
        """Kafka로부터 실시간 스트림 읽기"""
        logger.info("read_kafka_stream start ...")
        return self.spark.readStream.format("kafka") \
            .option("kafka.group.id", "spark-analyzer-group") \
            .option("commitOffsetsOnRead", "true") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", ECOMMERCE_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", 50000) \
            .load() \
            .select(F.from_json(F.col("value").cast("string"), self.raw_schema).alias("data")) \
            .select("data.*")

    # --- [2. 변환: 비즈니스 로직 적용] ---
    def transform(self, df):
        """이벤트 시간 정제 및 가중치 부여"""
        return df.withColumn("event_time", F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC", ""))) \
            .withColumn("year", F.year(F.col("event_time"))) \
            .withColumn("month", F.month(F.col("event_time"))) \
            .withColumn("day", F.day(F.col("event_time"))) \
            .withColumn("category_code",
                        F.coalesce(F.split(F.col("category_code"), "\.").getItem(0), F.lit("accessories"))) \
            .withColumn("event_weight",
                        F.when(F.col("event_type") == "view", 1)
                        .when(F.col("event_type") == "cart", 3)
                        .when(F.col("event_type") == "purchase", 10)
                        .when(F.col("event_type") == "remove_from_cart", -3).otherwise(0))

    def write_refined_to_s3(self, df):
        """1차 정제 데이터 S3 Delta 저장 (중복 제거 포함)"""
        return df.withWatermark("event_time", "10 minutes") \
            .dropDuplicates(["user_id", "event_time", "product_id", "event_type"]) \
            .writeStream.format("delta").outputMode("append") \
            .option("checkpointLocation", self.S3_CHK_REFINED) \
            .partitionBy("year", "month", "day") \
            .start(self.S3_REFINED_PATH)

    def write_user_pref_to_sinks(self, df):
        """유저별 선호도 집계 및 Redis/S3 저장"""
        agg_df = df.withWatermark("event_time", "10 minutes") \
            .groupBy("user_id", "category_code") \
            .agg(F.sum("event_weight").alias("total_score"),
                 F.max("event_time").alias("event_time"))

        r_host = self.redis_host
        r_port = self.redis_port
        def save_batch(batch_df, batch_id):
            # Redis 업데이트
            def redis_sync(partition):
                r = redis.Redis(host=r_host, port=r_port, db=0)
                pipe = r.pipeline()
                for row in partition:
                    key = f"pref:user:{row['user_id']}:today"
                    pipe.hset(key, row['category_code'], row['total_score'])
                    pipe.expire(key, 86400)
                pipe.execute()

            batch_df.foreachPartition(redis_sync)

            # S3 집계 결과 백업
            batch_df.write.format("delta").mode("append").save(self.S3_USER_CAT_SCORE_PATH)

        return agg_df.writeStream.outputMode("update") \
            .foreachBatch(save_batch) \
            .option("checkpointLocation", self.S3_CHK_USER_CAT) \
            .start()

    def write_top_products_to_sinks(self, df):
        """카테고리별 상품 순위 집계 및 Redis/S3 저장"""
        agg_df = df.withWatermark("event_time", "10 minutes") \
            .groupBy("category_code", "product_id") \
            .agg(F.sum("event_weight").alias("pop_score"),
                 F.max("event_time").alias("event_time"))
        r_host = self.redis_host
        r_port = self.redis_port
        def save_batch(batch_df, batch_id):
            # Redis 업데이트
            def redis_sync(partition):
                r = redis.Redis(host=r_host, port=r_port, db=0)
                pipe = r.pipeline()
                for row in partition:
                    key = f"cat:rank:{row['category_code']}:today"
                    pipe.zadd(key, {str(row['product_id']): row['pop_score']})
                    pipe.expire(key, 86400)
                pipe.execute()

            batch_df.foreachPartition(redis_sync)

            # S3 집계 결과 백업
            batch_df.write.format("delta").mode("append").save(self.S3_TOP_PRODUCTS_PATH)

            # 마감 마커 생성 체크
            self.check_and_write_s3_marker(batch_df)

        return agg_df.writeStream.outputMode("update") \
            .foreachBatch(save_batch) \
            .option("checkpointLocation", self.S3_CHK_CAT_PROD) \
            .start()

    def check_and_write_s3_marker(self, batch_df):
        """S3 마감 마커 파일 (.done) 생성"""
        max_time_row = batch_df.select(F.max("event_time")).collect()
        if max_time_row and max_time_row[0][0]:
            current_max = max_time_row[0][0]
            yesterday = (current_max - timedelta(days=1)).strftime("%Y-%m-%d")
            # 10분 워터마크 기준 날짜가 완전히 넘어갔는지 확인
            if (current_max - timedelta(minutes=10)).date() >= current_max.date():
                s3 = boto3.client('s3')
                # 마커는 보통 refined 폴더의 상위 status 폴더에 둡니다.
                bucket = self.S3_REFINED_PATH.split("/")[2]
                s3.put_object(Bucket=bucket, Key=f"status/streaming_done_{yesterday}.done", Body='')

    # --- [Step 3: 실행 컨트롤러] ---
    def run(self):
        logger.info("실시간 파이프라인 가동...")

        # 1. 원본 스트림 로드 및 공통 정제
        stream_df = self.read_kafka_stream()
        enriched_df = self.transform(stream_df)

        # 2. 독립적인 3개의 Sink 스트림 실행
        raw_query = self.write_refined_to_s3(enriched_df)
        user_query = self.write_user_pref_to_sinks(enriched_df)
        logger.info("유저별 선호도 집계 및 Redis/S3 저장")
        rank_query = self.write_top_products_to_sinks(enriched_df)
        logger.info("카테고리별 상품 순위 집계 및 Redis/S3 저장")

        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    ClickstreamAnalyzer().run()