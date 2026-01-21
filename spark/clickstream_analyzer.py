import redis
import boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


class ClickstreamAnalyzer:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.redis_host = "localhost"
        self.redis_port = 6379
        self.s3_bucket = "ecommerce-datalake"

        self.raw_schema = StructType([
            StructField("event_time", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("category_id", LongType(), False),
            StructField("category_code", StringType(), True),
            StructField("user_id", LongType(), False),
            StructField("user_session", StringType(), True)
        ])

    def create_spark_session(self):
        """Spark 세션 및 Delta Lake/Kafka 커넥터 설정"""
        return SparkSession.builder \
            .appName("ClickstreamRealtimeEngine") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", (
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4")) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    # --- [1. 입력: 데이터 로드] ---
    def read_kafka_stream(self, bootstrap_servers, topic):
        """Kafka로부터 실시간 스트림 읽기"""
        return self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
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

    # --- [3. 출력: S3 Delta 저장] ---
    def write_to_s3_delta(self, df):
        """중복 제거 후 S3에 영구 저장"""
        checkpoint_path = f"s3a://{self.s3_bucket}/checkpoints/refined/"
        output_path = f"s3a://{self.s3_bucket}/refined/events/"

        return df.withWatermark("event_time", "10 minutes") \
            .dropDuplicates(["user_id", "event_time", "product_id", "event_type"]) \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("year", "month", "day") \
            .start(output_path)

    # --- [4. 출력: Redis 실시간 및 마커 생성] ---
    def write_to_redis_realtime(self, df):
        """Redis Today 키 업데이트 및 마감 마커 관리"""
        checkpoint_path = f"s3a://{self.s3_bucket}/checkpoints/redis-today/"

        return df.writeStream \
            .foreachBatch(self.save_to_redis_today) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime='1 minute') \
            .start()

    def save_to_redis_today(self, batch_df, batch_id):
        """foreachBatch 내부 실행 로직 (Redis Sync & S3 Marker)"""

        # Redis 증분 업데이트 (Today용)
        def redis_sync(partition):
            r = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
            pipe = r.pipeline()
            for row in partition:
                u_key = f"pref:user:{row['user_id']}:today"
                pipe.hincrbyfloat(u_key, row['category_code'], row['event_weight'])
                pipe.expire(u_key, 86400)

                r_key = f"cat:rank:{row['category_code']}:today"
                pipe.zincrby(r_key, row['event_weight'], str(row['product_id']))
                pipe.expire(r_key, 86400)
            pipe.execute()

        batch_df.foreachPartition(redis_sync)

        # S3 마커 생성 체크
        self.check_and_write_s3_marker(batch_df)

    def check_and_write_s3_marker(self, batch_df):
        """데이터 시간에 따른 마감 마커 파일 생성"""
        max_time_row = batch_df.select(F.max("event_time")).collect()
        if max_time_row and max_time_row[0][0]:
            current_max = max_time_row[0][0]
            yesterday = (current_max - timedelta(days=1)).strftime("%Y-%m-%d")
            safe_threshold = current_max - timedelta(minutes=10)  # 워터마크 고려

            if safe_threshold.date() >= current_max.date():
                s3 = boto3.client('s3')
                marker_key = f"status/streaming_done_{yesterday}.done"
                s3.put_object(Bucket=self.s3_bucket, Key=marker_key, Body='')

    # --- [5. 메인 컨트롤러] ---
    def run(self):
        """전체 파이프라인 가동"""
        print("Initializing E-commerce Real-time Pipeline...")

        # 1. 데이터 소스 연결
        raw_stream = self.read_kafka_stream("localhost:9092", "clickstream")

        # 2. 공통 변환 로직 적용
        enriched_stream = self.transform(raw_stream)

        # 3. 각 Sink(저장소)로 스트리밍 시작
        print("Starting S3 Archiving Stream...")
        s3_query = self.write_to_s3_delta(enriched_stream)

        print("Starting Redis Real-time & Marker Stream...")
        redis_query = self.write_to_redis_realtime(enriched_stream)

        # 4. 모든 쿼리가 종료될 때까지 대기
        self.spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    ClickstreamAnalyzer().run()