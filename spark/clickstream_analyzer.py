from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

'''
1. spark 세션 초기화
2. 스키마 검증 
3. readStream 
4. transform
5. 데이터 S3 저장
6. 데이터 집계
6-1. 카테고리별 인기 상품 추천
6-2. 사용자별 카테고리 선호도 
7. 집계 데이터 Redis에 저장 
'''

class ClickstreamAnalyzer:
    def __init__(self):
        self.spark = self.create_spark_session()


    def create_spark_session(self):
        # Spark 세션 초기화 (S3, Kafka 커넥터 포함)
        return SparkSession.builder \
            .appName("ClickstreamDataAnalyzer") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.5.0") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.shuffle.partitions", "12") \
            .getOrCreate()


# Kafka에서 들어올 원본 JSON 스키마
raw_schema = StructType([
    StructField("event_time", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("category_id", LongType(), False),
    StructField("category_code", StringType(), True),
    StructField("user_id", LongType(), False),
    StructField("user_session", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "latest") \
    .load()

# JSON 파싱 및 구조 확장
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), raw_schema).alias("data")) \
    .select("data.*")


enriched_df = parsed_df \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .withColumn("year", year(col("event_time"))) \
    .withColumn("month", month(col("event_time"))) \
    .withColumn("day", day(col("event_time"))) \
    .withColumn("category_main", split(col("category_code"), "\.").getItem(0)) \
    .withColumn("category_main", coalesce(col("category_main"), lit("accessories"))) \
    .withColumn("event_weight",
        when(col("event_type") == "view", 1)
        .when(col("event_type") == "cart", 3)
        .when(col("event_type") == "purchase", 10)
        .when(col("event_type") == "remove_from_cart", -3)
        .otherwise(0))

s3_query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://my-bucket/silver/enriched_logs/") \
    .option("checkpointLocation", "s3a://my-bucket/checkpoints/silver/") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

import redis


def send_to_redis(batch_df, batch_id):
    # 배치 내에서 동일 유저/카테고리별 합산 (네트워크 부하 감소)
    user_pref = batch_df.groupBy("user_id", "category_main").sum("event_weight")

    # Redis 커넥션 (각 워커 노드에서 실행)
    r = redis.StrictRedis(host='localhost', port=6379, db=0)

    for row in user_pref.collect():
        user_key = f"pref:user:{row['user_id']}:today"
        r.zincrby(user_key, row['sum(event_weight)'], row['category_main'])
        r.expire(user_key, 86400)  # 24시간 후 만료


redis_query = enriched_df.writeStream \
    .foreachBatch(send_to_redis) \
    .option("checkpointLocation", "s3a://my-bucket/checkpoints/gold/") \
    .start()

spark.streams.awaitAnyTermination()