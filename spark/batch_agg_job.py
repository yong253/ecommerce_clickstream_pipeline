import sys
import redis
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Redis 저장 로직
def save_to_redis(partition):
    r = redis.Redis(host='redis', port=6379, db=0)
    pipe = r.pipeline()
    for row in partition:
        if 'user_id' in row: # 유저 선호도
            key = f"pref:user:{row['user_id']}:history"
            pipe.hset(key, row['category_code'], row['final_score'])
            pipe.expire(key, 86400 * 2) # 2일 유지
        else: # 상품 순위
            key = f"cat:rank:{row['category_code']}:yesterday"
            pipe.zadd(key, {str(row['product_id']): row['daily_score']})
            pipe.expire(key, 86400 * 2)
    pipe.execute()

def run_daily_batch(y, m, d):
    spark = SparkSession.builder \
        .appName(f"Batch_Agg_{y}{m}{d}") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    target_date = datetime(int(y), int(m), int(d))
    d1 = (target_date - timedelta(days=1)).strftime("%Y-%m-%d") # 어제
    d2 = (target_date - timedelta(days=2)).strftime("%Y-%m-%d") # 그저께
    d3 = (target_date - timedelta(days=3)).strftime("%Y-%m-%d") # 3일전

    base_path = "s3a://ecommerce-datalake-yong/refined/events"
    df = spark.read.format("delta").load(base_path) \
        .filter(F.col("event_time").cast("date").isin(d1, d2, d3))

    # --- [1] 유저 선호도 (3일 가중치 감쇄 적용) ---
    # 가중치: d1(1.0), d2(0.7), d3(0.4)
    user_pref = df.withColumn("decay_score",
        F.when(F.col("event_time").cast("date") == d1, F.col("event_weight") * 1.0)
         .when(F.col("event_time").cast("date") == d2, F.col("event_weight") * 0.7)
         .when(F.col("event_time").cast("date") == d3, F.col("event_weight") * 0.4).otherwise(0)
    ).groupBy("user_id", "category_code").agg(F.sum("decay_score").alias("final_score"))

    # --- [2] 카테고리별 상품 순위 (하루 전 데이터만) ---
    prod_rank = df.filter(F.col("event_time").cast("date") == d1) \
        .groupBy("category_code", "product_id").agg(F.sum("event_weight").alias("daily_score"))
    print(f"DEBUG: user_pref count is {user_pref.count()}")
    print(f"DEBUG: prod_rank count is {prod_rank.count()}")
    user_pref.foreachPartition(save_to_redis)
    prod_rank.foreachPartition(save_to_redis)

    # --- [3] Delta Lake 유지보수 (Small Files 합치기) ---
    spark.sql(f"OPTIMIZE delta.`{base_path}` WHERE year='{y}' AND month='{m}' AND day='{d}'")
    spark.stop()

if __name__ == "__main__":
    run_daily_batch(sys.argv[1], sys.argv[2], sys.argv[3])