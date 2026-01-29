from fastapi import FastAPI
import redis
import os
from collections import Counter

app = FastAPI()

# Redis 연결 (도커 네트워크 이름인 'redis' 사용)
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    decode_responses=True
)

@app.get("/user/preference/{user_id}")
def get_user_preference(user_id: str):
    try:
        hist = r.hgetall(f"pref:user:{user_id}:history") or {}
        today = r.hgetall(f"pref:user:{user_id}:today") or {}
        combined = Counter({k: int(float(v)) for k, v in hist.items()})
        combined.update({k: int(float(v)) for k, v in today.items()})
        # 데이터가 없을 경우 기본값 반환
        return sorted(combined.items(), key=lambda x: x[1], reverse=True)[:3]
    except Exception as e:
        return []

@app.get("/categories")
def get_categories():
    try:
        keys = r.keys("cat:rank:*:today")
        cats = list(set([k.split(":")[2] for k in keys]))
        return sorted(cats) if cats else ["electronics", "appliances"]
    except:
        return []

@app.get("/category/rank/{cat}")
def get_rank(cat: str):
    try:
        # 높은 점수 순서대로 5개 (zrevrange)
        data = r.zrevrange(f"cat:rank:{cat}:today", 0, 4, withscores=True)
        return [{"product_id": str(p[0]), "score": int(p[1])} for p in data]
    except:
        return []