import redis
from dotenv import load_dotenv, dotenv_values
import os

load_dotenv()

env_config = dotenv_values(".env")
redis_end = env_config.get('REDIS_END') or os.environ["REDIS_END"]
redis_port = env_config.get('REDIS_PORT') or os.environ["REDIS_PORT"]
redis_password = env_config.get('REDIS_PASSWORD') or os.environ["REDIS_PASSWORD"]

# Create a connection pool
redis_pool = redis.ConnectionPool(
    host=redis_end,
    port=redis_port,
    password=redis_password,
    db=0,
    decode_responses=True
)

def get_redis_connection():
    return redis.Redis(connection_pool=redis_pool)
