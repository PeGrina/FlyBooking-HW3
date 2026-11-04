import logging
import os

from google.protobuf.message import DecodeError
from redis import RedisError
from redis.sentinel import Sentinel


log = logging.getLogger(__name__)


class Cache:
    def __init__(self):
        self.ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))
        if not 300 <= self.ttl <= 600:
            raise ValueError("CACHE_TTL_SECONDS must be between 300 and 600")
        addresses = os.getenv("REDIS_SENTINELS", "redis-sentinel-1:26379").split(",")
        sentinels = [(host, int(port)) for host, port in (value.strip().rsplit(":", 1) for value in addresses)]
        sentinel = Sentinel(
            sentinels,
            socket_timeout=0.3,
            socket_connect_timeout=0.3,
            sentinel_kwargs={"socket_timeout": 0.3, "socket_connect_timeout": 0.3},
        )
        self.redis = sentinel.master_for(os.getenv("REDIS_MASTER_NAME", "mymaster"))

    def get(self, key, message_type):
        try:
            value = self.redis.get(key)
            if value is not None:
                result = message_type.FromString(value)
                log.info("cache hit %s", key)
                return result
        except (RedisError, DecodeError) as error:
            log.warning("cache read failed: %s", error)
        log.info("cache miss %s", key)
        return None

    def put(self, key, message):
        try:
            self.redis.set(key, message.SerializeToString(), ex=self.ttl)
        except RedisError as error:
            log.warning("cache write failed: %s", error)

    def invalidate(self, version):
        try:
            for key in self.redis.scan_iter(match=f"*:v{version}", count=100):
                self.redis.delete(key)
        except RedisError as error:
            log.warning("cache invalidation failed: %s", error)
