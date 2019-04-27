import json
from redis import Redis

class Cache(object):

    def __init__(self, caching_time=60 * 12):
        self.redis = Redis(host='redis')
        self.caching_time = caching_time
    
    def set(self, key, data):
        self.redis.set(key, json.dumps(data))
        self.redis.expire(key, self.caching_time)
    
    def get(self, key):
        cache = self.redis.get(key)
        if cache:
            cache = json.loads(cache.decode())
        return cache
            
