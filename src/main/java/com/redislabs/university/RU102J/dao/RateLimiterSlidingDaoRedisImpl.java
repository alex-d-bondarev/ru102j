package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        String redisKey = RedisSchema.getSlidingRateLimiterKey(name, windowSizeMS, maxHits);
        try (Jedis jedis = jedisPool.getResource()) {
            long timestamp = System.currentTimeMillis();
            int randNum = (int)(Math.random() * (99) + 1);
            String id = String.format("%d-%d", timestamp, randNum);

            try (Transaction jedisT = jedis.multi()) {
                jedisT.zadd(redisKey, timestamp, id);
                jedisT.zremrangeByScore(redisKey, 0, timestamp - windowSizeMS);
                Response<Long> length = jedisT.zcard(redisKey);

                jedisT.exec();

                if (length.get() > maxHits) {
                    throw new RateLimitExceededException();
                }
            }
        }
    }
}
