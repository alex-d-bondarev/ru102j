package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Coordinate;
import com.redislabs.university.RU102J.api.GeoQuery;
import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.*;

import java.util.*;
import java.util.stream.Collectors;

public class SiteGeoDaoRedisImpl implements SiteGeoDao {
    private JedisPool jedisPool;
    final static private Double capacityThreshold = 0.2;

    public SiteGeoDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Site findById(long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> fields =
                    jedis.hgetAll(RedisSchema.getSiteHashKey(id));
            if (fields == null || fields.isEmpty()) {
                return null;
            }
            return new Site(fields);
        }
    }

    @Override
    public Set<Site> findAll() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.zrange(RedisSchema.getSiteGeoKey(), 0, -1);

            Pipeline p = jedis.pipelined();
            Set<Site> sites = new HashSet<>(keys.size());
            List<Response<Map<String, String>>> pendingSites = new ArrayList<>();

            for (String key : keys) {
                pendingSites.add(p.hgetAll(key));
            }

            p.sync();

            for (Response<Map<String, String>> site : pendingSites){
                if (!site.get().isEmpty()) {
                    sites.add(new Site(site.get()));
                }
            }

            return sites;
        }
    }

    @Override
    public Set<Site> findByGeo(GeoQuery query) {
        if (query.onlyExcessCapacity()) {
            return findSitesByGeoWithCapacity(query);
        } else {
            return findSitesByGeo(query);
        }
    }

    private Set<Site> findSitesByGeoWithCapacity(GeoQuery query) {
        Set<Site> results = new HashSet<>();
        Coordinate coord = query.getCoordinate();
        Double radius = query.getRadius();
        GeoUnit radiusUnit = query.getRadiusUnit();

         try (Jedis jedis = jedisPool.getResource()) {
             List<GeoRadiusResponse> radiusResponses = jedis.georadius(
                     RedisSchema.getSiteGeoKey(), coord.getLng(), coord.getLat(), radius, radiusUnit);

             Set<Site> sites = radiusResponses.stream()
                     .map(response -> jedis.hgetAll(response.getMemberByString()))
                     .filter(Objects::nonNull)
                     .map(Site::new).collect(Collectors.toSet());

             Pipeline pipeline = jedis.pipelined();
             Map<Long, Response<Double>> scores = new HashMap<>(sites.size());
             String rankingKey = RedisSchema.getCapacityRankingKey();

             for (Site site : sites) {
                 scores.put(
                         site.getId(),
                         pipeline.zscore(rankingKey, String.valueOf(site.getId())));
             }

             pipeline.sync();

             for (Site site : sites) {
                 if (scores.get(site.getId()).get() >= capacityThreshold) {
                     results.add(site);
                 }
             }
         }

         return results;
    }

    private Set<Site> findSitesByGeo(GeoQuery query) {
        Coordinate coord = query.getCoordinate();
        Double radius = query.getRadius();
        GeoUnit radiusUnit = query.getRadiusUnit();

        try (Jedis jedis = jedisPool.getResource()) {
            List<GeoRadiusResponse> radiusResponses =
                    jedis.georadius(RedisSchema.getSiteGeoKey(), coord.getLng(),
                            coord.getLat(), radius, radiusUnit);

            return radiusResponses.stream()
                    .map(response -> jedis.hgetAll(response.getMemberByString()))
                    .filter(Objects::nonNull)
                    .map(Site::new).collect(Collectors.toSet());
        }
    }

    @Override
    public void insert(Site site) {
         try (Jedis jedis = jedisPool.getResource()) {
             String key = RedisSchema.getSiteHashKey(site.getId());
             jedis.hmset(key, site.toMap());

             if (site.getCoordinate() == null) {
                 throw new IllegalArgumentException("Coordinate required for Geo " +
                         "insert.");
             }
             Double longitude = site.getCoordinate().getGeoCoordinate().getLongitude();
             Double latitude = site.getCoordinate().getGeoCoordinate().getLatitude();
             jedis.geoadd(RedisSchema.getSiteGeoKey(), longitude, latitude,
                     key);
         }
    }
}
