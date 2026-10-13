package com.mediator.s3gateway.repository;

import java.time.Duration;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class CachedMediatorObjectLookup {

    private final MediatorObjectRepository repository;
    private final Cache<ObjectKey, MediatorObjectRepository.DatabaseObject> cache;
    private static final Logger log = LoggerFactory.getLogger(CachedMediatorObjectLookup.class);

    public CachedMediatorObjectLookup(
            MediatorObjectRepository repository,
            @Value("${gateway.object-cache.ttl-seconds:60}") long ttlSeconds,
            @Value("${gateway.object-cache.maximum-size:10000}") long maximumSize) {

        this.repository = repository;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterAccess(Duration.ofSeconds(ttlSeconds))
                .build();
    }

    public Optional<MediatorObjectRepository.DatabaseObject> find(
            String bucket,
            String key) {

        ObjectKey cacheKey = new ObjectKey(bucket, key);

        MediatorObjectRepository.DatabaseObject cached = cache.getIfPresent(cacheKey);

        if (cached != null) {
            log.debug(
                    "Manager object cache HIT: bucket={}, key={}",
                    bucket,
                    key);

            return Optional.of(cached);
        }
        log.debug(
                "Manager object cache MISS: bucket={}, key={}",
                bucket,
                key);
        Optional<MediatorObjectRepository.DatabaseObject> loaded = repository.find(bucket, key);

        loaded.ifPresent(value -> {
            cache.put(cacheKey, value);

            log.debug(
                    "Manager object cache STORED: bucket={}, key={}",
                    bucket,
                    key);
        });

        return loaded;
    }

    public void invalidate(String bucket, String key) {
        cache.invalidate(new ObjectKey(bucket, key));

        log.debug(
                "Manager object cache INVALIDATED: bucket={}, key={}",
                bucket,
                key);
    }

    private record ObjectKey(String bucket, String key) {
    }
}