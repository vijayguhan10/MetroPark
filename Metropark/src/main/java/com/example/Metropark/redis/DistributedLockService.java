package com.example.Metropark.redis;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Mono;

@Service
public class DistributedLockService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLockService.class);

    private static final String PLATE_LOCK_PREFIX = "lock:plate:";
    private static final String SLOT_CLAIM_PREFIX = "claim:slot:";

    private static final Duration SLOT_CLAIM_TTL = Duration.ofSeconds(30);

    private static final Duration DEFAULT_TTL = Duration.ofSeconds(30);

    private static final RedisScript<Long> RELEASE_IF_OWNED = RedisScript.of(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final ReactiveRedisTemplate<String, Object> redisTemplate;

    public DistributedLockService(ReactiveRedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public record Lock(String key, String token) {
    }

    public Mono<Lock> acquirePlateLock(String licensePlate) {
        return acquire(PLATE_LOCK_PREFIX + licensePlate, DEFAULT_TTL);
    }

    public Mono<Lock> claimSlot(Integer slotId) {
        return acquire(SLOT_CLAIM_PREFIX + slotId, SLOT_CLAIM_TTL);
    }

    public Mono<Lock> acquire(String key, Duration ttl) {
        String token = UUID.randomUUID().toString();

        return redisTemplate.opsForValue()
                .setIfAbsent(key, token, ttl)
                .flatMap(acquired -> Boolean.TRUE.equals(acquired)
                        ? Mono.just(new Lock(key, token))
                        : Mono.empty())
                .doOnNext(lock -> LOGGER.debug("Acquired lock {}", key))
                .onErrorResume(error -> {
                    LOGGER.error("Error acquiring lock {}", key, error);
                    return Mono.error(error);
                });
    }

    public Mono<Void> release(Lock lock) {
        if (lock == null) {
            return Mono.empty();
        }

        return redisTemplate
                .execute(RELEASE_IF_OWNED, List.of(lock.key()), List.of(lock.token()))
                .next()
                .doOnNext(released -> {
                    if (released == null || released == 0L) {
                        LOGGER.warn("Lock {} had already expired before release", lock.key());
                    }
                })
                .onErrorResume(error -> {
                    LOGGER.error("Error releasing lock {}; it will expire on its own", lock.key(), error);
                    return Mono.empty();
                })
                .then();
    }
}
