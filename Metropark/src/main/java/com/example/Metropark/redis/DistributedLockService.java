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

/**
 * A mutual-exclusion lock held in Redis, so it holds across every instance of
 * this application rather than only within one JVM.
 *
 * <p>
 * Used by {@link com.example.Metropark.camera.consumer.ParkingCameraConsumer} to
 * serialise work per number plate. A real ANPR camera reads the same car several
 * times as it rolls past, and RabbitMQ redelivers on top of that; without this,
 * two deliveries for one plate would each allocate a slot and open a session,
 * and the second would violate {@code unique_active_session_per_vehicle}.
 */
@Service
public class DistributedLockService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLockService.class);

    private static final String PLATE_LOCK_PREFIX = "lock:plate:";
    private static final String SLOT_CLAIM_PREFIX = "claim:slot:";

    /**
     * A slot claim only has to outlive the entry that took it. It is released
     * explicitly on both paths; the TTL exists purely so a consumer killed between
     * claiming and parking cannot strand a slot.
     */
    private static final Duration SLOT_CLAIM_TTL = Duration.ofSeconds(30);

    /**
     * Long enough to cover an entry or exit (Redis writes plus a publish), short
     * enough that a consumer killed mid-hold frees the plate on its own. Without a
     * TTL a crash would block that plate forever.
     */
    private static final Duration DEFAULT_TTL = Duration.ofSeconds(30);

    /**
     * Release must be compare-and-delete, not a bare DEL. If a holder overruns the
     * TTL, the lock is already someone else's; deleting it unconditionally would
     * free a lock this caller no longer owns and let a third party in alongside the
     * new holder.
     */
    private static final RedisScript<Long> RELEASE_IF_OWNED = RedisScript.of(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final ReactiveRedisTemplate<String, Object> redisTemplate;

    public DistributedLockService(ReactiveRedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * A held lock: the key it occupies and the token proving who owns it.
     */
    public record Lock(String key, String token) {
    }

    public Mono<Lock> acquirePlateLock(String licensePlate) {
        return acquire(PLATE_LOCK_PREFIX + licensePlate, DEFAULT_TTL);
    }

    /**
     * Claims one parking slot so two concurrent entries cannot be handed the same
     * one.
     *
     * <p>
     * Needed on top of the slot's own status because that status only turns
     * OCCUPIED once the entry has been written; between choosing a slot and writing
     * it, the slot still reads AVAILABLE to everybody else. The claim closes that
     * window.
     */
    public Mono<Lock> claimSlot(Integer slotId) {
        return acquire(SLOT_CLAIM_PREFIX + slotId, SLOT_CLAIM_TTL);
    }

    /**
     * Completes with the lock when it was taken, and EMPTY when it was already
     * held. Empty rather than an error: losing the race is the normal, expected
     * outcome for a duplicate camera read, not a failure to report.
     */
    public Mono<Lock> acquire(String key, Duration ttl) {
        String token = UUID.randomUUID().toString();

        return redisTemplate.opsForValue()
                .setIfAbsent(key, token, ttl)
                .flatMap(acquired -> Boolean.TRUE.equals(acquired)
                        ? Mono.just(new Lock(key, token))
                        : Mono.empty())
                .doOnNext(lock -> LOGGER.debug("Acquired lock {}", key))
                .onErrorResume(error -> {
                    // Redis being unreachable must not be mistaken for "someone else
                    // holds it" - that would silently drop the event instead of
                    // letting the consumer dead letter it.
                    LOGGER.error("Error acquiring lock {}", key, error);
                    return Mono.error(error);
                });
    }

    /**
     * Best effort by design: the lock's TTL is the real guarantee, so a failed
     * release is logged and swallowed rather than turned into a failure of the work
     * that already succeeded.
     */
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
