package io.tapstate.runtime.engine;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Samples stored namespace counts without making an observation wait for the store. */
public final class StoredCountSampler implements AutoCloseable {

    private static final Duration DEFAULT_REFRESH_AFTER = Duration.ofSeconds(15);
    private static final Duration DEFAULT_EXPIRE_AFTER = Duration.ofSeconds(30);
    private static final Duration DEFAULT_RETRY_AFTER = Duration.ofSeconds(5);
    private static final int DEFAULT_MAX_KEYS = 4096;
    private static final int DEFAULT_QUEUE_CAPACITY = 64;
    private static final int WORKERS = 2;

    @FunctionalInterface
    public interface Counter {
        long count(String database, String namespace);
    }

    public record Sample(long value, Instant observedAt) {}

    public record Health(long completed, long failed, long rejected, int queued, int active,
                         long totalDurationNanos) {}

    private record Key(String pipelineId, long jobId, String database, String namespace) {}

    private static final class Entry {
        private Sample sample;
        private boolean inFlight;
        private Instant retryAt;
        private Runnable task;
    }

    private final Counter counter;
    private final Clock clock;
    private final int maxKeys;
    private final Duration refreshAfter;
    private final Duration expireAfter;
    private final Duration retryAfter;
    private final ThreadPoolExecutor workers;
    private final Map<Key, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong totalDurationNanos = new AtomicLong();
    private Instant capacityRetryAt;
    private boolean closed;

    public StoredCountSampler(Counter counter, Clock clock) {
        this(counter, clock, DEFAULT_MAX_KEYS, DEFAULT_QUEUE_CAPACITY,
                DEFAULT_REFRESH_AFTER, DEFAULT_EXPIRE_AFTER, DEFAULT_RETRY_AFTER);
    }

    StoredCountSampler(Counter counter, Clock clock, int maxKeys, int queueCapacity,
                       Duration refreshAfter, Duration expireAfter, Duration retryAfter) {
        this.counter = Objects.requireNonNull(counter, "counter");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxKeys < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("Sampler capacities must be positive");
        }
        this.maxKeys = maxKeys;
        this.refreshAfter = positive(refreshAfter, "refreshAfter");
        this.expireAfter = positive(expireAfter, "expireAfter");
        this.retryAfter = positive(retryAfter, "retryAfter");
        if (refreshAfter.compareTo(expireAfter) >= 0) {
            throw new IllegalArgumentException("Refresh must precede expiry");
        }
        AtomicLong threadNumber = new AtomicLong();
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task, "stored-count-sampler-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        workers = new ThreadPoolExecutor(WORKERS, WORKERS, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), threadFactory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    /** Returns only a completed count younger than the expiry, and requests refresh when due. */
    public synchronized Optional<Sample> sample(String pipelineId, long jobId,
                                                 String database, String namespace) {
        Key key = new Key(Objects.requireNonNull(pipelineId, "pipelineId"), jobId,
                Objects.requireNonNull(database, "database"),
                Objects.requireNonNull(namespace, "namespace"));
        if (closed) {
            return Optional.empty();
        }
        Instant now = clock.instant();
        Entry entry = entries.get(key);
        if (entry == null) {
            if (capacityRetryAt != null && now.isBefore(capacityRetryAt)) {
                return Optional.empty();
            }
            if (entries.size() >= maxKeys && !evictCompletedEntry()) {
                rejected.incrementAndGet();
                capacityRetryAt = now.plus(retryAfter);
                return Optional.empty();
            }
            entry = new Entry();
            entries.put(key, entry);
        }
        Sample current = entry.sample;
        if (current != null && !fresh(current, now)) {
            entry.sample = null;
            current = null;
        }
        if (!entry.inFlight && (current == null || age(current, now).compareTo(refreshAfter) >= 0)
                && (entry.retryAt == null || !now.isBefore(entry.retryAt))) {
            refresh(key, entry, now);
        }
        return Optional.ofNullable(entry.sample);
    }

    private Duration age(Sample sample, Instant now) {
        return Duration.between(sample.observedAt(), now);
    }

    private boolean fresh(Sample sample, Instant now) {
        Duration age = age(sample, now);
        return !age.isNegative() && age.compareTo(expireAfter) < 0;
    }

    private boolean evictCompletedEntry() {
        Iterator<Map.Entry<Key, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            if (!iterator.next().getValue().inFlight) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    private void refresh(Key key, Entry entry, Instant now) {
        entry.inFlight = true;
        Runnable task = () -> count(key, entry);
        entry.task = task;
        try {
            workers.execute(task);
        } catch (RejectedExecutionException rejection) {
            entry.inFlight = false;
            entry.task = null;
            entry.retryAt = now.plus(retryAfter);
            rejected.incrementAndGet();
        }
    }

    private void count(Key key, Entry entry) {
        long started = System.nanoTime();
        long value = 0L;
        Throwable failure = null;
        try {
            value = counter.count(key.database(), key.namespace());
            if (value < 0L) {
                throw new IllegalStateException("Stored count cannot be negative");
            }
        } catch (Throwable thrown) {
            failure = thrown;
        } finally {
            long duration = Math.max(0L, System.nanoTime() - started);
            synchronized (this) {
                if (entries.get(key) == entry && !closed) {
                    entry.inFlight = false;
                    entry.task = null;
                    if (failure == null) {
                        entry.sample = new Sample(value, clock.instant());
                        entry.retryAt = null;
                    } else {
                        entry.retryAt = clock.instant().plus(retryAfter);
                    }
                }
            }
            totalDurationNanos.addAndGet(duration);
            if (failure == null) {
                completed.incrementAndGet();
            } else {
                failed.incrementAndGet();
            }
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    /** Discards every completed or pending sample owned by this pipeline. */
    public synchronized void forget(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Iterator<Map.Entry<Key, Entry>> iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Key, Entry> item = iterator.next();
            if (item.getKey().pipelineId().equals(pipelineId)) {
                Runnable task = item.getValue().task;
                if (task != null) {
                    workers.remove(task);
                }
                iterator.remove();
            }
        }
    }

    public Health health() {
        return new Health(completed.get(), failed.get(), rejected.get(),
                workers.getQueue().size(), workers.getActiveCount(), totalDurationNanos.get());
    }

    int retainedKeys() {
        synchronized (this) {
            return entries.size();
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            entries.clear();
        }
        workers.shutdownNow();
    }
}
