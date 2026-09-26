package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.RateSample;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The per-pipeline history of movement samples: one document per sample, appended on a fixed cadence and
 * kept for a bounded time, so that a rate and a delay can be drawn as lines. A pure interface over the
 * sample model in the core ring (rule R2); it exposes the persistence surface only.
 *
 * <p>This is the one store here that is a series and not a latest state. Two things follow. Nothing is
 * ever overwritten: {@link #append} adds and only adds. And nothing is ever trimmed by the writer:
 * samples leave by age, and the adapter behind this port is what makes them leave — a writer that trimmed
 * on its own way in would never trim the history of a pipeline that has stopped writing, which is exactly
 * the pipeline whose history has nobody else to bound it. {@link #retention} says how long a sample is
 * kept; how the adapter enforces it is the adapter's business, behind this port.
 *
 * <p>Reads are by one pipeline and one time range and nothing else. A read across pipelines, or across all
 * time, is a scan of a collection that grows by the minute, and no read face is allowed to depend on one.
 */
public interface RateHistoryStore {

    /** The largest number of samples one store round-trip may return to a caller. */
    int MAX_PAGE_SIZE = 1024;

    /**
     * A stable position in the store ordering. The second component is deliberately opaque outside the
     * adapter: callers may carry it in a signed cursor, but must not infer a database type or expose it.
     */
    record Key(Instant observedAt, String internalKey) {
        public Key {
            Objects.requireNonNull(observedAt, "observedAt");
            Objects.requireNonNull(internalKey, "internalKey");
            if (internalKey.isBlank()) {
                throw new IllegalArgumentException("a rate-history internal key is not blank");
            }
        }
    }

    /** One stored sample together with the opaque key that makes equal timestamps stable. */
    record Entry(Key key, RateSample sample) {
        public Entry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(sample, "sample");
            if (!key.observedAt().equals(sample.observedAt())) {
                throw new IllegalArgumentException("a rate-history key and sample name different instants");
            }
        }
    }

    /** A bounded keyset page, ordered by {@code (observedAt, internalKey)}. */
    record Page(List<Entry> entries, boolean hasMore) {
        public Page {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
            if (entries.size() > MAX_PAGE_SIZE) {
                throw new IllegalArgumentException("a rate-history page exceeds " + MAX_PAGE_SIZE);
            }
            if (hasMore && entries.isEmpty()) {
                throw new IllegalArgumentException("an empty rate-history page cannot have a successor");
            }
        }

        /** The key a following page starts strictly after. */
        public Optional<Key> lastKey() {
            return entries.isEmpty() ? Optional.empty() : Optional.of(entries.get(entries.size() - 1).key());
        }
    }

    /** Null incarnation reads only legacy data; a named one may also include legacy data until TTL. */
    record Visibility(String incarnationId, boolean includeLegacy) {
        public Visibility {
            if (incarnationId == null && !includeLegacy) {
                throw new IllegalArgumentException("legacy-only history visibility includes legacy data");
            }
            if (incarnationId != null && incarnationId.isBlank()) {
                throw new IllegalArgumentException("history incarnation must not be blank");
            }
        }
    }

    /** Adds one sample. Never overwrites: a second sample at the same instant is a second document. */
    void append(RateSample sample);

    /** Appends one new-run sample with its internal owner; legacy samples retain no owner. */
    default void appendScoped(RateSample sample, ObservationStore.Scope scope) {
        throw new UnsupportedOperationException("scoped rate-history writes are unavailable");
    }

    /**
     * Reads at most {@code limit} samples of one pipeline in {@code [from, to)}, oldest first and with
     * equal timestamps ordered by their opaque internal key. When {@code after} is present, the page
     * starts strictly after that key. Implementations read one extra document to decide {@link Page#hasMore}
     * but never return more than {@link #MAX_PAGE_SIZE} entries.
     */
    Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit);

    /** Reads this incarnation's samples, optionally retaining upgrade-era unscoped samples. */
    default Page readPageVisible(String pipelineId, Visibility visibility,
            Instant from, Instant to, Key after, int limit) {
        throw new UnsupportedOperationException("scoped rate-history reads are unavailable");
    }

    /** Reads one exact opaque key when it still exists, for continuity across an external page. */
    Optional<Entry> read(String pipelineId, Key key);

    default Optional<Entry> readVisible(String pipelineId, Visibility visibility, Key key) {
        throw new UnsupportedOperationException("scoped rate-history key reads are unavailable");
    }

    /** The last sample strictly before {@code at}, used only as the left boundary of a rate interval. */
    Optional<Entry> predecessor(String pipelineId, Instant at);

    default Optional<Entry> predecessorVisible(String pipelineId, Visibility visibility, Instant at) {
        throw new UnsupportedOperationException("scoped rate-history predecessor reads are unavailable");
    }

    /** The first sample at or after {@code at}, used only as the right boundary of an aggregate interval. */
    Optional<Entry> successor(String pipelineId, Instant at);

    default Optional<Entry> successorVisible(String pipelineId, Visibility visibility, Instant at) {
        throw new UnsupportedOperationException("scoped rate-history successor reads are unavailable");
    }

    /**
     * Removes every sample of {@code pipelineId}, so a pipeline that no longer exists leaves no history.
     * Removing nothing is a no-op, not an error, for the reason the observation store's delete is one.
     */
    void deleteAll(String pipelineId);

    /** Deletes only the old incarnation captured when the artifact was removed. */
    default void deleteIncarnation(String pipelineId, String incarnationId) {
        throw new UnsupportedOperationException("scoped rate-history cleanup is unavailable");
    }

    /** Deletes only documents from before internal run identity was assigned. */
    default void deleteLegacy(String pipelineId) {
        throw new UnsupportedOperationException("legacy rate-history cleanup is unavailable");
    }

    /** How long a sample is kept before the store lets it go. */
    Duration retention();
}
