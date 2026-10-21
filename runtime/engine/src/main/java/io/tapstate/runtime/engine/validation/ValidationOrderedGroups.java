package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Consumes a natively ordered read into complete equal-key groups. It retains one bounded sample,
 * counts every occurrence and never sorts or treats an empty batch as EOF. The owner must reserve
 * two sample rows plus their key/identity metadata before construction: the returned group and one
 * lookahead can coexist after their delivery-row leases have closed. Those retained values remain
 * charged to that reservation, not to the upstream delivery slots.
 *
 * <p>Use next and group access from one admitted blocking-I/O consumer. close may cancel that read
 * from another I/O thread. A group must close before the consumer advances.
 */
public final class ValidationOrderedGroups implements AutoCloseable {
    public record Key(ValidationKey comparison, ValidationPhysicalIdentity physical) {
        public Key {
            Objects.requireNonNull(comparison, "comparison");
            Objects.requireNonNull(physical, "physical");
        }
    }

    private record Sample(Key key, ValidationValues.Row values) {}

    private final String readId;
    private final ValidationRowCursor input;
    private final Function<ValidationValues.Row, Key> project;
    private final long maxSampleBytes;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Sample lookahead;
    private volatile Group outstanding;
    private volatile ValidationKey previous;
    private long expectedOrdinal;
    private boolean eof;
    private Throwable failure;

    public ValidationOrderedGroups(String readId, ValidationRowCursor input,
            Function<ValidationValues.Row, Key> project, long maxSampleBytes) {
        this.readId = Objects.requireNonNull(readId, "readId");
        this.input = Objects.requireNonNull(input, "input");
        this.project = Objects.requireNonNull(project, "project");
        if (readId.isBlank() || maxSampleBytes < 1 || maxSampleBytes > 32L * 1024 * 1024) {
            throw new IllegalArgumentException("invalid ordered-group admission");
        }
        this.maxSampleBytes = maxSampleBytes;
    }

    public Optional<Group> next() {
        try {
            checkActive();
            if (outstanding != null && !outstanding.isClosed()) {
                throw new IllegalStateException("close the current validation group before advancing");
            }
            outstanding = null;
            Sample first = lookahead;
            lookahead = null;
            if (first == null) first = readOne();
            if (first == null) return Optional.empty();
            long occurrences = 1;
            while (true) {
                Sample following = readOne();
                if (following == null) break;
                if (first.key().comparison().compareTo(following.key().comparison()) != 0) {
                    lookahead = following;
                    break;
                }
                if (occurrences == Long.MAX_VALUE) throw limit("groupOccurrences");
                occurrences++;
            }
            checkActive();
            Group group = new Group(first, occurrences);
            outstanding = group;
            if (closed.get()) {
                group.close();
                checkActive();
            }
            return Optional.of(group);
        } catch (RuntimeException | Error stopped) {
            if (failure == null) failure = stopped;
            // close may have won while projection was in flight; discard ownership published late.
            lookahead = null;
            previous = null;
            Group leased = outstanding;
            if (leased != null) leased.close();
            try { close(); }
            catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throwFailure();
            throw stopped;
        }
    }

    private Sample readOne() {
        checkActive();
        if (eof) return null;
        Optional<ValidationRowCursor.Row> delivered = Objects.requireNonNull(input.next(), "row result");
        if (delivered.isEmpty()) {
            eof = true;
            return null;
        }
        try (var row = delivered.orElseThrow()) {
            checkActive();
            if (expectedOrdinal == Long.MAX_VALUE || row.ordinal() != expectedOrdinal++) throw protocol();
            ValidationValues.Row values = Objects.requireNonNull(row.values(), "row values");
            if (values.accountedBytes() > maxSampleBytes) throw limit("groupSampleBytes");
            Key key = Objects.requireNonNull(project.apply(values), "projected key");
            ValidationKey prior = previous;
            if (prior != null && prior.compareTo(key.comparison()) > 0) throw protocol();
            previous = key.comparison();
            return new Sample(key, values);
        }
    }

    private void checkActive() {
        throwFailure();
        if (closed.get()) throw new TapstateException(ValidationError.READ_CANCELLED, Map.of("table", readId), null);
    }

    private void throwFailure() {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }

    private TapstateException protocol() {
        return new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", readId), null);
    }

    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Group group = outstanding;
        if (group != null) group.close();
        lookahead = null;
        previous = null;
        input.close();
    }

    /** Only the first sample is retained; multiplicity covers the entire group, including all batches. */
    public static final class Group implements AutoCloseable {
        private final AtomicReference<Sample> sample;
        private final long occurrences;

        private Group(Sample sample, long occurrences) {
            this.sample = new AtomicReference<>(sample);
            this.occurrences = occurrences;
        }

        public Key key() { return sample().key(); }
        public ValidationValues.Row sampleValues() { return sample().values(); }
        public long occurrences() { sample(); return occurrences; }
        public boolean isClosed() { return sample.get() == null; }

        private Sample sample() {
            Sample retained = sample.get();
            if (retained == null) throw new IllegalStateException("validation group has been released");
            return retained;
        }

        @Override public void close() { sample.set(null); }
    }
}
