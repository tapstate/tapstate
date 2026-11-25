package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Thread-confined cardinality facts from one complete relation read, after independent participation
 * rules. This is not the COUNT validation method. Indeterminate means a known, separately retained
 * source data issue; I/O, unsupported rules and authorization failures must abort the read instead.
 */
public final class ValidationRelationRows implements AutoCloseable {
    public enum Disposition { INCLUDED, EXCLUDED, INDETERMINATE }
    public record Limits(long bytes, ValidationValues.Limits row) {
        public Limits {
            if (bytes < 1 || bytes > 32L * 1024 * 1024) throw ValidationError.invalid("relationRows.limits");
            Objects.requireNonNull(row, "row");
        }
    }

    /** Raw read facts and current rule output stay separate; unavailable physical keys are not invented. */
    public record Sample(ValidationValues.Row original, ValidationValues.Row working,
                         Optional<ValidationPhysicalIdentity> physical) {
        public Sample {
            Objects.requireNonNull(original, "original");
            Objects.requireNonNull(working, "working");
            Objects.requireNonNull(physical, "physical");
        }
        public long accountedBytes() {
            return 128L + original.accountedBytes() + (original == working ? 0 : working.accountedBytes())
                    + physical.map(ValidationPhysicalIdentity::retainedBytes).orElse(0L);
        }
    }

    public static final class Result {
        private final ValidationObservation observation;
        private final ValidationCheckEvent.ReadBoundary read;
        private final long included;
        private final long excluded;
        private final long indeterminate;
        private final List<Sample> includedSamples;
        private final List<Sample> indeterminateSamples;
        private final long accountedBytes;
        private Result(ValidationRelationRows rows) {
            observation = rows.observation;
            read = rows.boundary;
            included = rows.included;
            excluded = rows.excluded;
            indeterminate = rows.indeterminate;
            includedSamples = List.copyOf(rows.includedSamples);
            indeterminateSamples = List.copyOf(rows.indeterminateSamples);
            accountedBytes = rows.bytes;
        }
        public ValidationObservation observation() { return observation; }
        public ValidationCheckEvent.ReadBoundary read() { return read; }
        public long included() { return included; }
        public long excluded() { return excluded; }
        public long indeterminate() { return indeterminate; }
        public List<Sample> includedSamples() { return includedSamples; }
        public List<Sample> indeterminateSamples() { return indeterminateSamples; }
        public long accountedBytes() { return accountedBytes; }
        public boolean participationComplete() { return indeterminate == 0; }
        public boolean definitelyEmpty() { return participationComplete() && included == 0; }
        public boolean atLeastTwo() { return included >= 2; }
        public Optional<Sample> unique() {
            return participationComplete() && included == 1 ? Optional.of(includedSamples.getFirst()) : Optional.empty();
        }
    }

    private final ValidationObservation observation;
    private final String readId;
    private final Limits limits;
    private final ArrayList<Sample> includedSamples = new ArrayList<>(2);
    private final ArrayList<Sample> indeterminateSamples = new ArrayList<>(2);
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private ValidationCheckEvent.ReadBoundary boundary;
    private long total;
    private long included;
    private long excluded;
    private long indeterminate;
    private long bytes;
    private boolean closed;

    public ValidationRelationRows(ValidationObservation observation, String readId, Limits limits) {
        this.observation = Objects.requireNonNull(observation, "observation");
        this.readId = ValidationError.identifier(readId, "relationRows.readId");
        this.limits = Objects.requireNonNull(limits, "limits");
        charge(initialReservationBytes(this.observation, this.readId));
    }

    /** Reserves only staging metadata; every retained original/working sample is charged separately. */
    public static long initialReservationBytes(ValidationObservation observation, String readId) {
        Objects.requireNonNull(observation, "observation"); ValidationError.identifier(readId, "relationRows.readId");
        long metadata = 16 * 1024L + 32L + 2L * readId.length() + observation.root().rootKey().retainedBytes();
        for (ValidationRoot.Ancestor ancestor : observation.root().ancestors()) {
            metadata += 64L + ancestor.key().retainedBytes();
            for (String segment : ancestor.fieldPath()) metadata += 40L + 2L * segment.length();
        }
        return metadata;
    }

    /** Every returned row receives an explicit disposition, including uncertain participation. */
    public void accept(ValidationObservation request, String requestId, Disposition disposition, Sample sample) {
        mutate(() -> {
            sameRequest(request, requestId);
            Objects.requireNonNull(disposition, "disposition");
            Objects.requireNonNull(sample, "sample");
            if (boundary != null) throw ValidationError.invalid("relationRows.readComplete");
            bounded(sample.original()); bounded(sample.working());
            if (total == Long.MAX_VALUE) throw limit("occurrences");
            total++;
            switch (disposition) {
                case EXCLUDED -> excluded++;
                case INCLUDED -> { included++; retain(includedSamples, sample); }
                case INDETERMINATE -> { indeterminate++; retain(indeterminateSamples, sample); }
            }
        });
    }

    /** Normal whole-query completion only; a callback batch, preview cap or cancellation is insufficient. */
    public void complete(ValidationObservation request, ValidationCheckEvent.ReadBoundary completed) {
        mutate(() -> {
            Objects.requireNonNull(completed, "completed");
            sameRequest(request, completed.readId());
            if (boundary != null) {
                if (!boundary.equals(completed)) throw ValidationError.invalid("relationRows.readBoundary");
                return;
            }
            charge(128L + 2L * completed.contractVersion().length());
            boundary = completed;
        });
    }

    public Result finish() {
        ensureOpen();
        try {
            if (boundary == null) throw ValidationError.invalid("relationRows.incomplete");
            Result result = new Result(this);
            close();
            return result;
        } catch (RuntimeException | Error error) { fail(error); throw error; }
    }
    private void retain(ArrayList<Sample> samples, Sample sample) {
        if (samples.size() == 2) return;
        charge(sample.accountedBytes());
        samples.add(sample);
    }
    private void bounded(ValidationValues.Row row) {
        if (row.accountedBytes() > limits.row().bytes() || row.nodes() > limits.row().nodes()
                || row.depth() > limits.row().depth()) throw limit("row");
    }
    private void sameRequest(ValidationObservation request, String requestId) {
        if (!observation.equals(request) || !readId.equals(requestId)) throw ValidationError.invalid("relationRows.request");
    }
    private void mutate(Runnable action) {
        ensureOpen();
        try { action.run(); }
        catch (RuntimeException | Error error) { fail(error); throw error; }
    }
    private void charge(long amount) {
        if (amount > limits.bytes() - bytes) throw limit("bytes");
        bytes += amount;
    }
    private void ensureOpen() {
        Throwable cause = failure.get();
        if (cause instanceof Error error) throw error;
        if (cause instanceof RuntimeException error) throw error;
        if (closed) throw new IllegalStateException("Relation row staging is closed");
    }
    private void fail(Throwable cause) { failure.record(cause); close(); }
    @Override public void close() {
        closed = true;
        includedSamples.clear(); indeterminateSamples.clear(); boundary = null;
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "relationRows." + dimension), null);
    }
}
