package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Bounded unordered membership facts for one declared array under one exact parent observation.
 * Inputs are independently read, admitted, participating members with explicitly bound identities.
 * Complete reads are required: this is not a root result, a query adapter or a production assembler.
 * Staging is thread-confined; reader callbacks must be marshalled to the same validation processor.
 */
public final class ValidationNestArrayMembers implements AutoCloseable {
    public enum Side { SOURCE, TARGET }
    public enum RowIdentityComparison { MATCHES, DIFFERS, NOT_OBSERVABLE, UNDECIDABLE }
    public record Limits(long bytes, int elementKeys, int rowKeys, ValidationValues.Limits row) {
        public Limits {
            if (bytes < 1 || bytes > 32L * 1024 * 1024 || elementKeys < 1 || elementKeys > 65536
                    || rowKeys < 1 || rowKeys > 65536) throw ValidationError.invalid("nestMembers.limits");
            Objects.requireNonNull(row, "row");
        }
    }

    /** Declared row identity, local array identity and actual physical read identity are distinct. */
    public record Member(Optional<ValidationKey> rowKey, ValidationKey elementKey, ValidationPhysicalIdentity physical,
                         ValidationValues.Row values) {
        public Member {
            Objects.requireNonNull(rowKey, "rowKey");
            Objects.requireNonNull(elementKey, "elementKey");
            Objects.requireNonNull(physical, "physical");
            Objects.requireNonNull(values, "values");
        }
        public Member(ValidationKey rowKey, ValidationKey elementKey, ValidationPhysicalIdentity physical, ValidationValues.Row values) {
            this(Optional.of(rowKey), elementKey, physical, values);
        }
    }

    /** Exact counts after complete reads; at most two members are retained per side and element key. */
    public static final class Group {
        private final ValidationRoot scope;
        private final long sourceCount;
        private final long targetCount;
        private final List<Member> sourceSamples;
        private final List<Member> targetSamples;
        private final boolean sourceConflict;
        private final boolean targetConflict;
        private Group(ValidationRoot scope, long sourceCount, long targetCount, List<Member> sourceSamples,
                      List<Member> targetSamples, boolean sourceConflict, boolean targetConflict) {
            this.scope = scope;
            this.sourceCount = sourceCount;
            this.targetCount = targetCount;
            this.sourceSamples = List.copyOf(sourceSamples);
            this.targetSamples = List.copyOf(targetSamples);
            this.sourceConflict = sourceConflict;
            this.targetConflict = targetConflict;
        }
        public ValidationRoot scope() { return scope; }
        public long sourceCount() { return sourceCount; }
        public long targetCount() { return targetCount; }
        public List<Member> sourceSamples() { return sourceSamples; }
        public List<Member> targetSamples() { return targetSamples; }
        public boolean sourceConflict() { return sourceConflict; }
        public boolean targetConflict() { return targetConflict; }
        public boolean canCompareValues() { return sourceCount == 1 && targetCount == 1 && !sourceConflict && !targetConflict; }
        public RowIdentityComparison rowIdentityComparison() {
            if (!canCompareValues()) return RowIdentityComparison.UNDECIDABLE;
            Optional<ValidationKey> targetKey = targetSamples.getFirst().rowKey();
            if (targetKey.isEmpty()) return RowIdentityComparison.NOT_OBSERVABLE;
            return sourceSamples.getFirst().rowKey().equals(targetKey)
                    ? RowIdentityComparison.MATCHES : RowIdentityComparison.DIFFERS;
        }
    }

    public static final class Result {
        private final ValidationObservation observation;
        private final List<String> arrayPath;
        private final List<Group> groups;
        private final Map<Side, ValidationCheckEvent.ReadBoundary> reads;
        private final long accountedBytes;
        private Result(ValidationObservation observation, List<String> arrayPath, List<Group> groups,
                       Map<Side, ValidationCheckEvent.ReadBoundary> reads, long accountedBytes) {
            this.observation = observation;
            this.arrayPath = List.copyOf(arrayPath);
            this.groups = List.copyOf(groups);
            this.reads = Map.copyOf(reads);
            this.accountedBytes = accountedBytes;
        }
        public ValidationObservation observation() { return observation; }
        public List<String> arrayPath() { return arrayPath; }
        public List<Group> groups() { return groups; }
        public Map<Side, ValidationCheckEvent.ReadBoundary> reads() { return reads; }
        public long accountedBytes() { return accountedBytes; }
    }

    private final ValidationObservation observation;
    private final List<String> path;
    private final Limits limits;
    private final Map<ValidationKey, Bucket> buckets = new LinkedHashMap<>();
    private final Map<ValidationKey, Long> sourceRows = new LinkedHashMap<>();
    private final Map<ValidationKey, Long> targetRows = new LinkedHashMap<>();
    private final EnumMap<Side, ValidationCheckEvent.ReadBoundary> reads = new EnumMap<>(Side.class);
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private ValidationKey elementDomain;
    private ValidationKey sourceRowDomain;
    private ValidationKey targetRowDomain;
    private Boolean targetRowIdentityVisible;
    private long bytes;
    private boolean closed;

    public ValidationNestArrayMembers(ValidationObservation parent, List<String> arrayPath, Limits limits) {
        observation = Objects.requireNonNull(parent, "parent");
        this.limits = Objects.requireNonNull(limits, "limits");
        if (parent.root().ancestors().size() >= 32 || arrayPath == null || arrayPath.isEmpty() || arrayPath.size() > 32) {
            throw ValidationError.invalid("nestMembers.scope");
        }
        for (String segment : arrayPath) ValidationError.identifier(segment, "nestMembers.arrayPath");
        path = List.copyOf(arrayPath);
        charge(16 * 1024L + rootBytes(parent.root()));
        for (String segment : path) charge(textBytes(segment));
    }

    /** The caller may interleave sides; order does not change membership or overwrite duplicates. */
    public void add(Side side, ValidationObservation request, Member member) {
        mutate(() -> {
            sameObservation(request);
            Objects.requireNonNull(side, "side");
            Objects.requireNonNull(member, "member");
            if (reads.containsKey(side)) throw ValidationError.invalid("nestMembers.readAlreadyComplete");
            if (member.values().accountedBytes() > limits.row().bytes() || member.values().nodes() > limits.row().nodes()
                    || member.values().depth() > limits.row().depth()) throw limit("row");
            // Compare only to validate domains. Nothing is sorted or coerced to repair native reads.
            if (elementDomain != null) member.elementKey().compareTo(elementDomain);
            else elementDomain = member.elementKey();
            if (side == Side.SOURCE && member.rowKey().isEmpty()) throw ValidationError.invalid("nestMembers.sourceRowKey");
            if (side == Side.TARGET) {
                // Absence means the fixed projection hides this identity, not a missing required row field.
                if (targetRowIdentityVisible != null && targetRowIdentityVisible != member.rowKey().isPresent()) {
                    throw ValidationError.invalid("nestMembers.targetRowIdentity");
                }
                targetRowIdentityVisible = member.rowKey().isPresent();
            }
            if (member.rowKey().isPresent()) {
                ValidationKey rowKey = member.rowKey().orElseThrow();
                Map<ValidationKey, Long> rows = side == Side.SOURCE ? sourceRows : targetRows;
                ValidationKey rowDomain = side == Side.SOURCE ? sourceRowDomain : targetRowDomain;
                if (rowDomain != null) rowKey.compareTo(rowDomain);
                else if (side == Side.SOURCE) sourceRowDomain = rowKey;
                else targetRowDomain = rowKey;
                if (sourceRowDomain != null && targetRowDomain != null) sourceRowDomain.compareTo(targetRowDomain);
                if (!rows.containsKey(rowKey)) {
                    if (rows.size() == limits.rowKeys()) throw limit("rowKeys");
                    charge(128L + rowKey.retainedBytes());
                }
                rows.put(rowKey, increment(rows.getOrDefault(rowKey, 0L)));
            }
            Bucket bucket = buckets.get(member.elementKey());
            if (bucket == null) {
                if (buckets.size() == limits.elementKeys()) throw limit("elementKeys");
                charge(512L + 8L * (observation.root().ancestors().size() + 1) + member.elementKey().retainedBytes());
                bucket = new Bucket();
                buckets.put(member.elementKey(), bucket);
            }
            ArrayList<Member> samples = side == Side.SOURCE ? bucket.source : bucket.target;
            if (samples.size() < 2) {
                charge(128L + member.rowKey().map(ValidationKey::retainedBytes).orElse(0L) + member.elementKey().retainedBytes()
                        + member.physical().retainedBytes() + member.values().accountedBytes());
                samples.add(member);
            }
            if (side == Side.SOURCE) bucket.sourceCount = increment(bucket.sourceCount);
            else bucket.targetCount = increment(bucket.targetCount);
        });
    }

    /** Call only after normal completion of the whole admitted side, not a page or an arbitrary limit. */
    public void complete(Side side, ValidationObservation request, ValidationCheckEvent.ReadBoundary boundary) {
        mutate(() -> {
            sameObservation(request);
            Objects.requireNonNull(side, "side");
            Objects.requireNonNull(boundary, "boundary");
            ValidationCheckEvent.ReadBoundary previous = reads.get(side);
            if (previous != null) {
                if (!previous.equals(boundary)) throw ValidationError.invalid("nestMembers.readBoundary");
                return;
            }
            charge(128L + textBytes(boundary.readId()) + textBytes(boundary.contractVersion()));
            reads.put(side, boundary);
        });
    }

    public Result finish() {
        ensureOpen();
        try {
            if (reads.size() != Side.values().length) throw ValidationError.invalid("nestMembers.incomplete");
            var groups = new ArrayList<Group>(buckets.size());
            for (var entry : buckets.entrySet()) {
                Bucket bucket = entry.getValue();
                var ancestors = new ArrayList<>(observation.root().ancestors());
                ancestors.add(new ValidationRoot.Ancestor(path, entry.getKey()));
                var scope = new ValidationRoot(observation.root().mappingId(), observation.root().outputId(),
                        observation.root().rootKey(), ancestors);
                groups.add(new Group(scope, bucket.sourceCount, bucket.targetCount, bucket.source, bucket.target,
                        conflict(bucket.sourceCount, bucket.source, sourceRows), conflict(bucket.targetCount, bucket.target, targetRows)));
            }
            Result result = new Result(observation, path, groups, reads, bytes);
            close();
            return result;
        } catch (RuntimeException | Error error) {
            fail(error);
            throw error;
        }
    }

    private static boolean conflict(long count, List<Member> samples, Map<ValidationKey, Long> rows) {
        return count > 1 || samples.stream().anyMatch(sample -> sample.rowKey().map(key -> rows.get(key) > 1).orElse(false));
    }
    private void sameObservation(ValidationObservation request) {
        if (!observation.equals(request)) throw ValidationError.invalid("nestMembers.observation");
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
    private static long increment(long count) {
        if (count == Long.MAX_VALUE) throw limit("occurrences");
        return count + 1;
    }
    private void ensureOpen() {
        Throwable cause = failure.get();
        if (cause instanceof Error error) throw error;
        if (cause instanceof RuntimeException error) throw error;
        if (closed) throw new IllegalStateException("Array member staging is closed");
    }
    private void fail(Throwable cause) { failure.record(cause); close(); }
    @Override public void close() {
        closed = true;
        buckets.clear(); sourceRows.clear(); targetRows.clear(); reads.clear();
        elementDomain = null; sourceRowDomain = null; targetRowDomain = null; targetRowIdentityVisible = null;
    }
    private static long rootBytes(ValidationRoot root) {
        long bytes = 128L + textBytes(root.mappingId()) + textBytes(root.outputId()) + root.rootKey().retainedBytes();
        for (ValidationRoot.Ancestor ancestor : root.ancestors()) {
            bytes += 64L + ancestor.key().retainedBytes();
            for (String segment : ancestor.fieldPath()) bytes += textBytes(segment) + 8;
        }
        return bytes;
    }
    private static long textBytes(String text) { return text == null ? 0 : 32L + 2L * text.length(); }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "nestMembers." + dimension), null);
    }
    private static final class Bucket {
        private long sourceCount;
        private long targetCount;
        private final ArrayList<Member> source = new ArrayList<>(2);
        private final ArrayList<Member> target = new ArrayList<>(2);
    }
}
