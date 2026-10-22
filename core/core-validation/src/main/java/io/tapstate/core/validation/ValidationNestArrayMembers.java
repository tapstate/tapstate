package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

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

    /** Declared and physical identities, raw diagnostics and independently transformed values stay distinct. */
    public record Member(Optional<ValidationKey> rowKey, ValidationKey elementKey, ValidationPhysicalIdentity physical,
                         ValidationValues.Row original, ValidationValues.Row values) {
        public Member {
            Objects.requireNonNull(rowKey, "rowKey");
            Objects.requireNonNull(elementKey, "elementKey");
            Objects.requireNonNull(physical, "physical");
            Objects.requireNonNull(original, "original");
            Objects.requireNonNull(values, "values");
        }
        public Member(Optional<ValidationKey> rowKey, ValidationKey elementKey, ValidationPhysicalIdentity physical, ValidationValues.Row values) {
            this(rowKey, elementKey, physical, values, values);
        }
        public Member(ValidationKey rowKey, ValidationKey elementKey, ValidationPhysicalIdentity physical, ValidationValues.Row values) {
            this(Optional.of(rowKey), elementKey, physical, values, values);
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

    /**
     * Immutable complete source membership, with no target count or completion claim. It preserves
     * all element counts and cross-element row collisions, including identities outside retained
     * diagnostic samples. The original limits remain binding when target staging is created.
     */
    public static final class Source {
        private final ValidationObservation observation;
        private final List<String> arrayPath;
        private final ValidationCheckEvent.ReadBoundary read;
        private final List<SourceGroup> groups;
        private final Map<ValidationKey, Long> rows;
        private final ValidationKey elementDomain;
        private final ValidationKey rowDomain;
        private final Limits limits;
        private final long accountedBytes;
        private Source(ValidationObservation observation, List<String> arrayPath, ValidationCheckEvent.ReadBoundary read,
                List<SourceGroup> groups, Map<ValidationKey, Long> rows, ValidationKey elementDomain, ValidationKey rowDomain,
                Limits limits, long accountedBytes) {
            this.observation = observation; this.arrayPath = List.copyOf(arrayPath); this.read = read;
            this.groups = List.copyOf(groups); this.rows = Map.copyOf(rows);
            this.elementDomain = elementDomain; this.rowDomain = rowDomain; this.limits = limits; this.accountedBytes = accountedBytes;
        }
        public ValidationObservation observation() { return observation; }
        public List<String> arrayPath() { return arrayPath; }
        public ValidationCheckEvent.ReadBoundary read() { return read; }
        public List<SourceGroup> groups() { return groups; }
        public long accountedBytes() { return accountedBytes; }
    }

    /** A complete source query whose omitted indeterminate participants leave membership unknown. */
    public static final class UncertainSource {
        private final ValidationObservation observation;
        private final List<String> arrayPath;
        private final ValidationCheckEvent.ReadBoundary read;
        private final List<SourceGroup> groups;
        private final List<ValidationRootCheck.Finding> causes;
        private final long accountedBytes;
        private UncertainSource(ValidationObservation observation, List<String> arrayPath, ValidationCheckEvent.ReadBoundary read,
                List<SourceGroup> groups, List<ValidationRootCheck.Finding> causes, long accountedBytes) {
            this.observation = observation; this.arrayPath = arrayPath; this.read = read;
            this.groups = List.copyOf(groups); this.causes = List.copyOf(causes); this.accountedBytes = accountedBytes;
        }
        public ValidationObservation observation() { return observation; }
        public List<String> arrayPath() { return arrayPath; }
        public ValidationCheckEvent.ReadBoundary read() { return read; }
        public List<ValidationRootCheck.Finding> causes() { return causes; }
        public long accountedBytes() { return accountedBytes; }
        public List<SourceGroup> groups() { return groups; }
    }

    private final ValidationObservation observation;
    private final List<String> path;
    private final Limits limits;
    private final LongConsumer reserve;
    private final Consumer<Throwable> failureSink;
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
        this(parent, arrayPath, limits, ignored -> {}, ignored -> {}, initialReservationBytes(parent, arrayPath));
    }

    private ValidationNestArrayMembers(ValidationObservation parent, List<String> arrayPath, Limits limits, LongConsumer reserve, Consumer<Throwable> failureSink, long initial) {
        observation = Objects.requireNonNull(parent, "parent");
        this.limits = Objects.requireNonNull(limits, "limits"); this.reserve = Objects.requireNonNull(reserve, "reserve");
        this.failureSink = Objects.requireNonNull(failureSink, "failureSink");
        if (initial > limits.bytes()) throw limit("bytes");
        path = List.copyOf(arrayPath);
        bytes = initial;
    }

    /** Shared-root admission happens before frame allocation; every later growth calls the same reservation. */
    public static ValidationNestArrayMembers reserved(ValidationObservation parent, List<String> arrayPath, Limits limits, LongConsumer reserve) {
        return reserved(parent, arrayPath, limits, reserve, ignored -> {});
    }

    private static ValidationNestArrayMembers reserved(ValidationObservation parent, List<String> arrayPath, Limits limits,
            LongConsumer reserve, Consumer<Throwable> failureSink) {
        Objects.requireNonNull(limits, "limits"); Objects.requireNonNull(reserve, "reserve");
        long initial = initialReservationBytes(parent, arrayPath);
        if (initial > limits.bytes()) throw limit("bytes");
        reserve.accept(initial);
        return new ValidationNestArrayMembers(parent, arrayPath, limits, reserve, failureSink, initial);
    }

    public static long initialReservationBytes(ValidationObservation parent, List<String> arrayPath) {
        Objects.requireNonNull(parent, "parent");
        if (parent.root().ancestors().size() >= 32 || arrayPath == null || arrayPath.isEmpty() || arrayPath.size() > 32)
            throw ValidationError.invalid("nestMembers.scope");
        long initial = 16 * 1024L + rootBytes(parent.root());
        for (String segment : arrayPath) { ValidationError.identifier(segment, "nestMembers.arrayPath"); initial += textBytes(segment); }
        return initial;
    }

    void requireRequest(ValidationObservation parent, List<String> arrayPath) {
        mutate(() -> {
            sameObservation(parent);
            if (!path.equals(arrayPath)) throw ValidationError.invalid("nestMembers.arrayPath");
        });
    }

    /** The caller may interleave sides; order does not change membership or overwrite duplicates. */
    public void add(Side side, ValidationObservation request, Member member) {
        mutate(() -> {
            sameObservation(request);
            Objects.requireNonNull(side, "side");
            Objects.requireNonNull(member, "member");
            if (reads.containsKey(side)) throw ValidationError.invalid("nestMembers.readAlreadyComplete");
            bounded(member.original());
            bounded(member.values());
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
                        + member.physical().retainedBytes() + member.original().accountedBytes()
                        + (member.original() == member.values() ? 0 : member.values().accountedBytes()));
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

    public record SourceGroup(ValidationRoot scope, long count, List<Member> samples, boolean conflict) {
        public SourceGroup { samples = List.copyOf(samples); }
    }

    ValidationCheckEvent.ReadBoundary requireSourceComplete() {
        ensureOpen();
        try {
            var read = reads.get(Side.SOURCE);
            if (read == null) throw ValidationError.invalid("nestMembers.sourceIncomplete");
            return read;
        } catch (RuntimeException | Error error) { fail(error); throw error; }
    }

    /** Independent source facts carry no inferred target count or target completion. */
    void visitSourceGroups(Consumer<SourceGroup> visitor) {
        mutate(() -> {
            requireSourceComplete();
            Objects.requireNonNull(visitor, "visitor");
            for (var entry : buckets.entrySet()) {
                Bucket bucket = entry.getValue();
                if (bucket.sourceCount > 0) visitor.accept(new SourceGroup(scope(entry.getKey()), bucket.sourceCount,
                        bucket.source, conflict(bucket.sourceCount, bucket.source, sourceRows)));
            }
        });
    }

    /**
     * Normal source EOF is still mandatory. Known included members are lower bounds only; this
     * terminal path never publishes target membership or permits the ordinary two-sided finish.
     * Causes concern parent fields or this array's descendants, never a sibling array. They are
     * retained source problems, not query, budget, codec or authorization failures.
     */
    public UncertainSource finishUncertainSource(List<ValidationRootCheck.Finding> causes) {
        ensureOpen();
        try {
            var read = requireSourceComplete();
            if (causes == null || causes.isEmpty() || causes.size() > 256
                    || reads.containsKey(Side.TARGET) || targetRowIdentityVisible != null) {
                throw ValidationError.invalid("nestMembers.uncertainSource");
            }
            for (var cause : causes) {
                Objects.requireNonNull(cause, "source cause");
                var actual = cause.location().scope(); var parent = observation.root();
                if (!cause.kind().sourceProblem() || !parent.mappingId().equals(actual.mappingId())
                        || !parent.outputId().equals(actual.outputId()) || !parent.rootKey().equals(actual.rootKey())
                        || actual.ancestors().size() < parent.ancestors().size()
                        || !actual.ancestors().subList(0, parent.ancestors().size()).equals(parent.ancestors())
                        || actual.ancestors().size() > parent.ancestors().size()
                            && !actual.ancestors().get(parent.ancestors().size()).fieldPath().equals(path)) {
                    throw ValidationError.invalid("nestMembers.uncertaintyCause");
                }
                charge(128L + rootBytes(actual));
                for (Object segment : cause.location().fieldPath()) charge(segment instanceof String text ? textBytes(text) : 16L);
            }
            var groups = new ArrayList<SourceGroup>();
            visitSourceGroups(group -> { charge(64L + 8L * group.samples().size()); groups.add(group); });
            var result = new UncertainSource(observation, path, read, groups, causes, bytes);
            close();
            return result;
        } catch (RuntimeException | Error error) { fail(error); throw error; }
    }

    /** Normal complete source facts only; callers must separately preserve any indeterminate participation. */
    public Source finishSource() {
        ensureOpen();
        try {
            var read = requireSourceComplete();
            if (reads.containsKey(Side.TARGET) || targetRowIdentityVisible != null) throw ValidationError.invalid("nestMembers.sourceOnly");
            // Reserve the immutable handoff metadata before copying it while staging is still live.
            charge(128L + 64L * sourceRows.size());
            var groups = new ArrayList<SourceGroup>();
            visitSourceGroups(group -> { charge(64L + 8L * group.samples().size()); groups.add(group); });
            var result = new Source(observation, path, read, groups, sourceRows, elementDomain, sourceRowDomain, limits, bytes);
            close();
            return result;
        } catch (RuntimeException | Error error) { fail(error); throw error; }
    }

    /**
     * Reopens target staging under exactly the same immutable source scope and original limits.
     * The source handoff and the new mutable indexes coexist, so both are charged to this frame.
     * The root caller still reserves all simultaneously live frames; this is not a global quota.
     */
    public static ValidationNestArrayMembers forTarget(Source source) { return forTarget(source, ignored -> {}); }

    /** The shared root must also cover the source handoff and target-index coexistence. */
    public static ValidationNestArrayMembers forTarget(Source source, LongConsumer reserve) {
        return forTarget(source, reserve, ignored -> {});
    }

    /** Construction and later local failures invalidate the owning root, not only this frame. */
    public static ValidationNestArrayMembers forTarget(Source source, LongConsumer reserve, Consumer<Throwable> failureSink) {
        Objects.requireNonNull(source, "source");
        var result = reserved(source.observation, source.arrayPath, source.limits, reserve, failureSink);
        try {
            result.charge(source.accountedBytes);
            result.charge(128L + 64L * source.rows.size());
            result.sourceRows.putAll(source.rows);
            result.elementDomain = source.elementDomain; result.sourceRowDomain = source.rowDomain;
            for (var group : source.groups) {
                result.charge(512L + 8L * group.samples().size());
                var bucket = new Bucket(); bucket.sourceCount = group.count(); bucket.source.addAll(group.samples());
                result.buckets.put(group.scope().ancestors().getLast().key(), bucket);
            }
            result.reads.put(Side.SOURCE, source.read);
            return result;
        } catch (RuntimeException | Error error) { result.fail(error); throw error; }
    }

    public Result finish() {
        ensureOpen();
        try {
            if (reads.size() != Side.values().length) throw ValidationError.invalid("nestMembers.incomplete");
            var groups = new ArrayList<Group>(buckets.size());
            for (var entry : buckets.entrySet()) {
                Bucket bucket = entry.getValue();
                groups.add(new Group(scope(entry.getKey()), bucket.sourceCount, bucket.targetCount, bucket.source, bucket.target,
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

    private ValidationRoot scope(ValidationKey elementKey) {
        var ancestors = new ArrayList<>(observation.root().ancestors());
        ancestors.add(new ValidationRoot.Ancestor(path, elementKey));
        return new ValidationRoot(observation.root().mappingId(), observation.root().outputId(),
                observation.root().rootKey(), ancestors);
    }

    private static boolean conflict(long count, List<Member> samples, Map<ValidationKey, Long> rows) {
        return count > 1 || samples.stream().anyMatch(sample -> sample.rowKey().map(key -> rows.get(key) > 1).orElse(false));
    }
    private void bounded(ValidationValues.Row row) {
        if (row.accountedBytes() > limits.row().bytes() || row.nodes() > limits.row().nodes()
                || row.depth() > limits.row().depth()) throw limit("row");
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
        reserve.accept(amount);
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
    private void fail(Throwable cause) {
        failure.record(cause); close();
        try { failureSink.accept(failure.get()); }
        catch (RuntimeException | Error callbackFailure) {
            failure.record(callbackFailure);
            if (failure.get() instanceof Error fatal) throw fatal;
        }
    }
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
