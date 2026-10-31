package io.tapstate.core.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Current source cardinality for object/flat gathering or a non-NULL identity reference. It never
 * chooses a historical winner or makes target conclusions. Payload/key/path-contribution checks for
 * a unique participant remain separate; every affected scope is explicitly supplied by the rule.
 */
public final class ValidationNestSingleSource {
    public enum Relation { GATHER, REFERENCE }
    public enum State { EMPTY, UNIQUE, BLOCKED }
    public record Reference(ValidationReferenceFields.Complete fields, SampledSide parent) {
        public Reference {
            Objects.requireNonNull(fields, "fields");
            Objects.requireNonNull(parent, "parent");
        }
    }
    /** Immutable source facts; a parent sample may name a read that the enclosing reader still owns. */
    public record Issue(Finding finding, Optional<SampledSide> sample, List<Location> affected) {
        public Issue {
            Objects.requireNonNull(finding, "finding"); Objects.requireNonNull(sample, "sample");
            affected = List.copyOf(affected);
        }
    }
    public static final class Result {
        private final State state;
        private final Optional<ValidationRelationRows.Sample> unique;
        private final ValidationObservation observation;
        private final ValidationCheckEvent.ReadBoundary read;
        private final Location location;
        private final List<Issue> issues;
        private final long accountedBytes;
        private Result(State state, Optional<ValidationRelationRows.Sample> unique, ValidationRelationRows.Result rows, Location location,
                       List<Issue> issues, long accountedBytes) {
            this.state = state; this.unique = unique;
            this.observation = rows.observation(); this.read = rows.read(); this.location = location;
            this.issues = List.copyOf(issues); this.accountedBytes = accountedBytes;
        }
        public State state() { return state; }
        public Optional<ValidationRelationRows.Sample> unique() { return unique; }
        public ValidationObservation observation() { return observation; }
        public ValidationCheckEvent.ReadBoundary read() { return read; }
        public Location location() { return location; }
        public List<Issue> issues() { return issues; }
        public long accountedBytes() { return accountedBytes; }
    }
    private ValidationNestSingleSource() {}

    public static Result check(ValidationRelationRows.Result rows, Relation relation, Optional<Reference> reference,
                               Location location, List<Location> affected, List<Finding> uncertaintyCauses, Builder root) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(rows, "rows");
            requireScope(rows.observation(), root.observation());
            Objects.requireNonNull(reference, "reference").ifPresent(value -> root.requireSampleRead(Side.SOURCE, value.parent()));
            Result result = classify(rows, relation, reference, location, affected, uncertaintyCauses, 32L * 1024 * 1024);
            root.read(Side.SOURCE, result.read());
            for (Issue issue : result.issues()) {
                root.finding(issue.finding(), issue.sample(), Optional.empty());
                block(root, issue.affected(), issue.finding());
            }
            return result;
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
        }
    }

    /**
     * Classifies completed child reads without publishing into a root report. This may run before the
     * enclosing parent query reaches EOF; its samples retain the original read IDs, never fabricated
     * completed boundaries. The caller reserves the returned facts and imports each issue once only
     * after its actual read obligations are known. Byte exhaustion is an execution failure.
     */
    public static Result classify(ValidationRelationRows.Result rows, Relation relation, Optional<Reference> reference,
            Location location, List<Location> affected, List<Finding> uncertaintyCauses, long maxBytes) {
        Objects.requireNonNull(rows, "rows"); Objects.requireNonNull(relation, "relation");
        Objects.requireNonNull(reference, "reference"); Objects.requireNonNull(location, "location");
        if (maxBytes < 1 || maxBytes > 32L * 1024 * 1024 || affected == null || affected.isEmpty() || affected.size() > 256
                || uncertaintyCauses == null || uncertaintyCauses.size() > 256
                || (relation == Relation.REFERENCE) != reference.isPresent()
                || (rows.indeterminate() > 0) != !uncertaintyCauses.isEmpty()) throw ValidationError.invalid("singleSource.contract");
        affected = List.copyOf(affected); uncertaintyCauses = List.copyOf(uncertaintyCauses);
        if (!rows.observation().root().equals(location.scope())
                || affected.stream().anyMatch(path -> !rows.observation().root().equals(path.scope())))
            throw ValidationError.invalid("singleSource.location");
        var budget = new FactsBudget(maxBytes);
        budget.add(rows.accountedBytes() + 256 + locationBytes(location));
        var issues = new ArrayList<Issue>();
        for (Finding cause : uncertaintyCauses) {
            if (!cause.kind().sourceProblem() || !within(cause.location().scope(), rows.observation().root()))
                throw ValidationError.invalid("singleSource.uncertainty");
            addIssue(issues, new Issue(cause, Optional.empty(), affected), budget);
        }
        State state; Optional<ValidationRelationRows.Sample> unique = Optional.empty();
        if (rows.atLeastTwo()) {
            var kind = relation == Relation.REFERENCE ? Kind.SOURCE_DUPLICATE : Kind.SOURCE_CARDINALITY;
            var cause = new Finding(kind, location, Optional.of(new Multiplicity(rows.included(), rows.participationComplete())));
            addIssue(issues, new Issue(cause, sample(rows.includedSamples().getFirst(), rows.read().readId()), affected), budget);
            state = State.BLOCKED;
        } else if (!rows.participationComplete()) {
            state = State.BLOCKED;
        } else if (rows.definitelyEmpty()) {
            if (relation == Relation.GATHER) state = State.EMPTY;
            else {
                var cause = new Finding(Kind.SOURCE_REFERENCE_MISSING, location);
                addIssue(issues, new Issue(cause, Optional.of(reference.orElseThrow().parent()), affected), budget);
                state = State.BLOCKED;
            }
        } else { state = State.UNIQUE; unique = rows.unique(); }
        return new Result(state, unique, rows, location, issues, budget.bytes);
    }
    private static void addIssue(List<Issue> issues, Issue issue, FactsBudget budget) {
        budget.add(128L + locationBytes(issue.finding().location()) + issue.sample().map(SampledSide::accountedBytes).orElse(0L));
        for (Location affected : issue.affected()) budget.add(32L + locationBytes(affected));
        issues.add(issue);
    }
    private static long locationBytes(Location location) {
        long bytes = 256L + location.scope().rootKey().retainedBytes();
        for (Object part : location.fieldPath()) bytes += part instanceof String text ? 48L + 2L * text.length() : 24L;
        for (var ancestor : location.scope().ancestors()) {
            bytes += 64L + ancestor.key().retainedBytes();
            for (String part : ancestor.fieldPath()) bytes += 48L + 2L * part.length();
        }
        return bytes;
    }
    private static final class FactsBudget {
        private final long limit;
        private long bytes;
        private FactsBudget(long limit) { this.limit = limit; }
        private void add(long amount) {
            if (amount > limit - bytes) throw new io.tapstate.core.common.TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    java.util.Map.of("limit", "singleSource.facts"), null);
            bytes += amount;
        }
    }

    private static void requireScope(ValidationObservation actual, ValidationObservation expected) {
        if (!expected.run().equals(actual.run()) || expected.revision() != actual.revision()
                || !expected.root().mappingId().equals(actual.root().mappingId())
                || !expected.root().outputId().equals(actual.root().outputId())
                || !expected.root().rootKey().equals(actual.root().rootKey())) throw ValidationError.invalid("singleSource.observation");
    }
    private static boolean within(ValidationRoot actual, ValidationRoot parent) {
        return parent.mappingId().equals(actual.mappingId()) && parent.outputId().equals(actual.outputId())
                && parent.rootKey().equals(actual.rootKey()) && actual.ancestors().size() >= parent.ancestors().size()
                && actual.ancestors().subList(0, parent.ancestors().size()).equals(parent.ancestors());
    }
    private static Optional<SampledSide> sample(ValidationRelationRows.Sample sample, String readId) {
        return sample.physical().map(physical -> new SampledSide(physical, sample.original(), readId));
    }
    private static void block(Builder root, List<Location> affected, Finding cause) {
        for (Location path : affected) {
            root.notCompared(Check.STRUCTURE, path, cause);
            if (root.method() == ValidationMethod.FULL_COLUMN) root.notCompared(Check.VALUES, path, cause);
        }
    }
}
