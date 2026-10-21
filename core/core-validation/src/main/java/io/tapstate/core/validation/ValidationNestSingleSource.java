package io.tapstate.core.validation;

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
    public static final class Result {
        private final State state;
        private final Optional<ValidationRelationRows.Sample> unique;
        private final ValidationObservation observation;
        private final ValidationCheckEvent.ReadBoundary read;
        private final Location location;
        private Result(State state, Optional<ValidationRelationRows.Sample> unique, ValidationRelationRows.Result rows, Location location) {
            this.state = state; this.unique = unique;
            this.observation = rows.observation(); this.read = rows.read(); this.location = location;
        }
        public State state() { return state; }
        public Optional<ValidationRelationRows.Sample> unique() { return unique; }
        public ValidationObservation observation() { return observation; }
        public ValidationCheckEvent.ReadBoundary read() { return read; }
        public Location location() { return location; }
    }
    private ValidationNestSingleSource() {}

    public static Result check(ValidationRelationRows.Result rows, Relation relation, Optional<Reference> reference,
                               Location location, List<Location> affected, List<Finding> uncertaintyCauses, Builder root) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(rows, "rows");
            Objects.requireNonNull(relation, "relation");
            Objects.requireNonNull(reference, "reference");
            Objects.requireNonNull(location, "location");
            if (affected == null || affected.isEmpty() || affected.size() > 256
                    || uncertaintyCauses == null || uncertaintyCauses.size() > 256
                    || (relation == Relation.REFERENCE) != reference.isPresent()
                    || (rows.indeterminate() > 0) != !uncertaintyCauses.isEmpty()) {
                throw ValidationError.invalid("singleSource.contract");
            }
            affected = List.copyOf(affected);
            uncertaintyCauses = List.copyOf(uncertaintyCauses);
            requireScope(rows.observation(), root.observation());
            if (!rows.observation().root().equals(location.scope())
                    || affected.stream().anyMatch(path -> !rows.observation().root().equals(path.scope()))) {
                throw ValidationError.invalid("singleSource.location");
            }
            for (Finding cause : uncertaintyCauses) {
                if (!cause.kind().sourceProblem() || !within(cause.location().scope(), rows.observation().root())) {
                    throw ValidationError.invalid("singleSource.uncertainty");
                }
            }
            reference.ifPresent(value -> root.requireSampleRead(Side.SOURCE, value.parent()));
            root.read(Side.SOURCE, rows.read());
            for (Finding cause : uncertaintyCauses) {
                root.finding(cause);
                block(root, affected, cause);
            }
            if (rows.atLeastTwo()) {
                var kind = relation == Relation.REFERENCE ? Kind.SOURCE_DUPLICATE : Kind.SOURCE_CARDINALITY;
                Finding cause = new Finding(kind, location,
                        Optional.of(new Multiplicity(rows.included(), rows.participationComplete())));
                root.finding(cause, sample(rows.includedSamples().getFirst(), rows.read().readId()), Optional.empty());
                block(root, affected, cause);
                return new Result(State.BLOCKED, Optional.empty(), rows, location);
            }
            if (!rows.participationComplete()) return new Result(State.BLOCKED, Optional.empty(), rows, location);
            if (rows.definitelyEmpty()) {
                if (relation == Relation.GATHER) return new Result(State.EMPTY, Optional.empty(), rows, location);
                Finding cause = new Finding(Kind.SOURCE_REFERENCE_MISSING, location);
                root.finding(cause, Optional.of(reference.orElseThrow().parent()), Optional.empty());
                block(root, affected, cause);
                return new Result(State.BLOCKED, Optional.empty(), rows, location);
            }
            return new Result(State.UNIQUE, rows.unique(), rows, location);
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
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
