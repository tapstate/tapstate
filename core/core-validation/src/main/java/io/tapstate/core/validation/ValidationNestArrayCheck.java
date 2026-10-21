package io.tapstate.core.validation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Folds complete array membership facts into an enclosing root observation. The callback checks the
 * unique pair's projected fields and descendants; this class does not supply their business values.
 * The caller marks root obligations complete only after all independent branches have settled.
 */
public final class ValidationNestArrayCheck {
    private ValidationNestArrayCheck() {}

    public static void check(ValidationNestArrayMembers.Result members, Builder root,
                             Consumer<ValidationNestArrayMembers.Group> uniquePair) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(members, "members");
            Objects.requireNonNull(uniquePair, "uniquePair");
            requireScope(members, root);
            var sourceRead = members.reads().get(ValidationNestArrayMembers.Side.SOURCE);
            var targetRead = members.reads().get(ValidationNestArrayMembers.Side.TARGET);
            root.read(Side.SOURCE, sourceRead).read(Side.TARGET, targetRead);
            for (var group : members.groups()) {
                Location location = new Location(group.scope(), List.of());
                Optional<SampledSide> source = sample(group.sourceSamples(), sourceRead.readId());
                Optional<SampledSide> target = sample(group.targetSamples(), targetRead.readId());
                if (group.sourceConflict()) {
                    sourceProblem(group.sourceCount(), location, source, root);
                }
                if (group.targetConflict()) {
                    Finding cause = duplicate(group.targetCount(), Kind.TARGET_DUPLICATE, Kind.TARGET_ROW_DUPLICATE, location);
                    root.finding(cause, Optional.empty(), target);
                    if (!group.sourceConflict()) skipDependents(root, location, cause);
                }
                if (group.sourceConflict()) continue;
                if (group.sourceCount() == 0) {
                    Finding cause = new Finding(Kind.TARGET_EXTRA, location);
                    root.finding(cause, Optional.empty(), target);
                    skipDependents(root, location, cause);
                } else if (group.targetCount() == 0) {
                    Finding cause = new Finding(Kind.TARGET_MISSING, location);
                    root.finding(cause, source, Optional.empty());
                    skipDependents(root, location, cause);
                } else if (group.canCompareValues()) {
                    if (group.rowIdentityComparison() == ValidationNestArrayMembers.RowIdentityComparison.DIFFERS) {
                        root.finding(new Finding(Kind.TARGET_IDENTITY, location), source, target);
                    }
                    uniquePair.accept(group);
                }
            }
        } catch (RuntimeException | Error failure) {
            root.close();
            throw failure;
        }
    }

    /** Reports source facts only; the entire array remains unavailable to parent participation rules. */
    public static void checkUncertainSource(ValidationNestArrayMembers.UncertainSource source, Builder root) {
        Objects.requireNonNull(root, "root");
        try {
            Objects.requireNonNull(source, "source");
            var actual = source.observation(); var expected = root.observation();
            if (!expected.run().equals(actual.run()) || expected.revision() != actual.revision()
                    || !expected.root().mappingId().equals(actual.root().mappingId())
                    || !expected.root().outputId().equals(actual.root().outputId())
                    || !expected.root().rootKey().equals(actual.root().rootKey())) throw ValidationError.invalid("nestCheck.sourceScope");
            root.read(Side.SOURCE, source.read());
            var array = new Location(actual.root(), new java.util.ArrayList<>(source.arrayPath()));
            for (var cause : source.causes()) {
                root.finding(cause).notCompared(Check.STRUCTURE, array, cause);
                if (root.method() == ValidationMethod.FULL_COLUMN) root.notCompared(Check.VALUES, array, cause);
            }
            for (var group : source.groups()) {
                if (group.conflict()) sourceProblem(group.count(), new Location(group.scope(), List.of()),
                        sample(group.samples(), source.read().readId()), root, false);
            }
        } catch (RuntimeException | Error failure) { root.close(); throw failure; }
    }

    /** Called before discarding a malformed target frame, without fabricating an empty target read. */
    static void retainSourceProblems(ValidationNestArrayMembers members, Builder root) {
        var read = members.requireSourceComplete();
        root.read(Side.SOURCE, read);
        members.visitSourceGroups(group -> {
            if (group.conflict()) sourceProblem(group.count(), new Location(group.scope(), List.of()), sample(group.samples(), read.readId()), root);
        });
    }

    private static void sourceProblem(long count, Location location, Optional<SampledSide> sample, Builder root) {
        sourceProblem(count, location, sample, root, true);
    }

    private static void sourceProblem(long count, Location location, Optional<SampledSide> sample, Builder root, boolean exact) {
        Finding cause = count > 1 ? new Finding(Kind.SOURCE_DUPLICATE, location, Optional.of(new Multiplicity(count, exact)))
                : new Finding(Kind.SOURCE_ROW_DUPLICATE, location, Optional.of(new Multiplicity(2, false)));
        root.finding(cause, sample, Optional.empty()).notCompared(Check.STRUCTURE, location, cause);
        if (root.method() == ValidationMethod.FULL_COLUMN) root.notCompared(Check.VALUES, location, cause);
    }

    private static void requireScope(ValidationNestArrayMembers.Result members, Builder root) {
        ValidationObservation expected = root.observation();
        ValidationObservation actual = members.observation();
        if (!expected.run().equals(actual.run()) || expected.revision() != actual.revision()
                || !expected.root().mappingId().equals(actual.root().mappingId())
                || !expected.root().outputId().equals(actual.root().outputId())
                || !expected.root().rootKey().equals(actual.root().rootKey())
                || root.participation() != ValidationCheckEvent.Participation.SHOULD_EXIST
                || root.targetPresence() != TargetPresence.PRESENT) {
            throw ValidationError.invalid("nestCheck.rootScope");
        }
    }

    private static Finding duplicate(long elementCount, Kind elementKind, Kind rowKind, Location location) {
        return elementCount > 1
                ? new Finding(elementKind, location, Optional.of(new Multiplicity(elementCount, true)))
                : new Finding(rowKind, location, Optional.of(new Multiplicity(2, false)));
    }

    private static void skipDependents(Builder root, Location location, Finding cause) {
        root.notApplicable(Check.STRUCTURE, location, cause);
        if (root.method() == ValidationMethod.FULL_COLUMN) root.notApplicable(Check.VALUES, location, cause);
    }

    private static Optional<SampledSide> sample(List<ValidationNestArrayMembers.Member> samples, String readId) {
        return samples.isEmpty() ? Optional.empty() : Optional.of(new SampledSide(samples.getFirst().physical(), samples.getFirst().original(), readId));
    }
}
