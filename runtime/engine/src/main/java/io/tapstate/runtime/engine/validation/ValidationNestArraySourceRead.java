package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.validation.*;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Streams every participating independent source row into one bounded array membership frame.
 * The ordinary relation reader's two diagnostic samples are never used to reconstruct membership.
 * Native admission, rule scratch and all simultaneously live root frames remain caller reservations.
 */
public final class ValidationNestArraySourceRead {
    public record Limits(ValidationRelationRows.Limits read, ValidationNestArrayMembers.Limits members, int rows, int causes) {
        public Limits {
            Objects.requireNonNull(read, "read"); Objects.requireNonNull(members, "members");
            if (rows < 1 || rows > 65536 || causes < 1 || causes > 256) throw invalid("arrayRead.limits");
        }
    }
    public record Context(ValidationObservation parent, List<String> path, String readId) {
        public Context { path = List.copyOf(path); }
    }
    /** Explicit confirmed row/element keys and independently projected values; no target input. */
    public record Classified(ValidationRootReader.Source source, Optional<ValidationNestArrayMembers.Member> member) {
        public Classified {
            Objects.requireNonNull(source, "source"); Objects.requireNonNull(member, "member");
            if ((source.row().disposition() == ValidationRelationRows.Disposition.INCLUDED) != member.isPresent())
                throw invalid("arrayRead.memberDisposition");
        }
    }
    @FunctionalInterface public interface Rule {
        Classified apply(Context context, ValidationValues.Row original);
    }
    @FunctionalInterface public interface Facts {
        /**
         * Validates and retains this classification's source evidence/uncertainty in the original root
         * before the array frame retains any references. The caller enforces its shared root budget,
         * including original/working rows, source samples and all finding scopes; returning normally
         * admits that retention. Execution failures must propagate, never become source data problems.
         */
        void retain(Context context, ValidationRootReader.Source source);
    }
    public static final class Result {
        private final ValidationNestLookupBinding.Result preparation;
        private final Optional<ValidationNestArrayMembers.Source> source;
        private final Optional<ValidationNestArrayMembers.UncertainSource> uncertain;
        private final Optional<ValidationNestSingleSource.Result> referenceProblem;
        private Result(ValidationNestLookupBinding.Result preparation, Optional<ValidationNestArrayMembers.Source> source,
                Optional<ValidationNestArrayMembers.UncertainSource> uncertain, Optional<ValidationNestSingleSource.Result> referenceProblem) {
            this.preparation = preparation; this.source = source; this.uncertain = uncertain; this.referenceProblem = referenceProblem;
        }
        public ValidationNestLookupBinding.Result preparation() { return preparation; }
        /** Present only after actual normal query EOF, row/cursor cleanup and complete participation. */
        public Optional<ValidationNestArrayMembers.Source> source() { return source; }
        /** Known members are lower bounds only; this cannot be restored for target membership. */
        public Optional<ValidationNestArrayMembers.UncertainSource> uncertain() { return uncertain; }
        /** Non-NULL references retain missing, duplicate or indeterminate source relations instead of a target array. */
        public Optional<ValidationNestSingleSource.Result> referenceProblem() { return referenceProblem; }
    }
    private ValidationNestArraySourceRead() {}

    /**
     * The caller handles parent-field conflicts before requesting this read, and reserves both bounded
     * accumulators, bounded reference classification and at most 256 finding references. No-reference,
     * invalid and unresolved keys return preparation only: no invented child EOF or empty membership.
     * A no-reference empty array must be established from the actual completed parent observation.
     */
    public static Result read(ValidationObservation parent, ValidationNestLookupBinding binding,
            ValidationStaticMap.Projection workingParent, ValidationRootCheck.SampledSide originalParent, ValidationNestRelationRead.Queries queries, Limits limits,
            Clock clock, Runnable requireAuthorized, Rule rule, Facts facts) {
        return read(new ValidationNestScope(parent), binding, workingParent, originalParent, queries, limits, clock, requireAuthorized, rule, facts);
    }

    /** The parent projection supplies local join fields; object prefixes belong only to logical scope. */
    public static Result read(ValidationNestScope scope, ValidationNestLookupBinding binding,
            ValidationStaticMap.Projection workingParent, ValidationRootCheck.SampledSide originalParent, ValidationNestRelationRead.Queries queries, Limits limits,
            Clock clock, Runnable requireAuthorized, Rule rule, Facts facts) {
        Objects.requireNonNull(scope, "scope");
        var parent = scope.observation();
        Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(limits, "limits"); Objects.requireNonNull(rule, "rule"); Objects.requireNonNull(facts, "facts");
        Objects.requireNonNull(queries, "queries"); Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        Objects.requireNonNull(originalParent, "originalParent");
        if (binding.relation().shape() != EmbedAs.ARRAY) throw invalid("arrayRead.shape");
        var path = scope.path(List.of(binding.relation().outputPath().orElseThrow().split("\\.", -1)));
        final Context[] context = { null };
        final int[] rows = { 0 };
        var causes = new LinkedHashSet<ValidationRootCheck.Finding>();
        try (var members = new ValidationNestArrayMembers(parent, path, limits.members())) {
            var relation = ValidationNestRelationRead.read(parent, binding, workingParent, request -> {
                var query = Objects.requireNonNull(queries.prepare(request), "array source query");
                if (query.readId().equals(originalParent.readId())) throw invalid("arrayRead.reusedParentReadId");
                context[0] = new Context(parent, path, query.readId());
                return query;
            }, limits.read(), clock, requireAuthorized, raw -> {
                if (rows[0] == limits.rows()) throw limit("rows");
                rows[0]++;
                var classified = Objects.requireNonNull(rule.apply(context[0], raw), "array classification");
                var source = classified.source();
                if (classified.member().isPresent()) {
                    var member = classified.member().orElseThrow();
                    if (member.original() != raw || member.values() != source.row().working()
                            || !source.row().physical().equals(Optional.of(member.physical())))
                        throw invalid("arrayRead.memberFacts");
                }
                facts.retain(context[0], source);
                for (var cause : source.uncertainty()) {
                    if (!causes.contains(cause) && causes.size() == limits.causes()) throw limit("causes");
                    causes.add(cause);
                }
                classified.member().ifPresent(member -> members.add(ValidationNestArrayMembers.Side.SOURCE, parent, member));
                return source.row();
            });
            requireAuthorized.run();
            if (relation.rows().isEmpty()) return new Result(relation.preparation(), Optional.empty(), Optional.empty(), Optional.empty());
            var complete = relation.rows().orElseThrow();
            members.complete(ValidationNestArrayMembers.Side.SOURCE, parent, complete.read());
            if (binding.relation().direction() == ValidationNestRelations.Direction.REFERENCE) {
                var reference = new ValidationNestSingleSource.Reference(binding.requireReferenceFields(workingParent.row()), originalParent);
                var location = new ValidationRootCheck.Location(parent.root(), new ArrayList<>(path));
                var single = ValidationNestSingleSource.classify(complete, ValidationNestSingleSource.Relation.REFERENCE,
                        Optional.of(reference), location, List.of(location), new ArrayList<>(causes), limits.members().bytes());
                if (single.state() != ValidationNestSingleSource.State.UNIQUE) {
                    requireAuthorized.run();
                    return new Result(relation.preparation(), Optional.empty(), Optional.empty(), Optional.of(single));
                }
            }
            Result result;
            if (!causes.isEmpty()) result = new Result(relation.preparation(), Optional.empty(),
                    Optional.of(members.finishUncertainSource(new ArrayList<>(causes))), Optional.empty());
            else {
                if (!complete.participationComplete()) throw invalid("arrayRead.uncertaintyMissing");
                result = new Result(relation.preparation(), Optional.of(members.finishSource()), Optional.empty(), Optional.empty());
            }
            requireAuthorized.run();
            return result;
        }
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "arrayRead." + dimension), null);
    }
}
