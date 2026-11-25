package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationRelationRows;
import io.tapstate.core.validation.ValidationValues;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Reads one independently admitted current relationship from a partial parent projection. Only a
 * known non-NULL key opens a cursor; absence, data problems and unresolved dependencies do not invent
 * an empty database result or read boundary. Normal EOF and actual cursor cleanup are still required.
 * This is a source-read component, not root participation, recursive rendering or target comparison.
 */
public final class ValidationNestRelationRead {
    /** The physical predicate is separately confirmed; working names are never used as native names. */
    public record Request(ValidationObservation parent, ValidationKeyBinding physical, ValidationKey key) {
        public Request {
            Objects.requireNonNull(parent, "parent"); Objects.requireNonNull(physical, "physical"); Objects.requireNonNull(key, "key");
        }
    }
    @FunctionalInterface
    public interface Queries {
        /**
         * Builds metadata for the fixed admitted source/table and complete relationship predicate.
         * Does not open I/O. Native versions, indexes, budgets and deadlines remain caller obligations.
         */
        ValidationRootReader.Query prepare(Request request);
    }
    public static final class Result {
        private final ValidationObservation parent;
        private final ValidationNestLookupBinding.Result preparation;
        private final Optional<ValidationRelationRows.Result> rows;
        private Result(ValidationObservation parent, ValidationNestLookupBinding.Result preparation,
                       Optional<ValidationRelationRows.Result> rows) {
            this.parent = parent; this.preparation = preparation; this.rows = rows;
        }
        public ValidationObservation parent() { return parent; }
        public ValidationNestLookupBinding.Result preparation() { return preparation; }
        /** Present only after an actual complete query; no-reference is deliberately not a zero-row read. */
        public Optional<ValidationRelationRows.Result> rows() { return rows; }
    }
    private ValidationNestRelationRead() {}

    /**
     * Classifies complete independently projected child rows. Any partial descendant knowledge and its
     * causes must remain in the enclosing source tree; passing only its row here would discard facts.
     * The caller also reserves physical-key binding scratch under the admitted row/key limits. Every
     * returned raw row must independently match the requested physical equality predicate before its
     * rule runs. Execution failures propagate; no partial result may be published after an exception.
     */
    public static Result read(ValidationObservation parent, ValidationNestLookupBinding binding,
            ValidationStaticMap.Projection workingParent, Queries queries, ValidationRelationRows.Limits limits,
            Clock clock, Runnable requireAuthorized, Function<ValidationValues.Row, ValidationRelationReader.Evaluation> classify) {
        Objects.requireNonNull(parent, "parent"); Objects.requireNonNull(binding, "binding");
        Objects.requireNonNull(workingParent, "workingParent"); Objects.requireNonNull(queries, "queries");
        Objects.requireNonNull(limits, "limits"); Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(requireAuthorized, "requireAuthorized"); Objects.requireNonNull(classify, "classify");
        requireAuthorized.run();
        var prepared = binding.prepare(workingParent);
        requireAuthorized.run();
        if (!(prepared instanceof ValidationNestLookupBinding.Ready ready)) return new Result(parent, prepared, Optional.empty());
        var query = Objects.requireNonNull(queries.prepare(new Request(parent, binding.physical(), ready.key())), "relation query");
        requireAuthorized.run();
        var rows = ValidationRelationReader.read(parent, query.readId(), query.contractVersion(), query.open().get(),
                limits, clock, requireAuthorized, raw -> {
                    var actual = binding.physical().read(raw);
                    if (!(actual instanceof ValidationKeyBinding.Bound bound) || !bound.key().equals(ready.key())) {
                        throw new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", query.readId()), null);
                    }
                    return classify.apply(raw);
                });
        requireAuthorized.run();
        return new Result(parent, prepared, Optional.of(rows));
    }
}
