package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationCheckEvent;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationRelationRows;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Consumes an admitted independent relation query into bounded current-source facts. Run on blocking
 * I/O, with reservations for the cursor, each rule's working set and the retained relation samples.
 * The controller owns query/deadline admission and cancels the cursor on loss of authority. The
 * classifier independently retains known source issues before returning INDETERMINATE; it must not
 * turn execution failures into that disposition. No sorting or preview truncation is performed.
 */
public final class ValidationRelationReader {
    public record Evaluation(ValidationRelationRows.Disposition disposition, ValidationValues.Row working,
                             Optional<ValidationPhysicalIdentity> physical) {
        public Evaluation {
            Objects.requireNonNull(disposition, "disposition");
            Objects.requireNonNull(working, "working");
            Objects.requireNonNull(physical, "physical");
        }
    }
    private ValidationRelationReader() {}

    /** Owns and closes the cursor on every outcome; only normal complete EOF yields a result. */
    public static ValidationRelationRows.Result read(ValidationObservation observation, String readId, String contractVersion,
            ValidationRowCursor cursor, ValidationRelationRows.Limits limits, Clock clock, Runnable requireAuthorized,
            Function<ValidationValues.Row, Evaluation> classify) {
        Objects.requireNonNull(cursor, "cursor");
        var failure = new ValidationFailureLatch();
        ValidationRelationRows staging = null;
        ValidationRelationRows.Result result = null;
        try {
            Objects.requireNonNull(clock, "clock");
            Objects.requireNonNull(requireAuthorized, "requireAuthorized");
            Objects.requireNonNull(classify, "classify");
            // Validate the read metadata before consuming any business rows.
            var started = clock.instant();
            new ValidationCheckEvent.ReadBoundary(readId, contractVersion, started, started);
            staging = new ValidationRelationRows(observation, readId, limits);
            long ordinal = 0;
            while (true) {
                requireAuthorized.run();
                var next = Objects.requireNonNull(cursor.next(), "cursor result");
                if (next.isEmpty()) break;
                var row = next.orElseThrow();
                try {
                    requireAuthorized.run();
                    if (ordinal == Long.MAX_VALUE || row.ordinal() != ordinal++) {
                        throw new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", readId), null);
                    }
                    var original = Objects.requireNonNull(row.values(), "row values");
                    if (original.accountedBytes() > limits.row().bytes() || original.nodes() > limits.row().nodes()
                            || original.depth() > limits.row().depth()) {
                        throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "relationRead.row"), null);
                    }
                    var evaluated = Objects.requireNonNull(classify.apply(original), "row classification");
                    requireAuthorized.run();
                    staging.accept(observation, readId, evaluated.disposition(),
                            new ValidationRelationRows.Sample(original, evaluated.working(), evaluated.physical()));
                } catch (RuntimeException | Error error) {
                    failure.record(error);
                } finally {
                    try { row.close(); }
                    catch (RuntimeException | Error cleanup) { recordCleanup(failure, cleanup); }
                }
                throwFailure(failure);
            }
            requireAuthorized.run();
            // The interval describes this consumption, not a snapshot or a source commit boundary.
            staging.complete(observation, new ValidationCheckEvent.ReadBoundary(readId, contractVersion, started, clock.instant()));
            result = staging.finish();
        } catch (RuntimeException | Error error) {
            failure.record(error);
        } finally {
            if (staging != null) staging.close();
            try { cursor.close(); }
            catch (RuntimeException | Error cleanup) { recordCleanup(failure, cleanup); }
        }
        throwFailure(failure);
        requireAuthorized.run();
        return Objects.requireNonNull(result, "completed relation result");
    }

    private static void recordCleanup(ValidationFailureLatch failure, Throwable cleanup) {
        failure.record(cleanup);
        ValidationFailureLatch.addContext(failure.get(), cleanup);
    }
    private static void throwFailure(ValidationFailureLatch failure) {
        if (failure.get() instanceof Error error) throw error;
        if (failure.get() instanceof RuntimeException error) throw error;
    }
}
