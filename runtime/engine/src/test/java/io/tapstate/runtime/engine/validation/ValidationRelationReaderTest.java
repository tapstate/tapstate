package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationRelationReaderTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final ValidationRelationRows.Limits LIMITS = new ValidationRelationRows.Limits(32768, ROW);
    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

    @Test void normalEofRetainsOnlyBoundedSamplesAndEveryDeliveryLeaseIsClosed() {
        var cursor = new Cursor(20000);
        var result = read(cursor, LIMITS, () -> {}, row -> new ValidationRelationReader.Evaluation(
                ValidationRelationRows.Disposition.INCLUDED, values(Map.of("price", 9)), Optional.empty()));
        assertThat(result.included()).isEqualTo(20000);
        assertThat(result.includedSamples()).hasSize(2);
        assertThat(result.includedSamples().getFirst().original().fields()).containsEntry("amount", 9).doesNotContainKey("price");
        assertThat(result.includedSamples().getFirst().working().fields()).containsEntry("price", 9).doesNotContainKey("amount");
        assertThat(cursor.rowsClosed).hasValue(20000);
        assertThat(cursor.closed).isTrue();
        assertThat(result.read().readId()).isEqualTo("relation");
    }

    @Test void classificationAccountsForExcludedAndIndeterminateRowsWithoutInventingUniqueness() {
        var cursor = new Cursor(3);
        var dispositions = List.of(ValidationRelationRows.Disposition.INCLUDED, ValidationRelationRows.Disposition.EXCLUDED,
                ValidationRelationRows.Disposition.INDETERMINATE);
        var current = new AtomicInteger();
        var result = read(cursor, LIMITS, () -> {}, row -> new ValidationRelationReader.Evaluation(dispositions.get(current.getAndIncrement()), row, Optional.empty()));
        assertThat(result.included()).isEqualTo(1);
        assertThat(result.excluded()).isEqualTo(1);
        assertThat(result.indeterminate()).isEqualTo(1);
        assertThat(result.unique()).isEmpty();
        assertThat(result.definitelyEmpty()).isFalse();
    }

    @Test void onlyNormalEmptyEofCanProduceAnEmptyRelation() {
        var empty = new Cursor(0);
        assertThat(read(empty, LIMITS, () -> {}, ValidationRelationReaderTest::included).definitelyEmpty()).isTrue();
        assertThat(empty.closed).isTrue();
        var broken = new Cursor(0);
        var readFailure = failure(ValidationError.READ_FAILED);
        broken.readFailure = readFailure;
        assertThat(catchThrowable(() -> read(broken, LIMITS, () -> {}, ValidationRelationReaderTest::included))).isSameAs(readFailure);
        assertThat(broken.closed).isTrue();
    }

    @Test void lostAuthorityDuringClassificationOrFinalCleanupCannotReturnAResult() {
        for (boolean duringCleanup : List.of(false, true)) {
            var authorized = new AtomicBoolean(true);
            var cursor = new Cursor(1);
            if (duringCleanup) cursor.afterClose = () -> authorized.set(false);
            var lost = failure(ValidationError.OWNER_NOT_AUTHORIZED);
            assertThat(catchThrowable(() -> read(cursor, LIMITS, () -> { if (!authorized.get()) throw lost; }, row -> {
                if (!duringCleanup) authorized.set(false);
                return included(row);
            }))).isSameAs(lost);
            assertThat(cursor.closed).isTrue();
            assertThat(cursor.rowsClosed).hasValue(1);
        }
    }

    @Test void protocolAndInputBudgetsAreCheckedBeforeRunningRulesAndReleaseTheBorrowedRow() {
        for (boolean protocol : List.of(true, false)) {
            var cursor = new Cursor(1);
            if (protocol) cursor.ordinalOffset = 1;
            var calls = new AtomicInteger();
            var limits = protocol ? LIMITS : new ValidationRelationRows.Limits(32768, new ValidationValues.Limits(1, 1, 1));
            assertThatThrownBy(() -> read(cursor, limits, () -> {}, row -> { calls.incrementAndGet(); return included(row); }))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(
                            protocol ? ValidationError.READ_PROTOCOL_VIOLATION : ValidationError.VALUE_LIMIT_EXCEEDED));
            assertThat(calls).hasValue(0);
            assertThat(cursor.rowsClosed).hasValue(1);
            assertThat(cursor.closed).isTrue();
        }
    }

    @Test void firstOrdinaryFailureSurvivesCleanupAndFatalCleanupErrorsEscape() {
        for (boolean fatal : List.of(false, true)) {
            var cursor = new Cursor(1);
            var body = failure(ValidationError.RULE_UNSUPPORTED);
            Throwable cleanup = fatal ? new AssertionError("fatal cleanup") : failure(ValidationError.READ_CANCELLED);
            cursor.rowCleanup = cleanup;
            Throwable actual = catchThrowable(() -> read(cursor, LIMITS, () -> {}, row -> { throw body; }));
            assertThat(actual).isSameAs(fatal ? cleanup : body);
            assertThat(List.of(actual.getSuppressed())).contains(fatal ? body : cleanup);
            assertThat(cursor.closed).isTrue();
        }
    }

    @Test void evenCleanupAfterEofMustSucceedAndInvalidMetadataStillClosesTheCursor() {
        var cursor = new Cursor(0);
        var cleanup = failure(ValidationError.READ_FAILED);
        cursor.cursorCleanup = cleanup;
        assertThat(catchThrowable(() -> read(cursor, LIMITS, () -> {}, ValidationRelationReaderTest::included))).isSameAs(cleanup);
        var invalid = new Cursor(1);
        assertThatThrownBy(() -> ValidationRelationReader.read(observation(), "", "complete", invalid, LIMITS, CLOCK,
                () -> {}, ValidationRelationReaderTest::included)).isInstanceOf(TapstateException.class);
        assertThat(invalid.closed).isTrue();
        assertThat(invalid.delivered).isZero();
    }

    private static ValidationRelationRows.Result read(Cursor cursor, ValidationRelationRows.Limits limits,
            Runnable authority, Function<ValidationValues.Row, ValidationRelationReader.Evaluation> classify) {
        return ValidationRelationReader.read(observation(), "relation", "complete", cursor, limits, CLOCK, authority, classify);
    }
    private static ValidationRelationReader.Evaluation included(ValidationValues.Row row) {
        return new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED, row, Optional.empty());
    }
    private static ValidationValues.Row values(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static ValidationObservation observation() {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
                new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(102))), List.of()), 1);
    }
    private static TapstateException failure(ValidationError code) { return new TapstateException(code, Map.of(), null); }
    private static void raise(Throwable failure) {
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException error) throw error;
    }
    private static final class Cursor implements ValidationRowCursor {
        private final int count;
        private int delivered;
        private int ordinalOffset;
        private boolean closed;
        private boolean outstanding;
        private final AtomicInteger rowsClosed = new AtomicInteger();
        private Throwable readFailure;
        private Throwable rowCleanup;
        private Throwable cursorCleanup;
        private Runnable afterClose = () -> {};
        private Cursor(int count) { this.count = count; }
        @Override public Optional<Row> next() {
            if (closed || outstanding) throw new IllegalStateException("cursor contract violated");
            raise(readFailure);
            if (delivered == count) return Optional.empty();
            long ordinal = delivered++ + ordinalOffset;
            outstanding = true;
            return Optional.of(new Row() {
                @Override public long ordinal() { return ordinal; }
                @Override public ValidationValues.Row values() { return ValidationRelationReaderTest.values(Map.of("amount", 9)); }
                @Override public void close() { outstanding = false; rowsClosed.incrementAndGet(); raise(rowCleanup); }
            });
        }
        @Override public void close() { closed = true; afterClose.run(); raise(cursorCleanup); }
    }
}
