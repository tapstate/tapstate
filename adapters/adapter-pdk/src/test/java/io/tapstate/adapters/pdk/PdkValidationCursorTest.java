package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkValidationCursorTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(4096, 100, 12);

    @Test void streamsBeyondPreviewRowLimitWithoutRetainingTheWholeRead() {
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 for (int i = 0; i < 25001; i++) consumer.accept(result(Map.of("id", i)));
             }, row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { }, worker, 2, LIMITS)) {
            int count = 0;
            while (true) {
                var next = cursor.next();
                if (next.isEmpty()) break;
                try (var row = next.orElseThrow()) {
                    assertThat(row.ordinal()).isEqualTo(count);
                    assertThat(row.values().fields()).containsEntry("id", count);
                    count++;
                }
            }
            assertThat(count).isEqualTo(25001);
            assertThat(cursor.availableRowSlots()).isEqualTo(2);
        }
    }

    @Test void cumulativeReadSizeDoesNotBecomeAPreviewTotalSizeLimit() {
        var limits = new ValidationValues.Limits(256 * 1024, 100, 12);
        String text = "x".repeat(65536);
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 for (int i = 0; i < 600; i++) consumer.accept(result(Map.of("id", i, "body", text)));
             }, row -> ValidationValues.ownRow(row, limits), () -> { }, () -> { }, worker, 2, limits)) {
            long characters = 0;
            int count = 0;
            while (true) {
                var next = cursor.next();
                if (next.isEmpty()) break;
                try (var row = next.orElseThrow()) {
                    characters += ((String) row.values().fields().get("body")).length();
                    count++;
                }
            }
            assertThat(count).isEqualTo(600);
            assertThat(characters).isGreaterThan(32L * 1024 * 1024);
        }
    }

    @Test void anEmptyCallbackDoesNotEndTheReadBeforeTheFunctionReturns() throws Exception {
        CountDownLatch emptySent = new CountDownLatch(1);
        CountDownLatch continueQuery = new CountDownLatch(1);
        try (var worker = Executors.newSingleThreadExecutor(); var caller = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 consumer.accept(result());
                 emptySent.countDown();
                 continueQuery.await();
                 consumer.accept(result(Map.of("id", 1)));
             }, row -> ValidationValues.ownRow(row, LIMITS), () -> { }, continueQuery::countDown, worker, 1, LIMITS)) {
            try {
                assertThat(emptySent.await(3, TimeUnit.SECONDS)).isTrue();
                var next = caller.submit(cursor::next);
                assertThatThrownBy(() -> next.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
                continueQuery.countDown();
                try (var row = next.get(3, TimeUnit.SECONDS).orElseThrow()) {
                    assertThat(row.values().fields()).containsEntry("id", 1);
                }
                assertThat(cursor.next()).isEmpty();
            } finally { continueQuery.countDown(); }
        }
    }

    @Test void callbackErrorRemainsFailureEvenIfTheConnectorSwallowsItAndReturns() {
        var cause = new IllegalStateException("fixture read error");
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 FilterResults result = result();
                 result.setError(cause);
                 try { consumer.accept(result); } catch (TapstateException swallowed) { }
             }, row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { }, worker, 1, LIMITS)) {
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class, failure -> {
                assertThat(failure.code()).isEqualTo(ValidationError.READ_FAILED);
                assertThat(failure.getCause()).isSameAs(cause);
            });
        }
    }

    @Test void ordinaryFailureCannotHideALaterFatalError() throws Exception {
        var fatal = new AssertionError("fatal after invalid row");
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 try { consumer.accept(new FilterResults()); } catch (TapstateException swallowed) { }
                 throw fatal;
             }, row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { }, worker, 1, LIMITS)) {
            worker.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThatThrownBy(cursor::next).isSameAs(fatal);
            assertThat(fatal.getSuppressed()).hasSize(1);
            assertThat(((TapstateException)fatal.getSuppressed()[0]).code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION);
        }
    }

    @Test void fatalAdmissionStillCancelsOnlyItsOwnedQueryResource() {
        var fatal = new AssertionError("fatal admission");
        var cleaned = new AtomicInteger();
        try (var worker = Executors.newSingleThreadExecutor()) {
            assertThatThrownBy(() -> new PdkValidationCursor("table", sink -> { },
                    row -> ValidationValues.ownRow(row, LIMITS), () -> { throw fatal; },
                    cleaned::incrementAndGet, worker, 1, LIMITS)).isSameAs(fatal);
            assertThat(cleaned).hasValue(1);
        }
    }

    @Test void fatalAdmissionCleanupEscapesWithTheEarlierRefusalAsContext() {
        var refusal = denied();
        var fatal = new AssertionError("fatal admission cleanup");
        try (var worker = Executors.newSingleThreadExecutor()) {
            assertThatThrownBy(() -> new PdkValidationCursor("table", sink -> { },
                    row -> ValidationValues.ownRow(row, LIMITS), () -> { throw refusal; },
                    () -> { throw fatal; }, worker, 1, LIMITS)).isSameAs(fatal);
            assertThat(fatal.getSuppressed()).containsExactly(refusal);
        }
    }

    @Test void nullResultPayloadIsAProtocolViolationRatherThanAnEmptyTable() {
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> consumer.accept(new FilterResults()),
                     row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { }, worker, 1, LIMITS)) {
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION));
        }
    }

    @Test void closeReleasesQueuedRowsButKeepsBorrowedRowsCharged() throws Exception {
        AtomicInteger decoded = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        CountDownLatch secondDecoded = new CountDownLatch(1);
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 for (int i = 0; i < 100; i++) consumer.accept(result(Map.of("id", i)));
             }, row -> {
                 if (decoded.incrementAndGet() == 2) secondDecoded.countDown();
                 return ValidationValues.ownRow(row, LIMITS);
             }, () -> { }, cancelled::incrementAndGet, worker, 2, LIMITS)) {
            var borrowed = cursor.next().orElseThrow();
            try {
                assertThat(secondDecoded.await(3, TimeUnit.SECONDS)).isTrue();
                assertThat(decoded).hasValue(2);
                cursor.close();
                worker.submit(() -> { }).get(3, TimeUnit.SECONDS);
                assertThat(cursor.availableRowSlots()).isEqualTo(1);
                assertThat(cancelled).hasValue(1);
                assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_CANCELLED));
            } finally { borrowed.close(); }
            borrowed.close();
            assertThat(cursor.availableRowSlots()).isEqualTo(2);
            assertThatThrownBy(borrowed::values).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void authorityIsCheckedAgainAfterDecodingBeforePublishingTheRow() throws Exception {
        AtomicBoolean authorized = new AtomicBoolean(true);
        CountDownLatch decoded = new CountDownLatch(1);
        Runnable guard = () -> {
            if (!authorized.get()) throw denied();
        };
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> consumer.accept(result(Map.of("id", 1))),
                     row -> {
                         var value = ValidationValues.ownRow(row, LIMITS);
                         authorized.set(false);
                         decoded.countDown();
                         return value;
                     }, guard, () -> { }, worker, 1, LIMITS)) {
            assertThat(decoded.await(3, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.OWNER_NOT_AUTHORIZED));
            worker.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThat(cursor.availableRowSlots()).isEqualTo(1);
        }
    }

    @Test void lateCallbacksCannotReopenTheCompletedInvocation() {
        AtomicReference<Consumer<FilterResults>> callback = new AtomicReference<>();
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", callback::set,
                     row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { }, worker, 1, LIMITS)) {
            assertThat(cursor.next()).isEmpty();
            assertThatThrownBy(() -> callback.get().accept(result(Map.of("id", 1))))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION));
            assertThatThrownBy(cursor::next).isInstanceOf(TapstateException.class);
            assertThat(cursor.availableRowSlots()).isEqualTo(1);
        }
    }

    @Test void initialAuthorizationFailureClosesOnlyTheSuppliedQueryResource() {
        AtomicInteger cancelled = new AtomicInteger();
        AtomicInteger called = new AtomicInteger();
        try (var worker = Executors.newSingleThreadExecutor()) {
            assertThatThrownBy(() -> new PdkValidationCursor("table", consumer -> called.incrementAndGet(),
                    row -> ValidationValues.ownRow(row, LIMITS), () -> { throw denied(); },
                    cancelled::incrementAndGet, worker, 1, LIMITS)).isInstanceOf(TapstateException.class);
            assertThat(cancelled).hasValue(1);
            assertThat(called).hasValue(0);
        }
    }

    @Test void rejectedIoSubmissionIsCodedAndReleasesTheQuery() {
        AtomicInteger cancelled = new AtomicInteger();
        try (var worker = Executors.newSingleThreadExecutor()) {
            worker.shutdown();
            assertThatThrownBy(() -> new PdkValidationCursor("table", consumer -> { },
                    row -> ValidationValues.ownRow(row, LIMITS), () -> { }, cancelled::incrementAndGet,
                    worker, 1, LIMITS)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_FAILED));
            assertThat(cancelled).hasValue(1);
        }
    }

    @Test void decoderCannotPublishRowsBeyondTheAdmittedDepth() {
        var shallow = new ValidationValues.Limits(4096, 100, 2);
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> consumer.accept(result(Map.of("a", Map.of("b", 1)))),
                     row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { }, worker, 1, shallow)) {
            assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        }
    }

    @Test void cleanupFailureDoesNotReplaceTheReadFailure() {
        var primary = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "table"), null);
        var cleanup = new IllegalStateException("fixture cleanup failed");
        try (var worker = Executors.newSingleThreadExecutor()) {
            var cursor = new PdkValidationCursor("table", consumer -> { throw primary; },
                    row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { throw cleanup; }, worker, 1, LIMITS);
            assertThatThrownBy(cursor::next).isSameAs(primary);
            cursor.close();
            assertThat(primary.getSuppressed()).containsExactly(cleanup);
        }
    }

    @Test void interruptedConsumptionCannotLaterReportSuccessfulEof() throws Exception {
        CountDownLatch continueQuery = new CountDownLatch(1);
        CountDownLatch polling = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        AtomicInteger cancelled = new AtomicInteger();
        Runnable guard = () -> {
            if (Thread.currentThread().getName().equals("validation-interrupted-reader")) polling.countDown();
        };
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 boolean released = false;
                 while (!released) {
                     try { continueQuery.await(); released = true; }
                     catch (InterruptedException ignored) { /* Model a query that returns normally after cancellation. */ }
                 }
             }, row -> ValidationValues.ownRow(row, LIMITS), guard,
                     () -> { cancelled.incrementAndGet(); continueQuery.countDown(); }, worker, 1, LIMITS)) {
            Thread reader = new Thread(() -> {
                try { cursor.next(); }
                catch (Throwable failure) { observed.set(failure); }
                finally { Thread.interrupted(); stopped.countDown(); }
            }, "validation-interrupted-reader");
            try {
                reader.start();
                assertThat(polling.await(3, TimeUnit.SECONDS)).isTrue();
                reader.interrupt();
                assertThat(stopped.await(3, TimeUnit.SECONDS)).isTrue();
                assertThat(observed.get()).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_CANCELLED));
                continueQuery.countDown();
                worker.submit(() -> { }).get(3, TimeUnit.SECONDS);
                assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_CANCELLED));
                assertThat(cancelled).hasValue(1);
            } finally {
                continueQuery.countDown();
                cursor.close();
                reader.join(3000);
            }
        }
    }

    @Test void recordedReadFailureSurvivesLaterAuthorityLossAndClose() throws Exception {
        for (boolean explicitlyClose : List.of(false, true)) {
            var primary = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "table"), null);
            AtomicBoolean authorized = new AtomicBoolean(true);
            try (var worker = Executors.newSingleThreadExecutor();
                 var cursor = new PdkValidationCursor("table", consumer -> { throw primary; },
                         row -> ValidationValues.ownRow(row, LIMITS),
                         () -> { if (!authorized.get()) throw denied(); }, () -> { }, worker, 1, LIMITS)) {
                worker.submit(() -> { }).get(3, TimeUnit.SECONDS);
                if (explicitlyClose) cursor.close();
                else authorized.set(false);
                assertThatThrownBy(cursor::next).isSameAs(primary);
            }
        }
    }

    @Test void explicitCancellationRemainsTheCauseWhenTheQueryThenThrowsInterrupted() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var worker = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 entered.countDown();
                 release.await();
             }, row -> ValidationValues.ownRow(row, LIMITS), () -> { }, () -> { }, worker, 1, LIMITS)) {
            try {
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                cursor.close();
                worker.submit(() -> { }).get(3, TimeUnit.SECONDS);
                assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.READ_CANCELLED));
            } finally { release.countDown(); }
        }
    }

    @Test void workerExitDoesNotFollowCancellationUntilAnIgnoringQueryReallyReturns() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var releasedReservations = new AtomicInteger();
        try (var io = Executors.newSingleThreadExecutor();
             var cursor = new PdkValidationCursor("table", consumer -> {
                 entered.countDown();
                 while (true) {
                     try { if (!release.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture timed out"); break; }
                     catch (InterruptedException ignored) { }
                 }
             }, row -> ValidationValues.ownRow(row, LIMITS), () -> {}, () -> {}, io, 1, LIMITS)) {
            try {
                cursor.workersExited().whenComplete((ignored, error) -> releasedReservations.incrementAndGet());
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                cursor.close();
                assertThat(cursor.workersExited().toCompletableFuture().isDone()).isFalse();
                assertThat(releasedReservations).hasValue(0);
                release.countDown(); io.submit(() -> {}).get(3, TimeUnit.SECONDS);
                assertThatThrownBy(() -> cursor.workersExited().toCompletableFuture().get(3, TimeUnit.SECONDS))
                        .isInstanceOfSatisfying(java.util.concurrent.ExecutionException.class, error ->
                                assertThat(error.getCause()).isInstanceOfSatisfying(TapstateException.class,
                                        cause -> assertThat(cause.code()).isEqualTo(ValidationError.READ_CANCELLED)));
                assertThat(releasedReservations).hasValue(1);
            } finally { release.countDown(); }
        }
    }

    @Test void preStartCancellationProvesNoQueryCanStartButNeverPretendsSuccessfulEof() throws Exception {
        var occupied = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var queries = new AtomicInteger();
        try (var io = Executors.newSingleThreadExecutor()) {
            io.execute(() -> {
                occupied.countDown();
                try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            });
            assertThat(occupied.await(3, TimeUnit.SECONDS)).isTrue();
            try (var cursor = new PdkValidationCursor("table", consumer -> queries.incrementAndGet(),
                    row -> ValidationValues.ownRow(row, LIMITS), () -> {}, () -> {}, io, 1, LIMITS)) {
                cursor.close();
                assertThat(cursor.workersExited().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
                assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ValidationError.READ_CANCELLED));
                release.countDown(); io.submit(() -> {}).get(3, TimeUnit.SECONDS);
                assertThat(queries).hasValue(0);
            } finally { release.countDown(); }
        } finally { release.countDown(); }
    }

    @Test void closingAfterNormalEofDoesNotTurnActualWorkerExitIntoAReadFailure() throws Exception {
        try (var io = Executors.newSingleThreadExecutor()) {
            for (int i = 0; i < 50; i++) {
                try (var cursor = new PdkValidationCursor("table", consumer -> {}, row -> ValidationValues.ownRow(row, LIMITS),
                        () -> {}, () -> {}, io, 1, LIMITS)) {
                    assertThat(cursor.next()).isEmpty();
                    cursor.close();
                    assertThat(cursor.workersExited().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
                }
            }
        }
    }

    @SafeVarargs private static FilterResults result(Map<String, Object>... rows) {
        FilterResults result = new FilterResults();
        result.setResults(List.of(rows));
        return result;
    }

    private static TapstateException denied() {
        return new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
    }
}
