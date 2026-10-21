package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationRowCursor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.function.LongFunction;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationOrderedMergeTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(16384, 100, 12);

    @Test void pairsCompleteGroupsAndPreservesBothPhysicalIdentitiesAndSingleSidedTails() {
        var left = rows("source_key", 1, 3, 3, 7);
        var right = rows("target_key", 2, 3, 8);
        var facts = new ArrayList<String>();
        try (var merge = merge(left, right)) {
            while (true) {
                var next = merge.next();
                if (next.isEmpty()) break;
                try (var pair = next.orElseThrow()) {
                    var l = pair.source();
                    var r = pair.target();
                    if (l.isPresent()) assertThat(l.orElseThrow().key().physical().keyFields())
                            .containsExactly(List.of("source_key"));
                    if (r.isPresent()) assertThat(r.orElseThrow().key().physical().keyFields())
                            .containsExactly(List.of("target_key"));
                    facts.add(pair.state() + ":" + l.map(group -> group.sampleValues().fields().get("source_key")
                            + "x" + group.occurrences()).orElse("-") + ":"
                            + r.map(group -> group.sampleValues().fields().get("target_key")
                            + "x" + group.occurrences()).orElse("-"));
                }
            }
            assertThat(merge.next()).isEmpty();
        }
        assertThat(facts).containsExactly("SOURCE_ONLY:1x1:-", "TARGET_ONLY:-:2x1", "MATCHED:3x2:3x1",
                "SOURCE_ONLY:7x1:-", "TARGET_ONLY:-:8x1");
        assertThat(left.closed).isTrue();
        assertThat(right.closed).isTrue();
        assertThat(left.borrowed).hasValue(0);
        assertThat(right.borrowed).hasValue(0);
    }

    @Test void entireDuplicateGroupIsCountedAcrossOneRowDeliverySlotsWithOnlyASampleRetained() {
        var rows = new Rows("source_key", 30000, ordinal -> Map.of("source_key", 4L, "sample", ordinal));
        try (var groups = groups(rows); var group = groups.next().orElseThrow()) {
            assertThat(group.occurrences()).isEqualTo(30000);
            assertThat(group.sampleValues().fields()).containsEntry("sample", 0L);
            assertThat(rows.delivered).isEqualTo(30000);
            assertThat(rows.maxBorrowed).isEqualTo(1);
            assertThat(rows.borrowed).hasValue(0);
        }
    }

    @Test void streamsPastPreviewCountAndCumulativeByteLimitsAndCompletesBothTails() {
        String text = "x".repeat(2048);
        var source = new Rows("source_key", 25001, n -> Map.of("source_key", n, "body", text));
        var target = new Rows("target_key", 25002, n -> Map.of("target_key", n, "body", text));
        long pairs = 0;
        long bytes = 0;
        try (var merge = merge(source, target)) {
            while (true) {
                var next = merge.next();
                if (next.isEmpty()) break;
                try (var pair = next.orElseThrow()) {
                    bytes += pair.target().orElseThrow().sampleValues().accountedBytes();
                    pairs++;
                }
            }
        }
        assertThat(pairs).isEqualTo(25002);
        assertThat(bytes).isGreaterThan(32L * 1024 * 1024);
        assertThat(source.maxBorrowed).isEqualTo(1);
        assertThat(target.maxBorrowed).isEqualTo(1);
    }

    @Test void emptySourceStillDeliversEveryTargetGroupAndEmptyPairIsNeverEmitted() {
        try (var merge = merge(rows("source_key"), rows("target_key", 1, 2))) {
            try (var first = merge.next().orElseThrow()) {
                assertThat(first.source()).isEmpty();
                assertThat(first.target().orElseThrow().sampleValues().fields()).containsEntry("target_key", 1L);
            }
            try (var second = merge.next().orElseThrow()) {
                assertThat(second.source()).isEmpty();
                assertThat(second.target().orElseThrow().sampleValues().fields()).containsEntry("target_key", 2L);
            }
            assertThat(merge.next()).isEmpty();
        }
        try (var empty = merge(rows("source_key"), rows("target_key"))) {
            assertThat(empty.next()).isEmpty();
        }
    }

    @Test void decreasingNativeOrderFailsAndCancelsBothReadersRatherThanSorting() {
        var source = rows("source_key", 1, 3, 2);
        var target = rows("target_key", 1, 2, 3);
        try (var merge = merge(source, target)) {
            try (var first = merge.next().orElseThrow()) {
                assertThat(first.source().orElseThrow().sampleValues().fields()).containsEntry("source_key", 1L);
            }
            assertCode(merge::next, ValidationError.READ_PROTOCOL_VIOLATION);
            assertCode(merge::next, ValidationError.READ_PROTOCOL_VIOLATION);
            assertThat(source.closed).isTrue();
            assertThat(target.closed).isTrue();
        }
    }

    @Test void incompatibleKeyDomainsAreAnErrorEvenForDifferentLeadingComponents() {
        var source = rows("source_key", 1);
        var target = rows("target_key", 2);
        try (var merge = new ValidationOrderedMerge(groups(source), new ValidationOrderedGroups("target", target,
                row -> {
                    ValidationKey key = new ValidationKey(List.of(ValidationKey.Part.text("2")));
                    return new ValidationOrderedGroups.Key(key, identity(target, key));
                }, LIMITS.bytes()))) {
            assertCode(merge::next, ValidationError.INCOMPATIBLE_KEY);
        }
    }

    @Test void anOrdinalGapDoesNotSilentlyBecomeAMissingDataCandidate() {
        var source = rows("source_key", 1, 2);
        source.ordinalDeltaAfterFirst = 1;
        var target = rows("target_key", 1, 2);
        try (var merge = merge(source, target)) {
            assertCode(merge::next, ValidationError.READ_PROTOCOL_VIOLATION);
            assertThat(source.borrowed).hasValue(0);
            assertThat(target.closed).isTrue();
        }
    }

    @Test void incompleteGroupReadFailurePreservesTheCauseAndReleasesBothInputs() {
        var source = rows("source_key", 1, 1);
        var target = rows("target_key", 1);
        var cause = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "source"), null);
        source.failAt = 1;
        source.readFailure = cause;
        target.closeFailure = new IllegalStateException("cleanup failed");
        try (var merge = merge(source, target)) {
            assertThatThrownBy(merge::next).isSameAs(cause);
            assertThat(cause.getSuppressed()).containsExactly(target.closeFailure);
            assertThat(source.borrowed).hasValue(0);
            assertThat(target.closed).isTrue();
            assertThatThrownBy(merge::next).isSameAs(cause);
        }
    }

    @Test void overBudgetSampleFailsBeforeRetainingItAsAGroup() {
        var source = new Rows("source_key", 1, n -> Map.of("source_key", n, "body", "x".repeat(3000)));
        try (var groups = new ValidationOrderedGroups("source", source, row -> key(source, row), 1024)) {
            assertCode(groups::next, ValidationError.VALUE_LIMIT_EXCEEDED);
            assertThat(source.borrowed).hasValue(0);
            assertThat(source.closed).isTrue();
        }
    }

    @Test void advancingWithAnUnreleasedPairCannotAccumulateRetainedSamples() {
        var source = rows("source_key", 1, 2, 3);
        var target = rows("target_key", 1, 2, 3);
        try (var merge = merge(source, target)) {
            var pair = merge.next().orElseThrow();
            assertThatThrownBy(merge::next).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(pair::source).isInstanceOf(IllegalStateException.class);
            assertThat(source.closed).isTrue();
            assertThat(target.closed).isTrue();
        }
    }

    @Test void explicitCloseCancelsFurtherReadsAndClosesTheOutstandingGroup() {
        var source = rows("source_key", 1, 2);
        try (var groups = groups(source)) {
            var first = groups.next().orElseThrow();
            groups.close();
            assertThat(first.isClosed()).isTrue();
            assertThatThrownBy(first::sampleValues).isInstanceOf(IllegalStateException.class);
            assertCode(groups::next, ValidationError.READ_CANCELLED);
        }
    }

    @Test void closeDuringProjectionCannotLeaveALateLookaheadRetained() throws Exception {
        var source = rows("source_key", 1, 2);
        CountDownLatch projectingSecond = new CountDownLatch(1);
        CountDownLatch continueProjection = new CountDownLatch(1);
        try (var io = Executors.newSingleThreadExecutor();
             var groups = new ValidationOrderedGroups("source", source, row -> {
                 if (row.fields().get("source_key").equals(2L)) {
                     projectingSecond.countDown();
                     try { continueProjection.await(); }
                     catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                 }
                 return key(source, row);
             }, LIMITS.bytes())) {
            var next = io.submit(groups::next);
            try {
                assertThat(projectingSecond.await(3, TimeUnit.SECONDS)).isTrue();
                groups.close();
                continueProjection.countDown();
                assertThatThrownBy(() -> next.get(3, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                        failed -> assertThat(((TapstateException)failed.getCause()).code())
                                .isEqualTo(ValidationError.READ_CANCELLED));
                // Inspect retained ownership after the call exits, not GC timing or approximate heap size.
                var retained = ValidationOrderedGroups.class.getDeclaredField("lookahead");
                retained.setAccessible(true);
                assertThat(retained.get(groups)).isNull();
                assertThat(source.borrowed).hasValue(0);
            } finally { continueProjection.countDown(); }
        }
    }

    @Test void mergeCloseDoesNotWaitOnTheLockHeldByABlockedInputRead() throws Exception {
        var source = rows("source_key", 1);
        var target = rows("target_key", 1);
        CountDownLatch projecting = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var io = Executors.newSingleThreadExecutor();
             var merge = new ValidationOrderedMerge(groups(source), new ValidationOrderedGroups("target", target,
                     row -> {
                         projecting.countDown();
                         try { release.await(); }
                         catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                         return key(target, row);
                     }, LIMITS.bytes()))) {
            var next = io.submit(merge::next);
            try {
                assertThat(projecting.await(3, TimeUnit.SECONDS)).isTrue();
                merge.close();
                assertThat(source.closed).isTrue();
                assertThat(target.closed).isTrue();
                release.countDown();
                assertThatThrownBy(() -> next.get(3, TimeUnit.SECONDS)).isInstanceOfSatisfying(ExecutionException.class,
                        failed -> assertThat(((TapstateException)failed.getCause()).code())
                                .isEqualTo(ValidationError.READ_CANCELLED));
            } finally { release.countDown(); }
        }
    }

    @Test void groupAccessRacingCloseReturnsASnapshotOrReleaseNeverNull() throws Exception {
        var source = new Rows("source_key", 20000, n -> Map.of("source_key", n));
        var active = new AtomicReference<ValidationOrderedGroups.Group>();
        var acknowledged = new AtomicReference<ValidationOrderedGroups.Group>();
        var stopped = new AtomicBoolean();
        var unexpected = new AtomicReference<Throwable>();
        try (var worker = Executors.newSingleThreadExecutor(); var groups = groups(source)) {
            var access = worker.submit(() -> {
                while (!stopped.get()) {
                    var group = active.get();
                    if (group == null) { Thread.onSpinWait(); continue; }
                    acknowledged.set(group);
                    try {
                        assertThat(group.key()).isNotNull();
                        assertThat(group.sampleValues()).isNotNull();
                    } catch (IllegalStateException released) {
                        // Explicit release is a valid outcome when close won the race.
                    } catch (Throwable failure) {
                        unexpected.compareAndSet(null, failure);
                        return;
                    }
                }
            });
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
                for (int i = 0; i < 20000 && unexpected.get() == null; i++) {
                    try (var group = groups.next().orElseThrow()) {
                        active.set(group);
                        while (acknowledged.get() != group && unexpected.get() == null) {
                            if (System.nanoTime() >= deadline) throw new AssertionError("access worker did not acknowledge group");
                            Thread.onSpinWait();
                        }
                    }
                }
            } finally {
                stopped.set(true);
                access.get(3, TimeUnit.SECONDS);
            }
            assertThat(unexpected.get()).isNull();
        }
    }

    private static ValidationOrderedMerge merge(Rows source, Rows target) {
        return new ValidationOrderedMerge(groups(source), groups(target));
    }

    private static ValidationOrderedGroups groups(Rows rows) {
        return new ValidationOrderedGroups(rows.field, rows, row -> key(rows, row), LIMITS.bytes());
    }

    private static ValidationOrderedGroups.Key key(Rows rows, ValidationValues.Row row) {
        long id = ((Number)row.fields().get(rows.field)).longValue();
        ValidationKey key = new ValidationKey(List.of(ValidationKey.Part.number(id)));
        return new ValidationOrderedGroups.Key(key, identity(rows, key));
    }

    private static ValidationPhysicalIdentity identity(Rows rows, ValidationKey actual) {
        return new ValidationPhysicalIdentity(rows.field + "-source", "database", "schema", "records",
                List.of(List.of(rows.field)), actual);
    }

    private static Rows rows(String field, long... ids) {
        return new Rows(field, ids.length, n -> Map.of(field, ids[(int)n]));
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, ValidationError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(code));
    }

    /** A single borrowed row is allowed, so retaining cursor leases to gather a group cannot pass. */
    private static final class Rows implements ValidationRowCursor {
        private final String field;
        private final long count;
        private final LongFunction<Map<String, Object>> values;
        private final AtomicInteger borrowed = new AtomicInteger();
        private int maxBorrowed;
        private long delivered;
        private boolean closed;
        private int ordinalDeltaAfterFirst;
        private long failAt = -1;
        private RuntimeException readFailure;
        private RuntimeException closeFailure;

        private Rows(String field, long count, LongFunction<Map<String, Object>> values) {
            this.field = field;
            this.count = count;
            this.values = values;
        }

        @Override public Optional<Row> next() {
            if (closed) throw new IllegalStateException("fixture read after close");
            if (delivered == failAt) throw readFailure;
            if (delivered == count) return Optional.empty();
            assertThat(borrowed).as("one delivery slot").hasValue(0);
            long ordinal = delivered++;
            var row = ValidationValues.ownRow(values.apply(ordinal), LIMITS);
            maxBorrowed = Math.max(maxBorrowed, borrowed.incrementAndGet());
            return Optional.of(new Row() {
                private boolean released;
                @Override public long ordinal() { return ordinal + (ordinal == 0 ? 0 : ordinalDeltaAfterFirst); }
                @Override public ValidationValues.Row values() { return row; }
                @Override public void close() { if (!released) { released = true; borrowed.decrementAndGet(); } }
            });
        }

        @Override public void close() {
            closed = true;
            if (closeFailure != null) throw closeFailure;
        }
    }
}
