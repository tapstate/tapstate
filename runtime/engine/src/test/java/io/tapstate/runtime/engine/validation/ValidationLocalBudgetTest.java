package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Optional;
import io.tapstate.spi.store.ValidationReadHandle;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationLocalBudgetTest {
    private static final ValidationLocalBudget.Amount ONE = new ValidationLocalBudget.Amount(1024, 1, 1, 1);
    private static final ValidationLocalBudget.Amount TWO = new ValidationLocalBudget.Amount(2048, 2, 2, 2);
    private static final ValidationLocalBudget.Amount ZERO = new ValidationLocalBudget.Amount(0, 0, 0, 0);
    @Test void onlyTheOriginalAdmissionControllerCanGrowItsEnvelope(){
        try(var owner=budget(TWO);var other=budget(TWO)){
            var run=owner.admit(run("growth-owner"),ONE,()->{});
            assertThatThrownBy(()->other.prepareGrowth(run,TWO)).isInstanceOf(TapstateException.class);
            assertThat(run.envelope()).isEqualTo(ONE);var growing=owner.prepareGrowth(run,TWO);growing.commit();growing.commit();
            assertThat(run.envelope()).isEqualTo(TWO);assertThat(owner.snapshot().reserved()).isEqualTo(TWO);
            run.close();assertThat(owner.snapshot().reserved()).isEqualTo(ZERO);
        }
    }

    @Test void simultaneousRunsShareTheNodeEnvelopeBeforeAnyBusinessAllocation() {
        try (var budget = budget(TWO)) {
            var full = budget.admit(run("full"), ONE, () -> {});
            var incremental = budget.admit(run("incremental"), ONE, () -> {});
            assertThat(budget.snapshot().reserved()).isEqualTo(TWO);
            assertThatThrownBy(() -> budget.admit(run("third"), ONE, () -> {})).isInstanceOf(TapstateException.class);
            full.close();
            assertThat(budget.snapshot().reserved()).isEqualTo(ONE);
            var third = budget.admit(run("third"), ONE, () -> {});
            incremental.close(); third.close();
            assertThat(budget.snapshot().reserved()).isEqualTo(ZERO);
        }
    }

    @Test void retirementRetainsItsEnvelopeUntilTheLastOriginalReservationIsReturned() {
        try (var budget = budget(ONE)) {
            var run = budget.admit(run("old"), ONE, () -> {});
            var reservation = run.reserve(ONE);
            run.close(); run.close();
            assertThat(run.released().toCompletableFuture().isDone()).isFalse();
            assertThatThrownBy(() -> run.reserve(ONE)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> budget.admit(run("new"), ONE, () -> {})).isInstanceOf(TapstateException.class);
            reservation.close(); reservation.close();
            assertThat(run.released().toCompletableFuture()).isCompletedWithValue(null);
            var next = budget.admit(run("new"), ONE, () -> {});
            reservation.close();
            assertThat(budget.snapshot().reserved()).isEqualTo(ONE);
            next.close();
        }
    }

    @Test void everyDimensionIsEnforcedWithoutLeavingAPartialReservation() {
        for (var request : java.util.List.of(new ValidationLocalBudget.Amount(1025, 1, 1, 1),
                new ValidationLocalBudget.Amount(1024, 2, 1, 1), new ValidationLocalBudget.Amount(1024, 1, 2, 1),
                new ValidationLocalBudget.Amount(1024, 1, 1, 2))) {
            try (var budget = budget(ONE); var run = budget.admit(run("run"), ONE, () -> {})) {
                assertThatThrownBy(() -> run.reserve(request)).isInstanceOf(TapstateException.class);
                assertThat(run.used()).isEqualTo(ZERO);
                run.reserve(ONE).close();
            }
        }
    }

    @Test void duplicateRunAndForeignOwnerAreRejectedBeforeAllocation() {
        try (var budget = budget(TWO); var run = budget.admit(run("same"), ONE, () -> {})) {
            assertThatThrownBy(() -> budget.admit(run("same"), ONE, () -> {})).isInstanceOf(TapstateException.class);
            var normal = run("foreign");
            var owner = new ValidationOwner("cluster", "pipeline", "other-node", "boot", 1, 1, 0);
            var foreign = new ValidationRunIdentity(owner, normal.taskId(), normal.runId(), normal.mode(), normal.specHash(),
                    normal.keyMappingRevision(), normal.comparisonRulesVersion());
            assertThatThrownBy(() -> budget.admit(foreign, ONE, () -> {})).isInstanceOf(TapstateException.class);
            assertThat(budget.snapshot().runs()).isEqualTo(1);
            assertThat(budget.snapshot().reserved()).isEqualTo(ONE);
        }
    }

    @Test void lostHeadroomOrAuthorityPreventsNewWorkButCannotPreventRelease() {
        var headroom = new java.util.concurrent.atomic.AtomicBoolean(true);
        var authorized = new java.util.concurrent.atomic.AtomicBoolean(true);
        var denied = new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
        var pressure = new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "synchronization.headroom"), null);
        try (var budget = new ValidationLocalBudget("cluster", "node", "boot", ONE, 4, () -> { if (!headroom.get()) throw pressure; })) {
            var run = budget.admit(run("run"), ONE, () -> { if (!authorized.get()) throw denied; });
            var first = run.reserve(new ValidationLocalBudget.Amount(512, 0, 0, 0));
            headroom.set(false);
            assertThat(catchThrowable(() -> run.reserve(new ValidationLocalBudget.Amount(512, 0, 0, 0)))).isSameAs(pressure);
            authorized.set(false);
            assertThat(catchThrowable(() -> run.reserve(new ValidationLocalBudget.Amount(512, 0, 0, 0)))).isSameAs(denied);
            run.close(); first.close();
            assertThat(budget.snapshot().reserved()).isEqualTo(ZERO);
        }
    }

    @Test void racingReservationsCannotOverdrawTheLastSlot() throws Exception {
        try (var budget = budget(ONE); var run = budget.admit(run("run"), ONE, () -> {});
                var workers = java.util.concurrent.Executors.newFixedThreadPool(4)) {
            var start = new CountDownLatch(1); var won = new AtomicInteger(); var rejected = new AtomicInteger();
            var held = new java.util.concurrent.ConcurrentLinkedQueue<ValidationLocalBudget.Reservation>();
            var attempts = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 16; i++) attempts.add(workers.submit(() -> {
                await(start);
                try { held.add(run.reserve(ONE)); won.incrementAndGet(); }
                catch (TapstateException limited) { assertThat(limited.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED); rejected.incrementAndGet(); }
            }));
            start.countDown(); for (var attempt : attempts) attempt.get(3, TimeUnit.SECONDS);
            assertThat(won).hasValue(1); assertThat(rejected).hasValue(15);
            assertThat(run.used()).isEqualTo(ONE);
            held.forEach(ValidationLocalBudget.Reservation::close);
        }
    }

    @Test void maximumNumericCapacitiesCannotOverflowIntoFreeCapacity() {
        var maximum = new ValidationLocalBudget.Amount(Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        try (var budget = budget(maximum); var run = budget.admit(run("maximum"), maximum, () -> {})) {
            var all = run.reserve(maximum);
            assertThatThrownBy(() -> run.reserve(ONE)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> budget.admit(run("more"), ONE, () -> {})).isInstanceOf(TapstateException.class);
            all.close();
            assertThat(run.used()).isEqualTo(ZERO);
        }
    }

    @Test void nodeShutdownDoesNotReclaimReadResourcesStillHeldAfterCheckerExit() throws Exception {
        try (var cleanup = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1)); var budget = budget(ONE)) {
            var observation = ValidationRootWorkTest.observation(1);
            var run = budget.admit(observation.run(), ONE, () -> {});
            var reservation = run.reserve(ONE);
            var workers = new CompletableFuture<Void>(); var rows = new CompletableFuture<Void>(); var closed = new CountDownLatch(1);
            var scope = new ValidationReadScope(observation, cleanup, () -> {}, 1);
            scope.open("records", guard -> new ValidationReadHandle() {
                @Override public Optional<Row> next() { return Optional.empty(); }
                @Override public void close() { closed.countDown(); }
                @Override public CompletionStage<Void> workersExited() { return workers.minimalCompletionStage(); }
                @Override public CompletionStage<Void> rowsReleased() { return rows.minimalCompletionStage(); }
            });
            scope.drained().whenComplete((ignored, error) -> reservation.close());
            scope.close(); await(closed); budget.close(); workers.complete(null);
            assertThat(budget.snapshot().reserved()).isEqualTo(ONE);
            assertThat(run.released().toCompletableFuture().isDone()).isFalse();
            rows.complete(null);
            run.released().toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(budget.snapshot().reserved()).isEqualTo(ZERO);
            assertThatThrownBy(() -> budget.admit(run("after-shutdown"), ONE, () -> {})).isInstanceOf(TapstateException.class);
        }
    }

    private static ValidationRunIdentity run(String id) {
        var run = ValidationRootWorkTest.observation(1).run();
        return new ValidationRunIdentity(run.owner(), run.taskId(), id, run.mode(), run.specHash(), run.keyMappingRevision(), run.comparisonRulesVersion());
    }
    private static ValidationLocalBudget budget(ValidationLocalBudget.Amount capacity) {
        return new ValidationLocalBudget("cluster", "node", "boot", capacity, 4, () -> {});
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
