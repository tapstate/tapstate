package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KeyMoveHandoffsTest {
    @Test
    void twoUpdatesWaitingOnTheSameMoveEnterInSourceOrder() {
        var coordinator = new KeyMoveHandoffs<String>(4);
        var move = coordinator.tryRegister("A", "B");
        var first = coordinator.tryRegister("B", "B");
        var second = coordinator.tryRegister("B", "B");
        var admitted = new ArrayList<Integer>();
        first.ready().thenRun(() -> admitted.add(1));
        second.ready().thenRun(() -> admitted.add(2));
        assertThat(coordinator.tryRegister("C", "C").ready().toCompletableFuture()).isCompleted();
        move.arrival().queued();
        assertThat(admitted).isEmpty();
        move.departure().queued();
        assertThat(admitted).containsExactly(1);
        first.arrival().queued();
        assertThat(admitted).containsExactly(1, 2);
    }

    @Test
    void aRoundTripCanComputeBeforeItsFirstWriteReturnsWithoutExceedingCapacity() {
        var coordinator = new KeyMoveHandoffs<String>(32);
        var first = coordinator.tryRegister("A", "B");
        var next = coordinator.tryRegister("B", "A");
        for (int i = 0; i < 30; i++) coordinator.tryRegister("other" + i, "other" + i);
        assertThat(next.ready().toCompletableFuture()).isNotDone();
        first.departure().written();
        first.departure().written();
        assertThat(next.ready().toCompletableFuture()).isNotDone();
        first.arrival().queued();
        assertThat(next.ready().toCompletableFuture()).isCompleted();
        assertThat(coordinator.tryRegister("C", "C")).isNull();
        first.arrival().written();
        assertThat(coordinator.tryRegister("C", "C")).isNotNull();
    }

    @Test
    void abortRetiresTheOtherWaiterWhileAnAdmissionCallbackIsStillRunning() throws Exception {
        var coordinator = new KeyMoveHandoffs<String>(3);
        var move = coordinator.tryRegister("A", "B");
        var first = coordinator.tryRegister("A", "A");
        var second = coordinator.tryRegister("B", "B");
        var entered = new CountDownLatch(1);
        var resume = new CompletableFuture<Void>();
        var admissions = new AtomicInteger();
        Runnable dispatch = () -> {
            if (admissions.incrementAndGet() == 1) {
                entered.countDown();
                resume.orTimeout(10, TimeUnit.SECONDS).join();
            }
        };
        first.ready().thenRun(dispatch);
        second.ready().thenRun(dispatch);
        move.departure().written();
        var target = CompletableFuture.runAsync(() -> move.arrival().written());
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            coordinator.abort(new IllegalStateException("execution retired"));
        } finally {
            resume.complete(null);
            target.get(5, TimeUnit.SECONDS);
        }
        assertThat(admissions.get()).isOne();
    }

    @Test
    void aMoveWaitsForEarlierWorkAtBothKeysAndAbortRetiresWaiters() {
        var coordinator = new KeyMoveHandoffs<String>(5);
        var orders = List.of(coordinator.tryRegister("A", "A"), coordinator.tryRegister("B", "B"));
        var move = coordinator.tryRegister("A", "B");
        var later = coordinator.tryRegister("B", "C");
        orders.get(1).arrival().written();
        assertThat(move.ready().toCompletableFuture()).isNotDone();
        orders.get(0).arrival().queued();
        assertThat(move.ready().toCompletableFuture()).isCompleted();
        coordinator.abort(new IllegalStateException("member left"));
        move.arrival().written();
        move.departure().written();
        assertThat(later.ready().toCompletableFuture()).isCompletedExceptionally();
        assertThatThrownBy(() -> coordinator.tryRegister("A", "A")).hasMessage("execution ended");
    }
}
