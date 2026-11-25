package io.tapstate.e2e;

import static io.tapstate.runtime.engine.validation.ValidationRootHintOutbox.Settlement.ACCEPTED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.app.ValidationCaptureHintTap;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRootHintBatch.Change;
import io.tapstate.core.validation.ValidationRootHintBatch.Domain;
import io.tapstate.core.validation.ValidationRootHintBatch.Sender;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.runtime.engine.validation.ValidationCandidateQueue;
import io.tapstate.runtime.engine.validation.ValidationRootHintInbox;
import io.tapstate.runtime.engine.validation.ValidationRootHintOutbox;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class ValidationHintsSettleWhileCaptureContinuesIT {
    @Test
    void bothCaptureBatchesReachCandidateAdmission() throws Exception {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "boot", 1, 1, 1),
                "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
        var sender = new Sender("sender", "execution", "source", "orders", "capture", "boot", 1, 1,
                "route", "key-contract", "mapping", "output", List.of(new Domain(ValidationKey.Kind.EXACT_NUMBER)));
        var settling = new CountDownLatch(1);
        var captureContinued = new CountDownLatch(1);
        var transportThread = new ThreadLocal<Boolean>();
        long window = SECONDS.toNanos(30);
        try (var worker = Executors.newVirtualThreadPerTaskExecutor();
             var queue = new ValidationCandidateQueue(run, new ValidationCandidateQueue.Limits(2, 65536, 2, 2, 0, window), System::nanoTime);
             var inbox = new ValidationRootHintInbox(queue, List.of(sender), new ValidationRootHintInbox.Limits(1, 65536, 1, 32768));
             var outbox = new ValidationRootHintOutbox(run, sender, new ValidationRootHintOutbox.Limits(2, 65536, 1, 32768, 2, 0, window), () -> {
                 if (Boolean.TRUE.equals(transportThread.get())) {
                     settling.countDown();
                     await(captureContinued);
                 }
                 return System.nanoTime();
             })) {
            var delivered = new ArrayList<Envelope>();
            var tap = ValidationCaptureHintTap.wrap((events, position) -> delivered.addAll(events),
                    List.of(new ValidationCaptureHintTap.Binding(outbox, (kind, before, after) -> new Change(kind, null,
                            new ValidationKey(List.of(ValidationKey.Part.number((long) after.get("id"))))))),
                    new ValidationCaptureHintTap.Limits(1, 1, 32768));
            var first = Envelope.insert(0, "orders", Map.of("id", 1L), Map.of());
            var second = Envelope.insert(0, "orders", Map.of("id", 2L), Map.of());
            tap.onBatch(List.of(first), Optional.empty());
            var lease = outbox.poll().orElseThrow();
            var receipt = inbox.tryAccept(lease.batch());
            var settlement = worker.submit(() -> {
                transportThread.set(true);
                return outbox.settle(lease, receipt);
            });
            try {
                await(settling);
                tap.onBatch(List.of(second), Optional.empty());
            } finally {
                captureContinued.countDown();
            }
            assertThat(settlement.get(5, SECONDS)).as("first receipt=%s, outbox failure=%s", receipt.disposition(), outbox.failure())
                    .isEqualTo(ACCEPTED);
            var next = outbox.poll().orElseThrow();
            assertThat(outbox.settle(next, inbox.tryAccept(next.batch()))).isEqualTo(ACCEPTED);
            assertThat(queue.snapshot().roots()).isEqualTo(2);
            assertThat(delivered).containsExactly(first, second);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }
}
