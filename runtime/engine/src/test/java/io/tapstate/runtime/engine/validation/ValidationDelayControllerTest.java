package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.assertj.core.api.Assertions.*;

class ValidationDelayControllerTest {
    @Test void anUnknownAcknowledgmentReconcilesTheOriginalApplicationWithoutMovingExistingWaits() {
        var clock = new AtomicLong(); var run = run();
        try (var queue = queue(run, clock)) {
            var control = new Control(queue); var controller = new ValidationDelayController(control, run, queue, () -> {});
            var change = new ValidationControlStore.Delay(run, 1, UUID.randomUUID(), 20); control.saved = change;
            var before = root(1); queue.tryOffer(java.util.List.of(before)); control.loseReply = true;
            assertThatThrownBy(() -> controller.apply(change)).isInstanceOf(TapstateException.class);
            assertThat(control.applied).isEqualTo(change); assertThat(queue.delayState().schedulingRevision()).isOne();
            assertThat(controller.apply(change)).isEqualTo(ValidationDelayController.Application.ACKNOWLEDGED);
            queue.tryOffer(java.util.List.of(root(2))); clock.set(10_000_000L);
            var old = queue.poll().orElseThrow(); assertThat(old.observation().root()).isEqualTo(before); queue.settle(old, ValidationCandidateQueue.Completion.SETTLED);
            assertThat(queue.poll()).isEmpty(); clock.set(20_000_000L); assertThat(queue.poll()).isPresent();
        }
    }

    @Test void aNewSaveBetweenLocalApplicationAndAcknowledgmentNeverConfirmsTheOldTuple() {
        var run = run();
        try (var queue = queue(run, new AtomicLong())) {
            var control = new Control(queue); var controller = new ValidationDelayController(control, run, queue, () -> {});
            var first = new ValidationControlStore.Delay(run, 1, UUID.randomUUID(), 20);
            var latest = new ValidationControlStore.Delay(run, 2, UUID.randomUUID(), 30);
            control.saved = first; control.beforeAcknowledgment = () -> control.saved = latest;
            assertThat(controller.apply(first)).isEqualTo(ValidationDelayController.Application.STALE);
            assertThat(control.applied).isNull(); assertThat(queue.delayState().schedulingRevision()).isOne();
            control.beforeAcknowledgment = () -> {};
            assertThat(controller.apply(latest)).isEqualTo(ValidationDelayController.Application.ACKNOWLEDGED);
            assertThat(control.applied).isEqualTo(latest); assertThat(queue.delayState().delayNanos()).isEqualTo(30_000_000L);
            assertThat(controller.apply(first)).isEqualTo(ValidationDelayController.Application.STALE);
        }
    }

    @Test void localClosureAndOwnerLossCannotProduceAnApplicationAcknowledgment() {
        var run = run(); var live = new AtomicBoolean(true);
        try (var queue = queue(run, new AtomicLong())) {
            var control = new Control(queue); var change = new ValidationControlStore.Delay(run, 1, UUID.randomUUID(), 20); control.saved = change;
            var controller = new ValidationDelayController(control, run, queue, () -> {
                if (!live.get()) throw new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
            });
            control.afterRequest = () -> { live.set(false); queue.close(); };
            assertThatThrownBy(() -> controller.apply(change)).isInstanceOf(TapstateException.class);
            assertThat(control.applied).isNull(); assertThat(queue.delayState().schedulingRevision()).isZero();
            live.set(true); assertThat(controller.apply(change)).isEqualTo(ValidationDelayController.Application.CLOSED);
            assertThat(control.applied).isNull();
        }
    }

    private static final class Control implements ValidationControlStore {
        final ValidationCandidateQueue queue;
        Delay saved, requested, applied;
        boolean loseReply;
        Runnable afterRequest = () -> {}, beforeAcknowledgment = () -> {};
        Control(ValidationCandidateQueue queue) { this.queue = queue; }
        @Override public Mutation requestDelay(Delay change) {
            if (!change.equals(saved)) return Mutation.STALE;
            requested = change; afterRequest.run(); return Mutation.APPLIED;
        }
        @Override public Mutation acknowledgeDelay(Delay change) {
            beforeAcknowledgment.run();
            if (!change.equals(saved) || !change.equals(requested) || queue.delayState().closed()) return Mutation.STALE;
            assertThat(queue.delayState().schedulingRevision()).isEqualTo(change.schedulingRevision());
            assertThat(queue.delayState().commandId()).isEqualTo(change.commandId());
            assertThat(queue.delayState().delayNanos()).isEqualTo(change.firstCheckDelayMillis() * 1_000_000L);
            boolean duplicate = change.equals(applied); applied = change;
            if (loseReply) { loseReply = false; throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "test.unknownReply"), null); }
            return duplicate ? Mutation.ALREADY_APPLIED : Mutation.APPLIED;
        }
        @Override public Join join(Run run) { throw new AssertionError(); }
        @Override public Mutation transition(Transition command) { throw new AssertionError(); }
        @Override public Mutation latchProblemLimit(Run run) { throw new AssertionError(); }
        @Override public Mutation retire(Retirement command) { throw new AssertionError(); }
        @Override public Mutation terminateLostOwner(LostOwner command) { throw new AssertionError(); }
    }
    private static ValidationCandidateQueue queue(ValidationControlStore.Run run, AtomicLong clock) {
        return new ValidationCandidateQueue(run.identity(), new ValidationCandidateQueue.Limits(8, 65536, 4, 3, 10_000_000L, 100_000_000L, 7_000_000L), clock::get);
    }
    private static ValidationControlStore.Run run() {
        var owner = new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0);
        var identity = new ValidationRunIdentity(owner, "task", "run", ValidationMode.INCREMENTAL, "hash", 1, "rules");
        var task = new ValidationReportAdmission.Task("cluster", "pipeline", "task", UUID.randomUUID());
        return new ValidationControlStore.Run(identity, 1, new ValidationReportAdmission.Start(task, 1, 1, "command", identity));
    }
    private static ValidationRoot root(long key) { return new ValidationRoot("mapping", "output", new ValidationKey(java.util.List.of(ValidationKey.Part.number(key))), java.util.List.of()); }
}
