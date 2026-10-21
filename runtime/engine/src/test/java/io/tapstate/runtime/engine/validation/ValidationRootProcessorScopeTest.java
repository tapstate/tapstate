package io.tapstate.runtime.engine.validation;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationRootProcessorScopeTest {
    @Test void aMisroutedFactoryHandleCannotCancelAnotherObservationOrRun() throws Exception {
        var expected = ValidationRootWorkTest.observation(1);
        for (boolean otherRun : List.of(false, true)) {
            var original = ValidationRootWorkTest.observation(2);
            var foreign = otherRun ? new ValidationObservation(new ValidationRunIdentity(original.run().owner(), "other-task", "other-run",
                    original.run().mode(), "spec", 1, "rules"), original.root(), original.revision()) : original;
            var unblock = new CountDownLatch(1);
            var entered = new CountDownLatch(1);
            var cancellations = new AtomicInteger();
            try (var executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy())) {
                var work = ValidationRootWork.submit(foreign, executor, (scope, guard) -> {
                    entered.countDown(); await(unblock); return ValidationRootWorkTest.result(scope);
                }, () -> {}, cancellations::incrementAndGet, () -> {}, 32768);
                await(entered);
                var processor = new ValidationRootProcessor(expected.run(), (scope, guard, bytes) -> work, ignored -> {}, 32768);
                var outbox = new TestOutbox(new int[]{1}, 1);
                processor.init(outbox, new TestProcessorContext());
                var inbox = new TestInbox(); inbox.addAll(List.of(expected));
                assertThatThrownBy(() -> processor.process(0, inbox)).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
                processor.close();
                unblock.countDown(); executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(cancellations).hasValue(0);
                assertThat(work.takeResult()).isPresent();
                assertThat(outbox.queue(0)).isEmpty();
            } finally { unblock.countDown(); }
        }
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("fixture latch timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
