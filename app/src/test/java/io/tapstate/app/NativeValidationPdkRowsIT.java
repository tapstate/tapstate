package io.tapstate.app;

import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;

/** The actual owner/run budget cannot drain on worker exit while native cleanup is unresolved. */
@RequiresDocker
class NativeValidationPdkRowsIT {
    @Container static final MongoDBContainer MONGO = NativeValidationOwnerReservationsIT.MONGO;

    @Test void exceptionalWorkerExitAndAnIdempotentCloseCannotReleaseTheNativeReadBudget() throws Exception {
        try (var f = new NativeValidationOwnerReservationsIT.Fixture()) {
            var request = f.request("FULL", 1);
            var accepted = f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var original = f.owners.take(f.owners.original(request, accepted.owner()));
            original.beginExecution();
            var allocation = original.resources();
            var plan = new NativeValidationPdkRows(original, original.declarations, List.of(), Map.of(), Clock.systemUTC());
            // Isolate the production retention state machine from plugin behavior covered by adapter tests.
            var read = ReflectionTestUtils.invokeMethod(plan, "admit");
            var workers = new CompletableFuture<Void>();
            var rows = new CompletableFuture<Void>();
            var cleanup = new CompletableFuture<Void>();
            ReflectionTestUtils.invokeMethod(read, "attach", (Runnable) () -> {}, workers, rows, cleanup);
            ReflectionTestUtils.invokeMethod(read, "opened");
            var charged = allocation.budget().used();
            assertThat(charged.isEmpty()).isFalse();
            var originalFailure = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "orders"), null);
            workers.completeExceptionally(originalFailure);
            rows.complete(null);
            plan.close();
            allocation.close();
            allocation.processorsExited();
            assertThat(plan.drained()).isFalse();
            assertThat(plan.failure()).isSameAs(originalFailure);
            assertThat(allocation.budget().used()).isEqualTo(charged);
            assertThat(allocation.released().toCompletableFuture()).isNotDone();
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isFalse();
            // Successful native cleanup supplies the final receipt, without replacing the original read.
            cleanup.complete(null);
            allocation.released().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(plan.drained()).isTrue();
            assertThat(allocation.budget().used().isEmpty()).isTrue();
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();
        }
    }
}
