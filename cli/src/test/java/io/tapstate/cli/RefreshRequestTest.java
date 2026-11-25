package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class RefreshRequestTest {

    @Test
    void cancelWaitsForAnInProgressGuardedMutationAndRejectsLaterMutations() throws Exception {
        RefreshRequest.CancellationToken token = new RefreshRequest.CancellationToken();
        CountDownLatch mutationEntered = new CountDownLatch(1);
        CountDownLatch releaseMutation = new CountDownLatch(1);
        CountDownLatch cancellationTaskEntered = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> ordering = new ConcurrentLinkedQueue<>();
        AtomicBoolean firstMutationRan = new AtomicBoolean();
        AtomicBoolean lateMutationRan = new AtomicBoolean();

        assertThat(Modifier.isSynchronized(RefreshRequest.CancellationToken.class
                .getDeclaredMethod("cancel")
                .getModifiers())).isTrue();

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<Boolean> mutation = executor.submit(() -> token.mutateIfActive(() -> {
                assertThat(Thread.holdsLock(token)).isTrue();
                ordering.add("mutation-entered");
                mutationEntered.countDown();
                await(releaseMutation);
                firstMutationRan.set(true);
                ordering.add("mutation-finished");
            }));
            assertThat(mutationEntered.await(2, TimeUnit.SECONDS)).isTrue();

            Future<?> cancellation = executor.submit(() -> {
                cancellationTaskEntered.countDown();
                token.cancel();
                ordering.add("cancel-returned");
            });

            assertThat(cancellationTaskEntered.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(cancellation.isDone()).isFalse();
            assertThat(ordering).containsExactly("mutation-entered");

            releaseMutation.countDown();
            assertThat(mutation.get(2, TimeUnit.SECONDS)).isTrue();
            cancellation.get(2, TimeUnit.SECONDS);
        }

        boolean accepted = token.mutateIfActive(() -> lateMutationRan.set(true));
        assertThat(ordering).containsExactlyElementsOf(
                List.of("mutation-entered", "mutation-finished", "cancel-returned"));
        assertThat(firstMutationRan).isTrue();
        assertThat(accepted).isFalse();
        assertThat(lateMutationRan).isFalse();
        assertThat(token.isCancelled()).isTrue();
    }

    private static void await(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
