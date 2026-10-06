package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Real container destruction distinguishes an ended host from unknown or failed owned resources. */
class InProcessServerShutdownTest {
    @Test
    void anInactiveContextWithNoReceiptDoesNotQualifyAnEndedSource() {
        try (var context = new GenericApplicationContext()) {
            context.refresh();
            var server = new InProcessServer(context, URI.create("http://127.0.0.1:1"));
            server.close();
            assertThat(context.isActive()).isFalse();
            assertThat(server.terminated()).isFalse();
        }
    }

    @Test
    void normalOwnedResourceCompletionIsCapturedBeforeTheContainerDropsItsBeans() {
        AtomicBoolean closed = new AtomicBoolean();
        try (var context = new GenericApplicationContext()) {
            context.registerBean("ownedResource", OwnedResource.class, () -> new OwnedResource(() -> closed.set(true)),
                    definition -> definition.setDestroyMethodName("close"));
            context.registerBean("localCaptureShutdownComplete", BooleanSupplier.class, () -> closed::get);
            context.refresh();
            var server = new InProcessServer(context, URI.create("http://127.0.0.1:1"));
            assertThat(server.terminated()).isFalse();
            server.close();
            assertThat(closed).isTrue();
            assertThat(server.terminated()).isTrue();
            server.close();
            assertThat(server.terminated()).isTrue();
        }
    }

    @Test
    void aSwallowedResourceDestroyFailureDoesNotQualifySourceCleanup() {
        AtomicBoolean closed = new AtomicBoolean();
        try (var context = new GenericApplicationContext()) {
            context.registerBean("ownedResource", OwnedResource.class,
                    () -> new OwnedResource(() -> { throw new IllegalStateException("controlled fixture close refusal"); }),
                    definition -> definition.setDestroyMethodName("close"));
            context.registerBean("localCaptureShutdownComplete", BooleanSupplier.class, () -> closed::get);
            context.refresh();
            var server = new InProcessServer(context, URI.create("http://127.0.0.1:1"));
            server.close();
            assertThat(context.isActive()).isFalse();
            assertThat(closed).isFalse();
            assertThat(server.terminated()).isFalse();
        }
    }

    @Test
    void aPendingReceiptCannotBecomeAnEarlierSuccessfulOwnerEndAfterHostClose() {
        CompletableFuture<Void> ended = new CompletableFuture<>();
        try (var context = new GenericApplicationContext()) {
            context.registerBean("localCaptureShutdownComplete", BooleanSupplier.class,
                    () -> () -> ended.isDone() && !ended.isCompletedExceptionally());
            context.refresh();
            var server = new InProcessServer(context, URI.create("http://127.0.0.1:1"));
            server.close();
            assertThat(context.isActive()).isFalse();
            assertThat(server.terminated()).isFalse();
            ended.complete(null);
            server.close();
            assertThat(server.terminated()).as("late completion does not rewrite the captured owner-end receipt").isFalse();
        }
    }

    private record OwnedResource(Runnable teardown) implements AutoCloseable {
        @Override public void close() { teardown.run(); }
    }
}
