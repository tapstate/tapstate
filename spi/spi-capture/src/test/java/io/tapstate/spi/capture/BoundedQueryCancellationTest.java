package io.tapstate.spi.capture;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedQueryCancellationTest {

    @Test
    void invokesRegisteredHooksOnlyOnceAndAllowsRegistrationRemoval() {
        BoundedQueryCancellation cancellation = new BoundedQueryCancellation();
        AtomicInteger invoked = new AtomicInteger();
        BoundedQueryCancellation.Registration removed = cancellation.onCancel(invoked::incrementAndGet);
        removed.close();
        cancellation.onCancel(invoked::incrementAndGet);
        cancellation.cancel();
        cancellation.cancel();

        assertThat(cancellation.isCancelled()).isTrue();
        assertThat(invoked).hasValue(1);
        assertThatThrownBy(cancellation::throwIfCancelled).isInstanceOf(CancellationException.class);
    }

    @Test
    void invokesHooksRegisteredAfterCancellationAndIgnoresFailingHooks() {
        BoundedQueryCancellation cancellation = new BoundedQueryCancellation();
        cancellation.onCancel(() -> { throw new IllegalStateException("best effort"); });
        cancellation.cancel();
        AtomicInteger late = new AtomicInteger();
        cancellation.onCancel(late::incrementAndGet);
        assertThat(late).hasValue(1);
    }
}
