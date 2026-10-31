package io.tapstate.core.validation;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationFailureLatchTest {
    @Test void firstOrdinaryFailureWinsUntilAnErrorMustEscape() {
        var latch = new ValidationFailureLatch();
        var first = new IllegalStateException("first");
        assertThat(latch.record(first)).isTrue();
        assertThat(latch.record(new IllegalArgumentException("later"))).isFalse();
        assertThat(latch.get()).isSameAs(first);
        var fatal = new AssertionError("fatal");
        assertThat(latch.record(fatal)).isFalse();
        assertThat(latch.get()).isSameAs(fatal);
        assertThat(fatal.getSuppressed()).containsExactly(first);
        latch.record(new AssertionError("later fatal"));
        assertThat(latch.get()).isSameAs(fatal);
    }

    @Test void repeatedContextAndDirectCauseCyclesDoNotGrowTheFailureGraph() {
        var first = new IllegalStateException("first");
        var fatal = new AssertionError("fatal");
        for (int i = 0; i < 1000; i++) ValidationFailureLatch.addContext(fatal, first);
        assertThat(fatal.getSuppressed()).containsExactly(first);
        ValidationFailureLatch.addContext(fatal, fatal);
        ValidationFailureLatch.addContext(fatal, new IllegalStateException("wrap", fatal));
        assertThat(fatal.getSuppressed()).containsExactly(first);
        for (int i = 0; i < 1000; i++) ValidationFailureLatch.addContext(fatal, new IllegalStateException("secondary"));
        assertThat(fatal.getSuppressed()).hasSize(8);
    }

    @Test void concurrentFatalReportsCannotLeaveAnOrdinaryFailureAsPrimary() throws Exception {
        var latch = new ValidationFailureLatch();
        var first = new IllegalStateException("first");
        latch.record(first);
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var a = workers.submit(() -> { start.await(); latch.record(new AssertionError("a")); return null; });
            var b = workers.submit(() -> { start.await(); latch.record(new AssertionError("b")); return null; });
            start.countDown();
            a.get(3, TimeUnit.SECONDS);
            b.get(3, TimeUnit.SECONDS);
        }
        assertThat(latch.get()).isInstanceOf(AssertionError.class);
        assertThat(latch.get().getSuppressed()).containsExactly(first);
    }
}
