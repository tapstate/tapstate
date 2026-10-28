package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class ClusterFixtureTimingTest {
    @Test void nestedScopesRestoreTheExactOwner() {
        try (var outer = ClusterFixtureTiming.install()) {
            long first = ClusterFixtureTiming.beginPreparation();
            try (var inner = ClusterFixtureTiming.install()) {
                ClusterFixtureTiming.beginPreparation();
                assertThat(inner.events()).hasSize(1);
            }
            long second = ClusterFixtureTiming.beginPreparation();
            assertThat(first).isNotEqualTo(second);
            assertThat(outer.events()).hasSize(2);
            assertThat(outer.valid()).isTrue();
        }
    }
    @Test void disabledObserverDoesNotCreateAnObservation() {
        try (var scope = ClusterFixtureTiming.suspend()) {
            assertThat(ClusterFixtureTiming.beginPreparation()).isZero();
            ClusterFixtureTiming.prepared(1, null, null, null, null, null);
            ClusterFixtureTiming.phase(null, ClusterFixtureTiming.Phase.EXIT_CONFIRMED);
            ClusterFixtureTiming.exitObserved(new PoisonProcess());
            assertThat(scope.events()).isEmpty();
            assertThat(scope.valid()).isTrue();
        }
    }
    @Test void unboundProcessCannotProvideCompleteEvidence() {
        try (var scope = ClusterFixtureTiming.install()) {
            ClusterFixtureTiming.phase(null, ClusterFixtureTiming.Phase.EXIT_CONFIRMED);
            assertThat(scope.valid()).isFalse();
            assertThat(scope.events()).isEmpty();
        }
    }
    @Test void deferredOuterCloseRestoresAfterInnerClose() {
        var before = ClusterFixtureTiming.activeScope();
        var outer = ClusterFixtureTiming.install();
        var inner = ClusterFixtureTiming.install();
        outer.close();
        assertThat(outer.valid()).isFalse();
        inner.close();
        assertThat(ClusterFixtureTiming.activeScope()).isSameAs(before);
        try (var disabled = ClusterFixtureTiming.suspend()) {
            assertThat(ClusterFixtureTiming.beginPreparation()).isZero();
        }
    }
    @Test void wrongThreadCannotCloseTheOwnerScope() throws InterruptedException {
        try (var scope = ClusterFixtureTiming.install()) {
            Thread other = new Thread(scope::close);
            other.start();
            other.join();
            assertThat(scope.valid()).isFalse();
            ClusterFixtureTiming.beginPreparation();
            assertThat(scope.events()).hasSize(1);
        }
    }
    private static final class PoisonProcess extends Process {
        @Override public java.io.OutputStream getOutputStream() { throw new AssertionError("process accessed"); }
        @Override public java.io.InputStream getInputStream() { throw new AssertionError("process accessed"); }
        @Override public java.io.InputStream getErrorStream() { throw new AssertionError("process accessed"); }
        @Override public int waitFor() { throw new AssertionError("process accessed"); }
        @Override public int exitValue() { throw new AssertionError("process accessed"); }
        @Override public void destroy() { throw new AssertionError("process accessed"); }
        @Override public boolean isAlive() { throw new AssertionError("process accessed"); }
    }
}
