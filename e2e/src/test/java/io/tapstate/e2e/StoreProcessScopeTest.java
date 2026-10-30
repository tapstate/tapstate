package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapstate.e2e.ClusterProcessControlFixtures.child;
import static io.tapstate.e2e.ClusterProcessControlFixtures.prepare;
import static io.tapstate.e2e.ClusterProcessControlFixtures.jar;
import static io.tapstate.e2e.ClusterProcessControlFixtures.arguments;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual children prove ownership; synthetic Process instances only model unknown OS answers. */
class StoreProcessScopeTest {
    private static final String STORE = "mongodb://127.0.0.1:1";

    @Test
    void aLiveOwnedChildRefusesCleanupAndARetiredChildAllowsRetryWithoutTouchingAForeignChild(
            @TempDir Path scratch) throws Exception {
        AtomicInteger cleanup = new AtomicInteger();
        StoreProcessScope scope = StoreProcessScope.open();
        try (RealProcessServer foreign = child(scratch, "foreign", "quiet", null, null, null)) {
            scope.rememberHostUri(STORE);
            try (RealProcessServer owned = child(scratch, "owned", "quiet", null, null, null)) {
                assertThatThrownBy(() -> scope.closeWith(cleanup::incrementAndGet))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("confirmed exit");
                assertThat(cleanup).hasValue(0);
                assertThat(owned.isAlive()).isTrue();
                assertThat(foreign.isAlive()).isTrue();
                assertThat(foreign.stagingDirectory()).isDirectory();
                assertThat(StoreProcessScope.matching(STORE)).isNull();
                owned.close();
                scope.closeWith(cleanup::incrementAndGet);
                assertThat(cleanup).hasValue(1);
                assertThat(foreign.isAlive()).isTrue();
                scope.closeWith(cleanup::incrementAndGet);
                assertThat(cleanup).hasValue(1);
            }
        } finally {
            scope.closeWith(cleanup::incrementAndGet);
        }
    }

    @Test
    void anExplicitlySyntheticUnknownExitRetainsCleanupAndDoesNotPoisonTheFollowingScope() {
        AtomicInteger firstCleanup = new AtomicInteger();
        AtomicInteger laterCleanup = new AtomicInteger();
        StoreProcessScope first = StoreProcessScope.open();
        UnknownProcess unknown = new UnknownProcess();
        first.rememberHostUri(STORE);
        first.launched(unknown);
        try {
            assertThatThrownBy(() -> first.closeWith(firstCleanup::incrementAndGet))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("confirmed exit");
            assertThat(firstCleanup).hasValue(0);
            assertThat(StoreProcessScope.matching(STORE)).isNull();
            StoreProcessScope later = StoreProcessScope.open();
            later.rememberHostUri("mongodb://127.0.0.1:2");
            later.closeWith(laterCleanup::incrementAndGet);
            assertThat(laterCleanup).hasValue(1);
            assertThat(firstCleanup).hasValue(0);
            unknown.confirm();
            first.closeWith(firstCleanup::incrementAndGet);
            assertThat(firstCleanup).hasValue(1);
        } finally {
            unknown.confirm();
            first.closeWith(firstCleanup::incrementAndGet);
        }
    }

    @Test
    void aFalseAliveAnswerWithoutAnExitValueStillRetainsTheStore() {
        AtomicInteger cleanup = new AtomicInteger();
        StoreProcessScope scope = StoreProcessScope.open();
        UnknownProcess inconsistent = new UnknownProcess() {
            @Override public boolean isAlive() { return false; }
        };
        scope.launched(inconsistent);
        try {
            assertThatThrownBy(() -> scope.closeWith(cleanup::incrementAndGet))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("confirmed exit")
                    .satisfies(failure -> assertThat(failure.getSuppressed())
                            .singleElement().isInstanceOf(IllegalThreadStateException.class));
            assertThat(cleanup).hasValue(0);
        } finally {
            inconsistent.confirm();
            scope.closeWith(cleanup::incrementAndGet);
        }
    }

    @Test
    void aReportedWaitFailureDoesNotKeepAChildWhoseActualExitIsConfirmed(@TempDir Path scratch)
            throws Exception {
        AtomicInteger cleanup = new AtomicInteger();
        StoreProcessScope scope = StoreProcessScope.open();
        scope.rememberHostUri(STORE);
        boolean originalInterrupt = Thread.currentThread().isInterrupted();
        try (RealProcessServer owned = child(scratch, "interrupted-wait", "refuse-term",
                scratch.resolve("interrupted-wait.entered"), null, null)) {
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(owned::close).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("interrupted");
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                assertThat(owned.isAlive()).isFalse();
            } finally {
                // The interrupt belongs to this control; its restoration was asserted above.
                if (!originalInterrupt) Thread.interrupted();
            }
            scope.closeWith(cleanup::incrementAndGet);
            assertThat(cleanup).hasValue(1);
        } finally {
            if (!originalInterrupt) Thread.interrupted();
            scope.closeWith(cleanup::incrementAndGet);
        }
    }

    @Test
    void aPreparedLaunchCannotCreateAChildAfterItsStoreScopeIsSealed(@TempDir Path scratch)
            throws Exception {
        AtomicInteger cleanup = new AtomicInteger();
        StoreProcessScope scope = StoreProcessScope.open();
        scope.rememberHostUri(STORE);
        AtomicReference<RealProcessServer> mistakenlyLaunched = new AtomicReference<>();
        Path ready = scratch.resolve("must-not-launch.ready");
        try (RealProcessServer.PreparedLaunch prepared = prepare(jar(scratch), null,
                ignored -> arguments(ready, "quiet", null, null, null))) {
            scope.closeWith(cleanup::incrementAndGet);
            assertThatThrownBy(() -> mistakenlyLaunched.set(prepared.launch()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("no longer open");
            Field unhanded = RealProcessServer.PreparedLaunch.class.getDeclaredField("unhandedProcess");
            unhanded.setAccessible(true);
            assertThat(unhanded.get(prepared)).as("validation happened before a real child could be created").isNull();
            assertThat(ready).doesNotExist();
            assertThat(prepared.stagingDirectory()).doesNotExist();
            assertThat(cleanup).hasValue(1);
        } finally {
            RealProcessServer unexpected = mistakenlyLaunched.get();
            if (unexpected != null) unexpected.close();
            scope.closeWith(cleanup::incrementAndGet);
        }
    }

    @Test
    void nestedOwnersBindTheirOwnUrisAndWrongOrderCannotDetachEitherOwner() {
        AtomicInteger outerCleanup = new AtomicInteger();
        AtomicInteger innerCleanup = new AtomicInteger();
        StoreProcessScope outer = StoreProcessScope.open();
        outer.rememberHostUri(STORE);
        StoreProcessScope inner = StoreProcessScope.open();
        inner.rememberHostUri("mongodb://127.0.0.1:2");
        try {
            assertThat(StoreProcessScope.matching(STORE)).isSameAs(outer);
            assertThat(StoreProcessScope.matching("mongodb://127.0.0.1:2")).isSameAs(inner);
            assertThatThrownBy(() -> outer.closeWith(outerCleanup::incrementAndGet))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("lexical order");
            assertThat(StoreProcessScope.matching(STORE)).isSameAs(outer);
            assertThat(StoreProcessScope.matching("mongodb://127.0.0.1:2")).isSameAs(inner);
            assertThat(outerCleanup).hasValue(0);
            assertThat(innerCleanup).hasValue(0);
        } finally {
            inner.closeWith(innerCleanup::incrementAndGet);
            outer.closeWith(outerCleanup::incrementAndGet);
        }
    }

    @Test
    void anotherThreadCannotDetachTheOwnerOrRunItsCleanup() throws Exception {
        AtomicInteger cleanup = new AtomicInteger();
        AtomicReference<Throwable> answer = new AtomicReference<>();
        StoreProcessScope scope = StoreProcessScope.open();
        scope.rememberHostUri(STORE);
        try {
            Thread wrong = new Thread(() -> {
                try { scope.closeWith(cleanup::incrementAndGet); }
                catch (RuntimeException | Error failure) { answer.set(failure); }
            }, "store-scope-wrong-owner-control");
            wrong.start();
            wrong.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(wrong.isAlive()).isFalse();
            assertThat(answer.get()).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("another thread");
            assertThat(cleanup).hasValue(0);
            assertThat(StoreProcessScope.matching(STORE)).isSameAs(scope);
        } finally {
            scope.closeWith(cleanup::incrementAndGet);
        }
    }

    @Test
    void anotherThreadCannotRetryASealedOwnerEvenAfterItsSyntheticChildBecomesConfirmed() throws Exception {
        AtomicInteger cleanup = new AtomicInteger();
        AtomicReference<Throwable> answer = new AtomicReference<>();
        StoreProcessScope scope = StoreProcessScope.open();
        UnknownProcess unknown = new UnknownProcess();
        scope.launched(unknown);
        try {
            assertThatThrownBy(() -> scope.closeWith(cleanup::incrementAndGet))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("confirmed exit");
            unknown.confirm();
            Thread wrong = new Thread(() -> {
                try { scope.closeWith(cleanup::incrementAndGet); }
                catch (RuntimeException | Error failure) { answer.set(failure); }
            }, "sealed-store-scope-wrong-owner-control");
            wrong.start();
            wrong.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(wrong.isAlive()).isFalse();
            assertThat(answer.get()).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("another thread");
            assertThat(cleanup).hasValue(0);
        } finally {
            unknown.confirm();
            scope.closeWith(cleanup::incrementAndGet);
        }
    }

    /** No OS child exists here; flipping confirmation only advances this explicit boundary stub. */
    static class UnknownProcess extends Process {
        private boolean confirmed;
        void confirm() { confirmed = true; }
        @Override public OutputStream getOutputStream() { return OutputStream.nullOutputStream(); }
        @Override public InputStream getInputStream() { return InputStream.nullInputStream(); }
        @Override public InputStream getErrorStream() { return InputStream.nullInputStream(); }
        @Override public int waitFor() { throw new IllegalStateException("synthetic wait answer is unknown"); }
        @Override public boolean waitFor(long timeout, TimeUnit unit) {
            throw new IllegalStateException("synthetic wait answer is unknown");
        }
        @Override public int exitValue() {
            if (!confirmed) throw new IllegalThreadStateException("synthetic exit answer is unknown");
            return 0;
        }
        @Override public void destroy() { }
        @Override public Process destroyForcibly() { return this; }
        @Override public boolean isAlive() { return !confirmed; }
    }
}
