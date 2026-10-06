package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual capture handles and Spring destruction exercise the shared lifecycle worker budget. */
class OwnedCaptureShutdownTest {

    @Test
    void normalSpringCloseQualifiesOnlyAfterTheActualWorkerClosesBothHandles() {
        AtomicInteger closed = new AtomicInteger();
        AtomicReference<String> worker = new AtomicReference<>();
        Fixture fixture = new Fixture(Duration.ofSeconds(5), id -> {
            worker.set(Thread.currentThread().getName()); closed.incrementAndGet();
        });
        fixture.startBoth();
        var context = context(fixture);
        BooleanSupplier receipt = context.getBean("localCaptureShutdownComplete", BooleanSupplier.class);
        assertThat(receipt.getAsBoolean()).isFalse();

        context.close();

        assertThat(context.isActive()).isFalse();
        assertThat(receipt.getAsBoolean()).isTrue();
        assertThat(closed).hasValue(2);
        assertThat(worker.get()).startsWith("tapstate-lifecycle-");
        assertThat(fixture.dispatcher.ownedCleanupPending()).isZero();
        assertThat(fixture.coordinator.isActive("a")).isFalse();
        assertThat(fixture.coordinator.isActive("b")).isFalse();
        context.close();
        fixture.coordinator.close();
        assertThat(closed).hasValue(2);
    }

    @Test
    void originalCloseFailureSurvivesSpringDestructionAndAnotherSourceStillCloses() {
        TapstateException original = new TapstateException(ConnectorError.CAPTURE_FAILED,
                Map.of("connector", "mysql", "detail", "controlled close refusal"), null);
        AtomicInteger failed = new AtomicInteger(), healthy = new AtomicInteger();
        Fixture fixture = new Fixture(Duration.ofSeconds(5), id -> {
            if (id.equals("a")) { failed.incrementAndGet(); throw original; }
            healthy.incrementAndGet();
        });
        fixture.startBoth();
        var context = context(fixture);
        BooleanSupplier receipt = context.getBean("localCaptureShutdownComplete", BooleanSupplier.class);

        context.close();

        assertThat(context.isActive()).isFalse();
        assertThat(receipt.getAsBoolean()).isFalse();
        assertThat(failed).hasValue(1);
        assertThat(healthy).hasValue(1);
        assertThat(fixture.coordinator.isActive("a")).as("a failed native close retains its actual local handle").isTrue();
        assertThat(fixture.coordinator.isActive("b")).isFalse();
        assertThatThrownBy(fixture.coordinator::close).isSameAs(original);
        assertThat(failed).as("an earlier failed close is not requalified by a repeated idempotent return").hasValue(1);
    }

    @Test
    void timeoutIsAggregateAndALateCloseCannotTurnTheFailedReceiptIntoSuccess() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), ended = new CountDownLatch(1);
        AtomicInteger healthy = new AtomicInteger();
        Fixture fixture = new Fixture(Duration.ofMillis(200), id -> {
            if (id.equals("a")) {
                entered.countDown(); awaitIgnoringInterrupt(release); ended.countDown();
            } else { healthy.incrementAndGet(); }
        });
        fixture.startBoth();
        var context = context(fixture);
        BooleanSupplier receipt = context.getBean("localCaptureShutdownComplete", BooleanSupplier.class);
        var closer = Executors.newSingleThreadExecutor();
        try {
            var closing = closer.submit(fixture.coordinator::close);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> closing.get(5, TimeUnit.SECONDS))
                    .isInstanceOfSatisfying(ExecutionException.class, refused ->
                            assertThat(refused.getCause()).isInstanceOfSatisfying(TapstateException.class,
                                    coded -> assertThat(coded.code()).isEqualTo(ActuationError.CAPTURE_SHUTDOWN_INCOMPLETE)));
            assertThat(healthy).as("the independent source is not blocked behind a native close").hasValue(1);
            assertThat(receipt.getAsBoolean()).isFalse();
            assertThat(fixture.coordinator.isActive("a")).isTrue();
            release.countDown();
            assertThat(ended.await(5, TimeUnit.SECONDS)).isTrue();
            fixture.dispatcher.beginOwnedShutdown();
            assertThat(fixture.dispatcher.awaitOwnedReconciliation(System.nanoTime() + TimeUnit.SECONDS.toNanos(5))).isTrue();
            context.close();
            assertThat(context.isActive()).isFalse();
            assertThat(receipt.getAsBoolean()).as("late teardown is not a successful original shutdown receipt").isFalse();
        } finally {
            release.countDown(); context.close(); closer.shutdownNow();
            assertThat(closer.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void completedReconcilePermitsAreRecoveredForCleanupOnTheSameFixedPool() throws Exception {
        try (LifecycleWorkDispatcher dispatcher = new LifecycleWorkDispatcher(1, 1)) {
            CountDownLatch secondEntered = new CountDownLatch(1);
            assertThat(dispatcher.offer("first", desired("first"), OwnedCaptureShutdownTest::done))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(dispatcher.offer("second", desired("second"), () -> { secondEntered.countDown(); return done(); }))
                    .isEqualTo(LifecycleWorkDispatcher.Submission.ACCEPTED);
            assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
            // The first task has returned through the same single worker. Neither result was taken.
            assertThat(dispatcher.activeCount()).isEqualTo(2);
            dispatcher.beginOwnedShutdown();
            AtomicReference<String> worker = new AtomicReference<>();
            var cleanup = dispatcher.offerOwnedCleanup(() -> worker.set(Thread.currentThread().getName()),
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(5)).orElseThrow();
            cleanup.get(5, TimeUnit.SECONDS);
            assertThat(worker.get()).startsWith("tapstate-lifecycle-");
            assertThat(dispatcher.activeCount()).isZero();
            assertThat(dispatcher.ownedCleanupPending()).isZero();
            assertThat(dispatcher.health().activeSlots()).isZero();
        }
    }

    @Test
    void anOlderUninterruptibleReconcileKeepsQualificationUntilItsActualFinally() throws Exception {
        Fixture fixture = new Fixture(Duration.ofMillis(200), id -> { });
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        fixture.dispatcher.offer("older", desired("older"), () -> {
            entered.countDown(); awaitIgnoringInterrupt(release); return done();
        });
        var context = context(fixture);
        BooleanSupplier receipt = context.getBean("localCaptureShutdownComplete", BooleanSupplier.class);
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(fixture.coordinator::close).isInstanceOfSatisfying(TapstateException.class,
                    coded -> assertThat(coded.code()).isEqualTo(ActuationError.CAPTURE_SHUTDOWN_INCOMPLETE));
            assertThat(fixture.dispatcher.activeCount()).isEqualTo(1);
            assertThat(receipt.getAsBoolean()).isFalse();
            release.countDown();
            assertThat(fixture.dispatcher.awaitOwnedReconciliation(System.nanoTime() + TimeUnit.SECONDS.toNanos(5))).isTrue();
            assertThat(fixture.dispatcher.activeCount()).isZero();
            assertThat(receipt.getAsBoolean()).isFalse();
        } finally { release.countDown(); context.close(); }
    }

    @Test
    void interruptionDoesNotGrantAReceiptAndKeepsTheCallerInterrupt() throws Exception {
        Fixture fixture = new Fixture(Duration.ofSeconds(5), id -> { });
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        fixture.dispatcher.offer("older", desired("older"), () -> {
            entered.countDown(); awaitIgnoringInterrupt(release); return done();
        });
        var context = context(fixture);
        BooleanSupplier receipt = context.getBean("localCaptureShutdownComplete", BooleanSupplier.class);
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.currentThread().interrupt();
            assertThatThrownBy(fixture.coordinator::close).isInstanceOf(TapstateException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(receipt.getAsBoolean()).isFalse();
        } finally {
            Thread.interrupted(); release.countDown(); context.close();
        }
    }

    @Test
    void aDiscardedQueuedCleanupIsTypedUnconfirmedAndItsActionNeverRan() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger queuedAction = new AtomicInteger();
        LifecycleWorkDispatcher dispatcher = new LifecycleWorkDispatcher(1, 1);
        try {
            dispatcher.beginOwnedShutdown();
            dispatcher.offerOwnedCleanup(() -> {
                entered.countDown();
                try { release.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException shutdown) { Thread.currentThread().interrupt(); }
            }, System.nanoTime() + TimeUnit.SECONDS.toNanos(5)).orElseThrow();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var neverStarted = dispatcher.offerOwnedCleanup(queuedAction::incrementAndGet,
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(5)).orElseThrow();
            dispatcher.close();

            assertThatThrownBy(() -> neverStarted.get(5, TimeUnit.SECONDS))
                    .isInstanceOfSatisfying(ExecutionException.class, refused ->
                            assertThat(refused.getCause()).isInstanceOfSatisfying(TapstateException.class,
                                    coded -> assertThat(coded.code()).isEqualTo(ActuationError.CAPTURE_SHUTDOWN_INCOMPLETE)));
            assertThat(queuedAction).as("the unconfirmed queued action was not a native close success").hasValue(0);
            assertThat(dispatcher.ownedCleanupPending()).isZero();
        } finally { release.countDown(); dispatcher.close(); }
    }

    private static final class Fixture {
        private final LifecycleWorkDispatcher dispatcher = new LifecycleWorkDispatcher(2, 2);
        private final StoreBackedPipelineCaptureCoordinator coordinator;
        private Fixture(Duration budget, Consumer<String> close) {
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            for (String id : List.of("a", "b")) {
                String source = "source_" + id;
                artifacts.save(new SourceResource(source, null, "mysql", Map.of("host", "controlled"), SourceMode.CDC,
                        List.of(TableRef.literal("orders")), new Srs("chain_" + id, null, null, null, null), null));
                artifacts.save(new PipelineResource(id, null, List.of(SourceRef.spec(source, true)), null, null,
                        new ServeBlock.Inline(null, FromRef.literal(source),
                                List.of(new SyncElement("sync", source, null, null, null)), null, null),
                        new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null));
            }
            InMemoryStorePort store = new InMemoryStorePort(artifacts);
            CaptureStarter starter = (spec, handoff) -> new CaptureRun(Optional.empty(), false, 0L, Optional.empty(),
                    Optional.of(() -> close.accept(spec.pipelineId())), new CaptureHealth());
            coordinator = new StoreBackedPipelineCaptureCoordinator(store, starter,
                    new SrsCoordinator(store.meta()), new SnapshotBuffer(), dispatcher, budget);
        }
        private void startBoth() { coordinator.startCapture("a"); coordinator.startCapture("b"); }
    }

    @Configuration(proxyBeanMethods = false)
    static class FixtureContext {
        @Bean(destroyMethod = "close") LifecycleWorkDispatcher lifecycleWorkDispatcher(Fixture fixture) { return fixture.dispatcher; }
        @Bean(destroyMethod = "close") StoreBackedPipelineCaptureCoordinator pipelineCaptureCoordinator(
                Fixture fixture, LifecycleWorkDispatcher dispatcher) {
            assertThat(dispatcher).isSameAs(fixture.dispatcher);
            return fixture.coordinator;
        }
        @Bean BooleanSupplier localCaptureShutdownComplete(StoreBackedPipelineCaptureCoordinator coordinator) {
            return new DataPlaneActuationConfiguration().localCaptureShutdownComplete(coordinator);
        }
    }

    private static AnnotationConfigApplicationContext context(Fixture fixture) {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(Fixture.class, () -> fixture);
        context.register(FixtureContext.class);
        context.refresh();
        return context;
    }
    private static DesiredState desired(String id) { return new DesiredState(id, PipelineState.RUNNING, "controlled-revision"); }
    private static ConvergeResult done() { return new ConvergeResult(ConvergeStatus.NOTHING_TO_DO, Optional.empty(), Optional.empty()); }
    private static void awaitIgnoringInterrupt(CountDownLatch release) {
        boolean interrupted = false;
        try {
            while (release.getCount() != 0) {
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) { throw new AssertionError("the controlled native operation was not released"); }
                } catch (InterruptedException ignored) { interrupted = true; }
            }
        } finally { if (interrupted) { Thread.currentThread().interrupt(); } }
    }
}
