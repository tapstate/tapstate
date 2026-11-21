package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProcessObservationWriterTest {
    private static final Instant AT = Instant.parse("2026-11-20T00:00:00Z");

    @Test void aSlowFlushKeepsOnlyTheNextLatestSnapshotAndDoesNotOverlapAnotherBatch() throws Exception {
        var cache = mock(MongoCacheDomain.class);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var writes = new ArrayList<List<MongoCacheDomain.ObservationWrite>>();
        when(cache.writeObservations(anyList())).thenAnswer(call -> {
            List<MongoCacheDomain.ObservationWrite> batch = call.getArgument(0);
            writes.add(batch);
            started.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return Collections.nCopies(batch.size(), null);
        });
        try (var writer = new ProcessObservationWriter(cache, false); var workers = Executors.newSingleThreadExecutor()) {
            var committed = new AtomicInteger();
            var first = stage(writer, "orders", 1);
            assertThat(writer.submit(first, committed::incrementAndGet, () -> {})).isTrue();
            var running = workers.submit(writer::flush);
            try {
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                for (int sample = 2; sample <= 1000; sample++)
                    assertThat(writer.submit(stage(writer, "orders", sample), committed::incrementAndGet, () -> {})).isTrue();
                writer.flush();
                assertThat(writer.usage().pending()).isOne();
                assertThat(writer.usage().inFlight()).isOne();
                assertThat(committed).hasValue(0);
            } finally {
                release.countDown();
            }
            running.get(5, TimeUnit.SECONDS);
            writer.flush();
            assertThat(writes).hasSize(2);
            assertThat(writes.get(1)).hasSize(1);
            assertThat(writes.get(1).getFirst().observation().metrics()).containsEntry("records.out", 1000L);
            assertThat(committed).hasValue(2);
            assertThat(writer.usage()).isEqualTo(new ProcessObservationWriter.Usage(0, 0, 0));
        }
    }

    @Test void durableCallbacksRunOnlyForTheirSuccessfulOriginalEntries() {
        var cache = mock(MongoCacheDomain.class);
        when(cache.writeObservations(anyList())).thenReturn(java.util.Arrays.asList(null, new IllegalStateException("fixture write refusal")));
        try (var writer = new ProcessObservationWriter(cache, false)) {
            var callbacks = new ArrayList<String>();
            var first = stage(writer, "first", 1);
            first.wrap(mock(ObservationStore.class)).afterSave(() -> {
                assertThat(org.slf4j.MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY)).isEqualTo("first");
                callbacks.add("first-watch");
            });
            writer.submit(first, () -> callbacks.add("first-history"), () -> callbacks.add("first-failed"));
            writer.submit(stage(writer, "second", 1), () -> callbacks.add("second-history"), () -> callbacks.add("second-failed"));
            assertThat(callbacks).isEmpty();
            writer.flush();
            assertThat(callbacks).containsExactly("first-watch", "first-history", "second-failed");
            assertThat(org.slf4j.MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY)).isNull();
        }
    }

    @Test void entryAndByteBudgetsRefuseGrowthAndDeletedPipelinesReleaseTheirSlots() {
        var cache = mock(MongoCacheDomain.class);
        try (var writer = new ProcessObservationWriter(cache, false)) {
            for (int pipeline = 0; pipeline < 1024; pipeline++)
                assertThat(writer.submit(stage(writer, "p" + pipeline, 1), () -> {}, () -> {})).isTrue();
            assertThat(writer.submit(stage(writer, "overflow", 1), () -> {}, () -> {})).isFalse();
            writer.offerHistory(history("synchronous-overflow", 1, () -> {}));
            assertThat(writer.usage().pending()).isEqualTo(1024);
            writer.retain(List.of("p0"));
            assertThat(writer.usage().pending()).isOne();
            var oversized = writer.stage(pass("oversized"));
            oversized.wrap(mock(ObservationStore.class)).save(new Observation("oversized", PipelineState.RUNNING,
                    Map.of(), Map.of(), Map.of("table", "x".repeat(3_700_000)), null, AT), 1);
            assertThat(writer.submit(oversized, () -> {}, () -> {})).isTrue();
            var overBudget = writer.stage(pass("over-budget"));
            overBudget.wrap(mock(ObservationStore.class)).save(new Observation("over-budget", PipelineState.RUNNING,
                    Map.of(), Map.of(), Map.of("table", "x".repeat(3_700_000)), null, AT), 1);
            assertThat(writer.submit(overBudget, () -> {}, () -> {})).isFalse();
            assertThat(writer.usage().pending()).isEqualTo(2);
            writer.retain(List.of());
            assertThat(writer.usage().retainedBytes()).isZero();
        }
    }

    @Test void aLifecyclePublicationDiscardsSupersededPendingFactsAndCompletesAtItsOriginalBarrier() {
        var cache = mock(MongoCacheDomain.class);
        try (var writer = new ProcessObservationWriter(cache, false)) {
            var callbacks = new ArrayList<String>();
            writer.submit(stage(writer, "orders", 1), () -> callbacks.add("queued"), () -> {});
            writer.now("orders", () -> callbacks.add("terminal"));
            assertThat(callbacks).containsExactly("terminal");
            writer.flush();
            assertThat(writer.usage()).isEqualTo(new ProcessObservationWriter.Usage(0, 0, 0));
            org.mockito.Mockito.verifyNoInteractions(cache);
        }
    }

    @Test void aLifecyclePublicationCompletesWhileAnIndependentHistoryAppendIsBlocked() throws Exception {
        var cache = mock(MongoCacheDomain.class);
        when(cache.writeObservations(anyList())).thenAnswer(call -> Collections.nCopies(((List<?>) call.getArgument(0)).size(), null));
        var appending = new CountDownLatch(1);
        var releaseHistory = new CountDownLatch(1);
        var publishing = new CountDownLatch(1);
        var samples = new AtomicInteger();
        try (var writer = new ProcessObservationWriter(cache, false); var workers = Executors.newFixedThreadPool(2)) {
            writer.submit(stage(writer, "slow", 1), () -> {}, history("slow", 1, () -> {
                appending.countDown();
                try {
                    assertThat(releaseHistory.await(5, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                samples.incrementAndGet();
            }), () -> {});
            var flush = workers.submit(writer::flush);
            try {
                assertThat(appending.await(5, TimeUnit.SECONDS)).isTrue();
                var barrier = workers.submit(() -> {
                    publishing.countDown();
                    writer.now("completing", () -> writer.offerHistory(history("completing", 2, () -> {
                        assertThat(samples).hasValue(1);
                        samples.incrementAndGet();
                    })));
                });
                assertThat(publishing.await(5, TimeUnit.SECONDS)).isTrue();
                barrier.get(1, TimeUnit.SECONDS);
                assertThat(flush.isDone()).isFalse();
                writer.flush();
                assertThat(samples).hasValue(0);
                assertThat(writer.usage().inFlight()).isOne();
                assertThat(writer.usage().pending()).isOne();
            } finally {
                releaseHistory.countDown();
            }
            flush.get(5, TimeUnit.SECONDS);
            writer.flush();
            assertThat(samples).hasValue(2);
        }
    }

    @Test void slowFailingHistoryRotatesBehindHealthyPipelinesWithinABoundedFlushAllowance() {
        var cache = mock(MongoCacheDomain.class);
        when(cache.writeObservations(anyList())).thenAnswer(call -> Collections.nCopies(((List<?>) call.getArgument(0)).size(), null));
        var time = new java.util.concurrent.atomic.AtomicLong();
        try (var writer = new ProcessObservationWriter(cache, false, time::get)) {
            var durable = new AtomicInteger();
            var histories = new ArrayList<String>();
            for (int round = 0; round < 2; round++) {
                for (String pipeline : List.of("slow", "healthy-a", "healthy-b")) {
                    writer.submit(stage(writer, pipeline, round), durable::incrementAndGet, history(pipeline, round, () -> {
                        histories.add(pipeline);
                        if (pipeline.equals("slow")) {
                            time.addAndGet(TimeUnit.SECONDS.toNanos(2));
                            throw new IllegalStateException("fixture history timeout");
                        }
                    }), () -> {});
                }
                writer.flush();
                if (round == 0) assertThat(histories).containsExactly("slow");
            }
            assertThat(durable).hasValue(6);
            assertThat(histories).containsExactly("slow", "healthy-a", "healthy-b", "slow");
            writer.retain(List.of());
            assertThat(writer.usage().retainedBytes()).isZero();
        }
    }

    @Test void anObservationAboveTheBatchTargetWritesAloneAndABoveBudgetSnapshotUsesTheSameSynchronousEntrance() {
        var cache = mock(MongoCacheDomain.class);
        var sizes = new ArrayList<Integer>();
        when(cache.writeObservations(anyList())).thenAnswer(call -> {
            List<?> batch = call.getArgument(0);
            sizes.add(batch.size());
            return Collections.nCopies(batch.size(), null);
        });
        try (var writer = new ProcessObservationWriter(cache, false)) {
            var committed = new AtomicInteger();
            var large = writer.stage(pass("large"));
            large.wrap(mock(ObservationStore.class)).save(new Observation("large", PipelineState.RUNNING,
                    Map.of(), Map.of(), Map.of("table", "x".repeat(2_100_000)), null, AT), 1);
            assertThat(writer.submit(large, committed::incrementAndGet, () -> {})).isTrue();
            assertThat(sizes).isEmpty();
            writer.flush();
            assertThat(sizes).containsExactly(1);
            var synchronous = writer.stage(pass("synchronous"));
            var sampled = new AtomicInteger();
            synchronous.wrap(mock(ObservationStore.class)).save(new Observation("synchronous", PipelineState.RUNNING,
                    Map.of(), Map.of(), Map.of("table", "x".repeat(4_300_000)), null, AT), 1);
            assertThat(writer.submit(synchronous, committed::incrementAndGet,
                    history("synchronous", 0, sampled::incrementAndGet), () -> {})).isTrue();
            assertThat(sizes).containsExactly(1, 1);
            assertThat(committed).hasValue(2);
            assertThat(sampled).hasValue(0);
            assertThat(writer.usage().retainedBytes()).isLessThan(1024 * 1024);
            writer.flush();
            assertThat(sampled).hasValue(1);
            assertThat(sizes).containsExactly(1, 1);
        }
    }

    @Test void aDelayedNativeExecutionGuardIsRetainedAndAnUnmeasuredFallbackKeepsItsOriginalGuard() {
        var cache = mock(MongoCacheDomain.class);
        when(cache.writeObservations(anyList())).thenAnswer(call -> {
            List<MongoCacheDomain.ObservationWrite> batch = call.getArgument(0);
            var failures = new ArrayList<RuntimeException>();
            for (var write : batch) {
                try { write.active().run(); failures.add(null); }
                catch (RuntimeException failure) { failures.add(failure); }
            }
            return failures;
        });
        try (var writer = new ProcessObservationWriter(cache, false)) {
            var current = new java.util.concurrent.atomic.AtomicBoolean(true);
            var checked = mock(ProcessObservationWriter.ActiveStore.class);
            org.mockito.Mockito.doAnswer(call -> {
                if (!current.get()) throw new IllegalStateException("fixture execution was superseded");
                return null;
            }).when(checked).requireWriteActive();
            var guarded = writer.stage(pass("guarded"));
            guarded.wrap(checked).save(new Observation("guarded", PipelineState.RUNNING,
                    Map.of("records.out", 1L), Map.of(), Map.of(), null, AT), 1);
            var callbacks = new ArrayList<String>();
            writer.submit(guarded, () -> callbacks.add("guarded-committed"), () -> callbacks.add("guarded-refused"));
            writer.submit(stage(writer, "fallback", 1), () -> callbacks.add("fallback-committed"), () -> callbacks.add("fallback-refused"));
            current.set(false);
            writer.flush();
            assertThat(callbacks).containsExactly("guarded-refused", "fallback-committed");
        }
    }

    @Test void anExecutionSupersededDuringTheBulkCannotPublishItsLateAlertsOrHistory() {
        var cache = mock(MongoCacheDomain.class);
        var current = new java.util.concurrent.atomic.AtomicBoolean(true);
        when(cache.writeObservations(anyList())).thenAnswer(call -> {
            current.set(false);
            return Collections.nCopies(((List<?>) call.getArgument(0)).size(), null);
        });
        try (var writer = new ProcessObservationWriter(cache, false)) {
            var checked = mock(ProcessObservationWriter.ActiveStore.class);
            org.mockito.Mockito.doAnswer(call -> {
                if (!current.get()) throw new IllegalStateException("fixture execution was superseded during IO");
                return null;
            }).when(checked).requireWriteActive();
            var stage = writer.stage(pass("orders"));
            stage.wrap(checked).save(new Observation("orders", PipelineState.RUNNING,
                    Map.of("records.out", 1L), Map.of(), Map.of(), null, AT), 1);
            var callbacks = new ArrayList<String>();
            writer.submit(stage, () -> callbacks.add("alert"), history("orders", 0, () -> callbacks.add("history")),
                    () -> callbacks.add("failed"));
            writer.flush();
            assertThat(callbacks).isEmpty();
            assertThat(writer.usage()).isEqualTo(new ProcessObservationWriter.Usage(0, 0, 0));
        }
    }

    private static ProcessObservationWriter.History history(String pipeline, long seconds, Runnable append) {
        return new ProcessObservationWriter.History(new RateSample(pipeline, AT.plusSeconds(seconds), Map.of("recordCount", 1L), Map.of(), null), append);
    }

    private static ProcessObservationWriter.Stage stage(ProcessObservationWriter writer, String pipeline, long count) {
        var stage = writer.stage(pass(pipeline));
        stage.wrap(mock(ObservationStore.class)).save(new Observation(pipeline, PipelineState.RUNNING,
                Map.of("records.out", count), Map.of(), Map.of(), null, AT.plusSeconds(count)), 7);
        return stage;
    }

    private static PipelineConvergencePass pass(String pipeline) {
        var pass = mock(PipelineConvergencePass.class);
        var scope = new PipelineIntentScope("cluster", 1, "cache", pipeline, 1);
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, pipeline),
                new WorkloadOwner("node", "boot"), 1, 1, 1);
        when(pass.scope()).thenReturn(scope);
        when(pass.claim()).thenReturn(claim);
        when(pass.guardOriginal(claim)).thenReturn(() -> {});
        return pass;
    }
}
