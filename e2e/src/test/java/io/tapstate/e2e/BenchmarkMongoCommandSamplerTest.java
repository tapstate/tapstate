package io.tapstate.e2e;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Server-wide Mongo command counters are parsed and compared without application instrumentation. */
class BenchmarkMongoCommandSamplerTest {

    @Test
    void deltasKeepRawCommandNamesAndStableFamiliesWithoutCountingSamplerReads() {
        BenchmarkMongoCommandSampler.Snapshot before = BenchmarkMongoCommandSampler.parse(status(100,
                Map.of("find", 10L, "update", 3L, "serverStatus", 5L, "ping", 2L)));
        BenchmarkMongoCommandSampler.Snapshot after = BenchmarkMongoCommandSampler.parse(status(105,
                Map.of("find", 13L, "update", 5L, "createIndexes", 1L,
                        "serverStatus", 6L, "ping", 4L)));

        BenchmarkMongoCommandSampler.Summary delta =
                BenchmarkMongoCommandSampler.difference(before, after, 500);
        assertThat(delta.byCommand()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "find", 3L, "update", 2L, "createIndexes", 1L, "ping", 2L));
        assertThat(delta.byCommand()).doesNotContainKey("serverStatus");
        assertThat(delta.byFamily()).containsEntry(BenchmarkMongoCommandSampler.Family.READ, 3L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.WRITE, 2L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.SCHEMA, 1L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.OTHER, 2L)
                .containsEntry(BenchmarkMongoCommandSampler.Family.TRANSACTION, 0L);
        assertThat(delta.totalCommands()).isEqualTo(8);
        assertThat(delta.elapsedMillis()).isEqualTo(500);
    }

    @Test
    void unavailableMalformedOrResetCountersFailClosed() {
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.parse(new Document("ok", 1)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("no complete command-counter header");
        Document malformed = status(100, Map.of("find", 1L));
        malformed.get("metrics", Document.class).get("commands", Document.class)
                .put("update", new Document("failed", 0));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.parse(malformed))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter is unavailable: update");

        BenchmarkMongoCommandSampler.Snapshot before = BenchmarkMongoCommandSampler.parse(status(100,
                Map.of("find", 10L, "update", 3L)));
        BenchmarkMongoCommandSampler.Snapshot backward = BenchmarkMongoCommandSampler.parse(status(105,
                Map.of("find", 9L, "update", 4L)));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.difference(before, backward, 1))
                .isInstanceOf(AssertionError.class).hasMessageContaining("moved backward: find");

        BenchmarkMongoCommandSampler.Snapshot missing = BenchmarkMongoCommandSampler.parse(status(105,
                Map.of("find", 11L)));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.difference(before, missing, 1))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter disappeared: update");

        BenchmarkMongoCommandSampler.Snapshot restarted = BenchmarkMongoCommandSampler.parse(status(10,
                Map.of("find", 11L, "update", 4L)));
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.difference(before, restarted, 1))
                .isInstanceOf(AssertionError.class).hasMessageContaining("server restart");
    }

    @Test
    void MongoSevenScalarUnknownCommandIsRetainedButOtherScalarShapesAreRefused() {
        Document status = status(100, Map.of("find", 1L));
        status.get("metrics", Document.class).get("commands", Document.class).put("<UNKNOWN>", 2L);
        assertThat(BenchmarkMongoCommandSampler.parse(status).commandTotals())
                .containsEntry("<UNKNOWN>", 2L);
        status.get("metrics", Document.class).get("commands", Document.class).put("update", 2L);
        assertThatThrownBy(() -> BenchmarkMongoCommandSampler.parse(status))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter is unavailable: update");
    }

    @Test void checkpoint_reads_one_immutable_prefix_without_resetting_the_finish_baseline() {
        AtomicInteger reads = new AtomicInteger(); AtomicLong clock = new AtomicLong();
        try (var sampler = BenchmarkMongoCommandSampler.from(() -> {
            int index = reads.incrementAndGet();
            return BenchmarkMongoCommandSampler.parse(status(100 + index,
                    Map.of("find", 10L + index, "update", 20L + 2 * index, "serverStatus", (long) index)));
        }, () -> clock.addAndGet(1_000_000))) {
            sampler.start(); var checkpoint = sampler.checkpoint();
            assertThat(checkpoint.startedAtNanos()).isEqualTo(2_000_000);
            assertThat(checkpoint.completedAtNanos()).isEqualTo(3_000_000);
            assertThat(checkpoint.summary().byCommand()).containsExactlyInAnyOrderEntriesOf(Map.of("find", 1L, "update", 2L));
            assertThat(checkpoint.summary().byCommand()).doesNotContainKey("serverStatus");
            assertThatThrownBy(() -> checkpoint.summary().byCommand().put("find", 99L)).isInstanceOf(UnsupportedOperationException.class);
            var full = sampler.finish();
            assertThat(full.byCommand()).containsExactlyInAnyOrderEntriesOf(Map.of("find", 2L, "update", 4L));
            assertThat(full.elapsedMillis()).isEqualTo(3); assertThat(reads).hasValue(3);
            assertThat(checkpoint.summary().byCommand()).containsEntry("find", 1L);
        }
    }

    @Test void default_command_path_performs_only_the_original_two_reads_and_a_failed_checkpoint_does_not_reset_it() {
        AtomicInteger normalReads = new AtomicInteger();
        try (var sampler = BenchmarkMongoCommandSampler.from(() -> {
            int value = normalReads.incrementAndGet(); return BenchmarkMongoCommandSampler.parse(status(value, Map.of("find", (long) value)));
        }, new AtomicLong()::incrementAndGet)) {
            sampler.start(); assertThat(sampler.finish().totalCommands()).isEqualTo(1); assertThat(normalReads).hasValue(2);
        }
        AtomicInteger reads = new AtomicInteger(); IllegalStateException primary = new IllegalStateException("controlled command failure");
        try (var sampler = BenchmarkMongoCommandSampler.from(() -> {
            int value = reads.incrementAndGet(); if (value == 2) { throw primary; }
            return BenchmarkMongoCommandSampler.parse(status(value, Map.of("find", 10L + value)));
        }, new AtomicLong()::incrementAndGet)) {
            sampler.start();
            var failure = org.assertj.core.api.Assertions.catchThrowableOfType(sampler::checkpoint, BenchmarkMongoCommandSampler.CheckpointFailure.class);
            assertThat(failure.getCause()).isSameAs(primary);
            assertThat(failure.reason()).isEqualTo(BenchmarkMongoCommandSampler.CheckpointReason.READ_FAILURE);
            assertThat(failure.readEvidence().orElseThrow().completedAtNanos()).isPresent();
            assertThat(failure.readEvidence().orElseThrow().snapshot()).isEmpty();
            assertThat(sampler.finish().byCommand()).containsEntry("find", 2L); assertThat(reads).hasValue(3);
        }
    }

    @Test void timed_out_checkpoint_retains_pending_then_actual_late_completion_without_retry_or_baseline_reset() throws Exception {
        AtomicInteger reads = new AtomicInteger(); CountDownLatch active = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<Future<BenchmarkMongoCommandSampler.Checkpoint>> future = new AtomicReference<>();
        TimeoutException primary = new TimeoutException("controlled wait timeout");
        try (var sampler = BenchmarkMongoCommandSampler.fromWithCheckpointWaiter(() -> {
            int index = reads.incrementAndGet();
            if (index == 2) { active.countDown(); await(release); }
            return BenchmarkMongoCommandSampler.parse(status(100 + index, Map.of("find", 10L + index)));
        }, new AtomicLong()::incrementAndGet, (pending, timeout, unit) -> {
            future.set(pending); assertThat(timeout).isEqualTo(5); assertThat(unit).isEqualTo(TimeUnit.SECONDS);
            await(active); throw primary;
        })) {
            try {
                sampler.start();
                var failure = org.assertj.core.api.Assertions.catchThrowableOfType(sampler::checkpoint, BenchmarkMongoCommandSampler.CheckpointFailure.class);
                assertThat(failure.reason()).isEqualTo(BenchmarkMongoCommandSampler.CheckpointReason.WAIT_TIMEOUT);
                assertThat(failure.getCause()).isSameAs(primary);
                var pending = failure.readEvidence().orElseThrow();
                assertThat(pending.startedAtNanos()).isEqualTo(2); assertThat(pending.completedAtNanos()).isEmpty();
                assertThat(pending.snapshot()).isEmpty(); assertThat(sampler.checkpointEvidence()).isEmpty(); assertThat(reads).hasValue(2);
                var rawPending = (Map<?, ?>) failure.retainedEvidence().get("actualRead");
                assertThat(rawPending.get("state")).isEqualTo("PENDING_READ");
                assertThat(rawPending.keySet().stream().map(Object::toString).toList()).doesNotContain("completedAtNanos");
                release.countDown(); var actual = future.get().get(2, TimeUnit.SECONDS);
                assertThat(actual.summary().byCommand()).containsEntry("find", 1L);
                assertThat(sampler.checkpointEvidence()).contains(actual);
                assertThat(sampler.checkpointReadEvidence().orElseThrow().completedAtNanos()).isPresent();
                assertThat(pending.completedAtNanos()).isEmpty();
                assertThat(sampler.finish().byCommand()).containsEntry("find", 2L); assertThat(reads).hasValue(3);
            } finally { release.countDown(); }
        }
    }

    @Test void a_blocked_checkpoint_read_does_not_hold_the_sampler_monitor_during_owner_cleanup() throws Exception {
        CountDownLatch active = new CountDownLatch(1), release = new CountDownLatch(1); AtomicInteger reads = new AtomicInteger();
        AtomicReference<Future<BenchmarkMongoCommandSampler.Checkpoint>> future = new AtomicReference<>();
        var sampler = BenchmarkMongoCommandSampler.fromWithCheckpointWaiter(() -> {
            int index = reads.incrementAndGet(); if (index == 2) { active.countDown(); await(release); }
            return BenchmarkMongoCommandSampler.parse(status(100 + index, Map.of("find", (long) index)));
        }, new AtomicLong()::incrementAndGet, (pending, timeout, unit) -> {
            future.set(pending); await(active); throw new TimeoutException("controlled timeout");
        });
        try {
            sampler.start(); assertThatThrownBy(sampler::checkpoint).isInstanceOf(BenchmarkMongoCommandSampler.CheckpointFailure.class);
            Thread closer = new Thread(sampler::close, "owned-checkpoint-close-control"); closer.setDaemon(true);
            closer.start(); closer.join(1000);
            assertThat(closer.isAlive()).as("a blocked checkpoint must not monopolize the sampler monitor").isFalse();
        } finally { release.countDown(); sampler.close(); }
    }

    @Test void checkpoint_owner_deadline_remains_bounded_when_the_actual_counter_read_does_not_return() throws Exception {
        AtomicInteger reads = new AtomicInteger(); CountDownLatch readStarted = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<Throwable> refusal = new AtomicReference<>();
        var sampler = BenchmarkMongoCommandSampler.from(() -> {
            if (reads.incrementAndGet() == 2) {
                readStarted.countDown();
                boolean interrupted = false;
                while (true) {
                    try { release.await(); break; }
                    catch (InterruptedException unavailable) { interrupted = true; }
                }
                if (interrupted) { Thread.currentThread().interrupt(); }
            }
            return BenchmarkMongoCommandSampler.parse(status(100, Map.of("find", (long) reads.get())));
        }, new AtomicLong()::incrementAndGet);
        Thread owner = new Thread(() -> {
            try { sampler.checkpoint(); } catch (Throwable failure) { refusal.set(failure); }
        }, "owned-command-checkpoint-deadline-control"); owner.setDaemon(true);
        try {
            sampler.start(); owner.start(); assertThat(readStarted.await(2, TimeUnit.SECONDS)).isTrue();
            owner.join(6000);
            assertThat(owner.isAlive()).as("checkpoint owner must return within its fixed five-second read deadline").isFalse();
            assertThat(refusal.get()).isInstanceOf(AssertionError.class);
            assertThat(reads).hasValue(2);
        } finally { release.countDown(); owner.join(2000); sampler.close(); }
    }

    private static void await(CountDownLatch latch) {
        try { assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }

    private static Document status(long uptimeMillis, Map<String, Long> totals) {
        Document commands = new Document();
        totals.forEach((name, total) -> commands.put(name, new Document("total", total).append("failed", 0)));
        return new Document("host", "benchmark-mongo").append("pid", 1234L)
                .append("uptimeMillis", uptimeMillis)
                .append("metrics", new Document("commands", commands));
    }
}
