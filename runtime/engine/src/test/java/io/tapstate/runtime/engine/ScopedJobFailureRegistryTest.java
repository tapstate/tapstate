package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.JobStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ScopedJobFailureRegistryTest {
    @Test void anOldRecorderAndClearCannotOverwriteOrRemoveTheNewRunFailure() {
        var first = job(1, "run-a"); var next = job(2, "run-b"); var f = new EngineRunHandleTest.Fixture(first);
        var registry = JobFailureRegistry.of(f.member); var a = registry.attach("p", 1, "run-a");
        a.record(new IllegalStateException("first"));
        var oldHandle = f.engine.findRun("p", "run-a").orElseThrow();
        f.named.set(next); var b = registry.attach("p", 2, "run-b");
        var expected = new IllegalStateException("current"); b.record(expected);
        a.record(new IllegalStateException("late")); a.clear(); oldHandle.cancel();
        assertThat(registry.existing("p", 2, "run-b").get()).containsSame(expected);
        assertThat(registry.existing("p", 1, "run-a").get()).isEmpty(); assertThat(a.get()).isEmpty();
    }

    @Test void engineAttachmentPreservesAProcessorFailureRecordedBeforeSubmissionReturned() {
        var current = job(2, "run-b"); var f = new EngineRunHandleTest.Fixture(current);
        var registry = JobFailureRegistry.of(f.member); var processor = registry.attach("p", 2, "run-b");
        var expected = new IllegalStateException("fast-failure"); processor.record(expected);
        var submitted = f.engine.submitRun("p", new com.hazelcast.jet.core.DAG(), java.util.Map.of(),
                io.tapstate.runtime.engine.nest.NestSettings.defaults(), "run-b");
        current.status.set(JobStatus.FAILED);
        assertThat(submitted.failure()).containsSame(expected);
        assertThat(registry.existing("p", 2, "run-b").get()).containsSame(expected);
    }

    @Test void anEqualIdentityStillDoesNotMergeDifferentConcreteJobs() {
        var f = new EngineRunHandleTest.Fixture(job(1, "same")); var registry = JobFailureRegistry.of(f.member);
        var first = registry.attach("p", 1, "same"); first.record(new IllegalStateException("first"));
        f.named.set(job(2, "same")); var next = registry.attach("p", 2, "same"); var expected = new IllegalStateException("current"); next.record(expected);
        first.record(new IllegalStateException("late")); first.clear();
        assertThat(registry.existing("p", 1, "same").get()).isEmpty();
        assertThat(registry.existing("p", 2, "same").get()).containsSame(expected);
    }

    @Test void legacyAndReadOnlyOldRunAccessCannotChangeAScopedSlot() {
        var first = job(1, "run-a"); var next = job(2, "run-b"); var f = new EngineRunHandleTest.Fixture(next);
        var registry = JobFailureRegistry.of(f.member); var expected = new IllegalStateException("current"); registry.attach("p", 2, "run-b").record(expected);
        registry.record("p", new IllegalStateException("legacy")); registry.clear("p"); assertThat(registry.get("p")).isEmpty();
        f.named.set(first);
        // The lookup returns its already-read old Job, while subsequent authoritative reads see the successor.
        f.onJobLookup = () -> f.named.set(next);
        f.engine.findRun("p", "run-a").orElseThrow().cancel();
        registry.existing("p", 1, "run-a").record(new IllegalStateException("read-must-not-attach"));
        assertThat(registry.existing("p", 2, "run-b").get()).containsSame(expected);
    }

    @Test void aClearedRecorderCannotReinsertItsFailureThroughALateCallback() {
        var f = new EngineRunHandleTest.Fixture(job(1, "run-a")); var registry = JobFailureRegistry.of(f.member);
        var original = registry.attach("p", 1, "run-a"); original.record(new IllegalStateException("before")); original.clear();
        original.record(new IllegalStateException("after-clear"));
        assertThat(registry.existing("p", 1, "run-a").get()).isEmpty(); assertThat(original.get()).isEmpty();
    }

    @Test void aSlowOriginalAttachCannotReplaceTheSlotPublishedByTheFollowingJob() throws Exception {
        var f = new EngineRunHandleTest.Fixture(job(1, "run-a")); var registry = JobFailureRegistry.of(f.member);
        var checked = new CountDownLatch(1); var resume = new CountDownLatch(1); var first = new AtomicBoolean(true);
        f.onJobLookup = () -> { if (first.compareAndSet(true, false)) { checked.countDown(); await(resume); } };
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> registry.attach("p", 1, "run-a")); assertThat(checked.await(10, TimeUnit.SECONDS)).isTrue();
            f.named.set(job(2, "run-b")); var nextStarted = new CountDownLatch(1);
            var b = executor.submit(() -> { nextStarted.countDown(); return registry.attach("p", 2, "run-b"); });
            assertThat(nextStarted.await(10, TimeUnit.SECONDS)).isTrue();
            try { b.get(1, TimeUnit.SECONDS); } catch (TimeoutException serialized) { /* The slow update may still own the original PID slot. */ }
            finally { resume.countDown(); }
            var old = a.get(10, TimeUnit.SECONDS); var current = b.get(10, TimeUnit.SECONDS);
            var expected = new IllegalStateException("current"); current.record(expected); old.record(new IllegalStateException("late")); old.clear();
            assertThat(registry.existing("p", 2, "run-b").get()).containsSame(expected);
            assertThat(registry.existing("p", 1, "run-a").get()).isEmpty();
        } finally { resume.countDown(); }
    }

    @Test void aHandleFoundBeforeAttachmentReadsTheLaterTypedFailureOfItsExactJob() {
        var original = job(1, "run-a"); var f = new EngineRunHandleTest.Fixture(original);
        var early = f.engine.findRun("p", "run-a").orElseThrow();
        var registry = JobFailureRegistry.of(f.member);
        var expected = new io.tapstate.core.common.TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, java.util.Map.of("pipeline", "p"), null);
        registry.attach("p", 1, "run-a").record(expected); original.status.set(JobStatus.FAILED);
        original.future.completeExceptionally(new IllegalStateException("degraded job result"));
        assertThat(early.failure()).containsSame(expected);
    }

    @Test void aHandleFoundBeforeAttachmentClearsOnlyTheLaterSlotOfItsExactJob() {
        var original = job(1, "run-a"); var f = new EngineRunHandleTest.Fixture(original);
        var early = f.engine.findRun("p", "run-a").orElseThrow(); var registry = JobFailureRegistry.of(f.member);
        registry.attach("p", 1, "run-a").record(new IllegalStateException("late attachment")); early.cancel();
        assertThat(registry.existing("p", 1, "run-a").get()).isEmpty();
        f.named.set(job(2, "run-b")); var expected = new IllegalStateException("current"); registry.attach("p", 2, "run-b").record(expected);
        early.cancel(); assertThat(registry.existing("p", 2, "run-b").get()).containsSame(expected);
    }

    @Test void aLateProcessorInitializationCannotRecreateTheSlotOfACancelledJob() {
        var original = job(1, "run-a"); var f = new EngineRunHandleTest.Fixture(original); var registry = JobFailureRegistry.of(f.member);
        registry.forJob(1, original.job.getConfig()).record(new IllegalStateException("before"));
        var run = f.engine.findRun("p", "run-a").orElseThrow(); run.cancel();
        registry.forJob(1, original.job.getConfig()).record(new IllegalStateException("late init"));
        assertThat(registry.existing("p", 1, "run-a").get()).isEmpty(); assertThat(run.failure()).isEmpty();
    }

    @Test void cancellationBeforeAnySlotAlsoPreventsALateInitializationOfThatJob() {
        var original = job(1, "run-a"); var f = new EngineRunHandleTest.Fixture(original); var registry = JobFailureRegistry.of(f.member);
        var run = f.engine.findRun("p", "run-a").orElseThrow(); run.cancel();
        registry.forJob(1, original.job.getConfig()).record(new IllegalStateException("late init"));
        assertThat(registry.existing("p", 1, "run-a").get()).isEmpty(); assertThat(run.failure()).isEmpty();
        f.named.set(job(2, "run-a")); var current = f.named.get();
        var expected = new IllegalStateException("next concrete job"); registry.forJob(2, current.job.getConfig()).record(expected);
        assertThat(registry.existing("p", 2, "run-a").get()).containsSame(expected);
    }

    @Test void cancellationBeforeAttachmentAlsoReplacesThePredecessorJobsRemainingSlot() {
        var f = new EngineRunHandleTest.Fixture(job(1, "run-a")); var registry = JobFailureRegistry.of(f.member);
        registry.forJob(1, f.named.get().job.getConfig()).record(new IllegalStateException("previous job"));
        var current = job(2, "run-b"); f.named.set(current);
        var early = f.engine.findRun("p", "run-b").orElseThrow(); early.cancel();
        registry.forJob(2, current.job.getConfig()).record(new IllegalStateException("late current init"));
        assertThat(registry.existing("p", 2, "run-b").get()).isEmpty(); assertThat(early.failure()).isEmpty();
    }

    private static EngineRunHandleTest.FakeJob job(long id, String identity) { return new EngineRunHandleTest.FakeJob(id, identity, JobStatus.RUNNING); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("attach barrier"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
