package io.tapstate.runtime.engine;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EngineRunHandleTest {
    @Test void aCapturedHandleNeverControlsOrWaitsForTheReplacementUnderTheSameName() {
        var first = new FakeJob(1, "run-a", JobStatus.RUNNING); var second = new FakeJob(2, "run-b", JobStatus.RUNNING);
        var fixture = new Fixture(first);
        var run = fixture.engine.findRun("p", "run-a").orElseThrow();
        fixture.named.set(second);
        run.suspend(); run.resume(); run.cancel();
        assertThat(run.awaitTerminal(Duration.ZERO)).isTrue();
        assertThat(run.jobId()).isEqualTo(1);
        assertThat(first.suspends.get()).isOne(); assertThat(first.resumes.get()).isOne(); assertThat(first.cancels.get()).isOne();
        assertThat(second.suspends.get()).isZero(); assertThat(second.resumes.get()).isZero(); assertThat(second.cancels.get()).isZero();
        assertThat(second.futures.get()).isZero(); assertThat(second.status.get()).isEqualTo(JobStatus.RUNNING);
    }

    @Test void aLateLookupDoesNotAdoptAnotherRunOrAnUnmarkedJob() {
        for (String identity : new String[]{"run-b", null}) {
            var replacement = new FakeJob(2, identity, JobStatus.RUNNING); var fixture = new Fixture(replacement);
            assertThat(fixture.engine.findRun("p", "run-a")).isEmpty();
            assertThat(replacement.cancels.get()).isZero();
        }
    }

    @Test void submissionRefusesAnActiveForeignRunWithoutSubmittingOrControllingIt() {
        var replacement = new FakeJob(2, "run-b", JobStatus.RUNNING); var fixture = new Fixture(replacement);
        assertThatThrownBy(() -> fixture.engine.submitRun("p", new DAG(), Map.of(), io.tapstate.runtime.engine.nest.NestSettings.defaults(), "run-a"))
                .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
        assertThat(fixture.submits.get()).isZero(); assertThat(replacement.cancels.get()).isZero();
    }

    @Test void aRaceReturningAnotherActiveJobCannotBeReportedAsTheRequestedSubmission() {
        var replacement = new FakeJob(2, "run-b", JobStatus.RUNNING); var fixture = new Fixture(null);
        fixture.onSubmit = () -> { fixture.named.set(replacement); return replacement.job; };
        assertThatThrownBy(() -> fixture.engine.submitRun("p", new DAG(), Map.of(), io.tapstate.runtime.engine.nest.NestSettings.defaults(), "run-a"))
                .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED));
        assertThat(fixture.submits.get()).isOne(); assertThat(replacement.cancels.get()).isZero();
        assertThat((Object) fixture.submitted.get().getArgument(Engine.RUN_IDENTITY_ARGUMENT)).isEqualTo("run-a");
    }

    @Test void waitingForAnEndingJobUsesTheObjectReadBeforeItsNameWasReused() {
        var first = new FakeJob(1, "run-a", JobStatus.COMPLETING); var second = new FakeJob(2, "run-b", JobStatus.RUNNING);
        var fixture = new Fixture(first); first.future.complete(null); second.future.complete(null);
        first.onStatus = () -> fixture.named.set(second);
        fixture.engine.submit("p", new DAG());
        assertThat(first.futures.get()).isOne(); assertThat(second.futures.get()).isZero();
    }

    static final class Fixture {
        final AtomicReference<FakeJob> named = new AtomicReference<>(); final AtomicInteger submits = new AtomicInteger();
        final AtomicReference<JobConfig> submitted = new AtomicReference<>(); java.util.function.Supplier<Job> onSubmit;
        final Engine engine;
        final HazelcastInstance member;
        Runnable onJobLookup = () -> {};
        Fixture(FakeJob initial) {
            named.set(initial);
            JetService jet = proxy(JetService.class, (method, args) -> switch (method) {
                case "getJob" -> { var captured = named.get(); onJobLookup.run(); yield captured == null ? null : captured.job; }
                case "newJobIfAbsent" -> { submits.incrementAndGet(); submitted.set((JobConfig) args[1]); yield onSubmit == null ? named.get().job : onSubmit.get(); }
                default -> throw new AssertionError("unexpected Jet operation: " + method);
            });
            var context = new ConcurrentHashMap<String, Object>(); var config = new Config();
            member = proxy(HazelcastInstance.class, (method, args) -> switch (method) {
                case "getJet" -> jet;
                case "getConfig" -> config;
                case "getDistributedObjects" -> java.util.List.of();
                case "getUserContext" -> context;
                default -> throw new AssertionError("unexpected member operation: " + method);
            });
            engine = new Engine(member);
        }
    }
    static final class FakeJob {
        final AtomicReference<JobStatus> status; final CompletableFuture<Void> future = new CompletableFuture<>();
        final AtomicInteger suspends = new AtomicInteger(), resumes = new AtomicInteger(), cancels = new AtomicInteger(), futures = new AtomicInteger();
        final Job job; Runnable onStatus = () -> {};
        FakeJob(long id, String identity, JobStatus initial) {
            status = new AtomicReference<>(initial); JobConfig config = new JobConfig().setName("p");
            if (identity != null) config.setArgument(Engine.RUN_IDENTITY_ARGUMENT, identity);
            job = proxy(Job.class, (method, args) -> switch (method) {
                case "getId" -> id;
                case "getConfig" -> config;
                case "getDistributedObjects" -> java.util.List.of();
                case "getName" -> "p";
                case "getStatus" -> { onStatus.run(); yield status.get(); }
                case "getFuture" -> { futures.incrementAndGet(); yield future; }
                case "suspend" -> { suspends.incrementAndGet(); status.set(JobStatus.SUSPENDED); yield null; }
                case "resume" -> { resumes.incrementAndGet(); status.set(JobStatus.RUNNING); yield null; }
                case "cancel" -> { cancels.incrementAndGet(); status.set(JobStatus.FAILED); future.completeExceptionally(new java.util.concurrent.CancellationException()); yield null; }
                default -> throw new AssertionError("unexpected job operation: " + method);
            });
        }
    }
    @SuppressWarnings("unchecked") private static <T> T proxy(Class<T> type, Call calls) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            if (method.getName().equals("toString")) return type.getSimpleName();
            return calls.call(method.getName(), args);
        });
    }
    @FunctionalInterface private interface Call { Object call(String method, Object[] args); }
}
