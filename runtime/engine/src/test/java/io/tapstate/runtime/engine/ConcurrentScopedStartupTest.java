package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import io.tapstate.core.common.TapstateException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class ConcurrentScopedStartupTest {

    private static final String STARTUP_BARRIER = ConcurrentScopedStartupTest.class.getName();
    private static final String LOOPBACK = "127.0.0.1";

    @Test
    void twoScopedJobsInitializeOnAWorkerWithTwoGenericOperationThreads() throws Exception {
        String cluster = "scoped-starts-" + System.nanoTime();
        HazelcastInstance coordinator = member(cluster, 4, List.of());
        try {
            int coordinatorPort = coordinator.getCluster().getLocalMember().getAddress().getPort();
            HazelcastInstance worker = member(cluster, 2, List.of(LOOPBACK + ":" + coordinatorPort));
            try {
                assertThat(worker.getCluster().getMembers()).hasSize(2);
                var barrier = new StartupBarrier(new CountDownLatch(2), new CountDownLatch(1));
                worker.getUserContext().put(STARTUP_BARRIER, barrier);
                try {
                    var first = coordinator.getJet().newJob(transform(cooperativeTransform()), scoped("first"));
                    var second = coordinator.getJet().newJob(transform(cooperativeTransform()), scoped("second"));
                    // Each StartExecutionOperation now holds one worker thread waiting for its own
                    // real transform to finish initializing. A named Jet lookup needs a free worker
                    // thread to execute the coordinator's follow-up query, which would wait in turn.
                    assertThat(barrier.entered().await(15, TimeUnit.SECONDS))
                            .as("both worker-side transform initializations reached the barrier")
                            .isTrue();
                    barrier.release().countDown();
                    try {
                        CompletableFuture.allOf(first.getFuture(), second.getFuture()).get(15, TimeUnit.SECONDS);
                    } catch (TimeoutException timedOut) {
                        throw new AssertionError("both worker-side initializations entered, but the scoped jobs did not finish",
                                timedOut);
                    }
                    var registry = JobFailureRegistry.of(worker);
                    assertThat(registry.retainedRunFailure("first", "first-run")).isPresent();
                    assertThat(registry.retainedRunFailure("second", "second-run")).isPresent();
                } finally {
                    barrier.release().countDown();
                }
            } finally {
                worker.getLifecycleService().terminate();
            }
        } finally {
            coordinator.getLifecycleService().terminate();
        }
    }

    @Test
    void scopedPlacementRefusalRetainsItsTypedCauseBeforeAnyProcessorExists() throws Exception {
        HazelcastInstance coordinator = member("scoped-refusal-" + System.nanoTime(), 2, List.of());
        try {
            var registry = JobFailureRegistry.of(coordinator);
            var previous = coordinator.getJet().newJob(transform(cooperativeTransform()),
                    scoped("refused").setArgument(Engine.RUN_IDENTITY_ARGUMENT, "previous-run"));
            previous.getFuture().get(15, TimeUnit.SECONDS);
            registry.attach("refused", previous.getId(), "previous-run");
            assertThat(registry.retainedRunFailure("refused", "previous-run")).isPresent();

            var barrier = new StartupBarrier(new CountDownLatch(1), new CountDownLatch(0));
            coordinator.getUserContext().put(STARTUP_BARRIER, barrier);
            var job = coordinator.getJet().newJob(
                    transform(PlannedMembersGuard.of(cooperativeTransform(), 2)), scoped("refused"));

            assertThatThrownBy(() -> job.getFuture().get(15, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            assertThat(registry.existing("refused", job.getId(), "refused-run").get())
                    .hasValueSatisfying(cause -> assertThat(cause).isInstanceOfSatisfying(TapstateException.class, refusal -> {
                        assertThat(refusal.code()).isEqualTo(EngineError.MEMBERSHIP_CHANGED_BEFORE_START);
                        assertThat(refusal.args()).containsEntry("planned", 2).containsEntry("actual", 1);
                    }));
            var local = registry.retainedRunFailure("refused", "refused-run").orElseThrow();
            assertThat(registry.attach("refused", job.getId(), "refused-run").get()).containsSame(local.cause());
            assertThat(registry.retainedRunFailure("refused", "previous-run")).isEmpty();
            assertThat(barrier.entered().getCount()).isOne();
        } finally {
            coordinator.getLifecycleService().terminate();
        }
    }

    @Test
    void aRetainedReadDoesNotAttributeAnEarlierJobsErrorToASuccessorWithTheSameRunIdentity() throws Exception {
        HazelcastInstance coordinator = member("scoped-replacement-" + System.nanoTime(), 2, List.of());
        try {
            var registry = JobFailureRegistry.of(coordinator);
            var first = coordinator.getJet().newJob(transform(cooperativeTransform()), scoped("replaced"));
            first.getFuture().get(15, TimeUnit.SECONDS);
            var firstFailure = new IllegalStateException("first job");
            registry.attach("replaced", first.getId(), "replaced-run").record(firstFailure);
            assertThat(registry.retainedRunFailure("replaced", "replaced-run").orElseThrow().cause())
                    .isSameAs(firstFailure);

            var second = coordinator.getJet().newJob(transform(cooperativeTransform()), scoped("replaced"));
            second.getFuture().get(15, TimeUnit.SECONDS);
            assertThat(registry.existing("replaced", second.getId(), "replaced-run").get()).isEmpty();
            assertThat(registry.retainedRunFailure("replaced", "replaced-run")).isEmpty();
            registry.attach("replaced", second.getId(), "replaced-run");
            assertThat(registry.existing("replaced", first.getId(), "replaced-run").get()).isEmpty();
            assertThat(registry.retainedRunFailure("replaced", "replaced-run").orElseThrow().cause()).isNull();
        } finally {
            coordinator.getLifecycleService().terminate();
        }
    }

    @Test
    void retirementBeforeProcessorInitializationKeepsTheExactJobRetired() throws Exception {
        HazelcastInstance coordinator = member("scoped-retirement-" + System.nanoTime(), 2, List.of());
        var barrier = new StartupBarrier(new CountDownLatch(1), new CountDownLatch(1));
        try {
            coordinator.getUserContext().put(STARTUP_BARRIER, barrier);
            var job = coordinator.getJet().newJob(transform(cooperativeTransform()), scoped("retired"));
            assertThat(barrier.entered().await(15, TimeUnit.SECONDS)).isTrue();
            var registry = JobFailureRegistry.of(coordinator);
            registry.retireRun("retired", job.getId(), "retired-run");
            barrier.release().countDown();
            job.getFuture().get(15, TimeUnit.SECONDS);
            assertThat(registry.retainedRunFailure("retired", "retired-run")).isEmpty();
        } finally {
            barrier.release().countDown();
            coordinator.getLifecycleService().terminate();
        }
    }

    private static HazelcastInstance member(String cluster, int genericThreads, List<String> seeds) {
        Config config = new Config().setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", Integer.toString(genericThreads));
        var network = config.getNetworkConfig();
        network.setPort(0).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface(LOOPBACK);
        var join = network.getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(seeds);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static JobConfig scoped(String name) {
        return new JobConfig().setName(name).setArgument(Engine.RUN_IDENTITY_ARGUMENT, name + "-run");
    }

    private static DAG transform(ProcessorMetaSupplier supplier) {
        var dag = new DAG();
        dag.newVertex("rows_through", supplier).localParallelism(1);
        return dag;
    }

    private static ProcessorMetaSupplier cooperativeTransform() {
        return ProcessorMetaSupplier.of(ProcessorSupplier.of(BarrierTransform::new));
    }

    private record StartupBarrier(CountDownLatch entered, CountDownLatch release) { }

    private static final class BarrierTransform extends ProcessorWrapper {

        private BarrierTransform() {
            super(new TransformProcessor(List::of));
        }

        @Override
        protected Context initContext(Context context) {
            if (context.hazelcastInstance().getUserContext().get(STARTUP_BARRIER) instanceof StartupBarrier barrier) {
                barrier.entered().countDown();
                try {
                    if (!barrier.release().await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("the startup barrier was never released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted at the startup barrier", interrupted);
                }
            }
            return super.initContext(context);
        }
    }
}
