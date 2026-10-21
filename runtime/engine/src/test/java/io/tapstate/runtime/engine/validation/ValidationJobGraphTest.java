package io.tapstate.runtime.engine.validation;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;

/** Two real loopback members in one test JVM; not a multi-host deployment or a durable handover proof. */
class ValidationJobGraphTest {
    private static final Map<String, ConcurrentLinkedQueue<String>> LOCATIONS = new ConcurrentHashMap<>();
    private static final Map<String, ConcurrentLinkedQueue<Integer>> RESULTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicBoolean> RELEASE = new ConcurrentHashMap<>();
    private static final ValidationJobGraph.Limits LIMITS = new ValidationJobGraph.Limits(4, 4, 2);

    @Test @Timeout(100)
    void allVerticesAndEdgesStayOnTheSelectedMemberAndJobsCancelIndependentlyWithoutRecovery() throws Exception {
        String cluster = "validation-placement-" + UUID.randomUUID();
        HazelcastInstance first = null;
        HazelcastInstance second = null;
        try {
            first = Hazelcast.newHazelcastInstance(config(cluster, null));
            Address seed = first.getCluster().getLocalMember().getAddress();
            second = Hazelcast.newHazelcastInstance(config(cluster, seed));
            HazelcastInstance coordinator = first;
            HazelcastInstance target = second;
            await(() -> coordinator.getCluster().getMembers().size() == 2 && target.getCluster().getMembers().size() == 2);
            var member = second.getCluster().getLocalMember();
            Address owner = member.getAddress();
            String expectedLocation = member.getUuid().toString();
            assertThat(owner).isNotEqualTo(seed);

            String complete = id("complete");
            RELEASE.put(complete, new AtomicBoolean(true));
            var graph = graph(complete, owner, member.getUuid());
            Job job = graph.submit(first.getJet(), () -> { });
            job.getFuture().get(20, TimeUnit.SECONDS);
            assertThat(job.getStatus()).isEqualTo(JobStatus.COMPLETED);
            assertThat(RESULTS.get(complete)).containsExactlyElementsOf(IntStream.range(0, 32).map(i -> i * 2).boxed().toList());
            locations(complete, expectedLocation);
            assertThat(job.getName()).isEqualTo("validation:" + complete);
            var config = job.getConfig();
            assertThat(config.getProcessingGuarantee()).isEqualTo(ProcessingGuarantee.NONE);
            assertThat(config.isAutoScaling()).isFalse();
            assertThat(config.isSuspendOnFailure()).isFalse();
            assertThat(config.isSplitBrainProtectionEnabled()).isFalse();
            assertThat(config.getInitialSnapshotName()).isNull();
            assertThat(config.<String>getArgument("validation.owner.member")).isEqualTo(expectedLocation);
            assertThatThrownBy(() -> graph.submit(coordinator.getJet(), () -> { })).isInstanceOf(IllegalStateException.class);

            // Matching address with a different member incarnation is not the selected owner.
            String stale = id("stale");
            RELEASE.put(stale, new AtomicBoolean(true));
            Job staleJob = graph(stale, owner, UUID.randomUUID()).submit(first.getJet(), () -> { });
            // Jet transports an unknown application exception as JetException with its coded text;
            // it does not promise to reconstruct the application's Throwable subtype on the caller.
            assertThatThrownBy(() -> staleJob.getFuture().get(20, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .hasMessageContaining(ValidationError.OWNER_NOT_AUTHORIZED.code());
            assertThat(staleJob.getStatus()).isEqualTo(JobStatus.FAILED);
            assertThat(LOCATIONS.keySet()).noneMatch(key -> key.startsWith(stale + ":"));

            String cancelled = id("cancelled");
            String independent = id("independent");
            RELEASE.put(cancelled, new AtomicBoolean(false));
            RELEASE.put(independent, new AtomicBoolean(false));
            Job cancelledJob = graph(cancelled, owner, member.getUuid()).submit(first.getJet(), () -> { });
            Job independentJob = graph(independent, owner, member.getUuid()).submit(first.getJet(), () -> { });
            await(() -> initialized(cancelled) && initialized(independent));
            cancelledJob.cancel();
            assertThatThrownBy(() -> cancelledJob.getFuture().get(20, TimeUnit.SECONDS))
                    .isInstanceOfAny(java.util.concurrent.ExecutionException.class, java.util.concurrent.CancellationException.class);
            assertThat(independentJob.getStatus()).isEqualTo(JobStatus.RUNNING);
            RELEASE.get(independent).set(true);
            independentJob.getFuture().get(20, TimeUnit.SECONDS);
            locations(independent, expectedLocation);
            assertThat(RESULTS.get(independent)).hasSize(32);

            String departing = id("departing");
            RELEASE.put(departing, new AtomicBoolean(false));
            Job departingJob = graph(departing, owner, member.getUuid()).submit(first.getJet(), () -> { });
            await(() -> initialized(departing));
            second.getLifecycleService().terminate();
            second = null;
            assertThatThrownBy(() -> departingJob.getFuture().get(20, TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(departingJob.getStatus()).isEqualTo(JobStatus.FAILED);
            locations(departing, expectedLocation);
            assertThat(RESULTS.getOrDefault(departing, new ConcurrentLinkedQueue<>())).isEmpty();
        } finally {
            if (second != null && second.getLifecycleService().isRunning()) second.shutdown();
            if (first != null && first.getLifecycleService().isRunning()) first.shutdown();
            LOCATIONS.clear(); RESULTS.clear(); RELEASE.clear();
        }
    }

    private static ValidationJobGraph graph(String label, Address address, UUID member) {
        var owner = new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 2, 3);
        var run = new ValidationRunIdentity(owner, label, label, ValidationMode.FULL, "fixed-spec", 1, "fixed-rules");
        var graph = new ValidationJobGraph(run, address, member, LIMITS);
        for (String stage : List.of("read", "check", "result")) {
            graph.vertex(stage, ProcessorSupplier.of((SupplierEx<Processor>) () -> new Probe(label, stage)));
        }
        graph.edge("read", 0, "check", 0);
        graph.edge("check", 0, "result", 0);
        return graph;
    }

    private static Config config(String cluster, Address seed) {
        var config = new Config().setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.partition.count", "17");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", "2");
        config.setProperty("hazelcast.event.thread.count", "1");
        config.setProperty("hazelcast.io.input.thread.count", "1");
        config.setProperty("hazelcast.io.output.thread.count", "1");
        config.setProperty("hazelcast.wait.seconds.before.join", "0");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(seed == null ? List.of()
                : List.of(seed.getHost() + ":" + seed.getPort()));
        return config;
    }

    private static final class Probe extends AbstractProcessor {
        private final String label;
        private final String stage;
        private int next;
        private Probe(String label, String stage) { this.label = label; this.stage = stage; }
        @Override protected void init(Processor.Context context) {
            assertThat(context.jobConfig().<String>getArgument("validation.run")).isEqualTo(label);
            LOCATIONS.computeIfAbsent(label + ":" + stage, ignored -> new ConcurrentLinkedQueue<>())
                    .add(context.hazelcastInstance().getCluster().getLocalMember().getUuid().toString());
        }
        @Override public boolean complete() {
            if (!stage.equals("read")) return true;
            if (!RELEASE.get(label).get()) return false;
            while (next < 32) {
                if (!tryEmit(next)) return false;
                next++;
            }
            return true;
        }
        @Override protected boolean tryProcess(int ordinal, Object item) {
            if (stage.equals("check")) return tryEmit(((Integer)item) * 2);
            if (stage.equals("result")) {
                RESULTS.computeIfAbsent(label, ignored -> new ConcurrentLinkedQueue<>()).add((Integer)item);
                return true;
            }
            throw new IllegalStateException("a read vertex cannot consume data");
        }
    }

    private static String id(String name) { return name + "-" + UUID.randomUUID(); }
    private static boolean initialized(String label) {
        return List.of("read", "check", "result").stream().allMatch(stage -> LOCATIONS.containsKey(label + ":" + stage));
    }
    private static void locations(String label, String owner) {
        for (String stage : List.of("read", "check", "result")) {
            assertThat(new ArrayList<>(LOCATIONS.get(label + ":" + stage))).containsExactly(owner);
        }
    }
    private static void await(BooleanSupplier done) throws InterruptedException {
        long end = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!done.getAsBoolean()) {
            if (System.nanoTime() - end >= 0) fail("member or processor did not become ready");
            Thread.sleep(10);
        }
    }
}
