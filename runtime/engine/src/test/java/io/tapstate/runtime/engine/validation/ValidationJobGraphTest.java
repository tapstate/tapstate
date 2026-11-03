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
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationRootCheck;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.core.validation.ValidationMethod;
import io.tapstate.core.validation.ValidationRelationRows;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationFieldCheck;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final Map<String, ConcurrentLinkedQueue<ValidationRootCheck>> ROOT_RESULTS = new ConcurrentHashMap<>();
    private static final Map<String, ConcurrentLinkedQueue<String>> ROOT_IO_THREADS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> ROOT_EXITS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> ROOT_READ_CLOSES = new ConcurrentHashMap<>();
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
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
            assertThat(job.getName()).isEqualTo(graph.jobName()).startsWith("validation:" + complete + ":");
            assertThat(graph.reconcile(first.getJet(), () -> {}).orElseThrow().getId()).isEqualTo(job.getId());
            var config = job.getConfig();
            assertThat(config.getProcessingGuarantee()).isEqualTo(ProcessingGuarantee.NONE);
            assertThat(config.isAutoScaling()).isFalse();
            assertThat(config.isSuspendOnFailure()).isFalse();
            assertThat(config.isSplitBrainProtectionEnabled()).isFalse();
            assertThat(config.getInitialSnapshotName()).isNull();
            assertThat(config.<String>getArgument("validation.owner.member")).isEqualTo(expectedLocation);
            assertThatThrownBy(() -> graph.submit(coordinator.getJet(), () -> { })).isInstanceOf(IllegalStateException.class);

            String roots = id("roots");
            ROOT_RESULTS.put(roots, new ConcurrentLinkedQueue<>());
            ROOT_IO_THREADS.put(roots, new ConcurrentLinkedQueue<>());
            ROOT_EXITS.put(roots, new AtomicInteger()); ROOT_READ_CLOSES.put(roots, new AtomicInteger());
            var rootGraph = new ValidationJobGraph(run(roots), owner, member.getUuid(), LIMITS);
            rootGraph.vertex("read", ProcessorSupplier.of((SupplierEx<Processor>) () -> new RootSource(roots)));
            rootGraph.vertex("check", new RootSupplier(roots));
            rootGraph.vertex("result", ProcessorSupplier.of((SupplierEx<Processor>) () -> new RootSink(roots)));
            rootGraph.edge("read", 0, "check", 0); rootGraph.edge("check", 0, "result", 0);
            Job rootJob = rootGraph.submit(first.getJet(), () -> {});
            rootJob.getFuture().get(20, TimeUnit.SECONDS);
            locations(roots, expectedLocation);
            assertThat(ROOT_RESULTS.get(roots)).extracting(result -> result.observedAgreement() ? "EQUAL" : result.counts().toString())
                    .containsExactly("EQUAL", "{TARGET_VALUE=1}", "{TARGET_MISSING=1}", "{TARGET_EXTRA=1}");
            assertThat(ROOT_IO_THREADS.get(roots)).hasSize(4).allMatch(name -> name.equals("validation-root-io-" + expectedLocation));
            assertThat(ROOT_EXITS.get(roots)).hasValue(4);
            assertThat(ROOT_READ_CLOSES.get(roots)).hasValue(8);

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
            ROOT_RESULTS.clear(); ROOT_IO_THREADS.clear(); ROOT_EXITS.clear(); ROOT_READ_CLOSES.clear();
        }
    }

    private static ValidationJobGraph graph(String label, Address address, UUID member) {
        var graph = new ValidationJobGraph(run(label), address, member, LIMITS);
        for (String stage : List.of("read", "check", "result")) {
            graph.vertex(stage, ProcessorSupplier.of((SupplierEx<Processor>) () -> new Probe(label, stage)));
        }
        graph.edge("read", 0, "check", 0);
        graph.edge("check", 0, "result", 0);
        return graph;
    }

    private static ValidationRunIdentity run(String label) {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 2, 3),
                label, label, ValidationMode.FULL, "fixed-spec", 1, "fixed-rules");
    }

    private static ValidationObservation observation(String label, int id) {
        return new ValidationObservation(run(label), new ValidationRoot("mapping", "output", key(id), List.of()), 1);
    }
    private static ValidationKey key(int id) { return new ValidationKey(List.of(ValidationKey.Part.number(id))); }
    private static void location(String label, String stage, Processor.Context context) {
        LOCATIONS.computeIfAbsent(label + ":" + stage, ignored -> new ConcurrentLinkedQueue<>())
                .add(context.hazelcastInstance().getCluster().getLocalMember().getUuid().toString());
    }

    private static final class RootSource extends AbstractProcessor {
        private final String label;
        private int next = 1;
        private RootSource(String label) { this.label = label; }
        @Override protected void init(Processor.Context context) { location(label, "read", context); }
        @Override public boolean complete() {
            while (next <= 4) { if (!tryEmit(observation(label, next))) return false; next++; }
            return true;
        }
    }
    private static final class RootSink extends AbstractProcessor {
        private final String label;
        private RootSink(String label) { this.label = label; }
        @Override protected void init(Processor.Context context) { location(label, "result", context); }
        @Override protected boolean tryProcess(int ordinal, Object item) {
            var result = (ValidationRootCheck)item;
            assertThat(result.observation().run()).isEqualTo(run(label));
            ROOT_RESULTS.get(label).add(result); return true;
        }
    }
    private static final class RootSupplier implements ProcessorSupplier {
        private final String label;
        private transient ThreadPoolExecutor io;
        private RootSupplier(String label) { this.label = label; }
        @Override public void init(Context context) {
            String member = context.hazelcastInstance().getCluster().getLocalMember().getUuid().toString();
            assertThat(context.jobConfig().<String>getArgument("validation.owner.member")).isEqualTo(member);
            LOCATIONS.computeIfAbsent(label + ":check", ignored -> new ConcurrentLinkedQueue<>()).add(member);
            io = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
                    runnable -> new Thread(runnable, "validation-root-io-" + member), new ThreadPoolExecutor.AbortPolicy());
        }
        @Override public Collection<? extends Processor> get(int count) {
            assertThat(count).isEqualTo(1);
            return List.of(new ValidationRootProcessor(run(label), (observation, guard, bytes) -> ValidationRootWork.submit(observation, io,
                    (scope, active) -> {
                        ROOT_IO_THREADS.get(label).add(Thread.currentThread().getName());
                        return rootRead(label, scope, active);
                    }, guard, () -> {}, () -> ROOT_EXITS.get(label).incrementAndGet(), bytes),
                    scope -> assertThat(scope.run()).isEqualTo(run(label)), 32768));
        }
        @Override public void close(Throwable error) throws Exception {
            if (io != null) { io.shutdown(); assertThat(io.awaitTermination(5, TimeUnit.SECONDS)).isTrue(); }
        }
    }

    private static ValidationRootCheck rootRead(String label, ValidationObservation scope, Runnable guard) {
        int id = ((Number)scope.root().rootKey().parts().getFirst().value()).intValue();
        return ValidationRootReader.read(scope, ValidationMethod.FULL_COLUMN,
                new ValidationRootReader.Query("source", "fixture-complete", () -> cursor(label, id, true)),
                new ValidationRootReader.Query("target", "fixture-complete", () -> cursor(label, id, false)),
                new ValidationRelationRows.Limits(65536, ROW), new ValidationRootCheck.Limits(32768, 20, 20, 10, 2),
                Clock.systemUTC(), guard, row -> new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(
                        ValidationRelationRows.Disposition.INCLUDED, row, Optional.of(physical(row, "source"))), List.of()),
                row -> new ValidationRootReader.Target(row, physical(row, "target")),
                (pair, root) -> ValidationFieldCheck.check(scope, List.of(), pair.source().working(), pair.target().working(),
                        pair.sourceSample(), pair.targetSample(), Optional.empty(), String::equals,
                        new ValidationValueComparison.TraversalLimits(ROW, 10000), root));
    }
    private static ValidationPhysicalIdentity physical(ValidationValues.Row row, String side) {
        return new ValidationPhysicalIdentity(side, "db", null, "records", List.of(List.of("id")), key((Integer)row.fields().get("id")));
    }
    private static ValidationRowCursor cursor(String label, int id, boolean source) {
        boolean exists = source ? id != 4 : id != 3;
        var row = ValidationValues.ownRow(Map.of("id", id, "amount", !source && id == 2 ? 99 : 1), ROW);
        return new ValidationRowCursor() {
            private boolean delivered;
            private boolean closed;
            @Override public Optional<Row> next() {
                if (closed) throw new IllegalStateException("fixture cursor closed");
                if (delivered || !exists) return Optional.empty();
                delivered = true;
                return Optional.of(new Row() {
                    @Override public long ordinal() { return 0; }
                    @Override public ValidationValues.Row values() { return row; }
                    @Override public void close() { }
                });
            }
            @Override public void close() { if (!closed) ROOT_READ_CLOSES.get(label).incrementAndGet(); closed = true; }
        };
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
