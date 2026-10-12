package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A step the plan runs natively runs its per-member count on every member, and every change of one row still
 * meets on one processor in the order it was read.
 *
 * <p>Two members, two processors of the step on each: four in all. Every row is changed several times in a row,
 * and each processor records which rows it saw and in what order. One row seen by two processors, or one row's
 * changes seen out of order, is exactly what routing by anything but the row's key produces - and on one member
 * with one processor neither can happen whatever the routing, which is why this runs wider than that.
 */
class ANativeStepKeepsEachKeyOnOneProcessorTest {

    /** Which processor instance saw each row, and in what order it saw that row's versions. */
    private static final Map<Integer, Set<Integer>> PROCESSORS_BY_ROW = new ConcurrentHashMap<>();
    private static final Map<Integer, List<Integer>> VERSIONS_BY_ROW = new ConcurrentHashMap<>();

    private HazelcastInstance first;
    private HazelcastInstance second;

    @BeforeEach
    void startTwoMembers() {
        PROCESSORS_BY_ROW.clear();
        VERSIONS_BY_ROW.clear();
        String cluster = "native-step-" + System.nanoTime();
        first = Hazelcast.newHazelcastInstance(clustered(cluster));
        second = Hazelcast.newHazelcastInstance(clustered(cluster));
        assertThat(first.getCluster().getMembers()).hasSize(2);
    }

    @AfterEach
    void stopBoth() {
        if (second != null) {
            second.shutdown();
        }
        if (first != null) {
            first.shutdown();
        }
    }

    private static Config clustered(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(15811).setPortAutoIncrement(true).setPortCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15811", "127.0.0.1:15812"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        return config;
    }

    private static PipelineResource pipeline() {
        return new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders")),
                List.of(Step.inline("stamp",
                        FromClause.list(FromRef.literal("orders")),
                        new TransformBody.Js("row"), null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("stamp"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null)),
                        null, null),
                null, null);
    }

    private static DagBindings bindings(String sink) {
        // A port per processor, so the instance identifies the processor: each keeps its own identity.
        SupplierEx<TransformPort> recording = () -> new RecordingPort();
        SupplierEx<SinkWriter> collecting = () -> new CollectingSinkWriter(sink);
        return new DagBindings(
                sourceId -> ProcessorMetaSupplier.forceTotalParallelismOne(
                        ProcessorSupplier.of((SupplierEx<Processor>) VersionedRows::new), "orders"),
                step -> recording,
                syncElement -> collecting,
                ref -> Map.of(
                        FromRef.literal("orders"), List.of("orders"),
                        FromRef.literal("stamp"), List.of("stamp")).getOrDefault(ref, List.of()));
    }

    private static ExecutionShape stampRunsTwoPerMember(int plannedMembers) {
        return new ExecutionShape(plannedMembers,
                Map.of("stamp", new NodeParallelism("stamp", 4, NodeParallelism.Origin.EXPLICIT,
                        NodeParallelism.Scope.NATIVE, plannedMembers, 2, 2 * plannedMembers, List.of())),
                Map.of("stamp", Map.of("orders", List.of("id"))));
    }

    @Test
    void everyRowIsSeenByOneProcessorInTheOrderItsVersionsWereRead() {
        DAG dag = PipelineDagBuilder.build(pipeline(), bindings("native"), null, null, stampRunsTwoPerMember(2));
        CollectingSinkWriter.reset("native");

        assertThat(dag.getVertex("stamp").getLocalParallelism()).isEqualTo(2);
        Edge into = dag.getInboundEdges("stamp").get(0);
        assertThat(into.getRoutingPolicy()).isEqualTo(Edge.RoutingPolicy.PARTITIONED);
        assertThat(into.isDistributed()).isTrue();

        first.getJet().newJob(dag).join();

        assertThat(CollectingSinkWriter.collected("native")).hasSize(VersionedRows.ROWS * VersionedRows.VERSIONS);
        assertThat(PROCESSORS_BY_ROW).hasSize(VersionedRows.ROWS);
        PROCESSORS_BY_ROW.forEach((row, processors) ->
                assertThat(processors).as("processors that saw row %s", row).hasSize(1));
        VERSIONS_BY_ROW.forEach((row, versions) ->
                assertThat(versions).as("versions of row %s in the order they arrived", row).isSorted()
                        .hasSize(VersionedRows.VERSIONS));
        // More than one processor did the work: a routing that sent everything to one would pass the two
        // checks above and run no wider than before.
        assertThat(PROCESSORS_BY_ROW.values().stream().flatMap(Set::stream).distinct().count())
                .isGreaterThan(1);
    }

    @Test
    void aRunStartingOnAMemberCountOtherThanItsPlanIsRefusedBeforeAnyProcessorRuns() {
        DAG dag = PipelineDagBuilder.build(pipeline(), bindings("refused"), null, null, stampRunsTwoPerMember(1));
        CollectingSinkWriter.reset("refused");

        Throwable failure = catchThrowable(
                () -> first.getJet().newJob(dag, new JobConfig().setName("p")).join());

        assertThat(failure).isNotNull();
        // The engine hands back only a rendering of the cause once the job is over, so the cause itself is
        // read where the guard recorded it - on whichever member coordinated the start.
        Throwable recorded = JobFailureRegistry.of(first).get("p")
                .or(() -> JobFailureRegistry.of(second).get("p"))
                .orElseThrow();
        assertThat(recorded).isInstanceOfSatisfying(TapstateException.class, refused -> {
            assertThat(refused.code()).isEqualTo(EngineError.MEMBERSHIP_CHANGED_BEFORE_START);
            assertThat(refused.args()).containsEntry("pipeline", "p")
                    .containsEntry("planned", 1).containsEntry("actual", 2);
        });
        assertThat(PROCESSORS_BY_ROW).isEmpty();
        assertThat(CollectingSinkWriter.collected("refused")).isEmpty();
    }

    /** A pass-through port that records which of its instances saw each row and in which order. */
    private static final class RecordingPort implements TransformPort {
        @Override
        public List<Envelope> transform(Envelope event) {
            int row = (Integer) event.after().get("id");
            int version = (Integer) event.after().get("version");
            PROCESSORS_BY_ROW.computeIfAbsent(row, ignored -> ConcurrentHashMap.newKeySet())
                    .add(System.identityHashCode(this));
            VERSIONS_BY_ROW.computeIfAbsent(row, ignored -> new CopyOnWriteArrayList<>()).add(version);
            return List.of(event);
        }
    }

    /** A source that reads every row several times, each version an update of the one before. */
    private static final class VersionedRows extends AbstractProcessor {
        static final int ROWS = 40;
        static final int VERSIONS = 5;
        private final List<Envelope> pending = new ArrayList<>();
        private int next;

        @Override
        protected void init(Processor.Context context) {
            for (int version = 0; version < VERSIONS; version++) {
                for (int row = 0; row < ROWS; row++) {
                    Map<String, Object> after = Map.of("id", row, "version", version);
                    pending.add(version == 0
                            ? Envelope.insert(row, "orders", after, null)
                            : Envelope.update(row, "orders", Map.of("id", row, "version", version - 1), after,
                                    null));
                }
            }
        }

        @Override
        public boolean complete() {
            while (next < pending.size()) {
                if (!tryEmit(pending.get(next))) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }
}
