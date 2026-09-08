package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a pipeline built the ordinary way runs on a cluster of more than one member.
 *
 * <p>Every vertex the builder draws for a stateless step, for a union and for a sink is pinned to a
 * single processor in the whole cluster, because a sink acks an ordered stream of positions and a
 * second lane would break that order. Pinning alone is not enough: the one processor stands on one
 * member, so an edge that hands its items to whatever is local delivers half of them to a member that
 * has no processor of that vertex at all, and the job dies there. Being pinned and being reachable are
 * two separate properties, and the second is the one this case is about.
 *
 * <p><b>One member cannot tell the difference.</b> There the local processor is the only place input
 * can come from, so a pinned vertex is reachable no matter how its edges route - the same job, the same
 * output, the same everything. So this runs two members, and the sources it builds the pipeline over
 * run one processor on <em>each</em> of them. That is what makes the wrong-member case certain rather
 * than a coin toss: whichever member a pinned vertex lands on, the other one is producing for it.
 */
class ABuiltPipelineRunsAcrossMembersTest {

    private HazelcastInstance first;
    private HazelcastInstance second;

    @BeforeEach
    void startTwoMembers() {
        String cluster = "built-pipeline-across-members-" + System.nanoTime();
        first = Hazelcast.newHazelcastInstance(clustered(cluster));
        second = Hazelcast.newHazelcastInstance(clustered(cluster));
        assertThat(first.getCluster().getMembers())
                .describedAs("both members have to be in one cluster, or this is two separate "
                        + "single-member runs and the whole question does not arise")
                .hasSize(2);
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

    /** A member that joins others of the same cluster name over the loopback, configured as the app is. */
    private static Config clustered(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(15801).setPortAutoIncrement(true).setPortCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1:15801", "127.0.0.1:15802"));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        // The same registration the assembly root makes: a change is three row images of names to whatever
        // the source had, which nothing zero-configuration will write. Without it every edge between these
        // two members fails on its first event, before the question this case asks is reached.
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        return config;
    }

    @Test
    @DisplayName("a source on both members reaches the pinned transform and the pinned sink")
    void everyRowFromBothMembersReachesTheSinkThroughAStatelessStep() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("orders_src")),
                List.of(Step.inline("keep_even",
                        FromClause.list(FromRef.literal("orders_src")),
                        new TransformBody.Filter("row.id % 2 == 0"), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("keep_even"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null, null)),
                        null, null),
                null, null);

        SupplierEx<TransformPort> keepEvenIds = () -> event ->
                ((Integer) event.after().get("id")) % 2 == 0 ? List.of(event) : List.of();
        SupplierEx<SinkWriter> intoOut = () -> new CollectingSinkWriter("across");

        DagBindings bindings = new DagBindings(
                sourceId -> onEveryMember(sourceId, 10, 4),
                step -> keepEvenIds,
                syncElement -> intoOut,
                ref -> Map.of(
                        FromRef.literal("orders_src"), List.of("orders_src"),
                        FromRef.literal("keep_even"), List.of("keep_even")).getOrDefault(ref, List.of()));

        CollectingSinkWriter.reset("across");
        first.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings)).join();

        // Ids 10..13 on one member and 20..23 on the other, of which the step keeps the even ones. Both
        // decades have to be there: one of them missing is exactly the half that was produced on the
        // member the pinned vertices did not land on.
        assertThat(CollectingSinkWriter.collected("across"))
                .containsExactlyInAnyOrder(10, 12, 20, 22);
    }

    @Test
    @DisplayName("a union gathering two sources on both members reaches the pinned sink")
    void everyRowFromBothMembersReachesTheSinkThroughAUnion() {
        PipelineResource pipeline = new PipelineResource(
                "p", null,
                List.of(SourceRef.bare("a_src"), SourceRef.bare("b_src")),
                List.of(Step.inline("u",
                        FromClause.list(FromRef.literal("a_src"), FromRef.literal("b_src")),
                        new TransformBody.Union(), null, null)),
                null,
                new ServeBlock.Inline(null, FromRef.literal("u"),
                        List.of(new SyncElement("sync_1", "orders_dest", null, null, null, null)),
                        null, null),
                null, null);

        SupplierEx<SinkWriter> intoOut = () -> new CollectingSinkWriter("across");

        DagBindings bindings = new DagBindings(
                sourceId -> onEveryMember(sourceId, sourceId.equals("a_src") ? 100 : 200, 2),
                step -> {
                    throw new AssertionError("union must not consult transformPorts");
                },
                syncElement -> intoOut,
                ref -> Map.of(
                        FromRef.literal("a_src"), List.of("a_src"),
                        FromRef.literal("b_src"), List.of("b_src"),
                        FromRef.literal("u"), List.of("u")).getOrDefault(ref, List.of()));

        CollectingSinkWriter.reset("across");
        first.getJet().newJob(PipelineDagBuilder.build(pipeline, bindings)).join();

        assertThat(CollectingSinkWriter.collected("across"))
                .containsExactlyInAnyOrder(100, 101, 110, 111, 200, 201, 210, 211);
    }

    /**
     * A source running one processor on every member, each emitting {@code count} ids from its own
     * decade. Deliberately not pinned: what this case needs is a producer on the member a pinned vertex
     * did <em>not</em> land on, and a pinned source would be on one member like everything else.
     */
    private static ProcessorMetaSupplier onEveryMember(String src, int base, int count) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new PerMemberSource(src, base, count)));
    }

    private static final class PerMemberSource extends AbstractProcessor {
        private final String src;
        private final int base;
        private final int count;
        private int first;
        private int next;

        PerMemberSource(String src, int base, int count) {
            this.src = src;
            this.base = base;
            this.count = count;
        }

        @Override
        protected void init(Processor.Context context) {
            first = base + 10 * context.globalProcessorIndex();
        }

        @Override
        public boolean complete() {
            while (next < count) {
                int id = first + next;
                if (!tryEmit(Envelope.insert(id, src, Map.of("id", id), null))) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }
}
