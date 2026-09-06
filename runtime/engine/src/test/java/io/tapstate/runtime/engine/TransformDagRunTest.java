package io.tapstate.runtime.engine;

import static com.hazelcast.jet.core.Edge.between;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs a minimal source to transform to sink DAG on an embedded single member: the transform vertex
 * is the generic adapter over a stateless port, proving the adapter is wired correctly and actually
 * transforms events inside a real Jet job. Also fixes the runtime-ring idiom for a loopback,
 * join-disabled, Jet-enabled test member.
 */
class TransformDagRunTest {

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void transform_vertex_applies_the_port_in_a_running_job() {
        SupplierEx<TransformPort> keepEvenIds = () -> event ->
                ((Integer) event.after().get("id")) % 2 == 0 ? List.of(event) : List.of();

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", insertsSource(4, "orders"));
        Vertex transform = dag.newVertex("transform", TransformProcessor.metaSupplier("transform", keepEvenIds));
        Vertex project = dag.newVertex("project",
                Processors.mapP((Envelope event) -> (Integer) event.after().get("id")));
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("out"));
        dag.edge(between(source, transform))
                .edge(between(transform, project))
                .edge(between(project, sink));

        member.getJet().newJob(dag).join();

        IList<Integer> out = member.getList("out");
        assertThat(out).containsExactlyInAnyOrder(2, 4);
    }

    /** A test source that builds insert envelopes on the member from a serializable count. */
    private static ProcessorMetaSupplier insertsSource(int count, String src) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new InsertsSource(count, src)));
    }

    private static final class InsertsSource extends AbstractProcessor {
        private final int count;
        private final String src;
        private int next = 1;

        InsertsSource(int count, String src) {
            this.count = count;
            this.src = src;
        }

        @Override
        public boolean complete() {
            while (next <= count) {
                if (!tryEmit(Envelope.insert(next, src, Map.of("id", next), null))) {
                    return false;
                }
                next++;
            }
            return true;
        }
    }
}
