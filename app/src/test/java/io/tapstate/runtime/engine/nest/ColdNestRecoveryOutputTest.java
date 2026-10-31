package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.app.RecoveredNestDocuments;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.EnvelopeSerializer;
import io.tapstate.runtime.engine.ReplayFloor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A recovery release can cross Inbox boundaries without changing the final keyed documents. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ColdNestRecoveryOutputTest {
    private static final NestTopology TOPOLOGY = topology();
    private HazelcastInstance member;

    @BeforeAll void startMember() {
        var config = new Config();
        config.setClusterName("cold-output-" + UUID.randomUUID());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", "2");
        var join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class).setImplementation(new EnvelopeSerializer()));
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll void stopMember() {
        if (member != null) member.shutdown();
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2})
    void allInboxGroupingsRecoverTheSameTwoCompleteDocuments(int splitPairs) throws Exception {
        var emissions = recoveredEmissions(splitPairs);
        assertThat(emissions).hasSize(2 + splitPairs);
        RecoveredNestDocuments.assertRecovered(member, materialize(emissions));
    }

    @Test void aMissingRecoveredChildStillFailsTheDocumentAssertion() throws Exception {
        var emissions = recoveredEmissions(1).stream()
                .filter(event -> !RecoveredNestDocuments.key(event).equals("C2") || childCount(event) == 0).toList();
        var target = materialize(emissions);
        assertThatThrownBy(() -> RecoveredNestDocuments.assertRecovered(member, target))
                .isInstanceOf(AssertionError.class).hasMessageContaining("child of C2");
    }

    @Test void aMissingRecoveredRootStillFailsTheDocumentAssertion() throws Exception {
        var emissions = recoveredEmissions(1).stream()
                .filter(event -> !RecoveredNestDocuments.key(event).equals("C2")).toList();
        var target = materialize(emissions);
        assertThatThrownBy(() -> RecoveredNestDocuments.assertRecovered(member, target))
                .isInstanceOf(AssertionError.class);
    }

    private String materialize(List<Envelope> emissions) throws Exception {
        String sourceName = "changelog-" + UUID.randomUUID();
        String targetName = "documents-" + UUID.randomUUID();
        member.<Envelope>getList(sourceName).addAll(emissions);
        var dag = new DAG();
        var source = dag.newVertex("recovered", SourceProcessors.readListP(sourceName)).localParallelism(1);
        var target = dag.newVertex("materialized", RecoveredNestDocuments.sink(targetName));
        dag.edge(Edge.between(source, target).partitioned((Envelope event) -> RecoveredNestDocuments.key(event)));
        member.getJet().newJob(dag).getFuture().toCompletableFuture().get(20, TimeUnit.SECONDS);
        member.getList(sourceName).destroy();
        return targetName;
    }

    private static List<Envelope> recoveredEmissions(int splitPairs) throws Exception {
        var roots = new Bytes<RootAssembly>();
        var states = new Bytes<ResolverState>();
        var vertex = TOPOLOGY.vertexAt(List.of("$root"));
        for (int index = 1; index <= 2; index++) {
            var parent = Map.<String, Object>of("policy_id", "P" + index,
                    "customer_id", "C" + index, "policy_no", "PN" + index);
            var root = new RootAssembly();
            root.applyRoot(Map.of("customer_id", "C" + index), new SourceOrder(1, SourceOrder.SNAPSHOT_SEQ));
            root.applyElement(new ElementRef(List.of("$root"), null, List.of("PN" + index), List.of("P" + index)),
                    parent, new SourceOrder(1, 2), Map.of());
            roots.save(List.of("C" + index), root);
            var out = new TestOutbox(1);
            var resolver = new ResolverProcessor(vertex, states, (from, child) -> { });
            resolver.init(out, new TestProcessorContext());
            feed(resolver, 0, Envelope.delete(1, "policy", parent, null).withOrder(new SourceOrder(1, 1)));
            out.drainQueueAndReset(0, new ArrayList<>(), false);
            feed(resolver, 1, Envelope.insert(3, "claim", Map.of("claim_id", "K" + index,
                    "policy_id", "P" + index, "value", 1), null).withOrder(new SourceOrder(1, 3))
                    .withSrcPos("original-child-" + index));
            feed(resolver, 0, Envelope.insert(2, "policy", parent, null).withOrder(new SourceOrder(1, 2)));
            var accepted = new ArrayList<Object>();
            out.drainQueueAndReset(0, accepted, false);
            assertThat(accepted).hasSize(1);
            var retained = states.load(List.of("P" + index));
            assertThat(retained.pending()).isEqualTo(1);
            assertThat(retained.releases().getFirst().deliveries()).hasSize(2);
        }
        var recoveredOut = new TestOutbox(16);
        var restarted = new ResolverProcessor(vertex, states, (from, child) -> { });
        restarted.init(recoveredOut, new TestProcessorContext());
        for (int index = 1; index <= 2; index++) {
            feed(restarted, vertex.inbound().size(), new NestReleaseRecovery.Wake(List.of("P" + index)));
        }
        var recovered = new ArrayList<Object>();
        recoveredOut.drainQueueAndReset(0, recovered, false);
        assertThat(recovered).hasSize(4);
        var parents = new LinkedHashMap<String, Object>();
        var children = new LinkedHashMap<String, Object>();
        for (Object value : recovered) {
            var item = (KeyedElement) value;
            String key = (String) ((List<?>) item.key()).getFirst();
            (item.element().ref().pathId().size() == 1 ? parents : children).put(key, item);
        }
        var out = new TestOutbox(16);
        var assembler = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), roots, "out", null, null,
                ReplayFloor.NONE, NestSettings.defaults(), () -> 0L, NestSendPolicy.within(50));
        assembler.init(out, new TestProcessorContext());
        int cascade = TOPOLOGY.assembler().inboundFor(List.of("$root")).ordinal();
        if (splitPairs == 0) {
            feed(assembler, cascade, parents.get("C1"), children.get("C1"), parents.get("C2"), children.get("C2"));
        } else if (splitPairs == 1) {
            feed(assembler, cascade, parents.get("C2"));
            feed(assembler, cascade, parents.get("C1"), children.get("C1"));
            feed(assembler, cascade, children.get("C2"));
        } else {
            feed(assembler, cascade, parents.get("C1"));
            feed(assembler, cascade, parents.get("C2"));
            feed(assembler, cascade, children.get("C1"));
            feed(assembler, cascade, children.get("C2"));
        }
        assertThat(assembler.complete()).isTrue();
        var emitted = new ArrayList<Object>();
        out.drainQueueAndReset(0, emitted, false);
        return emitted.stream().map(Envelope.class::cast).toList();
    }

    private static int childCount(Envelope event) {
        return ((List<?>) ((Map<?, ?>) ((List<?>) event.after().get("$root")).getFirst()).get("child")).size();
    }

    private static void feed(Processor processor, int ordinal, Object... items) throws Exception {
        var inbox = new TestInbox();
        inbox.queue().addAll(List.of(items));
        processor.process(ordinal, inbox);
        assertThat(inbox.isEmpty()).isTrue();
    }

    private static NestTopology topology() {
        var child = new Embed("claim", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY,
                "child", List.of("claim_id"), null, null, null);
        var parent = new Embed("policy", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY,
                "$root", List.of("policy_no"), null, null, List.of(child));
        var tree = new TransformBody.Nest(null, null,
                new NestRoot("customer", List.of("customer_id"), null, null, List.of(parent)));
        var tables = Map.of("customer", new NestTable("customers", List.of("customer_id")),
                "policy", new NestTable("policies", List.of("policy_id")),
                "claim", new NestTable("claims", List.of("claim_id")));
        return NestTopology.compile("flow", "step", tree, tables::get);
    }

    private static final class Bytes<T extends Serializable> implements NestStore<T> {
        private final Map<Object, byte[]> values = new LinkedHashMap<>();
        @SuppressWarnings("unchecked") public T load(Object key) {
            byte[] value = values.get(key);
            if (value == null) return null;
            try (var input = new ObjectInputStream(new ByteArrayInputStream(value))) {
                return (T) input.readObject();
            } catch (Exception failure) { throw new AssertionError(failure); }
        }
        public void save(Object key, T value) {
            try (var bytes = new ByteArrayOutputStream(); var output = new ObjectOutputStream(bytes)) {
                output.writeObject(value); output.flush(); values.put(key, bytes.toByteArray());
            } catch (Exception failure) { throw new AssertionError(failure); }
        }
        public void remove(Object key) { values.remove(key); }
        public long count() { return values.size(); }
    }
}
