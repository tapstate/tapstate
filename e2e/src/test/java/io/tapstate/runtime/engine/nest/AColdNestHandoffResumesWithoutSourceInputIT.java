package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.*;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.PipelineIntentScope;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static io.tapstate.runtime.engine.nest.NestFixtures.*;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;

/** The source has no restart-without-input verb, so this case drives the actual Jet edge in Java. */
class AColdNestHandoffResumesWithoutSourceInputIT {
    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc",
            nest("customer", List.of("customer_id"), tracking(embed("policy", "customer_id", "customer_id",
                    EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))))), tables());
    private static final NestVertex VERTEX = TOPOLOGY.vertexAt(List.of("policies"));
    private static final List<Object> KEY = List.of("P2");

    @Test void aNewMemberFindsTheSerializedHandoffWithNoBusinessEvent() throws Exception {
        var cold = new HeapKeyedStateStore();
        var original = manifest(1);
        HazelcastInstance first = member(cold, original);
        try {
            var stores = NestBinding.onMap(original).forStep("p", "doc").bind(first);
            var root = TOPOLOGY.assembler();
            var assembly = new AssemblerProcessor(root, TOPOLOGY.slots(), stores.forAssembler(root), "doc",
                    null, null, chain -> Optional.of(at(50)), NestSettings.defaults(), NestClock.SYSTEM);
            assembly.init(new TestOutbox(16), new TestProcessorContext());
            feed(assembly, 0, Envelope.insert(1, "customer", row("customer_id", "C1"), null).withOrder(at(1)));
            var out = new TestOutbox(1);
            var receiver = new ResolverProcessor(VERTEX, stores.forResolver(VERTEX), (from, child) -> {},
                    null, null, chain -> Optional.of(at(50)), NestClock.SYSTEM, NestSettings.defaults(), stores.forParking(VERTEX));
            receiver.init(out, new TestProcessorContext());
            var inbox = new TestInbox();
            inbox.queue().add(Envelope.update(2, "policy", row("policy_id", "P1", "customer_id", "C1", "policy_no", "PN1"),
                    row("policy_id", "P2", "customer_id", "C1", "policy_no", "PN1"), null).withOrder(at(2)));
            receiver.process(0, inbox);
            var parent = new ArrayList<Object>(); out.drainQueueAndReset(0, parent, false);
            int cascade = root.inbound().stream().filter(NestInbound::isCascade).findFirst().orElseThrow().ordinal();
            for (Object item : parent) feed(assembly, cascade, item);
            stores.forParking(VERTEX).save(new ParkedSubtree.At(VERTEX.pathId(), KEY), new ParkedSubtree(List.of(child("K1"), child("K2"))));
            receiver.tryProcess();
            assertThat(stores.forResolver(VERTEX).load(KEY).pending()).isEqualTo(2);
            assertThat(cold.pending(original.forStep("p", "doc").nest(VERTEX.pathId(), OperatorMapCoordinates.NestMap.STATE, null)
                    .placement().mapName(), null, 64)).hasSize(1);
        } finally { first.shutdown(); }

        var resumed = manifest(2);
        HazelcastInstance restarted = member(cold, resumed);
        try {
            var dag = new DAG();
            var inputs = new LinkedHashMap<String, List<Vertex>>();
            for (String alias : List.of("customer", "policy", "claim"))
                inputs.put(alias, List.of(dag.newVertex(alias, Processors.noopP()).localParallelism(1)));
            var binding = new NestBinding(tables(), NestBinding.onMap(resumed), (from, child) -> {},
                    member -> chain -> Optional.of(at(50))).forStep("p", "doc");
            var ordinals = new HashMap<Vertex, Integer>();
            var output = NestDag.attach(dag, TOPOLOGY, "doc", "customer", "doc", inputs::get, binding,
                    vertex -> ordinals.merge(vertex, 1, Integer::sum) - 1, null, new NodeWidth("doc", 2, 1, null));
            var sink = dag.newVertex("observed", SinkProcessors.writeListP("recovered"));
            dag.edge(Edge.between(output, sink));
            restarted.getJet().newJob(dag).getFuture().get(30, TimeUnit.SECONDS);
            assertThat(restarted.getList("recovered")).anySatisfy(item ->
                    assertThat(listAt(((Envelope)item).after(), "policies", "claims"))
                            .anySatisfy(claim -> assertThat(claim).containsEntry("claim_id", "K2")));
        } finally { restarted.shutdown(); }
    }

    private static NestElement child(String id) {
        return new NestElement(new ElementRef(List.of("policies", "claims"), KEY, List.of(id), null),
                row("claim_id", id, "policy_id", "P2"), at(1), Map.of());
    }

    private static void feed(Processor processor, int ordinal, Object item) {
        var inbox = new TestInbox(); inbox.queue().add(item); processor.process(ordinal, inbox);
    }

    private static OperatorRunManifest manifest(int attempt) {
        var scope = new PipelineIntentScope("cluster", 1, "cache", "p", 1);
        var bindings = new ArrayList<OperatorMapBinding>();
        for (var vertex : TOPOLOGY.vertices()) for (var kind : List.of(OperatorMapCoordinates.NestMap.STATE, OperatorMapCoordinates.NestMap.PARKING)) {
            var at = OperatorMapCoordinates.nest(scope, "cache", "doc", vertex.pathId(), kind, null);
            bindings.add(new OperatorMapBinding(at, "attempt-" + attempt, String.format("%064x", attempt)));
        }
        return new OperatorRunManifest("p", "attempt-" + attempt, bindings);
    }

    private static HazelcastInstance member(HeapKeyedStateStore cold, OperatorRunManifest original) {
        var config = new Config().setClusterName("cold-handoff-" + UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.operation.thread.count", "2");
        config.setProperty("hazelcast.operation.generic.thread.count", "2");
        config.setProperty("hazelcast.io.input.thread.count", "1");
        config.setProperty("hazelcast.io.output.thread.count", "1");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getSerializationConfig().addSerializerConfig(new com.hazelcast.config.SerializerConfig()
                .setTypeClass(Envelope.class).setImplementation(new EnvelopeSerializer()));
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var member = Hazelcast.newHazelcastInstance(config);
        OperatorMapBindings.bindTo(member, (binding, active) -> { active.run(); return cold; });
        OperatorMapConfigurations.install(member, original, original.bindings().stream()
                .map(binding -> new OperatorMapCapacity(binding.placement(), 512)).toList());
        return member;
    }
}
