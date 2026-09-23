package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FlatAssemblerProcessorTest {

    private static final TransformBody.Nest TREE = new TransformBody.Nest(null, null,
            new NestRoot("customer", List.of("customer_id"), null, null, List.of(new Embed(
                    "profile",
                    Map.of("owner_id", "customer_id"),
                    EmbedAs.FLAT,
                    null,
                    List.of("profile_id"),
                    null,
                    null,
                    null,
                    null))));
    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final int CUSTOMER = ordinal("customer");
    private static final int PROFILE = ordinal("profile");

    private final HeapNestStore<RootAssembly> store = new HeapNestStore<>();
    private final AssemblerProcessor processor =
            new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), store, "doc");
    private final TestOutbox outbox = new TestOutbox(64);

    @BeforeEach
    void init() throws Exception {
        processor.init(outbox, new TestProcessorContext());
    }

    @Test
    void snapshotThenCdcUpdateAndDeleteKeepOneFlatDocumentCurrent() {
        feed(CUSTOMER, Envelope.read(1, "customer", row("customer_id", "C1", "name", "Ada"), null)
                .withOrder(at(1)));

        List<Envelope> snapshot = feed(PROFILE,
                Envelope.read(2, "profile", row("profile_id", "P1", "owner_id", "C1", "tier", "gold"), null)
                        .withOrder(at(2)));
        assertThat(snapshot.getLast().after()).containsEntry("tier", "gold");

        List<Envelope> update = feed(PROFILE,
                Envelope.update(3, "profile",
                                row("profile_id", "P1", "owner_id", "C1", "tier", "gold"),
                                row("profile_id", "P1", "owner_id", "C1", "tier", "platinum"), null)
                        .withOrder(at(3)));
        assertThat(update.getLast().after()).containsEntry("tier", "platinum");

        List<Envelope> deletion = feed(PROFILE,
                Envelope.delete(4, "profile",
                                row("profile_id", "P1", "owner_id", "C1", "tier", "platinum"), null)
                        .withOrder(at(4)));
        assertThat(deletion.getLast().after()).containsEntry("customer_id", "C1")
                .doesNotContainKey("tier");
        assertThat(deletion.getLast().removed()).contains("tier");
    }

    @Test
    void aOneToManyFlatRelationStopsTheProcessorWithTheCardinalityCode() throws Exception {
        Embed events = new Embed(
                "event",
                Map.of("owner_id", "customer_id"),
                EmbedAs.FLAT,
                null,
                List.of("event_id"),
                null,
                null,
                null,
                null);
        TransformBody.Nest tree = new TransformBody.Nest(null, null,
                new NestRoot("customer", List.of("customer_id"), null, null, List.of(events)));
        Map<String, NestTable> models = Map.of(
                "customer", new NestTable("customers", List.of("customer_id")),
                "event", new NestTable("events", List.of("event_id")));
        NestTopology topology = NestTopology.compile("p", "doc", tree, models::get);
        AssemblerProcessor local = new AssemblerProcessor(
                topology.assembler(), topology.slots(), new HeapNestStore<>(), "doc");
        TestOutbox localOutbox = new TestOutbox(64);
        local.init(localOutbox, new TestProcessorContext());
        int customer = topology.assembler().inbound().stream()
                .filter(edge -> "customer".equals(edge.alias()))
                .findFirst().orElseThrow().ordinal();
        int event = topology.assembler().inbound().stream()
                .filter(edge -> "event".equals(edge.alias()))
                .findFirst().orElseThrow().ordinal();
        feed(local, localOutbox, customer,
                Envelope.read(1, "customer", row("customer_id", "C1"), null).withOrder(at(1)));

        assertThatThrownBy(() -> feed(local, localOutbox, event,
                Envelope.read(2, "event", row("event_id", "E1", "owner_id", "C1", "kind_a", true), null)
                        .withOrder(at(2)),
                Envelope.read(3, "event", row("event_id", "E2", "owner_id", "C1", "kind_b", true), null)
                        .withOrder(at(3))))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(NestError.FLAT_CARDINALITY_VIOLATION);
                    assertThat(error.args()).containsEntry("rows", 2);
                });
    }

    private List<Envelope> feed(int ordinal, Envelope... events) {
        return feed(processor, outbox, ordinal, events);
    }

    private static List<Envelope> feed(
            AssemblerProcessor target, TestOutbox targetOutbox, int ordinal, Envelope... events) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(events));
        target.process(ordinal, inbox);
        List<Object> drained = new ArrayList<>();
        targetOutbox.drainQueueAndReset(0, drained, false);
        return drained.stream().map(Envelope.class::cast).toList();
    }

    private static int ordinal(String alias) {
        return TOPOLOGY.assembler().inbound().stream()
                .filter(edge -> alias.equals(edge.alias()) && !edge.carriesDepartures())
                .findFirst()
                .orElseThrow()
                .ordinal();
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1, seq);
    }
}
