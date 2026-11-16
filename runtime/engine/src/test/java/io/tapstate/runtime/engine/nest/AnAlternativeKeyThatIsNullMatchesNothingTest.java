package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * How an embed's {@code orOn} alternatives compile, and how the lookup made for one treats a null key.
 *
 * <p>An alternative keys rows by an ordinary column, so most rows of a table can hold null there - every
 * location that has no hub id, every transaction that came from the system using codes. A null matches
 * nothing. Filed and recorded like any other value, it would make all those rows one key: each edit to one
 * such location would wake every transaction lacking a hub id, and enough of them would fail the job on
 * the ceiling of how many rows may point at one.
 */
class AnAlternativeKeyThatIsNullMatchesNothingTest {

    private static final Map<String, NestTable> TABLES = Map.of(
            "tran", new NestTable("wallet_tran", List.of("tran_id")),
            "loc", new NestTable("location", List.of("locn_id")));

    private static TransformBody.Nest tree(Map<String, String> on, List<Map<String, String>> orOn) {
        Embed location = new Embed("loc", on, EmbedAs.OBJECT, "location", null, null, null, null, null, orOn);
        return new TransformBody.Nest(null, null,
                new NestRoot("tran", List.of("tran_id"), null, null, List.of(location)));
    }

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc",
            tree(Map.of("locn_code", "locn_code"), List.of(Map.of("hub_locn_id", "hub_locn_id"))), TABLES::get);

    @Test
    void eachAlternativeCompilesToALookupOfItsOwnAndOneSlotReadsThemInOrder() {
        assertThat(TOPOLOGY.lookups())
                .extracting(NestLookup::partitionKey)
                .containsExactly(List.of("locn_code"), List.of("hub_locn_id"));
        assertThat(TOPOLOGY.lookups()).allMatch(NestLookup::alternative);
        assertThat(TOPOLOGY.lookups()).extracting(NestLookup::mapName).doesNotHaveDuplicates();
        assertThat(TOPOLOGY.lookups().get(0).rowIdentity()).containsExactly("locn_id");

        EmbedSlot slot = TOPOLOGY.slots().getFirst();
        assertThat(slot.references())
                .extracting(EmbedSlot.Reference::lookupMap)
                .containsExactly(TOPOLOGY.lookups().get(0).mapName(), TOPOLOGY.lookups().get(1).mapName());
        assertThat(TOPOLOGY.assembler().inbound())
                .filteredOn(NestInbound::carriesTouches)
                .as("each alternative tells the document about its own edits on an edge of its own")
                .hasSize(2);
    }

    @Test
    void anEmbedWithoutAlternativesCompilesExactlyAsBefore() {
        NestTopology plain = NestTopology.compile("p", "doc", tree(Map.of("locn_id", "locn_ref"), null),
                Map.of("tran", new NestTable("wallet_tran", List.of("tran_id")),
                        "loc", new NestTable("location", List.of("locn_id")))::get);
        assertThat(plain.lookups()).hasSize(1);
        assertThat(plain.lookups().getFirst().alternative()).isFalse();
        assertThat(plain.lookups().getFirst().mapName()).isEqualTo("nest.p.doc.location");
        assertThat(plain.slots().getFirst().hasAlternatives()).isFalse();
    }

    @Test
    void anAlternativeOnTheParentsOwnKeyIsRefused() {
        assertThatThrownBy(() -> NestTopology.compile("p", "doc",
                tree(Map.of("locn_code", "locn_code"), List.of(Map.of("tran_ref", "tran_id"))), TABLES::get))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(NestError.OR_ON_NOT_A_REFERENCE));
    }

    @Test
    void aRowWithANullAlternativeKeyIsNeitherFiledNorWakesAnything() throws Exception {
        NestLookup byHub = TOPOLOGY.lookups().get(1);
        HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
        HeapNestStore<Set<Object>> references = new HeapNestStore<>();
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = new LookupProcessor(byHub, rows, references, 2L, null);
        processor.init(outbox, new TestProcessorContext());

        feed(processor, LookupProcessor.REGISTRATIONS,
                tran(1, null), tran(2, null), tran(3, null), tran(4, "H2"));

        assertThatCode(() -> feed(processor, LookupProcessor.ROWS,
                location(1, null, "Atrium"), location(2, "H2", "Harbor"), location(1, null, "Atrium II")))
                .as("three transactions lack a hub id and the limit is two; they point at nothing, so none counts")
                .doesNotThrowAnyException();
        assertThat(rows.load(Arrays.asList((Object) null)))
                .as("nothing is filed under a key of nulls")
                .isNull();
        assertThat(woken(outbox))
                .as("only the transaction naming H2 is woken, and only by the location holding H2")
                .containsExactly(List.of(4L));
    }

    private static Envelope tran(long id, String hub) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("tran_id", id);
        fields.put("hub_locn_id", hub);
        return Envelope.insert(id, "wallet_tran", fields, null).withOrder(at(id));
    }

    private static Envelope location(long id, String hub, String name) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("locn_id", id);
        fields.put("hub_locn_id", hub);
        fields.put("name", name);
        return Envelope.insert(id, "location", fields, null).withOrder(at(100 + id));
    }

    private static void feed(LookupProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static List<Object> woken(TestOutbox outbox) {
        List<Object> sent = new ArrayList<>();
        outbox.drainQueueAndReset(0, sent, false);
        List<Object> keys = new ArrayList<>();
        sent.forEach(item -> {
            if (item instanceof NestTouch touch) {
                keys.add(touch.key());
            }
        });
        return keys;
    }
}
