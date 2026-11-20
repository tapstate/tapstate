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

    @Test
    void anAlternativeColumnMissingFromAFullEarlierRowIsReadAsNull() throws Exception {
        // A document store leaves a field out rather than holding null in it, so a transaction that
        // never had a hub id carries none in its earlier row either. That row is a full one - it holds
        // the transaction's other columns - so the missing field is the null it would be anywhere else.
        NestLookup byHub = TOPOLOGY.lookups().get(1);
        HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
        HeapNestStore<Set<Object>> references = new HeapNestStore<>();
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = new LookupProcessor(byHub, rows, references, 2L, null);
        processor.init(outbox, new TestProcessorContext());

        Map<String, Object> before = new LinkedHashMap<>();
        before.put("tran_id", 5L);
        before.put("locn_code", "A");
        before.put("status", "open");
        Map<String, Object> after = new LinkedHashMap<>(before);
        after.put("status", "closed");

        assertThatCode(() -> feed(processor, LookupProcessor.DEPARTED_REGISTRATIONS,
                Envelope.update(5L, "wallet_tran", before, after, null).withOrder(at(5))))
                .doesNotThrowAnyException();
    }

    @Test
    void anEarlierRowHoldingOnlyTheIdentityStillSaysNothingAboutWhereItPointed() throws Exception {
        // What a change stream sends with no pre-image configured: the key of the row and nothing else.
        // Reading the missing hub id as null there would leave an entry recorded under the real one.
        NestLookup byHub = TOPOLOGY.lookups().get(1);
        LookupProcessor processor = new LookupProcessor(byHub, new HeapNestStore<>(), new HeapNestStore<>(), 2L, null);
        processor.init(new TestOutbox(1024), new TestProcessorContext());

        Map<String, Object> after = new LinkedHashMap<>();
        after.put("tran_id", 6L);
        after.put("hub_locn_id", "H2");
        assertThatThrownBy(() -> feed(processor, LookupProcessor.DEPARTED_REGISTRATIONS,
                Envelope.update(6L, "wallet_tran", Map.of("tran_id", 6L), after, null).withOrder(at(6))))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(NestError.REFERENCE_TRACKING_REQUIRES_BEFORE_IMAGE));
    }

    @Test
    void aPlainReferenceMissingFromAFullEarlierRowIsTakenOutFromUnderNull() throws Exception {
        // The same document store, an embed with a single on: the transaction named no location, so it was
        // recorded under the null the missing field reads as, and pointing it somewhere takes it out of there.
        NestLookup byRef = NestTopology.compile("p", "doc", tree(Map.of("locn_id", "locn_ref"), null), TABLES::get)
                .lookups().getFirst();
        HeapNestStore<Set<Object>> references = new HeapNestStore<>();
        LookupProcessor processor = new LookupProcessor(byRef, new HeapNestStore<>(), references, 2L, null);
        processor.init(new TestOutbox(1024), new TestProcessorContext());

        Map<String, Object> before = new LinkedHashMap<>();
        before.put("tran_id", 7L);
        before.put("status", "open");
        Map<String, Object> after = new LinkedHashMap<>(before);
        after.put("locn_ref", 1L);
        feed(processor, LookupProcessor.REGISTRATIONS, Envelope.insert(7L, "wallet_tran", before, null).withOrder(at(7)));
        Object underNull = NestLookup.bucketKey(Arrays.asList((Object) null),
                NestLookup.bucketOf(List.of(7L)));
        assertThat(references.load(underNull)).as("control: recorded under null to begin with").isNotEmpty();

        assertThatCode(() -> feed(processor, LookupProcessor.DEPARTED_REGISTRATIONS,
                Envelope.update(8L, "wallet_tran", before, after, null).withOrder(at(8))))
                .doesNotThrowAnyException();
        assertThat(references.load(underNull)).as("and taken out once it points somewhere").isNullOrEmpty();
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
