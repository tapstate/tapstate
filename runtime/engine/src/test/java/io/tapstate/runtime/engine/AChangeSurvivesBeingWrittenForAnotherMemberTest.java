package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a change written for another member comes back as the change it was.
 *
 * <p>The case that watches a document read a row across members exercises this from end to end, and that
 * is the one that says the mechanism is wired up at all. What it cannot say is which <em>part</em> of a
 * change survives: a change that crosses carrying the right document proves nothing about the fields no
 * document on that path happens to use. A row image that is absent rather than empty, a position with no
 * token, a delete that carries only a before image - each of those is a shape the wire form has to have
 * an answer for, and each reads as correct data right up until the one path that needed it.
 *
 * <p><b>Absent and empty are asserted apart, deliberately.</b> They are one value to almost everything
 * that looks at a change and opposite answers to the thing that matters: an absent before image is a row
 * that was never there, an empty one is a row with no columns. Writing either as the other is a change
 * that survives the crossing and lies afterwards.
 */
class AChangeSurvivesBeingWrittenForAnotherMemberTest {

    private final InternalSerializationService serialization = new DefaultSerializationServiceBuilder()
            .setConfig(new Config().getSerializationConfig().addSerializerConfig(new SerializerConfig()
                    .setTypeClass(Envelope.class)
                    .setImplementation(new EnvelopeSerializer())))
            .build();

    private Envelope roundTrip(Envelope envelope) {
        return serialization.toObject(serialization.toData(envelope));
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put((String) fields[i], fields[i + 1]);
        }
        return row;
    }

    @Test
    @DisplayName("a change with all three row images comes back whole")
    void everyPartOfAChangeIsCarried() {
        Envelope written = Envelope
                .update(7L, "orders", row("id", 1, "name", "before"), row("id", 1, "name", "after"),
                        row("id", "int", "name", "string"))
                .withOrder(new SourceOrder(3, 42));

        Envelope read = roundTrip(written);

        assertThat(read.op()).isEqualTo(Op.UPDATE);
        assertThat(read.ts()).isEqualTo(7L);
        assertThat(read.src()).isEqualTo("orders");
        assertThat(read.before()).isEqualTo(row("id", 1, "name", "before"));
        assertThat(read.after()).isEqualTo(row("id", 1, "name", "after"));
        assertThat(read.schema()).isEqualTo(row("id", "int", "name", "string"));
        assertThat(read.positions())
                .describedAs("where a change sits on its chain is what a restart resumes from, so it "
                        + "travels with it rather than being worked out again on the other side")
                .isEqualTo(written.positions());
    }

    @Test
    @DisplayName("an absent row image comes back absent rather than empty")
    void whatWasNotThereIsStillNotThere() {
        Envelope written = Envelope.insert(1L, "orders", row("id", 1), null);

        Envelope read = roundTrip(written);

        assertThat(read.before())
                .describedAs("an insert has no before image. Coming back as an empty row would read as a "
                        + "row that existed and had no columns, which is a different fact and one nothing "
                        + "downstream would question")
                .isNull();
        assertThat(read.schema()).isNull();
        assertThat(read.after()).isEqualTo(row("id", 1));
    }

    @Test
    @DisplayName("an empty row image comes back empty rather than absent")
    void whatWasThereAndEmptyStaysThatWay() {
        Envelope written = Envelope.insert(1L, "orders", Map.of(), null);

        Envelope read = roundTrip(written);

        assertThat(read.after())
                .describedAs("the mirror of the case above, and the reason both are here: one value in "
                        + "the wire form for both is the shape that loses the distinction, and it loses "
                        + "it in whichever direction the person writing it happened to pick")
                .isNotNull()
                .isEmpty();
    }

    @Test
    @DisplayName("a delete carries the row that was there")
    void aDeleteKeepsItsBeforeImage() {
        Envelope read = roundTrip(Envelope.delete(9L, "orders", row("id", 4), null));

        assertThat(read.op()).isEqualTo(Op.DELETE);
        assertThat(read.before())
                .describedAs("a delete is the one shape whose meaning lives entirely in the image it "
                        + "carries; losing it turns the change into a statement about nothing")
                .isEqualTo(row("id", 4));
        assertThat(read.after()).isNull();
    }

    @Test
    @DisplayName("a position with no token comes back with none")
    void aPositionWithoutATokenIsCarriedAsSuch() {
        Envelope written = Envelope.insert(1L, "orders", row("id", 1), null)
                .withOrder(new SourceOrder(1, 5));

        Envelope read = roundTrip(written);

        assertThat(read.positions().values())
                .describedAs("most positions have no token at all, so this is the ordinary case rather "
                        + "than an edge one - and a wire form that cannot write an absent one fails on "
                        + "the first change rather than on a rare one")
                .allSatisfy(position -> assertThat(position.token()).isNull());
    }

    @Test
    @DisplayName("values that are not strings come back as themselves")
    void theOrdinaryScalarsSurvive() {
        Map<String, Object> mixed = row("i", 1, "l", 2L, "d", 3.5d, "b", true, "s", "text");

        assertThat(roundTrip(Envelope.insert(1L, "orders", mixed, null)).after())
                .describedAs("a row holds whatever the source had in it, and a wire form that quietly "
                        + "renders everything as text produces rows that compare equal to nothing")
                .isEqualTo(mixed);
    }

    @Test
    @DisplayName("a position carrying a token keeps it")
    void aTokenTravelsWhenThereIsOne() {
        Envelope written = new Envelope(Op.INSERT, 1L, "orders", null, row("id", 1), null,
                Map.of("orders", new ChainPosition(new SourceOrder(2, 9), "cursor-abc")));

        assertThat(roundTrip(written).positions())
                .isEqualTo(Map.of("orders", new ChainPosition(new SourceOrder(2, 9), "cursor-abc")));
    }

    @Test
    @DisplayName("a position with no order comes back with none")
    void aPositionWithoutAnOrderIsCarriedAsSuch() {
        Envelope written = new Envelope(Op.INSERT, 1L, "orders", null, row("id", 1), null,
                Map.of("orders", new ChainPosition(null, "cursor-abc")));

        assertThat(roundTrip(written).positions())
                .describedAs("either half of a position may be absent, so a change stamped with a token "
                        + "before its order was worked out is a shape the wire has to carry - and a form "
                        + "that reads the order off the object instead fails the job at the serializer, "
                        + "on the first such change to cross a member, with the row nowhere in the message")
                .isEqualTo(Map.of("orders", new ChainPosition(null, "cursor-abc")));
    }
}
