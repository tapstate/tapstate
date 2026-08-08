package io.tapstate.core.event;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class EnvelopeTest {

    private static Map<String, Object> row(String key, Object value) {
        Map<String, Object> m = new HashMap<>();
        m.put(key, value);
        return m;
    }

    @Test
    void insertCarriesAfterAndNoBefore() {
        Envelope e = Envelope.insert(100L, "src.customers", row("id", 1), null);
        assertThat(e.op()).isEqualTo(Op.INSERT);
        assertThat(e.ts()).isEqualTo(100L);
        assertThat(e.src()).isEqualTo("src.customers");
        assertThat(e.after()).containsEntry("id", 1);
        assertThat(e.before()).isNull();
    }

    @Test
    void updateCarriesBothBeforeAndAfter() {
        Envelope e = Envelope.update(1L, "s", row("v", "old"), row("v", "new"), null);
        assertThat(e.op()).isEqualTo(Op.UPDATE);
        assertThat(e.before()).containsEntry("v", "old");
        assertThat(e.after()).containsEntry("v", "new");
    }

    @Test
    void deleteCarriesBeforeAndNoAfter() {
        Envelope e = Envelope.delete(1L, "s", row("id", 7), null);
        assertThat(e.op()).isEqualTo(Op.DELETE);
        assertThat(e.before()).containsEntry("id", 7);
        assertThat(e.after()).isNull();
    }

    @Test
    void readCarriesAfterAndNoBefore() {
        Envelope e = Envelope.read(1L, "s", row("id", 7), null);
        assertThat(e.op()).isEqualTo(Op.READ);
        assertThat(e.after()).containsEntry("id", 7);
        assertThat(e.before()).isNull();
    }

    @Test
    void ddlCarriesSchemaAndNeitherRow() {
        Envelope e = Envelope.ddl(1L, "s", row("added_column", "email"));
        assertThat(e.op()).isEqualTo(Op.DDL);
        assertThat(e.before()).isNull();
        assertThat(e.after()).isNull();
        assertThat(e.schema()).containsEntry("added_column", "email");
    }

    @Test
    void rejectsNullOp() {
        assertThatNullPointerException()
                .isThrownBy(() -> new Envelope(null, 1L, "s", null, null, null))
                .withMessageContaining("op");
    }

    @Test
    void rejectsNullSrc() {
        assertThatNullPointerException()
                .isThrownBy(() -> new Envelope(Op.INSERT, 1L, null, null, row("id", 1), null))
                .withMessageContaining("src");
    }

    @Test
    void dataMapsAreUnmodifiable() {
        Envelope e = Envelope.insert(1L, "s", row("id", 1), null);
        assertThatThrownBy(() -> e.after().put("x", 2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void defensivelyCopiesSoLaterMutationOfTheSourceMapDoesNotLeakIn() {
        Map<String, Object> after = row("id", 1);
        Envelope e = Envelope.insert(1L, "s", after, null);
        after.put("id", 999);
        assertThat(e.after()).containsEntry("id", 1);
    }

    @Test
    void valueEqualityByContent() {
        Envelope a = Envelope.insert(5L, "s", row("id", 1), null);
        Envelope b = Envelope.insert(5L, "s", row("id", 1), null);
        assertThat(a).isEqualTo(b);
        assertThat(a).hasSameHashCodeAs(b);
    }

    @Test
    void factoriesCoverNothing() {
        assertThat(Envelope.insert(1L, "s", row("id", 1), null).positions()).isEmpty();
        assertThat(Envelope.update(1L, "s", row("v", "old"), row("v", "new"), null).positions()).isEmpty();
        assertThat(Envelope.delete(1L, "s", row("id", 7), null).positions()).isEmpty();
        assertThat(Envelope.read(1L, "s", row("id", 7), null).positions()).isEmpty();
        assertThat(Envelope.ddl(1L, "s", row("added_column", "email")).positions()).isEmpty();
        assertThat(Envelope.insert(1L, "s", row("id", 1), null).position()).isNull();
    }

    @Test
    void carriesWhatItCoversAsTheSeventhComponent() {
        ChainPosition at = new ChainPosition(new SourceOrder(1L, 3L), "gtid:aaa:99");
        Envelope e = new Envelope(Op.INSERT, 1L, "s", null, row("id", 1), null, Map.of("s", at));

        assertThat(e.positions()).containsExactly(entry("s", at));
        assertThat(e.position()).isEqualTo(at);
    }

    @Test
    void coversEveryChainItWasAssembledFromAndSitsOnNoneOfThemItself() {
        // The shape the slot is a map for: a document built out of two streams, going out on a third. No
        // single position could stand for it - the higher would claim the other chain, the lower deny it.
        ChainPosition onOrders = new ChainPosition(new SourceOrder(1L, 8L), "w8");
        ChainPosition onLines = new ChainPosition(new SourceOrder(1L, 3L), "w3");
        Envelope document = new Envelope(Op.INSERT, 1L, "orders_with_lines", null, row("id", 1), null,
                Map.of("orders", onOrders, "lines", onLines));

        assertThat(document.positions()).containsOnly(entry("orders", onOrders), entry("lines", onLines));
        assertThat(document.position())
                .describedAs("the stream it goes out on is not one it sits on")
                .isNull();
    }

    @Test
    void withPositionSetsWhereItSitsOnItsOwnStreamAndKeepsEverythingElse() {
        ChainPosition at = new ChainPosition(new SourceOrder(1L, 5L), "p1");
        Envelope e = Envelope.insert(42L, "orders", row("id", 1), null);

        Envelope stamped = e.withPosition(at);

        assertThat(stamped.positions()).containsExactly(entry("orders", at));
        assertThat(stamped.op()).isEqualTo(Op.INSERT);
        assertThat(stamped.ts()).isEqualTo(42L);
        assertThat(stamped.src()).isEqualTo("orders");
        assertThat(stamped.after()).containsEntry("id", 1);
        assertThat(e.positions()).describedAs("the original is unchanged").isEmpty();
    }

    @Test
    void withPositionsReplacesEverythingItCovered() {
        ChainPosition was = new ChainPosition(new SourceOrder(1L, 1L), "p1");
        ChainPosition now = new ChainPosition(new SourceOrder(1L, 2L), "p2");
        Envelope e = Envelope.insert(1L, "s", row("id", 1), null).withPosition(was);

        assertThat(e.withPositions(Map.of("other", now)).positions())
                .describedAs("what it covers is stated in full, never added to")
                .containsExactly(entry("other", now));
    }

    @Test
    void withPositionAcceptsNullToCoverNothing() {
        Envelope e = Envelope.insert(1L, "s", row("id", 1), null)
                .withPosition(new ChainPosition(new SourceOrder(1L, 1L), "p1"));

        assertThat(e.withPosition(null).positions()).isEmpty();
    }

    @Test
    void stampingATokenKeepsAnOrderAlreadyStampedAndTheOtherWayRound() {
        // The two are one position, so stamping either component never drops the other - a bound reported
        // without its token cannot be persisted, and a token without an order cannot be compared.
        Envelope ordered = Envelope.insert(1L, "s", row("id", 1), null).withOrder(new SourceOrder(1L, 7L));
        Envelope tokened = Envelope.insert(1L, "s", row("id", 1), null).withSrcPos("p9");

        assertThat(ordered.withSrcPos("p9").position())
                .isEqualTo(new ChainPosition(new SourceOrder(1L, 7L), "p9"));
        assertThat(tokened.withOrder(new SourceOrder(1L, 7L)).position())
                .isEqualTo(new ChainPosition(new SourceOrder(1L, 7L), "p9"));
    }

    @Test
    void whatItCoversParticipatesInValueEquality() {
        Envelope a = Envelope.insert(5L, "s", row("id", 1), null).withOrder(new SourceOrder(1L, 1L));
        Envelope b = Envelope.insert(5L, "s", row("id", 1), null).withOrder(new SourceOrder(1L, 1L));
        Envelope laterOrder = Envelope.insert(5L, "s", row("id", 1), null).withOrder(new SourceOrder(1L, 2L));
        Envelope otherToken = a.withSrcPos("p2");

        assertThat(a).isEqualTo(b);
        assertThat(a).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(laterOrder);
        assertThat(a).isNotEqualTo(otherToken);
    }

    @Test
    void whatItCoversCannotBeMutatedThroughTheMapItWasBuiltFrom() {
        Map<String, ChainPosition> handedIn = new LinkedHashMap<>();
        handedIn.put("s", new ChainPosition(new SourceOrder(1L, 1L), "p1"));
        Envelope e = Envelope.insert(1L, "s", row("id", 1), null).withPositions(handedIn);

        handedIn.put("sneaked", new ChainPosition(new SourceOrder(1L, 9L), "p9"));

        assertThat(e.positions()).containsOnlyKeys("s");
        assertThatThrownBy(() -> e.positions().put("s", null))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
