package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class UnwindConvertedContainerTest {
    private static final ConvertedValue PARENT = new ConvertedValue("parent", "objectId");

    private static UnwindPort port(boolean preserve, boolean byKey) {
        return new UnwindPort(UnwindSpec.from(new TransformBody.Unwind(
                "items", byKey ? null : "ordinal", preserve, byKey ? "sku" : null, null),
                List.of("id")), ignored -> { });
    }

    private static Map<String, Object> row(Object items) {
        return Map.of("id", PARENT, "items", items);
    }

    private static ConvertedValue array(Object... elements) {
        return new ConvertedValue(List.of(elements), "array");
    }

    private static ConvertedValue element(Object key) {
        return new ConvertedValue(Map.of("sku", key), "document");
    }

    @Test
    void aConvertedArrayExpandsWithoutStrippingElementOrParentCarriers() {
        ConvertedValue first = new ConvertedValue("a", "objectId");
        ConvertedValue second = new ConvertedValue("b", "objectId");
        List<Envelope> out = port(false, false).transform(new Envelope(Op.READ, 1L, "orders",
                null, row(array(first, second)), null));

        assertThat(out).hasSize(2);
        assertThat(out.get(0).after().get("items")).isSameAs(first);
        assertThat(out.get(1).after().get("items")).isSameAs(second);
        assertThat(out.get(0).after()).containsEntry("ordinal", 0L);
        assertThat(out.get(1).after()).containsEntry("ordinal", 1L);
        assertThat(out).allSatisfy(event -> assertThat(event.after().get("id")).isSameAs(PARENT));
    }

    @Test
    void anEmptyConvertedArrayObeysThePreservationOption() {
        Envelope event = new Envelope(Op.INSERT, 1L, "orders", null, row(array()), null);
        assertThat(port(false, false).transform(event)).isEmpty();
        assertThat(port(true, false).transform(event)).singleElement().satisfies(output -> {
            assertThat(output.after()).containsEntry("items", null).containsEntry("ordinal", null);
            assertThat(output.after().get("id")).isSameAs(PARENT);
        });
    }

    @Test
    void aConvertedMapSuppliesItsKeyAndKeepsItsCarrier() {
        ConvertedValue key = new ConvertedValue("a", "objectId");
        ConvertedValue element = element(key);
        List<Envelope> out = port(false, true).transform(new Envelope(Op.INSERT, 1L, "orders",
                null, row(element), null));

        assertThat(out).singleElement().satisfies(event -> {
            assertThat(event.after().get("items")).isSameAs(element);
            assertThat(event.after().get("sku")).isSameAs(key);
            assertThat(event.after().get("id")).isSameAs(PARENT);
        });
    }

    @Test
    void anUpdatePairsConvertedContainersWithPlainContainersByTheirValues() {
        ConvertedValue earlierA = element(new ConvertedValue("a", "objectId"));
        ConvertedValue earlierB = element(new ConvertedValue("b", "objectId"));
        List<Envelope> out = port(false, true).transform(new Envelope(Op.UPDATE, 1L, "orders",
                row(array(earlierA, earlierB)),
                row(List.of(Map.of("sku", "b"), Map.of("sku", "c"))), null));

        assertThat(out).extracting(Envelope::op).containsExactly(Op.DELETE, Op.UPDATE, Op.INSERT);
        assertThat(ConvertedValue.unwrap(out.get(0).before().get("sku"))).isEqualTo("a");
        assertThat(out.get(0).before().get("items")).isSameAs(earlierA);
        assertThat(out.get(1).before().get("items")).isSameAs(earlierB);
        assertThat(out.get(1).after()).containsEntry("sku", "b");
        assertThat(out.get(2).after()).containsEntry("sku", "c");
    }

    @Test
    void aDeleteExpandsTheConvertedEarlierArray() {
        ConvertedValue first = element("a");
        ConvertedValue second = element("b");
        List<Envelope> out = port(false, true).transform(new Envelope(Op.DELETE, 1L, "orders",
                row(array(first, second)), null, null));

        assertThat(out).extracting(Envelope::op).containsExactly(Op.DELETE, Op.DELETE);
        assertThat(out.get(0).before()).containsEntry("sku", "a");
        assertThat(out.get(1).before()).containsEntry("sku", "b");
        assertThat(out.get(0).before().get("items")).isSameAs(first);
        assertThat(out.get(1).before().get("items")).isSameAs(second);
    }
}
