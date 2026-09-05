package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Envelope;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins what a decoded row actually holds, as opposed to which field lands where (that is the golden's
 * job). A driver hands over whatever box its own client uses - an int column may arrive in any integral
 * box, a real one as a float - while the type namespace a column is resolved into names one width per
 * kind. This is the seam where the two are made to agree: the same boundary that says a column is
 * {@code INT64} also delivers a value that is one.
 *
 * <p>The conversions widen or leave alone and never round, so no value changes on the way in. Anything
 * the namespace does not name a lossless target for is left exactly as it came.
 *
 * <p>Every case here runs against a registry a connector has actually registered into, so the cells
 * below say what happens to an ordinary box <em>while</em> conversions are live - which is the whole
 * question, since a connector registers nothing for the ordinary boxes and they must stay bare.
 */
class TapEventValueModelTest {

    /** A driver's own type, standing in for the ones a real client hands back. */
    private record DriverKey(String hex) {
    }

    /** What a connector registers: its own types become portable values, the ordinary boxes are left. */
    private static final TapCodecsRegistry CODECS = new TapCodecsRegistry()
            .registerToTapValue(DriverKey.class, (value, tapType) ->
                    new TapStringValue(((DriverKey) value).hex()));

    @Test
    void anIntegerColumnArrivesAsTheSixtyFourBitIntegerItsTypeSaysItIs() {
        Envelope env = insert(row("qty", 5));

        assertThat(env.after().get("qty"))
                .as("the namespace has one integer width, so the row must speak it")
                .isEqualTo(5L)
                .isInstanceOf(Long.class);
    }

    @Test
    void theNarrowerIntegralBoxesWidenTheSameWay() {
        Envelope env = insert(row("small", (short) 7, "tiny", (byte) 3));

        assertThat(env.after().get("small")).isEqualTo(7L);
        assertThat(env.after().get("tiny")).isEqualTo(3L);
    }

    @Test
    void aFloatArrivesAsTheDoubleItsTypeSaysItIs() {
        Envelope env = insert(row("rate", 1.5f));

        assertThat(env.after().get("rate"))
                .as("a binary floating point column is one width in the namespace, as integers are")
                .isEqualTo(1.5d)
                .isInstanceOf(Double.class);
    }

    @Test
    void aBigIntegerInsideTheRangeArrivesAsTheSixtyFourBitInteger() {
        Envelope env = insert(row("big", BigInteger.valueOf(Long.MAX_VALUE)));

        assertThat(env.after().get("big")).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void aBigIntegerOutsideTheRangeIsLeftExactlyAsItCame() {
        BigInteger wider = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);

        Envelope env = insert(row("big", wider));

        // Narrowing it would hand every downstream reader a different number and report it as a
        // success. It stays what it is and is refused by name where it has to be.
        assertThat(env.after().get("big")).isEqualTo(wider);
    }

    @Test
    void aDecimalIsCarriedThroughUntouchedDownToItsScale() {
        BigDecimal amount = new BigDecimal("12345.6789");

        Envelope env = insert(row("amount", amount));

        // The one conversion that must never happen. Routing an exact fixed-point number through any
        // binary floating point type loses digits silently - green gate, green run, wrong money. The
        // assertion is equals rather than compareTo on purpose: compareTo ignores scale and would let
        // a rescaled value pass.
        assertThat(env.after().get("amount")).isEqualTo(amount).isInstanceOf(BigDecimal.class);
    }

    @Test
    void binaryStaysTheBytesItArrivedAs() {
        byte[] bytes = {1, 2, 3};

        Envelope env = insert(row("blob", bytes));

        // Wrapping bytes is an expression-language representation concern, not a value-model one: a
        // sink is owed the row's bytes.
        assertThat(env.after().get("blob")).isSameAs(bytes);
    }

    @Test
    void aValueTheNamespaceNamesNoWiderFormForIsLeftAlone() {
        Envelope env = insert(row("name", "eu", "flag", true));

        assertThat(env.after().get("name")).isEqualTo("eu");
        assertThat(env.after().get("flag")).isEqualTo(true);
    }

    @Test
    void aNestedDocumentAndAnArrayAreConvertedThroughToTheirLeaves() {
        Envelope env = insert(row(
                "doc", Map.of("qty", 5),
                "tags", List.of(1, 2)));

        // A document's own fields and an array's elements are as reachable from a reader as a
        // top-level column, so they hold the same currency.
        assertThat(env.after().get("doc")).isEqualTo(Map.of("qty", 5L));
        assertThat(env.after().get("tags")).isEqualTo(List.of(1L, 2L));
    }

    @Test
    void aContainerWithNothingToConvertIsNotCopied() {
        List<String> tags = List.of("eu", "us");

        Envelope env = insert(row("tags", tags));

        assertThat(env.after().get("tags"))
                .as("the ordinary row must not pay a copy per nested container")
                .isSameAs(tags);
    }

    @Test
    void theBeforeRowOfAnUpdateIsConvertedToo() {
        TapUpdateRecordEvent event = TapUpdateRecordEvent.create()
                .table("orders").referenceTime(1000L)
                .before(row("qty", 5)).after(row("qty", 6));

        Envelope env = TapEventCodec.decodeChange(event, CODECS);

        assertThat(env.before().get("qty")).isEqualTo(5L);
        assertThat(env.after().get("qty")).isEqualTo(6L);
    }

    @Test
    void aSnapshotRowIsConvertedTheSameWayAChangeIs() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders").referenceTime(1000L).after(row("qty", 5));

        Envelope env = TapEventCodec.decodeSnapshotRow(event, CODECS);

        // The phase says which op a row carries, never what its values are.
        assertThat(env.after().get("qty")).isEqualTo(5L);
    }

    // ---- the lane a connector's own types take ---------------------------------------------------

    @Test
    void aTypeTheConnectorRegisteredAConversionForArrivesAsThatConversion() {
        Envelope env = insert(row("_id", new DriverKey("64f0c0de")));

        // Before this, the driver's own object travelled untouched and every reader downstream met a
        // type only that driver's client knows - each one rendering it however its own serializer
        // happened to, which is how one row came to read two ways.
        assertThat(env.after().get("_id"))
                .isInstanceOf(TapValue.class)
                .extracting(value -> ((TapValue<?, ?>) value).getValue())
                .isEqualTo("64f0c0de");
    }

    @Test
    void theOriginalValueAndTheTypeItCameInAsTravelOnTheConvertedValue() {
        DriverKey key = new DriverKey("64f0c0de");

        Envelope env = insert(row("_id", key));

        // A sink of the same kind puts the value back the way it arrived by reading these two. Without
        // them the write side has only the text and writes a key as a string - silently, and only on
        // the target, where the read-side cases cannot see it.
        TapValue<?, ?> carried = (TapValue<?, ?>) env.after().get("_id");
        assertThat(carried.getOriginValue()).isSameAs(key);
        assertThat(carried.getOriginType()).isEqualTo("DriverKey");
    }

    @Test
    void anOrdinaryBoxIsLeftBareEvenWhileConversionsAreLive() {
        Envelope env = insert(row("qty", 5, "amount", new BigDecimal("1.50"), "name", "eu"));

        // The frozen surface registers nothing for these on purpose. Wrapping them anyway would pay a
        // carrier per value for a conversion that does not exist, and would make the row uniform by
        // spending exactly what the surface declines to spend.
        assertThat(env.after().get("qty")).isEqualTo(5L).isNotInstanceOf(TapValue.class);
        assertThat(env.after().get("amount")).isEqualTo(new BigDecimal("1.50")).isNotInstanceOf(TapValue.class);
        assertThat(env.after().get("name")).isEqualTo("eu").isNotInstanceOf(TapValue.class);
    }

    @Test
    void aRegisteredTypeIsConvertedInsideANestedDocumentAndInsideAnArray() {
        Envelope env = insert(row(
                "doc", Map.of("_id", new DriverKey("aa")),
                "keys", List.of(new DriverKey("bb"))));

        // A key one level down is as reachable from a reader as a top-level one, and a lane that
        // stopped at the top would leave the same two-shapes problem intact everywhere but there.
        assertThat(((Map<?, ?>) env.after().get("doc")).get("_id"))
                .isInstanceOf(TapValue.class)
                .extracting(value -> ((TapValue<?, ?>) value).getValue()).isEqualTo("aa");
        assertThat(((List<?>) env.after().get("keys")).get(0))
                .isInstanceOf(TapValue.class)
                .extracting(value -> ((TapValue<?, ?>) value).getValue()).isEqualTo("bb");
    }

    @Test
    void theSameRowThroughTwoRegistriesDecodesTwoWays() {
        Map<String, Object> after = row("_id", new DriverKey("64f0c0de"));

        Envelope registered = insert(after, CODECS);
        Envelope none = insert(after, new TapCodecsRegistry());

        // Which lane a value takes is the connector's answer and nothing else's. Were the registry
        // ignored - a shared one, a default one, one built on the spot - both sides would agree here
        // and every case above would still pass, since they all use the one registry.
        assertThat(registered.after().get("_id")).isInstanceOf(TapValue.class);
        assertThat(none.after().get("_id")).isInstanceOf(DriverKey.class);
    }

    @Test
    void aNestedContainerWithNothingToConvertIsHandedBackUncopied() {
        Map<String, Object> doc = Map.of("name", "eu");

        Envelope env = insert(row("doc", doc));

        // The row map itself is copied by the envelope, so this is where the no-copy property is
        // observable: a full row of already-converted values must cost nothing per container. Nothing
        // in the build measures allocation, so losing this would be silent until a large read.
        assertThat(env.after().get("doc")).isSameAs(doc);
    }

    private static Envelope insert(Map<String, Object> after) {
        return insert(after, CODECS);
    }

    private static Envelope insert(Map<String, Object> after, TapCodecsRegistry codecs) {
        return TapEventCodec.decodeChange(
                TapInsertRecordEvent.create().table("orders").referenceTime(1000L).after(after), codecs);
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            row.put((String) kv[i], kv[i + 1]);
        }
        return row;
    }
}
