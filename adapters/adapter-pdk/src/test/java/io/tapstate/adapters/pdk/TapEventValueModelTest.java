package io.tapstate.adapters.pdk;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.value.ByteData;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private record DriverKey(String hex) implements Serializable {
    }

    /** A driver type whose own object cannot be serialized - which several real ones are not. */
    private record DriverStamp(long seconds) {
    }

    /**
     * What a connector registers: its own types become portable values, the ordinary boxes are left,
     * and the pair is closed on the way out - a value that arrived as a key is written back as a key.
     * A real connector's from-conversion reads exactly these two, which is why it is mirrored here.
     */
    private static final TapCodecsRegistry CODECS = new TapCodecsRegistry()
            .registerToTapValue(DriverKey.class, (value, tapType) ->
                    new TapStringValue(((DriverKey) value).hex()))
            .registerToTapValue(DriverStamp.class, (value, tapType) ->
                    new TapStringValue(Long.toString(((DriverStamp) value).seconds())))
            .registerFromTapValue(TapStringValue.class, tapValue ->
                    tapValue.getOriginValue() instanceof DriverKey key ? key : tapValue.getValue());

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
                .isInstanceOf(ConvertedValue.class)
                .extracting(value -> ((ConvertedValue) value).value())
                .isEqualTo("64f0c0de");
    }

    @Test
    void theObjectTheValueWasConvertedFromTravelsOnTheConvertedValue() {
        DriverKey key = new DriverKey("64f0c0de");

        Envelope env = insert(row("_id", key));

        // A sink of the same kind puts the value back the way it arrived by reading this. Without it the
        // write side has only the text and writes a key as a string - silently, and only on the target,
        // where the read-side cases cannot see it.
        assertThat(((ConvertedValue) env.after().get("_id")).origin()).isSameAs(key);
    }

    @Test
    void aValueWhoseDriverObjectCannotTravelIsNotCarriedAtAll() {
        Envelope env = insert(row("at", new DriverStamp(1_700_000_000L)));

        // The carrier exists to get the driver's own object to the target, and a row crosses a wire to
        // get there. An object that cannot cross it would take the whole row down at the first hop - so
        // where there is nothing to carry, the portable value travels on its own and the write side
        // hands the target that, exactly as it does for a target of another kind.
        assertThat(env.after().get("at")).isEqualTo("1700000000").isNotInstanceOf(ConvertedValue.class);
    }

    @Test
    void anOrdinaryBoxIsLeftBareEvenWhileConversionsAreLive() {
        Envelope env = insert(row("qty", 5, "amount", new BigDecimal("1.50"), "name", "eu"));

        // The frozen surface registers nothing for these on purpose. Wrapping them anyway would pay a
        // carrier per value for a conversion that does not exist, and would make the row uniform by
        // spending exactly what the surface declines to spend.
        assertThat(env.after().get("qty")).isEqualTo(5L).isNotInstanceOf(ConvertedValue.class);
        assertThat(env.after().get("amount")).isEqualTo(new BigDecimal("1.50")).isNotInstanceOf(ConvertedValue.class);
        assertThat(env.after().get("name")).isEqualTo("eu").isNotInstanceOf(ConvertedValue.class);
    }

    @Test
    void aRegisteredTypeIsConvertedInsideANestedDocumentAndInsideAnArray() {
        Envelope env = insert(row(
                "doc", Map.of("_id", new DriverKey("aa")),
                "keys", List.of(new DriverKey("bb"))));

        // A key one level down is as reachable from a reader as a top-level one, and a lane that
        // stopped at the top would leave the same two-shapes problem intact everywhere but there.
        assertThat(((Map<?, ?>) env.after().get("doc")).get("_id"))
                .isInstanceOf(ConvertedValue.class)
                .extracting(value -> ((ConvertedValue) value).value()).isEqualTo("aa");
        assertThat(((List<?>) env.after().get("keys")).get(0))
                .isInstanceOf(ConvertedValue.class)
                .extracting(value -> ((ConvertedValue) value).value()).isEqualTo("bb");
    }

    @Test
    void theSameRowThroughTwoRegistriesDecodesTwoWays() {
        Map<String, Object> after = row("_id", new DriverKey("64f0c0de"));

        Envelope registered = insert(after, CODECS);
        Envelope none = insert(after, new TapCodecsRegistry());

        // Which lane a value takes is the connector's answer and nothing else's. Were the registry
        // ignored - a shared one, a default one, one built on the spot - both sides would agree here
        // and every case above would still pass, since they all use the one registry.
        assertThat(registered.after().get("_id")).isInstanceOf(ConvertedValue.class);
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

    @Test
    void aKeyIsWrittenBackAsTheKeyTheSourceHandedOver() {
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(row("_id", key, "qty", 5));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        // A target writes what it is given. Handed the text the key travelled as, it writes text - the
        // row lands, the write reports success, and only the target's own key column is the wrong type.
        assertThat(encoded.getAfter().get("_id")).isEqualTo(key);
        // The ordinary box took the bare lane in and must take it out: nothing converts it either way.
        assertThat(encoded.getAfter().get("qty")).isEqualTo(5L);
    }

    @Test
    void aKeyIsStillWrittenBackAsAKeyAfterTheRowHasCrossedAWire() {
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(row("_id", key));

        TapInsertRecordEvent encoded =
                (TapInsertRecordEvent) TapEventCodec.encode(overTheWire(decoded), CODECS);

        // Decode and encode sit on opposite sides of at least one serializer - a distributed edge, the
        // change-log store - so what the carrier holds has to survive one. Held as the connector's own
        // converted object it does not: that type's whole state is declared on a supertype that is not
        // serializable, so it arrives an empty shell and the write side restores nothing, while every
        // case that never crosses a wire stays green.
        assertThat(encoded.getAfter().get("_id")).isEqualTo(key);
    }

    @Test
    void aTargetThatDoesNotSpeakTheDriversTypeIsHandedTheValue() {
        Envelope decoded = insert(row("_id", new DriverKey("64f0c0de")));

        TapInsertRecordEvent encoded =
                (TapInsertRecordEvent) TapEventCodec.encode(decoded, new TapCodecsRegistry());

        // The target's own registry decides, as the source's did on the way in. A target that has never
        // heard of this driver type cannot be handed its object; it gets the portable value instead.
        assertThat(encoded.getAfter().get("_id")).isEqualTo("64f0c0de");
    }

    @Test
    void aCarriedValueNestedInADocumentIsRestoredToo() {
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(row("doc", new LinkedHashMap<>(Map.of("ref", key))));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        // A sub-document's fields are as reachable to a target as a top-level column is, and the way in
        // converted them. Restoring only the top level writes the carrier itself one level down.
        assertThat(encoded.getAfter().get("doc")).isEqualTo(Map.of("ref", key));
    }

    @Test
    void aDriverObjectFromAnotherConnectorsLoaderIsNotHandedToThisOnesConversion(@TempDir Path dir)
            throws ClassNotFoundException {
        // Two connectors, two isolated loaders, one class name. Conversions are looked up by name, so
        // the target's conversion for this name is found and would then cast the source's object to its
        // own copy of the class - a cast that cannot succeed, thrown inside the write, taking the whole
        // run down rather than one row. Measured on a real pair before this guard: a mongodb-to-mongodb
        // run died on its first row with a cast error naming org.bson.types.ObjectId twice.
        Path jar = SyntheticJar.compileToJar(dir, "synthetic.Key",
                "package synthetic; public class Key implements java.io.Serializable {"
                        + " public String toString() { return \"key-1\"; } }");
        try (ConnectorClassLoader readingConnector = ConnectorClassLoader.open(List.of(jar));
                ConnectorClassLoader writingConnector = ConnectorClassLoader.open(List.of(jar))) {
            Class<?> asTheSourceSeesIt = readingConnector.load("synthetic.Key");
            Class<?> asTheTargetSeesIt = writingConnector.load("synthetic.Key");
            assertThat(asTheTargetSeesIt)
                    .as("two connectors over one jar hold two unrelated classes of the same name")
                    .isNotSameAs(asTheSourceSeesIt);

            Object foreign = instanceOf(asTheSourceSeesIt);
            Envelope decoded = insert(row("_id", foreign), new TapCodecsRegistry()
                    .registerToTapValue(asTheSourceSeesIt, (value, tapType) ->
                            new TapStringValue(asTheSourceSeesIt.cast(value).toString())));
            assertThat(decoded.after().get("_id")).as("the row carries it").isInstanceOf(ConvertedValue.class);

            // The target's own conversion, written the way a real one is: it casts what it is handed to
            // the class its own loader defines. Registered under the same name, which is all the lookup
            // compares.
            TapCodecsRegistry target = new TapCodecsRegistry()
                    .registerToTapValue(asTheTargetSeesIt, (value, tapType) ->
                            new TapStringValue(asTheTargetSeesIt.cast(value).toString()))
                    .registerFromTapValue(TapStringValue.class, TapValue::getValue);

            TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, target);

            // Handed the portable value, which is the answer already written for a target that cannot
            // read this object. Not a restored identity - that is a separate thing this does not do -
            // but a row that lands rather than a run that stops.
            assertThat(encoded.getAfter().get("_id")).isEqualTo("key-1");
        }
    }

    private static Object instanceOf(Class<?> type) {
        try {
            return type.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not construct " + type, e);
        }
    }

    // ---- the seven driver types one real connector registers -------------------------------------

    /**
     * The conversions the MongoDB connector registers, mirrored value for value from its own source so
     * the cases below can be run without loading it. Seven driver types, and nothing else: an ordinary
     * integer, string or decimal is absent on purpose, which is why an ordinary column stays bare while
     * these seven do not.
     *
     * <p>Mirrored rather than loaded because the connector is a plugin that boots a runtime inside an
     * isolated loader, which no unit case can stand up. What is <em>not</em> mirrored is the part that
     * decides the lane: whether a driver type can cross a wire is read off the real class, so the split
     * below between a carried value and a bare one is the real one.
     *
     * @param omitted types to leave unregistered, so a case can ask what the absence of one costs
     */
    private static TapCodecsRegistry mongoCodecs(Set<Class<?>> omitted) {
        TapCodecsRegistry codecs = new TapCodecsRegistry();
        if (!omitted.contains(ObjectId.class)) {
            codecs.registerToTapValue(ObjectId.class, (value, tapType) ->
                    new TapStringValue(((ObjectId) value).toHexString()));
        }
        if (!omitted.contains(Binary.class)) {
            codecs.registerToTapValue(Binary.class, (value, tapType) -> {
                Binary binary = (Binary) value;
                return new TapBinaryValue(new ByteData(binary.getType(), binary.getData()));
            });
        }
        if (!omitted.contains(Code.class)) {
            codecs.registerToTapValue(Code.class, (value, tapType) ->
                    new TapStringValue(((Code) value).getCode()));
        }
        if (!omitted.contains(Decimal128.class)) {
            codecs.registerToTapValue(Decimal128.class, (value, tapType) ->
                    new TapNumberValue(((Decimal128) value).doubleValue()));
        }
        if (!omitted.contains(Symbol.class)) {
            codecs.registerToTapValue(Symbol.class, (value, tapType) ->
                    new TapStringValue(((Symbol) value).getSymbol()));
        }
        if (!omitted.contains(BsonTimestamp.class)) {
            codecs.registerToTapValue(BsonTimestamp.class, (value, tapType) -> new TapDateTimeValue(
                    new DateTime(Instant.ofEpochMilli(((BsonTimestamp) value).getTime())
                            .atZone(ZoneOffset.UTC))));
        }
        if (!omitted.contains(BsonRegularExpression.class)) {
            codecs.registerToTapValue(BsonRegularExpression.class, (value, tapType) -> {
                BsonRegularExpression regex = (BsonRegularExpression) value;
                return new TapStringValue("/" + regex.getPattern() + "/" + regex.getOptions());
            });
        }
        return codecs;
    }

    private static final TapCodecsRegistry MONGO = mongoCodecs(Set.of());

    private static Object decodedByMongo(Object value) {
        return insert(row("v", value), MONGO).after().get("v");
    }

    @Test
    void aKeyBecomesTheHexStringItIsKnownBy() {
        ObjectId id = new ObjectId("64f0c0de1234567890abcdef");

        assertThat(decodedByMongo(id))
                .isInstanceOf(ConvertedValue.class)
                .extracting(value -> ((ConvertedValue) value).value())
                .isEqualTo("64f0c0de1234567890abcdef");
    }

    @Test
    void aBinaryColumnBecomesTheContractsOwnBinaryBox() {
        Binary binary = new Binary((byte) 0, new byte[]{1, 2, 3});

        Object decoded = decodedByMongo(binary);

        // Not bytes: the connector's conversion answers with the contract's own box, and a row is
        // handed on whatever the connector answered. Which means unwrapping this one still does not
        // leave a value in this project's namespace - the box is the contract's.
        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        Object value = ((ConvertedValue) decoded).value();
        assertThat(value).isInstanceOf(ByteData.class);
        assertThat(((ByteData) value).getValue()).containsExactly(1, 2, 3);
    }

    @Test
    void aStoredJavascriptBecomesItsSourceText() {
        assertThat(decodedByMongo(new Code("function () {}")))
                .isInstanceOf(ConvertedValue.class)
                .extracting(value -> ((ConvertedValue) value).value())
                .isEqualTo("function () {}");
    }

    @Test
    void aSymbolBecomesItsText() {
        assertThat(decodedByMongo(new Symbol("eur")))
                .isInstanceOf(ConvertedValue.class)
                .extracting(value -> ((ConvertedValue) value).value())
                .isEqualTo("eur");
    }

    @Test
    void aRegularExpressionBecomesItsSlashDelimitedForm() {
        Object decoded = decodedByMongo(new BsonRegularExpression("^eu", "i"));

        // Nothing carries it: this driver type cannot cross a wire, so only the text travels and a
        // target of the same kind is handed the text rather than the expression.
        assertThat(decoded).isEqualTo("/^eu/i").isNotInstanceOf(ConvertedValue.class);
    }

    @Test
    void anExactDecimalLosesDigitsBecauseTheConnectorsConversionGoesThroughADouble() {
        Decimal128 exact = Decimal128.parse("1234567890.123456789012345678901234");

        Object decoded = decodedByMongo(exact);

        // Applying the connector's conversion is the decision, and this is what it costs on this
        // column: 34 significant digits through a double. Left alone the value is exact, so the loss
        // is this project's to own even though the conversion is not. Pinned rather than tolerated -
        // the day the upstream conversion is fixed, this case goes red and says so.
        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        Object value = ((ConvertedValue) decoded).value();
        assertThat(value).isInstanceOf(Double.class);
        assertThat(new java.math.BigDecimal(value.toString())).isNotEqualByComparingTo(exact.bigDecimalValue());
        // The exact number is still on the row - this type can cross a wire, so it is carried. What is
        // missing is a way back: the conversion pair the write side runs is keyed on the portable form,
        // and this connector registers no way back from a number, so a target of the same kind is
        // handed the double anyway. That half is not asserted here: reaching the fallback needs the
        // plugin runtime, which no unit case stands up, and it is witnessed by hand on a live pair.
        assertThat(((ConvertedValue) decoded).origin()).isSameAs(exact);
    }

    @Test
    void aTimestampReadsAsTheWrongInstantBecauseTheConnectorsConversionTakesSecondsForMillis() {
        // 2026-01-01T00:00:00Z, as a mongodb timestamp: seconds in the high half, a counter in the low.
        BsonTimestamp stamp = new BsonTimestamp(1_767_225_600, 7);

        Object decoded = decodedByMongo(stamp);

        // The same decision as the decimal above, with a worse shape: not a loss of precision but a
        // wrong value, off by a factor of a thousand, and the counter dropped entirely. Left alone the
        // value is right. Pinned for the same reason - a fix upstream reddens this and nothing else.
        assertThat(decoded).isInstanceOf(DateTime.class).isNotInstanceOf(ConvertedValue.class);
        assertThat(((DateTime) decoded).toInstant()).isEqualTo(Instant.ofEpochMilli(1_767_225_600L));
    }

    @Test
    void everyOneOfThoseSevenIsConvertedOnlyBecauseTheConnectorRegisteredIt() {
        Map<Class<?>, Object> values = new LinkedHashMap<>();
        values.put(ObjectId.class, new ObjectId("64f0c0de1234567890abcdef"));
        values.put(Binary.class, new Binary((byte) 0, new byte[]{1, 2, 3}));
        values.put(Code.class, new Code("function () {}"));
        values.put(Decimal128.class, Decimal128.parse("1.5"));
        values.put(Symbol.class, new Symbol("eur"));
        values.put(BsonTimestamp.class, new BsonTimestamp(1_767_225_600, 7));
        values.put(BsonRegularExpression.class, new BsonRegularExpression("^eu", "i"));

        values.forEach((type, value) -> {
            Object withIt = insert(row("v", value), MONGO).after().get("v");
            Object withoutIt = insert(row("v", value), mongoCodecs(Set.of(type))).after().get("v");

            // Dropping one registration must move that one column and no other. This is what separates
            // a case that watches the conversion from one that is green for its own reasons: a column
            // whose driver type nobody registered arrives as the driver's object, which is the shape
            // every one of these had before any conversion was applied at all.
            assertThat(withoutIt).as("%s, unregistered", type.getSimpleName()).isSameAs(value);
            assertThat(withIt).as("%s, registered", type.getSimpleName()).isNotSameAs(value);

            values.forEach((other, otherValue) -> {
                if (!other.equals(type)) {
                    assertThat(insert(row("v", otherValue), mongoCodecs(Set.of(type))).after().get("v"))
                            .as("%s, while %s is unregistered", other.getSimpleName(), type.getSimpleName())
                            .isNotSameAs(otherValue);
                }
            });
        });
    }

    /** One row through a serializer, the way an envelope reaches a sink from anywhere but the same step. */
    private static Envelope overTheWire(Envelope env) {
        Map<String, Object> row = new LinkedHashMap<>();
        env.after().forEach((name, value) -> row.put(name, roundTrip(value)));
        return Envelope.insert(env.ts(), env.src(), row, null);
    }

    private static Object roundTrip(Object value) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(value);
            }
            try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                return in.readObject();
            }
        } catch (Exception e) {
            throw new AssertionError("a row value must survive a serializer: " + value, e);
        }
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
