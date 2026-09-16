package io.tapstate.adapters.pdk;

import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
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
import java.lang.reflect.InvocationTargetException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /** What a schema calls the column those keys live in - the one thing the way back has to go on. */
    private static final String KEY_COLUMN = "DRIVER_KEY";

    /**
     * What a connector registers: its own types become portable values, the ordinary boxes are left,
     * and the pair is closed on the way out - a value that arrived as a key is written back as a key.
     * The way back reads the column's declared name and nothing else, which is what a real connector's
     * does and the only thing that can work: the object a value was converted from belongs to the
     * source's class loader, and the target runs in one of its own.
     */
    private static final TapCodecsRegistry CODECS = new TapCodecsRegistry()
            .registerToTapValue(DriverKey.class, (value, tapType) ->
                    new TapStringValue(((DriverKey) value).hex()))
            .registerToTapValue(DriverStamp.class, (value, tapType) ->
                    new TapStringValue(Long.toString(((DriverStamp) value).seconds())))
            .registerFromTapValue(TapStringValue.class, tapValue ->
                    KEY_COLUMN.equals(tapValue.getOriginType())
                            ? new DriverKey(tapValue.getValue())
                            : tapValue.getValue());

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

        Envelope env = TapEventCodec.decodeChange(event, CODECS, Map.of());

        assertThat(env.before().get("qty")).isEqualTo(5L);
        assertThat(env.after().get("qty")).isEqualTo(6L);
    }

    @Test
    void aSnapshotRowIsConvertedTheSameWayAChangeIs() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders").referenceTime(1000L).after(row("qty", 5));

        Envelope env = TapEventCodec.decodeSnapshotRow(event, CODECS, Map.of());

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
    void theNameTheSchemaGaveTheColumnTravelsOnTheConvertedValue() {
        Envelope env = insert(row("_id", new DriverKey("64f0c0de")), CODECS, Map.of("_id", KEY_COLUMN));

        // A sink of the same kind rebuilds the driver's type by reading this. Without it the write side
        // has only the text and writes a key as a string - silently, and only on the target, where the
        // read-side cases cannot see it.
        assertThat(((ConvertedValue) env.after().get("_id")).originType()).isEqualTo(KEY_COLUMN);
    }

    @Test
    void nothingTheDriverOwnsTravelsOnTheRow() {
        Envelope env = insert(row("at", new DriverStamp(1_700_000_000L)), CODECS, Map.of("at", "STAMP"));

        // A row leaves this module for rings that cannot name a driver type, crosses at least one
        // serializer, and is finally handed to a second connector in a class loader of its own. A
        // driver object in the row fails at all three, the last one as a cast error naming one class
        // twice. What travels is the portable value and the column's name - this type is not
        // serializable, and it makes no difference, because nothing driver-owned is carried anyway.
        Object carried = env.after().get("at");
        assertThat(carried).isInstanceOf(ConvertedValue.class);
        assertThat(((ConvertedValue) carried).value()).isEqualTo("1700000000");
        assertThat(((ConvertedValue) carried).originType()).isEqualTo("STAMP");
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
        Envelope decoded = insert(row("_id", key, "qty", 5), CODECS, Map.of("_id", KEY_COLUMN));

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
        Envelope decoded = insert(row("_id", key), CODECS, Map.of("_id", KEY_COLUMN));

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
    void aWayBackThatAnswersNothingLeavesTheValueRatherThanBlankingTheColumn() {
        Envelope decoded = insert(row("_id", new DriverKey("64f0c0de")));

        // Registered, so the lookup finds one - and it answers null, which is what a real one does for a
        // declared name it does not recognise: the pair source and target make is only closed when both
        // are the same kind. A carrier cannot hold null, the constructor refuses it, so a null here is
        // never the column having been null.
        TapCodecsRegistry answersNothing = new TapCodecsRegistry()
                .registerFromTapValue(TapStringValue.class, tapValue -> null);

        TapInsertRecordEvent encoded =
                (TapInsertRecordEvent) TapEventCodec.encode(decoded, answersNothing);

        // The portable value, the same answer as for a target that registered no way back at all. Taken
        // at its word instead, the row would land with the key blanked and the write would report success.
        assertThat(encoded.getAfter().get("_id")).isEqualTo("64f0c0de");
    }

    @Test
    void aCarriedValueInsideADocumentIsRestoredWhenTheSchemaNamesItsPath() {
        // Discovery names a field inside a document by its dotted path, in the same field map the
        // top-level columns come from - measured against a real connector: a document holding a key
        // reports `meta.ref` beside `meta` itself. So the interior does have a declared name, and the
        // way back has something to key on, as long as the lookup is by path rather than by column.
        Envelope decoded = insert(row("doc", new LinkedHashMap<>(Map.of("ref", new DriverKey("64f0c0de")))),
                CODECS, Map.of("doc.ref", KEY_COLUMN));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        // Restored as a key, the same answer the identical value gets as a top-level column. The
        // discriminating half is the type: the portable text and the restored key print alike, and
        // only one of them is what a target of this kind stores.
        assertThat(encoded.getAfter().get("doc")).isEqualTo(Map.of("ref", new DriverKey("64f0c0de")));
    }

    @Test
    void aFieldInsideADocumentIsNeverRebuiltAsWhateverTheDocumentItselfIsDeclaredToBe() {
        // The column is named, its interior is not - which is what discovery reports for a document
        // nobody sampled to that depth - and the column is declared the very type its interior holds,
        // so the document's own declared name is the only name in the row. Lending it down would
        // rebuild every field of the document as whatever the document is declared to be and report
        // success, which is worse than the portable value. Nothing else here names this driver type,
        // so the interior has no answer of its own and stays portable.
        Envelope decoded = insert(row("doc", new LinkedHashMap<>(Map.of("ref", new DriverKey("64f0c0de")))),
                CODECS, Map.of("doc", KEY_COLUMN));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("doc")).isEqualTo(Map.of("ref", "64f0c0de"));
    }

    @Test
    void aFieldTheSchemaNeverDescribedIsRestoredLikeAnArrayElementBesideIt() {
        // One document, one driver type, three places: a column the schema names, a field inside a
        // document it names nothing beneath, and an array element it has no way to name. Read beneath
        // arrays only, this row decodes two ways - the element restored, the field beside it holding
        // that very value arriving as the text it travelled as, with the better-described place
        // getting the worse answer. A field map naming `meta` and nothing under it is the fields
        // discovery met in the documents it sampled, not a statement that `meta.ref` has no type, so
        // that absence is read the same way the element's is.
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(
                row("_id", key, "meta", new LinkedHashMap<>(Map.of("ref", key)), "refs", List.of(key)),
                CODECS,
                Map.of("_id", KEY_COLUMN, "meta", "DOCUMENT", "refs", "ARRAY"));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("meta"))
                .as("the field whose place the schema never described")
                .isEqualTo(Map.of("ref", key));
        assertThat(encoded.getAfter().get("refs"))
                .as("the element beside it, which gets the same answer")
                .isEqualTo(List.of(key));
    }

    @Test
    void aColumnTheSchemaNeverDescribedIsRestoredTheSameWay() {
        // The same absence one level up. A field that first appeared after discovery sampled the
        // collection has no row in the field map either, and a schemaless source produces those
        // routinely; the declared name that does reach this driver type is read for it too, so the
        // column lands as the driver's own type rather than as its text.
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(row("_id", key, "parent", key), CODECS, Map.of("_id", KEY_COLUMN));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("parent")).isEqualTo(key);
    }

    @Test
    void anArrayElementIsNeverRebuiltAsWhateverTheArrayItselfIsDeclaredToBe() {
        // The array column is declared the very type its element is, and the row holds that type
        // nowhere else - so the array's own declared name is the only name on offer. Lending it to the
        // element would rebuild every element as whatever the array is declared to be and report
        // success, which is a worse answer than the portable value, so the element stays portable.
        Envelope decoded = insert(row("refs", List.of(new DriverKey("64f0c0de"))),
                CODECS, Map.of("refs", KEY_COLUMN));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("refs")).isEqualTo(List.of("64f0c0de"));
    }

    @Test
    void aCarriedValueInsideAnArrayIsRestoredLikeTheSameValueInsideADocument() {
        // The measured document, both halves in one row and one run: the same driver value inside a
        // document and inside an array, under the schema a real source reports for it - a dotted path
        // for the field inside the document, the array named as an array, and nothing named beneath it.
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(
                row("meta", new LinkedHashMap<>(Map.of("ref", key)), "arr", List.of(key)),
                CODECS,
                Map.of("meta.ref", KEY_COLUMN, "meta", "DOCUMENT", "arr", "ARRAY"));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("meta"))
                .as("the half the schema names, which the way back already restores")
                .isEqualTo(Map.of("ref", key));
        // The array half is what a target of the same kind stored wrongly before this reading existed:
        // the element arrived as the text it travelled as, the row landed, and the write reported
        // success. The array's own declared name cannot close that - it is declared an array here, and
        // rebuilding an element as whatever the array is declared to be would be a different defect
        // that also reported success. What restores it is the name this same schema gives that driver
        // type at the one place it does name one: the dotted path asserted above.
        assertThat(encoded.getAfter().get("arr"))
                .as("the same value inside an array, restored from what the schema calls that type")
                .isEqualTo(List.of(key));
    }

    @Test
    void anArrayElementIsRestoredWhicheverOrderTheConnectorReportedTheRowIn() {
        // The same row with the array reported first. What the schema calls this driver type is read
        // off the whole row before any value is converted, so the answer cannot depend on field order -
        // read as the walk went, this row would restore nothing and the one above would restore, and a
        // document would decode two ways for no reason a reader could see.
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(
                row("arr", List.of(key), "meta", new LinkedHashMap<>(Map.of("ref", key))),
                CODECS,
                Map.of("meta.ref", KEY_COLUMN, "meta", "DOCUMENT", "arr", "ARRAY"));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("arr")).isEqualTo(List.of(key));
    }

    @Test
    void aCarriedValueInsideADocumentInsideAnArrayIsRestoredTheSameWay() {
        // Below an element there is no place the schema could name either, so the same reading answers
        // all the way down rather than stopping at the element itself.
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(
                row("meta", new LinkedHashMap<>(Map.of("ref", key)),
                        "arr", List.of(new LinkedHashMap<>(Map.of("ref", key)))),
                CODECS,
                Map.of("meta.ref", KEY_COLUMN, "meta", "DOCUMENT", "arr", "ARRAY"));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("arr")).isEqualTo(List.of(Map.of("ref", key)));
    }

    @Test
    void bothImagesOfOneUpdateRestoreItsArrayTheSameWay() {
        // A before image the connector reported without the column that names this driver type, beside
        // an after image that has it - which a connector is free to do, and several do. Read per image,
        // the half that carries the name would restore and the half that does not would not, so one
        // change would say an array changed when nothing in it did.
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = TapEventCodec.decodeChange(
                TapUpdateRecordEvent.create().table("orders").referenceTime(1000L)
                        .before(row("arr", List.of(key)))
                        .after(row("_id", key, "arr", List.of(key))),
                CODECS,
                Map.of("_id", KEY_COLUMN, "arr", "ARRAY"));

        TapUpdateRecordEvent encoded = (TapUpdateRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getBefore().get("arr"))
                .as("the array on the side the naming column is missing from")
                .isEqualTo(encoded.getAfter().get("arr"))
                .isEqualTo(List.of(key));
    }

    @Test
    void aDriverTypeTheSchemaSpellsTwoWaysLeavesItsArrayElementsAlone() {
        // Two named columns of one driver type, declared differently - which a schema is free to do -
        // and this change holds a value under each, which is what makes the disagreement visible here.
        // There is then no single answer to what this source calls that type, and picking either
        // spelling would rebuild every element as one of them and report success. The case below is
        // the same schema with only one of the two columns in the change, which is not this case.
        DriverKey key = new DriverKey("64f0c0de");
        Envelope decoded = insert(
                row("id", key, "ref", key, "arr", List.of(key)),
                CODECS,
                Map.of("id", KEY_COLUMN, "ref", "OTHER_KEY", "arr", "ARRAY"));

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, CODECS);

        assertThat(encoded.getAfter().get("arr"))
                .as("the element, which has no name of its own and now no unambiguous type either")
                .isEqualTo(List.of("64f0c0de"));
        // The named halves are untouched by the ambiguity: each is looked up by its own place.
        assertThat(encoded.getAfter().get("id")).isEqualTo(key);
        assertThat(encoded.getAfter().get("ref")).isEqualTo("64f0c0de");
    }

    @Test
    void anAmbiguousSchemaOnlyRefusesTheChangesThatActuallyShowTheAmbiguity() {
        // The same two-way schema as above, and a change that carries only one of the two columns -
        // which is the ordinary shape of a sparse field, so it is the common case rather than the
        // corner one. The reading is taken off the change: a column that is not in it, or is null in
        // it, attaches its name to no class and so contradicts nothing, and the element is restored
        // from the one spelling on offer. Pinned because the refusal reads as an absolute and is not
        // one - nothing in a field map says which declared name belongs to which driver class until a
        // value arrives holding the two together, so no reading of the schema alone could do better.
        DriverKey key = new DriverKey("64f0c0de");

        Envelope missing = insert(
                row("id", key, "arr", List.of(key)),
                CODECS,
                Map.of("id", KEY_COLUMN, "ref", "OTHER_KEY", "arr", "ARRAY"));

        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(missing, CODECS)).getAfter().get("arr"))
                .as("the second spelling's column is not in this change, so it contradicts nothing")
                .isEqualTo(List.of(key));

        Map<String, Object> withNull = row("id", key, "arr", List.of(key));
        withNull.put("ref", null);
        Envelope nulled = insert(withNull, CODECS,
                Map.of("id", KEY_COLUMN, "ref", "OTHER_KEY", "arr", "ARRAY"));

        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(nulled, CODECS)).getAfter().get("arr"))
                .as("present but null names no class either, which is the same answer")
                .isEqualTo(List.of(key));
    }

    @Test
    void oneCollectionLandsBothWaysWhereTheColumnThatNamesTheTypeIsNullable() {
        // One schema, one collection, two documents that differ only in whether the nullable column
        // naming this driver type is populated. The reading is taken off the document's own values, so
        // the one that carries the name restores its array and the one that does not leaves its
        // elements portable: the same collection landing both ways, in the same run and the same
        // pipeline. That is the ordinary case rather than a corner of it - a nullable column of a
        // driver type is ordinary - so it is pinned here rather than left to be discovered in a target.
        //
        // Pinned rather than closed, because no reading of the schema alone can do better. A field map
        // says what a column is called and never which driver class that name belongs to; the registry
        // is keyed by class and carries no name. Only a value ties the two together, so the tie cannot
        // be worked out per table before the values arrive.
        //
        // Carrying the tie across documents instead - learning it from one and keeping it for the rest
        // of the table - would trade this for something worse. A document would then decode by what the
        // stream happened to deliver before it: the same document restored on one run and left portable
        // on the next after a resume from a different position, a snapshot and its change stream
        // disagreeing, and values already written to the target unable to be taken back when a second
        // spelling turned up later and withdrew the name. None of that is visible in the target. Taken
        // per document, the answer is a function of that document alone - whatever it is, it is the
        // same every time that document is read, which is the property a reader can act on.
        DriverKey key = new DriverKey("64f0c0de");
        Map<String, String> schema = Map.of("cover", KEY_COLUMN, "thumbs", "ARRAY");

        Envelope carried = insert(row("cover", key, "thumbs", List.of(key)), CODECS, schema);

        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(carried, CODECS)).getAfter().get("thumbs"))
                .as("the document that carries the naming column restores its elements")
                .isEqualTo(List.of(key));

        Map<String, Object> withoutCover = row("thumbs", List.of(key));
        withoutCover.put("cover", null);
        Envelope nulled = insert(withoutCover, CODECS, schema);

        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(nulled, CODECS)).getAfter().get("thumbs"))
                .as("the same array in the same collection, portable where that column is null")
                .isEqualTo(List.of("64f0c0de"));
    }

    @Test
    void aLaterUpdateOfOneRowRewritesTheArrayItsSnapshotRestored() {
        // The same row twice: the snapshot, which carries every column, and a later change that touches
        // only the array. A change stream reports the key plus what changed, so neither image of that
        // change holds the column naming this driver type - and the reading is taken off the change, so
        // there is nothing to take. The elements travel as text, and a write into a keyed target sets
        // the fields it is given, so that text lands over the values the snapshot already restored. The
        // write reports success and the target shows the text: the same field is the driver's type
        // after the snapshot and text after the update, in one table and one run.
        //
        // Pinned rather than closed, because the only readings that could answer here are the two the
        // per-document case above already weighs and rejects. Keeping the tie from an earlier change
        // makes a row decode by whatever the stream happened to deliver before it, so a resume from
        // another position silently changes the answer and nothing in the target shows it. Refusing to
        // write an element the reading cannot name turns a value the target can hold into a dropped
        // field or a failed row. A visibly wrong type in the target is the better of the three.
        DriverKey key = new DriverKey("64f0c0de");
        DriverKey added = new DriverKey("64f0c0df");
        Map<String, String> schema = Map.of("_id", "STRING", "cover", KEY_COLUMN, "thumbs", "ARRAY");

        Envelope snapshot = TapEventCodec.decodeSnapshotRow(
                TapInsertRecordEvent.create().table("albums").referenceTime(1000L)
                        .after(row("_id", "album-1", "cover", key, "thumbs", List.of(key))),
                CODECS, schema);

        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(snapshot, CODECS)).getAfter().get("thumbs"))
                .as("the snapshot carries the naming column, so the target stores the driver's type")
                .isEqualTo(List.of(key));

        Envelope update = TapEventCodec.decodeChange(
                TapUpdateRecordEvent.create().table("albums").referenceTime(2000L)
                        .before(row("_id", "album-1", "thumbs", List.of(key)))
                        .after(row("_id", "album-1", "thumbs", List.of(key, added))),
                CODECS, schema);

        TapUpdateRecordEvent encoded = (TapUpdateRecordEvent) TapEventCodec.encode(update, CODECS);

        assertThat(encoded.getAfter().get("thumbs"))
                .as("neither image names that type, so the same array is written back as text")
                .isEqualTo(List.of("64f0c0de", "64f0c0df"));
        assertThat(encoded.getBefore().get("thumbs"))
                .as("both halves of the change agree, which is what the one reading over two images buys")
                .isEqualTo(List.of("64f0c0de"));
    }

    @Test
    void aRowTheSchemaNamesThroughoutIsNotWalkedASecondTime() {
        // What this source calls a driver type is only ever asked for where the schema names no place,
        // so a row it names throughout has no use for the answer - and that is most rows on the hottest
        // path this adapter has, walked once per change and twice per update. Measured by counting the
        // conversion lookups one driver type takes: one, the walk's own. Taken up front instead, the
        // reading would walk this row a second time and look that same type up again, for an answer
        // nothing here ever asks for.
        CountingCodecs codecs = countingCodecs();

        Envelope decoded = insert(row("stamp", new DriverStamp(7), "qty", 5), codecs,
                Map.of("stamp", "STAMP", "qty", "INT64"));

        assertThat(decoded.after().get("qty")).as("the row decodes as it always did").isEqualTo(5L);
        assertThat(codecs.lookupsOf(DriverStamp.class))
                .as("the walk's own lookup, with no reading taken on top of it")
                .isEqualTo(1);
    }

    @Test
    void theReadingIsTakenOnceHoweverManyValuesTheSchemaNamesNoPlaceFor() {
        // Three array elements, none of which the schema names a place for, so each of them asks. The
        // answer is taken off the whole change either way, so it is taken on the first ask and kept:
        // the named column's own type is looked up once more, not once per element.
        DriverKey key = new DriverKey("64f0c0de");
        CountingCodecs codecs = countingCodecs();

        Envelope decoded = insert(
                row("id", key, "stamp", new DriverStamp(7), "arr", List.of(key, key, key)),
                codecs,
                Map.of("id", KEY_COLUMN, "stamp", "STAMP", "arr", "ARRAY"));

        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(decoded, codecs)).getAfter().get("arr"))
                .as("the reading ran and answered, which is what makes the count below mean anything")
                .isEqualTo(List.of(key, key, key));
        assertThat(codecs.lookupsOf(DriverStamp.class))
                .as("the walk's own lookup plus one reading, whatever the number of elements asking")
                .isEqualTo(2);
    }

    @Test
    void anArrayOfValuesNoConversionIsRegisteredForTakesNoReadingAtAll() {
        // Only a class the connector registered a conversion for can ever be in the reading, so an
        // element of any other kind is answered without taking one. This is the ordinary array - plain
        // text, numbers - and it asks on every element, the schema naming no place that reaches one.
        // Answering those off the reading instead would walk the whole change a second time to be told
        // nothing, on the hottest path this adapter has.
        CountingCodecs codecs = countingCodecs();

        Envelope decoded = insert(
                row("stamp", new DriverStamp(7), "tags", List.of("red", "blue"), "sizes", List.of(1, 2)),
                codecs,
                Map.of("stamp", "STAMP", "tags", "ARRAY", "sizes", "ARRAY"));

        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(decoded, codecs)).getAfter())
                .as("the arrays decode as they always did, which is what makes the count below mean anything")
                .containsEntry("tags", List.of("red", "blue"))
                .containsEntry("sizes", List.of(1L, 2L));
        assertThat(codecs.lookupsOf(DriverStamp.class))
                .as("the walk's own lookup, with no reading taken on top of it")
                .isEqualTo(1);
    }

    /**
     * The same registrations the cases above run against, counting what the decode asks it - which is
     * the one thing that says how many times a row was walked, since a walk cannot reach a value
     * without asking whether the connector converts its type.
     */
    private static final class CountingCodecs extends TapCodecsRegistry {

        private final Map<Class<?>, Integer> lookups = new LinkedHashMap<>();

        @Override
        public ToTapValueCodec<?> getCustomToTapValueCodec(Class<?> clazz) {
            lookups.merge(clazz, 1, Integer::sum);
            return super.getCustomToTapValueCodec(clazz);
        }

        int lookupsOf(Class<?> type) {
            return lookups.getOrDefault(type, 0);
        }
    }

    private static CountingCodecs countingCodecs() {
        CountingCodecs codecs = new CountingCodecs();
        codecs.registerToTapValue(DriverKey.class, (value, tapType) ->
                new TapStringValue(((DriverKey) value).hex()));
        codecs.registerToTapValue(DriverStamp.class, (value, tapType) ->
                new TapStringValue(Long.toString(((DriverStamp) value).seconds())));
        codecs.registerFromTapValue(TapStringValue.class, tapValue ->
                KEY_COLUMN.equals(tapValue.getOriginType())
                        ? new DriverKey(tapValue.getValue())
                        : tapValue.getValue());
        return codecs;
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
    void aBinaryColumnBecomesAValueRatherThanTheContractsOwnBox() {
        Binary binary = new Binary((byte) 4, new byte[]{1, 2, 3});

        Object decoded = decodedByMongo(binary);

        // The one portable result not handed on as the connector answered it. The contract's box for
        // bytes declares no equality, so carried as it comes a binary column keys a join by identity and
        // matches nothing - with no error, which is why it is translated here rather than guarded there.
        // The tag comes along: a target of the same kind writes it back, and bytes alone would arrive
        // tagged as whatever the default is.
        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        Object value = ((ConvertedValue) decoded).value();
        assertThat(value).isEqualTo(new Bytes((byte) 4, new byte[]{1, 2, 3}));
        assertThat(((Bytes) value).value()).containsExactly(1, 2, 3);
        assertThat(((Bytes) value).tag()).isEqualTo((byte) 4);
    }

    @Test
    void aBinaryColumnsTagReachesATargetOfTheSameKind() {
        Envelope decoded = insert(row("payload", new Binary((byte) 4, new byte[]{1, 2, 3})), MONGO);
        TapCodecsRegistry target = mongoCodecs(Set.of())
                .registerFromTapValue(TapBinaryValue.class, TapValue::getValue);

        TapInsertRecordEvent encoded = (TapInsertRecordEvent) TapEventCodec.encode(decoded, target);

        // Out the far side the contract's box again, tag and all. Translating on the way in is only
        // worth doing if it is undone on the way out: a uuid column that came back tagged as a plain
        // blob would be a different column from the one that was read.
        Object written = encoded.getAfter().get("payload");
        assertThat(written).isInstanceOf(ByteData.class);
        assertThat(((ByteData) written).getType()).isEqualTo((byte) 4);
        assertThat(((ByteData) written).getValue()).containsExactly(1, 2, 3);
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

        // Carried like every other converted value: what travels is the text plus the column's name,
        // and neither is the driver's object, so there is no longer a type that cannot travel.
        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        assertThat(((ConvertedValue) decoded).value()).isEqualTo("/^eu/i");
    }

    @Test
    void aDecimal128ColumnKeepsEverySignificantDigit() {
        Decimal128 exact = Decimal128.parse("1234567890.123456789012345678901234");

        Object withoutTheRegisteredConversion =
                insert(row("v", exact), mongoCodecs(Set.of(Decimal128.class))).after().get("v");
        Object decoded = decodedByMongo(exact);

        assertThat(withoutTheRegisteredConversion)
                .as("the same driver value when no Decimal128 conversion is registered")
                .isSameAs(exact);
        // Assert the value rather than a chosen repair representation. The read path may keep the
        // driver's exact number or carry another exact numeric form, but it must not make a distinct
        // database value indistinguishable by first narrowing it to a binary floating point number.
        Object value = decoded instanceof ConvertedValue converted ? converted.value() : decoded;
        BigDecimal carried = switch (value) {
            case Decimal128 decimal -> decimal.bigDecimalValue();
            case BigDecimal decimal -> decimal;
            case Number number -> new BigDecimal(number.toString());
            default -> throw new AssertionError("the decimal column arrived as " + value.getClass().getName());
        };
        assertThat(carried).isEqualTo(exact.bigDecimalValue());
    }

    @Test
    void aDecimal128ConversionThatDoesNotUseADoubleWinsUnchanged() {
        Decimal128 exact = Decimal128.parse("1234567890.123456789012345678901234");
        TapCodecsRegistry corrected = new TapCodecsRegistry()
                .registerToTapValue(Decimal128.class,
                        (value, tapType) -> new TapStringValue(((Decimal128) value).toString()));

        Object decoded = insert(row("v", exact), corrected).after().get("v");

        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        assertThat(((ConvertedValue) decoded).value()).isEqualTo(exact.toString());
    }

    @Test
    void aDecimal128SpecialValueKeepsTheConnectorsPortableValue() {
        Object decoded = decodedByMongo(Decimal128.NaN);

        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        assertThat(((ConvertedValue) decoded).value())
                .isInstanceOf(Double.class)
                .matches(value -> ((Double) value).isNaN());
    }

    @Test
    void aDecimal128NegativeZeroKeepsTheConnectorsPortableValue() {
        Object decoded = decodedByMongo(Decimal128.NEGATIVE_ZERO);

        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        assertThat(((ConvertedValue) decoded).value())
                .isInstanceOf(Double.class)
                .isEqualTo(-0.0d);
    }

    @Test
    void aDecimal128TypeWithoutTheRequiredAccessorsCrashesAsAProgrammerError() throws Exception {
        var decimal = TapEventCodec.class.getDeclaredMethod("decimal128Value", Object.class);
        decimal.setAccessible(true);

        assertThatThrownBy(() -> decimal.invoke(null, new Object()))
                .isInstanceOf(InvocationTargetException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("mongodb decimal128 has no exact-value accessor")
                .hasCauseInstanceOf(NoSuchMethodException.class);
    }

    @Test
    void aTimestampReadsFromEpochSecondsDespiteTheConnectorsMillisecondsConversion() {
        // 2026-01-01T00:00:00Z, as a mongodb timestamp: seconds in the high half, a counter in the low.
        BsonTimestamp stamp = new BsonTimestamp(1_767_225_600, 7);

        Object decoded = decodedByMongo(stamp);

        // A timestamp's time half is seconds since the epoch. Treating it as milliseconds moves this
        // 2026 value into January 1970 and silently leaves a plausible but wrong instant on the row.
        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        Object instant = ((ConvertedValue) decoded).value();
        assertThat(instant).isInstanceOf(DateTime.class);
        assertThat(((DateTime) instant).toInstant()).isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
    }

    @Test
    void anAlreadyCorrectTimestampConversionIsNotAdjustedAgain() {
        TapCodecsRegistry corrected = new TapCodecsRegistry();
        corrected.registerToTapValue(BsonTimestamp.class, (value, tapType) -> {
            long seconds = Integer.toUnsignedLong(((BsonTimestamp) value).getTime());
            return new TapDateTimeValue(new DateTime(Instant.ofEpochSecond(seconds)));
        });

        Object decoded = insert(row("v", new BsonTimestamp(1_767_225_600, 7)), corrected).after().get("v");

        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        assertThat(((ConvertedValue) decoded).value())
                .isInstanceOf(DateTime.class)
                .extracting(value -> ((DateTime) value).toInstant())
                .isEqualTo(Instant.parse("2026-01-01T00:00:00Z"));
    }

    @Test
    void aTimestampConversionToAnotherPortableTypeIsLeftAlone() {
        TapCodecsRegistry textual = new TapCodecsRegistry();
        textual.registerToTapValue(BsonTimestamp.class,
                (value, tapType) -> new TapStringValue("timestamp:" + ((BsonTimestamp) value).getValue()));

        Object decoded = insert(row("v", new BsonTimestamp(1_767_225_600, 7)), textual).after().get("v");

        assertThat(decoded).isInstanceOf(ConvertedValue.class);
        assertThat(((ConvertedValue) decoded).value()).isEqualTo("timestamp:7590176156653977607");
    }

    @Test
    void aTimestampTypeWithoutTheRequiredAccessorCrashesAsAProgrammerError() throws Exception {
        var seconds = TapEventCodec.class.getDeclaredMethod("bsonTimestampSeconds", Object.class);
        seconds.setAccessible(true);

        assertThatThrownBy(() -> seconds.invoke(null, new Object()))
                .isInstanceOf(InvocationTargetException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("mongodb timestamp has no time accessor")
                .hasCauseInstanceOf(NoSuchMethodException.class);
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
        return insert(after, codecs, Map.of());
    }

    /** The same, for a case that needs the schema to have named the column - which the way back reads. */
    private static Envelope insert(
            Map<String, Object> after, TapCodecsRegistry codecs, Map<String, String> columnTypes) {
        return TapEventCodec.decodeChange(
                TapInsertRecordEvent.create().table("orders").referenceTime(1000L).after(after),
                codecs, columnTypes);
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            row.put((String) kv[i], kv[i + 1]);
        }
        return row;
    }
}
