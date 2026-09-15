package io.tapstate.adapters.mongostore;

import com.mongodb.MongoClientSettings;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses that a row image survives being stored -- through an actual BSON encode and decode, not
 * through a copy of the map.
 *
 * <p>A row value the source connector converted travels inside a carrier holding the portable value and
 * the name the source's schema gave its column. Two things have to hold, and they fail in opposite ways.
 * BSON has to be able to write the encoded form at all: the carrier is a record whose value component is
 * declared {@code Object}, there is no codec for that, and the write goes down on the first converted row
 * -- loudly. And reading it back has to give the carrier again rather than whatever the encoding happened
 * to be: a row that comes back flattened has lost the declared type, the write side reads exactly that to
 * decide what to rebuild, and the target then writes a key as the text it travelled as -- silently.
 *
 * <p><strong>Every case goes over the wire on purpose.</strong> A round trip that only builds a
 * {@code Document} and reads it back is a copy of a map: the carrier is still the object it always was,
 * so the assertions hold no matter what the encoding does, and the whole class passes against a store
 * that cannot write a single converted row. Encoding into BSON and decoding out of it is what makes the
 * difference observable, and it needs no server to do it.
 */
class RowImagesTest {

    private static final String OID = "650f1a2b3c4d5e6f70819200";

    @Test
    void aCarriedValueSurvivesWithTheTypeItsColumnWasDeclaredAs() {
        Map<String, Object> read = overTheWire(Map.of("_id", new ConvertedValue(OID, "OBJECT_ID")));

        assertThat(read.get("_id"))
                .as("the declared type is the whole reason the carrier exists -- the target's way back "
                        + "reads it to decide whether to rebuild a key or write the text it travelled as")
                .isEqualTo(new ConvertedValue(OID, "OBJECT_ID"));
    }

    @Test
    void aCarrierNestedInsideADocumentSurvives() {
        Map<String, Object> read =
                overTheWire(Map.of("doc", Map.of("key", new ConvertedValue(OID, "OBJECT_ID"))));

        assertThat(read.get("doc"))
                .as("a document's own fields are converted just as top-level columns are, so an encoding "
                        + "that only looks at the top level leaves a carrier inside the document")
                .isEqualTo(Map.of("key", new ConvertedValue(OID, "OBJECT_ID")));
    }

    @Test
    void aCarrierNestedInsideAnArraySurvives() {
        Map<String, Object> read =
                overTheWire(Map.of("tags", List.of(new ConvertedValue(OID, "OBJECT_ID"))));

        assertThat(read.get("tags")).isEqualTo(List.of(new ConvertedValue(OID, "OBJECT_ID")));
    }

    @Test
    void aCarrierTheSchemaNamedNoTypeForComesBackWithNone() {
        Map<String, Object> read = overTheWire(Map.of("nested", new ConvertedValue("v", null)));

        assertThat(read.get("nested")).isEqualTo(new ConvertedValue("v", null));
        assertThat(((ConvertedValue) read.get("nested")).originType())
                .as("null and a type spelled with no characters must not arrive as the same answer -- the "
                        + "target's way back tests this")
                .isNull();
    }

    @Test
    void aCarriedBinaryValueSurvivesWithItsTag() {
        Map<String, Object> read = overTheWire(
                Map.of("blob", new ConvertedValue(new Bytes((byte) 4, new byte[] {1, 2, 3}), "UUID")));

        assertThat(read.get("blob"))
                .as("a target of the same kind writes the tag back, and bytes arriving tagged as the "
                        + "default are a different column from the one that was read")
                .isEqualTo(new ConvertedValue(new Bytes((byte) 4, new byte[] {1, 2, 3}), "UUID"));
    }

    @Test
    void aCarriedBinaryTaggedTheWayBsonTagsAPlainOneStillComesBackAsBytes() {
        Map<String, Object> read = overTheWire(
                Map.of("blob", new ConvertedValue(new Bytes((byte) 0, new byte[] {9}), "BINARY")));

        assertThat(read.get("blob"))
                .as("BSON hands a binary of that one tag back as a plain byte array rather than as a "
                        + "binary, so a decode that reads the tag off the binary loses this case alone")
                .isEqualTo(new ConvertedValue(new Bytes((byte) 0, new byte[] {9}), "BINARY"));
    }

    @Test
    void aRowThatMetNoConversionIsUnchanged() {
        Map<String, Object> row = Map.of("id", 1, "amount", "10.00");

        assertThat(overTheWire(row))
                .as("the ordinary row is every row on a source whose keys the connector does not convert")
                .isEqualTo(row);
    }

    @Test
    void anOrdinaryNestedDocumentIsNotMistakenForACarrier() {
        Map<String, Object> row = Map.of("doc", Map.of("value", "v", "originType", "OBJECT_ID"));

        assertThat(overTheWire(row))
                .as("a source's own document may have fields spelled like a carrier's, and reading one "
                        + "back as a carrier would hand the write side a type nobody declared")
                .isEqualTo(row);
    }

    /**
     * One row image encoded and decoded exactly the way the driver does on the way to the wire and back.
     *
     * <p>{@code Document.toBsonDocument} is deliberately not used: it returns a wrapper that encodes only
     * when something reads it, so a test that merely builds one passes while the encoding it was meant to
     * exercise never runs.
     */
    private static Map<String, Object> overTheWire(Map<String, Object> row) {
        DocumentCodec codec = new DocumentCodec(MongoClientSettings.getDefaultCodecRegistry());
        BsonDocument wire = new BsonDocument();
        codec.encode(new BsonDocumentWriter(wire), RowImages.toDocument(row),
                EncoderContext.builder().build());
        return RowImages.toRow(
                codec.decode(new BsonDocumentReader(wire), DecoderContext.builder().build()));
    }
}
