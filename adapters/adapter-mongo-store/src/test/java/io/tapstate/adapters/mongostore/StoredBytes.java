package io.tapstate.adapters.mongostore;

import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * How many bytes a document occupies where it is stored, the ceiling that size is counted against, and the
 * schema shape the cases that reach for either are measuring.
 *
 * <p>One home for all three because the cases around the record's size assert their premise in measured
 * bytes, which makes those premises one claim rather than several: a copy corrected on its own -- or one
 * answering a neighbouring question, such as how long the document is printed as text -- would leave the
 * others silently disagreeing, with nothing to compile against and no test to go red.
 */
final class StoredBytes {

    /**
     * The hard ceiling on one stored document: 16 MiB. How many entries of anything reach it is derived
     * from measured bytes wherever that is asked; that there is no writing past it is not.
     */
    static final long DOCUMENT_CEILING = 16L * 1024 * 1024;

    private StoredBytes() {
    }

    /**
     * The document's size as BSON, which is the size the ceiling applies to -- not its length printed as
     * text, which is a different number and is the one the ceiling does not count.
     */
    static long bsonSize(Document document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new DocumentCodec().encode(
                new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        return buffer.getPosition();
    }

    /**
     * A table's field schema at the given width: the field-per-column shape a real table's carries, so
     * what a history entry weighs here is what one weighs in a record.
     */
    static Map<String, Object> schemaOfWidth(int columns) {
        Map<String, Object> schema = new LinkedHashMap<>();
        for (int i = 0; i < columns; i++) {
            schema.put("column_" + i, Map.of("type", "varchar", "length", 255, "nullable", true));
        }
        return schema;
    }
}
