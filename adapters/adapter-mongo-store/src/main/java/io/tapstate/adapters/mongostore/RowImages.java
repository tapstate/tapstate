package io.tapstate.adapters.mongostore;

import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Row images on the way into a stored document and back out again.
 *
 * <p>A value the source connector converted travels inside a carrier -- the portable value together with
 * the name the source's own schema gave the column it came out of -- and a stored row has to give that
 * carrier back. BSON cannot write one as it stands: the carrier is a record whose value component is
 * declared {@code Object}, and a record's codec is built per component, so the write fails on the first
 * converted row with no codec for {@code Object}. Every source whose key the connector converts produces
 * one on every change, so this is the ordinary row rather than an exotic one.
 *
 * <p>Encoding a carrier as a sub-document under a reserved marker makes it writable, and the marker is
 * what tells a carrier from an ordinary nested document on the way back.
 *
 * <p><strong>The way back carries as much as the way in.</strong> A row whose carriers came back
 * flattened would have lost the declared type, and the write side reads exactly that to decide what to
 * rebuild -- so a key would be written as the text it travelled as, and nothing would report it.
 *
 * <p><strong>A binary column is the same problem without the crash.</strong> Its bytes travel in a value
 * of their own carrying the tag the source put on them, which is a record too -- but one BSON happens to
 * be able to write, field by field. It comes back as a plain map holding a driver type, so the write side
 * is handed something that is no longer bytes-with-a-tag, and nothing reports that either. It is written
 * as a BSON binary, which is what a tag and its bytes are, under a marker of its own: a binary written
 * bare could not be told from a column whose value is an ordinary byte array.
 *
 * <p><strong>Carriers are encoded wherever they sit</strong>, not only as a whole column: a document's
 * own fields and an array's elements are converted just as a top-level column is, so an encoding that
 * looked only at the top level would leave a carrier inside a nested document.
 *
 * <p>Nested documents come back as plain maps rather than as the driver's own document type, so no
 * driver type escapes the module (rule R3).
 */
final class RowImages {

    /**
     * The marker naming a carrier. Reserved rather than short on purpose: a row value may itself be a
     * document with fields of its own, and a name a source could plausibly give one of those fields would
     * be read back as a carrier.
     */
    private static final String CARRIED = "__tapstate_carried";

    /** The column's declared type, written only when the schema named one. */
    private static final String ORIGIN_TYPE = "__tapstate_originType";

    /** The marker naming a binary column's bytes, reserved for the same reason as the carrier's. */
    private static final String BINARY = "__tapstate_bytes";

    private RowImages() {
    }

    /** One row image as the document to store. */
    static Document toDocument(Map<String, Object> row) {
        Document document = new Document();
        row.forEach((name, value) -> document.append(name, encoded(value)));
        return document;
    }

    /** One stored image back as a row, with every carrier it held put back. */
    static Map<String, Object> toRow(Document image) {
        return decodedMap(image);
    }

    private static Object encoded(Object value) {
        if (value instanceof ConvertedValue carrier) {
            Document held = new Document(CARRIED, encoded(carrier.value()));
            // Written only when present, never as an explicit null: the target's way back tests this, and
            // "no declared type" and "a type spelled with no characters" must not arrive as one answer.
            if (carrier.originType() != null) {
                held.append(ORIGIN_TYPE, carrier.originType());
            }
            return held;
        }
        if (value instanceof Bytes bytes) {
            return new Document(BINARY, new Binary(bytes.tag(), bytes.value()));
        }
        if (value instanceof Map<?, ?> map) {
            Document nested = new Document();
            map.forEach((name, element) -> nested.append(String.valueOf(name), encoded(element)));
            return nested;
        }
        if (value instanceof List<?> list) {
            List<Object> elements = new ArrayList<>(list.size());
            for (Object element : list) {
                elements.add(encoded(element));
            }
            return elements;
        }
        return value;
    }

    private static Object decoded(Object value) {
        if (value instanceof Map<?, ?> map) {
            if (map.get(CARRIED) != null) {
                return new ConvertedValue(decoded(map.get(CARRIED)), (String) map.get(ORIGIN_TYPE));
            }
            Object bytes = map.get(BINARY);
            if (bytes instanceof Binary binary) {
                return new Bytes(binary.getType(), binary.getData());
            }
            // A binary written with the tag BSON treats as the plain one is handed back as a byte array
            // rather than as a binary, so the tag has to be put back rather than read off it.
            if (bytes instanceof byte[] data) {
                return new Bytes((byte) 0, data);
            }
            return decodedMap(map);
        }
        if (value instanceof List<?> list) {
            List<Object> elements = new ArrayList<>(list.size());
            for (Object element : list) {
                elements.add(decoded(element));
            }
            return elements;
        }
        return value;
    }

    private static Map<String, Object> decodedMap(Map<?, ?> map) {
        Map<String, Object> out = new LinkedHashMap<>(map.size());
        map.forEach((name, value) -> out.put(String.valueOf(name), decoded(value)));
        return out;
    }
}
