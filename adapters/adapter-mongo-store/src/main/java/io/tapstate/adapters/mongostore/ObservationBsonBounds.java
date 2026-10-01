package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Binary;

import java.util.Date;
import java.util.List;
import java.util.Map;

/** Four bounded descriptor slots leave space for the fixed manifest header within its physical limit. */
final class ObservationBsonBounds {
    static final int DESCRIPTOR_BYTES = 2 * 1024 * 1024;
    static final int ENVELOPE_BYTES = 12 * 1024 * 1024;
    private ObservationBsonBounds() { }

    static void requireDescriptor(String pipelineId, Document descriptor) {
        require(pipelineId, descriptor, DESCRIPTOR_BYTES);
    }

    static void requireHeader(String pipelineId, Document descriptor) {
        require(pipelineId, descriptor, DESCRIPTOR_BYTES - LatestObservationPayloadCodec.INLINE_PAYLOAD_LIMIT - 2048);
    }

    private static void require(String pipelineId, Document document, int limit) {
        // Count cell bytes before allocating the encoding buffer; oversized identity strings cannot inflate it.
        if (documentBytes(document) > limit || bsonBytes(document) > limit) {
            throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", pipelineId), null);
        }
    }

    static int bsonBytes(Document document) {
        try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, document, EncoderContext.builder().build());
            return output.getSize();
        }
    }

    private static long documentBytes(Map<String, ?> document) {
        long bytes = 5;
        for (var cell : document.entrySet()) {
            bytes += 2L + utf8Bytes(cell.getKey()) + valueBytes(cell.getValue());
            if (bytes > DESCRIPTOR_BYTES) { return bytes; }
        }
        return bytes;
    }

    private static long valueBytes(Object value) {
        if (value == null) { return 0; }
        if (value instanceof String text) { return 5L + utf8Bytes(text); }
        if (value instanceof Binary binary) { return 5L + binary.getData().length; }
        if (value instanceof byte[] binary) { return 5L + binary.length; }
        if (value instanceof Document document) { return documentBytes(document); }
        if (value instanceof List<?> list) {
            long bytes = 5;
            for (int index = 0; index < list.size(); index++) {
                bytes += 2L + Integer.toString(index).length() + valueBytes(list.get(index));
                if (bytes > DESCRIPTOR_BYTES) { return bytes; }
            }
            return bytes;
        }
        if (value instanceof Integer) { return 4; }
        if (value instanceof Long || value instanceof Double || value instanceof Float || value instanceof Date) { return 8; }
        if (value instanceof Boolean) { return 1; }
        throw new IllegalStateException("a descriptor has an unsupported BSON cell type: " + value.getClass().getName());
    }

    private static long utf8Bytes(String text) {
        long bytes = 0;
        for (int index = 0; index < text.length(); index++) {
            char value = text.charAt(index);
            if (value <= 0x7f) { bytes++; }
            else if (value <= 0x7ff) { bytes += 2; }
            else if (Character.isHighSurrogate(value) && index + 1 < text.length()
                    && Character.isLowSurrogate(text.charAt(index + 1))) { bytes += 4; index++; }
            else if (Character.isSurrogate(value)) { bytes++; }
            else { bytes += 3; }
            if (bytes > DESCRIPTOR_BYTES) { return bytes; }
        }
        return bytes;
    }
}
