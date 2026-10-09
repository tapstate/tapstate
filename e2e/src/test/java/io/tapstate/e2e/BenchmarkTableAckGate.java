package io.tapstate.e2e;

import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.List;
import java.util.Objects;

/** Verifies a fixed source consumer's complete writer confirmations, independently of its read cursor. */
final class BenchmarkTableAckGate {
    private BenchmarkTableAckGate() { }
    private static final JsonWriterSettings CANONICAL = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    record Binding(String physicalChain, String pipeline, String source, String table,
            String consumer, List<String> writers, Document fence) {
        Binding { writers = List.copyOf(writers); fence = copy(fence); }
        @Override public Document fence() { return copy(fence); }
    }

    private static Document copy(Document source) {
        return source == null ? null : Document.parse(source.toJson(CANONICAL));
    }

    static Binding bind(String physicalChain, String pipeline, String source, String table, Document cursor) {
        String expected = SrsConsumerId.of(pipeline, source).value();
        identity(cursor, physicalChain, pipeline, source, expected);
        List<String> writers = writers(cursor, table);
        Object fence = cursor.get("sinkAckFence");
        if (fence != null && !(fence instanceof Document)) { throw new AssertionError("invalid sink execution fence"); }
        return new Binding(physicalChain, pipeline, source, table, expected, writers, (Document) fence);
    }

    static boolean covers(Binding binding, BenchmarkTableTerminalObserver.Point point, Document cursor) {
        requireBinding(binding, cursor);
        String expectedRing = io.tapstate.runtime.srs.SrsRingbuffer.ringName(binding.physicalChain(), binding.table());
        if (!expectedRing.equals(point.ring()) || point.epoch() < 1 || point.seq() < 0) {
            throw new AssertionError("terminal log point belongs to another table or an unknown generation");
        }
        Document progress = document(cursor, "sinkWriterProgress");
        for (String writer : binding.writers()) {
            Document tables = progress == null ? null : document(progress, writer);
            Document acknowledged = tables == null ? null : document(tables, binding.table());
            if (!confirmed(acknowledged, point) || !reached(acknowledged, "ringDone", point.seq())) { return false; }
        }
        Document confirmed = document(cursor, "sinkAckedByTable");
        Document table = confirmed == null ? null : document(confirmed, binding.table());
        Document done = document(cursor, "perTableRingDone");
        return confirmed(table, point) && reached(done, binding.table(), point.seq());
    }

    static void requireBinding(Binding binding, Document cursor) {
        identity(cursor, binding.physicalChain(), binding.pipeline(), binding.source(), binding.consumer());
        if (!writers(cursor, binding.table()).equals(binding.writers())
                || !Objects.equals(cursor.get("sinkAckFence"), binding.fence())) {
            throw new AssertionError("benchmark source writer plan or execution fence changed");
        }
    }

    private static boolean confirmed(Document document, BenchmarkTableTerminalObserver.Point point) {
        if (document == null || document.get("sinkAckedEpoch") == null || document.get("sinkAckedSeq") == null) {
            return false;
        }
        long epoch = integer(document.get("sinkAckedEpoch"));
        if (epoch != point.epoch()) { throw new AssertionError("target confirmation belongs to another capture generation"); }
        return integer(document.get("sinkAckedSeq")) >= point.seq();
    }

    private static boolean reached(Document document, String field, long sequence) {
        return document != null && document.get(field) != null && integer(document.get(field)) >= sequence;
    }

    private static long integer(Object value) {
        if (!(value instanceof Integer || value instanceof Long)) { throw new AssertionError("non-integer target confirmation"); }
        return ((Number) value).longValue();
    }

    private static List<String> writers(Document cursor, String table) {
        Document plan = document(cursor, "expectedSinkWriters");
        if (plan == null || !plan.keySet().equals(java.util.Set.of(table))
                || !(plan.get(table) instanceof List<?> raw) || raw.isEmpty() || raw.size() > 64) {
            throw new AssertionError("benchmark source has no exact bounded one-table writer plan");
        }
        List<String> writers = raw.stream().map(value -> {
            if (!(value instanceof String writer) || writer.isBlank() || writer.length() > 256) {
                throw new AssertionError("invalid target writer identity");
            }
            return writer;
        }).toList();
        if (writers.stream().distinct().count() != writers.size()) { throw new AssertionError("duplicate expected target writer"); }
        return writers;
    }

    private static void identity(Document cursor, String chain, String pipeline, String source, String consumer) {
        Document key = cursor == null ? null : document(cursor, "_id");
        if (key == null || !chain.equals(key.get("chain")) || !consumer.equals(key.get("pipeline"))
                || !chain.equals(cursor.get("miningChainId")) || !consumer.equals(cursor.get("pipelineId"))
                || !pipeline.equals(cursor.get("ownerPipelineId")) || !source.equals(cursor.get("sourceNodeId"))) {
            throw new AssertionError("target confirmation source consumer identity differs");
        }
    }

    private static Document document(Document parent, String field) {
        Object value = parent.get(field);
        if (value != null && !(value instanceof Document)) { throw new AssertionError("invalid target confirmation document"); }
        return (Document) value;
    }
}
