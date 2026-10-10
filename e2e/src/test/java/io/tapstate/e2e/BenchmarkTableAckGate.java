package io.tapstate.e2e;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WriterProgress;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Verifies a fixed source consumer's complete writer confirmations, independently of its read cursor. */
final class BenchmarkTableAckGate {
    private BenchmarkTableAckGate() { }
    private static final JsonWriterSettings CANONICAL = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();

    record Binding(String physicalChain, String pipeline, String source, String table,
            String consumer, List<String> writers, Document fence, String writerRunId) {
        Binding { writers = List.copyOf(writers); fence = copy(fence); }
        @Override public Document fence() { return copy(fence); }
    }

    private static Document copy(Document source) {
        return source == null ? null : Document.parse(source.toJson(CANONICAL));
    }

    static Binding bind(String physicalChain, String pipeline, String source, String table, Document cursor) {
        String expected = SrsConsumerId.of(pipeline, source).value();
        identity(cursor, physicalChain, pipeline, source, expected);
        Run run = cursor.containsKey("writerRun") ? currentRun(cursor, table) : null;
        List<String> writers = run == null ? legacyWriters(cursor, table) : run.writers();
        Object fence = cursor.get("sinkAckFence");
        if (fence != null && !(fence instanceof Document)) { throw new AssertionError("invalid sink execution fence"); }
        return new Binding(physicalChain, pipeline, source, table, expected, writers, (Document) fence,
                run == null ? null : run.id());
    }

    static boolean covers(Binding binding, BenchmarkTableTerminalObserver.Point point, Document cursor) {
        requireBinding(binding, cursor);
        String expectedRing = io.tapstate.runtime.srs.SrsRingbuffer.ringName(binding.physicalChain(), binding.table());
        if (!expectedRing.equals(point.ring()) || point.epoch() < 1 || point.seq() < 0) {
            throw new AssertionError("terminal log point belongs to another table or an unknown generation");
        }
        if (binding.writerRunId() != null) {
            Run run = currentRun(cursor, binding.table());
            Long minimum = null;
            for (String writer : binding.writers()) {
                WriterProgress reported = run.progress().get(writer);
                if (reported == null) { return false; }
                if (reported.durableThrough().epoch() != point.epoch()) {
                    throw new AssertionError("target confirmation belongs to another capture generation");
                }
                long sequence = reported.durableThrough().seq();
                minimum = minimum == null ? sequence : Math.min(minimum, sequence);
            }
            if (minimum == null || minimum < point.seq()) { return false; }
        } else {
            // Old explicit-JAR witnesses keep their original schema; a present writerRun never enters this branch.
            Document progress = document(cursor, "sinkWriterProgress");
            for (String writer : binding.writers()) {
                Document tables = progress == null ? null : document(progress, writer);
                Document acknowledged = tables == null ? null : document(tables, binding.table());
                if (!confirmed(acknowledged, point) || !reached(acknowledged, "ringDone", point.seq())) { return false; }
            }
        }
        Document confirmed = document(cursor, "sinkAckedByTable");
        Document table = confirmed == null ? null : document(confirmed, binding.table());
        Document done = document(cursor, "perTableRingDone");
        return confirmed(table, point) && reached(done, binding.table(), point.seq());
    }

    static void requireBinding(Binding binding, Document cursor) {
        identity(cursor, binding.physicalChain(), binding.pipeline(), binding.source(), binding.consumer());
        boolean current = cursor.containsKey("writerRun");
        if (current != (binding.writerRunId() != null)) {
            throw new AssertionError("benchmark source writer accounting format changed");
        }
        Run run = current ? currentRun(cursor, binding.table()) : null;
        if (run != null && !run.id().equals(binding.writerRunId())) {
            throw new AssertionError("benchmark source writer run changed");
        }
        List<String> writers = run == null ? legacyWriters(cursor, binding.table()) : run.writers();
        if (!writers.equals(binding.writers())
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

    private static List<String> legacyWriters(Document cursor, String table) {
        return writers(document(cursor, "expectedSinkWriters"), table);
    }

    private static List<String> writers(Document plan, String table) {
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

    private record Run(String id, List<String> writers, Map<String, WriterProgress> progress) { }

    private static Run currentRun(Document cursor, String table) {
        Document run = requiredDocument(cursor, "writerRun");
        Object id = run.get("id");
        if (!(id instanceof String runId) || runId.isBlank() || runId.length() > 512) {
            throw new AssertionError("invalid benchmark source writer run identity");
        }
        List<String> writers = writers(requiredDocument(run, "expected"), table);
        Document progress = requiredDocument(run, "progress");
        if (!java.util.Set.of(table).containsAll(progress.keySet())) {
            throw new AssertionError("writer progress names an unexpected table");
        }
        Document entries = document(progress, table);
        Map<String, WriterProgress> byWriter = new LinkedHashMap<>();
        if (entries != null) {
            for (Map.Entry<String, Object> entry : entries.entrySet()) {
                if (!(entry.getValue() instanceof Document actual)
                        || !(actual.get("writer") instanceof String writer)
                        || !writers.contains(writer) || !entry.getKey().equals(writerKey(writer))
                        || byWriter.containsKey(writer)) {
                    throw new AssertionError("writer progress differs from the exact expected writer roster");
                }
                SourceOrder durable = order(actual, "durableEpoch", "durableSeq");
                ChainPosition tokened = null;
                if (actual.containsKey("token") || actual.containsKey("tokenEpoch") || actual.containsKey("tokenSeq")) {
                    if (!(actual.get("token") instanceof String token)) {
                        throw new AssertionError("invalid writer tokened position");
                    }
                    SourceOrder tokenOrder = order(actual, "tokenEpoch", "tokenSeq");
                    if (tokenOrder.compareTo(durable) > 0) {
                        throw new AssertionError("writer tokened position is ahead of its durable confirmation");
                    }
                    tokened = new ChainPosition(tokenOrder, token);
                }
                byWriter.put(writer, new WriterProgress(durable, tokened));
            }
        }
        return new Run(runId, writers, Map.copyOf(byWriter));
    }

    private static SourceOrder order(Document progress, String epochField, String sequenceField) {
        long epoch = integer(progress.get(epochField));
        long sequence = integer(progress.get(sequenceField));
        if (epoch < 1 || sequence < 0 && sequence != SourceOrder.SNAPSHOT_SEQ) {
            throw new AssertionError("invalid writer confirmation order");
        }
        return new SourceOrder(epoch, sequence);
    }

    private static String writerKey(String writer) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(writer.getBytes(StandardCharsets.UTF_8));
    }

    private static Document requiredDocument(Document parent, String field) {
        Document value = document(parent, field);
        if (value == null) { throw new AssertionError("missing target confirmation document: " + field); }
        return value;
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
