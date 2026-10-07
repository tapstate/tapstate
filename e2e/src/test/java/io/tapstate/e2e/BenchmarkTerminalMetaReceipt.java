package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.SrsConsumerId;
import org.bson.Document;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Reads terminal proof after the measured windows, without changing the application's store. */
final class BenchmarkTerminalMetaReceipt {
    private BenchmarkTerminalMetaReceipt() { }

    /** A failed window's exact source consumer, with opaque tokens replaced by numeric diagnostics. */
    static Map<String, Object> readPending(StoreDocuments documents, ControlPlane.PositionRead read,
            BenchmarkWorkloadDefinitions.SourceChain chain, Function<String, String> describe) {
        ControlPlane.PositionChain physical = physical(read, chain);
        Document root = documents.chain(physical.chainId());
        String scopedKey = SrsConsumerId.of(chain.pipelineId(), chain.sourceId()).value();
        Document split = documents.consumerOffset(physical.chainId(), scopedKey);
        StoredConsumer consumer = candidate(root == null ? null : nested(root, "consumerOffsets"),
                physical.chainId(), scopedKey, split);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("logicalChainId", chain.id());
        result.put("physicalChainId", physical.chainId());
        result.put("positionTargetAck", describe.apply(physical.targetAckedToken()));
        if (consumer == null) {
            result.put("state", "SCOPED_CONSUMER_ABSENT");
            return Map.copyOf(result);
        }
        Document cursor = consumer.document();
        checkIdentityField(cursor, "ownerPipelineId", chain.pipelineId());
        checkIdentityField(cursor, "sourceNodeId", chain.sourceId());
        result.put("actualConsumerId", consumer.id());
        result.put("consumerDocument", consumer.origin());
        result.put("scalarAck", pendingPoint(cursor, describe));
        Document tableAcks = nested(cursor, "sinkAckedByTable");
        result.put("tableAck", pendingPoint(tableAcks == null ? null : nested(tableAcks, chain.table()), describe));
        Document plan = nested(cursor, "expectedSinkWriters");
        if (plan != null) {
            if (plan.size() > 64) { throw new AssertionError("pending writer plan exceeded diagnostic bounds"); }
            result.put("writerPlanTables", List.copyOf(plan.keySet()));
            Object selected = plan.get(chain.table());
            if (!(selected instanceof List<?> writers) || writers.size() > 64) {
                throw new AssertionError("pending table has no bounded actual writer plan");
            }
            Document progress = nested(cursor, "sinkWriterProgress");
            Map<String, Object> recorded = new LinkedHashMap<>();
            for (Object raw : writers) {
                if (!(raw instanceof String writer) || writer.isBlank() || writer.length() > 256) {
                    throw new AssertionError("pending writer identity exceeded diagnostic bounds");
                }
                Document tables = progress == null ? null : nested(progress, writer);
                Document point = tables == null ? null : nested(tables, chain.table());
                Map<String, Object> value = new LinkedHashMap<>(pendingPoint(point, describe));
                if (point != null) {
                    copyOptional(value, "ringDone", point, "ringDone", Number.class);
                    copyOptional(value, "snapshotComplete", point, "snapshotComplete", Boolean.class);
                }
                recorded.put(writer, Map.copyOf(value));
            }
            result.put("expectedWriters", Map.copyOf(recorded));
        }
        copyTableNumber(result, "consumerReadSeq", cursor, "perTableSeq", chain.table());
        copyTableNumber(result, "consumerRingDoneSeq", cursor, "perTableRingDone", chain.table());
        result.put("readConsistency", "SEQUENTIAL_POINT_READS");
        return Map.copyOf(result);
    }

    private static Map<String, Object> pendingPoint(Document point, Function<String, String> describe) {
        Map<String, Object> result = new LinkedHashMap<>();
        Object token = point == null ? null : point.get("sinkAckedSrcpos");
        if (token != null && !(token instanceof String)) { throw new AssertionError("invalid pending ACK token"); }
        result.put("position", describe.apply((String) token));
        if (point != null) {
            copyOptional(result, "epoch", point, "sinkAckedEpoch", Number.class);
            copyOptional(result, "seq", point, "sinkAckedSeq", Number.class);
        }
        return Map.copyOf(result);
    }

    static Map<String, Object> read(StoreDocuments documents, ControlPlane.PositionRead read,
            BenchmarkWorkloadDefinitions.SourceChain chain, String terminal,
            BenchmarkAckOracle.PositionCoverage coverage) {
        Instant startedAt = Instant.now();
        ControlPlane.PositionChain physical = physical(read, chain);
        Document root = documents.chain(physical.chainId());
        // This is a lookup candidate. The receipt records the identity found in storage.
        String scopedKey = SrsConsumerId.of(chain.pipelineId(), chain.sourceId()).value();
        Document scoped = documents.consumerOffset(physical.chainId(), scopedKey);
        Document legacy = scoped == null ? documents.consumerOffset(physical.chainId(), chain.pipelineId()) : null;
        return interpret(read, chain, terminal, coverage, root, scoped, legacy, startedAt, Instant.now());
    }

    static Map<String, Object> interpret(ControlPlane.PositionRead read,
            BenchmarkWorkloadDefinitions.SourceChain chain, String terminal,
            BenchmarkAckOracle.PositionCoverage coverage, Document root, Document splitScoped,
            Document splitLegacy, Instant readStartedAt, Instant readCompletedAt) {
        ControlPlane.PositionChain physical = physical(read, chain);
        String wireAck = ControlPlane.resolveTargetAck(read, chain);
        if (!coverage.covers(wireAck, terminal)) {
            throw new AssertionError("position target ACK no longer covers terminal for " + chain.id());
        }
        if (root == null || !physical.chainId().equals(root.getString("_id"))) {
            throw new AssertionError("no exact chain META for " + chain.id());
        }
        Document embedded = nested(root, "consumerOffsets");
        String scopedKey = SrsConsumerId.of(chain.pipelineId(), chain.sourceId()).value();
        StoredConsumer consumer = candidate(embedded, physical.chainId(), scopedKey, splitScoped);
        boolean scoped = consumer != null;
        if (!scoped) {
            long sourceNodes = read.chains().stream().filter(candidate ->
                    candidate.chainId().equals(physical.chainId())).map(ControlPlane.PositionChain::sourceId)
                    .distinct().count();
            if (sourceNodes != 1L) {
                throw new AssertionError("ambiguous legacy source consumer for " + chain.id());
            }
            consumer = candidate(embedded, physical.chainId(), chain.pipelineId(), splitLegacy);
        }
        if (consumer == null) {
            throw new AssertionError("no actual stored consumer for " + chain.id());
        }
        String storedId = consumer.id();
        if (!SrsConsumerId.pipelineOf(storedId).equals(chain.pipelineId())
                || scoped && !SrsConsumerId.sourceOf(storedId).orElseThrow().equals(chain.sourceId())) {
            throw new AssertionError("stored consumer does not belong to source chain " + chain.id());
        }
        Document cursor = consumer.document();
        checkIdentityField(cursor, "ownerPipelineId", chain.pipelineId());
        if (scoped) { checkIdentityField(cursor, "sourceNodeId", chain.sourceId()); }
        Document tableAcks = nested(cursor, "sinkAckedByTable");
        Document selectedAck = tableAcks == null ? null : nested(tableAcks, chain.table());
        String ackField = "sinkAckedByTable." + chain.table();
        if (selectedAck == null) {
            Object kind = cursor.get("progressKind");
            Document readCursors = nested(cursor, "perTableSeq");
            boolean soleTableCursor = readCursors == null || readCursors.isEmpty()
                    || readCursors.keySet().equals(java.util.Set.of(chain.table()));
            boolean soleConfirmedTable = tableAcks == null || tableAcks.isEmpty()
                    || tableAcks.keySet().equals(java.util.Set.of(chain.table()));
            if (!scoped && (kind == null || "LEGACY".equals(kind)) && soleTableCursor && soleConfirmedTable) {
                selectedAck = cursor;
                ackField = "sinkAcked";
            }
        }
        Map<String, Object> confirmed = sinkPoint(selectedAck);
        if (!(confirmed.get("token") instanceof String ack) || !coverage.covers(ack, terminal)) {
            throw new AssertionError("stored table target ACK does not cover terminal for " + chain.id());
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("logicalChainId", chain.id());
        result.put("pipelineId", chain.pipelineId());
        result.put("sourceId", physical.sourceId());
        result.put("table", chain.table());
        result.put("physicalChainId", physical.chainId());
        result.put("actualConsumerId", storedId);
        SrsConsumerId.sourceOf(storedId).ifPresent(source -> result.put("consumerSourceId", source));
        result.put("consumerBinding", scoped ? "SOURCE_SCOPED_ID" : "SOLE_SOURCE_LEGACY_ID");
        result.put("consumerDocument", consumer.origin());
        result.put("metaAckField", ackField);
        result.put("sourceTerminalToken", terminal);
        result.put("targetAck", confirmed);
        Map<?, ?> wire = wireChain(read, physical);
        copyWirePoint(result, "positionTargetAck", wire.get("targetAcked"));
        copyWirePoint(result, "positionResumeFrom", wire.get("resumeFrom"));
        if (wire.get("recordedAt") != null) { result.put("positionRecordedAt", wire.get("recordedAt")); }
        Map<String, Object> checkpoint = sourcePoint(root);
        if (!checkpoint.isEmpty()) { result.put("sourceRead", checkpoint); }
        copyOptional(result, "sourceReadDurable", root, "sourceReadDurable", Boolean.class);
        copyOptional(result, "sourceReadAtEpochMillis", root, "sourceReadAt", Number.class);
        copyOptional(result, "miningEpoch", root, "epoch", Number.class);
        copyOptional(result, "progressKind", cursor, "progressKind", String.class);
        copyTableNumber(result, "consumerReadSeq", cursor, "perTableSeq", chain.table());
        copyTableNumber(result, "consumerRingDoneSeq", cursor, "perTableRingDone", chain.table());
        result.put("metaReadStartedAt", readStartedAt.toString());
        result.put("metaReadCompletedAt", readCompletedAt.toString());
        result.put("readConsistency", "SEQUENTIAL_POINT_READS");
        return Map.copyOf(result);
    }

    private static ControlPlane.PositionChain physical(ControlPlane.PositionRead read,
            BenchmarkWorkloadDefinitions.SourceChain chain) {
        ControlPlane.resolveTargetAckIfPresent(read, chain);
        ControlPlane.PositionChain result = read.chains().stream().filter(candidate ->
                candidate.sourceId().equals(chain.sourceId()) && candidate.tables().contains(chain.table()))
                .findFirst().orElseThrow();
        if (!result.tables().equals(List.of(chain.table()))) {
            throw new AssertionError("terminal META receipt requires the frozen one-table source " + chain.id());
        }
        return result;
    }

    private record StoredConsumer(String id, String origin, Document document) { }

    private static StoredConsumer candidate(Document embedded, String physical,
            String lookupId, Document split) {
        if (split != null) {
            Document key = nested(split, "_id");
            String storedId = split.getString("pipelineId");
            if (!lookupId.equals(storedId) || !physical.equals(split.getString("miningChainId"))
                    || key == null || !physical.equals(key.getString("chain"))
                    || !storedId.equals(key.getString("pipeline"))) {
                throw new AssertionError("split consumer identity differs from its queried coordinates");
            }
            return new StoredConsumer(storedId, "SPLIT_CONSUMER_DOCUMENT", split);
        }
        if (embedded == null || !embedded.containsKey(lookupId)) { return null; }
        Document value = nested(embedded, lookupId);
        if (value == null) { throw new AssertionError("embedded consumer is not a document"); }
        // The embedded map key is the stored identity, unlike a split document's pipelineId field.
        String storedId = embedded.keySet().stream().filter(lookupId::equals).findFirst().orElseThrow();
        return new StoredConsumer(storedId, "EMBEDDED_CHAIN_CONSUMER", value);
    }

    private static Map<?, ?> wireChain(ControlPlane.PositionRead read, ControlPlane.PositionChain physical) {
        if (read.observedDocument() == null || !(JsonReader.parse(read.observedDocument()) instanceof Map<?, ?> root)
                || !(root.get("chains") instanceof List<?> chains)) {
            throw new AssertionError("terminal receipt has no actual position document");
        }
        List<Map<?, ?>> matches = chains.stream().filter(Map.class::isInstance).<Map<?, ?>>map(value -> (Map<?, ?>) value)
                .filter(value -> physical.chainId().equals(value.get("chainId"))
                        && physical.sourceId().equals(value.get("sourceId"))
                        && physical.tables().equals(value.get("tables"))).toList();
        if (matches.size() != 1) { throw new AssertionError("terminal receipt position lineage is ambiguous"); }
        return matches.getFirst();
    }

    private static void copyWirePoint(Map<String, Object> into, String name, Object raw) {
        if (raw == null) { return; }
        if (!(raw instanceof Map<?, ?> point)) { throw new AssertionError("invalid position point " + name); }
        Map<String, Object> recorded = new LinkedHashMap<>();
        for (String field : List.of("token", "epoch", "seq")) {
            Object value = point.get(field);
            if (value != null) {
                if (field.equals("token") ? !(value instanceof String) : !(value instanceof Number)) {
                    throw new AssertionError("invalid position point field " + field);
                }
                recorded.put(field, value);
            }
        }
        if (!recorded.isEmpty()) { into.put(name, Map.copyOf(recorded)); }
    }

    private static Map<String, Object> sinkPoint(Document document) {
        return point(document, "sinkAckedSrcpos", "sinkAckedEpoch", "sinkAckedSeq");
    }

    private static Map<String, Object> sourcePoint(Document document) {
        return point(document, "sourceReadOffset", "sourceReadEpoch", "sourceReadSeq");
    }

    private static Map<String, Object> point(Document document, String token, String epoch, String seq) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (document != null) {
            copyOptional(result, "token", document, token, String.class);
            copyOptional(result, "epoch", document, epoch, Number.class);
            copyOptional(result, "seq", document, seq, Number.class);
        }
        return Map.copyOf(result);
    }

    private static void copyTableNumber(Map<String, Object> into, String name, Document document,
            String field, String table) {
        Document perTable = nested(document, field);
        if (perTable != null) { copyOptional(into, name, perTable, table, Number.class); }
    }

    private static void copyOptional(Map<String, Object> into, String name, Document document,
            String field, Class<?> type) {
        Object value = document.get(field);
        if (value == null) { return; }
        if (!type.isInstance(value)) { throw new AssertionError("invalid terminal META field " + field); }
        into.put(name, value);
    }

    private static Document nested(Document document, String field) {
        Object value = document.get(field);
        if (value == null) { return null; }
        if (!(value instanceof Document nested)) { throw new AssertionError("invalid terminal META document " + field); }
        return nested;
    }

    private static void checkIdentityField(Document document, String field, String expected) {
        if (document.get(field) != null && !Objects.equals(document.get(field), expected)) {
            throw new AssertionError("terminal META identity differs at " + field);
        }
    }
}
