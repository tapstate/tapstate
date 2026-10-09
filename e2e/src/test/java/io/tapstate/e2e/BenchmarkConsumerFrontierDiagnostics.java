package io.tapstate.e2e;

import io.tapstate.core.event.SourceOrder;
import org.bson.Document;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Reads only the already fetched consumer document; no arrival marker substitutes for actual ACK. */
final class BenchmarkConsumerFrontierDiagnostics {
    private BenchmarkConsumerFrontierDiagnostics() { }

    static Map<String, Object> read(BenchmarkTableAckGate.Binding binding, long epoch, Document cursor) {
        BenchmarkTableAckGate.requireBinding(binding, cursor);
        if (epoch < 1) { throw new IllegalArgumentException("consumer diagnostic needs a positive capture epoch"); }
        var result = new LinkedHashMap<String, Object>();
        result.put("epoch", epoch);
        result.put("scope", "SAME_CONSUMER_DOCUMENT_READ_TO_CONFIRMED_ACK_SEQUENCE_DISTANCE");
        result.put("sequenceDistanceScope", "NOT_DATA_ROWS_IN_FLIGHT_OR_AN_ATOMIC_STAGE_QUEUE");
        result.put("timeScope", "EXTERNAL_QUERY_BRACKET_NOT_EFFECT_TIME");
        result.put("ringDoneScope", "ARRIVAL_RING_DONE_ALONE_IS_NOT_ACK");
        result.put("performanceAcceptanceEligible", false);

        Nested reads = nested(cursor, "perTableSeq");
        Object rawRead = field(reads.document(), binding.table());
        retainScalar(result, "consumerReadSeq", rawRead);
        Long readSeq = integer(rawRead);
        Reason reason = reads.invalidShape() ? Reason.CONSUMER_READ_INVALID_SHAPE
                : rawRead == null ? Reason.CONSUMER_READ_MISSING
                : readSeq == null ? Reason.CONSUMER_READ_NOT_INTEGER
                : readSeq < 0 ? Reason.CONSUMER_READ_NEGATIVE : Reason.NONE;

        Nested progress = nested(cursor, "sinkWriterProgress");
        var writers = new LinkedHashMap<String, Object>();
        Long minimumAck = null;
        Long minimumRing = null;
        boolean allWritersConfirmed = true;
        for (String writerId : binding.writers()) {
            Nested tables = nested(progress.document(), writerId);
            Nested writer = nested(tables.document(), binding.table());
            Frontier frontier = frontier(writer.document(), field(writer.document(), "ringDone"), epoch,
                    progress.invalidShape() || tables.invalidShape() || writer.invalidShape());
            writers.put(writerId, frontier.evidence());
            if (frontier.reason() != Reason.NONE) {
                allWritersConfirmed = false;
                if (reason == Reason.NONE) { reason = frontier.reason(); }
            } else {
                minimumAck = minimumAck == null ? frontier.ackSeq() : Math.min(minimumAck, frontier.ackSeq());
                minimumRing = minimumRing == null ? frontier.ringDone() : Math.min(minimumRing, frontier.ringDone());
                if (readSeq != null && readSeq >= 0 && frontier.ackSeq() > readSeq && reason == Reason.NONE) {
                    reason = Reason.WRITER_ACK_AHEAD_OF_CONSUMER_READ;
                }
                if (readSeq != null && readSeq >= 0 && frontier.ringDone() > readSeq && reason == Reason.NONE) {
                    reason = Reason.WRITER_RING_DONE_AHEAD_OF_CONSUMER_READ;
                }
            }
        }
        result.put("expectedWriterCount", binding.writers().size());
        result.put("writerFrontiers", immutable(writers));
        result.put("allWritersConfirmed", allWritersConfirmed);

        Nested acknowledgements = nested(cursor, "sinkAckedByTable");
        Nested table = nested(acknowledgements.document(), binding.table());
        Nested completed = nested(cursor, "perTableRingDone");
        Frontier tableFrontier = frontier(table.document(), field(completed.document(), binding.table()), epoch,
                acknowledgements.invalidShape() || table.invalidShape() || completed.invalidShape());
        result.put("tableFrontier", tableFrontier.evidence());
        if (tableFrontier.reason() != Reason.NONE && reason == Reason.NONE) { reason = tableFrontier.reason(); }
        if (allWritersConfirmed) {
            result.put("minimumWriterAckSeq", minimumAck);
            result.put("minimumWriterRingDone", minimumRing);
            if (tableFrontier.reason() == Reason.NONE) {
                if (!tableFrontier.ackSeq().equals(minimumAck) && reason == Reason.NONE) {
                    reason = Reason.TABLE_ACK_DIFFERS_FROM_WRITER_MINIMUM;
                }
                if (!tableFrontier.ringDone().equals(minimumRing) && reason == Reason.NONE) {
                    reason = Reason.TABLE_RING_DONE_DIFFERS_FROM_WRITER_MINIMUM;
                }
            }
        }
        if (reason == Reason.NONE) {
            result.put("readToConfirmedAckSequenceDistance", Math.subtractExact(readSeq, minimumAck));
        }
        result.put("state", reason == Reason.NONE ? "RECORDED" : "UNKNOWN");
        result.put("unknownReason", reason.name());
        return immutable(result);
    }

    private static Frontier frontier(Document acknowledged, Object rawRingDone, long epoch, boolean invalidShape) {
        var values = new LinkedHashMap<String, Object>();
        Object rawEpoch = field(acknowledged, "sinkAckedEpoch");
        Object rawAck = field(acknowledged, "sinkAckedSeq");
        retainScalar(values, "sinkAckedEpoch", rawEpoch);
        retainScalar(values, "sinkAckedSeq", rawAck);
        retainScalar(values, "ringDone", rawRingDone);
        Long actualEpoch = integer(rawEpoch);
        Long actualAck = integer(rawAck);
        Long ringDone = integer(rawRingDone);
        Reason reason = invalidShape ? Reason.FRONTIER_INVALID_SHAPE
                : rawEpoch == null ? Reason.ACK_EPOCH_MISSING
                : actualEpoch == null ? Reason.ACK_EPOCH_NOT_INTEGER
                : actualEpoch != epoch ? Reason.ACK_EPOCH_MISMATCH
                : rawAck == null ? Reason.ACK_SEQUENCE_MISSING
                : actualAck == null ? Reason.ACK_SEQUENCE_NOT_INTEGER
                : actualAck == SourceOrder.SNAPSHOT_SEQ ? Reason.SNAPSHOT_ACK_ONLY
                : actualAck < 0 ? Reason.ACK_SEQUENCE_NEGATIVE
                : rawRingDone == null ? Reason.RING_DONE_MISSING
                : ringDone == null ? Reason.RING_DONE_NOT_INTEGER
                : ringDone < 0 ? Reason.RING_DONE_NEGATIVE
                : ringDone < actualAck ? Reason.RING_DONE_BEHIND_ACTUAL_ACK : Reason.NONE;
        values.put("state", reason == Reason.NONE ? "CONFIRMED" : "UNKNOWN");
        values.put("unknownReason", reason.name());
        return new Frontier(immutable(values), actualAck, ringDone, reason);
    }

    private static Nested nested(Document parent, String field) {
        Object raw = field(parent, field);
        return new Nested(raw instanceof Document document ? document : null, raw != null && !(raw instanceof Document));
    }

    private static Object field(Document parent, String field) { return parent == null ? null : parent.get(field); }

    private static Long integer(Object raw) {
        return raw instanceof Integer || raw instanceof Long ? ((Number) raw).longValue() : null;
    }

    /** Retains exact integer values and bounded numeric type evidence, never arbitrary document text. */
    private static void retainScalar(Map<String, Object> values, String field, Object raw) {
        if (raw == null) { return; }
        Long integer = integer(raw);
        if (integer != null) { values.put(field, integer); return; }
        var unsupported = new LinkedHashMap<String, Object>();
        unsupported.put("rawType", raw.getClass().getName());
        if (raw instanceof Double valueDouble && Double.isFinite(valueDouble)
                || raw instanceof Float valueFloat && Float.isFinite(valueFloat)) {
            Number number = (Number) raw;
            String text = number.toString();
            unsupported.put("rawNumericValue", text.length() <= 256 ? text : text.substring(0, 256));
            unsupported.put("rawNumericValueTruncated", text.length() > 256);
        }
        values.put(field, immutable(unsupported));
    }

    private static Map<String, Object> immutable(Map<String, Object> values) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    private record Nested(Document document, boolean invalidShape) { }
    private record Frontier(Map<String, Object> evidence, Long ackSeq, Long ringDone, Reason reason) { }

    private enum Reason {
        NONE, CONSUMER_READ_INVALID_SHAPE, CONSUMER_READ_MISSING, CONSUMER_READ_NOT_INTEGER,
        CONSUMER_READ_NEGATIVE, FRONTIER_INVALID_SHAPE, ACK_EPOCH_MISSING, ACK_EPOCH_NOT_INTEGER,
        ACK_EPOCH_MISMATCH, ACK_SEQUENCE_MISSING, ACK_SEQUENCE_NOT_INTEGER, SNAPSHOT_ACK_ONLY,
        ACK_SEQUENCE_NEGATIVE, RING_DONE_MISSING, RING_DONE_NOT_INTEGER, RING_DONE_NEGATIVE,
        RING_DONE_BEHIND_ACTUAL_ACK, WRITER_ACK_AHEAD_OF_CONSUMER_READ,
        WRITER_RING_DONE_AHEAD_OF_CONSUMER_READ,
        TABLE_ACK_DIFFERS_FROM_WRITER_MINIMUM, TABLE_RING_DONE_DIFFERS_FROM_WRITER_MINIMUM
    }
}
