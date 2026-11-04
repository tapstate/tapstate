package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import java.util.*;

/** Strict identities for independent validation preparation, separate from production state generations. */
final class MongoValidationPreparationFacts {
    enum Phase { UNASSIGNED, ISSUED, COMPLETE, CLOSED }
    enum Outcome { PREPARED, CANCELLED, RETIRED }
    record Subject(CacheBusinessScope scope, String pipelineId, UUID generation, UUID preparationId) {
        Subject {
            Objects.requireNonNull(scope); Objects.requireNonNull(generation); Objects.requireNonNull(preparationId);
            if (pipelineId == null || pipelineId.isBlank() || pipelineId.length() > 512) throw unreadable();
        }
    }
    record Receipt(Subject subject, Subject origin, Outcome outcome, long revision, long claimGeneration, long executionGeneration) {
        Receipt {
            Objects.requireNonNull(subject); Objects.requireNonNull(outcome);
            if (revision < 0 || revision == Long.MAX_VALUE || claimGeneration < 0 || executionGeneration < 0
                    || (origin == null) != (claimGeneration == 0) || origin == null && executionGeneration != 0
                    || outcome != Outcome.CANCELLED && origin == null
                    || origin != null && (!origin.scope().equals(subject.scope()) || !origin.pipelineId().equals(subject.pipelineId()))) throw unreadable();
        }
    }
    record State(UUID generation, long revision, Phase phase, Subject subject, UUID closingOperationId, Receipt previous) {
        State {
            Objects.requireNonNull(generation); Objects.requireNonNull(phase);
            if (revision < 0 || revision == Long.MAX_VALUE || subject != null && !generation.equals(subject.generation())
                    || (phase == Phase.ISSUED || phase == Phase.COMPLETE) && subject == null
                    || phase == Phase.UNASSIGNED && subject != null
                    || phase == Phase.CLOSED && closingOperationId == null
                    || previous != null && previous.outcome() == Outcome.PREPARED
                    || phase == Phase.CLOSED && subject != null && (previous == null || !subject.equals(previous.subject()))) throw unreadable();
        }
        State change(Phase next, Subject selected, UUID closing, Receipt predecessor) {
            if (revision == Long.MAX_VALUE - 1) throw unreadable();
            return new State(generation, revision + 1, next, selected, closing, predecessor);
        }
    }
    private MongoValidationPreparationFacts() {}

    static Document initial() { return encode(new State(UUID.randomUUID(), 0, Phase.UNASSIGNED, null, null, null)); }
    static Document encode(State state) {
        var value = new Document("formatVersion", 1).append("generation", state.generation().toString()).append("revision", state.revision()).append("phase", state.phase().name());
        if (state.subject() != null) value.append("scope", scope(state.subject())).append("preparationId", state.subject().preparationId().toString());
        if (state.closingOperationId() != null) value.append("closingOperationId", state.closingOperationId().toString());
        if (state.previous() != null) value.append("previous", encode(state.previous()));
        bounded(value); return value;
    }
    static State decode(Document value, String clusterId, String pipelineId) {
        try {
            if (value == null || !Integer.valueOf(1).equals(value.get("formatVersion"))) throw unreadable();
            var keys = new HashSet<>(Set.of("formatVersion", "generation", "revision", "phase"));
            UUID generation = uuid(value, "generation"); Subject subject = null;
            if (value.containsKey("scope") || value.containsKey("preparationId")) {
                keys.add("scope"); keys.add("preparationId");
                var original = value.get("scope", Document.class); var scope = cacheScope(original);
                subject = new Subject(scope, original.getString("pipelineId"), generation, uuid(value, "preparationId"));
                if (!scope(subject).equals(original)) throw unreadable();
            }
            UUID closing = null; Receipt previous = null;
            if (value.containsKey("closingOperationId")) { keys.add("closingOperationId"); closing = uuid(value, "closingOperationId"); }
            if (value.containsKey("previous")) { keys.add("previous"); previous = receipt(value.get("previous", Document.class)); }
            if (!keys.equals(value.keySet())) throw unreadable();
            var state = new State(generation, longValue(value, "revision"), Phase.valueOf(value.getString("phase")), subject, closing, previous);
            for (var identity : new Subject[]{subject, previous == null ? null : previous.subject()})
                if (identity != null && (!clusterId.equals(identity.scope().clusterId()) || !pipelineId.equals(identity.pipelineId()))) throw unreadable();
            if (!encode(state).equals(value)) throw unreadable(); return state;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable(); }
    }
    static Document encode(Subject subject) {
        return scope(subject).append("generation", subject.generation().toString()).append("preparationId", subject.preparationId().toString());
    }
    static Subject subject(Document value) {
        try {
            var subject = new Subject(cacheScope(value), value.getString("pipelineId"), uuid(value, "generation"), uuid(value, "preparationId"));
            if (!encode(subject).equals(value)) throw unreadable(); return subject;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable(); }
    }
    static Document encode(Receipt receipt) {
        var value = new Document("subject", encode(receipt.subject())).append("origin", receipt.origin() == null ? null : encode(receipt.origin()))
                .append("outcome", receipt.outcome().name()).append("revision", receipt.revision())
                .append("claimGeneration", receipt.claimGeneration()).append("executionGeneration", receipt.executionGeneration());
        bounded(value); return value;
    }
    static Receipt receipt(Document value) {
        try {
            if (value == null || !value.keySet().equals(Set.of("subject", "origin", "outcome", "revision", "claimGeneration", "executionGeneration"))) throw unreadable();
            var receipt = new Receipt(subject(value.get("subject", Document.class)), value.get("origin") == null ? null : subject(value.get("origin", Document.class)),
                    Outcome.valueOf(value.getString("outcome")), longValue(value, "revision"), longValue(value, "claimGeneration"), longValue(value, "executionGeneration"));
            if (!encode(receipt).equals(value)) throw unreadable(); return receipt;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable(); }
    }
    private static Document scope(Subject subject) { return new Document("clusterId", subject.scope().clusterId()).append("bindingVersion", subject.scope().bindingVersion())
            .append("cacheIncarnation", subject.scope().cacheIncarnation()).append("pipelineId", subject.pipelineId()); }
    private static CacheBusinessScope cacheScope(Document value) { return new CacheBusinessScope(value.getString("clusterId"), longValue(value, "bindingVersion"), value.getString("cacheIncarnation")); }
    private static UUID uuid(Document value, String field) {
        if (!(value.get(field) instanceof String text)) throw unreadable(); var uuid = UUID.fromString(text); if (!uuid.toString().equals(text)) throw unreadable(); return uuid;
    }
    static long longValue(Document value, String field) { if (!(value.get(field) instanceof Long number)) throw unreadable(); return number; }
    static void bounded(Document value) {
        try (var buffer = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(buffer)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build()); if (buffer.getSize() > 8 * 1024) throw unreadable();
        }
    }
    static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "validation", "field", "independent-preparation"), null); }
}
