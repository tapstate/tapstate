package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Strict control receipts for an isolated physical chain; serialization itself grants no store authority. */
final class MongoSrsRecoveryControls {
    enum Phase { CANCELLED, RETIRED }
    record Control(String operationId, String planHash, String replacementChain, Phase phase, String controlHash) {
        Control {
            if (operationId == null || !UUID.fromString(operationId).toString().equals(operationId)
                    || !hash(planHash) || replacementChain == null || !replacementChain.matches("srs-recovery-[0-9a-f]{64}") || phase == null || !hash(controlHash))
                throw new IllegalArgumentException("invalid shared recovery control");
        }
    }
    private MongoSrsRecoveryControls() {}

    static Optional<Control> read(Document root) {
        try {
            if (root == null) throw unreadable();
            if (!root.containsKey("recoveryControl")) {
                if (!Integer.valueOf(1).equals(root.get("formatVersion"))) throw unreadable();
                return Optional.empty();
            }
            if (!Integer.valueOf(2).equals(root.get("formatVersion"))) throw unreadable();
            var row = root.get("recoveryControl", Document.class);
            if (row == null || !row.keySet().equals(Set.of("formatVersion", "operationId", "planHash", "replacementChain", "phase", "controlHash"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion"))) throw unreadable();
            var result = new Control(row.getString("operationId"), row.getString("planHash"), row.getString("replacementChain"),
                    Phase.valueOf(row.getString("phase")), row.getString("controlHash"));
            if (!encode(result).equals(row) || result.replacementChain().equals(root.get("_id"))) throw unreadable();
            return Optional.of(result);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }

    static Document originalControlRoot(Document root) {
        var control = read(root);
        if (control.isEmpty()) return root;
        var original = new Document(root);
        original.remove("recoveryControl");
        original.put("formatVersion", 1);
        return original;
    }

    static void requireOrdinary(Document root) {
        if (read(root).isPresent()) {
            if (!(root.get("captureId") instanceof String capture) || capture.isBlank()) throw unreadable();
            throw MongoCapturePreparationFacts.history(capture);
        }
    }

    /** Produces a candidate for the caller's exact root CAS, never a persistence or execution permission. */
    static Document candidate(Document original, String operation, String planHash, String replacement, Phase phase,
            List<Document> memberships, List<Document> cursors) {
        var retained = read(original);
        if (retained.isPresent()) {
            var old = retained.orElseThrow();
            if (old.phase() == Phase.RETIRED || !old.operationId().equals(operation) || !old.replacementChain().equals(replacement)) throw unreadable();
        }
        var result = new Document(original);
        if (!(original.get("modelWriteFence") instanceof Long revision) || revision < 0 || revision >= Long.MAX_VALUE - 1) throw unreadable();
        result.put("formatVersion", 2);
        result.put("modelWriteFence", revision + 1);
        result.put("recoveryControl", encode(new Control(operation, planHash, replacement, phase, "0".repeat(64))));
        result.get("recoveryControl", Document.class).put("controlHash", digest(result, memberships, cursors));
        read(result);
        return result;
    }

    static void verify(Document root, List<Document> memberships, List<Document> cursors) {
        var control = read(root).orElseThrow(MongoSrsRecoveryControls::unreadable);
        if (!control.controlHash().equals(digest(root, memberships, cursors))) throw unreadable();
    }

    private static String digest(Document root, List<Document> memberships, List<Document> cursors) {
        var fixed = new Document(root);
        var control = new Document(root.get("recoveryControl", Document.class));
        control.remove("controlHash");
        fixed.put("recoveryControl", control);
        var payload = new TreeMap<String, Object>();
        payload.put("root", canonical(fixed));
        payload.put("memberships", canonical(sorted(memberships)));
        payload.put("cursors", canonical(sorted(cursors)));
        return CanonicalHash.ofText(JsonWriter.write(payload));
    }

    private static List<Document> sorted(List<Document> rows) {
        var result = new ArrayList<>(rows);
        try { result.sort(Comparator.comparing(row -> row.get("_id", Document.class).getString("consumer"))); }
        catch (NullPointerException | ClassCastException invalid) { throw unreadable(); }
        return result;
    }

    private static Object canonical(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Integer || value instanceof Long) return value;
        if (value instanceof Date date) return List.of("date-millis", date.getTime());
        if (value instanceof List<?> values) return values.stream().map(MongoSrsRecoveryControls::canonical).toList();
        if (value instanceof Map<?, ?> values) {
            var result = new TreeMap<String, Object>();
            for (var entry : values.entrySet()) {
                if (!(entry.getKey() instanceof String key)) throw unreadable();
                result.put(key, canonical(entry.getValue()));
            }
            return result;
        }
        throw unreadable();
    }

    private static Document encode(Control value) {
        return new Document("formatVersion", 1).append("operationId", value.operationId()).append("planHash", value.planHash())
                .append("replacementChain", value.replacementChain()).append("phase", value.phase().name()).append("controlHash", value.controlHash());
    }
    private static boolean hash(String value) { return value != null && value.matches("[0-9a-f]{64}"); }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "srs-recovery-control", "field", "control"), null);
    }
}
