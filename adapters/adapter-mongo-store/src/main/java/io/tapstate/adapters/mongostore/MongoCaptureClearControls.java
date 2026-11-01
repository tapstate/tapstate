package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CaptureClearPreparation;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.Document;

/** Strict original-operation markers; decoding a marker grants no command authority. */
final class MongoCaptureClearControls {
    enum Phase { FROZEN, CANCELLED, RETIRED }
    record Control(String operation, long sequence, String requestHash, String nextChain,
            String nextPreparation, Phase phase, String receiptHash) {}
    private MongoCaptureClearControls() {}
    static Optional<Control> read(Document row) {
        if (row == null) throw unreadable();
        if (!row.containsKey("clearControl")) return Optional.empty();
        try {
            if (!Integer.valueOf(3).equals(row.get("formatVersion")) || row.containsKey("recoveryControl")) throw unreadable();
            var value = row.get("clearControl", Document.class);
            if (value == null || !value.keySet().equals(Set.of("operationId", "sequence", "requestHash", "nextChain",
                    "nextPreparationId", "phase", "receiptHash")) || !(value.get("sequence") instanceof Long sequence)
                    || sequence < 1 || sequence == Long.MAX_VALUE) throw unreadable();
            var result = new Control(value.getString("operationId"), sequence, value.getString("requestHash"),
                    value.getString("nextChain"), value.getString("nextPreparationId"),
                    Phase.valueOf(value.getString("phase")), value.getString("receiptHash"));
            if (!java.util.UUID.fromString(result.operation()).toString().equals(result.operation())
                    || !java.util.UUID.fromString(result.nextPreparation()).toString().equals(result.nextPreparation())
                    || !result.requestHash().matches("[a-f0-9]{64}") || !result.receiptHash().matches("[a-f0-9]{64}")
                    || !result.nextChain().matches("srs-clear-[a-f0-9]{64}") || !encode(result).equals(value)) throw unreadable();
            return Optional.of(result);
        } catch (IllegalArgumentException | ClassCastException | NullPointerException invalid) { throw unreadable(); }
    }
    static Document encode(Control value) {
        return new Document("operationId", value.operation()).append("sequence", value.sequence())
                .append("requestHash", value.requestHash()).append("nextChain", value.nextChain())
                .append("nextPreparationId", value.nextPreparation()).append("phase", value.phase().name())
                .append("receiptHash", value.receiptHash());
    }
    static Document candidate(Document root, CaptureClearPreparation authority, Phase phase, String receipt) {
        var held = read(root);
        if (held.isPresent() && (held.orElseThrow().phase() == Phase.RETIRED
                || !same(held.orElseThrow(), authority) && !(held.orElseThrow().phase() == Phase.CANCELLED
                        && authority.sequence() > held.orElseThrow().sequence()))) throw unreadable();
        var control = new Control(authority.operationId(), authority.sequence(), authority.requestHash(),
                authority.replacement().chainId(), authority.replacement().preparationId(), phase, receipt);
        return new Document(root).append("formatVersion", 3).append("clearControl", encode(control));
    }
    static boolean same(Control control, CaptureClearPreparation authority) {
        return control.operation().equals(authority.operationId()) && control.sequence() == authority.sequence()
                && control.requestHash().equals(authority.requestHash()) && control.nextChain().equals(authority.replacement().chainId())
                && control.nextPreparation().equals(authority.replacement().preparationId());
    }
    /** Only a cancelled marker may be used by ordinary readers and writers again. */
    static Document ordinary(Document row) {
        var control = read(row);
        if (control.isEmpty()) return row;
        if (control.orElseThrow().phase() != Phase.CANCELLED) throw new TapstateException(IoError.CAPTURE_EXTERNAL_RETIREMENT_PENDING,
                Map.of("capture", row.getString("captureId"), "operation", control.orElseThrow().operation(), "reason", "cache-frozen"), null);
        var result = new Document(row); result.remove("clearControl"); result.put("formatVersion", 1); return result;
    }
    static String hash(Document value) {
        return CanonicalHash.ofText(JsonWriter.write(MongoSrsRecoveryControls.canonical(value)));
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "capture-clear", "field", "original-control"), null);
    }
}
