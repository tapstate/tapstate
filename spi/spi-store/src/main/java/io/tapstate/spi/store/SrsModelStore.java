package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Optional;

/** Actual models are durable before events may refer to them; publication never overwrites a version. */
public interface SrsModelStore {
    /** Keeps the original operation authorization before and after every callback attempt. */
    SrsModelStore guardedBy(Runnable requireActive);

    /** Each table/capture starts at actual version zero and advances by one; identical captured retries are safe. */
    SrsModel publish(SrsModel model, WorkloadClaimFence captureFence);

    /**
     * Reads published structural candidates for the first preparation of a new snapshot, in one cache snapshot.
     * Empty means at least one requested table has not published any model yet; known missing facts fail closed.
     * Callers must compare actual same-session metadata and durably pin the returned exact keys before reading rows.
     * This grants no capture authority or event-range proof, and must never replace saved snapshot references.
     */
    Optional<Map<String, SrsModel>> publishedForNewSnapshot(String miningChainId, Map<String, String> tableRings);

    /**
     * Resolve the exact model used by one hot or replayed event. The original recorded CAPTURE fence,
     * ring and sequence must belong to that model's retained effective range; the capture need not still
     * be live for historical replay. The caller's original reader lifecycle is retained by guardedBy.
     * This reads existing facts only and never publishes or falls back to a discovery result.
     */
    SrsModel requireEvent(SrsModelKey expected, String ringName, long sequence, WorkloadClaimFence originalCapture);

    /** Historical lookup is exact, never a request for the latest discovery result. */
    Optional<SrsModel> load(SrsModelKey key);

    default SrsModel require(SrsModelKey key) {
        return load(key).orElseThrow(() -> new TapstateException(IoError.SRS_MODEL_UNAVAILABLE,
                Map.of("chain", key.miningChainId(), "table", key.table(), "version", String.valueOf(key.modelVersion())), null));
    }
}
