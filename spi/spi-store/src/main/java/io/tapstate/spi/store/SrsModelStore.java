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

    /** Historical lookup is exact, never a request for the latest discovery result. */
    Optional<SrsModel> load(SrsModelKey key);

    default SrsModel require(SrsModelKey key) {
        return load(key).orElseThrow(() -> new TapstateException(IoError.SRS_MODEL_UNAVAILABLE,
                Map.of("chain", key.miningChainId(), "table", key.table(), "version", String.valueOf(key.modelVersion())), null));
    }
}
