package io.tapstate.spi.store;

import java.util.Map;

/** Store-issued original full facts; revalidation runs outside any CACHE transaction. */
public interface CompletedPipelineFull {
    String operationId();
    PipelineIntentScope targetScope();
    String preparationId();
    String confirmationHash();
    Map<String, String> sourceHashes();
    void requireCurrent();

    default SrsFreshFullOrigin origin(String sourceId) {
        if (!sourceHashes().containsKey(sourceId)) throw new IllegalArgumentException("source is outside the completed full");
        return new SrsFreshFullOrigin(operationId(), targetScope(), preparationId(), confirmationHash(), sourceId);
    }
}
