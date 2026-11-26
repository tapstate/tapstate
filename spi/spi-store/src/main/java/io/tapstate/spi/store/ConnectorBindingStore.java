package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/** One effective binding set and one retryable artifact handoff in the CONFIG transaction domain. */
public interface ConnectorBindingStore {
    Optional<ConnectorRunBindings> load(PipelineIntentScope scope);

    /** Reads current bindings for observation in one CONFIG snapshot, without requiring an active generation. */
    Optional<ConnectorRunBindings> loadCurrent(String pipelineId);

    /** Reads only the exact predecessor retained by this target's controlled CACHE replacement. */
    default Optional<ConnectorRunBindings> previousForRecovery(PipelineIntentScope target) { return Optional.empty(); }

    /** One current CONFIG snapshot of effective and pending bindings, keyed by Pipeline id. */
    java.util.Map<String, ConnectorRunBindings> loadAll();

    /** Captures the original legacy coordinates before preparing a candidate. */
    ConnectorRunBindings seed(PipelineIntentSnapshot original, List<ConnectorRunBindings.Binding> effective);

    /** Freezes the candidate set for this exact original explicit start. */
    ConnectorRunBindings prepare(PipelineIntentSnapshot original, List<ConnectorRunBindings.Binding> candidates);

    /** Publishes every source and target binding together after external preparation succeeds. */
    ConnectorRunBindings publish(PipelineIntentSnapshot original);

    /** Removes a published handoff only after its previous resources have been released. */
    void complete(PipelineIntentScope scope, long startIntentRevision);

    /** Removes an unpublished handoff only after its candidate resources have been released. */
    void abort(PipelineIntentScope scope, long startIntentRevision);
}
