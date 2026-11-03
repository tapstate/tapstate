package io.tapstate.spi.capture;

import io.tapstate.core.validation.ValidationTiming;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Optional semantic capability, not inferred from offset/query method availability. Providers use
 * existing capture and writer control metadata; no new subscription, source probe, source ACK or
 * recovery position is created. Missing capability keeps basic independent reads available.
 * All methods run under admitted bounded resources and deadline; failures stay coded failures.
 */
public interface ValidationTimingPort {
    List<ValidationTiming.Capabilities> capabilities(ValidationTiming.Execution execution);

    /** Register complete old/new dependency changes, including absent children, before L0. */
    CompletionStage<Watch> observe(ValidationTiming.Scope scope);

    interface Watch extends AutoCloseable {
        ValidationTiming.Scope scope();
        /** L0 follows registration; L1 follows all S1, target and S2 reads. Tokens are domain-local. */
        CompletionStage<ValidationTiming.Batch> sample(ValidationTiming.Phase phase, String batchId);
        /**
         * Reports only evidence for the exact issued batch. L0 includes all related normal output
         * commits (or proven zero output); L1 closes capture/hints/impact only. No candidate wait,
         * high-water maximum, queue emptiness, durable frontier or partial ACK substitutes for it.
         * Epoch/execution/authority violations fail; ordinary pending progress stays incomplete.
         */
        CompletionStage<ValidationTiming.Coverage> coverage(ValidationTiming.Batch batch);
        /** Release only this watch; never stop production capture or wait on its thread. */
        @Override void close();
    }
}
