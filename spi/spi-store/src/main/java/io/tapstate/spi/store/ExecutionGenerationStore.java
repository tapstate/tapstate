package io.tapstate.spi.store;

import java.util.Optional;
import java.util.OptionalLong;

/** The two authorized ways to advance one pipeline's durable execution sequence. */
public interface ExecutionGenerationStore {

    /** Advances only beneath the exact live actuation claim and committed topology revision. */
    Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long topologyRevision);

    /** Advances the same coordination document without creating a lease in standalone mode. */
    OptionalLong advanceStandalone(String clusterId, String pipelineId);
}
