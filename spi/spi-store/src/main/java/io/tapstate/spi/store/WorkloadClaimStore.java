package io.tapstate.spi.store;

import java.time.Duration;
import java.util.Optional;

/** Persistence port for Mongo-time owner leases and their monotonic fencing generations. */
public interface WorkloadClaimStore {

    /** Acquires an absent, expired, or already-self-owned claim atomically. */
    WorkloadClaimAttempt acquire(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl);

    /** Renews only the exact owner and generation while its current lease is still live. */
    Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl);

    /** Expires only the exact owner and generation; the document and generation remain. */
    boolean release(WorkloadClaim expected);

    /** Allocates the next execution generation under the exact live claim and topology revision. */
    Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision);

    Optional<WorkloadClaim> read(WorkloadClaimKey key);
}
