package io.tapstate.spi.store;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Persistence port for Mongo-time owner leases and their monotonic fencing generations. */
public interface WorkloadClaimStore {

    /** Acquires an absent, expired, or already-self-owned claim atomically. */
    WorkloadClaimAttempt acquire(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl);

    /** Renews only the exact owner and generation while its current lease is still live. */
    Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl);

    /** Expires only the exact owner and generation; the document and generation remain. */
    boolean release(WorkloadClaim expected);

    /** Allocates the next execution generation and records the members it was planned over atomically. */
    Optional<WorkloadClaim> advanceExecution(
            WorkloadClaim expected, long topologyRevision, Set<String> executionNodeIds);

    /** Records the first observed failure of this execution under the exact live claim. Later observations leave it intact. */
    Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean afterMemberLoss);

    /**
     * Reads a claim and, in the same answer, how much of its lease the store's own clock says is left.
     *
     * <p>Both, always: a caller that could read the record without the lease would have no way to tell an
     * owner that is still there from one whose record simply outlived it.
     */
    Optional<WorkloadClaimReading> read(WorkloadClaimKey key);

    /**
     * Reads every one of these claims, each together with how much of its lease the store's own clock says
     * is left: what {@link #read} answers for one, under the key it was asked for, with a key nobody has
     * ever claimed simply absent. Order is not part of the answer.
     *
     * <p>For a caller that needs many claims at once -- a read face answering for every pipeline in the
     * cluster needs all of theirs. Asked one at a time that is a round trip per claim, in sequence, against
     * the store every renewal in the cluster also goes through: a cost that grows with the cluster while
     * nothing reports it but the clock. The default loops over {@link #read} openly, so a store that cannot
     * do better inherits the honest version, and a store that overrides it is saying it did better. An
     * empty {@code keys} reaches the store not at all.
     */
    default Map<WorkloadClaimKey, WorkloadClaimReading> readAll(Collection<WorkloadClaimKey> keys) {
        Map<WorkloadClaimKey, WorkloadClaimReading> readings = new LinkedHashMap<>();
        for (WorkloadClaimKey key : keys) {
            read(key).ifPresent(reading -> readings.put(key, reading));
        }
        return readings;
    }
}
