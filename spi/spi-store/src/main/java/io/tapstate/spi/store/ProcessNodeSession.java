package io.tapstate.spi.store;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/** The in-process owner of an exclusive single-node OP installation. Mongo retains its fencing generation. */
public final class ProcessNodeSession {
    /** Stored in the legacy deadline field so timed readers treat this identity as expired. */
    public static final Instant PROCESS_OWNED = Instant.ofEpochMilli(-2);
    private static final ConcurrentHashMap<WorkloadClaimFence, Duration> ACTIVE = new ConcurrentHashMap<>();

    private ProcessNodeSession() {}

    public static boolean isProcessIdentity(WorkloadClaim claim) {
        return claim.key().type() == WorkloadClaimType.NODE_SESSION && PROCESS_OWNED.equals(claim.leaseUntil());
    }

    public static void open(WorkloadClaim claim, Duration proofWindow) {
        if (!isProcessIdentity(claim) || proofWindow.isZero() || proofWindow.isNegative())
            throw new IllegalArgumentException("invalid local Node Session");
        ACTIVE.put(WorkloadClaimFence.from(claim), proofWindow);
    }

    public static boolean owns(WorkloadClaimFence original) { return ACTIVE.containsKey(original); }

    public static void close(WorkloadClaim original) { ACTIVE.remove(WorkloadClaimFence.from(original)); }

    /** Bounds consumers' existing local proof caches. No periodic renewal is scheduled or persisted. */
    public static Duration proofWindow(WorkloadClaim original) {
        return ACTIVE.getOrDefault(WorkloadClaimFence.from(original), Duration.ZERO);
    }
}
