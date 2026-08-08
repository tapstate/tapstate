package io.tapstate.runtime.srs;

import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.spi.capture.Subscription;

import java.util.Objects;
import java.util.Optional;
import java.util.Map;

/**
 * The handle a {@link CaptureRunUnit#start started} source run hands back — what the assembly did and the
 * live pieces the downstream pipeline wires onto.
 *
 * <ul>
 *   <li>{@code chainId} — the mining chain provisioned for this run, present only on the shared-ring path;
 *       a snapshot-only or srs-disabled run opens no shared chain.</li>
 *   <li>{@code merged} — whether provisioning force-merged onto an already-open chain (a config coinciding
 *       with a running capture) rather than opening a fresh one; false when no chain was provisioned.</li>
 *   <li>{@code snapshotCount} — how many snapshot rows were drained straight to the pass-through sink; zero
 *       when the read mode runs no snapshot.</li>
 *   <li>{@code snapshotCounts} — the same count split by source stream; empty when the read mode runs no
 *       snapshot. This carries the table dimension needed by a multi-table source.</li>
 *   <li>{@code ringSource} — the self-built Jet source over the change ring, present only on the shared-ring
 *       path; the downstream reads the cdc tail from it.</li>
 *   <li>{@code cdcSubscription} — the handle that stops the cdc stream, present whenever a tail runs (the
 *       shared-ring writer or the srs-disabled direct stream); closing it stops the capture.</li>
 * </ul>
 *
 * <p>The handle is itself {@link AutoCloseable}: closing it tears the running capture down by closing the
 * cdc subscription it carries. A run that opened no tail (a snapshot-only or srs-disabled run) carries no
 * subscription, so closing it is a safe no-op. The subscription contract stops the capture with no checked
 * exception and is idempotent, so this close needs neither a throws clause nor its own guard.
 */
public record CaptureRun(
        Optional<MiningChainId> chainId,
        boolean merged,
        long snapshotCount,
        Map<String, Long> snapshotCounts,
        Optional<StreamSource<SrsItem>> ringSource,
        Optional<Subscription> cdcSubscription,
        CaptureHealth health) implements AutoCloseable {

    public CaptureRun(
            Optional<MiningChainId> chainId,
            boolean merged,
            long snapshotCount,
            Optional<StreamSource<SrsItem>> ringSource,
            Optional<Subscription> cdcSubscription,
            CaptureHealth health) {
        this(chainId, merged, snapshotCount, Map.of(), ringSource, cdcSubscription, health);
    }

    public CaptureRun {
        Objects.requireNonNull(chainId, "chainId");
        Objects.requireNonNull(snapshotCounts, "snapshotCounts");
        Objects.requireNonNull(ringSource, "ringSource");
        Objects.requireNonNull(cdcSubscription, "cdcSubscription");
        Objects.requireNonNull(health, "health");
        snapshotCounts = Map.copyOf(snapshotCounts);
    }

    /**
     * The failure the run's cdc stream died with, or empty while it is healthy or opened no tail. The
     * stream reports a failure on its own thread, so this is how a caller learns a tail died rather than
     * merely going quiet.
     */
    public Optional<Throwable> failure() {
        return health.failure();
    }

    /** Stops the capture by closing its cdc subscription, or does nothing when the run opened no tail. */
    @Override
    public void close() {
        cdcSubscription.ifPresent(Subscription::close);
    }
}
