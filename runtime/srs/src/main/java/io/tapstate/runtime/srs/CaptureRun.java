package io.tapstate.runtime.srs;

import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.spi.capture.Subscription;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
 * <p><b>A run {@linkplain CaptureRunUnit#begin begun} rather than started is handed back while its load is
 * still being read.</b> The load and the tail that follows it then run on a thread of the run's own, so the
 * two counts above say how far the load has got, and the subscription is there once the tail has opened.
 * A failure of either is reported through {@link #failure()} rather than thrown, because by then nobody is
 * waiting on the call that began the run.
 *
 * <p>The handle is itself {@link AutoCloseable}: closing it tears the running capture down -- it abandons a
 * load still being read, and closes the cdc subscription the run carries. A run that opened no tail (a
 * snapshot-only or srs-disabled run) carries no subscription, so closing one whose load is over is a safe
 * no-op. The subscription contract stops the capture with no checked exception and is idempotent, so this
 * close needs neither a throws clause nor its own guard.
 */
public final class CaptureRun implements AutoCloseable {

    private final Optional<MiningChainId> chainId;
    private final boolean merged;
    private final long snapshotCount;
    private final Map<String, Long> snapshotCounts;
    private final Optional<StreamSource<SrsItem>> ringSource;
    private final Optional<Subscription> cdcSubscription;
    private final CaptureHealth health;

    /** The load still being read behind this run; null for a run handed back with its load already over. */
    private final BackgroundLoad load;

    public CaptureRun(
            Optional<MiningChainId> chainId,
            boolean merged,
            long snapshotCount,
            Map<String, Long> snapshotCounts,
            Optional<StreamSource<SrsItem>> ringSource,
            Optional<Subscription> cdcSubscription,
            CaptureHealth health) {
        this.chainId = Objects.requireNonNull(chainId, "chainId");
        this.merged = merged;
        this.snapshotCount = snapshotCount;
        this.snapshotCounts = Map.copyOf(Objects.requireNonNull(snapshotCounts, "snapshotCounts"));
        this.ringSource = Objects.requireNonNull(ringSource, "ringSource");
        this.cdcSubscription = Objects.requireNonNull(cdcSubscription, "cdcSubscription");
        this.health = Objects.requireNonNull(health, "health");
        this.load = null;
    }

    public CaptureRun(
            Optional<MiningChainId> chainId,
            boolean merged,
            long snapshotCount,
            Optional<StreamSource<SrsItem>> ringSource,
            Optional<Subscription> cdcSubscription,
            CaptureHealth health) {
        this(chainId, merged, snapshotCount, Map.of(), ringSource, cdcSubscription, health);
    }

    /** A run whose load, and the tail after it, are being carried on by {@code load}. */
    CaptureRun(
            Optional<MiningChainId> chainId,
            boolean merged,
            Optional<StreamSource<SrsItem>> ringSource,
            CaptureHealth health,
            BackgroundLoad load) {
        this.chainId = Objects.requireNonNull(chainId, "chainId");
        this.merged = merged;
        this.snapshotCount = 0;
        this.snapshotCounts = Map.of();
        this.ringSource = Objects.requireNonNull(ringSource, "ringSource");
        this.cdcSubscription = Optional.empty();
        this.health = Objects.requireNonNull(health, "health");
        this.load = Objects.requireNonNull(load, "load");
    }

    public Optional<MiningChainId> chainId() {
        return chainId;
    }

    public boolean merged() {
        return merged;
    }

    /** The snapshot rows passed on so far: all of them once the load is over. */
    public long snapshotCount() {
        return load == null ? snapshotCount : load.rows();
    }

    /** {@link #snapshotCount()} by source stream. */
    public Map<String, Long> snapshotCounts() {
        return load == null ? snapshotCounts : load.rowsByTable();
    }

    public Optional<StreamSource<SrsItem>> ringSource() {
        return ringSource;
    }

    /** The subscription that stops the tail, once there is one: a begun run opens it after its load. */
    public Optional<Subscription> cdcSubscription() {
        return load == null ? cdcSubscription : load.tail();
    }

    public CaptureHealth health() {
        return health;
    }

    /** Whether this run is still reading its load, or opening the tail that follows it. */
    public boolean loading() {
        return load != null && !load.finished();
    }

    /**
     * Waits up to {@code timeout} for this run's load, and the tail that follows it, to be over -- read
     * through, failed or abandoned; answers whether it is. A run handed back with its load over answers at
     * once.
     */
    public boolean awaitLoaded(Duration timeout) throws InterruptedException {
        return load == null || load.awaitFinished(timeout);
    }

    /**
     * The failure the run's load or cdc stream died with, or empty while it is healthy or opened no tail.
     * Both report a failure on a thread of their own, so this is how a caller learns a run died rather than
     * merely going quiet.
     */
    public Optional<Throwable> failure() {
        return health.failure();
    }

    /**
     * Stops the capture: abandons a load still being read, then closes the cdc subscription, or does
     * nothing when the run has neither.
     */
    @Override
    public void close() {
        if (load != null) {
            load.cancel();
            return;
        }
        cdcSubscription.ifPresent(Subscription::close);
    }
}
