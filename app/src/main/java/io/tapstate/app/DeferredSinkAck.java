package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** One sink writer's confirmed progress, flushed from that writer's live and idle processor turns. */
final class DeferredSinkAck implements SinkAck {
    private static final long serialVersionUID = 1L;
    private static final long INTERVAL = TimeUnit.SECONDS.toNanos(10);
    private final transient SinkAck delegate;
    private final transient BooleanSupplier authorized;
    private final transient LongSupplier clock;
    private final transient Map<String, Progress> pending = new LinkedHashMap<>();
    private long pendingSince;
    private boolean failed;

    DeferredSinkAck(SinkAck delegate, BooleanSupplier authorized) {
        this(delegate, authorized, System::nanoTime);
    }

    DeferredSinkAck(SinkAck delegate, BooleanSupplier authorized, LongSupplier clock) {
        this.delegate = delegate;
        this.authorized = authorized;
        this.clock = clock;
    }

    @Override public SinkAck forWriter(String writerId) {
        return new DeferredSinkAck(delegate.forWriter(writerId), authorized, clock);
    }

    @Override public void advance(String stream, ChainPosition position) {
        Progress progress = pending(stream);
        progress.bound(position.order());
        if (position.token() != null && (progress.token == null || position.order().compareTo(progress.token.order()) > 0)) {
            progress.token = position;
        }
    }

    @Override public void started(long jobId, long executionId) {
        delegate.started(jobId, executionId);
    }

    @Override public void bounded(String stream, SourceOrder through) {
        // Initial-load readiness needs this exact boundary even if newer CDC progress is pending.
        if (through.seq() == SourceOrder.SNAPSHOT_SEQ) {
            if (failed) return;
            try {
                delegate.bounded(stream, through);
            } catch (RuntimeException | Error failure) {
                failed = true;
                throw failure;
            }
            return;
        }
        pending(stream).bound(through);
    }

    private Progress pending(String stream) {
        if (pending.isEmpty()) pendingSince = clock.getAsLong();
        return pending.computeIfAbsent(stream, ignored -> new Progress());
    }

    @Override public void flush(boolean complete) {
        if (pending.isEmpty() || failed || (!complete && clock.getAsLong() - pendingSince < INTERVAL)) return;
        try {
            for (var entry : pending.entrySet()) {
                String stream = entry.getKey();
                Progress progress = entry.getValue();
                if (progress.token != null) delegate.advance(stream, progress.token);
                if (progress.token == null || progress.through.compareTo(progress.token.order()) > 0) {
                    delegate.bounded(stream, progress.through);
                }
            }
            pending.clear();
        } catch (RuntimeException | Error failure) {
            failed = true;
            throw failure;
        }
    }

    @Override public void flushOnClose() {
        if (!pending.isEmpty() && !failed && authorized.getAsBoolean()) flush(true);
    }

    private static final class Progress {
        private SourceOrder through;
        private ChainPosition token;

        void bound(SourceOrder position) {
            if (through == null || position.compareTo(through) > 0) through = position;
        }
    }
}
