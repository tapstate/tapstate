package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.Outbox;
import java.util.Objects;

/** Observes ordinary data and frontier offers without changing their values, ordinals or retry state. */
final class OutputPressureOutbox implements Outbox {

    private final Outbox delegate;
    private final OutputPressureMetrics metrics;

    OutputPressureOutbox(Outbox delegate, OutputPressureMetrics metrics) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    @Override
    public int bucketCount() {
        return delegate.bucketCount();
    }

    @Override
    public boolean offer(int ordinal, Object item) {
        long scope = metrics.scope();
        boolean accepted = delegate.offer(ordinal, item);
        metrics.observed(scope, accepted);
        return accepted;
    }

    @Override
    public boolean offer(int[] ordinals, Object item) {
        long scope = metrics.scope();
        boolean accepted = delegate.offer(ordinals, item);
        metrics.observed(scope, accepted);
        return accepted;
    }

    @Override
    public boolean offer(Object item) {
        long scope = metrics.scope();
        boolean accepted = delegate.offer(item);
        metrics.observed(scope, accepted);
        return accepted;
    }

    /** Snapshot persistence is a separate path and never closes an ordinary output retry. */
    @Override
    public boolean offerToSnapshot(Object key, Object value) {
        return delegate.offerToSnapshot(key, value);
    }

    @Override
    public boolean hasUnfinishedItem() {
        return delegate.hasUnfinishedItem();
    }
}
