package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.srs.SourceChangeGuard;
import io.tapstate.runtime.engine.SourceKeyMovePreflight;
import io.tapstate.spi.store.*;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Resolves the configuration interlock and source progress on the member owning the CDC reader. */
record ScopedKeyMoveGuard(ExecutionFence fence, List<String> keys,
        PipelineKeyMoveWindows.Stream stream) implements SourceChangeGuard.Factory {
    static final String STORE_KEY = ScopedKeyMoveGuard.class.getName();

    ScopedKeyMoveGuard {
        keys = List.copyOf(keys);
        Objects.requireNonNull(stream);
    }

    @Override public SourceChangeGuard open(Processor.Context context) {
        return open(context, null);
    }

    @Override public SourceChangeGuard open(Processor.Context context, SourceKeyMovePreflight.Check preflight) {
        var member = context.hazelcastInstance();
        if (!(member.getUserContext().get(STORE_KEY) instanceof PipelineKeyMoveWindows windows))
            throw PipelineKeyMoveWindows.unresolved(fence.pipelineId());
        return new Guard(member, context.executionId(), windows, preflight);
    }

    private final class Guard implements SourceChangeGuard {
        private final HazelcastInstance member;
        private final long execution;
        private final String activation = fence.operatorExecutionKey();
        private final PipelineKeyMoveWindows windows;
        private final ExecutionAuthorization authorization;
        private PipelineKeyMoveWindows.Window open;
        private long nextPoll;
        private final SourceKeyMovePreflight.Check preflight;

        Guard(HazelcastInstance member, long execution, PipelineKeyMoveWindows windows, SourceKeyMovePreflight.Check preflight) {
            this.member = member;
            this.preflight = preflight;
            this.execution = execution;
            this.windows = windows;
            authorization = ExecutionAuthorization.of(member);
            authorization.require(fence);
            var previous = windows.read(fence.pipelineId());
            if (previous.isPresent()) requireOriginal(previous.get());
        }

        private void requireOriginal(PipelineKeyMoveWindows.Window window) {
            if (!window.scope().equals(fence.scope()) || window.executionId() != execution || !activation.equals(window.activation()))
                throw PipelineKeyMoveWindows.unresolved(fence.pipelineId());
        }

        @Override public void beforeBatch(Iterable<Envelope> rows) {
            PipelineKeyMoveWindows.Range changes = null;
            for (var row : rows) {
                if (!mayMove(row, keys)) continue;
                if (preflight == null) throw PipelineKeyMoveWindows.unresolved(fence.pipelineId());
                preflight.validate(row);
                var position = row.position();
                if (position == null || position.order() == null || position.order().seq() < 0)
                    throw PipelineKeyMoveWindows.unresolved(fence.pipelineId());
                var order = position.order();
                if (changes != null && changes.epoch() != order.epoch()) {
                    // Admission is atomic for a prefix. Never publish half a rejected request.
                    throw PipelineKeyMoveWindows.unresolved(fence.pipelineId());
                }
                changes = changes == null ? new PipelineKeyMoveWindows.Range(order.epoch(), order.seq(), order.seq())
                        : new PipelineKeyMoveWindows.Range(order.epoch(), Math.min(changes.first(), order.seq()), Math.max(changes.last(), order.seq()));
            }
            if (changes != null) arm(changes);
        }

        private void arm(PipelineKeyMoveWindows.Range changes) {
            authorization.require(fence);
            var previous = open == null ? null : open.ranges().get(stream);
            if (previous != null && previous.epoch() != changes.epoch()) reconcile();
            open = windows.arm(fence.scope(), execution, activation, stream, changes, open, () -> authorization.requireFresh(fence));
        }

        @Override public void poll() {
            if (open == null) return;
            long now = System.nanoTime();
            if (now - nextPoll < 0) return;
            nextPoll = now + TimeUnit.SECONDS.toNanos(1);
            reconcile();
        }

        private void reconcile() {
            var pending = windows.read(fence.pipelineId());
            if (pending.isEmpty()) {
                open = null;
                return;
            }
            open = pending.get();
            requireOriginal(open);
            var views = (SrsStoreViews) member.getUserContext().get(SrsStoreViews.USER_CONTEXT_KEY);
            if (KeyMoveRecovery.retire(windows, open, views, () -> authorization.require(fence))) open = null;
        }
    }

    static boolean mayMove(Envelope row, List<String> keys) {
        if (row.op() != Op.UPDATE) return false;
        for (var key : keys) {
            if (row.before() == null || row.after() == null || !row.before().containsKey(key) || !row.after().containsKey(key)
                    || !Objects.deepEquals(io.tapstate.core.event.ConvertedValue.unwrap(row.before().get(key)),
                            io.tapstate.core.event.ConvertedValue.unwrap(row.after().get(key)))) return true;
        }
        return false;
    }
}
