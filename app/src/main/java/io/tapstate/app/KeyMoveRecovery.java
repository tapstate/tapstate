package io.tapstate.app;

import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.*;
import java.util.HashMap;
import java.util.Map;

/** Inspects the old source and writer records before a new source or writer run replaces them. */
final class KeyMoveRecovery {
    private KeyMoveRecovery() {}

    static boolean retire(PipelineKeyMoveWindows windows, PipelineKeyMoveWindows.Window window,
            SrsStoreViews views, Runnable active) {
        var scope = window.scope();
        var cache = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
        var resumable = new HashMap<PipelineKeyMoveWindows.Stream, SourceOrder>();
        var settled = new HashMap<PipelineKeyMoveWindows.Stream, SourceOrder>();
        active.run();
        for (var stream : window.ranges().keySet()) {
            if (stream.privateCapture() != null) {
                var context = new DirectChannelContext(scope, stream.chain(), stream.source(), stream.privateCapture());
                var state = views.directChannels(cache, active).load(context);
                var writers = state.writerRun();
                if (state.resumeOverride() != null || state.cursor().sinkAcked() == null || writers == null
                        || !original(window, writers.commandFence())) return false;
                resumable.put(stream, state.cursor().sinkAcked().order());
                recordSettled(settled, stream, writers.accounting());
            } else {
                var stores = views.srs(cache, active);
                var context = new SrsConsumerContext(new SrsConsumerKey(stream.chain(), scope.pipelineId(),
                        stream.source()), scope.stateGeneration());
                var progress = stores.consumers().progress(context);
                if (progress.isEmpty() || progress.get().writerRun() == null) return false;
                var writers = progress.get().writerRun();
                if (!original(window, writers.commandFence()) || writers.membershipRevision() != progress.get().membershipRevision()) return false;
                var chain = stores.chains().load(stream.chain());
                if (chain.isEmpty() || chain.get().resumeOverride() != null || chain.get().checkpoint() == null) return false;
                var table = chain.get().checkpoint().tables().get(stream.table());
                var consumer = progress.get().ringDoneThrough().get(stream.table());
                var confirmed = progress.get().cursor().sinkAckedByTable().get(stream.table());
                if (table == null || consumer == null || confirmed == null) return false;
                var order = confirmed.order();
                // The reader resumes at the greater of the physical floor and this original confirmation.
                // A larger bare floor has no capture generation and cannot establish a safe restart here.
                if (order == null || order.seq() < consumer || order.seq() > table.sequence()
                        || order.epoch() != table.model().captureEpoch()) return false;
                resumable.put(stream, order);
                recordSettled(settled, stream, writers.withSnapshotEpoch(null));
            }
        }
        active.run();
        return windows.retire(window, resumable, settled);
    }

    private static boolean original(PipelineKeyMoveWindows.Window window, WorkloadClaimFence claim) {
        return window.activation().equals(new ExecutionFence(window.scope(), claim).operatorExecutionKey());
    }

    private static void recordSettled(Map<PipelineKeyMoveWindows.Stream, SourceOrder> settled,
            PipelineKeyMoveWindows.Stream stream, WriterRun run) {
        WriterFrontier.landed(run, stream.table()).ifPresent(floor -> settled.put(stream, floor.durableThrough()));
    }
}
