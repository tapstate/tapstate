package io.tapstate.cli;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Dedicated live-log worker; it never shares cancellation with mutation actions. */
final class WorkbenchLogCoordinator implements AutoCloseable {
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r, "tapstate-logs"); t.setDaemon(true); return t; });
    private Future<?> active;
    synchronized void start(WorkbenchActionGateway gateway, String id, Consumer<PipelineLogLevelOutcome> level,
            Consumer<LogsOutcome> page, Consumer<String> ended) {
        cancel();
        AtomicBoolean stopped = new AtomicBoolean();
        active = worker.submit(() -> {
            level.accept(gateway.readPipelineLogLevel(id));
            LogsOutcome initial = gateway.readPipelineLogs(id, null);
            page.accept(initial);
            if (!(initial instanceof LogsOutcome.Found found)) return;
            gateway.followPipelineLogs(id, found.nextCursor(), new LogStream() {
                @Override public void lines(String ignored, java.util.List<RemoteLogLine> ignoredLines) { }
                @Override public void page(LogsOutcome.Found next) { page.accept(next); }
            }, () -> stopped.get() || Thread.currentThread().isInterrupted());
        });
    }
    synchronized void cancel() { if (active != null) { active.cancel(true); active = null; } }
    @Override public synchronized void close() { cancel(); worker.shutdownNow(); }
}
