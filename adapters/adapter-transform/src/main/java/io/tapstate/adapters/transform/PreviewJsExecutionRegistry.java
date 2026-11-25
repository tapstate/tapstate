package io.tapstate.adapters.transform;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeoutException;
import org.graalvm.polyglot.Context;

/** Tracks preview-only JavaScript contexts so request cancellation can interrupt guest execution. */
final class PreviewJsExecutionRegistry {

    private static final Duration INTERRUPT_TIMEOUT = Duration.ofMillis(250);
    private static final ConcurrentHashMap<String, Execution> EXECUTIONS = new ConcurrentHashMap<>();

    private PreviewJsExecutionRegistry() {
    }

    static void register(String executionId, Context context) {
        if (executionId == null) {
            return;
        }
        Execution execution = EXECUTIONS.computeIfAbsent(executionId, ignored -> new Execution());
        boolean cancel;
        boolean close;
        synchronized (execution) {
            cancel = execution.cancelled;
            close = execution.finished;
            if (!cancel && !close) {
                execution.contexts.add(context);
            }
        }
        if (close) {
            close(context);
        } else if (cancel) {
            close(context);
        }
    }

    static void unregister(String executionId, Context context) {
        if (executionId == null) {
            return;
        }
        Execution execution = EXECUTIONS.get(executionId);
        if (execution != null) {
            synchronized (execution) {
                execution.contexts.remove(context);
            }
        }
    }

    static void cancel(String executionId) {
        Execution execution = EXECUTIONS.computeIfAbsent(executionId, ignored -> new Execution());
        ArrayList<Context> contexts;
        synchronized (execution) {
            if (execution.finished || execution.cancelled) {
                return;
            }
            execution.cancelled = true;
            contexts = new ArrayList<>(execution.contexts);
        }
        contexts.forEach(context -> ForkJoinPool.commonPool().execute(() -> interrupt(context)));
    }

    static void finish(String executionId) {
        Execution execution = EXECUTIONS.get(executionId);
        if (execution == null) {
            return;
        }
        ArrayList<Context> contexts;
        synchronized (execution) {
            if (execution.finished) {
                return;
            }
            execution.finished = true;
            contexts = new ArrayList<>(execution.contexts);
            execution.contexts.clear();
        }
        contexts.forEach(PreviewJsExecutionRegistry::close);
        EXECUTIONS.remove(executionId, execution);
    }

    private static void interrupt(Context context) {
        try {
            context.interrupt(INTERRUPT_TIMEOUT);
        } catch (TimeoutException stillExecuting) {
            close(context);
        } catch (IllegalStateException alreadyClosed) {
            // The processor may close its context while the cancellation request is in flight.
        }
    }

    private static void close(Context context) {
        try {
            context.close();
        } catch (IllegalStateException executing) {
            try {
                context.close(true);
            } catch (RuntimeException alreadyClosed) {
                // The preview is already ending; there is no useful recovery after forced closure.
            }
        } catch (RuntimeException alreadyClosed) {
            // Closing an interrupted context may report the guest cancellation that caused it to exit.
        }
    }

    private static final class Execution {
        private final Set<Context> contexts = ConcurrentHashMap.newKeySet();
        private boolean cancelled;
        private boolean finished;
    }
}
