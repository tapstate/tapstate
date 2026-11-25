package io.tapstate.app;

import io.tapstate.control.core.CloudStatusReporter;
import io.tapstate.core.common.TapstateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** Owns one outbound heartbeat task, separate from Pipeline threads and only after startup completes. */
final class CloudStatusLifecycle
        implements ApplicationListener<ApplicationReadyEvent>, ApplicationContextAware, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CloudStatusLifecycle.class);
    private static final long HEARTBEAT_SECONDS = 30;

    private final CloudStatusReporter reporter;
    private final Supplier<ScheduledExecutorService> executors;
    private final BiConsumer<Thread, Throwable> defects;
    private ApplicationContext owner;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> task;
    private volatile boolean closed;

    CloudStatusLifecycle(CloudStatusReporter reporter) {
        this(reporter, () -> Executors.newSingleThreadScheduledExecutor(work -> {
            Thread thread = new Thread(work, "tapstate-cloud-status");
            thread.setDaemon(true);
            return thread;
        }));
    }

    CloudStatusLifecycle(CloudStatusReporter reporter, Supplier<ScheduledExecutorService> executors) {
        this(reporter, executors, (worker, defect) ->
                worker.getUncaughtExceptionHandler().uncaughtException(worker, defect));
    }

    CloudStatusLifecycle(CloudStatusReporter reporter, Supplier<ScheduledExecutorService> executors,
            BiConsumer<Thread, Throwable> defects) {
        this.reporter = reporter;
        this.executors = Objects.requireNonNull(executors, "executors");
        this.defects = Objects.requireNonNull(defects, "defects");
    }

    boolean enabled() {
        return reporter != null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        owner = Objects.requireNonNull(applicationContext, "applicationContext");
    }

    @Override
    public synchronized void onApplicationEvent(ApplicationReadyEvent event) {
        if (event.getApplicationContext() != owner || reporter == null || closed || executor != null) {
            return;
        }
        executor = Objects.requireNonNull(executors.get(), "executor");
        // Fixed delay and one thread keep reports non-overlapping even while the SDK performs its own
        // bounded transport/retry. No second retry loop is introduced at the assembly boundary.
        task = executor.scheduleWithFixedDelay(this::report, 0, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    private void report() {
        if (closed) {
            return;
        }
        try {
            reporter.report();
        } catch (TapstateException unavailable) {
            // Provider parameters and cause chains can carry credentials. Keep only the canonical code
            // here; the adapter owns its safe diagnostic, while a later periodic tick can report again.
            LOG.warn("Cloud status report failed [{}]", unavailable.code().code());
        } catch (RuntimeException | Error defect) {
            // Periodic executors keep a thrown defect inside the Future and silently suppress later
            // ticks. Surface the original programmer failure through the worker's uncaught handler,
            // then retain that exact failure in the Future; never reclassify it as provider availability.
            try {
                defects.accept(Thread.currentThread(), defect);
            } catch (RuntimeException | Error observerFailure) {
                if (observerFailure != defect) defect.addSuppressed(observerFailure);
            }
            throw defect;
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (task != null) {
            task.cancel(true);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
