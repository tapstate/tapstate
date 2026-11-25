package io.tapstate.runtime.engine;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** One diagnostic per original execution, local member and close stage. */
public final class StopWaitLog {
    public record Site(String pipeline, String execution, String node, String operation) {
        public Site at(String nextOperation) { return new Site(pipeline, execution, node, nextOperation); }
    }

    record Notice(Site site, boolean recovered, long elapsedMillis, String stack) { }

    private static final System.Logger LOG = System.getLogger(StopWaitLog.class.getName());
    private static final StopWaitLog SHARED = new StopWaitLog(Duration.ofSeconds(30), notice -> {
        Site site = notice.site();
        if (notice.recovered()) {
            LOG.log(System.Logger.Level.INFO,
                    "Pipeline {0}, execution {1}, node {2}: {3} finished after {4} ms",
                    site.pipeline(), site.execution(), site.node(), site.operation(), notice.elapsedMillis());
        } else {
            LOG.log(System.Logger.Level.WARNING,
                    "Pipeline {0}, execution {1}, node {2}: {3} still waiting after {4} ms; stack: {5}",
                    site.pipeline(), site.execution(), site.node(), site.operation(), notice.elapsedMillis(), notice.stack());
        }
    });

    private static final class Active {
        private final long started = System.nanoTime();
        private final Set<Thread> threads = new LinkedHashSet<>();
        private ScheduledFuture<?> alarm;
        private boolean warned;
        private boolean failed;
    }

    private final Map<Site, Active> active = new HashMap<>();
    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon(true).name("tapstate-stop-diagnostic").factory());
    private final long allowance;
    private final Consumer<Notice> notices;

    StopWaitLog(Duration threshold, Consumer<Notice> notices) {
        timer.setRemoveOnCancelPolicy(true);
        timer.setKeepAliveTime(1, TimeUnit.MINUTES);
        timer.allowCoreThreadTimeOut(true);
        this.allowance = threshold.toNanos();
        this.notices = notices;
    }

    public static void around(Site site, Runnable action) {
        if (site == null) action.run();
        else SHARED.track(site, action);
    }

    void track(Site site, Runnable action) {
        Thread thread = Thread.currentThread();
        Active entry;
        synchronized (active) {
            entry = active.computeIfAbsent(site, ignored -> new Active());
            entry.threads.add(thread);
            if (entry.alarm == null) entry.alarm = timer.schedule(() -> warn(site, entry), allowance, TimeUnit.NANOSECONDS);
        }
        boolean succeeded = false;
        try {
            action.run();
            succeeded = true;
        } finally {
            synchronized (active) {
                entry.failed |= !succeeded;
                entry.threads.remove(thread);
                if (entry.threads.isEmpty()) {
                    active.remove(site);
                    entry.alarm.cancel(false);
                    if (entry.warned && !entry.failed) {
                        var notice = new Notice(site, true, elapsed(entry), "");
                        timer.execute(() -> notices.accept(notice));
                    }
                }
            }
        }
    }

    private void warn(Site site, Active entry) {
        Thread thread;
        synchronized (active) {
            if (active.get(site) != entry) return;
            entry.warned = true;
            thread = entry.threads.iterator().next();
        }
        var stack = Arrays.stream(thread.getStackTrace()).limit(10).map(StackTraceElement::toString)
                .collect(Collectors.joining(" <- "));
        notices.accept(new Notice(site, false, elapsed(entry), stack));
    }

    private static long elapsed(Active entry) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - entry.started);
    }
}
