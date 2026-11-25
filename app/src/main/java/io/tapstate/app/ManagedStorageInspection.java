package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoReadBudget;
import io.tapstate.adapters.mongostore.MongoStructureInspection;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/** Runs the same read-only catalog inspection once per hour on the selected member. */
final class ManagedStorageInspection implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ManagedStorageInspection.class);
    private static final Duration INTERVAL = Duration.ofHours(1);
    private static final Duration DOMAIN_BUDGET = Duration.ofSeconds(20);

    record Domain(SystemCollections.Database role,
            Function<MongoReadBudget, List<MongoStructureInspection.Difference>> read) {}

    record Observation(SystemCollections.Database domain, Instant observedAt,
            List<MongoStructureInspection.Difference> differences, boolean complete) {}

    private final Clock clock;
    private final BooleanSupplier selected;
    private final List<Domain> domains;
    private final java.util.function.Consumer<Observation> publish;
    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1,
            task -> ShutdownAwait.daemon(task, "tapstate-storage-inspection"));

    ManagedStorageInspection(Clock clock, BooleanSupplier selected, List<Domain> domains,
            java.util.function.Consumer<Observation> publish) {
        this.clock = clock;
        this.selected = selected;
        this.domains = List.copyOf(domains);
        this.publish = publish;
        timer.setRemoveOnCancelPolicy(true);
    }

    void start() {
        long period = INTERVAL.toMillis();
        long delay = period - Math.floorMod(clock.millis(), period);
        timer.scheduleAtFixedRate(this::runOnce, delay, period, TimeUnit.MILLISECONDS);
    }

    void runOnce() {
        if (timer.isShutdown() || !selected.getAsBoolean()) return;
        for (var domain : domains) {
            if (timer.isShutdown() || !selected.getAsBoolean()) return;
            List<MongoStructureInspection.Difference> differences;
            boolean complete = true;
            try {
                differences = domain.read().apply(new MongoReadBudget(DOMAIN_BUDGET));
            } catch (TapstateException unavailable) {
                LOG.warn("Storage inspection unavailable [domain={}, code={}]", domain.role(), unavailable.code().code());
                differences = List.of();
                complete = false;
            }
            if (timer.isShutdown() || !selected.getAsBoolean()) return;
            if (!differences.isEmpty()) {
                LOG.warn("Storage structure differs [domain={}, differences={}, first={}]",
                        domain.role(), differences.size(), differences.getFirst().description());
            }
            try {
                publish.accept(new Observation(domain.role(), clock.instant(), differences, complete));
            } catch (TapstateException unavailable) {
                LOG.warn("Storage inspection report unavailable [domain={}, code={}]", domain.role(), unavailable.code().code());
            }
        }
    }

    @Override public void close() { timer.shutdownNow(); }

    void awaitStopped(long deadline) { ShutdownAwait.executor(timer, deadline); }
}
