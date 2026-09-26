package io.tapstate.runtime.srs;

import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsLogStore;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Cuts only confirmed sequences of each table in the ring generation this capture owns. */
final class PerTableLogTrimmer {

    private final Supplier<Collection<ConsumerOffset>> consumers;
    private final SrsLogStore log;
    private final long epoch;
    private final Map<String, Route> routes = new LinkedHashMap<>();

    PerTableLogTrimmer(Supplier<Collection<ConsumerOffset>> consumers, SrsLogStore log, long epoch) {
        this.consumers = Objects.requireNonNull(consumers, "consumers");
        this.log = Objects.requireNonNull(log, "log");
        if (epoch < 1) {
            throw new IllegalArgumentException("ring epoch must be positive");
        }
        this.epoch = epoch;
    }

    synchronized void observed(String table, String ring, long lastSeq) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(ring, "ring");
        if (lastSeq < 0) {
            return;
        }
        Route route = routes.computeIfAbsent(table, ignored -> new Route(ring));
        if (!route.ring.equals(ring)) {
            throw new IllegalStateException("one table cannot use two ring names in one trim generation");
        }
        route.lastAdmitted = Math.max(route.lastAdmitted, lastSeq);
    }

    synchronized void trim() {
        trim(consumers.get());
    }

    synchronized void trim(Collection<ConsumerOffset> current) {
        if (routes.isEmpty()) {
            return;
        }
        Objects.requireNonNull(current, "current");
        if (current.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Route> entry : routes.entrySet()) {
            Route route = entry.getValue();
            long through = Math.min(route.lastAdmitted, confirmedThrough(current, entry.getKey(), epoch));
            if (through <= route.lastTrimmed) {
                continue;
            }
            log.trim(route.ring, through, epoch);
            route.lastTrimmed = through;
        }
    }

    /** An unknown selection or a cursor from another generation cannot prove this table is done. */
    static long confirmedThrough(Collection<ConsumerOffset> consumers, String table, long epoch) {
        long slowest = Long.MAX_VALUE;
        boolean selected = false;
        for (ConsumerOffset consumer : consumers) {
            if (consumer.selectedTables() == null) {
                return -1L;
            }
            if (!consumer.selectedTables().contains(table)) {
                continue;
            }
            selected = true;
            if (!Objects.equals(consumer.selectedTablesEpoch(), epoch)) {
                return -1L;
            }
            Long done = consumer.ringDoneThrough().get(table);
            if (done == null) {
                return -1L;
            }
            slowest = Math.min(slowest, done);
        }
        return selected ? slowest : -1L;
    }

    private static final class Route {
        private final String ring;
        private long lastAdmitted = -1L;
        private long lastTrimmed = -1L;

        private Route(String ring) {
            this.ring = ring;
        }
    }
}
