package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Selects two remote consumers while retaining every local consumer that must be cleared. */
final class CaptureTakeoverCandidates {
    private final String holder;
    private final String remote;
    private final int limit;
    private final List<String> local = new ArrayList<>();
    private final List<String> joiners = new ArrayList<>();
    private final Map<String, String> placements = new LinkedHashMap<>();

    CaptureTakeoverCandidates(String holder, String remote, int limit) {
        this.holder = holder;
        this.remote = remote;
        this.limit = limit;
    }

    boolean add(String pipeline, String driver, String trace) {
        if (placements.containsKey(pipeline) || placements.size() >= limit || ready()) {
            throw new AssertionError("candidate selection is already complete or repeats " + pipeline);
        }
        if (!holder.equals(driver) && !remote.equals(driver)) {
            throw new AssertionError("unexpected controller for " + pipeline + ": " + driver + "; " + trace);
        }
        placements.put(pipeline, trace);
        if (holder.equals(driver)) {
            local.add(pipeline);
            return false;
        }
        joiners.add(pipeline);
        return true;
    }

    boolean ready() { return joiners.size() == 2; }

    List<String> onTheHolder() { return List.copyOf(local); }

    String firstJoiner() { requireReady(); return joiners.get(0); }

    String remainingPipeline() { requireReady(); return joiners.get(1); }

    void requireReady() {
        if (!ready()) {
            throw new AssertionError("fewer than two pipelines of " + limit + " landed on the member "
                    + "not holding the capture; capture holder=" + holder + "; placements=" + placements);
        }
    }
}
