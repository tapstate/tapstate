package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** The bounded latest-run projection. Ownership and run correlation are proved by the publishing store. */
public record PipelineRunProgress(
        String runId,
        long version,
        List<Activity> active,
        List<Milestone> milestones,
        LoadProgress initialLoad,
        PipelineRunStage failedStage,
        Mode readMode,
        boolean runFailed) {

    public record Activity(PipelineRunStage stage, Instant since) {}
    public record Milestone(PipelineRunStage stage, Instant reachedAt) {}
    public record LoadProgress(long landed, long total) {
        public LoadProgress {
            if (landed < 0 || total < landed) throw new IllegalArgumentException("invalid destination confirmation count");
        }
    }
    public enum Mode { SNAPSHOT_ONLY, SNAPSHOT_AND_CDC, CDC_ONLY }

    public PipelineRunProgress(String runId, long version, List<Activity> active, List<Milestone> milestones,
            LoadProgress initialLoad, PipelineRunStage failedStage) {
        this(runId, version, active, milestones, initialLoad, failedStage, null, false);
    }

    public PipelineRunProgress {
        Objects.requireNonNull(runId, "runId");
        if (version < 0) throw new IllegalArgumentException("invalid progress version");
        active = List.copyOf(active);
        milestones = List.copyOf(milestones);
    }

    public static PipelineRunProgress empty(String runId) {
        return new PipelineRunProgress(runId, 0, List.of(), List.of(), null, null);
    }

    public PipelineRunProgress enter(PipelineRunStage stage, Instant since) {
        Objects.requireNonNull(since, "since");
        if (runFailed) return this;
        var next = new ArrayList<>(active);
        var current = next.stream().filter(activity -> activity.stage() == stage).findFirst().orElse(null);
        if (current != null && current.since() != null && !since.isAfter(current.since())) return this;
        if (current != null) next.remove(current);
        next.add(new Activity(stage, since));
        return new PipelineRunProgress(runId, version + 1, next, milestones, initialLoad, null, readMode, false);
    }

    /** An exit with a start time can only close that specific occurrence of the activity. */
    public PipelineRunProgress leave(PipelineRunStage stage, Instant since, boolean failed) {
        var next = active.stream()
                .filter(activity -> activity.stage() != stage || since != null && !since.equals(activity.since()))
                .toList();
        if (active.equals(next)) return this;
        return new PipelineRunProgress(runId, version + 1, next, milestones, initialLoad,
                failed ? stage : failedStage, readMode, runFailed);
    }

    public PipelineRunProgress reach(PipelineRunStage stage, Instant at) {
        var next = new ArrayList<>(milestones);
        if (next.stream().noneMatch(milestone -> milestone.stage() == stage)) next.add(new Milestone(stage, at));
        List<Activity> remaining = switch (stage) {
            case ENGINE_RUNNING -> active.stream()
                    .filter(activity -> activity.stage() == PipelineRunStage.INITIAL_LOAD_IN_PROGRESS).toList();
            case INITIAL_LOAD_COMPLETED -> active.stream()
                    .filter(activity -> activity.stage() != PipelineRunStage.INITIAL_LOAD_IN_PROGRESS).toList();
            case RUN_COMPLETED -> List.of();
            default -> active;
        };
        if (remaining.equals(active) && next.equals(milestones)) return this;
        return new PipelineRunProgress(runId, version + 1, remaining, next, initialLoad, failedStage, readMode, runFailed);
    }

    /** Accept only an already-proved snapshot of this execution's required destination confirmations. */
    public PipelineRunProgress observeInitialLoad(InitialLoad load) {
        if (load == null || load.readMode().equals("cdc_only") || load.tables().isEmpty()) return this;
        return observeInitialLoad(new LoadProgress(load.landedTables(), load.tables().size()), load.observedAt());
    }

    public PipelineRunProgress observeInitialLoad(LoadProgress counts, Instant observedAt) {
        if (initialLoad != null && (counts.total() != initialLoad.total() || counts.landed() < initialLoad.landed())) {
            return this;
        }
        var withCounts = counts.equals(initialLoad) ? this
                : new PipelineRunProgress(runId, version + 1, active, milestones, counts, failedStage, readMode, runFailed);
        if (counts.landed() == counts.total()) {
            return withCounts.reach(PipelineRunStage.INITIAL_LOAD_COMPLETED, observedAt);
        }
        return initialLoad == null
                ? withCounts.enter(PipelineRunStage.INITIAL_LOAD_IN_PROGRESS, observedAt) : withCounts;
    }

    public PipelineRunProgress beginExecution(Mode mode) {
        return readMode == mode && active.isEmpty() && failedStage == null && !runFailed ? this
                : new PipelineRunProgress(runId, version + 1, List.of(), milestones, initialLoad, null, mode, false);
    }

    public PipelineRunProgress inactive() {
        return active.isEmpty() ? this
                : new PipelineRunProgress(runId, version + 1, List.of(), milestones, initialLoad, failedStage, readMode, runFailed);
    }

    public PipelineRunProgress markRunFailed() {
        return runFailed ? this
                : new PipelineRunProgress(runId, version + 1, List.of(), milestones, initialLoad, failedStage, readMode, true);
    }

    public boolean awaitingMilestones() {
        var reached = milestones.stream().map(Milestone::stage).collect(java.util.stream.Collectors.toSet());
        return !runFailed && failedStage == null && !reached.contains(PipelineRunStage.RUN_COMPLETED)
                && (!reached.contains(PipelineRunStage.ENGINE_RUNNING)
                || readMode != Mode.CDC_ONLY && !reached.contains(PipelineRunStage.INITIAL_LOAD_COMPLETED)
                || readMode != Mode.SNAPSHOT_ONLY && !reached.contains(PipelineRunStage.CDC_READY));
    }
}
