package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupScope;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Bounded, process-owned display evidence; it never grants preparation or authentication authority. */
public final class StorageInitializationProgress {
    public enum Stage { SAVE_SETTINGS, PREPARE_STORAGE, START_RUNTIME, PUBLISH_READY }
    public enum State { PENDING, RUNNING, COMPLETE, FAILED }
    public record Milestone(Stage id, State state, Long elapsedMillis, String errorCode) {}
    public record View(String operationId, long generation, String attemptId, State state,
            Instant observedAt, long elapsedMillis, boolean restored, List<Milestone> milestones,
            Map<StorageSetupService.Layer, Instant> connectionChecks) {
        public View {
            milestones = List.copyOf(milestones);
            connectionChecks = Map.copyOf(connectionChecks);
        }
    }

    private static final class Timing {
        State state = State.PENDING;
        long started;
        Long elapsed;
        String errorCode;
    }
    private final Clock clock;
    private final LongSupplier ticker;
    private final EnumMap<Stage, Timing> timings = new EnumMap<>(Stage.class);
    private final EnumMap<StorageSetupService.Layer, Instant> checks = new EnumMap<>(StorageSetupService.Layer.class);
    private StorageSetupScope scope;
    private String attemptId;
    private State state;
    private long started;
    private long finished;
    private boolean restored;

    public StorageInitializationProgress(Clock clock) { this(clock, System::nanoTime); }
    StorageInitializationProgress(Clock clock, LongSupplier ticker) {
        this.clock = Objects.requireNonNull(clock);
        this.ticker = Objects.requireNonNull(ticker);
    }

    /** Completed prefixes must come from the original durable binding/setup phase, never a browser hint. */
    public synchronized void begin(StorageSetupScope original, int completedPrefix, boolean restored) {
        Objects.requireNonNull(original);
        if (completedPrefix < 0 || completedPrefix > 2) throw new IllegalArgumentException("invalid initialization prefix");
        boolean same = original.equals(scope);
        if (same && (state == State.RUNNING || state == State.COMPLETE)) return;
        long now = ticker.getAsLong();
        if (!same) started = now;
        if (!same || completedPrefix == 0) checks.clear();
        var previous = new EnumMap<>(timings);
        timings.clear();
        for (Stage stage : Stage.values()) {
            var timing = new Timing();
            if (stage.ordinal() < completedPrefix) {
                timing.state = State.COMPLETE;
                Timing known = same ? previous.get(stage) : null;
                timing.elapsed = known != null && known.state == State.COMPLETE ? known.elapsed : null;
            } else if (stage.ordinal() == completedPrefix) {
                timing.state = State.RUNNING;
                timing.started = now;
            }
            timings.put(stage, timing);
        }
        scope = original;
        attemptId = UUID.randomUUID().toString();
        state = State.RUNNING;
        this.restored = same ? this.restored : restored;
    }

    public synchronized void checked(StorageSetupScope original, StorageSetupService.Layer layer) {
        if (current(original)) checks.put(layer, clock.instant());
    }

    public synchronized void started(StorageSetupScope original, Stage stage) {
        if (!current(original)) return;
        Timing timing = timings.get(stage);
        if (timing.state == State.RUNNING || timing.state == State.COMPLETE) return;
        for (Stage previous : Stage.values())
            if (previous.ordinal() < stage.ordinal() && timings.get(previous).state != State.COMPLETE)
                throw new IllegalStateException("earlier initialization milestone is incomplete");
        timing.state = State.RUNNING;
        timing.started = ticker.getAsLong();
    }

    public synchronized void completed(StorageSetupScope original, Stage stage) {
        if (!current(original)) return;
        Timing timing = timings.get(stage);
        if (timing.state == State.COMPLETE) return;
        if (timing.state != State.RUNNING) throw new IllegalStateException("initialization milestone was not started");
        long now = ticker.getAsLong();
        timing.elapsed = elapsed(timing.started, now);
        timing.state = State.COMPLETE;
        if (stage == Stage.PUBLISH_READY) {
            state = State.COMPLETE;
            finished = now;
        }
    }

    public synchronized void failed(StorageSetupScope original, Throwable failure) {
        if (current(original)) fail(failure);
    }

    public synchronized void failed(Throwable failure) {
        if (state == State.RUNNING) fail(failure);
    }

    private void fail(Throwable failure) {
        long now = ticker.getAsLong();
        boolean recorded = false;
        for (Timing timing : timings.values()) {
            if (timing.state != State.RUNNING) continue;
            timing.state = State.FAILED;
            timing.elapsed = elapsed(timing.started, now);
            timing.errorCode = failure instanceof TapstateException coded ? coded.code().code() : null;
            recorded = true;
        }
        if (!recorded) {
            for (Timing timing : timings.values()) {
                if (timing.state != State.PENDING) continue;
                timing.state = State.FAILED;
                timing.errorCode = failure instanceof TapstateException coded ? coded.code().code() : null;
                break;
            }
        }
        state = State.FAILED;
        finished = now;
    }

    /** No IO or long-running action is performed while holding this monitor. */
    public synchronized Optional<View> snapshot(StorageSetupScope original) {
        if (!original.equals(scope)) return Optional.empty();
        long now = ticker.getAsLong();
        var milestones = new ArrayList<Milestone>();
        for (Stage stage : Stage.values()) {
            Timing timing = timings.get(stage);
            milestones.add(new Milestone(stage, timing.state,
                    timing.state == State.RUNNING ? Long.valueOf(elapsed(timing.started, now)) : timing.elapsed, timing.errorCode));
        }
        return Optional.of(new View(scope.operationId(), scope.generation(), attemptId, state, clock.instant(),
                elapsed(started, state == State.RUNNING ? now : finished), restored, milestones, checks));
    }

    private boolean current(StorageSetupScope original) { return original.equals(scope) && state == State.RUNNING; }
    private static long elapsed(long from, long to) { return TimeUnit.NANOSECONDS.toMillis(Math.max(0, to - from)); }
}
