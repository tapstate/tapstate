package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Everything a start check may look at, and nothing it could write through: the definition being
 * started and its content hash, how the start would load each target, a probe for what a target holds,
 * and the time by which every check has to have answered.
 */
public final class StartCheckContext {

    private final String pipelineId;
    private final StartIntent intent;
    private final PipelineResource definition;
    private final String contentHash;
    private final StartPlan plan;
    private final TapstateException planUnavailable;
    private final TargetProbe probe;
    private final ExecutorService executor;
    private final Clock clock;
    private final Instant deadline;
    private final Duration perTarget;
    private final Function<TapstateException, String> describe;

    StartCheckContext(String pipelineId, StartIntent intent, PipelineResource definition, String contentHash,
            StartPlan plan, TapstateException planUnavailable, TargetProbe probe, ExecutorService executor,
            Clock clock, Instant deadline, Duration perTarget, Function<TapstateException, String> describe) {
        this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
        this.intent = Objects.requireNonNull(intent, "intent");
        this.definition = Objects.requireNonNull(definition, "definition");
        this.contentHash = Objects.requireNonNull(contentHash, "contentHash");
        if ((plan == null) == (planUnavailable == null)) {
            throw new IllegalArgumentException("a start has either a plan or the reason it has none");
        }
        this.plan = plan;
        this.planUnavailable = planUnavailable;
        this.probe = Objects.requireNonNull(probe, "probe");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        this.perTarget = Objects.requireNonNull(perTarget, "perTarget");
        this.describe = Objects.requireNonNull(describe, "describe");
    }

    public String pipelineId() {
        return pipelineId;
    }

    public StartIntent intent() {
        return intent;
    }

    /** The definition the start would run, as stored or as a person's chosen change would leave it. */
    public PipelineResource definition() {
        return definition;
    }

    /** The content hash of the stored definition this evaluation is based on. */
    public String contentHash() {
        return contentHash;
    }

    /**
     * How the start would load each target. Throws the coded error that kept it from being worked out --
     * a source never discovered, say -- which makes the asking check's finding one it could not evaluate.
     */
    public StartPlan plan() {
        if (planUnavailable != null) {
            throw planUnavailable;
        }
        return plan;
    }

    /** By when every check has to have answered. */
    public Instant deadline() {
        return deadline;
    }

    public Clock clock() {
        return clock;
    }

    /**
     * Probes every listed target at once, each for no longer than its own share of time and none past
     * the deadline. A target that could not be probed -- a coded refusal, or no answer in time -- comes
     * back as the reason, never as an empty target.
     */
    public Map<String, Probed> probe(List<StartPlan.Entry> targets) {
        Map<String, Future<TargetProbe.TargetRows>> pending = new LinkedHashMap<>();
        for (StartPlan.Entry entry : targets) {
            String connection = entry.target().connection();
            String table = entry.target().table();
            pending.putIfAbsent(entry.coordinate(), executor.submit(() -> probe.rows(connection, table)));
        }
        Map<String, Probed> probed = new LinkedHashMap<>();
        List<Future<?>> abandoned = new ArrayList<>();
        for (Map.Entry<String, Future<TargetProbe.TargetRows>> each : pending.entrySet()) {
            probed.put(each.getKey(), await(each.getValue(), abandoned));
        }
        abandoned.forEach(future -> future.cancel(true));
        return probed;
    }

    private Probed await(Future<TargetProbe.TargetRows> future, List<Future<?>> abandoned) {
        Duration untilDeadline = Duration.between(clock.instant(), deadline);
        Duration wait = untilDeadline.compareTo(perTarget) < 0 ? untilDeadline : perTarget;
        try {
            return new Probed.Rows(future.get(Math.max(0L, wait.toMillis()), TimeUnit.MILLISECONDS));
        } catch (TimeoutException late) {
            abandoned.add(future);
            return new Probed.Unknown("it did not answer within " + perTarget.toMillis() + " ms", null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            abandoned.add(future);
            return new Probed.Unknown("the start was interrupted while it was being asked", null);
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof TapstateException coded) {
                return new Probed.Unknown(describe.apply(coded), coded.code().code());
            }
            if (failed.getCause() instanceof RuntimeException defect) {
                throw defect;
            }
            if (failed.getCause() instanceof Error fatal) {
                throw fatal;
            }
            throw new IllegalStateException(failed.getCause());
        }
    }

    /** What probing one target came back with. */
    public sealed interface Probed {

        /** What the target holds. */
        record Rows(TargetProbe.TargetRows rows) implements Probed {
        }

        /**
         * That it could not be told, and why.
         *
         * @param reason how a person reads why
         * @param code   the coded refusal's code, null when there was none (no answer in time)
         */
        record Unknown(String reason, String code) implements Probed {
        }
    }
}
