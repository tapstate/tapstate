package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;

import java.util.Objects;

/** One bounded lifecycle handoff beside its actual checkpoint. */
public record StopReservation(
        String pipelineId, String token, long sourceEpoch, long reservedEpoch,
        DesiredState originalDesired, Source source, Phase phase, CounterPolicy counterPolicy,
        StopAuthority writerAuthority, Successor successor, int formatVersion) {

    public static final int CURRENT_FORMAT = 16;
    public static final int LEGACY_FORMAT = 15;

    public enum Phase { STOPPING, REPLACEMENT_PENDING, SUCCESSOR_ADMITTED, SUCCESSOR_BOUND }

    public enum CounterPolicy {
        CONTINUE, RESET;

        public static CounterPolicy freeze(CheckpointDoc actual, DesiredState intent) {
            Objects.requireNonNull(actual, "actual"); Objects.requireNonNull(intent, "intent");
            return StateJson.parse(actual.stateJson()) == PipelineState.PAUSED
                    && intent.targetState() == PipelineState.RUNNING
                    && intent.rebuiltAtStateEpoch() == null && !intent.purgeState() ? CONTINUE : RESET;
        }
    }

    /** Immutable factual source; absence never invents a native job or metric scope. */
    public record Source(String clusterId, ObservationStore.Scope scope, JobIdentity oldJob) {
        public Source {
            clusterId = required(clusterId, "clusterId");
            if (oldJob != null && (scope == null || !clusterId.equals(oldJob.clusterId()))) {
                throw new IllegalArgumentException("an old job requires its real source scope and cluster");
            }
        }
    }

    /** One admitted target; its real native identity is installed only after binding. */
    public record Successor(ObservationStore.Scope scope, String submissionBootId, JobIdentity job) {
        public Successor {
            Objects.requireNonNull(scope, "scope");
            submissionBootId = required(submissionBootId, "submissionBootId");
            if (job != null && !submissionBootId.equals(job.bootId())) {
                throw new IllegalArgumentException("bound job boot differs from its admission");
            }
        }
    }

    public StopReservation {
        pipelineId = required(pipelineId, "pipelineId"); token = required(token, "token");
        Objects.requireNonNull(originalDesired, "originalDesired");
        Objects.requireNonNull(source, "source"); Objects.requireNonNull(phase, "phase");
        if (token.length() > 256 || sourceEpoch < 0 || reservedEpoch <= sourceEpoch
                || !pipelineId.equals(originalDesired.pipelineId())
                || formatVersion != CURRENT_FORMAT && formatVersion != LEGACY_FORMAT
                || formatVersion == CURRENT_FORMAT && counterPolicy == null
                || counterPolicy == CounterPolicy.CONTINUE && (originalDesired.targetState() != PipelineState.RUNNING
                        || originalDesired.rebuiltAtStateEpoch() != null || originalDesired.purgeState())
                || formatVersion == LEGACY_FORMAT && (counterPolicy != null || phase != Phase.STOPPING || successor != null)
                || writerAuthority == null && (source.scope() != null || source.oldJob() != null)
                || writerAuthority != null && !source.clusterId().equals(writerAuthority.clusterId())
                || writerAuthority != null && writerAuthority.claim() != null
                        && !pipelineId.equals(writerAuthority.claim().key().resourceId())) {
            throw new IllegalArgumentException("handoff identities, format or epochs disagree");
        }
        boolean occupied = phase == Phase.SUCCESSOR_ADMITTED || phase == Phase.SUCCESSOR_BOUND;
        if (occupied != (successor != null)
                || phase != Phase.STOPPING && originalDesired.targetState() != PipelineState.RUNNING
                || occupied && writerAuthority == null
                || occupied && successor.scope().executionGeneration() != writerAuthority.executionGeneration()
                || phase == Phase.STOPPING && source.scope() != null
                        && (writerAuthority == null || source.scope().executionGeneration() != writerAuthority.executionGeneration())
                || phase == Phase.SUCCESSOR_ADMITTED && successor.job() != null
                || phase == Phase.SUCCESSOR_BOUND && successor.job() == null
                || successor != null && source.scope() != null
                        && !source.scope().pipelineIncarnationId().equals(successor.scope().pipelineIncarnationId())
                || successor != null && successor.job() != null
                        && !source.clusterId().equals(successor.job().clusterId())) {
            throw new IllegalArgumentException("handoff phase and successor disagree");
        }
    }

    /** Exact legacy construction for callers that have not yet supplied a frozen policy. */
    public StopReservation(String pipelineId, String token, long sourceEpoch, long reservedEpoch,
            DesiredState desired, Subject subject) {
        this(pipelineId, token, sourceEpoch, reservedEpoch, desired, legacySource(subject), Phase.STOPPING,
                null, legacyAuthority(subject), null, LEGACY_FORMAT);
    }

    public static StopReservation stopping(String pipelineId, String token, long sourceEpoch,
            DesiredState desired, Source source, CounterPolicy policy, StopAuthority authority) {
        return new StopReservation(pipelineId, token, sourceEpoch, Math.incrementExact(sourceEpoch), desired,
                source, Phase.STOPPING, policy, authority, null, CURRENT_FORMAT);
    }

    public boolean legacy() { return formatVersion == LEGACY_FORMAT; }
    public StopAuthority authorityOrNull() { return writerAuthority; }

    /** Rebinding changes only the completing writer and current fence, never the immutable source. */
    public StopReservation rebind(StopAuthority nextAuthority, long nextEpoch) {
        Objects.requireNonNull(nextAuthority, "nextAuthority");
        if (nextEpoch <= reservedEpoch) { throw new IllegalArgumentException("rebinding must advance the epoch"); }
        return new StopReservation(pipelineId, token, sourceEpoch, nextEpoch, originalDesired, source, phase,
                counterPolicy, nextAuthority, successor, formatVersion);
    }

    /** Compatibility is available only when current authority still describes the factual old execution. */
    public Subject subject() {
        if (successor != null || source.scope() != null && (writerAuthority == null
                || source.scope().executionGeneration() != writerAuthority.executionGeneration())) {
            throw new IllegalStateException("a phased successor cannot be projected as an old stop subject");
        }
        return source.oldJob() == null ? new NoJob(source.clusterId(), writerAuthority)
                : new ExistingJob(source.scope().pipelineIncarnationId(), source.scope().executionGeneration(),
                        source.oldJob(), writerAuthority);
    }

    public HandoffIdentity handoffIdentity() {
        if (phase != Phase.SUCCESSOR_BOUND) { throw new IllegalStateException("handoff target is not bound"); }
        return new HandoffIdentity(pipelineId, token, counterPolicy, source.scope(), successor.scope(), successor.job());
    }

    public sealed interface Subject permits NoJob, ExistingJob { }

    public record NoJob(String clusterId, StopAuthority knownAuthority) implements Subject {
        public NoJob {
            clusterId = required(clusterId, "clusterId");
            if (knownAuthority != null && !clusterId.equals(knownAuthority.clusterId())) {
                throw new IllegalArgumentException("no-job authority belongs to another cluster");
            }
        }
    }

    public record ExistingJob(String pipelineIncarnationId, long executionGeneration,
            JobIdentity oldJob, StopAuthority authority) implements Subject {
        public ExistingJob {
            pipelineIncarnationId = required(pipelineIncarnationId, "pipelineIncarnationId");
            Objects.requireNonNull(oldJob, "oldJob"); Objects.requireNonNull(authority, "authority");
            if (executionGeneration < 1 || executionGeneration != authority.executionGeneration()
                    || !oldJob.clusterId().equals(authority.clusterId())) {
                throw new IllegalArgumentException("old job and source authority disagree");
            }
        }
    }

    public record JobIdentity(String clusterId, long jobId, String bootId) {
        public JobIdentity {
            clusterId = required(clusterId, "clusterId"); bootId = required(bootId, "bootId");
        }
    }

    private static Source legacySource(Subject subject) {
        Objects.requireNonNull(subject, "subject");
        return switch (subject) {
            case NoJob absent -> new Source(absent.clusterId(), null, null);
            case ExistingJob old -> new Source(old.oldJob().clusterId(),
                    new ObservationStore.Scope(old.pipelineIncarnationId(), old.executionGeneration()), old.oldJob());
        };
    }

    private static StopAuthority legacyAuthority(Subject subject) {
        return switch (subject) {
            case NoJob absent -> absent.knownAuthority();
            case ExistingJob old -> old.authority();
        };
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) { throw new IllegalArgumentException(name + " must not be blank"); }
        return value;
    }
}
