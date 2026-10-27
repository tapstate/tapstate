package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Captures the exception that killed a pipeline's job, synchronously, at the moment a processor catches
 * it — rather than reconstructing one later from Jet's own terminal job state. Once a job's terminal
 * result is recorded, {@code JobResult.getFailureAsThrowable()} rebuilds a "mock throwable" (Hazelcast's
 * own javadoc) from the failure's text representation, with no cause chain; a coded exception a sink or
 * connector threw is unrecoverable through Jet's own API a short window after the job fails, since the
 * live in-memory result that still carries it is torn down once the job's terminal record is durable.
 * This registry is the fix: a processor records the real, unwrapped exception at its throw site before
 * Jet's tasklet machinery ever wraps or degrades it, and {@link Engine#failureOf} consults it first.
 *
 * <p>One instance lives per Hazelcast member, resolved through {@link HazelcastInstance#getUserContext()}
 * so both sides of the DAG boundary reach the same shared instance: {@link Engine} outside a job, and a
 * processor's {@code Processor.Context} inside one — the DAG itself must stay serializable, so nothing
 * captures a live reference to this registry across that boundary; each side resolves it fresh from the
 * one {@link HazelcastInstance} they both already hold.
 */
public final class JobFailureRegistry {

    private static final String USER_CONTEXT_KEY = JobFailureRegistry.class.getName();

    private final Map<String, Slot> failures = new ConcurrentHashMap<>();
    private final HazelcastInstance member;

    private JobFailureRegistry(HazelcastInstance member) { this.member = member; }

    /** The registry shared by this member's engine and every processor its jobs run, created once. */
    public static JobFailureRegistry of(HazelcastInstance member) {
        return (JobFailureRegistry) member.getUserContext()
                .computeIfAbsent(USER_CONTEXT_KEY, ignored -> new JobFailureRegistry(member));
    }

    /** Legacy-only recording never overwrites a scope-aware run's slot. */
    public void record(String pipelineId, Throwable cause) {
        failures.compute(pipelineId, (id, before) -> {
            if (before != null && before.identity != null) return before;
            Slot slot = before == null ? new Slot(pipelineId, 0, null) : before;
            slot.record(cause); return slot;
        });
    }

    public Optional<Throwable> get(String pipelineId) {
        Slot slot = failures.get(pipelineId);
        return slot != null && slot.identity == null ? slot.get() : Optional.empty();
    }

    public void clear(String pipelineId) {
        failures.computeIfPresent(pipelineId, (id, slot) -> {
            if (slot.identity != null) return slot;
            slot.retire(); return null;
        });
    }

    /** Resolves once at job initialization; every later callback writes only its captured slot. */
    public Recorder forJob(long jobId, com.hazelcast.jet.config.JobConfig config) {
        if (config == null || config.getName() == null) return Recorder.NONE;
        Object identity = config.getArgument(Engine.RUN_IDENTITY_ARGUMENT);
        if (identity == null) return new Recorder(this, config.getName(), null);
        if (!(identity instanceof java.io.Serializable immutable)) throw new io.tapstate.core.common.TapstateException(
                EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", config.getName()), null);
        return attach(config.getName(), jobId, immutable);
    }

    /** Only initialization/submission may attach; reads of an old handle must never recreate a slot. */
    Recorder attach(String pipelineId, long jobId, java.io.Serializable identity) {
        java.util.Objects.requireNonNull(identity, "identity");
        var answer = new java.util.concurrent.atomic.AtomicReference<>(Recorder.NONE);
        failures.compute(pipelineId, (id, previous) -> {
            // Keep the live-job check inside this PID's atomic update, including slow lookups.
            if (!currentJobMatches(pipelineId, jobId, identity)) return previous;
            if (previous != null && previous.identity != null && previous.jobId == jobId) {
                // A cleared job stays cleared even if another processor initializes late under it.
                if (previous.matches(jobId, identity)) answer.set(new Recorder(this, pipelineId, previous));
                return previous;
            }
            Slot next = new Slot(pipelineId, jobId, identity);
            if (previous != null) previous.retire();
            answer.set(new Recorder(this, pipelineId, next)); return next;
        });
        return answer.get();
    }

    /** Reads only a slot attached while the concrete job was proved; never resolves or reattaches it. */
    Optional<RetainedFailure> retainedRunFailure(String pipelineId, java.io.Serializable identity) {
        Slot slot = failures.get(pipelineId);
        return slot == null ? Optional.empty() : slot.retained(identity);
    }

    record RetainedFailure(Throwable cause) {}

    Recorder existing(String pipelineId, long jobId, java.io.Serializable identity) {
        Slot slot = failures.get(pipelineId);
        return slot != null && slot.matches(jobId, identity) ? new Recorder(this, pipelineId, slot) : Recorder.NONE;
    }

    /** Cancellation can precede processor initialization; retain one small marker for this exact job. */
    void retireRun(String pipelineId, long jobId, java.io.Serializable identity) {
        java.util.Objects.requireNonNull(identity, "identity");
        failures.compute(pipelineId, (id, current) -> {
            if (current != null && current.jobId == jobId && java.util.Objects.equals(current.identity, identity)) {
                current.retire(); return current;
            }
            // An earlier job can still own the map's slot while this job has not initialized a processor.
            // A foreign slot is replaced only when the concrete current Jet job proves this cancellation's pair.
            if (current != null && !currentJobMatches(pipelineId, jobId, identity)) return current;
            if (current != null) current.retire();
            Slot retired = new Slot(pipelineId, jobId, identity); retired.retire(); return retired;
        });
    }

    private boolean currentJobMatches(String pipelineId, long jobId, java.io.Serializable identity) {
        try {
            var current = member.getJet().getJob(pipelineId);
            return current != null && current.getId() == jobId && identity.equals(current.getConfig().getArgument(Engine.RUN_IDENTITY_ARGUMENT));
        } catch (com.hazelcast.core.HazelcastInstanceNotActiveException stopped) {
            // No slot may be adopted through a member that can no longer prove the named job.
            return false;
        }
    }

    private void clear(Slot original) {
        retireRun(original.pipelineId, original.jobId, original.identity);
        original.retire();
    }

    /** A bounded member-local recording handle; it is never stored on the DAG or reattached by a callback. */
    public static final class Recorder {
        private static final Recorder NONE = new Recorder(null, null, null);
        private final JobFailureRegistry registry;
        private final String pipelineId;
        private final Slot slot;
        private Recorder(JobFailureRegistry registry, String pipelineId, Slot slot) {
            this.registry = registry; this.pipelineId = pipelineId; this.slot = slot;
        }
        public void record(Throwable failure) {
            if (slot != null) slot.record(failure);
            else if (registry != null) registry.record(pipelineId, failure);
        }
        Optional<Throwable> get() {
            return slot != null ? slot.get() : registry == null ? Optional.empty() : registry.get(pipelineId);
        }
        void clear() {
            if (slot != null) registry.clear(slot);
            else if (registry != null) registry.clear(pipelineId);
        }
    }

    private static final class Slot {
        final String pipelineId;
        final long jobId;
        final java.io.Serializable identity;
        private Throwable cause;
        private boolean retired;
        Slot(String pipelineId, long jobId, java.io.Serializable identity) {
            this.pipelineId = pipelineId; this.jobId = jobId; this.identity = identity;
        }
        synchronized boolean matches(long expectedJob, java.io.Serializable expectedIdentity) {
            return !retired && jobId == expectedJob && java.util.Objects.equals(identity, expectedIdentity);
        }
        synchronized Optional<RetainedFailure> retained(java.io.Serializable expectedIdentity) {
            return !retired && jobId != 0 && identity != null && identity.equals(expectedIdentity)
                    ? Optional.of(new RetainedFailure(cause)) : Optional.empty();
        }
        synchronized void record(Throwable failure) { if (!retired) cause = java.util.Objects.requireNonNull(failure, "failure"); }
        synchronized Optional<Throwable> get() { return retired ? Optional.empty() : Optional.ofNullable(cause); }
        synchronized void retire() { retired = true; cause = null; }
    }
}
