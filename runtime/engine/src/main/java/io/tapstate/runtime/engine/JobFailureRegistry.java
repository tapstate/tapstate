package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

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

    private final Map<String, Slots> failures = new ConcurrentHashMap<>();
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
            if (before != null && before.legacy() == null) return before;
            Slot slot = before == null ? new Slot(pipelineId, 0, null) : before.legacy();
            slot.capture(cause);
            return before == null ? new Slots(slot, null) : before;
        });
    }

    public Optional<Throwable> get(String pipelineId) {
        Slots slots = failures.get(pipelineId);
        Slot legacy = slots == null ? null : slots.legacy();
        return legacy == null ? Optional.empty() : legacy.get();
    }

    public void clear(String pipelineId) {
        failures.computeIfPresent(pipelineId, (id, slots) -> {
            Slot legacy = slots.legacy();
            if (legacy == null) return slots;
            legacy.retire();
            return null;
        });
    }

    /** Registration after submission is outside the processor and planning callbacks. */
    Recorder forJob(long jobId, JobConfig config) {
        return forJob(jobId, config, 0, null);
    }

    /** Registers without waiting on the cluster. A directly driven processor may have no member. */
    public static Recorder forProcessor(HazelcastInstance member, Processor.Context context) {
        if (member == null) return null;
        return of(member).forJob(context.jobId(), context.jobConfig(), context.executionId(),
                JetLiveExecution.StartupPhase.PROCESSOR);
    }

    /** Registers a planning failure on the coordinator before any worker context exists. */
    Recorder forPlanning(ProcessorMetaSupplier.Context context) {
        return forJob(context.jobId(), context.jobConfig(), context.executionId(), JetLiveExecution.StartupPhase.PLANNING);
    }

    private Recorder forJob(long jobId, JobConfig config, long executionId, JetLiveExecution.StartupPhase phase) {
        if (config == null || config.getName() == null) return Recorder.NONE;
        Object identity = config.getArgument(Engine.RUN_IDENTITY_ARGUMENT);
        if (identity == null) return new Recorder(this, config.getName(), null);
        if (!(identity instanceof Serializable immutable)) throw new io.tapstate.core.common.TapstateException(
                EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", config.getName()), null);
        if (phase == null) return attach(config.getName(), jobId, immutable);
        return attachLocal(new JetLiveExecution.Startup(config.getName(), jobId, executionId, immutable, phase));
    }

    private Recorder attachLocal(JetLiveExecution.Startup startup) {
        var answer = new AtomicReference<>(Recorder.NONE);
        failures.compute(startup.pipelineId(), (id, before) -> observeLocalStartup(before, startup, answer));
        return answer.get();
    }

    private Slots observeLocalStartup(Slots before, JetLiveExecution.Startup startup,
            AtomicReference<Recorder> answer) {
        if (!startup.isLocalTo(member)) return before;
        Slot named = before == null ? null : before.named;
        if (named != null && named.jobId == startup.jobId()) {
            answer.set(recorder(named, startup.jobId(), startup.identity()));
            return before;
        }

        Local local = before == null ? null : before.local;
        if (local != null && local.slot.jobId == startup.jobId()) {
            Slot slot = local.slot;
            if (!slot.matches(startup.jobId(), startup.identity())) return before;
            answer.set(recorder(slot, startup.jobId(), startup.identity()));
            return local.proof.equals(startup) ? before : withLocal(named, new Local(slot, startup));
        }

        // Local execution evidence must not displace another locally executing job.
        if (local != null && local.proof.isLocalTo(member)) return before;
        if (local != null) local.slot.retire();
        Slot slot = new Slot(startup.pipelineId(), startup.jobId(), startup.identity());
        answer.set(recorder(slot, startup.jobId(), startup.identity()));
        return withLocal(named, new Local(slot, startup));
    }

    private static Slots withLocal(Slot named, Local local) {
        if (named != null && named.identity == null) {
            named.retire();
            named = null;
        }
        return new Slots(named, local);
    }

    /** Only a named lookup can promote a local observation over a different named job. */
    Recorder attach(String pipelineId, long jobId, Serializable identity) {
        Objects.requireNonNull(identity, "identity");
        while (true) {
            Slots before = failures.get(pipelineId);
            if (!currentJobMatches(pipelineId, jobId, identity)) return Recorder.NONE;
            Slot known = before == null ? null : before.withJobId(jobId);
            if (known != null && !known.matches(jobId, identity)) return Recorder.NONE;
            Slot slot = known == null ? new Slot(pipelineId, jobId, identity) : known;
            if (updateIfCurrent(pipelineId, before, current -> promote(current, slot))) {
                return recorder(slot, jobId, identity);
            }
        }
    }

    /** Reads only a slot attached while the concrete job was proved; never resolves or reattaches it. */
    Optional<RetainedFailure> retainedRunFailure(String pipelineId, Serializable identity) {
        Slots slots = failures.get(pipelineId);
        return slots == null ? Optional.empty() : slots.retained(identity);
    }

    record RetainedFailure(Throwable cause) {}

    Recorder existing(String pipelineId, long jobId, Serializable identity) {
        Slots slots = failures.get(pipelineId);
        return recorder(slots == null ? null : slots.withJobId(jobId), jobId, identity);
    }

    /** Cancellation can precede processor initialization; retain one small marker for this exact job. */
    void retireRun(String pipelineId, long jobId, Serializable identity) {
        Objects.requireNonNull(identity, "identity");
        while (true) {
            Slots before = failures.get(pipelineId);
            if (retireRunAttempt(pipelineId, jobId, identity, before)) return;
        }
    }

    /** Returns false when a concurrent slot update requires another attempt. */
    private boolean retireRunAttempt(String pipelineId, long jobId, Serializable identity, Slots before) {
        Slot named = before == null ? null : before.named;
        Slot known = before == null ? null : before.withJobId(jobId);
        boolean exact = known != null && identity.equals(known.identity);
        if (exact && (known == named || named == null)) {
            return updateIfCurrent(pipelineId, before, current -> {
                known.retire();
                return new Slots(current.named, current.local);
            });
        }
        // A different named slot can be displaced only by authority over this cancellation's pair.
        boolean canPromote = before == null || currentJobMatches(pipelineId, jobId, identity);
        if (!canPromote && !exact) return true;
        Slot retired = exact ? known : new Slot(pipelineId, jobId, identity);
        return updateIfCurrent(pipelineId, before, current -> {
            retired.retire();
            return canPromote ? promote(current, retired) : new Slots(current.named, current.local);
        });
    }

    private Recorder recorder(Slot slot, long jobId, Serializable identity) {
        return slot != null && slot.matches(jobId, identity) ? new Recorder(this, slot.pipelineId, slot) : Recorder.NONE;
    }

    private static Slots promote(Slots before, Slot slot) {
        if (before != null) {
            if (before.named == slot) return before;
            if (before.named != null) before.named.retire();
            if (before.local != null && before.local.slot != slot) before.local.slot.retire();
        }
        return new Slots(slot, null);
    }

    private boolean updateIfCurrent(String pipelineId, Slots before, UnaryOperator<Slots> update) {
        var applied = new AtomicBoolean();
        failures.compute(pipelineId, (id, current) -> {
            // The entry object is the version. Refresh the named proof after any intervening update.
            if (current != before) return current;
            applied.set(true);
            return update.apply(current);
        });
        return applied.get();
    }

    private boolean currentJobMatches(String pipelineId, long jobId, Serializable identity) {
        try {
            var current = member.getJet().getJob(pipelineId);
            return current != null && current.getId() == jobId && identity.equals(current.getConfig().getArgument(Engine.RUN_IDENTITY_ARGUMENT));
        } catch (com.hazelcast.core.HazelcastInstanceNotActiveException | com.hazelcast.jet.core.JobNotFoundException stopped) {
            // No slot may be adopted through a member that can no longer prove the named job.
            return false;
        }
    }

    private void clear(Slot original) {
        failures.computeIfPresent(original.pipelineId, (id, slots) -> {
            if (slots.withJobId(original.jobId) != original) return slots;
            original.retire();
            return new Slots(slots.named, slots.local);
        });
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
            if (slot != null) slot.capture(failure);
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

    /** At most one named job and one locally observed job awaiting named attachment. */
    private record Slots(Slot named, Local local) {
        Slot withJobId(long jobId) {
            if (named != null && named.jobId == jobId) return named;
            return local != null && local.slot.jobId == jobId ? local.slot : null;
        }

        Slot legacy() {
            return named != null && named.identity == null ? named : null;
        }

        Optional<RetainedFailure> retained(Serializable identity) {
            var namedProof = named == null ? Optional.<RetainedFailure>empty() : named.retained(identity);
            var localProof = local == null ? Optional.<RetainedFailure>empty() : local.slot.retained(identity);
            // A lost member cannot distinguish two concrete jobs by run identity alone.
            if (namedProof.isPresent() && localProof.isPresent()) return Optional.empty();
            return namedProof.or(() -> localProof);
        }
    }

    private record Local(Slot slot, JetLiveExecution.Startup proof) {}

    private static final class Slot {
        final String pipelineId;
        final long jobId;
        final Serializable identity;
        private Throwable cause;
        private boolean retired;
        Slot(String pipelineId, long jobId, Serializable identity) {
            this.pipelineId = pipelineId; this.jobId = jobId; this.identity = identity;
        }
        synchronized boolean matches(long expectedJob, Serializable expectedIdentity) {
            return !retired && jobId == expectedJob && Objects.equals(identity, expectedIdentity);
        }
        synchronized Optional<RetainedFailure> retained(Serializable expectedIdentity) {
            return !retired && jobId != 0 && identity != null && identity.equals(expectedIdentity)
                    ? Optional.of(new RetainedFailure(cause)) : Optional.empty();
        }
        synchronized void capture(Throwable failure) { if (!retired) cause = Objects.requireNonNull(failure, "failure"); }
        synchronized Optional<Throwable> get() { return retired ? Optional.empty() : Optional.ofNullable(cause); }
        synchronized void retire() { retired = true; cause = null; }
    }
}
