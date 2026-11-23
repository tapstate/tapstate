package io.tapstate.app;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineStoreViews;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.Objects;
import java.util.function.LongSupplier;
import io.tapstate.core.lifecycle.StateJson;
import java.util.function.Function;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.DesiredStore;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope.Phase;

/**
 * Periodically reconciles every pipeline's actual state toward its desired intent, then publishes each
 * pipeline's observation for the read faces to serve. Spring's scheduler lives here in the assembly
 * layer, not in the runtime ring where the framework is banned, so this thin driver ticks the
 * framework-free {@link PipelineConverger} and {@link ObservationPublisher}. Each pipeline is handled
 * independently: a failure on one is logged and does not stop the pass, so one bad pipeline cannot
 * starve the rest.
 */
final class ConvergenceDriver implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConvergenceDriver.class);

    private final PipelineConverger converger;
    private final DesiredStore desired;
    private final ObservationPublisher publisher;
    private final BooleanSupplier reconcileEligible;
    private final PipelineActuationOwnership actuation;
    private final ScopedPorts scoped;
    private final PipelinePassExecutor passes;
    private final java.util.concurrent.atomic.AtomicBoolean scanning = new java.util.concurrent.atomic.AtomicBoolean();
    private int nextPipeline;
    private int nextPeriodicPipeline;
    private final LongSupplier nanoTime;
    private static final long TERMINAL_RECHECK_NANOS = java.time.Duration.ofSeconds(30).toNanos();
    private record TerminalPass(PipelineIntentScope scope, long revision, PipelineState target,
            PipelineState actual, long completedAt, StoppedEvidence stopped, PipelineActuationOwnership.StopRelease release) {
        TerminalPass checked(long now, PipelineActuationOwnership.StopRelease stoppedRelease) {
            return new TerminalPass(scope, revision, target, actual, now, stopped, stoppedRelease);
        }
    }
    private record StoppedEvidence(PipelineIntentSnapshot intent, CheckpointDoc checkpoint, WorkloadClaimFence claim,
            TerminalPublication.Receipt history, TerminalPublication.Receipt alerts) {
        TerminalPublication.Status status() {
            var first = history.status();
            var second = alerts.status();
            if (first == TerminalPublication.Status.FAILED || second == TerminalPublication.Status.FAILED)
                return TerminalPublication.Status.FAILED;
            return first == TerminalPublication.Status.COMPLETE && second == TerminalPublication.Status.COMPLETE
                    ? TerminalPublication.Status.COMPLETE : TerminalPublication.Status.PENDING;
        }
    }
    private final Map<String, TerminalPass> terminalPasses = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, Long> attemptedStarts = new java.util.concurrent.ConcurrentHashMap<>();

    private final PeriodicObservations periodicObservations;
    private final PipelineConvergences pipelineConvergences;
    private final Map<String, PipelineIntentScope> unavailableObservations = new ConcurrentHashMap<>();

    /** The fixed store root and per-pass bindings; no current-scope or current-claim lookup is supplied. */
    record ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable requireActive,
            Function<PipelineConvergencePass, LifecycleActuator> actuators,
            Function<PipelineConvergencePass, RebuildAdmission> rebuilds,
            Consumer<Collection<String>> retainRebuilds, PipelineAlertFeed alerts,
            Consumer<PipelineIntentStore.PendingStart> prepareStart,
            Consumer<String> resumeRebuild, NativeObservationSampling observations, ProcessObservationWriter writes,
            StoppedTerminalRecheck stoppedRecheck) {
        ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable active,
                Function<PipelineConvergencePass, LifecycleActuator> actuators,
                Function<PipelineConvergencePass, RebuildAdmission> rebuilds, Consumer<Collection<String>> retain,
                PipelineAlertFeed alerts, Consumer<PipelineIntentStore.PendingStart> prepareStart, Consumer<String> resumeRebuild,
                NativeObservationSampling observations, ProcessObservationWriter writes) {
            this(intents, stores, active, actuators, rebuilds, retain, alerts, prepareStart, resumeRebuild,
                    observations, writes, null);
        }
        ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable active,
                Function<PipelineConvergencePass, LifecycleActuator> actuators,
                Function<PipelineConvergencePass, RebuildAdmission> rebuilds, Consumer<Collection<String>> retain,
                PipelineAlertFeed alerts, Consumer<PipelineIntentStore.PendingStart> prepareStart, Consumer<String> resumeRebuild,
                NativeObservationSampling observations) {
            this(intents, stores, active, actuators, rebuilds, retain, alerts, prepareStart, resumeRebuild, observations, null);
        }
        ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable active,
                Function<PipelineConvergencePass, LifecycleActuator> actuators,
                Function<PipelineConvergencePass, RebuildAdmission> rebuilds, Consumer<Collection<String>> retain,
                PipelineAlertFeed alerts, Consumer<PipelineIntentStore.PendingStart> prepareStart, Consumer<String> resumeRebuild) {
            this(intents, stores, active, actuators, rebuilds, retain, alerts, prepareStart, resumeRebuild, null);
        }
        ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable active,
                Function<PipelineConvergencePass, LifecycleActuator> actuators,
                Function<PipelineConvergencePass, RebuildAdmission> rebuilds, Consumer<Collection<String>> retain) {
            this(intents, stores, active, actuators, rebuilds, retain, PipelineAlertFeed.none());
        }
        ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable active,
                Function<PipelineConvergencePass, LifecycleActuator> actuators,
                Function<PipelineConvergencePass, RebuildAdmission> rebuilds, Consumer<Collection<String>> retain,
                PipelineAlertFeed alerts) {
            this(intents, stores, active, actuators, rebuilds, retain, alerts, null, null);
        }
        ScopedPorts {
            Objects.requireNonNull(intents, "intents"); Objects.requireNonNull(stores, "stores");
            Objects.requireNonNull(requireActive, "requireActive"); Objects.requireNonNull(actuators, "actuators");
            Objects.requireNonNull(rebuilds, "rebuilds"); Objects.requireNonNull(retainRebuilds, "retainRebuilds");
            Objects.requireNonNull(alerts, "alerts");
        }
    }

    private record FailureStreak(PipelineIntentScope scope, long count) {}
    private record ObservationVersion(PipelineIntentSnapshot intent, WorkloadClaimFence claim,
            CheckpointDoc checkpoint) {}

    // Each Pipeline is single-flight; distinct Pipelines and retention may run concurrently.
    private final Map<String, FailureStreak> reconcileFailures = new ConcurrentHashMap<>();

    private final RateSampler sampler;

    /** The second projection of the facts, offered right after the first is stored; none() when nobody listens. */
    private final MetricsExport export;

    /** A driver that keeps no history, for the cases that are about convergence alone. */
    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher) {
        this(converger, desired, publisher, (RateSampler) null);
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler) {
        this(converger, desired, publisher, sampler, MetricsExport.none());
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export) {
        this(converger, desired, publisher, sampler, export, () -> true,
                PipelineActuationOwnership.single());
    }

    /** A driver on a member that only acts while the cluster says this member may. */
    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            BooleanSupplier reconcileEligible) {
        this(converger, desired, publisher, reconcileEligible, PipelineActuationOwnership.single());
    }

    /** The same, driving only the pipelines this member holds the actuation claim for. */
    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            BooleanSupplier reconcileEligible, PipelineActuationOwnership actuation) {
        this(converger, desired, publisher, null, MetricsExport.none(), reconcileEligible, actuation);
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation) {
        this(converger, desired, publisher, sampler, export, reconcileEligible, actuation, null, null, null);
    }

    static ConvergenceDriver scoped(PipelineConverger converger, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped) {
        return scoped(converger, publisher, sampler, export, reconcileEligible, actuation, scoped, (PeriodicObservations) null);
    }

    static ConvergenceDriver scoped(PipelineConverger converger, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, PeriodicObservations periodicObservations) {
        return scoped(converger, publisher, sampler, export, reconcileEligible, actuation, scoped,
                periodicObservations, null);
    }

    static ConvergenceDriver scoped(PipelineConverger converger, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, PeriodicObservations periodicObservations,
            PipelineConvergences pipelineConvergences) {
        return new ConvergenceDriver(converger, null, publisher, sampler, export, reconcileEligible,
                actuation, Objects.requireNonNull(scoped, "scoped"), periodicObservations, pipelineConvergences);
    }

    static ConvergenceDriver scoped(PipelineConverger converger, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, LongSupplier nanoTime) {
        return new ConvergenceDriver(converger, null, publisher, sampler, export, reconcileEligible,
                actuation, Objects.requireNonNull(scoped), nanoTime);
    }

    static ConvergenceDriver scoped(PipelineConverger converger, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, PeriodicObservations periodicObservations,
            PipelineConvergences pipelineConvergences, LongSupplier nanoTime) {
        return new ConvergenceDriver(converger, null, publisher, sampler, export, reconcileEligible,
                actuation, Objects.requireNonNull(scoped), periodicObservations, pipelineConvergences, nanoTime);
    }

    private ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, PeriodicObservations periodicObservations,
            PipelineConvergences pipelineConvergences) {
        this(converger, desired, publisher, sampler, export, reconcileEligible, actuation, scoped,
                periodicObservations, pipelineConvergences, System::nanoTime);
    }

    private ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, LongSupplier nanoTime) {
        this(converger, desired, publisher, sampler, export, reconcileEligible, actuation, scoped, null, null, nanoTime);
    }

    private ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, PeriodicObservations periodicObservations,
            PipelineConvergences pipelineConvergences, LongSupplier nanoTime) {
        this.nanoTime = Objects.requireNonNull(nanoTime);
        this.scoped = scoped;
        this.periodicObservations = periodicObservations;
        this.pipelineConvergences = pipelineConvergences;
        passes = new PipelinePassExecutor(4, pipelineConvergences == null ? null
                : (pipeline, operation) -> pipelineConvergences.offer(pipeline, operation) != PipelineConvergences.Offer.REJECTED);
        this.converger = converger;
        this.desired = scoped == null ? Objects.requireNonNull(desired, "desired") : null;
        this.publisher = publisher;
        this.sampler = sampler;
        this.export = export == null ? MetricsExport.none() : export;
        this.reconcileEligible = reconcileEligible;
        this.actuation = actuation;
    }

    @Scheduled(fixedDelayString = "${tapstate.converge.interval-ms:1000}")
    void schedule() { scan(true); }

    /** Inline without an external lifecycle worker; otherwise dispatch through its bounded queue. */
    void reconcile() { scan(pipelineConvergences != null); }

    private void scan(boolean asynchronous) {
        boolean eligible = PipelineDiagnostics.measure(null, Phase.PIPELINE_ELIGIBILITY, () -> {
            boolean allowed = reconcileEligible.getAsBoolean();
            PipelineDiagnostics.event(allowed ? "member_eligible" : "member_not_eligible");
            return allowed;
        });
        if (!eligible) return;
        if (!scanning.compareAndSet(false, true)) {
            PipelineDiagnostics.measure(null, Phase.PIPELINE_SCAN, () -> {
                PipelineDiagnostics.event("scan_already_in_flight"); return null;
            });
            return;
        }
        try {
            boolean prioritizeStarts = asynchronous && scoped != null;
            List<PipelineIntentStore.ScanCandidate> candidates = PipelineDiagnostics.measure(null, Phase.PIPELINE_SCAN,
                    () -> prioritizeStarts ? scoped.intents().scanCandidates()
                            : (scoped == null ? desired.pipelineIds() : scoped.intents().pipelineIds()).stream()
                                    .map(id -> new PipelineIntentStore.ScanCandidate(id, 0)).toList());
            List<String> pipelineIds = candidates.stream().map(PipelineIntentStore.ScanCandidate::pipelineId).toList();
            int size = candidates.size();
            int start = size == 0 ? 0 : Math.floorMod(nextPipeline, size);
            var periodic = new boolean[size];
            for (int offset = 0; offset < size; offset++) {
                int index = (start + offset) % size;
                var candidate = candidates.get(index);
                if (!asynchronous) passes.runInline(candidate.pipelineId(), () -> reconcilePipeline(candidate));
                else if (!prioritizeStarts) passes.submit(candidate.pipelineId(), () -> reconcilePipeline(candidate));
                else if (pipelineConvergences != null && candidate.targetState() == PipelineState.STOPPED) {
                    // A blocked intent read belongs to the isolated Stop worker too. The worker
                    // still reloads the current intent before admitting any Stop operation.
                    if (periodicDue(candidate)) pipelineConvergences.beginStop(candidate.pipelineId(),
                            () -> scheduledStop(candidate));
                }
                else if (isNewStart(candidate)) passes.submitStart(candidate.pipelineId(), () -> reconcileCurrentPipeline(candidate, false, true));
                else periodic[index] = true;
            }
            if (prioritizeStarts && size > 0) {
                // Keep the next turn across saturated scans, independently of new-start ordering.
                int periodicStart = Math.floorMod(nextPeriodicPipeline, size);
                for (int offset = 0; offset < size; offset++) {
                    int index = (periodicStart + offset) % size;
                    if (!periodic[index]) continue;
                    var candidate = candidates.get(index);
                    if (periodicDue(candidate) && passes.submitPeriodic(candidate.pipelineId(), () -> reconcilePipeline(candidate)))
                        nextPeriodicPipeline = index + 1;
                }
            }
            nextPipeline = size == 0 ? 0 : (start + 1) % size;
            var retained = new java.util.HashSet<>(pipelineIds);
            retained.addAll(passes.activePipelines());
            if (pipelineConvergences != null) {
                pipelineConvergences.retain(pipelineIds);
                retained.addAll(pipelineConvergences.pipelineIds());
            }
            // No other scan admits work until this snapshot has been pruned. Workers may only
            // remove their own completed reservation, so an active claim cannot be pruned away.
            PipelineDiagnostics.measure(null, Phase.PIPELINE_RETAIN, () -> { retain(List.copyOf(retained)); return null; });
        } finally { scanning.set(false); }
    }

    private boolean periodicDue(PipelineIntentStore.ScanCandidate candidate) {
        var terminal = terminalPasses.get(candidate.pipelineId());
        if (terminal == null) return true;
        // This hint only delays a no-op pass. It never authorizes an operation or replaces
        // fresh admission, scope, claim and write checks when that pass runs.
        if (!terminal.scope().equals(candidate.scope()) || terminal.revision() != candidate.intentRevision()
                || terminal.target() != candidate.targetState()) {
            terminalPasses.remove(candidate.pipelineId(), terminal);
            return true;
        }
        if (terminal.stopped() != null && terminal.release() == null
                && terminal.stopped().status() == TerminalPublication.Status.COMPLETE) return true;
        return scoped.alerts().needsFrequentTerminalSamples(terminal.scope(), terminal.actual())
                || nanoTime.getAsLong() - terminal.completedAt() >= TERMINAL_RECHECK_NANOS;
    }

    private void scheduledStop(PipelineIntentStore.ScanCandidate candidate) {
        if (!reconcileEligible.getAsBoolean()) return;
        var hint = terminalPasses.get(candidate.pipelineId());
        if (hint != null && hint.stopped() != null && hint.scope().equals(candidate.scope())
                && hint.revision() == candidate.intentRevision() && !scoped.alerts().needsFrequentTerminalSamples(hint.scope(), hint.actual())
                && checkStopped(candidate.pipelineId(), hint)) return;
        reconcilePipeline(candidate, true);
    }

    private boolean checkStopped(String pipeline, TerminalPass hint) {
        var evidence = hint.stopped();
        boolean retryRejected = hint.release()==null && evidence.history().status()==TerminalPublication.Status.COMPLETE
                && evidence.alerts().status()==TerminalPublication.Status.FAILED && evidence.alerts().rejectedWithoutAdmission();
        if (evidence.status() == TerminalPublication.Status.FAILED && !retryRejected
                || periodicObservations != null && !periodicObservations.idle(pipeline)) return false;
        try (var local = actuation.enterPass(hint.scope())) {
            if (terminalPasses.get(pipeline) != hint) return false;
            if (!actuation.finishStoppedRelease(pipeline)) return true;
            var release = hint.release();
            if (release != null && release.status() != PipelineActuationOwnership.StopReleaseStatus.CONFIRMED) return false;
            var tenure = release == null ? StoppedTerminalRecheck.Tenure.HELD : StoppedTerminalRecheck.Tenure.ENDED;
            if (!scoped.stoppedRecheck().unchanged(evidence.intent(), evidence.checkpoint(), evidence.claim(), tenure)) return false;
            if(retryRejected){
                if(evidence.history().status()!=TerminalPublication.Status.COMPLETE)return false;
                var retried=evidence.alerts().retryRejected();if(retried==null)return false;
                var repaired=new StoppedEvidence(evidence.intent(),evidence.checkpoint(),evidence.claim(),evidence.history(),retried);
                terminalPasses.replace(pipeline,hint,new TerminalPass(hint.scope(),hint.revision(),hint.target(),hint.actual(),nanoTime.getAsLong(),repaired,null));
                return true;
            }
            if (release == null && evidence.status() == TerminalPublication.Status.COMPLETE) {
                if (periodicObservations != null && !periodicObservations.idle(pipeline)
                        || sampler != null && !sampler.idle(hint.scope()) || !evidence.history().seal()) return false;
                if (!evidence.alerts().seal()) {
                    evidence.history().reopen();
                    return false;
                }
                release = local.relinquishStopped(hint.scope(), evidence.claim());
                if (release == null) {
                    // No release was sent. Let the current tenure accept legitimate work again.
                    evidence.history().reopen();
                    evidence.alerts().reopen();
                    return false;
                }
                if (release.status() == PipelineActuationOwnership.StopReleaseStatus.UNPROVEN) return false;
            }
            terminalPasses.replace(pipeline, hint, hint.checked(nanoTime.getAsLong(), release));
            return true;
        } catch (RuntimeException unavailable) {
            LOG.debug("Could not confirm terminal tenure for pipeline {}", pipeline, unavailable);
            return false;
        }
    }

    private void rememberTerminal(PipelineConvergencePass pass, ConvergeResult result,
            Optional<io.tapstate.core.lifecycle.Observation> projection, LifecycleActuator actuator, RebuildAdmission rebuilds, StoppedEvidence stopped) {
        if (pass == null || result.status() != ConvergeStatus.CONVERGED || projection.isEmpty()
                || result.checkpoint().isEmpty()) return;
        var checkpoint = result.checkpoint().orElseThrow();
        var actual = StateJson.parse(checkpoint.stateJson());
        if (scoped.alerts().needsFrequentTerminalSamples(pass.scope(), actual)) return;
        var intent = pass.intent();
        var desired = intent.desired();
        if (desired.purgeState() || desired.reassemble() && desired.rebuiltAtStateEpoch() == null
                || desired.rebuildOwedAt(checkpoint)
                || projection.orElseThrow().state() != actual) return;
        boolean settled = actual == PipelineState.STOPPED && desired.targetState() == PipelineState.STOPPED;
        var recovery = checkpoint.leaseRecovery();
        if (actual == PipelineState.FAILED && (desired.targetState() == PipelineState.RUNNING
                || desired.targetState() == PipelineState.PAUSED))
            settled = !rebuilds.mayAdmitLater() || recovery != null && recovery.intentRevision() == intent.intentRevision()
                    && recovery.attempts() >= PipelineConverger.MAX_LEASE_RECOVERY_ATTEMPTS;
        if (!settled) return;
        try {
            if (actuator.isCarryingAJob(pass.scope().pipelineId())
                    || pass.state().read(pass.scope().pipelineId()).filter(checkpoint::equals).isEmpty()) return;
            terminalPasses.put(pass.scope().pipelineId(), new TerminalPass(pass.scope(), intent.intentRevision(),
                    desired.targetState(), actual, nanoTime.getAsLong(), stopped, null));
        } catch (TapstateException unavailableHint) {
            // Publication already succeeded. An optional scheduling proof must not replace it
            // with a reconcile failure; without the hint the next tick performs a full pass.
            LOG.debug("Terminal scheduling hint unavailable for pipeline {}", pass.scope().pipelineId());
        }
    }

    static boolean initialClaimMatches(PipelineIntentStore.ScanCandidate candidate, PipelineIntentSnapshot intent) {
        return candidate.pipelineId().equals(intent.scope().pipelineId())
                && candidate.startRevision() > 0 && candidate.startRevision() == candidate.intentRevision()
                && candidate.intentRevision() == intent.intentRevision() && intent.scope().equals(candidate.scope())
                && candidate.targetState() == PipelineState.RUNNING && intent.desired().targetState() == PipelineState.RUNNING;
    }

    private boolean isNewStart(PipelineIntentStore.ScanCandidate candidate) {
        return candidate.startRevision() > attemptedStarts.getOrDefault(candidate.pipelineId(), 0L);
    }

    private void reconcilePipeline(PipelineIntentStore.ScanCandidate candidate) {
        reconcilePipeline(candidate, false);
    }

    private void reconcilePipeline(PipelineIntentStore.ScanCandidate candidate, boolean stopOperation) {
        reconcileCurrentPipeline(candidate, stopOperation);
    }

    private void retain(Collection<String> pipelineIds) {
        // Forget what is kept per pipeline for pipelines that are no longer desired, so a
        // deleted-while-failing pipeline does not leak a counter that nothing will ever clear. Both live
        // here rather than one at each end: it is one question -- which of these still exist -- and an
        // intent is removed only by the reclaim of the pipeline itself, so a merely stopped pipeline keeps
        // its intent and keeps both. The publisher's own account cannot be cleared by whoever deletes the
        // pipeline: the control ring's synchronous surface into the runtime is a closed set, and this set
        // is already crossing once a tick with the same answer in it.
        reconcileFailures.keySet().retainAll(pipelineIds);
        attemptedStarts.keySet().retainAll(pipelineIds);
        terminalPasses.keySet().retainAll(pipelineIds);
        unavailableObservations.keySet().retainAll(pipelineIds);
        // Same for the claims: a pipeline that is gone still has this member named as its driver until the
        // lease runs out, which delays nothing but reads as an owner over something that no longer exists.
        actuation.retain(pipelineIds);
        // And the rebuilds spent on them, or a pipeline created again under a deleted one's id would start
        // with whatever the deleted one had left of its budget.
        if (scoped == null) converger.retain(pipelineIds);
        else scoped.retainRebuilds().accept(pipelineIds);
        if (scoped != null) {
            alert(() -> scoped.alerts().retain(pipelineIds));
            if (scoped.observations() != null) scoped.observations().retain(pipelineIds);
            if (scoped.writes() != null) scoped.writes().retain(pipelineIds);
        }
        publisher.forgetPipelinesOutside(pipelineIds);
        if (sampler != null) {
            sampler.forgetPipelinesOutside(pipelineIds);
        }
        if (periodicObservations != null) {
            periodicObservations.retain(pipelineIds);
        }
        export.forgetPipelinesOutside(pipelineIds);
    }

    private void reconcileCurrentPipeline(PipelineIntentStore.ScanCandidate candidate, boolean stopOperation) {
        reconcileCurrentPipeline(candidate, stopOperation, false);
    }

    private void reconcileCurrentPipeline(PipelineIntentStore.ScanCandidate candidate, boolean stopOperation, boolean initialStart) {
        String pipelineId = candidate.pipelineId();
        if (!reconcileEligible.getAsBoolean()
                || !stopOperation && pipelineConvergences != null && pipelineConvergences.stopping(pipelineId)) return;
        var previousTerminal = terminalPasses.remove(pipelineId);
        boolean refreshTerminal = previousTerminal != null && previousTerminal.stopped() != null;
        if (scoped != null && scoped.stoppedRecheck() != null && !actuation.finishStoppedRelease(pipelineId)) return;
        // Attribute every line logged while reconciling this pipeline to it, so the logs read face can
        // tail per pipeline. Cleared per pipeline so the slot never leaks onto the next one or an idle tick.
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
        PipelineConvergencePass pass = null;
        PipelineActuationOwnership.Pass localPass = null;
        boolean periodic = false;
        boolean initialStartAwaitingProof = false;
        try {
            PipelineIntentStore.ActuationReading observationOrigin = null;
            PipelineActuationOwnership.Permit permit;
            if (scoped == null) {
                permit = actuation.permit(pipelineId);
            } else {
                Optional<PipelineIntentSnapshot> snapshot;
                if (scoped.observations() == null) {
                    snapshot = scoped.intents().loadForActuation(pipelineId);
                } else {
                    var reading = scoped.intents().readForActuation(pipelineId);
                    observationOrigin = reading.orElse(null);
                    snapshot = reading.map(PipelineIntentStore.ActuationReading::intent);
                }
                if (stopOperation && snapshot.filter(intent -> intent.desired() != null
                        && intent.desired().targetState() == PipelineState.STOPPED).isEmpty()) return;
                if (snapshot.isEmpty()) {
                    var pendingStart = scoped.prepareStart() == null
                            ? Optional.<PipelineIntentStore.PendingStart>empty() : scoped.intents().pendingStart(pipelineId);
                    if (pendingStart.isPresent()) {
                        var pending = pendingStart.orElseThrow();
                        MDC.put(io.tapstate.core.logging.PipelineAttribution.STATE_GENERATION_MDC_KEY,
                                Long.toString(pending.intent().scope().stateGeneration()));
                        PipelineDiagnostics.intent(pending.intent());
                        try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_PREPARATION)) {
                            if (!prepareStart(pending)) return;
                        }
                        snapshot = PipelineDiagnostics.measure(pipelineId, Phase.PIPELINE_INTENT,
                                () -> scoped.intents().loadForActuation(pipelineId).filter(pending.intent()::equals));
                        if (snapshot.isEmpty()) return;
                        if (scoped.observations() != null) {
                            var reading = scoped.intents().readForActuation(pipelineId).filter(value -> pending.intent().equals(value.intent()));
                            if (reading.isEmpty()) return;
                            observationOrigin = reading.orElseThrow();
                        }
                    } else {
                        if (scoped.resumeRebuild() != null) scoped.resumeRebuild().accept(pipelineId);
                        return;
                    }
                }
                PipelineDiagnostics.intent(snapshot.orElseThrow());
                if (snapshot.get().deleted()) return;
                if (!pipelineId.equals(snapshot.get().scope().pipelineId())) {
                    throw new TapstateException(
                            IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
                }
                if (!stopOperation && pipelineConvergences != null
                        && snapshot.get().desired().targetState() == PipelineState.STOPPED) {
                    pipelineConvergences.beginStop(pipelineId, () -> reconcilePipeline(candidate, true));
                    return;
                }
                MDC.put(io.tapstate.core.logging.PipelineAttribution.STATE_GENERATION_MDC_KEY,
                        Long.toString(snapshot.get().scope().stateGeneration()));
                try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_CLAIM)) {
                    var intent = snapshot.orElseThrow();
                    boolean priority = initialStart && isNewStart(candidate) && initialClaimMatches(candidate, intent);
                    permit = priority ? actuation.permit(intent.scope(), true) : actuation.permit(intent.scope());
                }
                if (permit.granted()) {
                    if (candidate.startRevision() != 0 && snapshot.get().intentRevision() == candidate.startRevision()
                            && snapshot.get().desired().targetState() == PipelineState.RUNNING) {
                        initialStartAwaitingProof = isNewStart(candidate);
                        attemptedStarts.merge(pipelineId, candidate.startRevision(), Math::max);
                    }
                    localPass = Objects.requireNonNull(actuation.enterPass(snapshot.get().scope()), "local convergence pass");
                    pass = new PipelineConvergencePass(snapshot.get(), permit.claim(),
                            scoped.stores(), scoped.requireActive(), this::expectingPublication);
                    actuation.renewalFailure(snapshot.get().scope(), permit.claim()).ifPresent(pass::renewalFailed);
                }
            }
            if (!permit.granted()) {
                // Another member drives this pipeline. Observe desired and actual; drive neither. Both
                // halves matter: converging here would call the same lifecycle verb a second time --
                // this member is carrying no job, which is exactly the condition the converge side
                // starts one in -- and publishing here would overwrite the driver's observation with
                // this member's own run statistics, which are absent because the run is not here.
                return;
            }
            PipelineConvergencePass originalPass = pass;
            if (scoped != null) expectingPublication(originalPass);
            LifecycleActuator actuator = scoped == null ? null : scoped.actuators().apply(pass);
            RebuildAdmission rebuilds = scoped == null ? null : scoped.rebuilds().apply(pass);
            initialStartAwaitingProof = false;
            ConvergeResult result;
            try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_CONVERGE)) {
                result = scoped == null ? converger.converge(pipelineId) : convergeScoped(originalPass, stopOperation, actuator, rebuilds);
            }
            if (result.status() == ConvergeStatus.RECOVERING) return;
            if (periodicObservations != null && result.status() == ConvergeStatus.CONVERGED
                    && result.checkpoint().filter(checkpoint ->
                            StateJson.parse(checkpoint.stateJson()) == PipelineState.RUNNING).isPresent()) {
                var observedPass = Objects.requireNonNull(pass);
                var original = observedPass.claim();
                var checkpoint = result.checkpoint().orElseThrow();
                // The worker owns observations only, never the local lifecycle pass.
                periodic = true;
                var config = observationOrigin;
                periodicObservations.offer(pipelineId,
                        new ObservationVersion(observedPass.intent(), original, checkpoint),
                        admitted -> publishPeriodicObservation(observedPass, original, checkpoint, config, admitted));
                reconcileFailures.remove(pipelineId);
                return;
            }
            discardPeriodic(pipelineId);
            ObservationFailure failure;
            if (result.status() == ConvergeStatus.FAILED) {
                // The job died on its own; the converge side moved the pipeline to the observable FAILED
                // state. This is the only pass that holds the cause, so it goes two ways: coded onto the
                // observation, so a read face can say why, and into the log with its code and stack.
                // Every later pass while the checkpoint stays FAILED publishes null and the publisher
                // carries the stored reason forward — durably, so it survives a process restart too.
                failure = failedObservation(pipelineId, result);
            } else failure = null;
            PipelineConvergencePass sampledPass = pass;
            var config = observationOrigin;
            writeObservationNow(pipelineId, () -> publishObservation(pipelineId, sampledPass, result, config, failure, actuator, rebuilds, refreshTerminal));
            // A clean pass ends the failure streak; the next throw starts counting from one again.
            reconcileFailures.remove(pipelineId);
        } catch (RuntimeException e) {
            PipelineDiagnostics.failed(e);
            if (initialStartAwaitingProof && e instanceof TapstateException failure && failure.code() == IoError.STORE_UNAVAILABLE)
                attemptedStarts.remove(pipelineId, candidate.startRevision());
            discardPeriodic(pipelineId);
            PipelineConvergencePass failedPass = pass;
            if (scoped != null) alert(() -> scoped.alerts().unavailable(failedPass));
            // A pass that keeps throwing never reaches publish(), so the read face would stay empty and a
            // permanently broken pipeline would look identical to a slow one. Count the consecutive
            // failures and publish them as an error observation so the failure is observable, not just
            // logged. Isolate this pipeline: one bad pipeline must not starve the rest of the pass.
            PipelineIntentScope failedScope = pass == null ? null : pass.scope();
            FailureStreak previous = reconcileFailures.get(pipelineId);
            long failures = previous != null && Objects.equals(previous.scope(), failedScope) ? previous.count() + 1 : 1;
            reconcileFailures.put(pipelineId, new FailureStreak(failedScope, failures));
            LOG.warn("Reconcile pass for pipeline {} failed {} time(s) in a row; retrying on the next tick",
                    pipelineId, failures, e);
            try {
                if (scoped == null) publisher.publishReconcileFailure(pipelineId, failures);
                else if (pass != null) writeObservationNow(pipelineId, () -> publisher.publishReconcileFailureScoped(
                        failedPass.scope(), failedPass.state(), failedPass.observationSample(), failures));
            } catch (RuntimeException unpublishable) {
                // The read face is unreachable too (e.g. the same store backs both), so nothing can be
                // surfaced this tick; the warning above is the only record.
                LOG.warn("Could not publish the reconcile failure for pipeline {}", pipelineId, unpublishable);
            }
        } finally {
            if (!periodic) discardPeriodic(pipelineId);
            if (localPass != null) localPass.close();
            MDC.remove(io.tapstate.core.logging.PipelineAttribution.STATE_GENERATION_MDC_KEY);
            MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        }
    }

    private ConvergeResult convergeScoped(PipelineConvergencePass pass, boolean stopOperation,
            LifecycleActuator actuator, RebuildAdmission rebuilds) {
        String pipelineId = pass.scope().pipelineId();
        var state = pass.state();
        Consumer<String> beforeCompletion = ignored -> {
            discardPeriodic(pipelineId);
            writeObservationNow(pipelineId, () -> publisher.publishScoped(pass.scope(),
                    pass.state(), pass.observationSample(), null));
        };
        BooleanSupplier currentIntent = () -> scoped.intents().loadForActuation(pipelineId)
                .filter(pass.intent()::equals).isPresent();
        if (stopOperation) return converger.convergeScoped(pass.intent(), state, actuator, rebuilds,
                beforeCompletion, currentIntent, true);
        return converger.convergeScoped(pass.intent(), state, actuator, rebuilds, beforeCompletion, currentIntent);
    }

    private void expectingPublication(PipelineConvergencePass pass) {
        if (scoped.writes() != null && scoped.stoppedRecheck() != null) scoped.writes().expecting(pass);
        alert(() -> scoped.alerts().expecting(pass));
    }

    private void writeObservationNow(String pipelineId, Runnable publication) {
        if (scoped != null && scoped.writes() != null) scoped.writes().now(pipelineId, publication);
        else publication.run();
    }

    private void publishObservation(String pipelineId, PipelineConvergencePass pass, ConvergeResult result,
            PipelineIntentStore.ActuationReading config, ObservationFailure failure,
            LifecycleActuator actuator, RebuildAdmission rebuilds, boolean refreshTerminal) {
        Optional<ObservationPublisher.Publication> publication;
        try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_PUBLISH)) {
            boolean terminal = result.checkpoint().map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
                    .filter(state -> state == PipelineState.STOPPED || state == PipelineState.FAILED).isPresent();
            if (scoped != null && terminal && scoped.observations() != null) scoped.observations().forget(pipelineId);
            boolean finalStopSample = terminal && scoped != null && scoped.stoppedRecheck() != null && pass != null
                    && pass.intent().desired().targetState() == PipelineState.STOPPED && !pass.intent().desired().purgeState()
                    && !pass.intent().desired().reassemble()
                    && result.checkpoint().filter(checkpoint -> StateJson.parse(checkpoint.stateJson()) == PipelineState.STOPPED).isPresent();
            publication = scoped == null ? publisher.publish(pipelineId, failure)
                    .map(value -> new ObservationPublisher.Publication(value, true))
                    : scoped.observations() == null || terminal
                            ? publisher.publishScopedReading(pass.scope(), pass.state(), pass.observationSample(), failure,
                                    !finalStopSample && !refreshTerminal && !scoped.alerts().needsFreshTerminalMetrics(pass.scope()))
                            : scoped.observations().publish(publisher, pass, result, config, failure)
                                    .map(value -> new ObservationPublisher.Publication(value, true));
        }
        StoppedEvidence evidence = null;
        if (publication.isPresent()) {
            var reading = publication.orElseThrow();
            var published = reading.observation();
            PipelineDiagnostics.event("observation_" + published.state().name());
            if (canSettleStopped(pass, result, published)) {
                var checkpoint = result.checkpoint().orElseThrow();
                var terminal = TerminalPublication.from(pass, checkpoint.epoch(), published);
                scoped.observations().terminal(terminal);
                var alertReceipt = TerminalPublication.failed();
                try {
                    alertReceipt = scoped.alerts().terminalPublished(pass, terminal, published, result.failure().orElse(null), reading.measured());
                } catch (RuntimeException unavailable) {
                    LOG.warn("Could not submit the final alert sample for pipeline {}", pipelineId);
                }
                var sample = reading.measured() && sampler != null ? RateSampler.sampleOf(published) : null;
                var history = sample == null ? null : new ProcessObservationWriter.History(sample,
                        remaining -> pass.rateHistory(remaining).append(sample));
                var historyReceipt = scoped.writes().terminalHistory(terminal,
                        history == null ? null : history.guardedBy(pass.guardOriginal(pass.claim())));
                evidence = new StoppedEvidence(pass.intent(), checkpoint, pass.claim(), historyReceipt, alertReceipt);
            } else {
                if (scoped != null) alert(() -> scoped.alerts().published(pass, published, result.failure().orElse(null), reading.measured()));
                if (reading.measured()) {
                    if (scoped == null || scoped.writes() == null) sample(published, pass);
                    else {
                        var history = history(published, pass);
                        if (history != null) scoped.writes().offerHistory(history.guardedBy(pass.guardOriginal(pass.claim())));
                    }
                }
            }
            if (reading.measured()) export(published);
        }
        if (pass != null) rememberTerminal(pass, result, publication.map(ObservationPublisher.Publication::observation),
                actuator, rebuilds, evidence);
    }

    private boolean canSettleStopped(PipelineConvergencePass pass, ConvergeResult result, Observation observation) {
        if (pass == null || scoped.stoppedRecheck() == null || scoped.writes() == null || scoped.observations() == null
                || result.status() != ConvergeStatus.CONVERGED || result.checkpoint().isEmpty()
                || observation.state() != PipelineState.STOPPED) return false;
        var desired = pass.intent().desired();
        var checkpoint = result.checkpoint().orElseThrow();
        return StateJson.parse(checkpoint.stateJson()) == PipelineState.STOPPED && desired.targetState() == PipelineState.STOPPED
                && !desired.purgeState() && !desired.reassemble() && !desired.rebuildOwedAt(checkpoint)
                && !scoped.alerts().needsFrequentTerminalSamples(pass.scope(), observation.state())
                && (periodicObservations == null || periodicObservations.idle(pass.scope().pipelineId()))
                && (sampler == null || sampler.idle(pass.scope()));
    }

    private static ObservationFailure failedObservation(String pipelineId, ConvergeResult result) {
        var original = result.failure().orElse(null);
        var failure = PipelineFailures.of(pipelineId, original, result.diagnostic(), result.publicFailure());
        var logDiagnostic = original == null ? failure.diagnostic()
                : io.tapstate.core.logging.FailureLogSnapshot.of(new io.tapstate.core.common.FailureFact(
                        original, failure.diagnostic(), result.publicFailure()));
        if (PipelineFailures.isCoded(original) && !logDiagnostic.containsKey("code")) {
            // The current coded primary is an observed fact, even when no connector was opened.
            var fields = new java.util.LinkedHashMap<String, Object>(logDiagnostic);
            fields.put("code", failure.code());
            fields.put("codeSource", io.tapstate.core.common.DiagnosticCodeSource.TAPSTATE_STANDARD);
            fields.put("params", failure.params());
            var rendered = io.tapstate.messages.MessageCatalog.bundled().render(failure.code(),
                    new java.util.LinkedHashMap<String, Object>(failure.params()));
            fields.put("message", rendered.message());
            if (rendered.solution() != null && !rendered.solution().isBlank()) fields.put("solution", rendered.solution());
            logDiagnostic = io.tapstate.core.logging.LogDiagnosticProjection.fit(fields);
        }
        var logParams = PipelineFailures.isCoded(original) ? failure.params()
                : Map.of("pipeline", pipelineId, "cause", original == null ? "unknown" : original.getClass().getName());
        LOG.atWarn().addKeyValue("diagnostic", logDiagnostic)
                .log("Pipeline {} entered FAILED [{}]: {}", pipelineId, failure.code(), logParams);
        return failure;
    }

    private void discardPeriodic(String pipelineId) {
        if (periodicObservations != null) {
            periodicObservations.discard(pipelineId);
            if (scoped.observations() != null) scoped.observations().forget(pipelineId);
        }
    }

    private void publishPeriodicObservation(PipelineConvergencePass pass, WorkloadClaimFence original,
            CheckpointDoc checkpoint, PipelineIntentStore.ActuationReading config, BooleanSupplier admitted) {
        var scope = pass.scope();
        String pipelineId = scope.pipelineId();
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
        MDC.put(io.tapstate.core.logging.PipelineAttribution.STATE_GENERATION_MDC_KEY,
                Long.toString(scope.stateGeneration()));
        try {
            BooleanSupplier current = () -> {
                if (!admitted.getAsBoolean()) return false;
                return reconcileEligible.getAsBoolean()
                        && scoped.intents().loadForActuation(pipelineId).filter(pass.intent()::equals).isPresent()
                        && pass.state().read(pipelineId).filter(checkpoint::equals).isPresent()
                        && actuation.proveObservationClaim(scope, original) && admitted.getAsBoolean();
            };
            Runnable requireCurrentTicket = () -> {
                if (!admitted.getAsBoolean()) throw ThreeLayerStorePort.refused();
            };
            var observation = scoped.stores().pipelineObservations(scope, original, () -> {
                scoped.requireActive().run();
                requireCurrentTicket.run();
            });
            var stage = scoped.writes() == null ? null : scoped.writes().stage(pass);
            UnaryOperator<ObservationStore> deferred = stage == null ? null
                    : store -> stage.wrap(NativeObservationSampling.guarded(store, () -> {
                        if (!current.getAsBoolean()) throw ThreeLayerStorePort.refused();
                    }));
            if (deferred != null && scoped.observations() != null) {
                var execution = new ExecutionFence(scope, original);
                observation = NativeObservationSampling.guarded(observation,
                        () -> scoped.observations().requireExecution(pass, execution));
            }
            var destination = deferred == null ? observation : deferred.apply(observation);
            var projection = scoped.observations() == null
                    ? publisher.publishRunningScoped(scope, checkpoint, destination, current)
                    : scoped.observations().publishRunning(publisher, pass, checkpoint, config, current,
                            requireCurrentTicket, () -> publisher.publishRunningScoped(scope, checkpoint, destination, current), deferred);
            projection.ifPresent(published -> {
                if (stage == null) {
                    periodicCommitted(pass, checkpoint, published, admitted, current);
                } else {
                    scoped.writes().submit(stage, () -> {
                        if (scoped.observations() != null) scoped.observations().recordPublishedRunning(pass, checkpoint);
                        periodicCommitted(pass, checkpoint, published, admitted, current);
                    }, history(published, pass), () -> alert(() -> scoped.alerts().unavailableIfExpected(pass)));
                }
            });
        } catch (RuntimeException unavailable) {
            // A failed measurement is not a new state reading. The last successful frame ages normally.
            if (admitted.getAsBoolean() && !scope.equals(unavailableObservations.put(pipelineId, scope))) {
                LOG.warn("Could not publish periodic observation for pipeline {}", pipelineId, unavailable);
            } else {
                LOG.debug("Periodic observation was refused or remains unavailable for pipeline {}", pipelineId, unavailable);
            }
        } finally {
            MDC.remove(io.tapstate.core.logging.PipelineAttribution.STATE_GENERATION_MDC_KEY);
            MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        }
    }

    private void periodicCommitted(PipelineConvergencePass pass, CheckpointDoc checkpoint, Observation published,
            BooleanSupplier admitted, BooleanSupplier current) {
        String pipelineId = pass.scope().pipelineId();
        if (unavailableObservations.remove(pipelineId, pass.scope())) {
            LOG.info("Periodic observation publication recovered for pipeline {}", pipelineId);
        }
        // The measurement worker can have already retired. The original checkpoint still fences this frame.
        BooleanSupplier alertCurrent = () -> {
            if (!admitted.getAsBoolean()) return false;
            return pass.state().read(pipelineId).filter(checkpoint::equals).isPresent() && admitted.getAsBoolean();
        };
        alert(() -> scoped.alerts().publishedPeriodic(pass, published, alertCurrent));
        if (scoped.writes() == null) sample(published, pass);
        if (current.getAsBoolean()) export(published, admitted);
    }

    private ProcessObservationWriter.History history(Observation published, PipelineConvergencePass pass) {
        if (sampler == null) return null;
        var sample = RateSampler.sampleOf(published);
        if (sample == null) return null;
        return new ProcessObservationWriter.History(sample,
                remaining -> sampler.offerPrepared(sample, pass.scope(), pass.rateHistory(remaining)));
    }

    void stopForShutdown() { passes.stop(); }
    void awaitStopped(long deadline) { passes.awaitStopped(deadline); }
    @Override public void close() { passes.close(); }

    private boolean prepareStart(PipelineIntentStore.PendingStart pending) {
        try {
            scoped.requireActive().run();
            scoped.prepareStart().accept(pending);
            return true;
        } catch (TapstateException failure) {
            if (!(failure.code() instanceof IoError error) || (error != IoError.DOCUMENT_UNREADABLE
                    && error != IoError.PIPELINE_STATE_REBUILD_REQUIRED && error != IoError.PIPELINE_INTENT_FENCED)) throw failure;
            scoped.intents().failStart(pending, error, failure.args());
            return false;
        }
    }

    private void alert(Runnable action) {
        try (var timing = PipelineDiagnostics.phase(Phase.PIPELINE_ALERTS)) { action.run(); }
        catch (RuntimeException unavailable) { LOG.warn("Alert collection is degraded for this convergence pass"); }
    }

    /**
     * Offers the facts just published to the export, which holds them for whatever backend it carries them
     * to. The same facts the observation was stored from, offered after it: the two projections read one
     * measurement, and neither reads the other. A backend that refuses is the export's to report; it does
     * not fail the pass that measured.
     */
    private void export(Observation published) {
        try {
            export.offer(published.pipelineId(), published.state(), published.observedAt(), published.facts());
        } catch (RuntimeException unexported) {
            LOG.warn("Could not offer the facts of pipeline {} for export", published.pipelineId(), unexported);
        }
    }

    private void export(Observation published, BooleanSupplier admitted) {
        try {
            export.offerIfCurrent(published.pipelineId(), published.state(), published.observedAt(),
                    published.facts(), admitted);
        } catch (RuntimeException unexported) {
            LOG.warn("Could not offer the facts of pipeline {} for export", published.pipelineId(), unexported);
        }
    }

    /**
     * Takes a sample off exactly what was published, at the time it was published, so the history and the
     * latest state never describe different passes of the run. The observation is the contract and the
     * history a record kept beside it: a history that cannot be written must not cost a pipeline the read
     * face that says it is alive at all.
     */
    private void sample(io.tapstate.core.lifecycle.Observation published, PipelineConvergencePass pass) {
        if (sampler == null) {
            return;
        }
        try {
            if (pass == null) sampler.offer(published);
            else sampler.offer(published, pass.scope(), pass.rateHistory());
        } catch (RuntimeException unsampled) {
            LOG.warn("Could not sample the history of pipeline {}", published.pipelineId(), unsampled);
        }
    }
}
