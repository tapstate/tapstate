package io.tapstate.app;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
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

/**
 * Periodically reconciles every pipeline's actual state toward its desired intent, then publishes each
 * pipeline's observation for the read faces to serve. Spring's scheduler lives here in the assembly
 * layer, not in the runtime ring where the framework is banned, so this thin driver ticks the
 * framework-free {@link PipelineConverger} and {@link ObservationPublisher}. Each pipeline is handled
 * independently: a failure on one is logged and does not stop the pass, so one bad pipeline cannot
 * starve the rest.
 */
final class ConvergenceDriver {

    private static final Logger LOG = LoggerFactory.getLogger(ConvergenceDriver.class);

    private final PipelineConverger converger;
    private final DesiredStore desired;
    private final ObservationPublisher publisher;
    private final BooleanSupplier reconcileEligible;
    private final PipelineActuationOwnership actuation;
    private final ScopedPorts scoped;
    private final PeriodicObservations periodicObservations;
    private final PipelineConvergences pipelineConvergences;
    private int nextPipeline;
    private String nextRejectedPipeline;
    private final Map<String, PipelineIntentScope> unavailableObservations = new ConcurrentHashMap<>();

    /** The fixed store root and per-pass bindings; no current-scope or current-claim lookup is supplied. */
    record ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable requireActive,
            Function<PipelineConvergencePass, LifecycleActuator> actuators,
            Function<PipelineConvergencePass, RebuildAdmission> rebuilds,
            Consumer<Collection<String>> retainRebuilds, PipelineAlertFeed alerts,
            Consumer<PipelineIntentStore.PendingStart> prepareStart,
            Consumer<String> resumeRebuild, NativeObservationSampling observations, ProcessObservationWriter writes) {
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
        return scoped(converger, publisher, sampler, export, reconcileEligible, actuation, scoped, null);
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

    private ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped, PeriodicObservations periodicObservations,
            PipelineConvergences pipelineConvergences) {
        this.scoped = scoped;
        this.periodicObservations = periodicObservations;
        this.pipelineConvergences = pipelineConvergences;
        this.converger = converger;
        this.desired = scoped == null ? Objects.requireNonNull(desired, "desired") : null;
        this.publisher = publisher;
        this.sampler = sampler;
        this.export = export == null ? MetricsExport.none() : export;
        this.reconcileEligible = reconcileEligible;
        this.actuation = actuation;
    }

    @Scheduled(fixedDelayString = "${tapstate.converge.interval-ms:1000}")
    void reconcile() {
        if (!reconcileEligible.getAsBoolean()) {
            return;
        }
        Optional<PipelineIntentStore.DispatchSnapshot> dispatch = scoped == null || pipelineConvergences == null
                ? Optional.empty() : scoped.intents().dispatchSnapshot();
        List<String> listed = dispatch.map(PipelineIntentStore.DispatchSnapshot::pipelineIds)
                .orElseGet(() -> scoped == null ? desired.pipelineIds() : scoped.intents().pipelineIds());
        List<String> pipelineIds;
        if (pipelineConvergences != null) {
            dispatchConvergences(listed, dispatch.map(PipelineIntentStore.DispatchSnapshot::stopping).orElseGet(Set::of));
            pipelineConvergences.retain(listed);
            // A deleted Pipeline keeps its claim and budgets until its original local pass has ended.
            var retained = new java.util.HashSet<>(listed);
            retained.addAll(pipelineConvergences.pipelineIds());
            pipelineIds = List.copyOf(retained);
        } else {
            listed.forEach(this::reconcilePipeline);
            pipelineIds = listed;
        }
        // Forget what is kept per pipeline for pipelines that are no longer desired, so a
        // deleted-while-failing pipeline does not leak a counter that nothing will ever clear. Both live
        // here rather than one at each end: it is one question -- which of these still exist -- and an
        // intent is removed only by the reclaim of the pipeline itself, so a merely stopped pipeline keeps
        // its intent and keeps both. The publisher's own account cannot be cleared by whoever deletes the
        // pipeline: the control ring's synchronous surface into the runtime is a closed set, and this set
        // is already crossing once a tick with the same answer in it.
        reconcileFailures.keySet().retainAll(pipelineIds);
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

    private void dispatchConvergences(List<String> listed, Set<String> stopping) {
        if (listed.isEmpty()) {
            nextRejectedPipeline = null;
            return;
        }
        int first = nextPipeline % listed.size();
        nextPipeline = (first + 1) % listed.size();
        int priority = nextRejectedPipeline == null ? -1 : listed.indexOf(nextRejectedPipeline);
        if (priority >= 0 && !offerConvergence(nextRejectedPipeline, stopping.contains(nextRejectedPipeline))) return;

        nextRejectedPipeline = null;
        for (int offset = 0; offset < listed.size(); offset++) {
            int index = (first + offset) % listed.size();
            if (index == priority) continue;
            String id = listed.get(index);
            if (!offerConvergence(id, stopping.contains(id))) {
                nextRejectedPipeline = id;
                return;
            }
        }
    }

    private boolean offerConvergence(String pipelineId, boolean stopRequested) {
        return pipelineConvergences.offer(pipelineId, () -> {
            if (pipelineConvergences.stopping(pipelineId)) return;
            if (stopRequested) pipelineConvergences.beginStop(pipelineId, () -> reconcilePipeline(pipelineId, true));
            else reconcilePipeline(pipelineId);
        }) != PipelineConvergences.Offer.REJECTED;
    }

    private void reconcilePipeline(String pipelineId) {
        reconcilePipeline(pipelineId, false);
    }

    private void reconcilePipeline(String pipelineId, boolean stopOperation) {
        if (!reconcileEligible.getAsBoolean()) return;
        // Attribute every line logged while reconciling this pipeline to it, so the logs read face can
        // tail per pipeline. Cleared per pipeline so the slot never leaks onto the next one or an idle tick.
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
        PipelineConvergencePass pass = null;
        PipelineActuationOwnership.Pass localPass = null;
        boolean periodic = false;
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
                        prepareStart(pending);
                    } else if (scoped.resumeRebuild() != null) {
                        scoped.resumeRebuild().accept(pipelineId);
                    }
                    return;
                }
                if (snapshot.get().deleted()) return;
                if (!pipelineId.equals(snapshot.get().scope().pipelineId())) {
                    throw new TapstateException(
                            IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
                }
                if (!stopOperation && pipelineConvergences != null
                        && snapshot.get().desired().targetState() == PipelineState.STOPPED) {
                    pipelineConvergences.beginStop(pipelineId, () -> reconcilePipeline(pipelineId, true));
                    return;
                }
                MDC.put(io.tapstate.core.logging.PipelineAttribution.STATE_GENERATION_MDC_KEY,
                        Long.toString(snapshot.get().scope().stateGeneration()));
                permit = actuation.permit(snapshot.get().scope());
                if (permit.granted()) {
                    localPass = Objects.requireNonNull(actuation.enterPass(snapshot.get().scope()), "local convergence pass");
                    pass = new PipelineConvergencePass(snapshot.get(), permit.claim(),
                            scoped.stores(), scoped.requireActive(), advanced -> alert(() -> scoped.alerts().expecting(advanced)));
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
            if (scoped != null) alert(() -> scoped.alerts().expecting(originalPass));
            ConvergeResult result = scoped == null ? converger.converge(pipelineId)
                    : converger.convergeScoped(pass.intent(), pass.state(), scoped.actuators().apply(pass),
                            scoped.rebuilds().apply(pass), ignored -> {
                                discardPeriodic(pipelineId);
                                writeObservationNow(pipelineId, () -> publisher.publishScoped(originalPass.scope(),
                                        originalPass.state(), originalPass.observationSample(), null));
                            },
                            () -> scoped.intents().loadForActuation(pipelineId).filter(originalPass.intent()::equals).isPresent(),
                            stopOperation);
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
            writeObservationNow(pipelineId, () -> publishObservation(pipelineId, sampledPass, result, config, failure));
            // A clean pass ends the failure streak; the next throw starts counting from one again.
            reconcileFailures.remove(pipelineId);
        } catch (RuntimeException e) {
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

    private void writeObservationNow(String pipelineId, Runnable publication) {
        if (scoped != null && scoped.writes() != null) scoped.writes().now(pipelineId, publication);
        else publication.run();
    }

    private void publishObservation(String pipelineId, PipelineConvergencePass pass, ConvergeResult result,
            PipelineIntentStore.ActuationReading config, ObservationFailure failure) {
        var projection = scoped == null ? publisher.publish(pipelineId, failure)
                : scoped.observations() == null
                        ? publisher.publishScoped(pass.scope(), pass.state(), pass.observationSample(), failure)
                        : scoped.observations().publish(publisher, pass, result, config, failure);
        projection.ifPresent(published -> {
            if (scoped != null) alert(() -> scoped.alerts().published(pass, published, result.failure().orElse(null)));
            if (scoped == null || scoped.writes() == null) sample(published, pass);
            else {
                var history = history(published, pass);
                if (history != null) scoped.writes().offerHistory(history.guardedBy(pass.guardOriginal(pass.claim())));
            }
            export(published);
        });
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

    private void prepareStart(PipelineIntentStore.PendingStart pending) {
        try {
            scoped.requireActive().run();
            scoped.prepareStart().accept(pending);
        } catch (TapstateException failure) {
            if (!(failure.code() instanceof IoError error) || (error != IoError.DOCUMENT_UNREADABLE
                    && error != IoError.PIPELINE_STATE_REBUILD_REQUIRED && error != IoError.PIPELINE_INTENT_FENCED)) throw failure;
            scoped.intents().failStart(pending, error);
        }
    }

    private void alert(Runnable action) {
        try { action.run(); }
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
