package io.tapstate.app;

import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineStoreViews;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.Objects;
import java.util.function.Function;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.DesiredStore;
import java.util.HashMap;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
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

    /** The fixed store root and per-pass bindings; no current-scope or current-claim lookup is supplied. */
    record ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable requireActive,
            Function<PipelineConvergencePass, LifecycleActuator> actuators,
            Function<PipelineConvergencePass, RebuildAdmission> rebuilds,
            Consumer<Collection<String>> retainRebuilds, PipelineAlertFeed alerts) {
        ScopedPorts(PipelineIntentStore intents, PipelineStoreViews stores, Runnable active,
                Function<PipelineConvergencePass, LifecycleActuator> actuators,
                Function<PipelineConvergencePass, RebuildAdmission> rebuilds, Consumer<Collection<String>> retain) {
            this(intents, stores, active, actuators, rebuilds, retain, PipelineAlertFeed.none());
        }
        ScopedPorts {
            Objects.requireNonNull(intents, "intents"); Objects.requireNonNull(stores, "stores");
            Objects.requireNonNull(requireActive, "requireActive"); Objects.requireNonNull(actuators, "actuators");
            Objects.requireNonNull(rebuilds, "rebuilds"); Objects.requireNonNull(retainRebuilds, "retainRebuilds");
            Objects.requireNonNull(alerts, "alerts");
        }
    }

    private record FailureStreak(PipelineIntentScope scope, long count) {}

    // Consecutive failed-reconcile passes per pipeline, so a pipeline that keeps throwing surfaces as a
    // climbing errorCount rather than an empty read face. Reconcile runs on a single scheduler thread with a
    // fixed delay (passes never overlap), so a plain map needs no synchronization.
    private final Map<String, FailureStreak> reconcileFailures = new HashMap<>();

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
        this(converger, desired, publisher, sampler, export, reconcileEligible, actuation, null);
    }

    static ConvergenceDriver scoped(PipelineConverger converger, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped) {
        return new ConvergenceDriver(converger, null, publisher, sampler, export, reconcileEligible,
                actuation, Objects.requireNonNull(scoped, "scoped"));
    }

    private ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier reconcileEligible,
            PipelineActuationOwnership actuation, ScopedPorts scoped) {
        this.scoped = scoped;
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
        List<String> pipelineIds = scoped == null ? desired.pipelineIds() : scoped.intents().pipelineIds();
        for (String pipelineId : pipelineIds) {
            // Attribute every line logged while reconciling this pipeline to it, so the logs read face can
            // tail per pipeline. Cleared per pipeline so the slot never leaks onto the next one or an idle tick.
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
            PipelineConvergencePass pass = null;
            PipelineActuationOwnership.Pass localPass = null;
            try {
                PipelineActuationOwnership.Permit permit;
                if (scoped == null) {
                    permit = actuation.permit(pipelineId);
                } else {
                    var snapshot = scoped.intents().loadForActuation(pipelineId);
                    if (snapshot.isEmpty() || snapshot.get().deleted()) continue;
                    if (!pipelineId.equals(snapshot.get().scope().pipelineId())) {
                        throw new TapstateException(
                                IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
                    }
                    permit = actuation.permit(snapshot.get().scope());
                    if (permit.granted()) {
                        localPass = Objects.requireNonNull(actuation.enterPass(snapshot.get().scope()), "local convergence pass");
                        pass = new PipelineConvergencePass(snapshot.get(), WorkloadClaimFence.from(permit.claim()),
                                scoped.stores(), scoped.requireActive(), advanced -> alert(() -> scoped.alerts().expecting(advanced)));
                    }
                }
                if (!permit.granted()) {
                    // Another member drives this pipeline. Observe desired and actual; drive neither. Both
                    // halves matter: converging here would call the same lifecycle verb a second time --
                    // this member is carrying no job, which is exactly the condition the converge side
                    // starts one in -- and publishing here would overwrite the driver's observation with
                    // this member's own run statistics, which are absent because the run is not here.
                    continue;
                }
                PipelineConvergencePass originalPass = pass;
                if (scoped != null) alert(() -> scoped.alerts().expecting(originalPass));
                ConvergeResult result = scoped == null ? converger.converge(pipelineId)
                        : converger.convergeScoped(pass.intent(), pass.state(), scoped.actuators().apply(pass),
                                scoped.rebuilds().apply(pass), ignored -> publisher.publishScoped(originalPass.scope(),
                                        originalPass.state(), originalPass.observations(), null),
                                () -> scoped.intents().loadForActuation(pipelineId).filter(originalPass.intent()::equals).isPresent());
                ObservationFailure failure = null;
                if (result.status() == ConvergeStatus.FAILED) {
                    // The job died on its own; the converge side moved the pipeline to the observable FAILED
                    // state. This is the only pass that holds the cause, so it goes two ways: coded onto the
                    // observation, so a read face can say why, and into the log with its code and stack.
                    // Every later pass while the checkpoint stays FAILED publishes null and the publisher
                    // carries the stored reason forward — durably, so it survives a process restart too.
                    failure = PipelineFailures.of(pipelineId, result.failure().orElse(null));
                    LOG.warn("Pipeline {} entered FAILED [{}]: its data-plane job died", pipelineId,
                            failure.code(), result.failure().orElse(null));
                }
                var projection = scoped == null ? publisher.publish(pipelineId, failure)
                        : publisher.publishScoped(pass.scope(), pass.state(), pass.observations(), failure);
                PipelineConvergencePass sampledPass = pass;
                projection.ifPresent(published -> {
                    if (scoped != null) alert(() -> scoped.alerts().published(sampledPass, published, result.failure().orElse(null)));
                    sample(published, sampledPass);
                    export(published);
                });
                // A clean pass ends the failure streak; the next throw starts counting from one again.
                reconcileFailures.remove(pipelineId);
            } catch (RuntimeException e) {
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
                    else if (pass != null) publisher.publishReconcileFailureScoped(
                            pass.scope(), pass.state(), pass.observations(), failures);
                } catch (RuntimeException unpublishable) {
                    // The read face is unreachable too (e.g. the same store backs both), so nothing can be
                    // surfaced this tick; the warning above is the only record.
                    LOG.warn("Could not publish the reconcile failure for pipeline {}", pipelineId, unpublishable);
                }
            } finally {
                if (localPass != null) localPass.close();
                MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
            }
        }
        // Forget what is kept per pipeline for pipelines that are no longer desired, so a
        // deleted-while-failing pipeline does not leak a counter that nothing will ever clear. Both live
        // here rather than one at each end: it is one question -- which of these still exist -- and an
        // intent is removed only by the reclaim of the pipeline itself, so a merely stopped pipeline keeps
        // its intent and keeps both. The publisher's own account cannot be cleared by whoever deletes the
        // pipeline: the control ring's synchronous surface into the runtime is a closed set, and this set
        // is already crossing once a tick with the same answer in it.
        reconcileFailures.keySet().retainAll(pipelineIds);
        // Same for the claims: a pipeline that is gone still has this member named as its driver until the
        // lease runs out, which delays nothing but reads as an owner over something that no longer exists.
        actuation.retain(pipelineIds);
        // And the rebuilds spent on them, or a pipeline created again under a deleted one's id would start
        // with whatever the deleted one had left of its budget.
        if (scoped == null) converger.retain(pipelineIds);
        else scoped.retainRebuilds().accept(pipelineIds);
        if (scoped != null) alert(() -> scoped.alerts().retain(pipelineIds));
        publisher.forgetPipelinesOutside(pipelineIds);
        if (sampler != null) {
            sampler.forgetPipelinesOutside(pipelineIds);
        }
        export.forgetPipelinesOutside(pipelineIds);
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
    private void export(io.tapstate.core.lifecycle.Observation published) {
        try {
            export.offer(published.pipelineId(), published.state(), published.observedAt(), published.facts());
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
