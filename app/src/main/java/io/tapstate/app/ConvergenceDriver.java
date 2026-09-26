package io.tapstate.app;

import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.control.core.PipelineExplanation.PendingReason;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.DesiredStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final BooleanSupplier businessEligible;
    private final PipelineActuationOwnership actuation;
    private final LifecycleWorkDispatcher lifecycleWork;
    private final ObservationScopeRegistry observationScopes;
    private final TelemetryDispatcher telemetryWork;
    private final LifecyclePendingRegistry pendingWork;

    // Consecutive failed-reconcile passes per pipeline, so a pipeline that keeps throwing surfaces as a
    // climbing errorCount rather than an empty read face. Reconcile runs on a single scheduler thread with a
    // fixed delay (passes never overlap), so a plain map needs no synchronization.
    private final Map<String, Long> reconcileFailures = new HashMap<>();
    // The queue is bounded. Rotate the first pipeline each tick so a stable list of quick jobs cannot
    // refill every slot before a later pipeline in that same list is ever offered one.
    private long dispatchTurn;

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
            BooleanSupplier businessEligible) {
        this(converger, desired, publisher, businessEligible, PipelineActuationOwnership.single());
    }

    /** The same, driving only the pipelines this member holds the actuation claim for. */
    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            BooleanSupplier businessEligible, PipelineActuationOwnership actuation) {
        this(converger, desired, publisher, null, MetricsExport.none(), businessEligible, actuation);
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier businessEligible,
            PipelineActuationOwnership actuation) {
        this(converger, desired, publisher, sampler, export, businessEligible, actuation,
                LifecycleWorkDispatcher.inline());
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier businessEligible,
            PipelineActuationOwnership actuation, LifecycleWorkDispatcher lifecycleWork) {
        this(converger, desired, publisher, sampler, export, businessEligible, actuation, lifecycleWork, null);
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier businessEligible,
            PipelineActuationOwnership actuation, LifecycleWorkDispatcher lifecycleWork,
            ObservationScopeRegistry observationScopes) {
        this(converger, desired, publisher, sampler, export, businessEligible, actuation, lifecycleWork,
                observationScopes, null);
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier businessEligible,
            PipelineActuationOwnership actuation, LifecycleWorkDispatcher lifecycleWork,
            ObservationScopeRegistry observationScopes, TelemetryDispatcher telemetryWork) {
        this(converger, desired, publisher, sampler, export, businessEligible, actuation, lifecycleWork,
                observationScopes, telemetryWork, null);
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler, MetricsExport export, BooleanSupplier businessEligible,
            PipelineActuationOwnership actuation, LifecycleWorkDispatcher lifecycleWork,
            ObservationScopeRegistry observationScopes, TelemetryDispatcher telemetryWork,
            LifecyclePendingRegistry pendingWork) {
        this.converger = converger;
        this.desired = desired;
        this.publisher = publisher;
        this.sampler = sampler;
        this.export = export == null ? MetricsExport.none() : export;
        this.businessEligible = businessEligible;
        this.actuation = actuation;
        this.lifecycleWork = lifecycleWork;
        this.observationScopes = observationScopes;
        this.telemetryWork = telemetryWork;
        this.pendingWork = pendingWork;
    }

    @Scheduled(fixedDelayString = "${tapstate.converge.interval-ms:1000}")
    void reconcile() {
        if (!businessEligible.getAsBoolean()) {
            lifecycleWork.cancelAll();
            if (pendingWork != null) {
                pendingWork.clearAll();
            }
            return;
        }
        List<String> pipelineIds = desired.pipelineIds();
        int first = pipelineIds.isEmpty() ? 0 : (int) Math.floorMod(dispatchTurn++, pipelineIds.size());
        for (int at = 0; at < pipelineIds.size(); at++) {
            String pipelineId = pipelineIds.get((first + at) % pipelineIds.size());
            // Attribute every line logged while reconciling this pipeline to it, so the logs read face can
            // tail per pipeline. Cleared per pipeline so the slot never leaks onto the next one or an idle tick.
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
            try {
                PipelineActuationOwnership.Permit permit = actuation.permit(pipelineId);
                if (permit.retry()) {
                    // This pipeline's execution generation is being advanced on a worker. Waiting for
                    // its per-pipeline lock would stall every later pipeline in this scheduler pass;
                    // treating that short wait as lost ownership would cancel the worker holding it.
                    publish(pipelineId, null).ifPresent(published -> {
                        sample(published);
                        export(published);
                    });
                    continue;
                }
                if (!permit.granted()) {
                    // Another member drives this pipeline. Observe desired and actual; drive neither. Both
                    // halves matter: converging here would call the same lifecycle verb a second time --
                    // this member is carrying no job, which is exactly the condition the converge side
                    // starts one in -- and publishing here would overwrite the driver's observation with
                    // this member's own run statistics, which are absent because the run is not here.
                    lifecycleWork.cancel(pipelineId);
                    if (pendingWork != null) {
                        pendingWork.clear(pipelineId);
                    }
                    continue;
                }
                LifecycleWorkDispatcher.Outcome completed = lifecycleWork.take(pipelineId);
                if (completed != null && pendingWork != null) {
                    pendingWork.clear(pipelineId);
                }
                if (completed == null || completed.superseded()) {
                    DesiredState intent = desired.read(pipelineId).orElse(null);
                    if (intent == null) {
                        lifecycleWork.cancel(pipelineId);
                        if (pendingWork != null) {
                            pendingWork.clear(pipelineId);
                        }
                        continue;
                    }
                    LifecycleWorkDispatcher.Submission submission = lifecycleWork.offer(
                            pipelineId, intent, () -> converger.converge(pipelineId));
                    notePending(pipelineId, intent, submission);
                    if (submission == LifecycleWorkDispatcher.Submission.CAPACITY) {
                        LOG.debug("Lifecycle work for pipeline {} is waiting for dispatcher capacity", pipelineId);
                    }
                    // Inline fixtures finish now; a real worker normally leaves this empty until a later
                    // tick. In both cases the same result path publishes the reconciled state.
                    completed = lifecycleWork.take(pipelineId);
                    if (completed != null && pendingWork != null) {
                        pendingWork.clear(pipelineId);
                    }
                }
                ObservationFailure failure = null;
                if (completed != null && completed.failure() != null) {
                    if (completed.failure() instanceof Error error) {
                        throw error;
                    }
                    if (completed.failure() instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new IllegalStateException("lifecycle work failed with a checked throwable",
                            completed.failure());
                }
                ConvergeResult result = completed == null ? null : completed.result();
                if (result != null && result.status() == ConvergeStatus.FAILED) {
                    // The job died on its own; the converge side moved the pipeline to the observable FAILED
                    // state. This is the only pass that holds the cause, so it goes two ways: coded onto the
                    // observation, so a read face can say why, and into the log with its code and stack.
                    // Every later pass while the checkpoint stays FAILED publishes null and the publisher
                    // carries the stored reason forward — durably, so it survives a process restart too.
                    failure = PipelineFailures.of(pipelineId, result.failure().orElse(null));
                    LOG.warn("Pipeline {} entered FAILED [{}]: its data-plane job died", pipelineId,
                            failure.code(), result.failure().orElse(null));
                }
                publish(pipelineId, failure).ifPresent(published -> {
                    sample(published);
                    export(published);
                });
                // A completed clean reconciliation, rather than an idle tick while work is pending,
                // ends the failure streak. Pending work has not yet shown it can succeed.
                if (result != null) {
                    reconcileFailures.remove(pipelineId);
                }
            } catch (RuntimeException e) {
                if (pendingWork != null) {
                    pendingWork.clear(pipelineId);
                }
                // A pass that keeps throwing never reaches publish(), so the read face would stay empty and a
                // permanently broken pipeline would look identical to a slow one. Count the consecutive
                // failures and publish them as an error observation so the failure is observable, not just
                // logged. Isolate this pipeline: one bad pipeline must not starve the rest of the pass.
                long failures = reconcileFailures.merge(pipelineId, 1L, Long::sum);
                LOG.warn("Reconcile pass for pipeline {} failed {} time(s) in a row; retrying on the next tick",
                        pipelineId, failures, e);
                try {
                    if (telemetryWork != null) {
                        if (observationScopes == null) {
                            telemetryWork.offerReconcileFailure(pipelineId, failures, null);
                        } else {
                            observationScopes.current(pipelineId).ifPresent(scope ->
                                    telemetryWork.offerReconcileFailure(pipelineId, failures, scope));
                        }
                    } else if (observationScopes == null) {
                        publisher.publishReconcileFailure(pipelineId, failures);
                    } else {
                        observationScopes.current(pipelineId).ifPresent(scope ->
                                publisher.publishReconcileFailureScoped(pipelineId, failures, scope));
                    }
                } catch (RuntimeException unpublishable) {
                    // The read face is unreachable too (e.g. the same store backs both), so nothing can be
                    // surfaced this tick; the warning above is the only record.
                    LOG.warn("Could not publish the reconcile failure for pipeline {}", pipelineId, unpublishable);
                }
            } finally {
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
        lifecycleWork.retain(pipelineIds);
        if (pendingWork != null) {
            pendingWork.retain(pipelineIds);
        }
        // Same for the claims: a pipeline that is gone still has this member named as its driver until the
        // lease runs out, which delays nothing but reads as an owner over something that no longer exists.
        actuation.retain(pipelineIds);
        publisher.forgetPipelinesOutside(pipelineIds);
        if (observationScopes != null) {
            observationScopes.retain(pipelineIds);
        }
        if (telemetryWork != null) {
            telemetryWork.retain(pipelineIds);
        }
        if (sampler != null) {
            sampler.forgetPipelinesOutside(pipelineIds);
        }
        export.forgetPipelinesOutside(pipelineIds);
    }

    private void notePending(String pipelineId, DesiredState intent, LifecycleWorkDispatcher.Submission submission) {
        if (pendingWork == null) {
            return;
        }
        if (intent.targetState() == PipelineState.RUNNING) {
            pendingWork.put(pipelineId, submission == LifecycleWorkDispatcher.Submission.CAPACITY
                    ? PendingReason.START_CAPACITY : PendingReason.START_PENDING);
        } else if (intent.targetState() == PipelineState.STOPPED) {
            pendingWork.put(pipelineId, submission == LifecycleWorkDispatcher.Submission.CAPACITY
                    ? PendingReason.STOP_CAPACITY : PendingReason.STOP_PENDING);
        } else {
            pendingWork.clear(pipelineId);
        }
    }

    private Optional<io.tapstate.core.lifecycle.Observation> publish(String pipelineId, ObservationFailure failure) {
        if (telemetryWork != null) {
            Optional<io.tapstate.spi.store.ObservationStore.Scope> scope = observationScopes == null
                    ? Optional.empty() : observationScopes.current(pipelineId);
            if (observationScopes != null && scope.isEmpty()) {
                return Optional.empty();
            }
            (scope.isPresent()
                    ? publisher.prepareScoped(pipelineId, failure, scope.get())
                    : publisher.prepare(pipelineId, failure))
                    .ifPresent(prepared -> telemetryWork.offer(prepared, scope.orElse(null)));
            return Optional.empty();
        }
        if (observationScopes == null) {
            return publisher.publish(pipelineId, failure);
        }
        return observationScopes.current(pipelineId)
                .flatMap(scope -> publisher.publishScoped(pipelineId, failure, scope));
    }

    /** Offers the same measured facts to export on the inline compatibility path. */
    private void export(io.tapstate.core.lifecycle.Observation published) {
        try {
            export.offer(published.pipelineId(), published.state(), published.observedAt(), published.facts());
        } catch (RuntimeException unexported) {
            LOG.warn("Could not offer the facts of pipeline {} for export", published.pipelineId(), unexported);
        }
    }

    /** Takes a sample from the same frame on the inline compatibility path. */
    private void sample(io.tapstate.core.lifecycle.Observation published) {
        if (sampler == null) {
            return;
        }
        try {
            sampler.offer(published);
        } catch (RuntimeException unsampled) {
            LOG.warn("Could not sample the history of pipeline {}", published.pipelineId(), unsampled);
        }
    }
}
