package io.tapstate.app;

import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.store.DesiredStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // Consecutive failed-reconcile passes per pipeline, so a pipeline that keeps throwing surfaces as a
    // climbing errorCount rather than an empty read face. Reconcile runs on a single scheduler thread with a
    // fixed delay (passes never overlap), so a plain map needs no synchronization.
    private final Map<String, Long> reconcileFailures = new HashMap<>();

    private final RateSampler sampler;

    /** A driver that keeps no history, for the cases that are about convergence alone. */
    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher) {
        this(converger, desired, publisher, null);
    }

    ConvergenceDriver(PipelineConverger converger, DesiredStore desired, ObservationPublisher publisher,
            RateSampler sampler) {
        this.converger = converger;
        this.desired = desired;
        this.publisher = publisher;
        this.sampler = sampler;
    }

    @Scheduled(fixedDelayString = "${tapstate.converge.interval-ms:1000}")
    void reconcile() {
        List<String> pipelineIds = desired.pipelineIds();
        for (String pipelineId : pipelineIds) {
            // Attribute every line logged while reconciling this pipeline to it, so the logs read face can
            // tail per pipeline. Cleared per pipeline so the slot never leaks onto the next one or an idle tick.
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
            try {
                ConvergeResult result = converger.converge(pipelineId);
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
                publisher.publish(pipelineId, failure).ifPresent(this::sample);
                // A clean pass ends the failure streak; the next throw starts counting from one again.
                reconcileFailures.remove(pipelineId);
            } catch (RuntimeException e) {
                // A pass that keeps throwing never reaches publish(), so the read face would stay empty and a
                // permanently broken pipeline would look identical to a slow one. Count the consecutive
                // failures and publish them as an error observation so the failure is observable, not just
                // logged. Isolate this pipeline: one bad pipeline must not starve the rest of the pass.
                long failures = reconcileFailures.merge(pipelineId, 1L, Long::sum);
                LOG.warn("Reconcile pass for pipeline {} failed {} time(s) in a row; retrying on the next tick",
                        pipelineId, failures, e);
                try {
                    publisher.publishReconcileFailure(pipelineId, failures);
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
        publisher.forgetPipelinesOutside(pipelineIds);
        if (sampler != null) {
            sampler.forgetPipelinesOutside(pipelineIds);
        }
    }

    /**
     * Takes a sample off exactly what was published, at the time it was published, so the history and the
     * latest state never describe different passes of the run. The observation is the contract and the
     * history a record kept beside it: a history that cannot be written must not cost a pipeline the read
     * face that says it is alive at all.
     */
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
