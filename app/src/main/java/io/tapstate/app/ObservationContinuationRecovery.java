package io.tapstate.app;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/** Cold continuation reads run on the existing latest worker, independently of lifecycle actuation. */
final class ObservationContinuationRecovery {
    record ResolvedTarget(ObservationScopeRegistry.ContinuationKey key,
            ObservationScopeRegistry.ActualTarget target, ObservationStore.StoredContinuation stored,
            CheckpointDoc checkpoint, BooleanSupplier current) { }

    private final ObservationScopeRegistry scopes;
    private final ObservationStore observations;
    private final StateStore states;
    private final DesiredStore desired;
    private final ArtifactStore artifacts;
    private final LifecycleActuator actuator;
    private final Engine engine;

    ObservationContinuationRecovery(ObservationScopeRegistry scopes, ObservationStore observations,
            StateStore states, DesiredStore desired, ArtifactStore artifacts,
            LifecycleActuator actuator, Engine engine) {
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.states = Objects.requireNonNull(states, "states");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.actuator = Objects.requireNonNull(actuator, "actuator");
        this.engine = Objects.requireNonNull(engine, "engine");
    }

    /** Only cold or unresolved handoffs read storage; a known active target publishes from its bounded local state. */
    boolean prepareHandoff(String id, ObservationStore.Scope requestedScope, BooleanSupplier owner) {
        var local = scopes.activeContinuationTarget(id);
        if (local.filter(target -> target.scope().equals(requestedScope)).isPresent()
                && scopes.continuing(id, requestedScope)) {
            var activeKey = scopes.activeContinuationKey(id).orElseThrow();
            var activeTarget = local.orElseThrow();
            var identity = new HandoffIdentity(id, activeKey.token(), activeKey.policy(), activeKey.sourceScope(),
                    activeTarget.scope(), activeTarget.job());
            if (scopes.durableReceipt(id, identity).isPresent()) { return qualifiedTarget(id, activeTarget, owner); }
        }
        if (!owner.getAsBoolean()) { return false; }
        var marker = states.supportsStopReservations() ? states.readStopReservation(id).orElse(null) : null;
        if (marker == null || marker.legacy() || marker.counterPolicy() != StopReservation.CounterPolicy.CONTINUE) {
            return true;
        }
        BooleanSupplier current = () -> owner.getAsBoolean()
                && states.readStopReservation(id).filter(marker::equals).isPresent()
                && desired.read(id).filter(marker.originalDesired()::equals).isPresent()
                && Objects.equals(marker.writerAuthority(), actuator.stopAuthority(id).orElse(null));
        if (!current.getAsBoolean()) { return false; }
        // Failed reads stay retryable. They cannot prove that a known predecessor had no counters.
        Optional<ObservationStore.StoredContinuation> stored = observations.readContinuation(id);
        Optional<ObservationStore.Stored> publicSource = observations.readStored(id);
        if (!current.getAsBoolean()) { return false; }
        var key = new ObservationScopeRegistry.ContinuationKey(marker.token(), marker.source().scope(),
                marker.counterPolicy(), marker.writerAuthority());
        var target = marker.phase() == StopReservation.Phase.SUCCESSOR_BOUND
                ? new ObservationScopeRegistry.ActualTarget(marker.successor().scope(), marker.successor().job()) : null;
        Optional<ObservationContinuation.Target> admittedTarget = marker.successor() == null ? Optional.empty()
                : Optional.of(new ObservationContinuation.Target(marker.successor().scope(),
                        Optional.ofNullable(marker.successor().job())));
        boolean sameBound = target != null && stored.filter(saved -> saved.receipt().knownBaseline()
                && saved.receipt().matches(marker.handoffIdentity())).isPresent();
        Optional<ObservationScopeRegistry.SourceSnapshot> frozen = Optional.empty();
        if (!sameBound) {
            // The exact verified carrier pins one prior real target across admission in this same handoff.
            Optional<ObservationScopeRegistry.ActualTarget> previous = stored
                    .filter(saved -> saved.continuation().token().equals(marker.token())
                            && Objects.equals(saved.continuation().sourceScope(), marker.source().scope()))
                    .flatMap(saved -> saved.continuation().target())
                    .filter(bound -> bound.realJob().isPresent()
                            && marker.writerAuthority() != null
                            && bound.scope().executionGeneration() <= marker.writerAuthority().executionGeneration()
                            && (target == null || bound.scope().executionGeneration() < target.scope().executionGeneration()))
                    .map(bound -> new ObservationScopeRegistry.ActualTarget(bound.scope(), bound.realJob().orElseThrow()));
            var sourceTicket = scopes.beginSourceContinuation(id, key, previous, current).orElse(null);
            if (sourceTicket == null) { return false; }
            var source = scopes.prepareSourceContinuation(sourceTicket, publicSource,
                    stored.map(ObservationStore.StoredContinuation::continuation), ObservationScopeRegistry.SourceReadStatus.READABLE);
            if (source.status() == ObservationScopeRegistry.PreparationStatus.INVALIDATED) { return false; }
            frozen = source.snapshot();
            if (frozen.isPresent()) {
                ObservationContinuation next = frozen.orElseThrow().continuation();
                if (admittedTarget.isPresent()) {
                    // A STOPPED frame for an admitted scope must retain its source floor even before
                    // the native Job exists. Admission pins the carrier without inventing a producer.
                    next = new ObservationContinuation(next.token(), next.sourceScope(),
                            admittedTarget,
                            next.baselineOrigin(), next.baselineFacts(), next.producerStates());
                }
                if (keepsUnsubmittedCarrier(marker, stored)) {
                    // The retired slot still protects this floor from a STOPPED frame at the same
                    // generation. Keep its verified carrier until an actual next slot can retarget it.
                    if (!scopes.continuationAttached(sourceTicket, stored.orElseThrow().receipt())
                            || !current.getAsBoolean()) { return false; }
                } else {
                    var receipt = observations.saveContinuation(id, marker, stored.map(ObservationStore.StoredContinuation::receipt), next);
                    if (receipt.isEmpty() || !current.getAsBoolean()) { return false; }
                    stored = observations.readContinuation(id);
                    if (stored.isEmpty() || !stored.orElseThrow().receipt().equals(receipt.orElseThrow())) { return false; }
                }
            }
        }
        if (target == null || requestedScope == null) { return current.getAsBoolean(); }
        if (!target.scope().equals(requestedScope) || !qualifiedTarget(id, target, current)) { return false; }
        var targetTicket = scopes.beginTargetContinuation(id, key, target, current).orElse(null);
        if (targetTicket == null) { return false; }
        var adopted = scopes.adoptTargetContinuation(targetTicket,
                stored.map(ObservationStore.StoredContinuation::continuation), frozen);
        if (adopted.status() == ObservationScopeRegistry.PreparationStatus.INVALIDATED) { return false; }
        stored.filter(saved -> saved.receipt().matches(marker.handoffIdentity()))
                .ifPresent(saved -> scopes.continuationRead(targetTicket, saved));
        return current.getAsBoolean();
    }

    private static boolean keepsUnsubmittedCarrier(StopReservation marker,
            Optional<ObservationStore.StoredContinuation> stored) {
        if (marker.phase() != StopReservation.Phase.REPLACEMENT_PENDING || marker.writerAuthority() == null) { return false; }
        return stored.filter(saved -> saved.receipt().knownBaseline()
                && saved.continuation().token().equals(marker.token())
                && Objects.equals(saved.continuation().sourceScope(), marker.source().scope())
                && saved.continuation().target().filter(target -> target.realJob().isEmpty()
                        && target.scope().pipelineIncarnationId().equals(marker.source().scope().pipelineIncarnationId())
                        && target.scope().executionGeneration() == marker.writerAuthority().executionGeneration()).isPresent()).isPresent();
    }

    boolean adoptExisting(ResolvedTarget resolved) {
        var ticket = scopes.beginTargetContinuation(resolved.checkpoint().pipelineId(), resolved.key(),
                resolved.target(), resolved.current()).orElse(null);
        if (ticket == null) { return false; }
        return scopes.adoptTargetContinuation(ticket, Optional.of(resolved.stored().continuation()), Optional.empty()).status()
                != ObservationScopeRegistry.PreparationStatus.INVALIDATED
                && scopes.continuationRead(ticket, resolved.stored());
    }

    Optional<ObservationScopeRegistry.ActualTarget> measuredTarget(String id, ObservationStore.Scope scope,
            BooleanSupplier current) {
        return scopes.activeContinuationTarget(id).filter(target -> target.scope().equals(scope))
                .filter(target -> qualifiedTarget(id, target, current));
    }

    private boolean qualifiedTarget(String id, ObservationScopeRegistry.ActualTarget target, BooleanSupplier current) {
        if (!current.getAsBoolean()) { return false; }
        var nativeJob = engine.executionJob(id);
        if (nativeJob.isPresent()) {
            return nativeJob.filter(job -> target.scope().equals(job.scope()) && target.job().equals(job.job())).isPresent()
                    && current.getAsBoolean();
        }
        // A concluded execution keeps its previously recorded actual identity for final known facts.
        // Native absence contributes no new sample or producer epoch.
        StopAuthority authority = actuator.stopAuthority(id).orElse(null);
        return states.read(id).filter(checkpoint -> terminal(checkpoint)).isPresent()
                && authority != null && authority.executionGeneration() == target.scope().executionGeneration()
                && authority.clusterId().equals(target.job().clusterId())
                && artifacts.pipelineIncarnationId(id).filter(target.scope().pipelineIncarnationId()::equals).isPresent()
                && engine.noUnfinishedJob(id) && current.getAsBoolean();
    }

    private static boolean terminal(CheckpointDoc checkpoint) {
        PipelineState actual = StateJson.parse(checkpoint.stateJson());
        return actual == PipelineState.FAILED || actual == PipelineState.COMPLETED;
    }

    ObservationStore.PublicationResult publish(io.tapstate.core.lifecycle.Observation observation,
            ObservationStore.Scope scope, ObservationStore.ContinuationWrite write) {
        return observations.saveScoped(observation, scope, write);
    }

    /** Same-job recovery loads its private base and native checkpoint, never its public cumulative total as a base. */
    Optional<ResolvedTarget> resolveExisting(String id, BooleanSupplier owner) {
        if (!owner.getAsBoolean()) { return Optional.empty(); }
        var marker = states.supportsStopReservations() ? states.readStopReservation(id) : Optional.<StopReservation>empty();
        var intent = desired.read(id);
        var checkpoint = states.read(id).orElse(null);
        StopAuthority authority = actuator.stopAuthority(id).orElse(null);
        var saved = observations.readContinuation(id).orElse(null);
        var nativeJob = engine.executionJob(id);
        Engine.ExecutionJob historical = null;
        if (nativeJob.isEmpty() && checkpoint != null && terminal(checkpoint) && saved != null
                && engine.noUnfinishedJob(id)) {
            historical = saved.continuation().target().filter(target -> target.realJob().isPresent())
                    .map(target -> new Engine.ExecutionJob(target.realJob().orElseThrow(), target.scope())).orElse(null);
        }
        Engine.ExecutionJob job = nativeJob.orElse(historical);
        if (checkpoint == null || authority == null || saved == null || job == null
                || authority.executionGeneration() != job.scope().executionGeneration()
                || !authority.clusterId().equals(job.job().clusterId())
                || artifacts.pipelineIncarnationId(id).filter(job.scope().pipelineIncarnationId()::equals).isEmpty()
                || saved.continuation().target().filter(target -> target.scope().equals(job.scope())
                        && target.realJob().filter(job.job()::equals).isPresent()).isEmpty()) { return Optional.empty(); }
        if (marker.isPresent() && (marker.orElseThrow().phase() != StopReservation.Phase.SUCCESSOR_BOUND
                || marker.orElseThrow().counterPolicy() != StopReservation.CounterPolicy.CONTINUE
                || !saved.receipt().matches(marker.orElseThrow().handoffIdentity()))) { return Optional.empty(); }
        BooleanSupplier current = () -> owner.getAsBoolean()
                && Objects.equals(authority, actuator.stopAuthority(id).orElse(null))
                && states.read(id).filter(checkpoint::equals).isPresent()
                && desired.read(id).equals(intent)
                && (!states.supportsStopReservations() || states.readStopReservation(id).equals(marker))
                && artifacts.pipelineIncarnationId(id).filter(job.scope().pipelineIncarnationId()::equals).isPresent()
                && qualifiedTarget(id, new ObservationScopeRegistry.ActualTarget(job.scope(), job.job()), owner);
        if (!current.getAsBoolean()) { return Optional.empty(); }
        return Optional.of(new ResolvedTarget(new ObservationScopeRegistry.ContinuationKey(
                saved.continuation().token(), saved.continuation().sourceScope(),
                StopReservation.CounterPolicy.CONTINUE, authority),
                new ObservationScopeRegistry.ActualTarget(job.scope(), job.job()), saved, checkpoint, current));
    }
}
