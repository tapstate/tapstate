package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentStore;
import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/** Couples a running observation to its original control pass and current engine execution. */
final class NativeObservationSampling {
    private record Published(long epoch, ExecutionFence execution) {}

    private record Attempt(boolean resolved, Optional<Observation> observation) {
        static Attempt fallback() { return new Attempt(false, Optional.empty()); }
    }

    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final ScopedPipelineCaptureCoordinator captures;
    private final Function<String, Optional<ExecutionFence>> execution;
    private final Function<String, ExecutionPlan> plans;
    private final String defaultSource;
    private final Clock clock;
    private final Map<String, Published> published = new ConcurrentHashMap<>();

    NativeObservationSampling(MongoConfigurationDomain configuration, MongoCacheDomain cache, ScopedPipelineCaptureCoordinator captures,
            Function<String, Optional<ExecutionFence>> execution, Function<String, ExecutionPlan> plans, String defaultSource, Clock clock) {
        this.configuration = configuration;
        this.cache = cache;
        this.captures = captures;
        this.execution = execution;
        this.plans = plans;
        this.defaultSource = defaultSource;
        this.clock = clock;
    }

    boolean hasPublishedRunning(PipelineConvergencePass pass, ConvergeResult result) {
        var last = published.get(pass.scope().pipelineId());
        return last != null && result.checkpoint().filter(checkpoint ->
                StateJson.parse(checkpoint.stateJson()) == PipelineState.RUNNING
                        && last.equals(new Published(checkpoint.epoch(), new ExecutionFence(pass.scope(), pass.claim()))))
                .isPresent();
    }

    Optional<Observation> publish(ObservationPublisher publisher, PipelineConvergencePass pass, ConvergeResult result,
            PipelineIntentStore.ActuationReading config, ObservationFailure failure) {
        return publish(publisher, pass, result, config, failure, null);
    }

    Optional<Observation> publish(ObservationPublisher publisher, PipelineConvergencePass pass, ConvergeResult result,
            PipelineIntentStore.ActuationReading config, ObservationFailure failure,
            UnaryOperator<ObservationStore> deferred) {
        String id = pass.scope().pipelineId();
        var checkpoint = result.checkpoint().orElse(null);
        if (failure == null && checkpoint != null && StateJson.parse(checkpoint.stateJson()) == PipelineState.RUNNING) {
            var original = new ExecutionFence(pass.scope(), pass.claim());
            return publishRunning(publisher, pass, checkpoint, config, () -> true, () -> {},
                    () -> {
                        var store = pass.observationSample();
                        if (deferred != null) store = deferred.apply(guarded(store, () -> requireExecution(pass, original)));
                        return publisher.publishScoped(pass.scope(), pass.state(), store, null);
                    }, deferred);
        }
        forget(id);
        var store = pass.observationSample();
        return publisher.publishScoped(pass.scope(), pass.state(), deferred == null ? store : deferred.apply(store), failure);
    }

    Optional<Observation> publishRunning(ObservationPublisher publisher, PipelineConvergencePass pass, CheckpointDoc checkpoint,
            PipelineIntentStore.ActuationReading config, BooleanSupplier stillCurrent, Runnable requireCurrentTicket,
            Supplier<Optional<Observation>> fallback) {
        return publishRunning(publisher, pass, checkpoint, config, stillCurrent, requireCurrentTicket, fallback, null);
    }

    Optional<Observation> publishRunning(ObservationPublisher publisher, PipelineConvergencePass pass, CheckpointDoc checkpoint,
            PipelineIntentStore.ActuationReading config, BooleanSupplier stillCurrent, Runnable requireCurrentTicket,
            Supplier<Optional<Observation>> fallback, UnaryOperator<ObservationStore> deferred) {
        String id = pass.scope().pipelineId();
        var original = new ExecutionFence(pass.scope(), pass.claim());
        if (execution.apply(id).filter(original::equals).isEmpty()) {
            forget(id);
            return fallback.get();
        }
        var reading = new Published(checkpoint.epoch(), original);
        if (reading.equals(published.get(id)) && config != null && config.configurationRevision().isPresent()) {
            var fast = combined(publisher, pass, config, checkpoint, original, stillCurrent, requireCurrentTicket, deferred);
            if (fast.resolved()) return fast.observation();
        }
        var observation = fallback.get();
        if (deferred == null && observation.filter(value -> value.state() == PipelineState.RUNNING).isPresent()
                && execution.apply(id).filter(original::equals).isPresent()) {
            published.put(id, reading);
        }
        return observation;
    }

    void recordPublishedRunning(PipelineConvergencePass pass, CheckpointDoc checkpoint) {
        var original = new ExecutionFence(pass.scope(), pass.claim());
        if (execution.apply(pass.scope().pipelineId()).filter(original::equals).isPresent()) {
            published.put(pass.scope().pipelineId(), new Published(checkpoint.epoch(), original));
        }
    }

    private Attempt combined(ObservationPublisher publisher, PipelineConvergencePass pass,
            PipelineIntentStore.ActuationReading config, CheckpointDoc checkpoint, ExecutionFence original,
            BooleanSupplier stillCurrent, Runnable requireCurrentTicket, UnaryOperator<ObservationStore> deferred) {
        String id = pass.scope().pipelineId();
        var inputs = captures.observationSelection(original);
        if (inputs.isEmpty()) return Attempt.fallback();
        var selection = inputs.orElseThrow();
        Runnable current = () -> {
            requireExecution(pass, original);
            requireCurrentTicket.run();
        };
        var admitted = cache.readObservation(pass.scope(), pass.claim(), selection.sources(), current);
        if (admitted.isEmpty()) return Attempt.fallback();
        var reading = admitted.orElseThrow();
        var facts = reading.facts();
        if (!checkpoint.equals(facts.checkpoint())) return Attempt.fallback();

        var capturesByBinding = new HashMap<String, MongoConfigurationDomain.ObservationCapture>();
        selection.sources().forEach((run, source) -> capturesByBinding.put(run.sourceId(),
                new MongoConfigurationDomain.ObservationCapture(source.sourceId(),
                        selection.preparations().get(run.sourceId()).captureId())));
        var checked = configuration.observationConfiguration(config, defaultSource, capturesByBinding);
        if (checked.changed()) return new Attempt(true, Optional.empty());
        var currentConfig = checked.configuration();
        if (currentConfig.isEmpty() || !matches(currentConfig.get(), facts, selection)) return Attempt.fallback();
        current.run();
        var sample = captures.observationSample(selection, facts, plans.apply(id), clock.instant());
        if (sample.isEmpty()) return Attempt.fallback();

        ObservationStore sink = new ProcessObservationWriter.ActiveStore() {
            @Override public Runnable writeGuard() { return current; }
            @Override public Optional<Observation> read(String pipeline) { return pass.observationSample().read(pipeline); }
            @Override public void save(Observation value) { throw new IllegalArgumentException("observation checkpoint is required"); }
            @Override public void save(Observation value, long epoch) {
                current.run();
                if (!reading.updateExisting(value, epoch))
                    cache.pipelineObservations(pass.scope(), pass.claim(), current).save(value, epoch);
            }
            @Override public void delete(String pipeline) { pass.observationSample().delete(pipeline); }
        };
        return new Attempt(true, publisher.publishRunning(pass.scope(), checkpoint,
                deferred == null ? sink : deferred.apply(sink), sample.orElseThrow(), stillCurrent));
    }

    void requireExecution(PipelineConvergencePass pass, ExecutionFence original) {
        pass.requirePipeline(pass.scope().pipelineId());
        if (!execution.apply(pass.scope().pipelineId()).filter(original::equals).isPresent()) throw ThreeLayerStorePort.refused();
    }

    static ObservationStore guarded(ObservationStore store, Runnable current) {
        Runnable guard = current;
        if (store instanceof ProcessObservationWriter.ActiveStore measuredStore) {
            var measured = measuredStore.writeGuard();
            guard = () -> { current.run(); measured.run(); };
        }
        Runnable writeGuard = guard;
        return new ProcessObservationWriter.ActiveStore() {
            @Override public Runnable writeGuard() { return writeGuard; }
            @Override public void save(Observation value) { current.run(); store.save(value); }
            @Override public void save(Observation value, long epoch) { current.run(); store.save(value, epoch); }
            @Override public void afterSave(Runnable action) { store.afterSave(action); }
            @Override public Optional<Observation> read(String pipeline) { return store.read(pipeline); }
            @Override public Optional<Observation> beforeCompletion(String pipeline, long epoch) { return store.beforeCompletion(pipeline, epoch); }
            @Override public void delete(String pipeline) { store.delete(pipeline); }
        };
    }

    private static boolean matches(MongoConfigurationDomain.ObservationConfiguration current, MongoCacheDomain.ObservationFacts facts,
            ScopedPipelineCaptureCoordinator.ObservationSelection selection) {
        for (var entry : selection.sources().entrySet()) {
            var original = entry.getKey();
            var source = current.sources().get(original.sourceId());
            var prepared = selection.preparations().get(original.sourceId());
            var ref = current.pipeline().sourceReference(original.sourceId());
            var settings = io.tapstate.core.model.PipelineSourceConfig.settings(ref, source);
            if (!prepared.equals(current.captures().get(prepared.captureId()))
                    || !prepared.chainId().equals(original.miningChainId())
                    || !prepared.sourceIdentityHash().equals(SourceCaptureIdentity.physical(source.connector(), settings))) return false;
            var model = SourceDiscovery.matched(Optional.ofNullable(facts.schemas().get(source.id())), source);
            if (model == null) return false;
            var resolved = SourceCaptureResolution.forPipeline(current.pipeline(), ref, source, model);
            if (resolved.isEmpty() || !resolved.get().chainId().value().equals(prepared.canonicalChainId())
                    || !Set.copyOf(resolved.get().tables()).equals(entry.getValue().tables())) return false;
        }
        return true;
    }

    void forget(String id) { published.remove(id); }

    void retain(Collection<String> ids) { published.keySet().retainAll(ids); }
}
