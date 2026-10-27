package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.SourceCaptureIdentity;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;

/** Fixed original position commands use normal audit and the existing runtime owners. */
final class NativeSourcePositionEdits implements PipelinePositionService.WriteBack {
    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final CacheBusinessScope business;
    private final ThreeLayerStorePort port;
    private final PipelinePositionService reader;
    private final PipelineActuationOwnership ownership;
    private final CaptureOwnership captureOwnership;
    private final Supplier<WorkloadClaimFence> node;
    private final Runnable active;
    private final Engine engine;
    private final EngineLifecycleActuator actuator;
    private final HazelcastOperatorReadiness operators;
    private final HazelcastExecutionPlans plans;
    private final Clock clock;

    NativeSourcePositionEdits(MongoConfigurationDomain configuration, MongoCacheDomain cache, CacheBusinessScope business,
            ThreeLayerStorePort port, PipelinePositionService reader, PipelineActuationOwnership ownership, CaptureOwnership captureOwnership,
            Supplier<WorkloadClaimFence> node, Runnable active, Engine engine, EngineLifecycleActuator actuator,
            HazelcastOperatorReadiness operators, HazelcastExecutionPlans plans, Clock clock) {
        this.configuration=configuration; this.cache=cache; this.business=business; this.port=port; this.reader=reader;
        this.ownership=ownership; this.captureOwnership=captureOwnership; this.node=node; this.active=active;
        this.engine=engine; this.actuator=actuator; this.operators=operators; this.plans=plans; this.clock=clock;
    }
    private record Logged(SrsChainState state, Map<SrsConsumerContext,SrsConsumerProgress> consumers, String token) {}
    private record Direct(DirectChannelProgress state, String token) {}
    private record Reader(PipelineIntentScope scope, PipelinePreparation preparation, Optional<Engine.Run> run) {}

    @Override public Runnable prepare(String pipelineId, PipelinePosition current, Map<String,String> moves) {
        active.run(); long revision=configuration.configurationRevision();
        if (!reader.read(pipelineId).equals(current)) throw fenced();
        var inventory=configuration.artifacts().list();
        var pipeline=StoredArtifacts.requirePipeline(configuration.artifacts(),pipelineId);
        var shared=cache.srs(business,active); var logged=new ArrayList<Logged>(); var direct=new ArrayList<Direct>();
        var readers=new TreeSet<String>(); readers.add(pipelineId);
        for (var move:moves.entrySet()) {
            var sourceReading=current.chains().stream().filter(item -> item.chainId().equals(move.getKey())).findFirst().orElseThrow(NativeSourcePositionEdits::fenced);
            var source=StoredArtifacts.requireSource(configuration.artifacts(),sourceReading.sourceId());
            boolean privateChannel=pipeline.sources().stream().anyMatch(ref -> ref.id().equals(source.id()) && ref instanceof SourceRef.Spec spec && !spec.srs());
            if (privateChannel) {
                var mode=pipeline.settings()==null || pipeline.settings().readMode()==null?ReadMode.SNAPSHOT_AND_CDC:pipeline.settings().readMode();
                if (mode==ReadMode.SNAPSHOT_ONLY) throw fenced();
                var preparation=DirectChannelPreparation.of(port.scope(pipelineId),source,mode,pipeline.settings()==null?null:pipeline.settings().startFrom(),sourceReading.tables());
                var before=cache.directChannels(business,active).load(preparation.context());
                if (!before.context().channelId().equals(move.getKey()) || before.cursor().sinkAcked()==null) throw fenced();
                direct.add(new Direct(before,move.getValue()));
            } else {
                var before=shared.chains().load(move.getKey()).orElseThrow(NativeSourcePositionEdits::fenced);
                if (before.checkpoint()==null) throw fenced();
                var consumers=new LinkedHashMap<SrsConsumerContext,SrsConsumerProgress>();
                for (var member:shared.consumers().attached(move.getKey()).consumers()) {
                    readers.add(member.context().key().pipelineId());
                    consumers.put(member.context(),shared.consumers().progress(member.context()).orElseThrow(NativeSourcePositionEdits::fenced));
                }
                logged.add(new Logged(before,Map.copyOf(consumers),move.getValue()));
                for (var resource:inventory) if (resource instanceof PipelineResource candidate) {
                    for (var reference:candidate.sources()) {
                        if (!(reference instanceof SourceRef.Spec spec) || !spec.srs()) continue;
                        var declared=StoredArtifacts.requireSource(configuration.artifacts(),reference.id());
                        String key=declared.srs()==null?null:declared.srs().key();
                        if (move.getKey().equals(SourceCaptureIdentity.resolve(declared.connector(),declared.config(),key))) readers.add(candidate.id());
                    }
                }
            }
        }
        var originals=new ArrayList<Reader>();
        for (String id:readers) {
            var scope=port.scope(id); var preparation=configuration.preparations().read(id).orElseThrow(NativeSourcePositionEdits::fenced);
            if (preparation.phase()==PipelinePreparation.Phase.ISSUED) throw fenced();
            requireRest(id); originals.add(new Reader(scope,preparation,engine.captureRun(id)));
        }
        if (revision!=configuration.configurationRevision()) throw fenced();
        return () -> apply(revision,List.copyOf(originals),List.copyOf(logged),List.copyOf(direct));
    }

    private void apply(long revision,List<Reader> readers,List<Logged> logged,List<Direct> direct) {
        var controls=new LinkedHashMap<Reader,PipelineActuationOwnership.Control>();
        var captures=new LinkedHashMap<String,CaptureOwnership.Permit>();
        Runnable fixed=() -> {
            active.run(); if (revision!=configuration.configurationRevision()) throw fenced();
            for (var original:readers) {
                if (!port.scope(original.scope().pipelineId()).equals(original.scope())
                        || !configuration.preparations().read(original.scope().pipelineId()).filter(original.preparation()::equals).isPresent()) throw fenced();
                requireRest(original.scope().pipelineId());
                var control=controls.get(original); if (control!=null) control.requireHeld();
                requireRun(original);
            }
        };
        Throwable primary = null;
        try {
            fixed.run();
            for (var original:readers) controls.put(original,original.preparation().phase()==PipelinePreparation.Phase.UNASSIGNED
                    ? ownership.unpreparedControl(original.scope()):ownership.control(original.scope()));
            fixed.run(); controls.values().forEach(PipelineActuationOwnership.Control::requireActive);
            for (var original:readers) {
                var claim=controls.get(original).claim(); var run=original.run();
                Optional<Engine.Run> current=run.filter(item -> item.operatorManifest().executionIdentity() instanceof ExecutionFence identity
                        && original.scope().equals(identity.scope()));
                if (current.isPresent()) {
                    var exact=current.orElseThrow();
                    ScopedPipelineCaptureCoordinator.requireNotLater((ExecutionFence)exact.operatorManifest().executionIdentity(),claim);
                    if (exact.isCarryingAJob()) exact.cancel();
                    if (!exact.awaitTerminal(Duration.ofSeconds(30))) throw new TapstateException(EngineError.JOB_STILL_ENDING,
                            Map.of("pipeline",original.scope().pipelineId(),"seconds",30),null);
                }
                fixed.run();
                var manifests=new LinkedHashSet<OperatorRunManifest>(operators.isolate(original.scope(),claim,fixed));
                current.map(Engine.Run::operatorManifest).ifPresent(manifests::add);
                for (var manifest:manifests) {
                    fixed.run(); operators.releaseHot(manifest);
                    plans.forRun((ExecutionFence)manifest.executionIdentity(),fixed).forget(original.scope().pipelineId());
                }
                operators.requireRetired(original.scope(),claim,fixed);
            }
            var names=new TreeSet<String>(); logged.forEach(item -> names.add(item.state().captureId())); direct.forEach(item -> names.add(item.state().context().captureId()));
            for (String name:names) {
                var permit=captureOwnership.acquire(new CaptureId(name));
                if (!permit.acquired() || permit.claim()==null) throw fenced();
                captures.put(name,permit);
            }
            fixed.run(); controls.values().forEach(PipelineActuationOwnership.Control::requireActive);
            var changes=new ArrayList<SourcePositionEdits.Move>();
            logged.forEach(item -> changes.add(new SourcePositionEdits.Logged(item.state(),item.consumers(),item.token(),captures.get(item.state().captureId()).fence())));
            direct.forEach(item -> changes.add(new SourcePositionEdits.Direct(item.state(),item.token(),captures.get(item.state().context().captureId()).fence())));
            var claims=new LinkedHashMap<PipelineIntentScope,WorkloadClaimFence>();
            controls.forEach((original,control) -> { if (control.claim()!=null) claims.put(original.scope(),control.claim()); });
            cache.sourcePositions(business,node.get(),fixed).rewrite(changes,claims,clock.instant());
            controls.values().stream().filter(control -> control.claim()!=null).forEach(PipelineActuationOwnership.Control::released);
            fixed.run();
        } catch (RuntimeException | Error failed) { primary = failed; throw failed; }
        finally {
            Throwable cleanup = null;
            for (var permit : captures.values()) try { captureOwnership.release(permit.claim()); }
            catch (RuntimeException | Error failed) { cleanup = combine(cleanup, failed); }
            var reverse = new ArrayList<>(controls.values()); Collections.reverse(reverse);
            for (var control : reverse) try { control.close(); }
            catch (RuntimeException | Error failed) { cleanup = combine(cleanup, failed); }
            if (cleanup != null) {
                if (primary != null) combine(primary, cleanup);
                else if (cleanup instanceof RuntimeException failed) throw failed;
                else throw (Error)cleanup;
            }
        }
    }
    private static Throwable combine(Throwable primary, Throwable next) {
        if (primary == null) return next;
        if (primary != next) primary.addSuppressed(next);
        return primary;
    }
    private void requireRun(Reader original) {
        var current=engine.captureRun(original.scope().pipelineId());
        if (current.isPresent()!=original.run().isPresent()) throw fenced();
        if (current.isEmpty()) { actuator.requireResolvedSubmission(original.scope().pipelineId(),Optional.empty()); return; }
        var observed=current.orElseThrow(); var saved=original.run().orElseThrow();
        if (observed.jobId()!=saved.jobId() || !observed.operatorManifest().equals(saved.operatorManifest())) throw fenced();
        actuator.requireResolvedSubmission(original.scope().pipelineId(),Optional.of(saved.operatorManifest()));
        if (!(saved.operatorManifest().executionIdentity() instanceof ExecutionFence identity) || identity.scope()==null) throw fenced();
        if (!original.scope().equals(identity.scope())) {
            if (saved.isCarryingAJob()) throw fenced(); configuration.requireRetiredPipelinePredecessor(original.scope(),identity.scope());
        }
    }
    private void requireRest(String pipeline) {
        var actual=port.state().read(pipeline).map(value -> StateJson.parse(value.stateJson())).orElse(PipelineState.NEW);
        var desired=port.desired().read(pipeline).map(DesiredState::targetState).orElse(PipelineState.NEW);
        if (!LivePipelines.isAtRest(actual,desired)) throw new TapstateException(PositionError.WRITE_BACK_WHILE_LIVE,Map.of("chain",pipeline,"pipelines",List.of(pipeline)),null);
    }
    private static TapstateException fenced() { return ThreeLayerStorePort.refused(); }
}
