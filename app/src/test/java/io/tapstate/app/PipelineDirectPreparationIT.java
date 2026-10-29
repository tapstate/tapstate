package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.capture.*;
import io.tapstate.runtime.srs.*;
import io.tapstate.core.event.Envelope;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.io.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.*;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Native preparation and producer/writer recovery over separate replica sets; no registered connector, running Jet job or target I/O is exercised. */
@RequiresDocker
class PipelineDirectPreparationIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(60);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node","boot");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-19T00:00:00Z"),ZoneOffset.UTC);

    @Test void retainedSharedStartsResolveTheirOwnCursorAndRestartKeepsIt() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,"latest",null));
            var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());var claims=f.cache.workloadClaims(f.business());
            var fence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
            var stores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(stores.log());var ports=new CopyOnWriteArrayList<NativeSharedPort>();
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),stores,member);
                var root=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,cacheOwnership(f,OWNER),Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,new SnapshotBuffer(8),fixtureProvisioner(),(fixed,config,guard,preparation,stream) -> {
                            var port=new NativeSharedPort(config,guard,preparation,stream);ports.add(port);return port;
                        })) {
                var first=root.forRun(f.scope,fence,new ExecutionFence(f.scope,fence));first.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),models);
                var physical=ports.getFirst();
                physical.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",1),null)),Optional.of(new SourcePosition("one")));
                physical.listener.get().onBatch(List.of(Envelope.insert(3,"orders",Map.of("id",2),null)),Optional.of(new SourcePosition("two")));
                var captured=first.captureBindings().logged().get("input");
                var events=stores.log().readBatch(SrsRingbuffer.ringName(captured.run().miningChainId(),"orders"),0,2).records();
                assertThat(events).containsOnlyKeys(0L,1L).allSatisfy((sequence,event) -> assertThat(event.op()).isEqualTo(io.tapstate.core.event.Op.INSERT));
                assertThat(events.values()).extracting(event -> ((Number)event.after().get("id")).intValue()).containsExactly(1,2);
                for(String start:List.of("earliest",Instant.ofEpochMilli(2).toString(),"latest")) {
                    String id="reader-"+(start.equals("earliest")?"earliest":start.equals("latest")?"latest":"time");
                    var pipeline=new PipelineResource(id,null,List.of(SourceRef.spec("input",true)),null,null,null,new Settings(null,null,null,null,ReadMode.CDC_ONLY,start),null);
                    f.configuration.artifacts().create(pipeline);var scope=new PipelineIntentScope("cluster",1,f.scope.cacheIncarnation(),id,1);
                    var discovered=SourceModelSnapshot.capture(id,List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
                    var inputs=discovered.preparationInputs(scope,pipeline,pipeline,Map.of("input",f.source()));f.preparer(f.configuration,f.cache).prepare(scope,inputs);
                    var original=claims.advanceExecution(claims.readPipeline(scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow();var claim=WorkloadClaimFence.from(original);
                    var run=root.forRun(scope,claim,new ExecutionFence(scope,claim));run.startCapture(id,ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),discovered);
                    var saved=run.captureBindings().logged().get("input");long expected=start.equals("earliest")?-1:start.equals("latest")?stores.log().bounds(SrsRingbuffer.ringName(saved.run().miningChainId(),"orders")).largestSequence():0;
                    assertThat(saved.recoveryAfter()).containsEntry("orders",expected);run.stopCapture(id,false);
                    physical.listener.get().onBatch(List.of(Envelope.insert(5,"orders",Map.of("id",3),null)),Optional.of(new SourcePosition("after-"+id)));
                    var next=WorkloadClaimFence.from(claims.advanceExecution(original,1,Set.of(OWNER.nodeId())).orElseThrow());
                    var restart=root.forRun(scope,next,new ExecutionFence(scope,next));restart.startCapture(id,ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),discovered);
                    assertThat(restart.captureBindings().logged().get("input").recoveryAfter()).isEqualTo(saved.recoveryAfter());restart.stopCapture(id,false);
                }
                first.stopCapture("flow",false);
            } finally {member.shutdown();}
        });
    }
    @ParameterizedTest @ValueSource(strings={"complete","lost-d-reply","new-start"})
    void nativeSharedRecoveryFinalizesTheEndedOriginalAndResumesItsExactAppliedReceipt(String mode) {
        nativeSharedRecoveryCase(mode);
    }
    @Test void aQuarantinedSharedRingReopensAtItsActualHighWatermarkWithoutRestoringBusinessAccess() {
        nativeSharedRecoveryCase("ring-reopen");
    }
    private void nativeSharedRecoveryCase(String mode) {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.SNAPSHOT_AND_CDC,"latest",null));
            var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());
            f.configuration.bindings().complete(new StorageSetupScope("cluster","installation","setup",1));
            f.configuration.firstAdmin().create(f.configuration.firstAdmin().load().orElseThrow().scope(),new User("admin","fixture-hash","admin"));
            var claims=f.cache.workloadClaims(f.business());
            var fence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
            var stores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(stores.log(),true);var ports=new CopyOnWriteArrayList<NativeSharedPort>();
            var properties=new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            var gate=new ClusterMembershipGate(properties);gate.observeNodeSession(() -> true);gate.install(new ClusterMembership("cluster",1,Set.of("node","node-b","fixture-third")));gate.canCommit(Set.of("node","node-b"));
            var ownership=PipelineActuationOwnership.scoped(f.business(),OWNER,gate,new ClusterWorkloadClaims(claims,gate),TTL,Duration.ofSeconds(10),System::nanoTime);
            var engine=mock(io.tapstate.runtime.engine.Engine.class);var actuator=mock(EngineLifecycleActuator.class);
            var configuration=spy(f.configuration);
            var failD=new java.util.concurrent.atomic.AtomicBoolean(mode.equals("lost-d-reply"));
            doReturn(failingCompletion(f.configuration.sharedRecoveryPreparations(),failD)).when(configuration).sharedRecoveryPreparations();
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),stores,member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,cacheOwnership(f,OWNER),Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,new SnapshotBuffer(8),fixtureProvisioner(),(fixed,config,guard,preparation,stream) -> {var port=new NativeSharedPort(config,guard,preparation,stream);ports.add(port);return port;});
                var bindings=io.tapstate.runtime.engine.OperatorMapBindings.bindTo(member,(ignored,guard) -> {throw new AssertionError("no operator maps in this fixture");})) {
                HazelcastOperatorReadiness.bindRetirement(member,coordinator,ACTIVE);
                var run=coordinator.forRun(f.scope,fence,new ExecutionFence(f.scope,fence));
                run.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),models);awaitLoadedCounts(run);
                var chain=run.captureBindings().logged().get("input").run().miningChainId();
                var stream=ports.stream().filter(port -> port.stream!=null).findFirst().orElseThrow();
                stream.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",2),null)),Optional.of(new SourcePosition("before-loss")));
                if(mode.equals("ring-reopen"))stream.listener.get().onBatch(List.of(Envelope.insert(2,"orders",Map.of("id",3),null),
                        Envelope.insert(3,"orders",Map.of("id",4),null)),Optional.of(new SourcePosition("retained-old-high-watermark")));
                String oldRing=SrsRingbuffer.ringName(chain,"orders");long oldHigh=stores.log().largestSequence(oldRing);
                status(f,f.scope,fence,"STOPPED");
                assertThat(f.configuration.intents().compareAndSet(f.scope,0,new io.tapstate.core.lifecycle.DesiredState("flow",
                        io.tapstate.core.lifecycle.PipelineState.STOPPED,io.tapstate.core.model.canonical.CanonicalHash.of(f.pipeline()),false))).isTrue();
                var bodies=f.cacheDatabase.getCollection("srs_models").find(new Document("_id.chain",chain)).into(new ArrayList<>());
                assertThat(bodies).hasSize(2);for(var body:bodies)f.cacheDatabase.getCollection("srs_models").deleteOne(new Document("_id",body.get("_id")));
                var auth=f.configuration.bindings().cachePreparation().orElseThrow().authScope();
                var selection=new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,Set.of(),chain,chain);
                try(var recovery=new NativeSharedSrsRecovery(configuration,f.cache,auth,ownership,() -> f.node,ACTIVE,engine,actuator,
                        new HazelcastOperatorReadiness(member,Duration.ofSeconds(10)),mock(HazelcastExecutionPlans.class),coordinator,member)) {
                    var manifest=recovery.inspect(selection);var h0=f.configuration.recoveryCommands().guardedBy(() -> recovery.requirePlanningCurrent(manifest)).plan(auth,manifest,null,null);
                    assertThatThrownBy(() -> recovery.requireFinalized(h0.manifest())).isInstanceOf(TapstateException.class);
                    var h1=recovery.prepared(h0,ACTIVE);assertThat(h1.operationId()).isEqualTo(h0.operationId());
                    assertThat(h1.controlRevision()).isGreaterThan(h0.controlRevision());recovery.requireFinalized(h1.manifest());
                    assertThat(coordinator.hasScope(f.scope)).isFalse();assertThat(stream.closed).hasValue(1);
                    assertThat(claims.readPipeline(f.scope).orElseThrow().leased()).isFalse();
                    if(mode.equals("new-start")) {
                        var intent=f.configuration.intents().load("flow").orElseThrow();
                        assertThat(f.configuration.intents().compareAndSet(f.scope,intent.intentRevision(),new io.tapstate.core.lifecycle.DesiredState("flow",
                                io.tapstate.core.lifecycle.PipelineState.RUNNING,intent.desired().revision(),false))).isTrue();
                        assertThatThrownBy(() -> recovery.verifyExternal(h1.manifest(),ACTIVE)).isInstanceOf(TapstateException.class);
                        assertThat(f.configuration.recoveryCommands().find(auth,h1.operationId()).orElseThrow().phase()).isEqualTo(StorageRecoveryCommands.Phase.PLANNED);return;
                    }
                    var confirmed=f.configuration.recoveryCommands().guardedBy(() -> recovery.requireFinalized(h1.manifest())).confirm(h1);
                    if(mode.equals("lost-d-reply")) {
                        assertThatThrownBy(() -> recovery.run(confirmed)).isInstanceOf(TapstateException.class);
                        assertThat(f.configuration.recoveryCommands().find(auth,h1.operationId()).orElseThrow().phase()).isEqualTo(StorageRecoveryCommands.Phase.RUNNING);
                        failD.set(false);doThrow(new AssertionError("already applied result must precede any old-run inspection")).when(engine).captureRun(anyString());
                        f.configuration.artifacts().save(source("input","later-authoring",null));
                    }
                    recovery.run(confirmed);
                    var complete=f.configuration.recoveryCommands().find(auth,h1.operationId()).orElseThrow();assertThat(complete.phase()).isEqualTo(StorageRecoveryCommands.Phase.COMPLETE);
                    var replacement=f.configuration.capturePreparations().read(f.business(),manifest.sharedSrs().previous().captureId()).orElseThrow();
                    assertThat(replacement.chainId()).isNotEqualTo(chain);assertThat(replacement.canonicalChainId()).isEqualTo(chain);
                    assertThat(f.configuration.pipelineStates().read("flow").orElseThrow().stateGeneration()).isEqualTo(f.scope.stateGeneration());
                    assertThat(f.configuration.intents().load("flow").orElseThrow().desired().targetState()).isEqualTo(io.tapstate.core.lifecycle.PipelineState.STOPPED);
                    assertThat(f.cacheDatabase.getCollection("srs_models").countDocuments(new Document("_id.chain",chain))).isZero();
                    if(mode.equals("ring-reopen")) {
                        assertThat(oldHigh).isEqualTo(2);coordinator.close();runtime.close();member.shutdown();
                        var before=sharedRecoveryCacheRows(f.cacheDatabase);
                        var reopened=sharedMember(stores.log());
                        try {
                            assertThat(reopened.getRingbuffer(oldRing).tailSequence()).isEqualTo(oldHigh);
                            String quiet=SrsRingbuffer.ringName(chain,"quiet");
                            assertThat(f.cacheDatabase.getCollection("srs_log_bounds").find(new Document("_id","srs-log-bounds:"+quiet)).first())
                                    .containsEntry("largestSequence",-1L);
                            assertThat(reopened.getRingbuffer(quiet).tailSequence()).isEqualTo(-1);
                            assertThatThrownBy(() -> stores.log().largestSequence(oldRing)).isInstanceOf(TapstateException.class);
                            assertThatThrownBy(() -> stores.log().bounds(oldRing)).isInstanceOf(TapstateException.class);
                            assertThatThrownBy(() -> stores.log().load(oldRing,0)).isInstanceOf(TapstateException.class);
                            assertThatThrownBy(() -> stores.log().readBatch(oldRing,0,1)).isInstanceOf(TapstateException.class);
                            assertThat(reopened.getRingbuffer(SrsRingbuffer.ringName(replacement.chainId(),"orders")).tailSequence()).isEqualTo(-1);
                            assertThat(sharedRecoveryCacheRows(f.cacheDatabase)).isEqualTo(before);
                            var bounds=f.cacheDatabase.getCollection("srs_log_bounds");
                            var boundsKey=new Document("_id","srs-log-bounds:"+oldRing);var originalBounds=bounds.find(boundsKey).first();
                            try {
                                bounds.replaceOne(boundsKey,new Document(originalBounds).append("largestSequence",1L));
                                assertThatThrownBy(() -> stores.log().ringInitializationBounds(oldRing)).isInstanceOf(TapstateException.class);
                                bounds.deleteOne(boundsKey);
                                assertThatThrownBy(() -> stores.log().ringInitializationBounds(oldRing)).isInstanceOf(TapstateException.class);
                                bounds.insertOne(new Document(originalBounds).append("largestSequence",2));
                                assertThatThrownBy(() -> stores.log().ringInitializationBounds(oldRing)).isInstanceOf(TapstateException.class);
                            } finally {bounds.replaceOne(boundsKey,originalBounds,new com.mongodb.client.model.ReplaceOptions().upsert(true));}
                            var roots=f.cacheDatabase.getCollection("srs_chains");var rootKey=new Document("_id",chain);var originalRoot=roots.find(rootKey).first();
                            try {
                                roots.updateOne(rootKey,new Document("$set",new Document("recoveryControl.controlHash","0".repeat(64))));
                                assertThatThrownBy(() -> stores.log().ringInitializationBounds(oldRing)).isInstanceOf(TapstateException.class);
                                roots.replaceOne(rootKey,new Document(originalRoot).append("cacheIncarnation","foreign-cache"));
                                assertThatThrownBy(() -> stores.log().ringInitializationBounds(oldRing)).isInstanceOf(TapstateException.class);
                            } finally {roots.replaceOne(rootKey,originalRoot);}
                            var contexts=f.cacheDatabase.getCollection("srs_model_contexts");
                            var context=contexts.find(new Document("ringName",oldRing)).first();var contextKey=new Document("_id",context.get("_id"));
                            try {
                                contexts.deleteOne(contextKey);
                                assertThatThrownBy(() -> stores.log().ringInitializationBounds(oldRing)).isInstanceOf(TapstateException.class);
                            } finally {contexts.insertOne(context);}
                            var consumers=f.cacheDatabase.getCollection("srs_consumers");
                            var consumer=consumers.find(new Document("_id.chain",chain)).first();var consumerKey=new Document("_id",consumer.get("_id"));
                            try {
                                consumers.deleteOne(consumerKey);
                                assertThatThrownBy(() -> stores.log().ringInitializationBounds(oldRing)).isInstanceOf(TapstateException.class);
                            } finally {consumers.insertOne(consumer);}
                            assertThat(stores.log().ringInitializationBounds(oldRing).largestSequence()).isEqualTo(oldHigh);
                            assertThat(sharedRecoveryCacheRows(f.cacheDatabase)).isEqualTo(before);
                        } finally {reopened.shutdown();}
                    }
                }
            } finally {member.shutdown();}
        });
    }

    private static Map<String,List<Document>> sharedRecoveryCacheRows(MongoDatabase database) {
        var rows=new TreeMap<String,List<Document>>();
        for(String name:List.of("workload_claims","srs_chains","srs_models","srs_model_contexts","srs_consumers","srs_consumer_offsets","srs_events","srs_log_bounds"))
            rows.put(name,database.getCollection(name).find().sort(new Document("_id",1)).into(new ArrayList<>()));
        return rows;
    }

    private static SharedSrsRecoveryPreparations failingCompletion(SharedSrsRecoveryPreparations original,java.util.concurrent.atomic.AtomicBoolean fail) {
        return new SharedSrsRecoveryPreparations() {
            @Override public SharedSrsRecoveryPreparations guardedBy(Runnable active){return failingCompletion(original.guardedBy(active),fail);}
            @Override public SharedSrsRecoveryPreparation reserve(StorageRecoveryCommands.Plan plan){return original.reserve(plan);}
            @Override public Optional<SharedSrsRecoveryPreparation> pending(CacheAuthScope scope,String id){return original.pending(scope,id);}
            @Override public StorageRecoveryCommands.Plan complete(SharedSrsRecoveryReceipt receipt){if(fail.get())throw ThreeLayerStorePort.refused();return original.complete(receipt);}
            @Override public StorageRecoveryCommands.Plan replan(SharedSrsRecoveryReceipt receipt,StorageRecoveryCommands.Manifest manifest){return original.replan(receipt,manifest);}
            @Override public Optional<SharedSrsRecoveryPreparation> reconfirmation(CacheAuthScope scope,String id){return original.reconfirmation(scope,id);}
            @Override public SharedSrsRecoveryPreparation reserve(StorageRecoveryCommands.Plan plan,SharedSrsRecoveryReceipt receipt){return original.reserve(plan,receipt);}
        };
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void stoppingTheOriginalLocalCaptureDoesNotNeedTheLostSharedModelBodies(boolean missingControls) {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.SNAPSHOT_AND_CDC,"latest",null));
            var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());var claims=f.cache.workloadClaims(f.business());
            var fence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
            var stores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(stores.log());var ports=new CopyOnWriteArrayList<NativeSharedPort>();
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),stores,member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,cacheOwnership(f,OWNER),Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,new SnapshotBuffer(8),fixtureProvisioner(),(fixed,config,guard,preparation,stream) -> {
                            var port=new NativeSharedPort(config,guard,preparation,stream);ports.add(port);return port;
                        })) {
                var first=coordinator.forRun(f.scope,fence,new ExecutionFence(f.scope,fence));
                first.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),models);awaitLoadedCounts(first);
                var binding=first.captureBindings().logged().get("input");String chain=binding.run().miningChainId();
                var physical=ports.stream().filter(port -> port.stream!=null).findFirst().orElseThrow();
                physical.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",2),null)),Optional.of(new SourcePosition("before-model-loss")));
                assertThat(stores.chains().load(chain).orElseThrow().checkpoint()).isNotNull();
                var prepared=f.configuration.capturePreparations().read(f.business(),io.tapstate.core.model.canonical.SourceCaptureIdentity.sharedCapture(chain)).orElseThrow();
                var captureKey=new WorkloadClaimKey(f.business().clusterId(),WorkloadClaimType.CAPTURE,prepared.captureId());
                synchronized(coordinator) {
                    // Remove only these model bodies; all original control and private history remains authoritative.
                    var bodies=f.cacheDatabase.getCollection("srs_models").find(new Document("_id.chain",chain)).into(new ArrayList<>());
                    assertThat(bodies).hasSize(2);
                    for(var body:bodies)assertThat(f.cacheDatabase.getCollection("srs_models").deleteOne(new Document("_id",body.get("_id"))).getDeletedCount()).isOne();
                    assertThatThrownBy(() -> stores.chains().load(chain)).isInstanceOf(TapstateException.class);
                    var observed=stores.consumers().inspectSharedRecovery(prepared);assertThat(observed.pipelines().keySet()).containsExactly("flow");
                    var capture=claims.read(captureKey).orElseThrow();assertThat(capture.leased()).isTrue();
                    if(missingControls) {
                        var offsets=f.cacheDatabase.getCollection("srs_consumer_offsets");
                        var offset=offsets.find(new Document("_id.chain",chain)).first();assertThat(offset).isNotNull();
                        assertThat(offsets.deleteOne(new Document("_id",offset.get("_id"))).getDeletedCount()).isOne();
                        assertThatThrownBy(() -> stores.consumers().inspectSharedRecovery(prepared)).isInstanceOf(TapstateException.class);
                        var damaged=sharedCleanupControls(f);
                        assertThatThrownBy(() -> first.stopCapture("flow",false)).isInstanceOf(TapstateException.class);
                        assertThat(physical.closed).hasValue(0);assertThat(coordinator.hasScope(f.scope)).isTrue();
                        var retainedClaim=claims.read(captureKey).orElseThrow();
                        assertThat(retainedClaim.claim()).isEqualTo(capture.claim());assertThat(retainedClaim.leased()).isTrue();
                        assertThat(sharedCleanupControls(f)).isEqualTo(damaged);
                        return;
                    }
                    var before=sharedCleanupControls(f);
                    assertThatThrownBy(() -> coordinator.retireShared(prepared,observed.captureFence())).isInstanceOf(TapstateException.class);
                    assertThatThrownBy(() -> coordinator.requireSharedRetired(prepared,observed.captureFence())).isInstanceOf(TapstateException.class);
                    first.stopCapture("flow",false);
                    assertThat(physical.closed).hasValue(1);assertThat(first.isCapturing("flow")).isFalse();assertThat(coordinator.hasScope(f.scope)).isFalse();
                    var ended=claims.read(captureKey).orElseThrow();assertThat(ended.leased()).isFalse();
                    assertThat(WorkloadClaimFence.from(ended.claim())).isEqualTo(WorkloadClaimFence.from(capture.claim()));
                    assertThat(ended.claim().executionNodeIds()).isEqualTo(capture.claim().executionNodeIds());
                    assertThat(sharedCleanupControls(f)).isEqualTo(before);
                    assertThat(f.cacheDatabase.getCollection("srs_models").countDocuments(new Document("_id.chain",chain))).isZero();
                    assertThatThrownBy(() -> stores.chains().load(chain)).isInstanceOf(TapstateException.class);
                    var retained=stores.consumers().inspectSharedRecovery(prepared);
                    assertThat(retained.consumers()).isEqualTo(observed.consumers());assertThat(retained.captureFence()).isEqualTo(observed.captureFence());
                    coordinator.retireShared(prepared,retained.captureFence());coordinator.requireSharedRetired(prepared,observed.captureFence());
                    assertThat(physical.closed).hasValue(1);
                }
            } finally {member.shutdown();}
        });
    }

    private static Map<String,List<Document>> sharedCleanupControls(Fixture f) {
        var result=new TreeMap<String,List<Document>>();
        for(String table:List.of("srs_chains","srs_model_contexts","srs_consumers","srs_consumer_offsets","srs_events","srs_log_bounds","pipeline_state"))
            result.put("cache."+table,f.cacheDatabase.getCollection(table).find().sort(new Document("_id",1)).into(new ArrayList<>()));
        for(String table:f.configDatabase.listCollectionNames())
            result.put("config."+table,f.configDatabase.getCollection(table).find().sort(new Document("_id",1)).into(new ArrayList<>()));
        return result;
    }

    @Test void freshSharedCaptureRefusesPastTimeBeforeAllocatingAnEpoch() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,CLOCK.instant().minusSeconds(1).toString(),null));
            var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());var claims=f.cache.workloadClaims(f.business());
            var fence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
            var stores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(stores.log());var ports=new CopyOnWriteArrayList<NativeSharedPort>();
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),stores,member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,cacheOwnership(f,OWNER),Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,new SnapshotBuffer(8),fixtureProvisioner(),(fixed,config,guard,preparation,stream) -> {
                            var port=new NativeSharedPort(config,guard,preparation,stream);ports.add(port);return port;
                        })) {
                var run=coordinator.forRun(f.scope,fence,new ExecutionFence(f.scope,fence));
                assertCode(() -> run.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),models),CaptureError.START_FROM_OUTSIDE_WINDOW);
                assertThat(ports).isEmpty();
                assertThat(f.cacheDatabase.getCollection("srs_chains").find().into(new ArrayList<>())).allSatisfy(chain -> assertThat(chain.getLong("epoch")).isZero());
            } finally {member.shutdown();}
        });
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void sharedMetadataPreflightDoesNotCancelTheOriginalTailBeforeItsStartCallback(boolean snapshot) {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,snapshot?ReadMode.SNAPSHOT_AND_CDC:ReadMode.CDC_ONLY,"latest",null));
            var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());var claims=f.cache.workloadClaims(f.business());
            var fence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
            var stores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(stores.log());var ports=new CopyOnWriteArrayList<NativeSharedPort>();
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),stores,member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,cacheOwnership(f,OWNER),Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,new SnapshotBuffer(8),fixtureProvisioner(),(fixed,config,guard,preparation,stream) -> {
                            var port=new NativeSharedPort(config,guard,preparation,stream,stream!=null);ports.add(port);return port;
                        })) {
                var run=coordinator.forRun(f.scope,fence,new ExecutionFence(f.scope,fence));
                assertThatCode(() -> run.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),models)).doesNotThrowAnyException();
                var binding=run.captureBindings().logged().get("input");var physical=ports.stream().filter(port -> port.stream!=null).findFirst().orElseThrow();
                var before=stores.chains().load(binding.run().miningChainId()).orElseThrow();
                assertThat(before.epoch()).isOne();assertThat(before.checkpoint()).isNull();assertThat(physical.closed).hasValue(0);
                assertThat(binding.recoveryAfter()).containsEntry("orders",-1L).containsEntry("quiet",-1L);
                physical.started();
                physical.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",2),null)),Optional.of(new SourcePosition("after-delayed-start")));
                var after=stores.chains().load(binding.run().miningChainId()).orElseThrow();
                assertThat(after.epoch()).isOne();assertThat(after.checkpoint().position().token()).isEqualTo("after-delayed-start");
                assertThat(run.captureFailure("flow")).isEmpty();
                if(snapshot)awaitSharedCounts(run,"flow");
                run.stopCapture("flow",false);assertThat(physical.closed).hasValue(1);
            } finally {member.shutdown();}
        });
    }
    @Test void actualSharedConsumersKeepSeparatePinsAndStoppingOneLeavesThePhysicalTailRunning() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.SNAPSHOT_AND_CDC,"latest",null));
            var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());var claims=f.cache.workloadClaims(f.business());
            var firstClaim=claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow();
            var firstFence=WorkloadClaimFence.from(firstClaim);var ownership=cacheOwnership(f,OWNER);var provisioner=fixtureProvisioner();
            var nativeStores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(nativeStores.log());var ports=new CopyOnWriteArrayList<NativeSharedPort>();var buffer=new SnapshotBuffer(8);
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),nativeStores,member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,ownership,Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,buffer,provisioner,(fixed,config,guard,preparation,stream) -> {var port=new NativeSharedPort(config,guard,preparation,stream);ports.add(port);return port;})) {
                var first=coordinator.forRun(f.scope,firstFence,new ExecutionFence(f.scope,firstFence));first.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),models);awaitLoadedCounts(first);
                var firstBinding=first.captureBindings().logged().get("input");var physical=ports.stream().filter(port -> port.stream!=null).findFirst().orElseThrow();
                assertThat(firstBinding.recoveryAfter()).containsOnlyKeys("orders","quiet").allSatisfy((table,sequence) -> assertThat(sequence).isEqualTo(-1));
                physical.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",2),null)),Optional.of(new SourcePosition("shared-one")));
                var otherPipeline=new PipelineResource("other",null,List.of(SourceRef.spec("input",true)),null,null,null,new Settings(null,null,null,null,ReadMode.SNAPSHOT_AND_CDC,null),null);
                f.configuration.artifacts().create(otherPipeline);var otherScope=new PipelineIntentScope("cluster",1,f.scope.cacheIncarnation(),"other",1);
                var otherModels=SourceModelSnapshot.capture("other",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
                var otherInputs=otherModels.preparationInputs(otherScope,otherPipeline,otherPipeline,Map.of("input",f.source()));
                f.preparer(f.configuration,f.cache).prepare(otherScope,otherInputs);
                var otherClaim=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(otherScope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
                var second=coordinator.forRun(otherScope,otherClaim,new ExecutionFence(otherScope,otherClaim));second.startCapture("other",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),otherModels);awaitSharedCounts(second,"other");
                var secondBinding=second.captureBindings().logged().get("input");
                assertThat(secondBinding.recoveryAfter()).containsEntry("orders",0L).containsEntry("quiet",-1L);
                assertThat(secondBinding.run().context()).isNotEqualTo(firstBinding.run().context());
                var a=nativeStores.consumers().progress(firstBinding.run().context()).orElseThrow();var b=nativeStores.consumers().progress(secondBinding.run().context()).orElseThrow();
                assertThat(a.snapshot().sourcePosition()).isEqualTo("snapshot-flow");assertThat(b.snapshot().sourcePosition()).isEqualTo("snapshot-other");
                assertThat(ports.stream().filter(port -> port.stream!=null)).hasSize(1);
                first.stopCapture("flow",false);assertThat(physical.closed).hasValue(0);
                physical.listener.get().onBatch(List.of(Envelope.insert(2,"orders",Map.of("id",3),null)),Optional.of(new SourcePosition("shared-two")));
                assertThat(nativeStores.log().bounds(SrsRingbuffer.ringName(firstBinding.run().miningChainId(),"orders")).largestSequence()).isEqualTo(1);
                assertThat(nativeStores.consumers().progress(secondBinding.run().context()).orElseThrow()).isEqualTo(b);
                second.stopCapture("other",false);assertThat(physical.closed).hasValue(1);
                var next=WorkloadClaimFence.from(claims.advanceExecution(firstClaim,1,Set.of(OWNER.nodeId())).orElseThrow());
                var resumed=coordinator.forRun(f.scope,next,new ExecutionFence(f.scope,next));resumed.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),models);awaitLoadedCounts(resumed);
                var replay=resumed.captureBindings().logged().get("input");assertThat(replay.snapshotEpoch()).isEqualTo(1);
                assertThat(replay.recoveryAfter()).containsEntry("orders",-1L).containsEntry("quiet",-1L);
                var latestPhysical=ports.stream().filter(port -> port.stream!=null).reduce((previous,current) -> current).orElseThrow();
                assertThat(latestPhysical.start.get()).isEqualTo(CaptureStart.resume(new SourcePosition("shared-two")));
                assertThat(nativeStores.chains().load(firstBinding.run().miningChainId()).orElseThrow().epoch()).isEqualTo(2);
                assertThat(nativeStores.consumers().progress(secondBinding.run().context()).orElseThrow()).isEqualTo(b);resumed.stopCapture("flow",false);
            } finally {member.shutdown();}
            assertThat(f.configDatabase.getCollection("capture_preparations").countDocuments()).isOne();
            assertThat(f.cacheDatabase.getCollection("srs_consumers").countDocuments()).isEqualTo(2);
            assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isZero();
        });
    }
    @ParameterizedTest @ValueSource(strings={"healthy","RUNNING","PAUSED","release-before","release-after"})
    void aForeignSharedConsumerWidensTheOriginalTailAndTakesOverWithoutChangingItsReadFloor(String retainedStatus) {
        withFixture(f -> {
            var firstSource=f.source();f.configuration.artifacts().save(new SourceResource(firstSource.id(),null,firstSource.connector(),firstSource.config(),firstSource.mode(),List.of(TableRef.literal("orders")),firstSource.srs(),null));
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,"latest",null));
            var otherSource=new SourceResource("other-input",null,firstSource.connector(),firstSource.config(),firstSource.mode(),List.of(TableRef.literal("quiet")),firstSource.srs(),null);
            var otherPipeline=new PipelineResource("other",null,List.of(SourceRef.spec(otherSource.id(),true)),null,null,null,new Settings(null,null,null,null,ReadMode.CDC_ONLY,"latest"),null);
            f.configuration.artifacts().create(otherSource);f.configuration.artifacts().create(otherPipeline);
            var otherScope=new PipelineIntentScope("cluster",1,f.scope.cacheIncarnation(),"other",1);
            var otherOwner=new WorkloadOwner("node-b","boot-b");var claims=f.cache.workloadClaims(f.business());
            var otherNode=WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,otherOwner.nodeId()),otherOwner,1,TTL).claim());
            var firstModels=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
            var otherModels=SourceModelSnapshot.capture("other",List.of(otherSource),f.cache.schemas(f.business(),ACTIVE));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());
            var otherInputs=otherModels.preparationInputs(otherScope,otherPipeline,otherPipeline,Map.of(otherSource.id(),otherSource));
            new PipelineFirstStatePreparer(f.business(),f.configuration.preparations(),f.cache.pipelinePreparations(f.business(),ACTIVE),() -> Optional.of(otherNode),ACTIVE,CLOCK,TTL).prepare(otherScope,otherInputs);
            var firstFence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
            var otherFence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(otherScope).orElseThrow().claim(),1,Set.of(otherOwner.nodeId())).orElseThrow());
            status(f,f.scope,firstFence,"RUNNING");status(f,otherScope,otherFence,"RUNNING");
            var stores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(stores.log());
            var firstOwnership=cacheOwnership(f,OWNER);
            var firstPorts=new CopyOnWriteArrayList<NativeSharedPort>();var otherPorts=new CopyOnWriteArrayList<NativeSharedPort>();
            try(var firstAuthorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var otherAuthorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var firstRuntime=new NativeSrsCaptureRunUnit(f.business(),stores,member);var otherRuntime=new NativeSrsCaptureRunUnit(f.business(),stores,member);
                var firstRoot=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),firstAuthorization,firstOwnership,Duration.ofSeconds(10),ACTIVE,CLOCK),
                        firstRuntime,new SnapshotBuffer(8),fixtureProvisioner(),(fixed,config,guard,preparation,stream) -> {var port=new NativeSharedPort(config,guard,preparation,stream);firstPorts.add(port);return port;});
                var otherRoot=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(otherNode),otherAuthorization,cacheOwnership(f,otherOwner),Duration.ofSeconds(10),ACTIVE,CLOCK),
                        otherRuntime,new SnapshotBuffer(8),fixtureProvisioner(),(fixed,config,guard,preparation,stream) -> {var port=new NativeSharedPort(config,guard,preparation,stream);otherPorts.add(port);return port;})) {
                var first=firstRoot.forRun(f.scope,firstFence,new ExecutionFence(f.scope,firstFence));
                first.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),firstModels);
                var firstPhysical=firstPorts.getFirst();var chain=first.captureBindings().logged().get("input").run().miningChainId();
                firstPhysical.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",1),null)),Optional.of(new SourcePosition("orders-one")));
                var other=otherRoot.forRun(otherScope,otherFence,new ExecutionFence(otherScope,otherFence));
                assertThatThrownBy(() -> other.startCapture("other",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),otherModels)).isInstanceOf(RingNotOpenYet.class);
                assertThat(otherPorts).isEmpty();
                awaitNative(() -> stores.chains().load(chain).orElseThrow().checkpoint().position().order().epoch()==2);
                assertThat(firstPorts).hasSize(2);var widened=firstPorts.getLast();assertThat(widened.config.node()).isEqualTo(firstPhysical.config.node());
                assertThat(widened.config.streams()).containsExactly("orders","quiet");assertThat(widened.start.get()).isEqualTo(CaptureStart.resume(new SourcePosition("orders-one")));
                assertThat(firstPhysical.closed).hasValue(1);
                other.startCapture("other",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),otherModels);
                var binding=other.captureBindings().logged().get(otherSource.id());var progress=stores.consumers().progress(binding.run().context()).orElseThrow();
                assertThat(binding.recoveryAfter()).containsOnlyKeys("quiet").containsEntry("quiet",-1L);assertThat(otherPorts).isEmpty();
                long events=f.cacheDatabase.getCollection("srs_events").countDocuments();
                assertCode(() -> firstPhysical.listener.get().onBatch(List.of(Envelope.insert(2,"orders",Map.of("id",99),null)),Optional.of(new SourcePosition("late-closed-tail"))),IoError.CACHE_BUSINESS_FENCED);
                assertThat(f.cacheDatabase.getCollection("srs_events").countDocuments()).isEqualTo(events);
                if(!retainedStatus.equals("healthy")) {
                    synchronized(firstRoot) { synchronized(otherRoot) {
                        status(f,otherScope,otherFence,retainedStatus.equals("PAUSED")?"PAUSED":"RUNNING");
                        assertThat(f.cacheDatabase.getCollection("srs_models").deleteMany(new Document("_id.chain",chain)).getDeletedCount()).isPositive();
                        assertThatThrownBy(() -> stores.chains().load(chain)).isInstanceOf(TapstateException.class);
                        status(f,f.scope,firstFence,"STOPPED");
                        // The remote consumer still needs this original physical owner even when model bodies are lost.
                        var stopped=sharedCleanupControls(f);
                        first.stopCapture("flow",false);
                        assertThat(widened.closed).hasValue(0);assertThat(firstRoot.hasScope(f.scope)).isFalse();
                        assertThat(sharedCleanupControls(f)).isEqualTo(stopped);
                        var key=new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,
                                io.tapstate.core.model.canonical.SourceCaptureIdentity.sharedCapture(chain));
                        assertThat(claims.read(key).orElseThrow().leased()).isTrue();
                        var prepared=f.configuration.capturePreparations().read(f.business(),key.resourceId()).orElseThrow();
                        var retained=stores.consumers().inspectSharedRecovery(prepared);
                        assertThat(retained.pipelines()).containsKeys("flow","other");
                        assertThatThrownBy(() -> firstRoot.retireShared(prepared,retained.captureFence())).isInstanceOf(TapstateException.class);
                        assertThatThrownBy(() -> firstRoot.requireSharedRetired(prepared,retained.captureFence())).isInstanceOf(TapstateException.class);
                        assertThat(widened.closed).hasValue(0);assertThat(sharedCleanupControls(f)).isEqualTo(stopped);
                        status(f,otherScope,otherFence,"STOPPED");other.stopCapture("other",false);
                        var beforeRetirement=sharedCleanupControls(f);
                        var wrong=new WorkloadClaimFence(key,retained.captureFence().owner(),retained.captureFence().claimGeneration()+1,
                                retained.captureFence().executionGeneration(),retained.captureFence().topologyRevision());
                        assertThatThrownBy(() -> firstRoot.retireShared(prepared,wrong)).isInstanceOf(TapstateException.class);
                        assertThat(widened.closed).hasValue(0);
                        if(retainedStatus.startsWith("release-")) {
                            var attempts=new java.util.concurrent.atomic.AtomicInteger();
                            var unavailable=new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","release response unavailable"),null);
                            doAnswer(call -> {
                                if(attempts.incrementAndGet()==1) {
                                    if(retainedStatus.equals("release-after"))call.callRealMethod();
                                    throw unavailable;
                                }
                                return call.callRealMethod();
                            }).when(firstOwnership).release(any());
                            assertThatThrownBy(() -> firstRoot.retireShared(prepared,retained.captureFence())).isSameAs(unavailable);
                            assertThatThrownBy(() -> firstRoot.requireSharedRetired(prepared,retained.captureFence())).isInstanceOf(TapstateException.class);
                            assertThat(widened.closed).hasValue(1);
                            assertThat(claims.read(key).orElseThrow().leased()).isEqualTo(retainedStatus.equals("release-before"));
                            assertThat(sharedCleanupControls(f)).isEqualTo(beforeRetirement);
                        }
                        firstRoot.retireShared(prepared,retained.captureFence());firstRoot.requireSharedRetired(prepared,retained.captureFence());
                        assertThat(widened.closed).hasValue(1);var ended=claims.read(key).orElseThrow();assertThat(ended.leased()).isFalse();
                        assertThat(WorkloadClaimFence.from(ended.claim())).isEqualTo(retained.captureFence());
                        assertThat(sharedCleanupControls(f)).isEqualTo(beforeRetirement);
                    } }
                    return;
                }
                status(f,f.scope,firstFence,"STOPPED");first.stopCapture("flow",false);assertThat(widened.closed).hasValue(0);
                widened.listener.get().onBatch(List.of(Envelope.insert(3,"quiet",Map.of("id",3),null)),Optional.of(new SourcePosition("quiet-two")));
                assertThat(stores.consumers().progress(binding.run().context()).orElseThrow()).isEqualTo(progress);
                firstRoot.close();assertThat(widened.closed).hasValue(1);
                awaitNative(() -> !otherPorts.isEmpty());var successor=otherPorts.getFirst();
                assertThat(other.captureFailure("other")).isEmpty();assertThat(successor.start.get()).isEqualTo(CaptureStart.resume(new SourcePosition("quiet-two")));
                assertThat(stores.chains().load(chain).orElseThrow().epoch()).isEqualTo(3);
                assertThat(other.captureBindings().logged().get(otherSource.id())).isEqualTo(binding);
                assertThat(stores.consumers().progress(binding.run().context()).orElseThrow()).isEqualTo(progress);
                successor.listener.get().onBatch(List.of(Envelope.insert(4,"quiet",Map.of("id",4),null)),Optional.of(new SourcePosition("quiet-three")));
                assertThat(stores.log().bounds(SrsRingbuffer.ringName(chain,"quiet")).largestSequence()).isEqualTo(1);
                status(f,otherScope,otherFence,"STOPPED");other.stopCapture("other",false);assertThat(successor.closed).hasValue(1);
            } finally {member.shutdown();}
            assertThat(f.configDatabase.getCollection("capture_preparations").countDocuments()).isOne();
        });
    }
    private static void status(Fixture f,PipelineIntentScope scope,WorkloadClaimFence original,String status) {
        var states=f.cache.pipelineStates(scope,original,ACTIVE);var prior=states.read(scope.pipelineId()).orElseThrow();
        assertThat(states.compareAndSwap(scope.pipelineId(),prior.epoch(),status,CLOCK.instant())).isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Applied.class);
    }
    private static void awaitNative(java.util.function.BooleanSupplier ready) throws InterruptedException {
        long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();while(!ready.getAsBoolean() && System.nanoTime()<deadline)Thread.sleep(10);
        assertThat(ready.getAsBoolean()).isTrue();
    }
    private static CaptureOwnership cacheOwnership(Fixture f,WorkloadOwner owner) {
        return cacheOwnership(f, owner, () -> true);
    }
    private static CaptureOwnership cacheOwnership(Fixture f,WorkloadOwner owner,java.util.function.BooleanSupplier nodeSession) {
        var properties=new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        var gate=new ClusterMembershipGate(properties);
        if (nodeSession != null) gate.observeNodeSession(nodeSession);
        gate.install(new ClusterMembership("cluster",1,Set.of("node","node-b","fixture-third")));
        gate.canCommit(Set.of("node","node-b"));
        return spy(new CaptureOwnership("cluster",owner,gate,new ClusterWorkloadClaims(f.cache.workloadClaims(f.business()),gate),TTL));
    }
    @ParameterizedTest @ValueSource(booleans = {false, true})
    void anUnprovedNodeSessionNeverAllocatesACaptureClaim(boolean suppliedFalseProof) {
        withFixture(f -> {
            var ownership = cacheOwnership(f, OWNER, suppliedFalseProof ? () -> false : null);
            var capture = new CaptureId("not-admitted");
            assertThat(ownership.acquire(capture).acquired()).isFalse();
            assertThat(f.cache.workloadClaims(f.business()).read(new WorkloadClaimKey(
                    "cluster", WorkloadClaimType.CAPTURE, capture.value()))).isEmpty();
        });
    }
    private static io.tapstate.adapters.pdk.ConnectorProvisioner fixtureProvisioner() {return fixtureProvisioner("c".repeat(64));}
    private static io.tapstate.adapters.pdk.ConnectorProvisioner fixtureProvisioner(String hash) {
        var provisioner=mock(io.tapstate.adapters.pdk.ConnectorProvisioner.class);var ref=new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"not.loaded","2.0.8",null,null,hash,false);
        when(provisioner.resolve("mongodb")).thenReturn(ref);when(provisioner.resolve("mongodb",hash)).thenReturn(ref);return provisioner;
    }
    private static HazelcastInstance sharedMember(SrsLogStore log) {return sharedMember(log,false);}
    private static HazelcastInstance sharedMember(SrsLogStore log,boolean recovery) {
        var config=new com.hazelcast.config.Config().setClusterName("scoped-shared-native-"+UUID.randomUUID());
        if(recovery)config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE,OWNER.nodeId())
                .setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE,OWNER.bootId());
        config.setProperty("hazelcast.phone.home.enabled","false");config.setProperty("hazelcast.shutdownhook.enabled","false");
        config.setProperty("hazelcast.operation.thread.count","2");config.setProperty("hazelcast.operation.generic.thread.count","2");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);config.getJetConfig().setEnabled(false);
        config.addRingBufferConfig(new com.hazelcast.config.RingbufferConfig("srs.*").setCapacity(8).setBackupCount(0).setInMemoryFormat(com.hazelcast.config.InMemoryFormat.OBJECT)
                .setRingbufferStoreConfig(new com.hazelcast.config.RingbufferStoreConfig().setEnabled(true).setFactoryImplementation(new SrsLogRingbufferStoreFactory(log))));
        config.getSerializationConfig().addSerializerConfig(new com.hazelcast.config.SerializerConfig().setTypeClass(SrsItem.class).setImplementation(new SrsItemSerializer()));
        return com.hazelcast.core.Hazelcast.newHazelcastInstance(config);
    }
    private static void awaitSharedCounts(PipelineCaptureCoordinator run,String pipeline) throws InterruptedException {
        long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();while(run.snapshotProgress(pipeline).byTable().size()!=2 && System.nanoTime()<deadline) {
            if(run.captureFailure(pipeline).isPresent())throw new AssertionError("native shared source failed",run.captureFailure(pipeline).orElseThrow());Thread.sleep(10);
        }
        assertThat(run.snapshotProgress(pipeline).byTable()).containsOnlyKeys("orders","quiet");
    }
    @ParameterizedTest @ValueSource(strings={"stable","changed-after-planning","unknown-model","replacement"})
    void anExplicitFullPublishesTheNewActualShapeBeforeRowsAndANewConsumerKeepsItsPendingCutAcrossAnotherEpoch(String mode) {
        withFixture(f -> {
            boolean changesAfterPlanning=mode.equals("changed-after-planning"),replacement=mode.equals("replacement");
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.SNAPSHOT_AND_CDC,"latest",null));
            f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs());var claims=f.cache.workloadClaims(f.business());
            assertThat(f.configuration.intents().compareAndSet(f.scope,0,new io.tapstate.core.lifecycle.DesiredState("flow",
                    io.tapstate.core.lifecycle.PipelineState.RUNNING,io.tapstate.core.model.canonical.CanonicalHash.of(f.pipeline()),false))).isTrue();
            var firstFence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(f.scope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
            var provisioner=fixtureProvisioner();CapturePreparation physicalPreparation=null;
            if(replacement) {
                // This empty retained pool tests physical handoff, not partial payload loss or a registered connector.
                f.configuration.bindings().complete(new StorageSetupScope("cluster","installation","setup",1));
                f.configuration.firstAdmin().create(f.configuration.firstAdmin().load().orElseThrow().scope(),new User("admin","fixture-hash","admin"));
                byte[] fixtureBytes="native-physical-handoff-fixture".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                var registration=f.cache.connectors(f.business(),ACTIVE).register("mongodb","2.0.8",RegistrationSource.REGISTER,fixtureBytes).registration();
                provisioner=fixtureProvisioner(registration.contentHash());
                var request=new CapturePreparationRequest(f.scope,firstFence,f.node,"input",f.inputs());
                var creation=((CaptureCreationResult.Issued)f.configuration.capturePreparations().issueFirstCreate(request)).creation();
                var birth=f.cache.capturePreparations(f.business(),ACTIVE).createFirst(creation).preparation();
                var config=new CaptureConfig(f.source().connector(),f.source().config(),List.of("orders","quiet"),new PipelineNode("flow","input"),new SharedNotes(birth.canonicalChainId(),List.of()));
                ConfiguredCaptureRun.prepare(new ExecutionFence(f.scope,firstFence),config,f.configuration.requiredNotes(),provisioner,ACTIVE);
                var observed=f.cache.srs(f.business(),ACTIVE).consumers().inspectSharedRecovery(birth);
                assertThat(observed.consumers()).isEmpty();assertThat(observed.pipelines()).isEmpty();
                var auth=f.configuration.bindings().cachePreparation().orElseThrow().authScope();
                var manifest=new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.SHARED_SRS,Set.of(),birth.canonicalChainId(),birth.chainId()),
                        Set.of(),Map.of("input",io.tapstate.core.model.canonical.CanonicalHash.of(f.source())),
                        io.tapstate.core.model.canonical.CanonicalHash.ofText(observed.controlHash()),f.configuration.configurationRevision(),observed);
                var confirmed=f.configuration.recoveryCommands().confirm(f.configuration.recoveryCommands().plan(auth,manifest,null,null));
                var reservation=f.configuration.sharedRecoveryPreparations().reserve(confirmed);
                var applied=f.cache.sharedSrsRecoveries(f.business(),ACTIVE).replace(reservation,f.node);
                assertThat(f.configuration.sharedRecoveryPreparations().complete(applied).phase()).isEqualTo(StorageRecoveryCommands.Phase.COMPLETE);
                physicalPreparation=reservation.replacement();
                assertThat(f.configuration.capturePreparations().read(f.business(),birth.captureId())).contains(physicalPreparation);
                assertThat(physicalPreparation.chainId()).isNotEqualTo(physicalPreparation.canonicalChainId());
            }
            var oldTable=new SourceTable("orders",List.of(new SourceField("id","INT")),List.of("id"),List.of());
            var changedTable=new SourceTable("orders",List.of(new SourceField("id","INT"),new SourceField("note","string")),List.of("id"),List.of());
            var quiet=new SourceTable("quiet",List.of(new SourceField("id","INT")),List.of("id"),List.of());
            var actual=new AtomicReference<Map<String,SourceTable>>(Map.of("orders",oldTable,"quiet",quiet));
            var stores=f.cache.srs(f.business(),ACTIVE);var member=sharedMember(stores.log());var ports=new CopyOnWriteArrayList<NativeSharedPort>();
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),stores,member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes(),ignored -> false,
                        ScopedPipelineCaptureCoordinator.BoundedLoads.NONE,f.configuration.rebuilds()::completedFull),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,cacheOwnership(f,OWNER),Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,new SnapshotBuffer(8),provisioner,(fixed,config,guard,preparation,stream) -> {
                            if(changesAfterPlanning && preparation!=null && actual.get().get("orders").fields().size()==2)
                                actual.set(Map.of("orders",new SourceTable("orders",List.of(new SourceField("id","INT"),
                                        new SourceField("note","string"),new SourceField("late","string")),List.of("id"),List.of()),"quiet",quiet));
                            String cut="snapshot-"+config.node().pipelineId()+(actual.get().get("orders").fields().size()==1?"-old":"-new");
                            var port=new NativeSharedPort(config,guard,preparation,stream,false,actual.get(),cut);ports.add(port);return port;
                        })) {
                var first=coordinator.forRun(f.scope,firstFence,new ExecutionFence(f.scope,firstFence));
                first.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE)));
                awaitLoadedCounts(first);var original=first.captureBindings().logged().get("input");
                if(replacement) {
                    assertThat(original.run().miningChainId()).isEqualTo(physicalPreparation.chainId());
                    assertThat(original.canonicalChainId()).isEqualTo(physicalPreparation.canonicalChainId());
                }
                var oldPhysical=ports.stream().filter(port -> port.stream!=null).findFirst().orElseThrow();
                oldPhysical.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",2),null)),Optional.of(new SourcePosition("before-change")));
                var oldCheckpoint=stores.chains().load(original.run().miningChainId()).orElseThrow().checkpoint();
                first.stopCapture("flow",false);assertThat(oldPhysical.closed).hasValue(1);
                var intent=f.configuration.intents().load("flow").orElseThrow();
                assertThat(f.configuration.intents().compareAndSet(f.scope,intent.intentRevision(),new io.tapstate.core.lifecycle.DesiredState(
                        "flow",io.tapstate.core.lifecycle.PipelineState.STOPPED,intent.desired().revision(),true))).isTrue();
                actual.set(Map.of("orders",changedTable,"quiet",quiet));
                var discovered=new SourceTable(changedTable.name(),changedTable.fields(),changedTable.primaryKey(),changedTable.indexes(),999L);
                f.cache.schemas(f.business(),ACTIVE).save(new DiscoveredSourceModel("input",f.source().connector(),2,new SourceModel(List.of(discovered,quiet))));
                var rebuild=new PipelineRebuildCoordinator(new PipelineRebuildCoordinator.Stores(f.configuration.artifacts(),f.cache.schemas(f.business(),ACTIVE),
                        f.cache,stores.consumers(),f.configuration.rebuilds(),f.cache.pipelineRebuilds(f.business(),ACTIVE)),
                        new PipelinePrivateNoteRetirement(f.configuration.requiredNotes(),f.configuration.rebuilds(),(held,config,purpose,cluster,guard) -> {throw new AssertionError("unchanged source needs no external retirement");}),
                        () -> f.node,ACTIVE,(request,manifest) -> {assertThat(coordinator.hasScope(f.scope)).isFalse();assertThat(oldPhysical.closed).hasValue(1);},TTL,CLOCK);
                var prepared=rebuild.rebuild(new PipelineConvergencePass(f.configuration.intents().load("flow").orElseThrow(),firstFence,f.cache,ACTIVE),Optional.empty());
                var stopped=f.configuration.intents().load("flow").orElseThrow();
                assertThat(f.configuration.intents().compareAndSet(prepared.scope(),stopped.intentRevision(),new io.tapstate.core.lifecycle.DesiredState(
                        "flow",io.tapstate.core.lifecycle.PipelineState.RUNNING,stopped.desired().revision(),false))).isTrue();
                var nextClaim=claims.acquirePipeline(prepared.scope(),OWNER,1,TTL).claim();
                var nextFence=WorkloadClaimFence.from(claims.advanceExecution(nextClaim,1,Set.of(OWNER.nodeId())).orElseThrow());
                var next=coordinator.forRun(prepared.scope(),nextFence,new ExecutionFence(prepared.scope(),nextFence));
                SchemaStore planningSchemas=mode.equals("unknown-model")?new SchemaStore(){
                    public void save(DiscoveredSourceModel value){throw new AssertionError("planning cannot discover or repair a model");}
                    public Optional<DiscoveredSourceModel> get(String id){return Optional.empty();}
                }:f.cache.schemas(f.business(),ACTIVE);
                var frozenModels=SourceModelSnapshot.capture("flow",List.of(f.source()),planningSchemas);
                if(!mode.equals("stable") && !replacement) {
                    if(mode.equals("unknown-model"))assertThat(frozenModels.model(f.source())).isNull();
                    else assertThat(frozenModels.model(f.source()).tables().stream().filter(table -> table.name().equals("orders")).findFirst().orElseThrow().fields())
                            .extracting(SourceField::name).containsExactly("id","note");
                    var failure=catchThrowable(() -> next.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),frozenModels));
                    if(failure==null && changesAfterPlanning)assertThat(stores.models().require(new SrsModelKey(original.run().miningChainId(),"orders",f.scope.cacheIncarnation(),2,0)).table().fields())
                            .extracting(SourceField::name).containsExactly("id","note","late");
                    assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                            refused -> assertThat(refused.code()).isEqualTo(IoError.SRS_FULL_PREPARATION_UNPROVEN));
                    var unchanged=stores.chains().load(original.run().miningChainId()).orElseThrow();
                    assertThat(unchanged.checkpoint()).isEqualTo(oldCheckpoint);assertThat(unchanged.pendingFreshFull()).isNull();
                    assertThat(f.cacheDatabase.getCollection("srs_models").countDocuments(new Document("_id.captureContext.epoch",2L))).isZero();
                    assertThat(stores.consumers().progress(new SrsConsumerContext(original.run().context().key(),2)).orElseThrow().snapshot()).isNull();
                    return;
                }
                next.startCapture("flow",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),frozenModels);
                awaitLoadedCounts(next);var binding=next.captureBindings().logged().get("input");
                if(replacement) {
                    assertThat(binding.run().miningChainId()).isEqualTo(physicalPreparation.chainId());
                    assertThat(binding.canonicalChainId()).isEqualTo(physicalPreparation.canonicalChainId());
                    buildBoundedDag(f,provisioner,next.captureBindings());
                }
                var pending=stores.chains().load(binding.run().miningChainId()).orElseThrow();
                var pin=stores.consumers().progress(binding.run().context()).orElseThrow().snapshot();
                assertThat(pending.checkpoint()).isEqualTo(oldCheckpoint);assertThat(pending.pendingFreshFull()).isNotNull();
                assertThat(pin.sourcePosition()).isEqualTo("snapshot-flow-new");assertThat(binding.snapshotEpoch()).isEqualTo(2);
                assertThat(binding.recoveryAfter()).containsEntry("orders",0L).containsEntry("quiet",-1L);
                assertThat(stores.models().require(pin.models().get("orders")).table().fields()).extracting(SourceField::name).containsExactly("id","note");
                var physicals=ports.stream().filter(port -> port.stream!=null).toList();assertThat(physicals).hasSize(3);
                assertThat(physicals.get(1).closed).hasValue(1);assertThat(physicals.get(2).start.get()).isEqualTo(CaptureStart.resume(new SourcePosition(pin.sourcePosition())));
                next.stopCapture("flow",false);
                var otherPipeline=new PipelineResource("other",null,List.of(SourceRef.spec("input",true)),null,null,null,new Settings(null,null,null,null,ReadMode.SNAPSHOT_AND_CDC,null),null);
                f.configuration.artifacts().create(otherPipeline);var otherScope=new PipelineIntentScope("cluster",1,f.scope.cacheIncarnation(),"other",1);
                assertThat(f.configuration.intents().compareAndSet(otherScope,0,new io.tapstate.core.lifecycle.DesiredState("other",
                        io.tapstate.core.lifecycle.PipelineState.RUNNING,io.tapstate.core.model.canonical.CanonicalHash.of(otherPipeline),false))).isTrue();
                var otherModels=SourceModelSnapshot.capture("other",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
                f.preparer(f.configuration,f.cache).prepare(otherScope,otherModels.preparationInputs(otherScope,otherPipeline,otherPipeline,Map.of("input",f.source())));
                var otherFence=WorkloadClaimFence.from(claims.advanceExecution(claims.readPipeline(otherScope).orElseThrow().claim(),1,Set.of(OWNER.nodeId())).orElseThrow());
                var other=coordinator.forRun(otherScope,otherFence,new ExecutionFence(otherScope,otherFence));
                other.startCapture("other",ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts()),otherModels);awaitSharedCounts(other,"other");
                var otherBinding=other.captureBindings().logged().get("input");var otherPin=stores.consumers().progress(otherBinding.run().context()).orElseThrow().snapshot();
                assertThat(otherPin).isEqualTo(pin);assertThat(otherBinding.recoveryAfter()).isEqualTo(binding.recoveryAfter());
                var latest=ports.stream().filter(port -> port.stream!=null).reduce((before,current) -> current).orElseThrow();
                assertThat(latest.start.get()).isEqualTo(CaptureStart.resume(new SourcePosition(pin.sourcePosition())));
                assertThat(stores.chains().load(binding.run().miningChainId()).orElseThrow().epoch()).isEqualTo(3);
                latest.listener.get().onBatch(List.of(Envelope.insert(2,"orders",Map.of("id",3,"note","live"),null)),Optional.of(new SourcePosition("actual-new-batch")));
                var complete=stores.chains().load(binding.run().miningChainId()).orElseThrow();
                assertThat(complete.pendingFreshFull()).isNull();assertThat(complete.checkpoint().positionEditsRestricted()).isTrue();
                assertThat(complete.checkpoint().position().token()).isEqualTo("actual-new-batch");other.stopCapture("other",false);
            } finally {member.shutdown();}
        });
    }

    private static final class NativeSharedPort implements CapturePort,SnapshotSession.PreparedProvider,AutoCloseable {
        final CaptureConfig config;final Runnable guard;final io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer snapshot;final io.tapstate.adapters.pdk.PdkCapturePort.StreamPreparer stream;
        final AtomicReference<CaptureStartedListener> listener=new AtomicReference<>();final AtomicReference<CaptureStart> start=new AtomicReference<>();final java.util.concurrent.atomic.AtomicInteger closed=new java.util.concurrent.atomic.AtomicInteger();
        final boolean delayedStart;
        final Map<String,SourceTable> suppliedTables;final String snapshotSeam;
        NativeSharedPort(CaptureConfig config,Runnable guard,io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer snapshot,io.tapstate.adapters.pdk.PdkCapturePort.StreamPreparer stream) {
            this(config,guard,snapshot,stream,false);
        }
        NativeSharedPort(CaptureConfig config,Runnable guard,io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer snapshot,io.tapstate.adapters.pdk.PdkCapturePort.StreamPreparer stream,boolean delayedStart) {
            this(config,guard,snapshot,stream,delayedStart,null,null);
        }
        NativeSharedPort(CaptureConfig config,Runnable guard,io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer snapshot,io.tapstate.adapters.pdk.PdkCapturePort.StreamPreparer stream,
                boolean delayedStart,Map<String,SourceTable> suppliedTables,String snapshotSeam) {
            this.config=config;this.guard=guard;this.snapshot=snapshot;this.stream=stream;this.delayedStart=delayedStart;
            this.suppliedTables=suppliedTables==null?null:Map.copyOf(suppliedTables);this.snapshotSeam=snapshotSeam;assertThat(config.sharedNotes()).isNotNull();
        }
        void started(){guard.run();var original=start.get();listener.get().onStart(original instanceof CaptureStart.Resume resume?resume.position():new SourcePosition("shared-start"));}
        Map<String,SourceTable> tables(CaptureConfig actual) {var models=new HashMap<String,SourceTable>();actual.streams().forEach(table -> models.put(table,suppliedTables==null?
                new SourceTable(table,List.of(new SourceField("id","INT")),List.of("id"),List.of()):Objects.requireNonNull(suppliedTables.get(table))));return models;}
        public void close(){closed.incrementAndGet();}
        public Subscription cdc(CaptureConfig actual,CaptureStart original,CaptureListener target) {
            guard.run();stream.prepare(tables(actual));listener.set((CaptureStartedListener)target);start.set(original);
            if(!delayedStart)started();
            return new Subscription(){public void close(){closed.incrementAndGet();}};
        }
        public SnapshotSession snapshotSession(CaptureConfig actual){throw new AssertionError("unprepared shared read");}
        public SnapshotSession snapshotSession(CaptureConfig actual,SnapshotPreparation request) {
            guard.run();return new SnapshotSession(){boolean prepared;
                public CaptureBatch read(String table) {
                    guard.run();var seam=new SourcePosition(snapshotSeam==null?"snapshot-"+actual.node().pipelineId():snapshotSeam);
                    if(!prepared){snapshot.prepare(request,seam,tables(actual));prepared=true;}
                    var rows=table.equals("orders")?List.of(Envelope.read(1,table,Map.of("id",1),null)).iterator():Collections.<Envelope>emptyIterator();
                    return new CaptureBatch(){public boolean hasNext(){guard.run();return rows.hasNext();}public Envelope next(){guard.run();return rows.next();}
                        public Optional<SourcePosition> seam(){return Optional.of(seam);}public void close(){}};
                }};
        }
        public CaptureBatch snapshot(CaptureConfig actual){throw new AssertionError("unprepared shared read");}
        public ConnectionReport testConnection(CaptureConfig actual){throw new AssertionError();}public DiscoveredSchema discoverSchema(CaptureConfig actual){throw new AssertionError();}
    }
    @Test void aNewDirectBaselineCannotReclaimAnOldUndeliveredNestRelease() {
        withFixture(f -> {
            var inputs=f.inputs();f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims=f.cache.workloadClaims(f.business());var initial=claims.readPipeline(f.scope).orElseThrow().claim();
            var running=claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow();var original=WorkloadClaimFence.from(running);
            var capture=f.capture(original,inputs);var channels=f.cache.directChannels(f.business(),ACTIVE);var context=inputs.directChannels().getFirst().context();
            channels.beginCapture(context,0,0,capture,original);channels.confirmAnchor(context,1,"anchor",capture,original);
            channels.beginWriterRun(context,1,"old-writers",Map.of("orders",List.of("writer"),"quiet",List.of("writer")),original);
            channels.recordBatch(new DirectBatch(context,1,1,new ChainPosition(new SourceOrder(1,5),"settled"),Map.of("orders",4L,"quiet",3L)),0,capture,original);
            channels.advanceWriter(context,1,"old-writers","orders","writer",new WriterProgress(new SourceOrder(1,4),new ChainPosition(new SourceOrder(1,4),"settled")),original);
            channels.advanceWriter(context,1,"old-writers","quiet","writer",new WriterProgress(new SourceOrder(1,3),null),original);
            var member=mock(HazelcastInstance.class);var local=new ConcurrentHashMap<String,Object>();when(member.getUserContext()).thenReturn(local);
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true)) {
                local.put(ExecutionAuthorization.USER_CONTEXT_KEY,authorization);local.put(SrsStoreViews.USER_CONTEXT_KEY,f.cache);
                var before=DirectSourceRun.of(new ExecutionFence(f.scope,original),channels.load(context));
                var first=new ScopedSrsReplayFloorFactory(Map.of(),Map.of("orders",new ScopedDirectSinkAckFactory.Source(before,"orders"),"quiet",new ScopedDirectSinkAckFactory.Source(before,"quiet"))).resolve(member);
                var pending=io.tapstate.runtime.engine.nest.DirectNestReleaseProbe.seed(first);
                var next=WorkloadClaimFence.from(claims.advanceExecution(running,1,Set.of(OWNER.nodeId())).orElseThrow());var held=channels.load(context);
                channels.beginCapture(context,held.revision(),1,capture,next);channels.confirmAnchor(context,2,"settled",capture,next);
                var after=DirectSourceRun.of(new ExecutionFence(f.scope,next),channels.load(context));
                var restored=new ScopedSrsReplayFloorFactory(Map.of(),Map.of("orders",new ScopedDirectSinkAckFactory.Source(after,"orders"),"quiet",new ScopedDirectSinkAckFactory.Source(after,"quiet"))).resolve(member);
                pending.assertRecovered(restored);
                assertThat(restored.of("orders")).contains(new SourceOrder(1,4));assertThat(restored.of("quiet")).contains(new SourceOrder(1,3));
                assertThat(channels.load(context).cursor().sinkAcked().order()).isEqualTo(SourceOrder.snapshotRow(2));
                channels.recordBatch(new DirectBatch(context,2,1,new ChainPosition(new SourceOrder(2,0),"heartbeat"),Map.of()),0,capture,next);
                assertThat(channels.load(context).cursor().sinkAckedSrcpos()).isEqualTo("heartbeat");
                pending.assertRecovered(restored);assertThat(restored.of("orders")).contains(new SourceOrder(1,4));
                assertThat(restored.of("quiet")).contains(new SourceOrder(1,3));
            }
        });
    }
    @Test void aPrivatePartialSnapshotKeepsItsPinAndCountsWhileCdcResumesTheSettledPrefix() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.SNAPSHOT_AND_CDC,"latest",null));
            var inputs=f.inputs();f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims=f.cache.workloadClaims(f.business());var initial=claims.readPipeline(f.scope).orElseThrow().claim();
            var running=claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow();var original=WorkloadClaimFence.from(running);
            var ownership=mock(CaptureOwnership.class);
            when(ownership.acquire(any())).thenAnswer(call -> {
                var id=call.getArgument(0,CaptureId.class);var attempt=claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,id.value()),OWNER,1,TTL);
                return new CaptureOwnership.Permit(attempt.acquired(),attempt.claim());
            });
            when(ownership.renew(any())).thenAnswer(call -> claims.renew(call.getArgument(0),TTL));when(ownership.ttl()).thenReturn(TTL);
            doAnswer(call -> {claims.release(call.getArgument(0));return null;}).when(ownership).release(any());
            var provisioner=mock(io.tapstate.adapters.pdk.ConnectorProvisioner.class);
            when(provisioner.resolve("mongodb")).thenReturn(new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"not.loaded","2.0.8",null,null,"c".repeat(64),false));
            var ports=new CopyOnWriteArrayList<NativeDirectSnapshotPort>();var buffer=new SnapshotBuffer(8);
            var member=mock(HazelcastInstance.class);var local=new ConcurrentHashMap<String,Object>();when(member.getUserContext()).thenReturn(local);
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),f.cache.srs(f.business(),ACTIVE),member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,ownership,Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,buffer,provisioner,(fixed,config,guard,preparation,stream) -> {
                            var port=new NativeDirectSnapshotPort(guard,preparation,ports.isEmpty()?"sampled-original":"sampled-later",ports.isEmpty()?"sampled-original":"settled",
                                    () -> {var current=f.cache.directChannels(f.business(),ACTIVE).load(inputs.directChannels().getFirst().context());
                                        assertThat(current.cursor().snapshotEpoch()).isOne();assertThat(current.current().anchor()).isNotNull();});
                            ports.add(port);return port;
                        })) {
                local.put(ExecutionAuthorization.USER_CONTEXT_KEY,authorization);local.put(SrsStoreViews.USER_CONTEXT_KEY,f.cache);
                var artifacts=ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts());var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
                var first=coordinator.forRun(f.scope,original,new ExecutionFence(f.scope,original));first.startCapture("flow",artifacts,models);awaitLoadedCounts(first);awaitStarted(ports.getFirst().direct);
                var one=first.captureBindings().direct().get("input");assertThat(one.snapshotEpoch()).isOne();assertThat(one.epoch()).isOne();
                assertThat(buffer.drain(first.captureBindings().bufferKey("input"),SrsRingbuffer.ringName(one.context().channelId(),"orders")))
                        .singleElement().satisfies(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(1)));
                var firstFactory=copied(new ScopedSrsSinkAckFactory(Map.of(),Map.of("orders",new ScopedDirectSinkAckFactory.Source(one,"orders"),"quiet",new ScopedDirectSinkAckFactory.Source(one,"quiet")),"first-snapshot"));
                firstFactory.beginRun(member,Map.of("orders",List.of("writer"),"quiet",List.of("writer")),original);var firstAck=firstFactory.resolve(member).forWriter("writer");
                firstAck.bounded("orders",SourceOrder.snapshotRow(1),original);assertThat(first.loadDelivered("flow")).isFalse();
                ports.getFirst().direct.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",2),null)),Optional.of(new SourcePosition("settled")));
                var changes=buffer.drain(first.captureBindings().bufferKey("input"),SrsRingbuffer.ringName(one.context().channelId(),"orders"));
                firstAck.advance("orders",changes.getFirst().position(),original);awaitTold(ports.getFirst().direct,"settled");first.stopCapture("flow",false);
                var next=WorkloadClaimFence.from(claims.advanceExecution(running,1,Set.of(OWNER.nodeId())).orElseThrow());
                var second=coordinator.forRun(f.scope,next,new ExecutionFence(f.scope,next));second.startCapture("flow",artifacts,models);awaitLoadedCounts(second);awaitStarted(ports.getLast().direct);
                var two=second.captureBindings().direct().get("input");assertThat(two.epoch()).isEqualTo(2);assertThat(two.snapshotEpoch()).isOne();assertThat(two.loadEpoch()).isOne();
                assertThat(ports.getLast().reads).containsExactly("quiet");assertThat(ports.getLast().request.get().replayAnchor()).isEqualTo(new SourcePosition("sampled-original"));
                assertThat(ports.getLast().direct.start.get()).isEqualTo(CaptureStart.resume(new SourcePosition("settled")));
                assertThat(second.snapshotProgress("flow").byTable().get("orders").rowsDone()).isOne();assertThat(second.snapshotProgress("flow").byTable().get("quiet").rowsDone()).isZero();
                var secondFactory=copied(new ScopedSrsSinkAckFactory(Map.of(),Map.of("orders",new ScopedDirectSinkAckFactory.Source(two,"orders"),"quiet",new ScopedDirectSinkAckFactory.Source(two,"quiet")),"second-snapshot"));
                secondFactory.beginRun(member,Map.of("orders",List.of("writer"),"quiet",List.of("writer")),next);
                assertThat(second.loadDelivered("flow")).isFalse();secondFactory.resolve(member).forWriter("writer").bounded("quiet",SourceOrder.snapshotRow(1),next);
                assertThat(second.loadDelivered("flow")).isTrue();second.stopCapture("flow",false);
            }
            assertThat(f.cacheDatabase.getCollection("srs_chains").countDocuments()).isZero();
            assertThat(f.cacheDatabase.getCollection("snapshot_orders").find().first().getLong("order")).isZero();
        });
    }
    private static void awaitStarted(DirectPort port) throws InterruptedException {
        long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();while(port.start.get()==null && System.nanoTime()<deadline)Thread.sleep(10);
        assertThat(port.start.get()).isNotNull();
    }
    private static final class NativeDirectSnapshotPort implements CapturePort,SnapshotSession.PreparedProvider {
        final Runnable guard,beforeRow;final io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer preparation;
        final String sampled;final DirectPort direct;final List<String> reads=new CopyOnWriteArrayList<>();final AtomicReference<SnapshotPreparation> request=new AtomicReference<>();
        NativeDirectSnapshotPort(Runnable guard,io.tapstate.adapters.pdk.PdkCapturePort.SnapshotPreparer preparation,String sampled,String resume,Runnable beforeRow) {
            this.guard=guard;this.preparation=Objects.requireNonNull(preparation);this.sampled=sampled;this.direct=new DirectPort(resume);this.beforeRow=beforeRow;
        }
        public SnapshotSession snapshotSession(CaptureConfig config){throw new AssertionError("unprepared snapshot");}
        public SnapshotSession snapshotSession(CaptureConfig config,SnapshotPreparation original) {
            guard.run();request.set(original);return new SnapshotSession() {
                boolean prepared;
                public CaptureBatch read(String table) {
                    guard.run();reads.add(table);
                    if(!prepared){var actual=new HashMap<String,SourceTable>();for(String selected:config.streams())actual.put(selected,new SourceTable(selected,List.of(new SourceField("id","INT")),List.of("id"),List.of()));
                        preparation.prepare(original,new SourcePosition(sampled),actual);prepared=true;}
                    var rows=table.equals("orders")?List.of(Envelope.read(1,table,Map.of("id",1),null)).iterator():Collections.<Envelope>emptyIterator();
                    return new CaptureBatch() {
                        public boolean hasNext(){guard.run();return rows.hasNext();}
                        public Envelope next(){guard.run();beforeRow.run();return rows.next();}
                        public Optional<SourcePosition> seam(){return Optional.of(new SourcePosition(sampled));}
                        public void close(){}
                    };
                }
            };
        }
        public CaptureBatch snapshot(CaptureConfig config){throw new AssertionError("unprepared snapshot");}
        public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener){guard.run();return direct.cdc(config,start,listener);}
        public ConnectionReport testConnection(CaptureConfig config){throw new AssertionError();}
        public DiscoveredSchema discoverSchema(CaptureConfig config){throw new AssertionError();}
    }
    @ParameterizedTest @ValueSource(strings={"latest","earliest","2026-10-18T00:00:00Z"})
    void actualScopedDirectCoordinatorFeedsOriginalWritersAndResumesItsSettledToken(String firstStart) {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.CDC_ONLY,firstStart,null));
            var inputs=f.inputs();f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims=f.cache.workloadClaims(f.business());var initial=claims.readPipeline(f.scope).orElseThrow().claim();
            var running=claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow();var original=WorkloadClaimFence.from(running);
            var ownership=mock(CaptureOwnership.class);
            when(ownership.acquire(any())).thenAnswer(call -> {
                var id=call.getArgument(0,CaptureId.class);
                var attempt=claims.acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,id.value()),OWNER,1,TTL);
                return new CaptureOwnership.Permit(attempt.acquired(),attempt.claim());
            });
            when(ownership.renew(any())).thenAnswer(call -> claims.renew(call.getArgument(0),TTL));when(ownership.ttl()).thenReturn(TTL);
            doAnswer(call -> {claims.release(call.getArgument(0));return null;}).when(ownership).release(any());
            var provisioner=mock(io.tapstate.adapters.pdk.ConnectorProvisioner.class);
            when(provisioner.resolve("mongodb")).thenReturn(new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"not.loaded","2.0.8",null,null,"c".repeat(64),false));
            var ports=new ArrayList<DirectPort>();var buffer=new SnapshotBuffer(8);
            var member=mock(HazelcastInstance.class);var local=new ConcurrentHashMap<String,Object>();when(member.getUserContext()).thenReturn(local);
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),f.cache.srs(f.business(),ACTIVE),member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,ownership,Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,buffer,provisioner,(fixed,config,guard,preparation,stream) -> {guard.run();var port=new DirectPort(ports.isEmpty()?"anchor":"settled");ports.add(port);return port;})) {
                local.put(ExecutionAuthorization.USER_CONTEXT_KEY,authorization);local.put(SrsStoreViews.USER_CONTEXT_KEY,f.cache);
                var artifacts=ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts());
                var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
                var run=coordinator.forRun(f.scope,original,new ExecutionFence(f.scope,original));run.startCapture("flow",artifacts,models);
                var fixed=run.captureBindings().direct().get("input");assertThat(fixed.epoch()).isOne();assertThat(run.captureBindings().freshFullLoad()).isFalse();
                assertThat(ports.getFirst().start.get()).isEqualTo(switch(firstStart) {
                    case "latest" -> CaptureStart.present();case "earliest" -> CaptureStart.earliest();default -> CaptureStart.at(Instant.parse(firstStart));
                });
                var streams=Map.of("orders",new ScopedDirectSinkAckFactory.Source(fixed,"orders"),"quiet",new ScopedDirectSinkAckFactory.Source(fixed,"quiet"));
                var factory=copied(new ScopedSrsSinkAckFactory(Map.of(),streams,"native-direct-run"));
                factory.beginRun(member,Map.of("orders",List.of("writer"),"quiet",List.of("writer")),original);
                ports.getFirst().listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",1),null)),Optional.of(new SourcePosition("settled")));
                var rows=buffer.drain(run.captureBindings().bufferKey("input"),SrsRingbuffer.ringName(fixed.context().channelId(),"orders"));
                assertThat(rows).hasSize(1);factory.resolve(member).forWriter("writer").advance("orders",rows.getFirst().position(),original);
                awaitTold(ports.getFirst(),"settled");assertThat(run.loadDelivered("flow")).isTrue();
                var old=run.captureBindings();run.stopCapture("flow",false);
                var next=WorkloadClaimFence.from(claims.advanceExecution(running,1,Set.of(OWNER.nodeId())).orElseThrow());
                var resumed=coordinator.forRun(f.scope,next,new ExecutionFence(f.scope,next));resumed.startCapture("flow",artifacts,models);
                assertThat(resumed.captureBindings().direct().get("input").epoch()).isEqualTo(2);
                assertThat(ports.getLast().start.get()).isEqualTo(CaptureStart.resume(new SourcePosition("settled")));
                var before=f.cacheSnapshot();
                assertThatThrownBy(() -> ports.getFirst().listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",99),null)),Optional.of(new SourcePosition("late"))))
                        .isInstanceOf(TapstateException.class);
                assertThat(buffer.drain(old.bufferKey("input"),SrsRingbuffer.ringName(fixed.context().channelId(),"orders"))).isEmpty();
                assertThat(f.cacheSnapshot()).isEqualTo(before);resumed.stopCapture("flow",false);
            }
            assertThat(f.cacheDatabase.getCollection("srs_chains").countDocuments()).isZero();
            assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isOne();
            assertThat(f.cacheDatabase.getCollection("snapshot_orders").find().first().getLong("order")).isZero();
        });
    }
    @Test void actualScopedCoordinatorUsesNativePreparationNotesAndCountsAcrossAKeepStateRestart() {
        withFixture(f -> {
            var definition=(PipelineResource)new io.tapstate.core.dsl.DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: flow
                    source: [{id: input, srs: false}]
                    settings: {read_mode: snapshot_only}
                    serve:
                      from: input
                      sync: [{id: copy, source: %s, on_full_load: clear}]
                    """.formatted(StorageBinding.defaultSourceId(StorageBinding.Mode.OP)));
            f.configuration.artifacts().save(definition);
            f.cache.schemas(f.business(),ACTIVE).save(new DiscoveredSourceModel("input","mongodb",0,new SourceModel(List.of(
                    new SourceTable("orders",List.of(new SourceField("id","INT")),List.of("id"),List.of()),
                    new SourceTable("quiet",List.of(new SourceField("id","INT")),List.of("id"),List.of())))));
            var inputs=f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims=f.cache.workloadClaims(f.business()); var initial=claims.readPipeline(f.scope).orElseThrow().claim();
            var running=claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow(); var original=WorkloadClaimFence.from(running);
            var ownership=mock(CaptureOwnership.class);
            when(ownership.acquire(any())).thenAnswer(call -> {
                var id=call.getArgument(0,CaptureId.class);
                var attempt=claims.acquire(new WorkloadClaimKey(f.scope.clusterId(),WorkloadClaimType.CAPTURE,id.value()),OWNER,1,TTL);
                return new CaptureOwnership.Permit(attempt.acquired(),attempt.claim());
            });
            when(ownership.renew(any())).thenAnswer(call -> claims.renew(call.getArgument(0),TTL));
            when(ownership.ttl()).thenReturn(TTL);
            doAnswer(call -> { claims.release(call.getArgument(0)); return null; }).when(ownership).release(any());
            var provisioner=mock(io.tapstate.adapters.pdk.ConnectorProvisioner.class);
            when(provisioner.resolve("mongodb")).thenReturn(new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"not.loaded","2.0.8",null,null,"c".repeat(64),false));
            var member=mock(HazelcastInstance.class); var buffer=new SnapshotBuffer(8); var opened=new CopyOnWriteArrayList<CaptureConfig>();
            try(var authorization=ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true);
                var runtime=new NativeSrsCaptureRunUnit(f.business(),f.cache.srs(f.business(),ACTIVE),member);
                var coordinator=new ScopedPipelineCaptureCoordinator(new ScopedPipelineCaptureCoordinator.Stores(f.business(),f.cache,f.cache,
                        f.configuration.capturePreparations(),f.cache.capturePreparations(f.business(),ACTIVE),f.configuration.requiredNotes()),
                        new ScopedPipelineCaptureCoordinator.Execution(() -> Optional.of(f.node),authorization,ownership,Duration.ofSeconds(10),ACTIVE,CLOCK),
                        runtime,buffer,provisioner,(fixed,config,guard,preparation,stream) -> { guard.run();opened.add(config);return new NativeBoundedPort(guard); })) {
                var artifacts=ReadOnlyArtifactSnapshot.capture(f.configuration.artifacts());
                var models=SourceModelSnapshot.capture("flow",List.of(f.source()),f.cache.schemas(f.business(),ACTIVE));
                var first=coordinator.forRun(f.scope,original,new ExecutionFence(f.scope,original)); first.startCapture("flow",artifacts,models);
                awaitLoadedCounts(first); var firstBindings=first.captureBindings();
                assertThat(firstBindings.snapshots().get("input").epoch()).isOne();
                assertThat(first.snapshotProgress("flow").byTable().get("orders").rowsDone()).isOne();
                assertThat(first.snapshotProgress("flow").byTable().get("quiet").rowsDone()).isZero();
                String ring=SourceCaptureResolution.forPipeline(f.pipeline(),f.source(),models.model(f.source())).orElseThrow().ringName("orders");
                assertThat(buffer.drain(firstBindings.bufferKey("input"),ring)).singleElement().satisfies(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(1)));
                var targetInputs=buildBoundedDag(f,provisioner,firstBindings);
                assertThat(targetInputs).hasSize(2).allSatisfy(input -> assertThat(input.fullLoad()).isTrue());
                var originalTargets=f.cache.sinkPreparations(f.scope,original,ACTIVE);
                for(var input:targetInputs)originalTargets.complete(originalTargets.begin(input),new SinkPreparationStore.Result(false));
                var readyTargets=f.cacheDatabase.getCollection("sink_preparations").find().into(new ArrayList<>());
                var oldView=f.cache.snapshotStates(f.scope,original,ACTIVE); assertThat(oldView.order()).isOne();
                first.stopCapture("flow",false); assertThat(oldView.count("input","orders",1)).hasValue(1);
                var next=WorkloadClaimFence.from(claims.advanceExecution(running,1,Set.of(OWNER.nodeId())).orElseThrow());
                var second=coordinator.forRun(f.scope,next,new ExecutionFence(f.scope,next)); second.startCapture("flow",artifacts,models); awaitLoadedCounts(second);
                assertThat(second.captureBindings().snapshots().get("input").epoch()).isEqualTo(2);
                assertThat(buildBoundedDag(f,provisioner,second.captureBindings())).containsExactlyElementsOf(targetInputs);
                assertThat(f.cacheDatabase.getCollection("sink_preparations").find().into(new ArrayList<>())).containsExactlyElementsOf(readyTargets);
                assertThat(f.cache.snapshotStates(f.scope,next,ACTIVE).count("input","quiet",2)).hasValue(0);
                assertThatThrownBy(oldView::order).isInstanceOf(TapstateException.class);
                second.stopCapture("flow",false);
                assertThat(opened).hasSize(2).allSatisfy(config -> {
                    assertThat(config.node()).isEqualTo(new PipelineNode("flow","input")); assertThat(config.sharedNotes()).isNull();
                    assertThat(config.settings()).containsEntry("password","test-secret");
                });
            }
            assertThat(f.cacheDatabase.getCollection("srs_chains").countDocuments()).isZero();
            assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isZero();
            assertThat(f.cacheDatabase.getCollection("snapshot_orders").countDocuments()).isOne();
            assertThat(f.configDatabase.getCollection("pdk_note_origins").find().first().toJson()).doesNotContain("test-secret","x.invalid");
        });
    }
    private static List<SinkPreparationStore.Input> buildBoundedDag(Fixture f,
            io.tapstate.adapters.pdk.ConnectorProvisioner provisioner,PipelineCaptureBindings bindings) {
        var mixed=mock(StorePort.class);when(mixed.artifacts()).thenReturn(f.configuration.artifacts());
        when(mixed.schemas()).thenReturn(f.cache.schemas(f.business(),ACTIVE));
        var views=mock(PipelineStoreViews.class,org.mockito.AdditionalAnswers.delegatesTo(f.cache));
        var inputs=new AtomicReference<List<SinkPreparationStore.Input>>();
        doAnswer(call -> {
            var actual=f.cache.sinkPreparations(call.getArgument(0),call.getArgument(1),call.getArgument(2));
            var observed=spy(actual);
            doAnswer(prepare -> { var targets=List.copyOf(prepare.<List<SinkPreparationStore.Input>>getArgument(0));
                inputs.set(targets);actual.prepareTargets(targets);return null;
            }).when(observed).prepareTargets(anyList());
            return observed;
        }).when(views).sinkPreparations(any(),any(),any());
        var scope=bindings.original().scope();
        var intent=f.configuration.intents().load(scope.pipelineId()).orElseGet(() -> new PipelineIntentSnapshot(scope,1,
                new io.tapstate.core.lifecycle.DesiredState(scope.pipelineId(),io.tapstate.core.lifecycle.PipelineState.RUNNING,
                        io.tapstate.core.model.canonical.CanonicalHash.of(f.pipeline()))));
        var pass=new PipelineConvergencePass(intent,
                new WorkloadClaimFence(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId()),OWNER,
                        bindings.original().claimGeneration(),bindings.original().executionGeneration(),bindings.original().topologyRevision()),views,ACTIVE);
        var source=new StoreBackedDagSource(mixed,new StoreBackedDagSource.PdkSinkWriterBinder(f.configuration.requiredNotes(),provisioner));
        var prepared=source.prepareStart(pass,"cache");pass.bindCapture(pass.claim(),bindings);
        assertThat(prepared.plan().build(bindings.original()).dag()).isNotNull();return inputs.get();
    }
    private static void awaitLoadedCounts(PipelineCaptureCoordinator run) throws InterruptedException {
        long deadline=System.nanoTime()+Duration.ofSeconds(10).toNanos();
        while(run.snapshotProgress("flow").byTable().size()!=2 && System.nanoTime()<deadline) {
            if(run.captureFailure("flow").isPresent())throw new AssertionError("native capture failed",run.captureFailure("flow").orElseThrow());
            Thread.sleep(25);
        }
        assertThat(run.snapshotProgress("flow").byTable()).containsOnlyKeys("orders","quiet");
    }
    private static final class NativeBoundedPort implements CapturePort,SnapshotOnlyCapture {
        final Runnable guard;
        NativeBoundedPort(Runnable guard) { this.guard=guard; }
        public CaptureBatch snapshotOnly(CaptureConfig config) {
            guard.run();var rows=List.of(Envelope.read(1,"orders",Map.of("id",1),null)).iterator();
            return new CaptureBatch() {
                public boolean hasNext() { guard.run();return rows.hasNext(); }
                public Envelope next() { guard.run();return rows.next(); }
                public Optional<SourcePosition> seam() { throw new AssertionError("bounded-only sources have no CDC seam"); }
                public void close() {}
            };
        }
        public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError(); }
        public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) { throw new AssertionError(); }
        public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
        public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
    }
    @Test void snapshotOnlyOrderAndCountsUseSeparateNativeResourcesAndSurviveOriginalScopeReopen() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.SNAPSHOT_ONLY,"latest",null));
            var inputs = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims = f.cache.workloadClaims(f.business()); var initial = claims.readPipeline(f.scope).orElseThrow().claim();
            var running = claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow(); var original = WorkloadClaimFence.from(running);
            var capture = f.capture(original,inputs); var preparation = f.configuration.capturePreparations().read(f.business(),capture.key().resourceId()).orElseThrow();
            var captured = f.cache.capturePreparations(f.business(),ACTIVE).load(preparation); var captures = Map.of("input",captured);
            assertThat(captured.preparation().mode()).isEqualTo(CapturePreparation.Mode.SNAPSHOT_ONLY);
            var snapshots = f.cache.snapshotStates(f.scope,original,ACTIVE); assertThat(snapshots.order()).isZero();
            assertThat(snapshots.advanceOrder(0,7,captures)).isEqualTo(8);
            assertThat(snapshots.advanceOrder(0,7,captures)).isEqualTo(8);
            snapshots.recordCount("input","orders",8,12); snapshots.recordCount("input","quiet",8,0);
            assertThat(snapshots.count("input","orders",8)).hasValue(12);
            assertThat(snapshots.count("input","quiet",8)).hasValue(0);
            assertThat(snapshots.count("input","orders",7)).isEmpty();
            var order = f.cacheDatabase.getCollection("snapshot_orders").find().first();
            assertThat(order.keySet()).containsExactlyInAnyOrder("_id","formatVersion","order","previousOrder","retainedFloor","command","captures");
            assertThat(f.cacheDatabase.getCollection("snapshot_counts").find().first().keySet()).containsExactlyInAnyOrder("_id","formatVersion","snapshotEpoch","rows");
            try (var reopened = MongoCacheDomain.connect(f.cacheConnection,Duration.ofSeconds(5),"snapshot-reopen")) {
                var same = reopened.snapshotStates(f.scope,original,ACTIVE); assertThat(same.order()).isEqualTo(8);
                assertThat(same.count("input","orders",8)).hasValue(12);
                var next = WorkloadClaimFence.from(claims.advanceExecution(running,1,Set.of(OWNER.nodeId())).orElseThrow());
                assertThatThrownBy(snapshots::order).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> snapshots.recordCount("input","orders",8,99)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> snapshots.count("input","orders",8)).isInstanceOf(TapstateException.class);
                var current = reopened.snapshotStates(f.scope,next,ACTIVE);
                assertThat(current.advanceOrder(8,7,captures)).isEqualTo(9);
                assertThat(current.count("input","orders",8)).hasValue(12);
                current.recordCount("input","orders",9,3);
                assertThat(current.count("input","orders",8)).isEmpty(); assertThat(current.count("input","orders",9)).hasValue(3);
                assertThatThrownBy(() -> current.recordCount("input","orders",8,100)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> current.recordCount("input","foreign",9,100)).isInstanceOf(TapstateException.class);
            }
            assertThat(f.cacheDatabase.getCollection("srs_chains").countDocuments()).isZero();
            assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isZero();
            assertThat(f.configDatabase.listCollectionNames()).doesNotContain("snapshot_orders","snapshot_counts");
        });
    }
    @Test void concurrentSnapshotOrderRetriesShareOneOriginalExecutionAllocation() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.SNAPSHOT_ONLY,"latest",null));
            var inputs = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims = f.cache.workloadClaims(f.business()); var initial = claims.readPipeline(f.scope).orElseThrow().claim();
            var original = WorkloadClaimFence.from(claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow());
            var capture = f.capture(original,inputs); var prepared = f.configuration.capturePreparations().read(f.business(),capture.key().resourceId()).orElseThrow();
            var captures = Map.of("input",f.cache.capturePreparations(f.business(),ACTIVE).load(prepared));
            var snapshots = f.cache.snapshotStates(f.scope,original,ACTIVE);
            try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2)) {
                var start = new java.util.concurrent.CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return snapshots.advanceOrder(0,0,captures); });
                var b = executor.submit(() -> { start.await(); return snapshots.advanceOrder(0,0,captures); }); start.countDown();
                assertThat(a.get(10,java.util.concurrent.TimeUnit.SECONDS)).isOne();
                assertThat(b.get(10,java.util.concurrent.TimeUnit.SECONDS)).isOne();
            }
            assertThat(snapshots.order()).isOne();
            assertThat(f.cacheDatabase.getCollection("snapshot_orders").countDocuments()).isOne();
        });
    }
    @Test void missingKnownSnapshotOrderCannotBeReseededByPreparationOrAnOrdinaryRun() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.SNAPSHOT_ONLY,"latest",null));
            var inputs = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims = f.cache.workloadClaims(f.business()); var initial = claims.readPipeline(f.scope).orElseThrow().claim();
            var original = WorkloadClaimFence.from(claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow());
            var capture = f.capture(original,inputs); var prepared = f.configuration.capturePreparations().read(f.business(),capture.key().resourceId()).orElseThrow();
            var captures = Map.of("input",f.cache.capturePreparations(f.business(),ACTIVE).load(prepared));
            var snapshots = f.cache.snapshotStates(f.scope,original,ACTIVE); snapshots.advanceOrder(0,0,captures);
            snapshots.recordCount("input","orders",1,12);
            f.cacheDatabase.getCollection("snapshot_orders").deleteMany(new Document());
            assertThatThrownBy(snapshots::order).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> snapshots.advanceOrder(0,0,captures)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,inputs)).isInstanceOf(TapstateException.class);
            assertThat(f.cacheDatabase.getCollection("snapshot_orders").countDocuments()).isZero();
            assertThat(f.cacheDatabase.getCollection("snapshot_counts").countDocuments()).isOne();
        });
    }
    @Test void aDirectCaptureCannotAuthorizeSnapshotOnlyOrderAllocation() {
        withFixture(f -> {
            var inputs = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims = f.cache.workloadClaims(f.business()); var initial = claims.readPipeline(f.scope).orElseThrow().claim();
            var original = WorkloadClaimFence.from(claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow());
            var capture = f.capture(original,inputs); var prepared = f.configuration.capturePreparations().read(f.business(),capture.key().resourceId()).orElseThrow();
            var captures = Map.of("input",f.cache.capturePreparations(f.business(),ACTIVE).load(prepared));
            var snapshots = f.cache.snapshotStates(f.scope,original,ACTIVE);
            assertThatThrownBy(() -> snapshots.advanceOrder(0,0,captures)).isInstanceOf(TapstateException.class);
            assertThat(snapshots.order()).isZero();
        });
    }
    @Test void aReleasedOriginalSnapshotCaptureCannotAllocateANewOrderOrChangeTheCheckpoint() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.SNAPSHOT_ONLY,"latest",null));
            var inputs=f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var claims=f.cache.workloadClaims(f.business()); var initial=claims.readPipeline(f.scope).orElseThrow().claim();
            var original=WorkloadClaimFence.from(claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow());
            var capture=f.capture(original,inputs); var prepared=f.configuration.capturePreparations().read(f.business(),capture.key().resourceId()).orElseThrow();
            var captures=Map.of("input",f.cache.capturePreparations(f.business(),ACTIVE).load(prepared));
            assertThat(claims.release(claims.read(capture.key()).orElseThrow().claim())).isTrue();
            var snapshots=f.cache.snapshotStates(f.scope,original,ACTIVE); var before=f.cacheSnapshot();
            assertThatThrownBy(() -> snapshots.advanceOrder(0,0,captures)).isInstanceOf(TapstateException.class);
            assertThat(snapshots.order()).isZero(); assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @Test void nativeProducerAndSerializedWriterCallbacksSettleOnlyTheCompletePrefixAndReopenAtItsActualToken() {
        withFixture(f -> {
            var inputs = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var context = inputs.directChannels().getFirst().context(); var claims = f.cache.workloadClaims(f.business());
            var initial = claims.readPipeline(f.scope).orElseThrow().claim();
            var running = claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow(); var original = WorkloadClaimFence.from(running);
            var capture = f.capture(original,inputs);
            var channels = f.cache.directChannels(f.business(),ACTIVE);
            assertThat(channels.beginCapture(context,0,0,capture,original)).hasValue(1);
            var opened = channels.load(context); var execution = new ExecutionFence(f.scope,original);
            var fixed = DirectSourceRun.of(execution,opened);
            var directSources = Map.of("input.orders",new ScopedDirectSinkAckFactory.Source(fixed,"orders"),
                    "input.quiet",new ScopedDirectSinkAckFactory.Source(fixed,"quiet"));
            var factory = copied(new ScopedSrsSinkAckFactory(Map.of(),directSources,"original-run"));
            var member = mock(HazelcastInstance.class); var local = new ConcurrentHashMap<String,Object>(); when(member.getUserContext()).thenReturn(local);
            try (var authorization = ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(2),System::nanoTime,() -> true)) {
                local.put(ExecutionAuthorization.USER_CONTEXT_KEY,authorization); local.put(SrsStoreViews.USER_CONTEXT_KEY,f.cache);
                factory.beginRun(member,Map.of("input.orders",List.of("fast","slow"),"input.quiet",List.of("fast","slow")),original);
                var ack = factory.resolve(member); var fast = ack.forWriter("fast"); var slow = ack.forWriter("slow");
                var floor = new ScopedSrsReplayFloorFactory(Map.of(),directSources).resolve(member);
                var rows = new ArrayList<Envelope>(); var port = new DirectPort("anchor"); var health = new CaptureHealth();
                var producer = new DirectCaptureWriter(opened,capture,original,channels,rows::add,32,ACTIVE);
                try (var acknowledgements = new SourceAcknowledgements();
                     var source = producer.start(port,new CaptureConfig("fixture",Map.of(),List.of("orders","quiet"),new PipelineNode("flow","input")),CaptureStart.present(),health);
                     var followed = acknowledgements.follow(channels,opened,source,health,ACTIVE)) {
                    assertThat(port.told).containsExactly("anchor");
                    port.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",1),null),Envelope.insert(1,"quiet",Map.of("id",2),null)),Optional.of(new SourcePosition("first")));
                    port.listener.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",3),null)),Optional.of(new SourcePosition("last")));
                    assertThat(channels.pending(context,1)).hasSize(2);
                    fast.advance("input.orders",rows.get(2).position(),original); fast.advance("input.quiet",rows.get(1).position(),original);
                    assertThat(channels.load(context).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
                    assertThat(floor.of("input.orders")).isEmpty();
                    slow.advance("input.orders",rows.getFirst().position(),original);
                    assertThat(channels.load(context).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
                    slow.advance("input.quiet",rows.get(1).position(),original);
                    assertThat(channels.pending(context,1)).singleElement().satisfies(batch -> assertThat(batch.boundary().token()).isEqualTo("last"));
                    assertThat(channels.load(context).cursor().sinkAckedSrcpos()).isEqualTo("first");
                    assertThat(floor.of("input.orders")).contains(new SourceOrder(1,0));
                    assertThat(floor.of("input.quiet")).contains(new SourceOrder(1,1));
                    awaitTold(port,"first");
                    slow.advance("input.orders",rows.get(2).position(),original);
                    assertThat(channels.pending(context,1)).isEmpty(); awaitTold(port,"last");
                    assertThat(port.told).containsExactly("anchor","first","last");
                }
                try (var reopened = MongoCacheDomain.connect(f.cacheConnection,Duration.ofSeconds(5),"direct-writer-reopen")) {
                    var nextChannels = reopened.directChannels(f.business(),ACTIVE); var retained = nextChannels.load(context);
                    assertThat(retained.cursor().sinkAckedSrcpos()).isEqualTo("last"); assertThat(nextChannels.pending(context,1)).isEmpty();
                    var next = WorkloadClaimFence.from(claims.advanceExecution(running,1,Set.of(OWNER.nodeId())).orElseThrow());
                    assertThat(nextChannels.beginCapture(context,retained.revision(),1,capture,next)).hasValue(2);
                    var resumed = nextChannels.load(context); var resumePort = new DirectPort("last");
                    try (var source = new DirectCaptureWriter(resumed,capture,next,nextChannels,rows::add,32,ACTIVE).start(resumePort,
                            new CaptureConfig("fixture",Map.of(),List.of("orders","quiet"),new PipelineNode("flow","input")),CaptureStart.resume(new SourcePosition("last")),new CaptureHealth())) {
                        assertThat(resumePort.start.get()).isEqualTo(CaptureStart.resume(new SourcePosition("last")));
                        assertThat(nextChannels.load(context).cursor().sinkAckedSrcpos()).isEqualTo("last");
                        var before = f.cacheSnapshot();
                        assertThatThrownBy(() -> slow.advance("input.orders",new ChainPosition(new SourceOrder(1,100),"late"),original)).isInstanceOf(TapstateException.class);
                        assertThatThrownBy(() -> floor.of("input.orders")).isInstanceOf(TapstateException.class);
                        assertThat(f.cacheSnapshot()).isEqualTo(before);
                    }
                }
            }
            assertThat(f.configDatabase.listCollectionNames()).doesNotContain("direct_channel_offsets","direct_batches","srs_chains");
        });
    }
    private static io.tapstate.runtime.engine.SinkAckFactory copied(io.tapstate.runtime.engine.SinkAckFactory factory) throws Exception {
        var bytes = new ByteArrayOutputStream(); try (var out = new ObjectOutputStream(bytes)) { out.writeObject(factory); }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { return (io.tapstate.runtime.engine.SinkAckFactory)in.readObject(); }
    }
    private static void awaitTold(DirectPort port,String token) throws InterruptedException {
        long deadline = System.nanoTime()+Duration.ofSeconds(10).toNanos();
        while (!port.told.contains(token) && System.nanoTime()<deadline) Thread.sleep(25);
        assertThat(port.told).contains(token);
    }
    private static final class DirectPort implements CapturePort {
        final String anchor; final AtomicReference<CaptureStartedListener> listener = new AtomicReference<>();
        final AtomicReference<CaptureStart> start = new AtomicReference<>(); final List<String> told = new CopyOnWriteArrayList<>();
        DirectPort(String anchor) { this.anchor = anchor; }
        @Override public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) {
            this.start.set(start); var started = (CaptureStartedListener)listener; this.listener.set(started); started.onStart(new SourcePosition(anchor));
            return new Subscription() { public void acknowledge(SourcePosition point) { told.add(point.token()); } public void close() {} };
        }
        @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError(); }
        @Override public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
        @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
    }
    @Test void oneNativeGrantSeedsThePrivateClosureWithoutCopyingTheManifestOrSecretsToConfiguration() {
        withFixture(f -> {
            long version = f.version(); var inputs = f.inputs(); var fixed = inputs.directChannels().getFirst();
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.version()).isEqualTo(version); assertThat(f.configDatabase.getCollection("operator_generations").find().first().toJson())
                    .doesNotContain("pipelineSources","directChannels","sourceHashes");
            var claim = f.claim(); assertThat(claim.get("pipelineSources",Document.class)).isNotNull();
            assertThat(claim.toJson()).doesNotContain("test-secret","mongodb://x.invalid/user");
            assertThat(f.cache.directChannels(f.business(),ACTIVE).load(fixed.context()).tables()).containsExactlyInAnyOrder("orders","quiet");
            assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isOne();
            assertThat(f.cacheDatabase.getCollection("direct_batches").countDocuments()).isZero();
        });
    }
    @Test void aNativeReopenVerifiesTheSameContractWithoutResettingRunningWriterOrPendingFacts() {
        withFixture(f -> {
            var inputs = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            var context = inputs.directChannels().getFirst().context(); var claims = f.cache.workloadClaims(f.business());
            var initial = claims.readPipeline(f.scope).orElseThrow().claim();
            var execution = WorkloadClaimFence.from(claims.advanceExecution(initial,1,Set.of(OWNER.nodeId())).orElseThrow());
            var capture = f.capture(execution,inputs);
            var direct = f.cache.directChannels(f.business(),ACTIVE); direct.beginCapture(context,0,0,capture,execution); direct.confirmAnchor(context,1,"anchor",capture,execution);
            direct.beginWriterRun(context,1,"run",Map.of("orders",List.of("writer"),"quiet",List.of("writer")),execution);
            direct.recordBatch(new DirectBatch(context,1,1,new ChainPosition(new SourceOrder(1,2),"high"),Map.of("orders",1L)),0,capture,execution);
            var before = f.cacheSnapshot();
            try (var configuration = MongoConfigurationDomain.open(f.settings(),f.scope.clusterId()); var cache = MongoCacheDomain.connect(f.cacheConnection,Duration.ofSeconds(5),"direct-reopen")) {
                assertThat(f.preparer(configuration,cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
                assertThat(cache.directChannels(f.business(),ACTIVE).pending(context,1)).hasSize(1);
                assertThat(cache.directChannels(f.business(),ACTIVE).load(context).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
            }
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"coordinates","reference","srs","selection","read-mode","start"})
    void identityChangesRefuseTheOldGenerationAndLeaveAllRetainedFactsUntouched(String change) {
        withFixture(f -> {
            var old = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,old);
            var source = f.source(); var pipeline = f.pipeline();
            switch (change) {
                case "coordinates" -> f.configuration.artifacts().save(source("input","y",null));
                case "reference" -> {
                    f.configuration.artifacts().create(source("other","x",null));
                    f.configuration.artifacts().save(pipeline("other",false,ReadMode.CDC_ONLY,"latest",null));
                }
                case "srs" -> f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,"latest",null));
                case "selection" -> f.configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),source.config(),source.mode(),List.of(TableRef.literal("orders")),source.srs(),null));
                case "read-mode" -> f.configuration.artifacts().save(pipeline("input",false,ReadMode.SNAPSHOT_ONLY,"latest",null));
                case "start" -> f.configuration.artifacts().save(pipeline("input",false,ReadMode.CDC_ONLY,"earliest",null));
                default -> throw new AssertionError(change);
            }
            var requested = f.inputs();
            if (change.equals("coordinates")) {
                assertThat(requested.directChannels().getFirst().context()).isEqualTo(old.directChannels().getFirst().context());
                assertThat(requested.directChannels().getFirst().sourceIdentity()).isNotEqualTo(old.directChannels().getFirst().sourceIdentity());
            }
            var before = f.cacheSnapshot(); var mapping = f.configDatabase.getCollection("operator_generations").find().first();
            assertCode(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,requested),IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN);
            assertThat(f.cacheSnapshot()).isEqualTo(before); assertThat(f.configDatabase.getCollection("operator_generations").find().first()).isEqualTo(mapping);
            assertThat(f.configuration.pipelineStates().read("flow").orElseThrow().stateGeneration()).isOne();
        });
    }
    @Test void sharedTableAdditionsRefuseThePreparedSelectionWithoutWrites() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,"latest",null));
            var source = f.source();
            f.configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),source.config(),
                    source.mode(),List.of(TableRef.literal("orders")),source.srs(),null));
            var original = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,original);
            f.configuration.artifacts().save(source);
            var requested = f.inputs();
            assertThat(original.selectedTables().get("input")).containsExactly("orders");
            assertThat(requested.selectedTables().get("input")).containsExactlyInAnyOrder("orders","quiet");
            var before = f.cacheSnapshot(); var mapping = f.configDatabase.getCollection("operator_generations").find().first();
            long version = f.version();
            assertCode(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,requested),IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
            assertThat(f.configDatabase.getCollection("operator_generations").find().first()).isEqualTo(mapping);
            assertThat(f.version()).isEqualTo(version);
        });
    }
    @Test void sharedReductionKeepsTheOriginalPreparationWithoutRewritingIt() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,"latest",null));
            var original = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,original);
            var source = f.source();
            f.configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),source.config(),
                    source.mode(),List.of(TableRef.literal("orders")),source.srs(),null));
            var before = f.cacheSnapshot();
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs()).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @Test void sharedInitialSelectionAndUnchangedReopenRemainSupported() {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",true,ReadMode.CDC_ONLY,"latest",null));
            var inputs = f.inputs();
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            var before = f.cacheSnapshot();
            try (var reopened = MongoConfigurationDomain.open(f.settings(),f.scope.clusterId())) {
                assertThat(f.preparer(reopened,f.cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            }
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @Test void metadataOnlyEditsUseTheSameRetainedRecoveryContract() {
        withFixture(f -> {
            var original = f.inputs(); f.preparer(f.configuration,f.cache).prepare(f.scope,original); var before = f.cacheSnapshot();
            var labels = new Metadata(Map.of("team","test"),"updated display text");
            f.configuration.artifacts().saveAll(List.of(source("input","x",labels),pipeline("input",false,ReadMode.CDC_ONLY,"latest",labels)));
            var next = f.inputs(); assertThat(next).isNotEqualTo(original); assertThat(next.sameRecovery(original)).isTrue();
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,next).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"progress","all-direct","manifest","checkpoint","claim"})
    void completedStatusCannotRecreateLostCacheHistory(String lost) {
        withFixture(f -> {
            var inputs = f.inputs(); var complete = f.preparer(f.configuration,f.cache).prepare(f.scope,inputs);
            switch (lost) {
                case "progress" -> f.cacheDatabase.getCollection("direct_channel_offsets").deleteMany(new Document());
                case "all-direct" -> { f.cacheDatabase.getCollection("direct_channel_offsets").deleteMany(new Document()); f.cacheDatabase.getCollection("direct_batches").deleteMany(new Document()); }
                case "manifest" -> f.cacheDatabase.getCollection("workload_claims").updateOne(new Document("_id",f.claim().get("_id")),new Document("$unset",new Document("pipelineSources","")));
                case "checkpoint" -> f.cacheDatabase.getCollection("pipeline_state").deleteMany(new Document());
                case "claim" -> f.cacheDatabase.getCollection("workload_claims").deleteOne(new Document("_id",f.claim().get("_id")));
                default -> throw new AssertionError(lost);
            }
            var before = f.cacheSnapshot();
            assertThatThrownBy(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,inputs)).isInstanceOf(TapstateException.class);
            assertThat(f.configuration.preparations().read("flow")).contains(complete);
            assertThat(f.configuration.preparations().issueFirstCreate(f.scope,f.node,inputs)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @Test void anExistingOriginalReservationCanFinishButIssuanceWithoutCacheHistoryCannotStartAgain() {
        withFixture(f -> {
            var inputs = f.inputs(); var grant = ((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(f.scope,f.node,inputs)).creation();
            var original = f.cache.pipelinePreparations(f.business(),ACTIVE).reserveFirst(grant,TTL);
            assertThat(original.phase()).isEqualTo(PipelineCachePreparations.Phase.RESERVED); assertThat(f.cacheDatabase.getCollection("direct_channel_offsets").countDocuments()).isZero();
            try (var configuration = MongoConfigurationDomain.open(f.settings(),f.scope.clusterId()); var cache = MongoCacheDomain.connect(f.cacheConnection,Duration.ofSeconds(5),"direct-resume")) {
                assertThat(f.preparer(configuration,cache).prepare(f.scope,inputs).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
                assertThat(cache.directChannels(f.business(),ACTIVE).load(inputs.directChannels().getFirst().context()).current()).isNull();
            }
        });
    }
    @Test void changedDefinitionsBeforeIssuanceCannotSpendTheFirstGrantForAStaleSnapshot() {
        withFixture(f -> {
            var stale = f.inputs(); f.configuration.artifacts().save(source("input","y",null)); var before = f.cacheSnapshot();
            assertCode(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,stale),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
            assertThat(f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs()).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
        });
    }
    @ParameterizedTest @ValueSource(strings={"not-an-instant","+1000000000-12-31T23:59:59.999999999Z","at:2026-10-19T00:00:00Z"})
    void invalidStartsCannotSpendTheIrreversibleConfigurationGrant(String start) {
        withFixture(f -> {
            f.configuration.artifacts().save(pipeline("input",false,ReadMode.CDC_ONLY,start,null)); var before = f.cacheSnapshot();
            assertCode(() -> f.preparer(f.configuration,f.cache).prepare(f.scope,f.inputs()),IoError.DOCUMENT_INVALID);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            assertThat(f.cacheSnapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"capture","channel","missing-direct","selection"})
    void configurationReDerivesPrivateCoordinatesInsteadOfTrustingTheCaller(String fault) {
        withFixture(f -> {
            var original = f.inputs(); var direct = original.directChannels().getFirst(); var context = direct.context();
            var wrong = new DirectChannelContext(f.scope,fault.equals("channel") ? "foreign-channel" : context.channelId(),context.sourceId(),fault.equals("capture") ? "foreign-capture" : context.captureId());
            var channels = fault.equals("missing-direct") ? List.<DirectChannelPreparation>of() : List.of(new DirectChannelPreparation(wrong,direct.tables(),direct.readMode(),direct.sourceIdentity()));
            var inputs = new PipelinePreparationInputs(original.pipelineHash(),original.sourceHashes(),original.selectedTables(),channels);
            if (fault.equals("selection")) {
                var altered = DirectChannelPreparation.of(f.scope,f.source(),ReadMode.CDC_ONLY,"latest",Set.of("foreign"));
                inputs = new PipelinePreparationInputs(original.pipelineHash(),original.sourceHashes(),Map.of("input",Set.of("foreign")),List.of(altered));
            }
            var requested = inputs;
            assertCode(() -> f.configuration.preparations().issueFirstCreate(f.scope,f.node,requested),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
        });
    }
    private static SourceResource source(String id,String database,Metadata metadata) {
        return new SourceResource(id,metadata,"mongodb",Map.of("uri","mongodb://"+database+".invalid/user","password","test-secret"),SourceMode.CDC,
                List.of(TableRef.literal("orders"),TableRef.literal("quiet")),new Srs("explicit-key",null,null,null,false),null);
    }
    private static PipelineResource pipeline(String source,boolean shared,ReadMode mode,String start,Metadata metadata) {
        return new PipelineResource("flow",metadata,List.of(SourceRef.spec(source,shared)),null,null,null,new Settings(null,null,null,null,mode,start),null);
    }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action,io.tapstate.core.common.TapstateErrorCode expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,failure -> { assertThat(failure.code()).isEqualTo(expected); assertThat(failure.getCause()).isNull(); });
    }
    @Test void departedBootCleanupReachesBothIndependentServersAndPreservesOtherBoots() {
        withFixture(f -> {
            String gone = UUID.randomUUID().toString(), other = UUID.randomUUID().toString();
            long heldAt = System.nanoTime();
            try (var configGone = heldProbe(CONFIG.getReplicaSetUrl(f.configDatabase.getName()), gone);
                    var cacheGone = heldProbe(CACHE.getReplicaSetUrl(f.cacheDatabase.getName()), gone);
                    var configOther = heldProbe(CONFIG.getReplicaSetUrl(f.configDatabase.getName()), other);
                    var cacheOther = heldProbe(CACHE.getReplicaSetUrl(f.cacheDatabase.getName()), other)) {
                assertThat(List.of(configGone, cacheGone, configOther, cacheOther)).allMatch(HeldProbe::held);
                var bound = mock(StorageRuntime.Bound.class);
                when(bound.cache()).thenReturn(f.cache);
                var port = new ThreeLayerStorePort(f.configuration, bound, ignored -> {
                    throw new AssertionError("departed transaction cleanup must not prepare a Pipeline");
                });

                assertThat(port.endTransactionsLeftOpenBy(gone)).isEqualTo(2);

                assertThat(Duration.ofNanos(System.nanoTime() - heldAt)).isLessThan(Duration.ofSeconds(40));
                assertThat(configGone.held()).isFalse();
                assertThat(cacheGone.held()).isFalse();
                assertThat(configOther.held()).isTrue();
                assertThat(cacheOther.held()).isTrue();
            }
        });
    }

    private record HeldProbe(com.mongodb.client.MongoClient client, com.mongodb.client.ClientSession session) implements AutoCloseable {
        boolean held() {
            return client.getDatabase("admin").aggregate(List.of(
                    new Document("$currentOp", new Document("allUsers", false).append("idleSessions", true)),
                    new Document("$match", new Document("lsid.id", session.getServerSession().getIdentifier().get("id"))
                            .append("transaction", new Document("$exists", true))))).first() != null;
        }
        @Override public void close() { try { session.close(); } finally { client.close(); } }
    }

    private static HeldProbe heldProbe(String uri, String boot) {
        var connectionString = new com.mongodb.ConnectionString(uri);
        var client = MongoClients.create(com.mongodb.MongoClientSettings.builder().applyConnectionString(connectionString)
                .applicationName(MongoConnection.processTag(boot))
                .applyToClusterSettings(builder -> builder.serverSelectionTimeout(5, java.util.concurrent.TimeUnit.SECONDS)).build());
        com.mongodb.client.ClientSession session = null;
        try {
            var collection = client.getDatabase(connectionString.getDatabase()).getCollection("departure_probe");
            String id = UUID.randomUUID().toString();
            collection.insertOne(new Document("_id", id).append("value", 0));
            session = client.startSession();
            session.startTransaction();
            assertThat(collection.updateOne(session, new Document("_id", id),
                    new Document("$inc", new Document("value", 1))).getMatchedCount()).isOne();
            return new HeldProbe(client, session);
        } catch (RuntimeException | Error failure) {
            try { if (session != null) session.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            try { client.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private static void withFixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-",""); var configName = "direct_cfg_"+suffix; var cacheName = "direct_cache_"+suffix;
        try (var configClient = MongoClients.create(CONFIG.getReplicaSetUrl()); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var configDb = configClient.getDatabase(configName); var cacheDb = cacheClient.getDatabase(cacheName);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,CACHE.getReplicaSetUrl(cacheName),cacheName,null);
            try (var configuration = MongoConfigurationBootstrap.prepare(settings,"cluster","installation","setup",1);
                    var cache = MongoCacheDomain.connect(cacheConnection,Duration.ofSeconds(5),"direct-native")) {
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP),null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                var committed = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster","installation","setup",1),StorageBinding.Mode.OP,cacheConnection,master);
                var business = committed.preparation().businessScope(); cache.prepareFirstAuth(committed.firstCreation().orElseThrow()); cache.prepareInitialBusiness(committed.preparation(),ACTIVE);
                configuration.artifacts().create(source("input","x",null)); configuration.artifacts().create(pipeline("input",false,ReadMode.CDC_ONLY,"latest",null));
                var scope = new PipelineIntentScope(business.clusterId(),business.bindingVersion(),business.cacheIncarnation(),"flow",1);
                var node = WorkloadClaimFence.from(cache.workloadClaims(business).acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,OWNER.nodeId()),OWNER,1,TTL).claim());
                scenario.run(new Fixture(configuration,cache,configDb,cacheDb,cacheConnection,scope,node));
            } finally { configDb.drop(); cacheDb.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoConfigurationDomain configuration,MongoCacheDomain cache,MongoDatabase configDatabase,MongoDatabase cacheDatabase,
            StorageBinding.Connection cacheConnection,PipelineIntentScope scope,WorkloadClaimFence node) {
        CacheBusinessScope business() { return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation()); }
        MongoConnectionSettings settings() { return new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configDatabase.getName()),null,Duration.ofSeconds(5)); }
        SourceResource source() { var pipeline = pipeline(); return (SourceResource)configuration.artifacts().get(pipeline.sourceIds().getFirst()).orElseThrow(); }
        PipelineResource pipeline() { return (PipelineResource)configuration.artifacts().get("flow").orElseThrow(); }
        PipelinePreparationInputs inputs() {
            var pipeline = pipeline(); var source = source(); var sources = Map.of(source.id(),source);
            var frozen = SourceModelSnapshot.capture("flow",sources.values(),cache.schemas(business(),ACTIVE));
            return frozen.preparationInputs(scope,pipeline,pipeline,sources);
        }
        WorkloadClaimFence capture(WorkloadClaimFence original,PipelinePreparationInputs inputs) {
            var issued = ((CaptureCreationResult.Issued)configuration.capturePreparations().issueFirstCreate(
                    new CapturePreparationRequest(scope,original,node,"input",inputs))).creation();
            var first = cache.capturePreparations(business(),ACTIVE).createFirst(issued);
            return WorkloadClaimFence.from(cache.workloadClaims(business()).acquire(first.claim().key(),OWNER,1,TTL).claim());
        }
        PipelineFirstStatePreparer preparer(MongoConfigurationDomain configuration,MongoCacheDomain cache) {
            return new PipelineFirstStatePreparer(business(),configuration.preparations(),cache.pipelinePreparations(business(),ACTIVE),() -> Optional.of(node),ACTIVE,CLOCK,TTL);
        }
        long version() { return configDatabase.getCollection("configuration_version").find().first().getLong("revision"); }
        Document claim() { return cacheDatabase.getCollection("workload_claims").find(new Document("_id.resourceType","PIPELINE_ACTUATION")).first(); }
        Map<String,List<Document>> cacheSnapshot() {
            var out = new TreeMap<String,List<Document>>();
            for (String table : List.of("workload_claims","pipeline_state","direct_channel_offsets","direct_batches","operator_cleanup","snapshot_orders","snapshot_counts"))
                out.put(table,cacheDatabase.getCollection(table).find().sort(new Document("_id",1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
