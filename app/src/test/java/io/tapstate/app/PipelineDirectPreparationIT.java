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

/** Native preparation and producer/writer recovery over separate replica sets; no connector, Jet or target is exercised. */
@RequiresDocker
class PipelineDirectPreparationIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(60);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node","boot");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-19T00:00:00Z"),ZoneOffset.UTC);

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
                        runtime,buffer,provisioner,(fixed,config,guard) -> { guard.run();opened.add(config);return new NativeBoundedPort(guard); })) {
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
        var pass=new PipelineConvergencePass(new PipelineIntentSnapshot(f.scope,1,new io.tapstate.core.lifecycle.DesiredState(
                "flow",io.tapstate.core.lifecycle.PipelineState.RUNNING,io.tapstate.core.model.canonical.CanonicalHash.of(f.pipeline()))),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),OWNER,
                        bindings.original().claimGeneration(),bindings.original().executionGeneration(),bindings.original().topologyRevision()),views,ACTIVE);
        var source=new StoreBackedDagSource(mixed,new StoreBackedDagSource.PdkSinkWriterBinder(f.configuration.requiredNotes(),provisioner));
        var prepared=source.prepareStart(pass,"cache");pass.bindCapture(pass.claim(),bindings);
        prepared.plan().build(bindings.original());return inputs.get();
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
                    assertThat(floor.of("input.orders")).contains(SourceOrder.snapshotRow(1));
                    slow.advance("input.orders",rows.getFirst().position(),original);
                    assertThat(channels.load(context).cursor().sinkAckedSrcpos()).isEqualTo("anchor");
                    slow.advance("input.quiet",rows.get(1).position(),original);
                    assertThat(channels.pending(context,1)).singleElement().satisfies(batch -> assertThat(batch.boundary().token()).isEqualTo("last"));
                    assertThat(channels.load(context).cursor().sinkAckedSrcpos()).isEqualTo("first");
                    assertThat(floor.of("input.orders")).contains(new SourceOrder(1,2));
                    assertThat(floor.of("input.quiet")).contains(new SourceOrder(1,2));
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
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action,IoError expected) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,failure -> { assertThat(failure.code()).isEqualTo(expected); assertThat(failure.getCause()).isNull(); });
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
