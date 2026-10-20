package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Concrete app orchestration over isolated CONFIG/CACHE replica sets. */
@RequiresDocker
class PipelineFirstStatePreparerIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-18T08:00:00Z"), ZoneOffset.UTC);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");

    @Test void theAppPreparerConsumesConcretePrivateGrantAndReceiptAcrossTwoInstances() {
        withFixture(f -> {
            long revision = f.version();
            var complete = f.preparer(f.configuration, f.cache).prepare(f.scope);
            assertThat(complete.phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.configuration.intents().loadForActuation("flow")).isPresent();
            assertThat(f.cache.pipelineStates(f.scope).read("flow").orElseThrow().stateJson()).isEqualTo("NEW");
            assertThat(f.state()).containsEntry("epoch", 0L).containsEntry("runtimeWriteFence", 1L);
            assertThat(f.claim()).containsEntry("claimGeneration", 1L).containsEntry("executionGeneration", 0L);
            assertThat(f.version()).isEqualTo(revision);
            assertThat(f.configDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_state", "workload_claims");
            assertThat(f.cacheDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("operator_generations", "pipeline_intents");
            // COMPLETE is historical preparation status, never proof that lost CACHE state is healthy.
            f.cacheDatabase.getCollection("pipeline_state").deleteMany(new Document());
            f.cacheDatabase.getCollection("workload_claims").deleteOne(f.pipelineFilter());
            var missing = f.snapshot();
            assertThat(f.preparer(f.configuration, f.cache).prepare(f.scope)).isEqualTo(complete);
            assertCode(() -> f.cache.workloadClaims(f.business()).acquirePipeline(f.scope, OWNER, 1, TTL), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(missing);
        });
    }

    @Test void reopeningClientsCannotReissueAnAuthorityWhoseCacheHistoryIsAbsent() {
        withFixture(f -> {
            var issued = f.configuration.preparations().issueFirstCreate(f.scope, f.node);
            assertThat(issued).isInstanceOf(FirstPipelineCreationResult.Issued.class);
            var before = f.snapshot(); var mapping = f.configDatabase.getCollection("operator_generations").find().first();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a");
                 var cache = MongoCacheDomain.connect(f.cacheConnection, Duration.ofSeconds(5), "reopened-test")) {
                assertCode(() -> f.preparer(reopened, cache).prepare(f.scope), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(reopened.preparations().issueFirstCreate(f.scope, f.node)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
            }
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.configDatabase.getCollection("operator_generations").find().first()).isEqualTo(mapping);
        });
    }

    @Test void reopeningAfterSeedCompletesFromTheConcreteReceiptWithoutAnyCacheMutation() {
        withFixture(f -> {
            var grant = ((FirstPipelineCreationResult.Issued) f.configuration.preparations().issueFirstCreate(f.scope, f.node)).creation();
            var preparer = f.cache.pipelinePreparations(f.business(), ACTIVE);
            preparer.seed(preparer.reserveFirst(grant, TTL), f.configuration.preparations(), CLOCK.instant());
            var before = f.snapshot();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a");
                 var cache = MongoCacheDomain.connect(f.cacheConnection, Duration.ofSeconds(5), "reopened-test")) {
                assertThat(f.preparer(reopened, cache).prepare(f.scope).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
                assertThat(reopened.intents().loadForActuation("flow")).isPresent();
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aScopedRunCannotWriteWithTheTopologyOfALaterSameOwnerAcquisition() {
        withFixture(f -> {
            f.preparer(f.configuration,f.cache).prepare(f.scope);
            var claims = f.cache.workloadClaims(f.business());
            var owned = claims.acquirePipeline(f.scope,OWNER,1,TTL).claim();
            var run = claims.advanceExecution(owned,1,java.util.Set.of(OWNER.nodeId())).orElseThrow();
            var original = new ExecutionFence(f.scope,WorkloadClaimFence.from(run));
            var later = claims.acquirePipeline(f.scope,OWNER,2,TTL).claim();
            assertThat(later.claimGeneration()).isEqualTo(run.claimGeneration());
            assertThat(later.executionGeneration()).isEqualTo(run.executionGeneration());
            assertThat(later.topologyRevision()).isEqualTo(2);
            var before = f.state();
            try (var guard = ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(10),System::nanoTime,() -> true)) {
                assertThatThrownBy(() -> {
                    var borrowed = guard.require(original);
                    f.cache.pipelineStates(f.scope,borrowed,ACTIVE).compareAndSwap("flow",0,"RUNNING",CLOCK.instant());
                }).isInstanceOfSatisfying(TapstateException.class,failure ->
                        assertThat(failure.code()).isEqualTo(io.tapstate.runtime.engine.EngineError.EXECUTION_NOT_AUTHORIZED));
                assertThat(f.state()).isEqualTo(before);
                assertCode(() -> f.cache.pipelineStates(f.scope,WorkloadClaimFence.from(run),ACTIVE)
                        .compareAndSwap("flow",0,"RUNNING",CLOCK.instant()),IoError.WORKLOAD_CLAIM_FENCED);
                guard.recordSinkWriteFailure(original);
                assertThat(claims.readPipeline(f.scope).orElseThrow().claim().failureClaimGeneration()).isZero();
                var actual = guard.require(new ExecutionFence(f.scope,WorkloadClaimFence.from(later)));
                f.cache.pipelineStates(f.scope,actual,ACTIVE).compareAndSwap("flow",0,"RUNNING",CLOCK.instant());
                assertThat(f.state().getLong("epoch")).isEqualTo(1);
            }
        });
    }

    @Test void actualMapStoreFactoriesKeepTheirOriginalClaimsOverTheControlledCacheAssembly() {
        withFixture(f -> {
            f.preparer(f.configuration,f.cache).prepare(f.scope);
            var claims = f.cache.workloadClaims(f.business()); var owned = claims.acquirePipeline(f.scope,OWNER,1,TTL).claim();
            var first = claims.advanceExecution(owned,1,java.util.Set.of(OWNER.nodeId())).orElseThrow();
            var topology = literalRootTopology();
            var placement = io.tapstate.runtime.engine.OperatorMapCoordinates.nest(f.scope,f.cacheDatabase.getName(),"step",List.of(),
                    io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.STATE,null);
            var embeddedPlacement = io.tapstate.runtime.engine.OperatorMapCoordinates.nest(f.scope,f.cacheDatabase.getName(),"step",List.of("$root"),
                    io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.STATE,null);
            assertThat(placement.mapName()).isNotEqualTo(embeddedPlacement.mapName());
            f.cache.nestShapes(f.scope,f.cacheDatabase.getName(),WorkloadClaimFence.from(first),ACTIVE).recordIfAbsent("step",topology.stateIdentityPaths());
            var member = org.mockito.Mockito.mock(com.hazelcast.core.HazelcastInstance.class);
            org.mockito.Mockito.when(member.getUserContext()).thenReturn(new java.util.concurrent.ConcurrentHashMap<>());
            org.mockito.Mockito.when(member.getDistributedObjects()).thenReturn(List.of());
            try (var authorization = ExecutionAuthorization.scoped(f.business(),claims,Duration.ofSeconds(10),System::nanoTime,() -> true);
                 var registry = io.tapstate.runtime.engine.OperatorMapBindings.bindTo(member,new ScopedOperatorStateProvider(f.cache,authorization))) {
                var originalFence = new ExecutionFence(f.scope,WorkloadClaimFence.from(first));
                var original = new io.tapstate.runtime.engine.OperatorMapBinding(placement,originalFence,originalFence.operatorExecutionKey());
                var embeddedOriginal = new io.tapstate.runtime.engine.OperatorMapBinding(embeddedPlacement,originalFence,originalFence.operatorExecutionKey());
                registry.install(List.of(original,embeddedOriginal)); var old = mapStore(member,original); var embeddedOld = mapStore(member,embeddedOriginal);
                Object key = List.of("key"); old.store(key,"before"); embeddedOld.store(key,"embedded-before");
                var nextClaim = claims.advanceExecution(first,1,java.util.Set.of(OWNER.nodeId())).orElseThrow();
                var nextFence = new ExecutionFence(f.scope,WorkloadClaimFence.from(nextClaim));
                var next = new io.tapstate.runtime.engine.OperatorMapBinding(placement,nextFence,nextFence.operatorExecutionKey());
                var embeddedNext = new io.tapstate.runtime.engine.OperatorMapBinding(embeddedPlacement,nextFence,nextFence.operatorExecutionKey());
                registry.install(List.of(next,embeddedNext)); var current = mapStore(member,next); var embeddedCurrent = mapStore(member,embeddedNext);
                assertThat(current.load(key)).isEqualTo("before"); assertThat(embeddedCurrent.load(key)).isEqualTo("embedded-before");
                current.store(key,"after"); embeddedCurrent.store(key,"embedded-after"); var before = f.state();
                assertCode(() -> old.store(key,"late"),IoError.WORKLOAD_CLAIM_FENCED);
                assertCode(() -> old.storeAll(Map.of(key,"late")),IoError.WORKLOAD_CLAIM_FENCED);
                assertCode(() -> old.delete(key),IoError.WORKLOAD_CLAIM_FENCED);
                assertCode(() -> old.deleteAll(List.of(key)),IoError.WORKLOAD_CLAIM_FENCED);
                assertCode(() -> embeddedOld.deleteAll(List.of(key)),IoError.WORKLOAD_CLAIM_FENCED);
                assertCode(() -> f.cache.operatorState(placement,WorkloadClaimFence.from(first),ACTIVE)
                        .save(placement.mapName(),"late",new byte[]{1}),IoError.WORKLOAD_CLAIM_FENCED);
                assertThat(f.state()).isEqualTo(before); assertThat(current.load(key)).isEqualTo("after");
                assertThat(registry.release(original)).isFalse(); assertThat(current.load(key)).isEqualTo("after");
                assertThat(registry.release(embeddedOriginal)).isFalse(); assertThat(embeddedCurrent.load(key)).isEqualTo("embedded-after");
                var encoded = new java.io.ByteArrayOutputStream();
                try (var output = new java.io.ObjectOutputStream(encoded)) { output.writeObject(key); }
                var rows = f.cacheDatabase.getCollection("nest_state");
                rows.updateMany(new Document(),new Document("$set",new Document("state",new org.bson.types.Binary(new byte[]{1}))
                        .append("pendingRelease",true).append("releaseKey",new org.bson.types.Binary(encoded.toByteArray()))));
                var damaged = rows.find().into(new ArrayList<>());
                assertCode(() -> current.load(key),IoError.DOCUMENT_UNREADABLE);
                assertCode(() -> current.loadAll(List.of(key)),IoError.DOCUMENT_UNREADABLE);
                assertThat(rows.find().into(new ArrayList<>())).isEqualTo(damaged);
            }
        });
    }

    @Test void aDeletedParentAndItsOriginalChildReloadThroughTheScopedMongoMapStore() {
        withFixture(f -> {
            f.preparer(f.configuration, f.cache).prepare(f.scope);
            var claims = f.cache.workloadClaims(f.business());
            var owned = claims.acquirePipeline(f.scope, OWNER, 1, TTL).claim();
            var run = claims.advanceExecution(owned, 1, java.util.Set.of(OWNER.nodeId())).orElseThrow();
            var topology = literalRootTopology();
            f.cache.nestShapes(f.scope, f.cacheDatabase.getName(), WorkloadClaimFence.from(run), ACTIVE)
                    .recordIfAbsent("step", topology.stateIdentityPaths());
            var placement = io.tapstate.runtime.engine.OperatorMapCoordinates.nest(f.scope, f.cacheDatabase.getName(), "step",
                    List.of("$root"), io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.STATE, null);
            var fence = new ExecutionFence(f.scope, WorkloadClaimFence.from(run));
            var binding = new io.tapstate.runtime.engine.OperatorMapBinding(placement, fence, fence.operatorExecutionKey());
            var config = new com.hazelcast.config.Config();
            config.setProperty("hazelcast.phone.home.enabled", "false");
            config.setProperty("hazelcast.shutdownhook.enabled", "false");
            config.setProperty("hazelcast.operation.thread.count", "2");
            config.setProperty("hazelcast.operation.generic.thread.count", "2");
            var join = config.getNetworkConfig().getJoin();
            join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false);
            join.getAutoDetectionConfig().setEnabled(false);
            config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
            config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
            var mapConfig = io.tapstate.runtime.engine.nest.NestSettings.defaults().backedStateMaps(binding.mapName());
            mapConfig.getMapStoreConfig().getProperties().setProperty(io.tapstate.runtime.engine.OperatorMapBindings.SCOPED_PROPERTY, "true");
            config.addMapConfig(mapConfig);
            var member = com.hazelcast.core.Hazelcast.newHazelcastInstance(config);
            try (var authorization = ExecutionAuthorization.scoped(f.business(), claims, Duration.ofSeconds(10), System::nanoTime, () -> true);
                 var registry = io.tapstate.runtime.engine.OperatorMapBindings.bindTo(member, new ScopedOperatorStateProvider(f.cache, authorization))) {
                registry.install(List.of(binding));
                var map = member.<Object, io.tapstate.runtime.engine.nest.ResolverState>getMap(binding.mapName());
                var state = new io.tapstate.runtime.engine.nest.ResolverState();
                state.deleteMapping(new io.tapstate.core.event.SourceOrder(1, 1));
                var child = new io.tapstate.runtime.engine.nest.NestElement(
                        new io.tapstate.runtime.engine.nest.ElementRef(List.of("$root", "child"), List.of("P1"), List.of("K1"), null),
                        Map.of("claim_id", "K1", "value", 1, "payload", new byte[]{1, 2}), new io.tapstate.core.event.SourceOrder(1, 3),
                        Map.of("claims", new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(1, 3), "original-opaque-token")));
                state.resolve(child, 1); map.set(List.of("P1"), state);
                assertThat(f.cacheDatabase.getCollection("nest_state").countDocuments()).isEqualTo(1);
                assertThat(f.configDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("nest_state");
                assertThat(map.evict(List.of("P1"))).isTrue();
                assertThat(map.getLocalMapStats().getOwnedEntryCount()).isZero();
                var cold = map.get(List.of("P1"));
                assertThat(cold.deleted()).isTrue(); assertThat(cold.pending()).isEqualTo(1);
                map.destroy();
                var reopened = member.<Object, io.tapstate.runtime.engine.nest.ResolverState>getMap(binding.mapName());
                var restored = reopened.get(List.of("P1"));
                restored.resolve(child, 2);
                assertThat(restored.pending()).isEqualTo(1);
                var released = restored.declare(List.of("C1"), new io.tapstate.core.event.SourceOrder(1, 2));
                assertThat(released).singleElement().satisfies(value -> {
                    assertThat(value.fields()).containsEntry("value", 1);
                    assertThat((byte[]) value.fields().get("payload")).containsExactly((byte) 1, (byte) 2);
                    assertThat(value.positions().get("claims").token()).isEqualTo("original-opaque-token");
                    assertThat(value.order()).isEqualTo(new io.tapstate.core.event.SourceOrder(1, 3));
                });
            } finally { member.shutdown(); }
        });
    }

    @Test void theActualGraphFindsColdHandoffsWithoutAnyNewSourceRows() {
        withFixture(f -> {
            f.preparer(f.configuration, f.cache).prepare(f.scope);
            var claims = f.cache.workloadClaims(f.business());
            var owned = claims.acquirePipeline(f.scope, OWNER, 1, TTL).claim();
            var run = claims.advanceExecution(owned, 1, java.util.Set.of(OWNER.nodeId())).orElseThrow();
            var fence = new ExecutionFence(f.scope, WorkloadClaimFence.from(run));
            var topology = literalRootTopology();
            f.cache.nestShapes(f.scope, f.cacheDatabase.getName(), WorkloadClaimFence.from(run), ACTIVE)
                    .recordIfAbsent("step", topology.stateIdentityPaths());
            var bindings = new ArrayList<io.tapstate.runtime.engine.OperatorMapBinding>();
            for (var vertex : topology.vertices()) for (var kind : List.of(
                    io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.STATE,
                    io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.PARKING)) {
                var at = io.tapstate.runtime.engine.OperatorMapCoordinates.nest(f.scope, f.cacheDatabase.getName(), "step", vertex.pathId(), kind, null);
                bindings.add(new io.tapstate.runtime.engine.OperatorMapBinding(at, fence, fence.operatorExecutionKey()));
            }
            var manifest = new io.tapstate.runtime.engine.OperatorRunManifest("flow", fence, bindings);
            var config = isolatedNestMember();
            for (var binding : bindings) {
                var mapConfig = io.tapstate.runtime.engine.nest.NestSettings.defaults().backedStateMaps(binding.mapName());
                mapConfig.getMapStoreConfig().getProperties().setProperty(io.tapstate.runtime.engine.OperatorMapBindings.SCOPED_PROPERTY,"true");
                config.addMapConfig(mapConfig);
            }
            var member = com.hazelcast.core.Hazelcast.newHazelcastInstance(config);
            try (var authorization = ExecutionAuthorization.scoped(f.business(), claims, Duration.ofSeconds(10), System::nanoTime, () -> true);
                 var registry = io.tapstate.runtime.engine.OperatorMapBindings.bindTo(member,new ScopedOperatorStateProvider(f.cache,authorization))) {
                registry.install(bindings);
                var names = manifest.forStep("flow","step");
                var rootName = names.nest(List.of(),io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.STATE,null).mapName();
                var resolverName = names.nest(List.of("$root"),io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap.STATE,null).mapName();
                var roots = member.<Object,io.tapstate.runtime.engine.nest.RootAssembly>getMap(rootName);
                var states = member.<Object,io.tapstate.runtime.engine.nest.ResolverState>getMap(resolverName);
                var spec = topology.vertexAt(List.of("$root"));
                var axes = io.tapstate.runtime.engine.ChainAxes.assign(List.of("policy","claim"));
                for (int key = 1; key <= 2; key++) {
                    String parentId = "P"+key, customer = "C"+key;
                    var parent = Map.<String,Object>of("policy_id",parentId,"customer_id",customer,"policy_no","PN"+key);
                    var assembly = new io.tapstate.runtime.engine.nest.RootAssembly();
                    assembly.applyRoot(Map.of("customer_id",customer),new io.tapstate.core.event.SourceOrder(1,io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ));
                    assembly.applyElement(new io.tapstate.runtime.engine.nest.ElementRef(List.of("$root"),null,List.of("PN"+key),List.of(parentId)),
                            parent,new io.tapstate.core.event.SourceOrder(1,2),Map.of());
                    roots.set(List.of(customer),assembly);
                    io.tapstate.runtime.engine.nest.NestStore<io.tapstate.runtime.engine.nest.ResolverState> state = new io.tapstate.runtime.engine.nest.NestStore<>() {
                        @Override public io.tapstate.runtime.engine.nest.ResolverState load(Object at) { return states.get(at); }
                        @Override public void save(Object at,io.tapstate.runtime.engine.nest.ResolverState value) { states.set(at,value); }
                        @Override public void remove(Object at) { states.delete(at); }
                        @Override public long count() { return states.size(); }
                    };
                    var receiver = new io.tapstate.runtime.engine.nest.ResolverProcessor(spec,state,(from,child) -> {},axes,
                            Map.of(0,List.of("policy"),1,List.of("claim")));
                    var remaining = new java.util.concurrent.atomic.AtomicInteger(1);
                    var out = org.mockito.Mockito.mock(com.hazelcast.jet.core.Outbox.class, invocation -> {
                        if (invocation.getMethod().getName().equals("bucketCount")) return 1;
                        if (invocation.getMethod().getName().equals("offer")) {
                            Object value = invocation.getArguments()[invocation.getArguments().length-1];
                            return value instanceof com.hazelcast.jet.core.Watermark || remaining.getAndDecrement() > 0;
                        }
                        return org.mockito.Mockito.RETURNS_DEFAULTS.answer(invocation);
                    });
                    receiver.init(out,org.mockito.Mockito.mock(com.hazelcast.jet.core.Processor.Context.class));
                    feedNest(receiver,0,io.tapstate.core.event.Envelope.delete(1,"policy",parent,null).withOrder(new io.tapstate.core.event.SourceOrder(1,1)));
                    receiver.tryProcessWatermark(0,new com.hazelcast.jet.core.Watermark(
                            io.tapstate.runtime.engine.FrontierOrders.pack("policy",new io.tapstate.core.event.SourceOrder(1,50)),axes.axisOf("policy")));
                    feedNest(receiver,1,io.tapstate.core.event.Envelope.insert(3,"claim",Map.of("claim_id","K"+key,"policy_id",parentId,"value",1),null)
                            .withOrder(new io.tapstate.core.event.SourceOrder(1,3)).withSrcPos("original-child-"+key));
                    remaining.set(1);
                    feedNest(receiver,0,io.tapstate.core.event.Envelope.insert(2,"policy",parent,null).withOrder(new io.tapstate.core.event.SourceOrder(1,2)));
                    assertThat(states.get(List.of(parentId)).pending()).isEqualTo(1);
                }
                assertThat(f.cacheDatabase.getCollection("nest_state").countDocuments(new Document("pendingRelease",true))).isEqualTo(2);
                roots.destroy(); states.destroy();
                var stores = io.tapstate.runtime.engine.nest.NestBinding.onMap(manifest).forStep("flow","step");
                var binding = new io.tapstate.runtime.engine.nest.NestBinding(alias -> null,stores,(from,child) -> {},
                        ignored -> chain -> Optional.of(new io.tapstate.core.event.SourceOrder(1,50)));
                var dag = new com.hazelcast.jet.core.DAG();
                var inputs = new java.util.HashMap<String,com.hazelcast.jet.core.Vertex>();
                for (String alias : List.of("customer","policy","claim")) inputs.put(alias,dag.newVertex(alias+"-empty",
                        com.hazelcast.jet.core.ProcessorMetaSupplier.of(com.hazelcast.jet.core.ProcessorSupplier.of(EmptyNestInput::new))).localParallelism(1));
                var ordinal = new java.util.HashMap<com.hazelcast.jet.core.Vertex,Integer>();
                var assembled = io.tapstate.runtime.engine.nest.NestDag.attach(dag,topology,"step","customer","out",
                        alias -> List.of(inputs.get(alias)),binding,vertex -> ordinal.merge(vertex,1,Integer::sum)-1,null,
                        new io.tapstate.runtime.engine.NodeWidth("step",2,1,null));
                var sink = dag.newVertex("collected",com.hazelcast.jet.core.processor.SinkProcessors.writeListP("recovered-documents"));
                dag.edge(com.hazelcast.jet.core.Edge.between(assembled,sink));
                member.getJet().newJob(dag).getFuture().toCompletableFuture().get(20,java.util.concurrent.TimeUnit.SECONDS);
                for (int key = 1; key <= 2; key++) {
                    var restored = member.<Object,io.tapstate.runtime.engine.nest.RootAssembly>getMap(rootName).get(List.of("C"+key));
                    var document = restored.render(topology.slots()).orElseThrow();
                    var parents = (List<?>) document.get("$root");
                    assertThat(parents).hasSize(1);
                    var children = (List<?>) ((Map<?,?>)parents.getFirst()).get("child");
                    assertThat(children).hasSize(1);
                    assertThat(((Map<?,?>)children.getFirst()).get("value")).isEqualTo(1);
                }
                assertThat(member.getList("recovered-documents")).hasSize(2);
            } finally { member.shutdown(); }
        });
    }

    private static com.hazelcast.config.Config isolatedNestMember() {
        var config = new com.hazelcast.config.Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getSerializationConfig().addSerializerConfig(new com.hazelcast.config.SerializerConfig()
                .setTypeClass(io.tapstate.core.event.Envelope.class).setImplementation(new io.tapstate.runtime.engine.EnvelopeSerializer()));
        config.setProperty("hazelcast.phone.home.enabled","false"); config.setProperty("hazelcast.shutdownhook.enabled","false");
        config.setProperty("hazelcast.operation.thread.count","2"); config.setProperty("hazelcast.operation.generic.thread.count","2");
        var join = config.getNetworkConfig().getJoin(); join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        return config;
    }
    private static void feedNest(io.tapstate.runtime.engine.nest.ResolverProcessor receiver,int ordinal,Object value) {
        var queue = new java.util.ArrayDeque<>(); queue.add(value);
        var inbox = org.mockito.Mockito.mock(com.hazelcast.jet.core.Inbox.class);
        org.mockito.Mockito.when(inbox.peek()).thenAnswer(ignored -> queue.peek());
        org.mockito.Mockito.doAnswer(ignored -> { queue.remove(); return null; }).when(inbox).remove();
        receiver.process(ordinal,inbox);
    }
    private static final class EmptyNestInput extends com.hazelcast.jet.core.AbstractProcessor {}

    private static io.tapstate.runtime.engine.nest.NestTopology literalRootTopology() {
        var child = new io.tapstate.core.model.Embed("claim",Map.of("policy_id","policy_id"),io.tapstate.core.model.EmbedAs.ARRAY,"child",List.of("claim_id"),null,null,null);
        var parent = new io.tapstate.core.model.Embed("policy",Map.of("customer_id","customer_id"),io.tapstate.core.model.EmbedAs.ARRAY,"$root",List.of("policy_no"),null,null,List.of(child));
        var tree = new io.tapstate.core.model.TransformBody.Nest(null,null,new io.tapstate.core.model.NestRoot("customer",List.of("customer_id"),null,null,List.of(parent)));
        var tables = Map.of("customer",new io.tapstate.runtime.engine.nest.NestTable("customers",List.of("customer_id")),
                "policy",new io.tapstate.runtime.engine.nest.NestTable("policies",List.of("policy_id")),"claim",new io.tapstate.runtime.engine.nest.NestTable("claims",List.of("claim_id")));
        return io.tapstate.runtime.engine.nest.NestTopology.compile("flow","step",tree,tables::get);
    }

    @SuppressWarnings("unchecked") private static com.hazelcast.map.MapStore<Object,Object> mapStore(com.hazelcast.core.HazelcastInstance member,
            io.tapstate.runtime.engine.OperatorMapBinding binding) {
        var properties = new java.util.Properties(); properties.setProperty(io.tapstate.runtime.engine.OperatorMapBindings.SCOPED_PROPERTY,"true");
        var loader = new io.tapstate.runtime.engine.nest.NestStateMapStoreFactory().newMapStore(binding.mapName(),properties);
        ((com.hazelcast.map.MapLoaderLifecycleSupport)loader).init(member,properties,binding.mapName());
        return (com.hazelcast.map.MapStore<Object,Object>)loader;
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(code));
    }
    private static void withFixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configName = "app_first_cfg_" + suffix; String cacheName = "app_first_cache_" + suffix;
        try (var configClient = MongoClients.create(CONFIG.getReplicaSetUrl()); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var configDb = configClient.getDatabase(configName); var cacheDb = cacheClient.getDatabase(cacheName);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheName), cacheName, null);
            try (var configuration = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1);
                 var cache = MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "app-preparation-test")) {
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                var committed = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cacheConnection, master);
                var business = committed.preparation().businessScope();
                cache.prepareFirstAuth(committed.firstCreation().orElseThrow());
                cache.prepareInitialBusiness(committed.preparation(),ACTIVE);
                var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");
                configuration.artifacts().create(pipeline);
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), "flow", 1);
                configuration.intents().compareAndSet(scope, 0, new DesiredState("flow", PipelineState.RUNNING, CanonicalHash.of(pipeline)));
                var node = cache.workloadClaims(business).acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a"), OWNER, 1, TTL).claim();
                scenario.run(new Fixture(configuration, cache, configDb, cacheDb, settings, cacheConnection, scope, WorkloadClaimFence.from(node)));
            } finally { configDb.drop(); cacheDb.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoConfigurationDomain configuration, MongoCacheDomain cache, MongoDatabase configDatabase,
            MongoDatabase cacheDatabase, MongoConnectionSettings settings, StorageBinding.Connection cacheConnection,
            PipelineIntentScope scope, WorkloadClaimFence node) {
        CacheBusinessScope business() { return new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()); }
        PipelineFirstStatePreparer preparer(MongoConfigurationDomain configuration, MongoCacheDomain cache) {
            return new PipelineFirstStatePreparer(business(), configuration.preparations(), cache.pipelinePreparations(business(), ACTIVE),
                    () -> Optional.of(node), ACTIVE, CLOCK, TTL);
        }
        long version() { return configDatabase.getCollection("configuration_version").find().first().getLong("revision"); }
        Document pipelineFilter() { return new Document("_id", new Document("clusterId", scope.clusterId()).append("resourceType", "PIPELINE_ACTUATION").append("resourceId", scope.pipelineId())); }
        Document claim() { return cacheDatabase.getCollection("workload_claims").find(pipelineFilter()).first(); }
        Document state() { return cacheDatabase.getCollection("pipeline_state").find().first(); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (String table : List.of("workload_claims", "pipeline_state", "cache_identity")) out.put(table, cacheDatabase.getCollection(table).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
