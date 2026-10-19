package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StoreBackedDagSourceScopedModelTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "p", 7);
    private static final WorkloadClaimFence ORIGINAL = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "p"), new WorkloadOwner("node-a", "boot-a"), 3, 0, 1);

    @Test void actualSourceJoinStepRecordsAndPinsUseOnlyTheAdvancedOriginalPass() {
        var f = fixture(true); var pass = f.pass(() -> {});
        f.store.derivedSchemas().record("p", "widen", Map.of("legacy_only", "INT"), "legacy", "legacy", "legacy");
        var prepared = f.source.prepareStart(pass, "default");
        assertThat(f.views.writes).isEmpty();
        WorkloadClaimFence next = advance(1); f.views.current.set(next); pass.advanced(next);
        bindCapturedSources(prepared,pass); prepared.plan().build(new ExecutionFence(SCOPE,advance(1)));
        assertThat(f.views.writes).isNotEmpty().containsOnly(next);
        assertThat(f.views.scopes).containsOnly(SCOPE);
        assertThat(f.views.models.latest("p", "src.orders")).isPresent();
        assertThat(f.views.models.pinned("p", "widen")).isPresent();
        assertThat(f.views.models.pinned("p", "after_join")).isPresent();
        assertThat(f.views.models.latest("p", "widen").orElseThrow().schema()).containsOnlyKeys("order_id", "customer_name");
        assertThat(f.store.derivedSchemas().latest("p", "widen").orElseThrow().schema()).containsOnlyKeys("legacy_only");
        assertThat(f.store.derivedSchemas().latest("p", "src.orders")).isEmpty();
        assertThat(f.store.derivedSchemas().pinned("p", "widen")).isEmpty();
    }

    @Test void startRetainsTheDefinitionCapturedBeforeAnotherApply() {
        var f = fixture(false); var pass = f.pass(() -> {});
        var prepared = f.source.prepareStart(pass, "default");
        f.store.artifacts().save(pipeline(false, "newer_step"));
        f.views.current.set(advance(1)); pass.advanced(advance(1));
        prepared.plan();
        assertThat(f.views.models.pinned("p", "original_step")).isPresent();
        assertThat(f.views.models.latest("p", "newer_step")).isEmpty();
        assertThat(prepared.artifactSnapshot().orElseThrow().get("p")).contains(f.pipeline);
    }

    @Test void discoveryChosenAtPreparationCannotChangeWhileTheOriginalExecutionIsBeingAllocated() {
        var f = fixture(false); var pass = f.pass(() -> {});
        var prepared = f.source.prepareStart(pass,"default");
        f.store.schemas().save(new DiscoveredSourceModel("src","mysql",1,new SourceModel(List.of(new SourceTable("orders",
                List.of(new SourceField("replacement","INT")),List.of("replacement"),List.of())))));
        var advanced = advance(1); f.views.current.set(advanced); pass.advanced(advanced);
        bindCapturedSources(prepared,pass); prepared.plan().build(new ExecutionFence(SCOPE,advance(1)));
        assertThat(f.views.models.latest("p","src.orders").orElseThrow().schema()).containsOnlyKeys("id","customer_ref");
        assertThat(f.store.schemas().get("src").orElseThrow().model().tables().getFirst().primaryKey()).containsExactly("replacement");
    }

    @Test void aPassWhoseRequestedDefinitionIsAlreadyReplacedNeverRecordsNewerModels() {
        var f = fixture(false); var pass = f.pass(() -> {});
        f.store.artifacts().save(pipeline(false, "newer_step"));
        assertThatThrownBy(() -> f.source.prepareStart(pass, "default")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
        assertThat(f.views.writes).isEmpty(); assertThat(f.views.scopes).isEmpty();
        assertThat(f.store.derivedSchemas().latest("p", "newer_step")).isEmpty();
    }

    @Test void anExpiredPreparationCannotAdoptAnotherPassClaimWhenPlanningFinallyRuns() {
        var f = fixture(false); var pass = f.pass(() -> {});
        var prepared = f.source.prepareStart(pass, "default");
        f.views.current.set(advance(2));
        assertThatThrownBy(prepared::plan).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(f.views.writes).containsExactly(ORIGINAL);
        assertThat(f.views.models.latest("p", "src.orders")).isEmpty();
    }

    @Test void theOriginalLifecycleGuardAlsoProtectsDeferredModelWrites() {
        var f = fixture(false); var active = new AtomicBoolean(true);
        var pass = f.pass(() -> { if (!active.get()) throw fenced(); });
        var prepared = f.source.prepareStart(pass, "default"); active.set(false);
        assertThatThrownBy(prepared::plan).isInstanceOf(TapstateException.class);
        assertThat(f.views.writes).isEmpty();
    }

    @Test void anUnscopedDagStandInNeverReceivesAFallbackFromTheScopedEntry() {
        var f = fixture(false);
        DagSource unknown = new DagSource() {
            @Override public com.hazelcast.jet.core.DAG dagFor(String id) { throw new AssertionError("bare Pipeline id fallback"); }
            @Override public NestCapacity capacityOf(String id) { throw new AssertionError("bare capacity fallback"); }
            @Override public List<io.tapstate.core.lifecycle.PipelineStateHolding> stateHeldBy(String id) { throw new AssertionError("bare state fallback"); }
        };
        assertThatThrownBy(() -> unknown.prepareStart(f.pass(() -> {}), "default"))
                .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
    }

    @Test void nestedLiteralValuesRemainFrozenAgainstInputAndReturnedSnapshotMutation() {
        var f = fixture(false); var values = new ArrayList<>(List.of("original"));
        var body = new io.tapstate.core.model.TransformBody.MapProjection(Map.of("payload", io.tapstate.core.model.FieldRule.literal(values)));
        var original = new PipelineResource("p", null, f.pipeline.sources(), List.of(io.tapstate.core.model.Step.inline("original_step",
                io.tapstate.core.model.FromClause.list(io.tapstate.core.model.FromRef.literal("src")), body, null)), null, f.pipeline.serve(), null, null);
        f.store.artifacts().save(original); var mutable = new Fixture(f.store, original, f.source, f.views);
        String expected = CanonicalHash.of(original);
        var prepared = f.source.prepareStart(mutable.pass(() -> {}), "default");
        values.add("changed-input");
        PipelineResource exposed = (PipelineResource) prepared.artifactSnapshot().orElseThrow().get("p").orElseThrow();
        var projection = (io.tapstate.core.model.TransformBody.MapProjection) ((io.tapstate.core.model.Step.Inline) exposed.transforms().getFirst()).body();
        Object value = ((io.tapstate.core.model.FieldRule.Literal) projection.fields().get("payload")).value();
        @SuppressWarnings("unchecked") var exposedValues = (List<Object>) value;
        try { exposedValues.add("changed-result"); } catch (UnsupportedOperationException immutable) { /* A detached or immutable result is safe. */ }
        assertThat(CanonicalHash.of(prepared.artifactSnapshot().orElseThrow().get("p").orElseThrow())).isEqualTo(expected);
        assertThat(CanonicalHash.of(prepared.artifactSnapshot().orElseThrow().list().stream().filter(row -> row.id().equals("p")).findFirst().orElseThrow())).isEqualTo(expected);
        prepared.plan();
        assertThat(f.views.models.latest("p", "original_step")).isPresent();
    }

    @Test void nestedConnectionConfigPassedToTheRealBinderIsFrozenAndNeverExposesItsStoredTree() {
        var f = fixture(false); var nested = new java.util.LinkedHashMap<String, Object>(Map.of("label", "original"));
        f.store.artifacts().save(new SourceResource("dest", null, "mongodb", Map.of("uri", "u", "extra", nested), null, null, null, null));
        var bound = new ArrayList<Map<String, Object>>();
        StoreBackedDagSource.SinkWriterBinder binder = (connector, settings, mode, ddl, target, node) -> {
            bound.add(settings); return (SupplierEx<SinkWriter>) () -> null;
        };
        var source = new StoreBackedDagSource(f.store, binder);
        var pass = f.pass(() -> {});
        var prepared = source.prepareStart(pass,"default"); nested.put("label","changed-input");
        f.views.current.set(advance(1)); pass.advanced(advance(1));
        SourceResource exposed = (SourceResource) prepared.artifactSnapshot().orElseThrow().list().stream().filter(row -> row.id().equals("dest")).findFirst().orElseThrow();
        @SuppressWarnings("unchecked") var extra = (Map<String, Object>) exposed.config().get("extra");
        try { extra.put("label", "changed-result"); } catch (UnsupportedOperationException immutable) { /* A detached or immutable result is safe. */ }
        bindCapturedSources(prepared,pass); prepared.plan().build(new ExecutionFence(SCOPE,advance(1)));
        assertThat(bound).isNotEmpty(); assertThat(bound.getLast()).containsEntry("extra", Map.of("label", "original"));
    }

    @Test void actualScopedNestUsesItsCapturedTypedPlanAndOriginalShapeClaimWithoutLegacyLocations() {
        var f = nestFixture(); var pass = f.pass(() -> {}); var prepared = f.source.prepareStart(pass,"cache");
        var plan = prepared.operatorPlan().orElseThrow();
        assertThat(prepared.capacity().mapDatabases()).isEmpty(); assertThat(prepared.stateLocations()).isEmpty();
        assertThat(plan.placements()).hasSize(4).allMatch(p -> p.database().equals("extra-cache"));
        assertThat(plan.nests().get("doc").shape()).containsExactlyInAnyOrder("[]","[\"customer\"]");
        assertThat(f.views.writes).isEmpty(); assertThat(f.views.shapes).isEmpty();
        var advanced = advance(1); f.views.current.set(advanced); pass.advanced(advanced);
        f.store.artifacts().save(pipeline(false,"later"));
        var planned = prepared.plan();
        assertThatThrownBy(() -> planned.build(new ExecutionFence("p",3,1))).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(f.views.shapes).isEmpty();
        bindCapturedSources(prepared,pass); var built = planned.build(new ExecutionFence(SCOPE,advanced));
        var vertices = new ArrayList<String>(); built.dag().forEach(vertex -> vertices.add(vertex.getName()));
        assertThat(vertices).contains("nest:doc");
        assertThat(f.views.shapes).containsEntry("extra-cache/doc",plan.nests().get("doc").shape());
        assertThat(f.views.writes).containsOnly(advanced);
        assertThat(f.views.scopes).containsOnly(SCOPE);
    }

    @Test void anOperatorCandidateCannotBeBuiltBeforeItsOriginalExecutionWasAllocated() {
        var f = nestFixture(); var pass = f.pass(() -> {}); var planned = f.source.prepareStart(pass,"cache").plan();
        assertThatThrownBy(() -> planned.build(new ExecutionFence(SCOPE,advance(1))))
                .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(f.views.shapes).isEmpty();
    }

    @Test void keylessJoinDimensionsStillHaveTheirOwnTypedMirrorAndReverseIndex() {
        var f = fixture(true);
        f.store.schemas().save(new DiscoveredSourceModel("customers_src","mysql",1,new SourceModel(List.of(new SourceTable("customers",
                List.of(new SourceField("id","INT"),new SourceField("name","VARCHAR")),List.of(),List.of())))));
        var prepared = f.source.prepareStart(f.pass(() -> {}),"cache");
        assertThat(prepared.operatorPlan().orElseThrow().placements()).contains(
                io.tapstate.runtime.engine.OperatorMapCoordinates.join(SCOPE,"cache","widen",OperatorPlacement.Resource.JOIN_DIMENSIONS,"c"),
                io.tapstate.runtime.engine.OperatorMapCoordinates.join(SCOPE,"cache","widen",OperatorPlacement.Resource.JOIN_INDEXES,"c"));
        var plan = prepared.operatorPlan().orElseThrow(); var manifest = plan.bind(new ExecutionFence(SCOPE,advance(1)));
        var config = new com.hazelcast.config.Config().setClusterName("keyless-join-"+java.util.UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled","false"); config.setProperty("hazelcast.shutdownhook.enabled","false");
        config.setProperty("hazelcast.logging.type","none"); config.setProperty("hazelcast.operation.thread.count","2");
        config.setProperty("hazelcast.operation.generic.thread.count","2"); config.setProperty("hazelcast.io.input.thread.count","1");
        config.setProperty("hazelcast.io.output.thread.count","1");
        var network = config.getNetworkConfig(); network.setPort(0).setPortAutoIncrement(false);
        network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join = network.getJoin(); join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
        plan.configurations(null).forEach(config::addMapConfig);
        var member = com.hazelcast.core.Hazelcast.newHazelcastInstance(config);
        try (var registry = io.tapstate.runtime.engine.OperatorMapBindings.bindTo(member,(binding,active) -> f.store.keyedState())) {
            registry.install(manifest.bindings());
            var stores = io.tapstate.runtime.engine.join.JoinStoresBinding.onTheCluster(manifest).bind(member,"p","widen");
            stores.putDimensionRow("c","d",Map.of("id",1,"name","customer")); stores.indexAdd("c","d","fact-1");
            assertThat(stores.dimensionRow("c","d")).containsEntry("name","customer");
            assertThat(stores.indexPage("c","d",0)).containsExactly("fact-1");
            for (var binding : manifest.bindings()) member.getMap(binding.mapName()).evictAll();
            assertThat(stores.dimensionRow("c","d")).containsEntry("name","customer");
            assertThat(stores.indexPage("c","d",0)).containsExactly("fact-1");
        } finally { member.shutdown(); }
    }

    @Test void aLateDrawCannotBorrowTheNextExecutionClaimFromItsMutablePassForShapePublication() {
        var f = nestFixture(); var originalPass = new AtomicReference<PipelineConvergencePass>();
        var guardCalls = new java.util.concurrent.atomic.AtomicInteger(); var drawing = new AtomicBoolean();
        var pass = f.pass(() -> {
            if (drawing.get() && guardCalls.incrementAndGet() == 2) {
                f.views.current.set(advance(2)); originalPass.get().advanced(advance(2));
            }
        });
        originalPass.set(pass);
        var prepared = f.source.prepareStart(pass,"cache"); f.views.current.set(advance(1)); pass.advanced(advance(1));
        var planned = prepared.plan(); drawing.set(true);
        assertThatThrownBy(() -> planned.build(new ExecutionFence(SCOPE,advance(1))))
                .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(pass.claim()).isEqualTo(advance(2));
        assertThat(f.views.shapes).isEmpty(); assertThat(f.views.writes).doesNotContain(advance(2));
    }

    @Test void scopedTopologyNeverConsultsLegacyMetaKeyedStateOrOperatorStores() {
        var f = nestFixture();
        var guarded = org.mockito.Mockito.mock(StorePort.class,org.mockito.AdditionalAnswers.delegatesTo(f.store));
        org.mockito.Mockito.doThrow(new AssertionError("legacy SRS meta must not serve a scoped run")).when(guarded).meta();
        org.mockito.Mockito.doThrow(new AssertionError("legacy state must not serve a scoped run")).when(guarded).keyedState();
        org.mockito.Mockito.doThrow(new AssertionError("legacy operator routes must not serve a scoped run")).when(guarded).operatorStateStores();
        org.mockito.Mockito.doThrow(new AssertionError("unscoped derivations must not be opened before a scoped pass")).when(guarded).derivedSchemas();
        var source = new StoreBackedDagSource(guarded,f.source.sinkWriterBinder()); var pass = f.pass(() -> {});
        var prepared = source.prepareStart(pass,"cache"); f.views.current.set(advance(1)); pass.advanced(advance(1));
        bindCapturedSources(prepared,pass); assertThat(prepared.plan().build(new ExecutionFence(SCOPE,advance(1))).dag()).isNotNull();
        org.mockito.Mockito.verify(guarded,org.mockito.Mockito.never()).meta();
        org.mockito.Mockito.verify(guarded,org.mockito.Mockito.never()).keyedState();
        org.mockito.Mockito.verify(guarded,org.mockito.Mockito.never()).operatorStateStores();
        org.mockito.Mockito.verify(guarded,org.mockito.Mockito.never()).derivedSchemas();
    }

    @Test void aScopedSourceMissingItsOriginalCaptureResultCannotBuildFromLegacyProgress() {
        var f = fixture(false); var pass = f.pass(() -> {}); var prepared = f.source.prepareStart(pass,"cache");
        f.views.current.set(advance(1)); pass.advanced(advance(1));
        pass.bindCapture(advance(1),new PipelineCaptureBindings(new ExecutionFence(SCOPE,advance(1)),Map.of(),false));
        assertThatThrownBy(() -> prepared.plan().build(new ExecutionFence(SCOPE,advance(1))))
                .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
    }

    private static void bindCapturedSources(DagSource.StartPreparation prepared,PipelineConvergencePass pass) {
        var artifacts = prepared.artifactSnapshot().orElseThrow();
        var pipeline = (PipelineResource) artifacts.get(pass.scope().pipelineId()).orElseThrow();
        var sources = new java.util.LinkedHashMap<String,PipelineCaptureBindings.LoggedSource>();
        for (String id : pipeline.sourceIds()) {
            var source = (SourceResource) artifacts.get(id).orElseThrow();
            var resolution = SourceCaptureResolution.forPipeline(pipeline,source,prepared.sourceModels().orElseThrow().model(source)).orElseThrow();
            var context = new SrsConsumerContext(new SrsConsumerKey(resolution.chainId().value(),pipeline.id(),id),pass.scope().stateGeneration());
            var membership = new SrsConsumerMembership(context,1,false,java.util.Set.copyOf(resolution.tables()),pass.claim());
            var floors = new java.util.LinkedHashMap<String,Long>(); resolution.tables().forEach(table -> floors.put(table,-1L));
            sources.put(id,new PipelineCaptureBindings.LoggedSource(SrsConsumerRun.of(pass.scope(),membership),1,floors));
        }
        pass.bindCapture(pass.claim(),new PipelineCaptureBindings(new ExecutionFence(pass.scope(),pass.claim()),sources,true));
    }

    private static Fixture nestFixture() {
        var f = fixture(false);
        var pipeline = (PipelineResource) new io.tapstate.core.dsl.DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: [src, customers_src]
                transforms:
                  - id: doc
                    type: nest
                    from: {o: orders, c: customers}
                    state: {database: extra-cache}
                    root:
                      from: o
                      key: [id]
                      embed:
                        - from: c
                          on: {id: customer_ref}
                          as: object
                          path: customer
                serve:
                  from: doc
                  sync: [{id: sync_1, source: dest}]
                """);
        f.store.artifacts().save(pipeline); return new Fixture(f.store,pipeline,f.source,f.views);
    }

    @Test void actualNativeDagFreezesAllTerminalModelsOnceUnderTheOriginalPass() {
        var f=fixture(false);
        var pipeline=(PipelineResource)new io.tapstate.core.dsl.DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: [src]
                serve:
                  from: src
                  sync:
                    - {id: first, source: dest, on_full_load: clear}
                    - {id: second, source: dest, on_full_load: append}
                """);
        f.store.artifacts().save(pipeline);
        var notes=org.mockito.Mockito.mock(RequiredNoteStore.class);
        org.mockito.Mockito.when(notes.guardedBy(org.mockito.ArgumentMatchers.any())).thenReturn(notes);
        org.mockito.Mockito.when(notes.open(org.mockito.ArgumentMatchers.any())).thenAnswer(call -> new RequiredNoteScope(call.getArgument(0),5));
        org.mockito.Mockito.when(notes.prepareOrigin(org.mockito.ArgumentMatchers.any(),org.mockito.ArgumentMatchers.any())).thenAnswer(call -> call.getArgument(1));
        var ref=new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"synthetic.NotLoaded","2.0.8",null,null,"a".repeat(64),false);
        var source=new StoreBackedDagSource(f.store,new StoreBackedDagSource.PdkSinkWriterBinder(notes,id -> ref));
        var pass=new PipelineConvergencePass(new PipelineIntentSnapshot(SCOPE,1,new DesiredState("p",PipelineState.RUNNING,CanonicalHash.of(pipeline))),ORIGINAL,f.views,() -> {});
        var prepared=source.prepareStart(pass,"default");f.views.current.set(advance(1));pass.advanced(advance(1));bindCapturedSources(prepared,pass);
        prepared.plan().build(new ExecutionFence(SCOPE,advance(1)));
        assertThat(f.views.targetRosters).hasSize(1);var roster=f.views.targetRosters.getFirst();assertThat(roster).hasSize(2);
        assertThat(roster).extracting(input -> input.key().nodeId()).containsExactlyInAnyOrder("first","second");
        assertThat(roster).allSatisfy(input -> {assertThat(input.key().scope()).isEqualTo(SCOPE);assertThat(input.key().purpose()).isEqualTo(io.tapstate.core.model.PipelineNode.Purpose.SYNC);assertThat(input.key().target()).isEqualTo("orders");assertThat(input.modelHash()).matches("[a-f0-9]{64}");});
        assertThat(roster).extracting(SinkPreparationStore.Input::policy).containsExactlyInAnyOrder(io.tapstate.core.model.OnFullLoad.CLEAR,io.tapstate.core.model.OnFullLoad.APPEND);
        org.mockito.Mockito.verify(notes,org.mockito.Mockito.never()).initialize(org.mockito.ArgumentMatchers.any());
        org.mockito.Mockito.verify(notes,org.mockito.Mockito.times(2)).prepareOrigin(org.mockito.ArgumentMatchers.any(),org.mockito.ArgumentMatchers.any());
    }

    private static Fixture fixture(boolean join) {
        var store = new InMemoryStorePort(); var parser = new io.tapstate.core.dsl.DslParser();
        store.artifacts().save(parser.parse("version: tapstate/v1\nkind: source\nid: src\nconnector: mysql\nconfig: {host: h}\nmode: cdc\ntables: [orders]\n"));
        store.artifacts().save(parser.parse("version: tapstate/v1\nkind: source\nid: customers_src\nconnector: mysql\nconfig: {host: h}\nmode: cdc\ntables: [customers]\n"));
        store.artifacts().save(parser.parse("version: tapstate/v1\nkind: source\nid: dest\nconnector: mongodb\nconfig: {uri: u}\n"));
        PipelineResource pipeline = pipeline(join, "original_step"); store.artifacts().save(pipeline);
        store.schemas().save(new DiscoveredSourceModel("src", "mysql", 0, new SourceModel(List.of(new SourceTable("orders",
                List.of(new SourceField("id", "INT"), new SourceField("customer_ref", "INT")), List.of("id"), List.of())))));
        store.schemas().save(new DiscoveredSourceModel("customers_src", "mysql", 0, new SourceModel(List.of(new SourceTable("customers",
                List.of(new SourceField("id", "INT"), new SourceField("name", "VARCHAR")), List.of("id"), List.of())))));
        OpenRingGenerations.forSources(store, "src", "customers_src");
        StoreBackedDagSource.SinkWriterBinder binder = (connector, settings, mode, ddl, target, node) -> (SupplierEx<SinkWriter>) () -> null;
        return new Fixture(store, pipeline, new StoreBackedDagSource(store, binder), new Views());
    }

    private static PipelineResource pipeline(boolean join, String step) {
        String transforms = join ? """
                transforms:
                  - id: widen
                    type: join
                    from: {o: orders, c: customers}
                    engine: builtin
                    sql: SELECT o.id AS order_id, c.name AS customer_name FROM o LEFT JOIN c ON o.customer_ref = c.id
                  - id: after_join
                    type: filter
                    from: widen
                    expr: "true"
                """ : """
                transforms:
                  - id: %s
                    type: filter
                    from: src
                    expr: "true"
                """.formatted(step);
        return (PipelineResource) new io.tapstate.core.dsl.DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: p\nsource: "
                + (join ? "[src, customers_src]\n" : "[src]\n") + transforms + "serve:\n  from: " + (join ? "after_join" : step) + "\n  sync: [{id: sync_1, source: dest}]\n");
    }

    private record Fixture(InMemoryStorePort store, PipelineResource pipeline, StoreBackedDagSource source, Views views) {
        PipelineConvergencePass pass(Runnable active) {
            return new PipelineConvergencePass(new PipelineIntentSnapshot(SCOPE, 1,
                    new DesiredState("p", PipelineState.RUNNING, CanonicalHash.of(pipeline))), ORIGINAL, views, active);
        }
    }
    private static final class Views implements PipelineStoreViews {
        final List<List<SinkPreparationStore.Input>> targetRosters=new ArrayList<>();
        @Override public SinkPreparationStore sinkPreparations(PipelineIntentScope scope,WorkloadClaimFence original,Runnable active) {
            return new SinkPreparationStore() {
                public void prepareTargets(List<Input> targets){active.run();scopes.add(scope);writes.add(original);if(!scope.equals(SCOPE)||!original.equals(current.get()))throw fenced();targetRosters.add(List.copyOf(targets));}
                public Snapshot load(Input input){throw new AssertionError("model planning opens no target");}
                public Attempt begin(Input input){throw new AssertionError("model planning opens no target");}
                public Snapshot complete(Attempt attempt,Result result){throw new AssertionError("model planning opens no target");}
            };
        }
        final InMemoryDerivedSchemaStore models = new InMemoryDerivedSchemaStore();
        final Map<String,java.util.Set<String>> shapes = new java.util.LinkedHashMap<>();
        @Override public NestShapeStore nestShapes(PipelineIntentScope scope,String database) {
            scopes.add(scope); return new NestShapeStore() {
                @Override public Optional<java.util.Set<String>> read(String step) { return Optional.ofNullable(shapes.get(database+"/"+step)); }
                @Override public Optional<java.util.Set<String>> recordIfAbsent(String step,java.util.Set<String> paths) { throw new AssertionError("read view"); }
            };
        }
        @Override public NestShapeStore nestShapes(PipelineIntentScope scope,String database,WorkloadClaimFence original,Runnable active) {
            scopes.add(scope); return new NestShapeStore() {
                @Override public Optional<java.util.Set<String>> read(String step) { return Optional.ofNullable(shapes.get(database+"/"+step)); }
                @Override public Optional<java.util.Set<String>> recordIfAbsent(String step,java.util.Set<String> paths) {
                    active.run(); writes.add(original); if (!scope.equals(SCOPE) || !original.equals(current.get())) throw fenced();
                    return Optional.ofNullable(shapes.putIfAbsent(database+"/"+step,java.util.Set.copyOf(paths)));
                }
            };
        }
        final AtomicReference<WorkloadClaimFence> current = new AtomicReference<>(ORIGINAL);
        final List<WorkloadClaimFence> writes = new ArrayList<>(); final List<PipelineIntentScope> scopes = new ArrayList<>();
        @Override public DerivedSchemaStore pipelineDerivedSchemas(PipelineIntentScope scope) { scopes.add(scope); return models; }
        @Override public DerivedSchemaStore pipelineDerivedSchemas(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            scopes.add(scope);
            return new DerivedSchemaStore() {
                private void prove() { active.run(); writes.add(original); if (!scope.equals(SCOPE) || !original.equals(current.get())) throw fenced(); }
                @Override public Optional<DerivedSchema> latest(String id, String step) { return models.latest(id, step); }
                @Override public void record(String id, String step, Map<String, String> schema, String statement, String from, String by) { prove(); models.record(id, step, schema, statement, from, by); }
                @Override public void pin(String id, String step, long version) { prove(); models.pin(id, step, version); }
                @Override public Optional<DerivedSchema> pinned(String id, String step) { return models.pinned(id, step); }
                @Override public void delete(String id) { throw new AssertionError("ordinary planning must not delete models"); }
            };
        }
        @Override public StateStore pipelineStates(PipelineIntentScope scope) { throw new AssertionError("unrelated state"); }
        @Override public StateStore pipelineStates(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) { throw new AssertionError("unrelated state"); }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope) { throw new AssertionError("unrelated observations"); }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) { throw new AssertionError("unrelated observations"); }
    }
    private static WorkloadClaimFence advance(long execution) { return new WorkloadClaimFence(ORIGINAL.key(), ORIGINAL.owner(), ORIGINAL.claimGeneration(), execution, ORIGINAL.topologyRevision()); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
