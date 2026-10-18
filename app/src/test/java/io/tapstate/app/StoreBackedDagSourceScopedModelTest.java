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
        prepared.plan().build(null);
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
        var prepared = source.prepareStart(f.pass(() -> {}), "default"); nested.put("label", "changed-input");
        SourceResource exposed = (SourceResource) prepared.artifactSnapshot().orElseThrow().list().stream().filter(row -> row.id().equals("dest")).findFirst().orElseThrow();
        @SuppressWarnings("unchecked") var extra = (Map<String, Object>) exposed.config().get("extra");
        try { extra.put("label", "changed-result"); } catch (UnsupportedOperationException immutable) { /* A detached or immutable result is safe. */ }
        prepared.plan().build(null);
        assertThat(bound).isNotEmpty(); assertThat(bound.getLast()).containsEntry("extra", Map.of("label", "original"));
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
        final InMemoryDerivedSchemaStore models = new InMemoryDerivedSchemaStore();
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
