package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import io.tapstate.control.core.PipelineWriteTargets;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A view is written with the write mode and full-load policy it declares -- inline, or on the shared
 * definition it uses -- and with the same defaults a sync element has when it declares neither.
 */
class AViewWritesWithItsDeclaredSettingsTest {

    private static final String PIPE = "leads";

    @Test
    void anInlineViewBindsItsDeclaredSettings() {
        InMemoryStorePort store = store(new ViewBlock.Inline("lead", FromRef.literal("src"), "id", null,
                io.tapstate.core.model.WriteMode.APPEND, io.tapstate.core.model.OnFullLoad.CLEAR));
        CapturingBinder binder = start(store);
        assertThat(binder.mode).isEqualTo(WriteMode.APPEND);
        assertThat(binder.policy).isEqualTo(OnFullLoad.CLEAR);
        assertThat(binder.fullLoad).isTrue();
    }

    @Test
    void aViewThatDeclaresNothingIsWrittenAsBefore() {
        CapturingBinder binder = start(store(new ViewBlock.Inline("lead", FromRef.literal("src"), "id", null)));
        assertThat(binder.mode).isEqualTo(WriteMode.UPSERT);
        assertThat(binder.policy).isEqualTo(OnFullLoad.APPEND);
    }

    @Test
    void aSharedDefinitionsSettingsReachTheRun() {
        InMemoryStorePort store = store(new ViewBlock.Use("lead", "v_lead", FromRef.literal("src")));
        store.artifacts().save(new ViewResource("v_lead", null, "id", null,
                io.tapstate.core.model.WriteMode.APPEND, io.tapstate.core.model.OnFullLoad.FAIL, null));
        CapturingBinder binder = start(store);
        assertThat(binder.mode).isEqualTo(WriteMode.APPEND);
        assertThat(binder.policy).isEqualTo(OnFullLoad.FAIL);
    }

    @Test
    void theStartPlanNamesTheViewsCollectionAndWhereItsPolicyIsDeclared() {
        InMemoryStorePort store = store(new ViewBlock.Use("lead", "v_lead", FromRef.literal("src")));
        store.artifacts().save(new ViewResource("v_lead", null, "id", null,
                null, io.tapstate.core.model.OnFullLoad.CLEAR, null));
        PipelineResource stored = StoredArtifacts.requirePipeline(store.artifacts(), PIPE);

        assertThat(new StoreBackedPipelineWriteTargets(store).of(stored)).singleElement().satisfies(target -> {
            assertThat(target.kind()).isEqualTo(PipelineWriteTargets.WriteTarget.Kind.VIEW);
            assertThat(target.element()).isEqualTo("lead");
            assertThat(target.connection()).isEqualTo(ViewTargetResolver.STATE_STORE_SOURCE_ID);
            assertThat(target.table()).isEqualTo("lead");
            assertThat(target.onFullLoad()).isEqualTo(io.tapstate.core.model.OnFullLoad.CLEAR);
            assertThat(target.definedIn()).isEqualTo("v_lead");
        });
    }

    private static CapturingBinder start(InMemoryStorePort store) {
        CapturingBinder binder = new CapturingBinder();
        new StoreBackedDagSource(store, binder).prepareStart(PIPE, "tapstate").build(null);
        return binder;
    }

    private static InMemoryStorePort store(ViewBlock view) {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null));
        store.artifacts().save(new SourceResource(ViewTargetResolver.STATE_STORE_SOURCE_ID, null, "mongodb",
                Map.of("uri", "mongodb://views"), null, null, null, null));
        store.schemas().save(new DiscoveredSourceModel("src", "mysql", 0L, new SourceModel(List.of(
                new SourceTable("orders", List.of(new SourceField("id", "bigint", TapstateType.INT64, null)),
                        List.of("id"), List.of())))));
        store.artifacts().save(new PipelineResource(PIPE, null, List.of(SourceRef.spec("src", true)), null,
                view, null, new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, null), null));
        OpenRingGenerations.forSources(store, "src");
        return store;
    }

    private static final class CapturingBinder implements StoreBackedDagSource.SinkWriterBinder {
        WriteMode mode;
        OnFullLoad policy;
        boolean fullLoad;

        @Override
        public SupplierEx<? extends SinkWriter> bind(String connector, Map<String, Object> settings,
                WriteMode mode, DdlPolicy ddl, TargetTable target, PipelineNode node) {
            throw new AssertionError("a view sink must be bound with its full-load policy");
        }

        @Override
        public SupplierEx<? extends SinkWriter> bind(String connector, Map<String, Object> settings,
                WriteMode mode, DdlPolicy ddl, Map<String, TargetTable> targets, PipelineNode node,
                OnFullLoad policy, boolean fullLoad) {
            this.mode = mode;
            this.policy = policy;
            this.fullLoad = fullLoad;
            return new PdkSinkWriterFactory(connector, settings, mode, ddl, targets, node, policy, fullLoad);
        }
    }
}
