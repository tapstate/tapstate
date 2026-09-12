package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.*;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class FullLoadSinkContextTest {
    @Test
    void freshLoadAndRerunHonorTheDeclaredPolicy() {
        InMemoryStorePort store = store(ReadMode.SNAPSHOT_AND_CDC, io.tapstate.core.model.OnFullLoad.CLEAR);
        CapturingBinder binder = bind(store);
        assertThat(binder.factory.onFullLoad()).isEqualTo(OnFullLoad.CLEAR);
        assertThat(binder.factory.fullLoad()).isTrue();
    }

    @Test
    void cdcOnlyOverridesClear() {
        CapturingBinder binder = bind(store(ReadMode.CDC_ONLY, io.tapstate.core.model.OnFullLoad.CLEAR));
        assertThat(binder.factory.onFullLoad()).isEqualTo(OnFullLoad.CLEAR);
        assertThat(binder.factory.fullLoad()).isFalse();
    }

    @Test
    void aPreviousConsumerMakesTheNextLoadAResume() {
        InMemoryStorePort store = store(ReadMode.SNAPSHOT_AND_CDC, io.tapstate.core.model.OnFullLoad.CLEAR);
        SourceResource source = StoredArtifacts.requireSource(store.artifacts(), "src");
        String chain = SourceCaptureResolution.of(source, SourceDiscovery.model(store, source)).chainId().value();
        store.meta().advanceConsumerReadSeq(chain, "pipe", "orders", 0L);
        assertThat(bind(store).factory.fullLoad()).isFalse();
    }

    @Test
    void anotherPipelinesConsumerDoesNotMakeThisPipelinesLoadAResume() {
        InMemoryStorePort store = store(ReadMode.SNAPSHOT_AND_CDC, io.tapstate.core.model.OnFullLoad.APPEND);
        SourceResource source = StoredArtifacts.requireSource(store.artifacts(), "src");
        String chain = SourceCaptureResolution.of(source, SourceDiscovery.model(store, source)).chainId().value();
        store.meta().advanceConsumerReadSeq(chain, "other", "orders", 0L);
        assertThat(bind(store).factory.fullLoad()).isTrue();
    }

    private static CapturingBinder bind(InMemoryStorePort store) {
        CapturingBinder binder = new CapturingBinder();
        new StoreBackedDagSource(store, binder).dagFor("pipe");
        return binder;
    }

    private static InMemoryStorePort store(ReadMode mode, io.tapstate.core.model.OnFullLoad policy) {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null));
        store.artifacts().save(new SourceResource("dest", null, "mysql", Map.of("host", "other"), null, null, null, null));
        store.schemas().save(new DiscoveredSourceModel("src", "mysql", 0L, new SourceModel(List.of(
                new SourceTable("orders", List.of(new SourceField("id", "bigint", TapstateType.INT64, null)),
                        List.of("id"), List.of())))));
        store.artifacts().save(new PipelineResource("pipe", null, List.of(SourceRef.spec("src", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("src"), List.of(
                        new SyncElement("sink", "dest", null, null, null, policy)), null, null),
                new Settings(null, null, null, null, mode, null), null));
        OpenRingGenerations.forSources(store, "src");
        return store;
    }

    private static final class CapturingBinder implements StoreBackedDagSource.SinkWriterBinder {
        PdkSinkWriterFactory factory;
        public SupplierEx<? extends SinkWriter> bind(String connector, Map<String, Object> settings,
                WriteMode mode, DdlPolicy ddl, TargetTable target, PipelineNode node) {
            throw new AssertionError("the complete sink policy must reach the binder");
        }
        public SupplierEx<? extends SinkWriter> bind(String connector, Map<String, Object> settings,
                WriteMode mode, DdlPolicy ddl, Map<String, TargetTable> targets, PipelineNode node,
                OnFullLoad policy, boolean fullLoad) {
            factory = new PdkSinkWriterFactory(connector, settings, mode, ddl, targets, node, policy, fullLoad);
            return factory;
        }
    }
}
