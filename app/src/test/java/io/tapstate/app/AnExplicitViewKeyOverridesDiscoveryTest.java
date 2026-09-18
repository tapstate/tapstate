package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThatCode;

import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AnExplicitViewKeyOverridesDiscoveryTest {

    @Test
    void a_discovered_source_identity_does_not_override_the_explicit_view_key() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(new SourceResource("src", null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null));
        artifacts.save(new SourceResource(ViewTargetResolver.STATE_STORE_SOURCE_ID, null, "fake",
                Map.of("host", "d"), null, null, null, null));
        artifacts.save(new PipelineResource("orders_pipeline", null,
                List.of(SourceRef.spec("src", true)), null,
                new ViewBlock.Inline("order_state", FromRef.literal("orders"), "id", null),
                null, new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));

        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        // The index arrives as Mongo discovery reports one, descriptor and all, because that descriptor
        // is what shows the uniqueness covering every document rather than only the ones it qualifies.
        // The explicit key wins over the discovered identity, but only against an identity that holds.
        store.schemas().save(new DiscoveredSourceModel("src", "fake", 0L, new SourceModel(List.of(
                new SourceTable("orders",
                        List.of(new SourceField("_id", "objectId"), new SourceField("id", "int")),
                        List.of("_id"), List.of(new SourceIndex(
                                "__t__{\"v\": 2, \"key\": {\"id\": 1}, \"name\": \"id_unique\", "
                                        + "\"unique\": true}", List.of("id"), true)))))));

        assertThatCode(() -> new StoreBackedDagSource(store).dagFor("orders_pipeline"))
                .doesNotThrowAnyException();
    }
}
