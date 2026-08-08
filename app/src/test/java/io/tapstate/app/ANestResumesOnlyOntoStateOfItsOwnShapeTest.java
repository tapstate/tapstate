package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The assembled product's half of the refusal: that a nest's state paths are written down where they
 * outlive the process, and read back by the next job built over the same store.
 *
 * <p>The engine owns the comparison and is tested beside it; what these pin is that production supplies
 * something to compare against at all. A binding that reached the engine with no ledger behind it would
 * pass every engine test and refuse nothing in the field - so the assertions here are made through a
 * second, freshly built {@code StoreBackedDagSource}, which is the closest a test without a restart gets
 * to being a different process reading the same store.
 */
class ANestResumesOnlyOntoStateOfItsOwnShapeTest {

    private static final String PIPELINE = "nested_orders";
    private static final String PARENT_SOURCE = "src_orders";
    private static final String CHILD_SOURCE = "src_items";
    private static final String DEST_ID = "tgt_mongo";
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String STEP = "order_doc";

    @Test
    void aNestBuiltTwiceOverTheSameTreeResumesOntoItsOwnState() {
        InMemoryStorePort store = seedStore("items");

        new StoreBackedDagSource(store).dagFor(PIPELINE);

        assertThatCode(() -> new StoreBackedDagSource(store).dagFor(PIPELINE))
                .doesNotThrowAnyException();
    }

    @Test
    void aNestWhoseEmbedPathWasRenamedIsRefusedRatherThanRebuiltFromEmpty() {
        InMemoryStorePort store = seedStore("items");
        new StoreBackedDagSource(store).dagFor(PIPELINE);

        store.artifacts().save(pipeline("lines"));

        assertThatThrownBy(() -> new StoreBackedDagSource(store).dagFor(PIPELINE))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).code().code())
                .isEqualTo("nest.state-paths-changed");
    }

    @Test
    void whatTheFirstBuildWroteDownOutlivesTheThingThatWroteIt() {
        InMemoryStorePort store = seedStore("items");

        new StoreBackedDagSource(store).dagFor(PIPELINE);

        assertThat(store.keyedState().load("nest.shape." + PIPELINE, STEP))
                .describedAs("the record is in the store the state itself is in, not in the object that "
                        + "happened to build the job")
                .isPresent();
    }

    // ---- fixtures ---------------------------------------------------------------------

    /** The two sources, the sink connection, a discovered model each, and the nest pipeline. */
    private static InMemoryStorePort seedStore(String embedPath) {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source(PARENT_SOURCE, PARENT_TABLE));
        artifacts.save(source(CHILD_SOURCE, CHILD_TABLE));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));
        artifacts.save(pipeline(embedPath));

        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.schemas().save(discovered(PARENT_SOURCE,
                new SourceTable(PARENT_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("name", "string")),
                        List.of("id"), List.of())));
        store.schemas().save(discovered(CHILD_SOURCE,
                new SourceTable(CHILD_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("order_id", "int"),
                                new SourceField("sku", "string")),
                        List.of("id"), List.of())));
        OpenRingGenerations.forSources(store, PARENT_SOURCE, CHILD_SOURCE);
        return store;
    }

    private static PipelineResource pipeline(String embedPath) {
        Embed item = new Embed("i", Map.of("order_id", "id"), EmbedAs.ARRAY, embedPath, List.of("id"),
                null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("id"), null, null, List.of(item)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("o", FromRef.literal(PARENT_TABLE));
        aliases.put("i", FromRef.literal(CHILD_TABLE));
        return new PipelineResource(PIPELINE, null, List.of(PARENT_SOURCE, CHILD_SOURCE),
                List.of(Step.inline(STEP, FromClause.aliases(aliases), body, null, null)), null,
                new ServeBlock.Inline(null, FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null);
    }

    private static SourceResource source(String id, String table) {
        return new SourceResource(id, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null, null);
    }

    private static DiscoveredSourceModel discovered(String connectionId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, "fake", 0L, new SourceModel(List.of(table)));
    }
}
