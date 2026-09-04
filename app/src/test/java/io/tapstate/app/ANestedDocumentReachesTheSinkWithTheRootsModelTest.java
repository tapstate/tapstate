package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * A nest's assembled documents reach the sink carrying a resolved target model, like every other
 * stream that reaches a sink does.
 *
 * <p>What this pins is the seam between two halves that were each right on their own. A nest emits
 * its documents under the id of the step that assembled them, because that is the stream identity
 * the rest of the engine addresses the node by. The write-side model resolution answers by
 * <em>source table</em>, because every stream that had ever reached a sink before was a source
 * table. Nothing joined the two, so the sink looked up the step id, found nothing, and fell back to
 * a table descriptor built from a bare name - carrying no key for an upsert to match on and no
 * field map at all.
 *
 * <p>Why no test caught it: every nest test in this repository binds a capturing sink, and a
 * capturing sink does not read the model. Only a real connector does, and the tests that drive one
 * are gated on connector jars, so they run outside the build that would have reported this.
 *
 * <p>The discriminating half is the second and third assertions rather than the first. A resolution
 * that registered the step id against an <em>empty</em> model would satisfy "the sink is told about
 * this stream" while leaving the sink exactly as unable to key an upsert - so the name the documents
 * land under and the key they are matched on are both read here. The root's key is what a document
 * is addressed by; matching on anything else turns a re-sent document into a second row.
 */
class ANestedDocumentReachesTheSinkWithTheRootsModelTest {

    private static final String PARENT_SOURCE = "src_orders";
    private static final String CHILD_SOURCE = "src_items";
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String DEST_ID = "tgt";
    private static final String STEP = "order_doc";
    private static final String PIPELINE = "nested_orders";
    private static final String EMBED_PATH = "items";

    @Test
    void theSinkIsToldWhatModelTheAssembledDocumentsCarry() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(seedStore(), capturing(bound)).dagFor(PIPELINE);

        Map<String, TargetTable> targets = bound.get();
        assertThat(targets)
                .as("the streams the sink was given a model for; the nest emits under '%s'", STEP)
                .containsKey(STEP);

        TargetTable assembled = targets.get(STEP);
        assertThat(assembled.name())
                .as("the table assembled documents land in, which is the root's rather than the step's")
                .isEqualTo(PARENT_TABLE);
        assertThat(assembled.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .as("the key an upsert matches a re-sent document on")
                .containsExactly("id");
    }

    /**
     * The same claim over resources the product's own parser produced, rather than over model objects a
     * fixture assembled. What a nest step's {@code from:} aliases resolve to is decided while parsing, so a
     * fixture that hand-builds them can agree with the resolution here while a parsed document does not.
     */
    @Test
    void theSinkIsToldTheSameWhenTheDocumentsCameThroughTheParser() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(), capturing(bound)).dagFor(PIPELINE);

        assertThat(bound.get())
                .as("the streams the sink was given a model for, from parsed resources")
                .containsKey(STEP);
        assertThat(bound.get().get(STEP).name())
                .as("the table assembled documents land in")
                .isEqualTo(PARENT_TABLE);
    }

    /**
     * The table half of the same claim, over a root that was never discovered. The columns are a
     * discovery's to supply and are absent here; which table the documents land in is the topology's, and
     * is known without one.
     *
     * <p>Undiscovered, this stream used to reach the sink as a name and nothing else, and the sink's
     * fallback then named the collection after the transform step. That put assembled documents somewhere
     * no discovered run would ever put them, so whether a discovery had run decided where the data went.
     */
    @Test
    void theSinkIsToldWhereUndiscoveredAssembledDocumentsLand() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(undiscoveredStore(), capturing(bound)).dagFor(PIPELINE);

        assertThat(bound.get())
                .as("the streams the sink was given a model for, with no discovery to draw on")
                .containsKey(STEP);
        assertThat(bound.get().get(STEP).name())
                .as("the table assembled documents land in, which is the root's rather than the step's")
                .isEqualTo(PARENT_TABLE);
    }

    /** The same workspace with no discovery saved for either source. */
    private static InMemoryStorePort undiscoveredStore() {
        return new InMemoryStorePort(seedArtifacts());
    }

    /** The witness's own documents, parsed by the product's parser and stored as an apply would. */
    private static InMemoryStorePort parsedStore() {
        DslParser parser = new DslParser();
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        for (String document : List.of(
                """
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: mysql
                config: { host: h, port: 3306, database: d, username: u, password: p }
                mode: cdc
                tables: [ orders ]
                """,
                """
                version: tapstate/v1
                kind: source
                id: src_items
                connector: mysql
                config: { host: h, port: 3306, database: d, username: u, password: p }
                mode: cdc
                tables: [ order_items ]
                """,
                """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                config: { uri: "mongodb://localhost:27017/t" }
                """,
                """
                version: tapstate/v1
                kind: pipeline
                id: nested_orders
                source: [ src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """)) {
            artifacts.save(parser.parse(document));
        }

        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        // The connector id has to be the one the parsed sources declare: a discovery belonging to another
        // connector is deliberately not read, so a mismatch here would empty the resolution rather than
        // exercise it.
        store.schemas().save(new DiscoveredSourceModel(PARENT_SOURCE, "mysql", 0L,
                new SourceModel(List.of(new SourceTable(PARENT_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("name", "string")),
                        List.of("id"), List.of())))));
        store.schemas().save(new DiscoveredSourceModel(CHILD_SOURCE, "mysql", 0L,
                new SourceModel(List.of(new SourceTable(CHILD_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("order_id", "int"),
                                new SourceField("sku", "string")),
                        List.of("id"), List.of())))));
        return store;
    }

    /** Records the map handed to the sink binder without building a writer. */
    private static StoreBackedDagSource.SinkWriterBinder capturing(
            AtomicReference<Map<String, TargetTable>> bound) {
        return new StoreBackedDagSource.SinkWriterBinder() {

            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, TargetTable target) {
                return (SupplierEx<SinkWriter>) () -> null;
            }

            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, Map<String, TargetTable> targets) {
                bound.set(targets);
                return (SupplierEx<SinkWriter>) () -> null;
            }
        };
    }

    /** Two single-table sources, a sink connection, and a nest pipeline serving the assembled step. */
    private static InMemoryStorePort seedStore() {
        InMemoryStorePort store = new InMemoryStorePort(seedArtifacts());
        store.schemas().save(discovered(PARENT_SOURCE,
                new SourceTable(PARENT_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("name", "string")),
                        List.of("id"), List.of())));
        store.schemas().save(discovered(CHILD_SOURCE,
                new SourceTable(CHILD_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("order_id", "int"),
                                new SourceField("sku", "string")),
                        List.of("id"), List.of())));
        return store;
    }

    /** The artifacts both witnesses share; only whether a discovery was saved separates them. */
    private static InMemoryArtifactStore seedArtifacts() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source(PARENT_SOURCE, PARENT_TABLE));
        artifacts.save(source(CHILD_SOURCE, CHILD_TABLE));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));

        Embed item = new Embed("i", Map.of("order_id", "id"), EmbedAs.ARRAY, EMBED_PATH, List.of("id"),
                null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("id"), null, null, List.of(item)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("o", FromRef.literal(PARENT_TABLE));
        aliases.put("i", FromRef.literal(CHILD_TABLE));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(PARENT_SOURCE, true), SourceRef.spec(CHILD_SOURCE, true)),
                List.of(step), null,
                new ServeBlock.Inline(null, FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        return artifacts;
    }

    private static SourceResource source(String id, String table) {
        return new SourceResource(id, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null, null);
    }

    private static DiscoveredSourceModel discovered(String connectionId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, "fake", 0L, new SourceModel(List.of(table)));
    }
}
