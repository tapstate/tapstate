package io.tapstate.app;

import io.tapstate.core.model.PipelineNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.NumericType;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.math.BigDecimal;
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
        assertThat(bound.get().get(STEP).fields().stream().map(TargetField::name))
                .as("the root fields retained by the target model")
                .containsExactly("id", "name");
        assertThat(bound.get().get(STEP).fields().stream()
                .filter(TargetField::primaryKey).map(TargetField::name))
                .as("the root key the preview materializer uses to identify documents")
                .containsExactly("id");
    }

    /**
     * A root fed through a map rather than straight from its table - the shape a soft-delete map
     * takes, since it has to sit on the stream the root reads. The map keeps every row as one row
     * under the same key, so the documents still land in the root's table, upserted by the root's
     * key. Resolved by the alias alone, the root named the map step and no key at all: the target
     * was created under the step's name and appended a document per change.
     */
    @Test
    void aRootFedThroughAMapStillLandsInTheRootsTableByItsKey() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_orders.orders ]
                            soft_delete: is_deleted
                        """)), capturing(bound)).dagFor(PIPELINE);

        TargetTable assembled = bound.get().get(STEP);
        assertThat(assembled.name())
                .as("the table assembled documents land in, which is the root table's, not the map step's")
                .isEqualTo(PARENT_TABLE);
        assertThat(assembled.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .as("the key an upsert matches a re-sent document on")
                .containsExactly("id");
        assertThat(assembled.fields())
                .as("the marker the map writes on every document, declared like the columns it carries")
                .filteredOn(field -> field.name().equals("is_deleted"))
                .singleElement()
                .satisfies(field -> {
                    assertThat(field.inferredType()).isEqualTo(TapstateType.BOOLEAN);
                    assertThat(field.primaryKey()).isFalse();
                });
    }

    /**
     * The target is created to the shape the map writes: a column the map drops is not declared, and a
     * computed column is, with the type its expression gives and no part in the key.
     */
    @Test
    void aRootFedThroughAMapIsDeclaredWithTheColumnsTheMapWrites() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_orders.orders ]
                            fields: { name: false, label: "='order'" }
                            soft_delete: is_deleted
                        """)), capturing(bound)).dagFor(PIPELINE);

        TargetTable assembled = bound.get().get(STEP);
        assertThat(assembled.fields().stream().map(TargetField::name))
                .as("the key first, then the columns in the order the map writes them")
                .containsExactly("id", "label", "is_deleted");
        assertThat(assembled.fields())
                .filteredOn(field -> field.name().equals("label"))
                .singleElement()
                .satisfies(field -> {
                    assertThat(field.inferredType()).isEqualTo(TapstateType.STRING);
                    assertThat(field.primaryKey()).isFalse();
                });
    }

    /**
     * A computed column that only moves another keeps that column's descriptors, as a rename does:
     * without them a moved decimal reaches the target with no precision, scale or range to build its
     * column from.
     */
    @Test
    void aDecimalAMapMovesKeepsItsPrecisionAndScale() {
        NumericType money = new NumericType(null, true, false, null,
                new BigDecimal("-99999999.99"), new BigDecimal("99999999.99"), 10, 2);
        DiscoveredSourceModel withAmount = new DiscoveredSourceModel(PARENT_SOURCE, "mysql", 1L,
                new SourceModel(List.of(new SourceTable(PARENT_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("name", "string"),
                                new SourceField("amount", "decimal(10,2)", TapstateType.DECIMAL, null, money)),
                        List.of("id"), List.of()))));
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_orders.orders ]
                            fields: { amount_copy: "=after.amount" }
                            soft_delete: is_deleted
                        """), List.of(), List.of(withAmount)), capturing(bound)).dagFor(PIPELINE);

        assertThat(bound.get().get(STEP).fields())
                .filteredOn(field -> field.name().equals("amount_copy"))
                .singleElement()
                .satisfies(field -> assertThat(field.numericType()).isEqualTo(money));
    }

    /**
     * A map that writes a literal over the table's key column. Every row then carries the same value
     * under the key's name, so that name no longer tells rows apart: inferring it as the root's key
     * would upsert every document onto one. With no key declared, the nest is refused for having no
     * root key instead of being keyed on the literal.
     */
    @Test
    void aRootWhoseKeyAMapReplacesTakesNoKeyFromTheTable() {
        StoreBackedDagSource dag = new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("      key: [ id ]\n", "")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_orders.orders ]
                            fields: { id: 0 }
                            soft_delete: is_deleted
                        """)), capturing(new AtomicReference<>()));

        assertThatThrownBy(() -> dag.dagFor(PIPELINE))
                .as("the root's key would otherwise be the column every row now holds 0 in")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("TS0490006");
    }

    /**
     * The same walk, where the map reads the source by its id. A source that selects one table is read
     * by naming the source, and that name is not a table: the walk has to finish at the table the source
     * selects, or the target is keyless again and named after the source.
     */
    @Test
    void aRootFedThroughAMapThatNamesItsSourceStillLandsInTheRootsTableByItsKey() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_orders ]
                            soft_delete: is_deleted
                        """)), capturing(bound)).dagFor(PIPELINE);

        TargetTable assembled = bound.get().get(STEP);
        assertThat(assembled.name()).isEqualTo(PARENT_TABLE);
        assertThat(assembled.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .containsExactly("id");
    }

    /**
     * A map that renames the root's key column: the key is written in the name the map gave it, and
     * the target is keyed on that name. Read off the raw source table instead, the key names a column
     * the table does not have, nothing is marked primary, and every change is appended.
     */
    @Test
    void aRootKeyedOnAColumnTheMapRenamedIsKeyedOnTheNewName() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("key: [ id ]", "key: [ order_key ]")
                .replace("on: { order_id: id }", "on: { order_id: order_key }")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_orders.orders ]
                            fields: { order_key: $id }
                            soft_delete: is_deleted
                        """)), capturing(bound)).dagFor(PIPELINE);

        TargetTable assembled = bound.get().get(STEP);
        assertThat(assembled.name()).isEqualTo(PARENT_TABLE);
        assertThat(assembled.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .containsExactly("order_key");
    }

    /**
     * A root that leaves its key to discovery, fed through a map: the map writes the root table's rows,
     * so that table's key identifies them, and the nest is not refused for having no key.
     */
    @Test
    void aRootFedThroughAMapTakesItsTablesKeyWhenItDeclaresNone() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("      key: [ id ]\n", "")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_orders.orders ]
                            soft_delete: is_deleted
                        """)), capturing(bound)).dagFor(PIPELINE);

        TargetTable assembled = bound.get().get(STEP);
        assertThat(assembled.name()).isEqualTo(PARENT_TABLE);
        assertThat(assembled.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .containsExactly("id");
    }

    /**
     * A script between the table and the map ends the walk. What a script emits is not the table it
     * reads, so the target stays where it was before the walk existed - under the alias's step - rather
     * than moving to the script's name.
     */
    @Test
    void aWalkThatMeetsAScriptLeavesTheTargetWhereItWas() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("from: { o: src_orders.orders,", "from: { o: live_orders,")
                .replace("transforms:\n", """
                        transforms:
                          - id: cleanup
                            type: js
                            from: [ src_orders.orders ]
                            script: |
                              function process(record, ctx) { return record; }
                          - id: live_orders
                            type: map
                            from: [ cleanup ]
                            soft_delete: is_deleted
                        """)), capturing(bound)).dagFor(PIPELINE);

        assertThat(bound.get().get(STEP).name()).isEqualTo("live_orders");
    }

    /**
     * Two sources read tables of one name, and the map reads the second by its source id. The walk has
     * to end at that source's own stream: ending at the bare table name lets the first source's model
     * answer, and the target then neither carries the second source's key nor matches its stream.
     */
    @Test
    void aRootReadThroughASourceIdKeepsThatSourcesStreamWhenTableNamesRepeat() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("source: [ src_orders, src_items ]", "source: [ src_orders, src_items, src_archive ]")
                // The other source's same-named table is read too, as a second embed, so both are streams.
                .replace("from: { o: src_orders.orders, i: src_items.order_items }",
                        "from: { o: live_orders, i: src_items.order_items, c: src_orders.orders }")
                .replace("key: [ id ]", "key: [ archive_id ]")
                .replace("- { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }",
                        "- { from: i, on: { order_id: archive_id }, as: array, path: items, arrayKey: [ id ] }\n"
                        + "        - { from: c, on: { id: archive_id }, as: object, path: current }")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ src_archive ]
                            soft_delete: is_deleted
                        """),
                List.of("""
                        version: tapstate/v1
                        kind: source
                        id: src_archive
                        connector: mysql
                        config: { host: h, port: 3306, database: d2, username: u, password: p }
                        mode: cdc
                        tables: [ orders ]
                        """),
                List.of(new DiscoveredSourceModel("src_archive", "mysql", 0L,
                        new SourceModel(List.of(new SourceTable(PARENT_TABLE,
                                List.of(new SourceField("archive_id", "int"), new SourceField("name", "string")),
                                List.of("archive_id"), List.of())))))),
                capturing(bound)).dagFor(PIPELINE);

        TargetTable assembled = bound.get().get(STEP);
        assertThat(assembled.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .as("the key of the source the map reads, not of the other source's same-named table")
                .containsExactly("archive_id");
    }

    /**
     * A source whose id is the same word as another source's table. The DAG reads the reference as the
     * source, so the walk does too: answering with the same-named table would land the documents in the
     * other source's table, keyed on its key.
     */
    @Test
    void aSourceIdWinsOverATableOfTheSameName() {
        AtomicReference<Map<String, TargetTable>> bound = new AtomicReference<>();
        new StoreBackedDagSource(parsedStore(PARSED_PIPELINE
                .replace("source: [ src_orders, src_items ]", "source: [ src_orders, src_items, orders ]")
                .replace("from: { o: src_orders.orders, i: src_items.order_items }",
                        "from: { o: live_orders, i: src_items.order_items, c: src_orders.orders }")
                .replace("key: [ id ]", "key: [ archive_id ]")
                .replace("- { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }",
                        "- { from: i, on: { order_id: archive_id }, as: array, path: items, arrayKey: [ id ] }\n"
                        + "        - { from: c, on: { id: archive_id }, as: object, path: current }")
                .replace("transforms:\n", """
                        transforms:
                          - id: live_orders
                            type: map
                            from: [ orders ]
                            soft_delete: is_deleted
                        """),
                List.of("""
                        version: tapstate/v1
                        kind: source
                        id: orders
                        connector: mysql
                        config: { host: h, port: 3306, database: d2, username: u, password: p }
                        mode: cdc
                        tables: [ archived_orders ]
                        """),
                List.of(new DiscoveredSourceModel("orders", "mysql", 0L,
                        new SourceModel(List.of(new SourceTable("archived_orders",
                                List.of(new SourceField("archive_id", "int"), new SourceField("name", "string")),
                                List.of("archive_id"), List.of())))))),
                capturing(bound)).dagFor(PIPELINE);

        TargetTable assembled = bound.get().get(STEP);
        assertThat(assembled.name()).isEqualTo("archived_orders");
        assertThat(assembled.fields().stream().filter(TargetField::primaryKey).map(TargetField::name))
                .containsExactly("archive_id");
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

    private static final String PARSED_PIPELINE = """
                version: tapstate/v1
                kind: pipeline
                id: nested_orders
                source: [ src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: src_orders.orders, i: src_items.order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """;

    /** The witness's own documents, parsed by the product's parser and stored as an apply would. */
    private static InMemoryStorePort parsedStore() {
        return parsedStore(PARSED_PIPELINE);
    }

    private static InMemoryStorePort parsedStore(String pipeline) {
        return parsedStore(pipeline, List.of(), List.of());
    }

    private static InMemoryStorePort parsedStore(String pipeline, List<String> moreSources,
            List<DiscoveredSourceModel> moreModels) {
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
                pipeline)) {
            artifacts.save(parser.parse(document));
        }
        moreSources.forEach(document -> artifacts.save(parser.parse(document)));

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
        moreModels.forEach(store.schemas()::save);
        return store;
    }

    /** Records the map handed to the sink binder without building a writer. */
    private static StoreBackedDagSource.SinkWriterBinder capturing(
            AtomicReference<Map<String, TargetTable>> bound) {
        return new StoreBackedDagSource.SinkWriterBinder() {

            @Override
            public SupplierEx<SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, TargetTable target, PipelineNode node) {
                return (SupplierEx<SinkWriter>) () -> null;
            }

            @Override
            public SupplierEx<SinkWriter> bind(String connectorId, Map<String, Object> settings,
                    WriteMode writeMode, DdlPolicy ddl, Map<String, TargetTable> targets, PipelineNode node) {
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
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null));

        Embed item = new Embed("i", Map.of("order_id", "id"), EmbedAs.ARRAY, EMBED_PATH, List.of("id"),
                null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("id"), null, null, List.of(item)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("o", FromRef.literal(PARENT_TABLE));
        aliases.put("i", FromRef.literal(CHILD_TABLE));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null);

        artifacts.save(new PipelineResource(PIPELINE, null, List.of(SourceRef.spec(PARENT_SOURCE, true), SourceRef.spec(CHILD_SOURCE, true)),
                List.of(step), null,
                new ServeBlock.Inline(null, FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null));
        return artifacts;
    }

    private static SourceResource source(String id, String table) {
        return new SourceResource(id, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null);
    }

    private static DiscoveredSourceModel discovered(String connectionId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, "fake", 0L, new SourceModel(List.of(table)));
    }
}
