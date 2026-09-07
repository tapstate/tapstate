package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What key a join's output is written under.
 *
 * <p>A join publishes under the id of the step that produced it, the way a nest does, so nothing that
 * resolves a target per source table says anything about it. Left unregistered the sink resolves no
 * model for that stream at all and writes it through a bare descriptor: no columns, no key, no
 * indexes. An upsert with nothing to match on is the failure this whole file is about, and it is
 * silent -- the write succeeds, the target fills, and every count looks right.
 *
 * <p><b>The key is the fact table's primary key, under the names the projection publishes it by.</b>
 * The driver publishes exactly one row per fact row, so the fact row's key already identifies the
 * result row; a dimension's key adds no discrimination and, where the dimension is outer-joined, adds
 * a null that a SQL target silently duplicates on rather than converges on.
 */
class JoinOutputTargetTest {

    @Test
    void theJoinsOutputReachesTheSinkKeyedOnTheFactTablesPublishedKey() {
        Map<String, TargetTable> targets = bind(JOIN_PIPELINE);

        // The join's own stream, and only it: the source tables feed the join rather than the sink,
        // so handing the sink their models would key the wide rows on a table they are not.
        assertThat(targets).containsOnlyKeys("widen");
        assertThat(keyOf(targets.get("widen"))).containsExactly("order_id");
        assertThat(namesOf(targets.get("widen"))).containsExactly("order_id", "customer_name");
    }

    /** The widened rows are the fact table's, so they land in the fact table's name unless renamed. */
    @Test
    void theTargetTableIsTheFactTablesNameNotTheStepsAlias() {
        assertThat(bind(JOIN_PIPELINE).get("widen").name()).isEqualTo("orders");
    }

    /**
     * The key column carries the type the fact table declared for it, so the sink can create the
     * column. A column no source declares - one computed by an expression - carries none and is left
     * for the connector to infer, which is what the view path already does with a type it cannot
     * resolve.
     */
    @Test
    void aPublishedSourceColumnCarriesItsDeclaredTypeAndAComputedOneCarriesNone() {
        Map<String, TargetTable> targets = bind(COMPUTED_COLUMN_PIPELINE);

        assertThat(typeOf(targets.get("widen"), "order_id")).isEqualTo("bigint");
        assertThat(typeOf(targets.get("widen"), "shout")).isNull();
    }

    /**
     * The fact key is not in the SELECT. Nothing then identifies a result row: every fact row would
     * write the same target row and the target would hold one row however many orders there are, with
     * no error anywhere. Refused with a code naming the column to add.
     */
    @Test
    void aJoinThatDoesNotPublishItsFactKeyIsRefusedWithACode() {
        assertThatThrownBy(() -> bind(UNKEYED_PIPELINE))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.join-output-key-not-published"));
    }

    /**
     * <b>The discriminating case.</b> The fact key column is named in the SELECT, but through an
     * expression: {@code UPPER(o.id) AS order_id}. A reader matching on the column being mentioned
     * anywhere would accept it, and the result is a key that is a function of the real key - two
     * different orders whose ids differ only in case collapse onto one target row, silently. Only a
     * bare column reference publishes the key.
     */
    @Test
    void aFactKeyReachingTheOutputOnlyThroughAnExpressionDoesNotCountAsPublished() {
        assertThatThrownBy(() -> bind(EXPRESSION_KEY_PIPELINE))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.join-output-key-not-published"));
    }

    /** A composite fact key has to be published whole; publishing half of it identifies nothing. */
    @Test
    void aCompositeFactKeyIsPublishedWholeOrRefused() {
        Map<String, TargetTable> whole = bind(COMPOSITE_KEY_PIPELINE, List.of("region", "id"));
        assertThat(keyOf(whole.get("widen"))).containsExactly("region", "order_id");

        assertThatThrownBy(() -> bind(HALF_COMPOSITE_KEY_PIPELINE, List.of("region", "id")))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.join-output-key-not-published"));
    }

    /**
     * A dimension's key is published like any other column and stays out of the target key. Measured
     * on postgres 16 and mysql 8.0: under a unique index over a key column holding null - which is
     * what an outer-joined dimension publishes for a fact row it did not match - the same logical row
     * written twice leaves two rows rather than one, because SQL compares nulls as distinct. The
     * duplication is unbounded (one row per republication) and reads as ordinary output.
     */
    @Test
    void aPublishedDimensionKeyIsAnOrdinaryColumnRatherThanPartOfTheKey() {
        Map<String, TargetTable> targets = bind(DIMENSION_KEY_PUBLISHED_PIPELINE);

        assertThat(keyOf(targets.get("widen"))).containsExactly("order_id");
        assertThat(namesOf(targets.get("widen"))).contains("customer_id");
    }

    /**
     * The other spelling. {@code FROM orders o} makes the plan call the source by its alias while the
     * table behind it is {@code orders}; {@code FROM o} makes both the alias. The key is resolved
     * against the name the plan uses and the target is named after the real table, so the two have to
     * be looked up separately - conflating them names the target after an alias, or fails to find the
     * key at all and refuses a perfectly ordinary query.
     */
    @Test
    void theAliasedFromSpellingResolvesTheSameKeyAndTheSameTable() {
        Map<String, TargetTable> targets = bind(ALIASED_FROM_PIPELINE);

        assertThat(keyOf(targets.get("widen"))).containsExactly("order_id");
        assertThat(targets.get("widen").name()).isEqualTo("orders");
    }

    /**
     * An append never matches a write to an existing row, so it has no use for a key - the reading the
     * source-side key rule already takes, and the two must not disagree about what a keyless write
     * means. Measured while the refusal was unconditional: this pipeline was refused by name for a
     * key it never needed. The upsert half of the pair is what keeps the refusal from being switched
     * off altogether; without it, "no key needed" would read the same as "the check is gone".
     */
    @Test
    void anAppendNeedsNoFactKeyWhileAnUpsertStillDoes() {
        Map<String, TargetTable> appended = bind(unkeyedInto("append"));
        assertThat(keyOf(appended.get("widen"))).isEmpty();
        assertThat(namesOf(appended.get("widen"))).containsExactly("total", "customer_name");

        assertThatThrownBy(() -> bind(unkeyedInto("upsert")))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.join-output-key-not-published"));
    }

    /** The same, with the mode left out entirely - which is how the upsert default is written. */
    @Test
    void anAbsentWriteModeIsTheUpsertItDefaultsToAndStillNeedsTheKey() {
        assertThatThrownBy(() -> bind(UNKEYED_PIPELINE))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.join-output-key-not-published"));
    }

    // ---- fixtures ----------------------------------------------------------------------

    private static final String ORDERS_SRC = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String CUSTOMERS_SRC = """
            version: tapstate/v1
            kind: source
            id: customers_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ customers ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: orders_dest
            connector: mongodb
            config: { uri: u }
            """;

    private static String pipeline(String select) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: wide
                source: [ orders_src, customers_src ]
                transforms:
                  - id: widen
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      %s
                      FROM o LEFT JOIN c ON o.customer_ref = c.cust_ref
                serve:
                  from: widen
                  sync: [ { id: sync_1, source: orders_dest } ]
                """.formatted(select);
    }

    /** {@code FROM orders o} rather than {@code FROM o}: the alias and the table differ. */
    private static final String ALIASED_FROM_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: wide
            source: [ orders_src, customers_src ]
            transforms:
              - id: widen
                type: join
                from: { o: orders, c: customers }
                engine: builtin
                sql: |
                  SELECT o.id AS order_id, c.name AS customer_name
                  FROM orders o LEFT JOIN customers c ON o.customer_ref = c.cust_ref
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    /** A projection with no fact key in it, written into a sink with the given write mode. */
    private static String unkeyedInto(String writeMode) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: wide
                source: [ orders_src, customers_src ]
                transforms:
                  - id: widen
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      SELECT o.region AS total, c.name AS customer_name
                      FROM o LEFT JOIN c ON o.customer_ref = c.cust_ref
                serve:
                  from: widen
                  sync: [ { id: sync_1, source: orders_dest, write_mode: %s } ]
                """.formatted(writeMode);
    }

    private static final String JOIN_PIPELINE =
            pipeline("SELECT o.id AS order_id, c.name AS customer_name");

    private static final String COMPUTED_COLUMN_PIPELINE =
            pipeline("SELECT o.id AS order_id, UPPER(c.name) AS shout");

    private static final String UNKEYED_PIPELINE =
            pipeline("SELECT o.customer_ref AS ref_out, c.name AS customer_name");

    private static final String EXPRESSION_KEY_PIPELINE =
            pipeline("SELECT UPPER(o.id) AS order_id, c.name AS customer_name");

    private static final String DIMENSION_KEY_PUBLISHED_PIPELINE =
            pipeline("SELECT o.id AS order_id, c.id AS customer_id, c.name AS customer_name");

    private static final String COMPOSITE_KEY_PIPELINE =
            pipeline("SELECT o.region AS region, o.id AS order_id, c.name AS customer_name");

    private static final String HALF_COMPOSITE_KEY_PIPELINE =
            pipeline("SELECT o.id AS order_id, c.name AS customer_name");

    /** Builds the pipeline and returns the target models the serve sink was bound with. */
    private static Map<String, TargetTable> bind(String pipeline) {
        return bind(pipeline, List.of("id"));
    }

    /** The same, with the fact table declaring {@code factKey} as its primary key. */
    private static Map<String, TargetTable> bind(String pipeline, List<String> factKey) {
        InMemoryStorePort store = validated(ORDERS_SRC, CUSTOMERS_SRC, TARGET, pipeline);
        discovered(store, "orders_src", "orders", factKey,
                field("id", "bigint"), field("region", "varchar"), field("customer_ref", "bigint"));
        discovered(store, "customers_src", "customers", List.of("id"),
                field("id", "bigint"), field("cust_ref", "bigint"), field("name", "varchar"));

        Map<String, Map<String, TargetTable>> captured = new LinkedHashMap<>();
        new StoreBackedDagSource(store, capturing(captured)).dagFor("wide");
        return captured.getOrDefault("mongodb", Map.of());
    }

    private static StoreBackedDagSource.SinkWriterBinder capturing(
            Map<String, Map<String, TargetTable>> into) {
        return new StoreBackedDagSource.SinkWriterBinder() {
            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId,
                    Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                    TargetTable target) {
                return (SupplierEx<SinkWriter>) () -> null;
            }

            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId,
                    Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                    Map<String, TargetTable> targets) {
                into.put(connectorId, targets);
                return (SupplierEx<SinkWriter>) () -> null;
            }
        };
    }

    private static List<String> keyOf(TargetTable target) {
        List<String> key = new ArrayList<>();
        for (TargetField field : target.fields()) {
            if (field.primaryKey()) {
                key.add(field.name());
            }
        }
        return key;
    }

    private static List<String> namesOf(TargetTable target) {
        return target.fields().stream().map(TargetField::name).toList();
    }

    private static String typeOf(TargetTable target, String name) {
        for (TargetField field : target.fields()) {
            if (field.name().equals(name)) {
                return field.type();
            }
        }
        throw new IllegalStateException("no field named '" + name + "'");
    }

    private static SourceField field(String name, String type) {
        return new SourceField(name, type);
    }

    private static InMemoryStorePort validated(String... documents) {
        DslParser parser = new DslParser();
        List<Resource> resources = new ArrayList<>();
        for (String document : documents) {
            resources.add(parser.parse(document));
        }
        Workspace.of(resources);
        InMemoryStorePort store = new InMemoryStorePort();
        resources.forEach(store.artifacts()::save);
        OpenRingGenerations.forSources(store, "orders_src", "customers_src");
        return store;
    }

    private static void discovered(InMemoryStorePort store, String connectionId, String table,
            List<String> key, SourceField... fields) {
        store.schemas().save(new DiscoveredSourceModel(connectionId, "mysql", 0L,
                new SourceModel(List.of(new SourceTable(table, List.of(fields), key, null)))));
    }
}
