package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A join feeding a {@code view:} block, which is the shape the validator's own valid corpus writes.
 *
 * <p>What feeds a view is resolved by walking its from-reference to leaves, and a join used to be
 * walked through to the tables under it - so a view fed by one join counted as a view fed by two
 * tables and was refused as such. Measured before the fix: {@code actuation.view-fed-by-many-tables}
 * with {@code tables=orders, customers}. The validator accepted the pipeline and the builder would
 * not build it, which is the one disagreement between the two halves that nothing else looks for.
 *
 * <p>A join is one stream, and its identity is the target model registered for the step - so the
 * gate that refuses a view whose key is not the identity of its feed now has something true to
 * compare against, rather than two tables it was never fed by.
 */
class JoinViewTargetTest {

    @Test
    void aJoinFeedingAViewIsOneFeedAndBuilds() {
        assertThat(build(VIEW_PIPELINE, List.of("id"))).isTrue();
    }

    /**
     * The gate still bites, on the thing it is for. A view carries one key column, so a join whose
     * fact key is composite has an identity the view cannot converge on - two orders differing only
     * in the column the view leaves out would take turns overwriting one document.
     */
    @Test
    void aViewKeyedOnLessThanTheJoinsIdentityIsStillRefused() {
        assertThatThrownBy(() -> build(VIEW_PIPELINE, List.of("region", "id")))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("actuation.view-key-not-feed-identity"));
    }

    private static boolean build(String pipeline, List<String> factKey) {
        InMemoryStorePort store = validated(ORDERS_SRC, CUSTOMERS_SRC, STATE_STORE, pipeline);
        discovered(store, "orders_src", "orders", factKey,
                new SourceField("id", "bigint"), new SourceField("region", "varchar"),
                new SourceField("customer_ref", "bigint"));
        discovered(store, "customers_src", "customers", List.of("id"),
                new SourceField("id", "bigint"), new SourceField("cust_ref", "bigint"),
                new SourceField("name", "varchar"));
        new StoreBackedDagSource(store, discarding()).dagFor("cust_stats");
        return true;
    }

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

    private static final String STATE_STORE = """
            version: tapstate/v1
            kind: source
            id: views
            connector: mongodb
            config: { uri: u }
            """;

    private static final String VIEW_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: cust_stats
            source: [ orders_src, customers_src ]
            transforms:
              - id: cust_orders
                type: join
                from: { o: orders, c: customers }
                engine: builtin
                sql: |
                  SELECT o.id AS order_id, o.region AS region, c.name AS customer_name
                  FROM o JOIN c ON o.customer_ref = c.cust_ref
            view:
              id: cust_stats
              from: cust_orders
              primary_key: order_id
              storage: { warm: { collection: cust_stats } }
            """;

    private static StoreBackedDagSource.SinkWriterBinder discarding() {
        return new StoreBackedDagSource.SinkWriterBinder() {
            @Override
            public SupplierEx<? extends SinkWriter> bind(String connectorId,
                    Map<String, Object> settings, WriteMode writeMode, DdlPolicy ddl,
                    TargetTable target) {
                return (SupplierEx<SinkWriter>) () -> null;
            }
        };
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
