package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.math.BigDecimal;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/** The declarative MySQL seed supports integer and string columns. This case needs a fixed-scale decimal. */
class AJoinedDecimalPassesThroughAScriptToMysqlIT {

    private static final String PIPELINE = "joined_decimal";

    @BeforeAll
    static void requireMysql() {
        DockerGate.require();
        RealConnectorGate.require("mysql");
    }

    @Test
    void theJoinedDecimalKeepsItsTypeThroughAPassThroughScript() throws Exception {
        var source = SharedMySql.settings("joined_decimal_source");
        var target = SharedMySql.settings("joined_decimal_target");
        try (var connection = SharedMySql.connect(source); var sql = connection.createStatement()) {
            sql.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(32))");
            sql.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(18,4))");
            sql.execute("INSERT INTO customers VALUES (1, 'Ada')");
            sql.execute("INSERT INTO orders VALUES (7, 1, 12345678.9010)");
        }

        try (var storage = PreparedNativeStore.create(); var server = InProcessServer.startPrepared(storage)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.discoverSchema("shop", "mysql", source);
            control.apply(Map.of(
                    "shop.tap.yml", mysqlSource("shop", source) + "mode: cdc\ntables: [orders, customers]\n",
                    "warehouse.tap.yml", mysqlSource("warehouse", target),
                    "pipeline.tap.yml", """
                            version: tapstate/v1
                            kind: pipeline
                            id: joined_decimal
                            source: shop
                            settings: { read_mode: snapshot_only }
                            transforms:
                              - id: widened
                                type: join
                                from: { o: orders, c: customers }
                                engine: builtin
                                sql: |
                                  SELECT o.id, o.amount, c.name AS customer_name
                                  FROM o JOIN c ON o.customer_id = c.id
                              - id: forwarded
                                type: js
                                from: widened
                                script: 'function process(record, ctx) { return record; }'
                            serve:
                              from: forwarded
                              sync: [ { source: warehouse } ]
                            """));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            PipelineState state = Await.answered("the joined MySQL snapshot to finish", () ->
                    control.state(PIPELINE).filter(value -> value == PipelineState.COMPLETED || value == PipelineState.FAILED));
            assertThat(state).as("pipeline failure code: %s", control.failureCode(PIPELINE))
                    .isEqualTo(PipelineState.COMPLETED);

            try (var connection = SharedMySql.connect(target); var sql = connection.createStatement();
                    var rows = sql.executeQuery("SELECT id, amount, customer_name FROM orders")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getInt("id")).isEqualTo(7);
                assertThat(rows.getBigDecimal("amount")).isEqualTo(new BigDecimal("12345678.9010"));
                assertThat(rows.getString("customer_name")).isEqualTo("Ada");
                assertThat(rows.next()).isFalse();
            }
        }
    }

    private static String mysqlSource(String id, Map<String, Object> config) {
        return new Yaml().dump(Map.of("version", "tapstate/v1", "kind", "source",
                "id", id, "connector", "mysql", "config", config));
    }
}
