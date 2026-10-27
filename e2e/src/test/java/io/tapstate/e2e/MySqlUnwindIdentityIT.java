package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.adapters.pdk.PdkTargetIdentityPlanner;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A native duplicate-key upsert must never choose a conflicting business index. */
@RequiresDocker
class MySqlUnwindIdentityIT {
    @Test
    void exactNumericKeysWorkButBusinessPrefixAndCollatedConstraintsBlock() throws Exception {
        RealConnectorGate.require("mysql");
        Path jar = ConnectorJars.pathFor("mysql");
        var inspected = new ConnectorIntrospector().introspect(List.of(jar));
        ConnectorRef connector = new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(), null, inspected.spec());
        var settings = SharedMySql.settings("unwind_" + UUID.randomUUID().toString().replace("-", ""));
        TargetTable target = new TargetTable("expanded", List.of(
                new TargetField("id", "bigint", true, TapstateType.INT64),
                new TargetField("position", "bigint", true, TapstateType.INT64),
                new TargetField("value", "varchar(80)", false, TapstateType.STRING)),
                List.of(new TargetIndex(List.of("id", "position"), true)),
                new UnwindIdentity(List.of("id"), "_unwind_source_id"));
        var config = new SinkConfig("mysql", settings, WriteMode.UPSERT, DdlPolicy.FAIL, target,
                null, OnFullLoad.APPEND, false);
        var planner = new PdkTargetIdentityPlanner(id -> connector);
        try (var db = SharedMySql.connect(settings); var sql = db.createStatement()) {
            sql.execute("CREATE TABLE expanded (id BIGINT AUTO_INCREMENT PRIMARY KEY, _unwind_source_id BIGINT NOT NULL, "
                    + "position BIGINT NOT NULL, value VARCHAR(80), UNIQUE (_unwind_source_id, position))");
            try {
                assertThat(planner.assess(config, Map.of("expanded", target)).get("expanded").status()).isEqualTo("automatic");
                try (var writer = new PdkSinkPort(id -> connector).open(config)) {
                    var rows = List.of(Envelope.insert(1L, "expanded", Map.of("id", 700L, "position", 0L, "value", "same-a"), null),
                            Envelope.insert(1L, "expanded", Map.of("id", 700L, "position", 1L, "value", "same-b"), null));
                    writer.write(rows).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    writer.write(rows).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    // A bad later row must reject the whole batch before even its valid prefix is written.
                    var mismatched = List.of(
                            Envelope.insert(2L, "expanded", Map.of("id", 702L, "position", 0L, "value", "valid-prefix"), null),
                            Envelope.insert(2L, "expanded", Map.of("id", "01", "position", 0L, "value", "first-string"), null),
                            Envelope.insert(2L, "expanded", Map.of("id", "1", "position", 0L, "value", "second-string"), null));
                    assertThatThrownBy(() -> writer.write(mismatched).toCompletableFuture().get(30, TimeUnit.SECONDS))
                            .hasRootCauseInstanceOf(io.tapstate.core.common.TapstateException.class)
                            .rootCause().satisfies(error -> {
                                var coded = (io.tapstate.core.common.TapstateException) error;
                                assertThat(coded.code().code()).isEqualTo("connector.target-identity-conflict");
                                assertThat(coded.args()).containsEntry("reason", "key-representation-unsupported");
                            });
                    try (var count = sql.executeQuery("SELECT COUNT(*) FROM expanded")) {
                        assertThat(count.next()).isTrue();
                        assertThat(count.getInt(1)).isEqualTo(2);
                    }
                }
                try (var rows = sql.executeQuery("SELECT COUNT(*), COUNT(DISTINCT id) FROM expanded WHERE _unwind_source_id = 700")) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getInt(1)).isEqualTo(2);
                    assertThat(rows.getInt(2)).isEqualTo(2);
                }
                var stringFields = new java.util.ArrayList<>(target.fields());
                stringFields.set(0, new TargetField("id", "varchar(80)", true, TapstateType.STRING));
                var stringSource = new TargetTable(target.name(), stringFields, target.indexes(), target.unwindIdentity());
                assertThat(planner.assess(config, Map.of("expanded", stringSource)).get("expanded").reason())
                        .isEqualTo("key-representation-unsupported");
                sql.execute("DELETE FROM expanded");
                sql.execute("CREATE UNIQUE INDEX business_prefix ON expanded(value(4))");
                assertThat(planner.assess(config, Map.of("expanded", target)).get("expanded").reason()).isEqualTo("other-unique-constraint");
                sql.execute("DROP INDEX business_prefix ON expanded");
                sql.execute("ALTER TABLE expanded MODIFY _unwind_source_id VARCHAR(80) COLLATE utf8mb4_general_ci NOT NULL");
                assertThat(planner.assess(config, Map.of("expanded", target)).get("expanded").reason()).isEqualTo("other-unique-constraint");
                sql.execute("DROP TABLE expanded");
                sql.execute("CREATE TABLE expanded (id BIGINT NOT NULL, position BIGINT NOT NULL, value VARCHAR(80), "
                        + "PRIMARY KEY(id, position), UNIQUE(id))");
                assertThat(planner.assess(config, Map.of("expanded", target)).get("expanded").reason()).isEqualTo("other-unique-constraint");
            } finally {
                sql.execute("DROP TABLE IF EXISTS expanded");
            }
        }
    }
}
