package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.adapters.pdk.PdkTargetIdentityPlanner;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Generated relational identities obey the same preservation and logical matching rule. */
@RequiresDocker
class PostgresUnwindIdentityIT {
    @Test
    void generatedIdentityKeepsSiblingRowsAndReplaysWithoutDuplicates() throws Exception {
        RealConnectorGate.require("postgres");
        Path jar;
        try (var paths = Files.list(Path.of(System.getProperty("tapstate.e2e.connectors-dir")))) {
            jar = paths.filter(path -> path.getFileName().toString().startsWith("postgres-connector")
                    && path.toString().endsWith(".jar")).findFirst().orElseThrow();
        }
        var inspected = new ConnectorIntrospector().introspect(List.of(jar));
        ConnectorRef connector = new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(), null, inspected.spec());
        var settings = SharedPostgres.settings("unwind_" + UUID.randomUUID().toString().replace("-", ""));
        TargetTable target = new TargetTable("expanded", List.of(
                new TargetField("id", "bigint", true, TapstateType.INT64),
                new TargetField("position", "bigint", true, TapstateType.INT64),
                new TargetField("value", "text", false, TapstateType.STRING)),
                List.of(new TargetIndex(List.of("id", "position"), true)),
                new UnwindIdentity(List.of("id"), "_unwind_source_id"));
        var connectorSettings = new java.util.LinkedHashMap<>(settings);
        connectorSettings.put("user", connectorSettings.remove("username"));
        connectorSettings.put("schema", "public");
        var config = new SinkConfig("postgres", connectorSettings, WriteMode.UPSERT, DdlPolicy.FAIL, target,
                new PipelineNode("pg_unwind", "sink"), OnFullLoad.APPEND, false);
        try (var db = SharedPostgres.connect(settings); var sql = db.createStatement()) {
            sql.execute("CREATE TABLE expanded (id bigserial PRIMARY KEY, _unwind_source_id bigint NOT NULL, "
                    + "position bigint NOT NULL, value text, UNIQUE (_unwind_source_id, position))");
            try {
                var sink = new PdkSinkPort(id -> connector);
                long generated;
                try (var writer = sink.open(config)) {
                    writer.write(List.of(Envelope.insert(1L, "expanded", row(700, 0, "a"), null),
                            Envelope.insert(1L, "expanded", row(700, 1, "b"), null)))
                            .toCompletableFuture().get(30, TimeUnit.SECONDS);
                    try (var rows = sql.executeQuery("SELECT id, _unwind_source_id FROM expanded ORDER BY position")) {
                        assertThat(rows.next()).isTrue();
                        generated = rows.getLong(1);
                        assertThat(generated).isNotEqualTo(700);
                        assertThat(rows.getLong(2)).isEqualTo(700);
                        assertThat(rows.next()).isTrue();
                        assertThat(rows.getLong(1)).isNotEqualTo(generated);
                        assertThat(rows.next()).isFalse();
                    }
                }
                var changes = List.of(Envelope.update(2L, "expanded", row(700, 0, "a"), row(700, 0, "changed"), null),
                        Envelope.delete(3L, "expanded", row(700, 1, "b"), null));
                try (var writer = sink.open(config)) {
                    writer.write(changes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    writer.write(changes).toCompletableFuture().get(30, TimeUnit.SECONDS);
                }
                try (var rows = sql.executeQuery("SELECT id, value FROM expanded")) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getLong(1)).isEqualTo(generated);
                    assertThat(rows.getString(2)).isEqualTo("changed");
                    assertThat(rows.next()).isFalse();
                }
                sql.execute("CREATE UNIQUE INDEX business_partial ON expanded(value) WHERE value IS NOT NULL");
                var refused = new PdkTargetIdentityPlanner(id -> connector).assess(config, Map.of("expanded", target)).get("expanded");
                assertThat(refused.status()).isEqualTo("action-required");
                assertThat(refused.reason()).isEqualTo("other-unique-constraint");
            } finally {
                sql.execute("DROP TABLE expanded");
            }
        }
    }

    private static Map<String, Object> row(long parent, long position, String value) {
        return Map.of("id", parent, "position", position, "value", value);
    }
}
