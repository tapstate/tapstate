package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.runtime.engine.validation.ValidationOrderedGroups;
import io.tapstate.runtime.engine.validation.ValidationOrderedMerge;
import io.tapstate.spi.store.ValidationRowCursor;
import io.tapstate.testsupport.RequiresDocker;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real independent JDBC/Mongo reads; no pipeline execution, strict window or general admission proof. */
@RequiresDocker
class ValidationJdbcMongoReadIT {
    private static final String DATABASE = "validation_reads";
    private static final String PASSWORD = "validation-fixture-only";
    private static final long EXACT = 9_007_199_254_740_993L;
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(8192, 100, 12);

    @Test @Timeout(120)
    void mysqlNativeReadPreservesDecimalsAndCompletesBothMergeTails() throws Exception { verify(true); }

    @Test @Timeout(120)
    void postgresNativeReadPreservesDecimalsAndCompletesBothMergeTails() throws Exception { verify(false); }

    private void verify(boolean mysql) throws Exception {
        String kind = mysql ? "mysql" : "postgres";
        var sourceRef = artifact(kind);
        var targetRef = artifact("mongodb");
        int port = mysql ? 3306 : 5432;
        try (var jdbc = new GenericContainer<>(DockerImageName.parse(mysql ? "mysql:8.0" : "postgres:16"))
                    .withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75));
             var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            if (mysql) {
                jdbc.withEnv("MYSQL_ROOT_PASSWORD", PASSWORD).withEnv("MYSQL_DATABASE", DATABASE)
                        .withEnv("MYSQL_USER", "validation").withEnv("MYSQL_PASSWORD", PASSWORD)
                        .withCommand("--innodb-buffer-pool-size=64M", "--max-connections=20")
                        .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n", 1));
            } else {
                jdbc.withEnv("POSTGRES_PASSWORD", PASSWORD).withEnv("POSTGRES_USER", "validation")
                        .withEnv("POSTGRES_DB", DATABASE)
                        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
            }
            jdbc.start();
            mongo.start();
            sql(jdbc, mysql, "CREATE TABLE source_records (source_key BIGINT NOT NULL, amount DECIMAL(30,9), "
                    + "fractional DECIMAL(30,9), label VARCHAR(40), nullable VARCHAR(40));"
                    + "INSERT INTO source_records VALUES "
                    + "(5," + EXACT + ",123456789.123456789,' 001 ',NULL),"
                    + "(1," + EXACT + ",123456789.123456789,' 001 ',NULL),"
                    + "(4," + EXACT + ",123456789.123456789,' 001 ',NULL),"
                    + "(2," + EXACT + ",123456789.123456789,' 001 ',NULL),"
                    + "(3," + EXACT + ",123456789.123456789,' 001 ',NULL);");
            sql(jdbc, mysql, "ALTER TABLE source_records ADD payload " + (mysql ? "VARBINARY(8)" : "BYTEA")
                    + "; ALTER TABLE source_records ADD local_time " + (mysql ? "DATETIME(6)" : "TIMESTAMP(6)")
                    + "; UPDATE source_records SET local_time='2026-10-21 01:02:03.123456',payload="
                    + (mysql ? "X'0001FF'" : "decode('0001FF','hex')") + ";");
            if (mysql) addJson(jdbc, true);
            var sourceSettings = new LinkedHashMap<String, Object>();
            sourceSettings.put("host", jdbc.getHost());
            sourceSettings.put("port", jdbc.getMappedPort(port));
            sourceSettings.put("database", DATABASE);
            sourceSettings.put(mysql ? "username" : "user", "validation");
            sourceSettings.put("password", PASSWORD);
            sourceSettings.put("timezone", "+00:00");
            sourceSettings.put("oldVersionTimezone", false);
            if (mysql) sourceSettings.put("addtionalString", "useSSL=false&allowPublicKeyRetrieval=true");
            else sourceSettings.put("schema", "public");
            String uri = mongo.getReplicaSetUrl(DATABASE);
            try (var observer = MongoClients.create(uri)) {
                observer.getDatabase(DATABASE).getCollection("target_records").insertMany(
                        List.of(6L, 4L, 2L, 5L, 1L).stream().map(id -> new Document("target_key", id)
                                .append("amount", id == 2 ? EXACT - 1 : EXACT)
                                .append("label", id == 5 ? "001" : " 001 ").append("nullable", null)).toList());
            }
            var facts = new ArrayList<String>();
            var checks = new ArrayList<String>();
            try (var queryIo = Executors.newFixedThreadPool(2); var decodeIo = Executors.newFixedThreadPool(2);
                 var source = scan(kind, sourceRef, sourceSettings, "source_records", "source_key", queryIo, decodeIo);
                 var target = scan("mongodb", targetRef, Map.of("uri", uri, "database", DATABASE, "isUri", true),
                         "target_records", "target_key", queryIo, decodeIo);
                 var merge = new ValidationOrderedMerge(
                         new ValidationOrderedGroups("source", source, row -> key(row, "source_key", kind), LIMITS.bytes()),
                         new ValidationOrderedGroups("target", target, row -> key(row, "target_key", "mongodb"), LIMITS.bytes()))) {
                while (true) {
                    var next = merge.next();
                    if (next.isEmpty()) break;
                    try (var pair = next.orElseThrow()) {
                        facts.add(pair.state() + ":" + pair.source().map(group -> group.occurrences()).orElse(0L)
                                + ":" + pair.target().map(group -> group.occurrences()).orElse(0L));
                        pair.source().ifPresent(group -> {
                            var values = group.sampleValues().fields();
                            assertThat(value(values.get("amount"))).isEqualTo(new BigDecimal(EXACT + ".000000000"));
                            assertThat(value(values.get("fractional"))).isEqualTo(new BigDecimal("123456789.123456789"));
                            assertThat(value(values.get("label"))).isEqualTo(" 001 ");
                            assertThat(values).containsKey("nullable").doesNotContainKey("absent");
                            assertThat(values.get("nullable")).isNull();
                            var binary = (ValidationValues.Binary)value(values.get("payload"));
                            assertThat(binary.bytes()).containsExactly((byte)0, (byte)1, (byte)-1);
                            assertThat(binary.subtype()).isEqualTo(-1);
                            assertThat(value(values.get("local_time"))).isEqualTo(LocalDateTime.parse("2026-10-21T01:02:03.123456"));
                            if (mysql) {
                                var json = (Map<?, ?>)value(values.get("body"));
                                assertThat(json.get("n")).isEqualTo(new BigInteger("9007199254740993"));
                                assertThat(json.get("items")).isEqualTo(Arrays.asList(BigInteger.ONE, null, BigInteger.valueOf(3)));
                                assertThat(json.get("text")).isEqualTo(" 001 ");
                            }
                        });
                        if (pair.source().isPresent() && pair.target().isPresent()) {
                            var left = pair.source().orElseThrow().sampleValues().fields();
                            var right = pair.target().orElseThrow().sampleValues().fields();
                            // Only these controlled common payload fields are compared. This does not
                            // implement general key mapping or an exemption for generated target fields.
                            var difference = ValidationValueComparison.compare(payload(left), payload(right));
                            checks.add(value(left.get("source_key")) + ":"
                                    + difference.map(diff -> diff.path() + ":" + diff.kind()).orElse("EQUAL"));
                        }
                    }
                }
            }
            assertThat(facts).containsExactly("MATCHED:1:1", "MATCHED:1:1", "SOURCE_ONLY:1:0",
                    "MATCHED:1:1", "MATCHED:1:1", "TARGET_ONLY:0:1");
            assertThat(checks).containsExactly("1:EQUAL", "2:[amount]:VALUE", "4:EQUAL", "5:[label]:VALUE");
            // These are fresh read-only sessions; no CDC slot or source mutation was needed.
            assertThat(sql(jdbc, mysql, "SELECT COUNT(*) FROM source_records;").trim()).isEqualTo("5");
            if (!mysql) {
                assertThat(sql(jdbc, false, "SELECT COUNT(*) FROM pg_replication_slots;").trim()).isEqualTo("0");
                // The retained PGobject custom codec explicitly returns text. A field declared as JSON
                // must not override that category and parse its output again to manufacture agreement.
                addJson(jdbc, false);
                try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor();
                     var cursor = scan(kind, sourceRef, sourceSettings, "source_records", "source_key", queryIo, decodeIo,
                             PdkValidationRows.Shape.TEXT)) {
                    int rows = 0;
                    while (true) {
                        var next = cursor.next();
                        if (next.isEmpty()) break;
                        try (var row = next.orElseThrow()) {
                            var body = (ValidationValues.Typed)row.values().fields().get("body");
                            assertThat(body.logicalType()).isEqualTo("text");
                            assertThat(body.conversionOrigin()).isEqualTo("custom");
                            assertThat(body.value()).isEqualTo("{\"n\":9007199254740993,\"items\":[1,null,3],\"text\":\" 001 \"}");
                            rows++;
                        }
                    }
                    assertThat(rows).isEqualTo(5);
                }
                try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor();
                     var cursor = scan(kind, sourceRef, sourceSettings, "source_records", "source_key", queryIo, decodeIo)) {
                    assertThatThrownBy(cursor::next).isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE);
                        assertThat(failure.args()).containsEntry("connector", "postgres").containsEntry("field", "body");
                    });
                }
            }
        }
    }

    private static void addJson(GenericContainer<?> jdbc, boolean mysql) throws Exception {
        sql(jdbc, mysql, "ALTER TABLE source_records ADD body JSON; UPDATE source_records SET "
                + "body='{\"n\":9007199254740993,\"items\":[1,null,3],\"text\":\" 001 \"}';");
    }

    private static ConnectorRef artifact(String kind) {
        String configured = System.getProperty("tapstate.pdk.it." + kind + "-jar");
        assumeTrue(configured != null && !configured.isBlank(), "a real " + kind + " connector jar is required");
        var paths = List.of(Path.of(configured));
        var artifact = new ConnectorIntrospector().introspect(paths);
        return new ConnectorRef(paths, artifact.className(), artifact.pdkApiVersion(), null, artifact.spec());
    }

    private static String sql(GenericContainer<?> jdbc, boolean mysql, String statement) throws Exception {
        var result = mysql
                ? jdbc.execInContainer("mysql", "--protocol=TCP", "--host=127.0.0.1", "--user=validation", "--password=" + PASSWORD, "--batch", "--skip-column-names", DATABASE, "-e", statement)
                : jdbc.execInContainer("psql", "-U", "validation", "-d", DATABASE, "-tAc", statement);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
        return result.getStdout();
    }

    private static ValidationRowCursor scan(String kind, ConnectorRef ref, Map<String, Object> settings,
            String table, String key, ExecutorService queryIo, ExecutorService decodeIo) {
        return scan(kind, ref, settings, table, key, queryIo, decodeIo, PdkValidationRows.Shape.JSON);
    }

    private static ValidationRowCursor scan(String kind, ConnectorRef ref, Map<String, Object> settings,
            String table, String key, ExecutorService queryIo, ExecutorService decodeIo, PdkValidationRows.Shape bodyShape) {
        return PdkValidationSession.scan(kind, ref, settings, "retained-" + kind + "-artifact",
                new PdkValidationSession.Scan(table, List.of(key), 2),
                (model, codecs) -> new PdkValidationRows(model, codecs, path -> {
                    if (path.size() == 1 && List.of(key, "amount", "fractional").contains(path.getFirst()))
                        return PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER);
                    if (path.equals(List.of("local_time")))
                        return PdkValidationRows.Rule.of(PdkValidationRows.Shape.LOCAL_DATE_TIME);
                    if (path.equals(List.of("body"))) return PdkValidationRows.Rule.of(bodyShape);
                    return PdkValidationRows.Rule.AUTO;
                }, LIMITS), () -> { }, queryIo, decodeIo, 1, LIMITS);
    }

    private static ValidationOrderedGroups.Key key(ValidationValues.Row row, String field, String kind) {
        var actual = value(row.fields().get(field));
        assertThat(actual).isInstanceOf(Long.class);
        var key = new ValidationKey(List.of(ValidationKey.Part.number((Long)actual)));
        return new ValidationOrderedGroups.Key(key, new ValidationPhysicalIdentity(kind + "-fixture", DATABASE,
                kind.equals("postgres") ? "public" : null,
                kind.equals("mongodb") ? "target_records" : "source_records", List.of(List.of(field)), key));
    }

    private static ValidationValues.Row payload(Map<String, Object> row) {
        var values = new LinkedHashMap<String, Object>();
        for (String field : List.of("amount", "label", "nullable")) values.put(field, row.get(field));
        return ValidationValues.ownRow(values, LIMITS);
    }

    private static Object value(Object value) { return ((ValidationValues.Typed)value).value(); }
}
