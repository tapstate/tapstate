package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ValidationPredicateCompiler;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.ValidationMethod;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationNestSingleSource;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationPredicate;
import io.tapstate.core.validation.ValidationRelationRows;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationRootCheck;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationCheckEvent;
import io.tapstate.runtime.engine.validation.ValidationRelationReader;
import io.tapstate.runtime.engine.validation.ValidationRootReader;
import io.tapstate.runtime.engine.validation.ValidationStaticMap;
import java.time.Clock;
import java.util.Optional;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFieldCheck;
import java.time.Instant;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationPhysicalIdentity;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.runtime.engine.validation.ValidationOrderedGroups;
import io.tapstate.runtime.engine.validation.ValidationOrderedMerge;
import io.tapstate.spi.store.ValidationRowCursor;
import io.tapstate.spi.store.ValidationReadHandle;
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
import java.util.Set;
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
                        List.of(6L, 4L, 2L, 5L, 1L).stream().map(id -> {
                            var document = new Document("target_key", id).append("amount", id == 2 ? EXACT - 1 : EXACT)
                                    .append("label", id == 5 ? "001" : " 001 ").append("nullable", null);
                            if (id == 4) document.append("extra_business", 9);
                            return document;
                        }).toList());
                observer.getDatabase(DATABASE).getCollection("target_records").find().forEach(document ->
                        assertThat(document.get("_id")).isInstanceOf(org.bson.types.ObjectId.class));
            }
            var facts = new ArrayList<String>();
            var checks = new ArrayList<String>();
            var fullChecks = new ArrayList<String>();
            var projection = ValidationStaticMap.compile("fixed-output", new TransformBody.MapProjection(Map.of(
                    "target_key", FieldRule.rename("source_key"), "fractional", FieldRule.drop(), "payload", FieldRule.drop(),
                    "local_time", FieldRule.drop(), "body", FieldRule.drop())), LIMITS);
            Instant readStarted = Instant.now();
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
                            // Retain the small scalar control alongside the complete fixed projection.
                            var difference = ValidationValueComparison.compare(payload(left), payload(right));
                            checks.add(value(left.get("source_key")) + ":"
                                    + difference.map(diff -> diff.path() + ":" + diff.kind()).orElse("EQUAL"));
                            var sourceGroup = pair.source().orElseThrow();
                            var targetGroup = pair.target().orElseThrow();
                            var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                                    "task", "native-fields-" + kind, ValidationMode.FULL, "fixed-projection", 1, "rules"),
                                    new ValidationRoot("mapping", "output", sourceGroup.key().comparison(), List.of()), 1);
                            try (var root = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN,
                                    ValidationCheckEvent.Participation.SHOULD_EXIST, ValidationRootCheck.TargetPresence.PRESENT,
                                    new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 4))) {
                                Instant observed = Instant.now();
                                // Conservative diagnostic intervals cover session creation through group delivery.
                                root.read(ValidationRootCheck.Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "native-group-v1", readStarted, observed));
                                root.read(ValidationRootCheck.Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "native-group-v1", readStarted, observed));
                                // This fixture inserted without _id, confirmed native ObjectId values,
                                // maps no source _id and uses only source_key/target_key as identity.
                                var generated = ValidationFieldCheck.generatedId(observation.run(), "mapping", "output", true, false, false,
                                        id -> id instanceof ValidationValues.Typed typed && typed.logicalType().equals("text") && typed.value() instanceof String);
                                ValidationFieldCheck.check(observation, List.of(), projection.apply(sourceGroup.sampleValues()), targetGroup.sampleValues(),
                                        Optional.of(new ValidationRootCheck.SampledSide(sourceGroup.key().physical(), sourceGroup.sampleValues(), "source")),
                                        Optional.of(new ValidationRootCheck.SampledSide(targetGroup.key().physical(), targetGroup.sampleValues(), "target")),
                                        generated, String::equals, new ValidationValueComparison.TraversalLimits(LIMITS, 10000), root);
                                for (var check : ValidationRootCheck.Check.values()) root.checked(check);
                                var result = root.finish();
                                fullChecks.add(value(left.get("source_key")) + ":" + (result.observedAgreement() ? "EQUAL" : result.counts()));
                            }
                        }
                    }
                }
            }
            assertThat(facts).containsExactly("MATCHED:1:1", "MATCHED:1:1", "SOURCE_ONLY:1:0",
                    "MATCHED:1:1", "MATCHED:1:1", "TARGET_ONLY:0:1");
            assertThat(checks).containsExactly("1:EQUAL", "2:[amount]:VALUE", "4:EQUAL", "5:[label]:VALUE");
            assertThat(fullChecks).containsExactly("1:EQUAL", "2:{TARGET_VALUE=1}", "4:{TARGET_EXTRA=1}", "5:{TARGET_VALUE=1}");
            verifyLookups(jdbc, mysql, kind, sourceRef, sourceSettings, targetRef, uri);
            verifyRootReads(jdbc, mysql, kind, sourceRef, sourceSettings, targetRef, uri, projection);
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

    private static void verifyRootReads(GenericContainer<?> jdbc, boolean mysql, String kind, ConnectorRef sourceRef,
            Map<String, Object> sourceSettings, ConnectorRef targetRef, String uri, ValidationStaticMap projection) throws Exception {
        var targetSettings = Map.<String, Object>of("uri", uri, "database", DATABASE, "isUri", true);
        try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor()) {
            var actual = new ArrayList<String>();
            for (long id = 1; id <= 6; id++) {
                var result = rootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings, "source_records", "target_records", id,
                        projection, Optional.empty(), queryIo, decodeIo);
                actual.add(id + ":" + (result.observedAgreement() ? "EQUAL" : result.counts()));
                assertThat(result.reads()).hasSize(2);
            }
            assertThat(actual).containsExactly("1:EQUAL", "2:{TARGET_VALUE=1}", "3:{TARGET_MISSING=1}",
                    "4:{TARGET_EXTRA=1}", "5:{TARGET_VALUE=1}", "6:{TARGET_EXTRA=1}");
        }
        sql(jdbc, mysql, "CREATE TABLE root_check_records (source_key BIGINT NOT NULL, amount DECIMAL(30,0));"
                + "INSERT INTO root_check_records VALUES (7," + (EXACT - 1) + "),(7," + EXACT + "),"
                + "(8," + EXACT + "),(8," + EXACT + "),(9,NULL),(10," + EXACT + "),(11," + (EXACT - 1) + ");");
        try (var observer = MongoClients.create(uri)) {
            observer.getDatabase(DATABASE).getCollection("root_check_records").insertMany(
                    List.of(7L, 8L, 9L, 10L, 10L, 11L).stream().map(id -> new Document("target_key", id).append("price", EXACT)).toList());
        }
        var mapped = ValidationStaticMap.compile("eligible-root", new TransformBody.MapProjection(
                Map.of("target_key", FieldRule.rename("source_key"), "price", FieldRule.rename("amount"))), LIMITS);
        var predicate = ValidationPredicateCompiler.compile("eligible", "after.price >= 9007199254740993",
                new ValidationPredicate.Limits(LIMITS, 2000, false));
        try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor()) {
            var actual = new ArrayList<String>();
            for (long id = 7; id <= 11; id++) {
                var result = rootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings, "root_check_records", "root_check_records", id,
                        mapped, Optional.of(predicate), queryIo, decodeIo);
                actual.add(id + ":" + (result.observedAgreement() ? "EQUAL" : result.counts()));
                if (id == 8 || id == 9) {
                    assertThat(result.targetPresence()).isEqualTo(ValidationRootCheck.TargetPresence.NOT_READ);
                    assertThat(result.reads()).singleElement().satisfies(read -> assertThat(read.side()).isEqualTo(ValidationRootCheck.Side.SOURCE));
                }
            }
            assertThat(actual).containsExactly("7:EQUAL", "8:{SOURCE_DUPLICATE=1}", "9:{SOURCE_FIELD_INVALID=1}",
                    "10:{TARGET_DUPLICATE=1}", "11:{TARGET_EXTRA=1}");
        }
    }

    private static ValidationRootCheck rootRead(String kind, ConnectorRef sourceRef, Map<String, Object> sourceSettings,
            ConnectorRef targetRef, Map<String, Object> targetSettings, String sourceTable, String targetTable, long id,
            ValidationStaticMap projection, Optional<ValidationPredicate> predicate, ExecutorService queryIo, ExecutorService decodeIo) throws Exception {
        var logical = new ValidationKey(List.of(ValidationKey.Part.number(id)));
        var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "native-roots-" + kind, ValidationMode.FULL, "fixed-projection", 1, "rules"),
                new ValidationRoot("mapping", "output", logical, List.of()), 1);
        var generated = ValidationFieldCheck.generatedId(observation.run(), "mapping", "output", true, false, false,
                value -> value instanceof ValidationValues.Typed typed && typed.logicalType().equals("text") && typed.value() instanceof String);
        var handles = new ArrayList<ValidationReadHandle>();
        var checked = ValidationRootReader.read(observation, ValidationMethod.FULL_COLUMN,
                new ValidationRootReader.Query("source", "native-root-v1", () -> {
                    var handle = rootLookup(kind, sourceRef, sourceSettings, sourceTable, "source_key", logical, queryIo, decodeIo);
                    handles.add(handle); return handle;
                }),
                new ValidationRootReader.Query("target", "native-root-v1", () -> {
                    var handle = rootLookup("mongodb", targetRef, targetSettings, targetTable, "target_key", logical, queryIo, decodeIo);
                    handles.add(handle); return handle;
                }),
                new ValidationRelationRows.Limits(65536, LIMITS), new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 4),
                Clock.systemUTC(), () -> {}, original -> {
                    var working = projection.apply(original);
                    var evaluated = predicate.map(rule -> rule.evaluate(working)).orElse(new ValidationPredicate.Result(true, null));
                    var disposition = evaluated.problem() != null ? ValidationRelationRows.Disposition.INDETERMINATE
                            : evaluated.matches() ? ValidationRelationRows.Disposition.INCLUDED : ValidationRelationRows.Disposition.EXCLUDED;
                    var actual = key(original, "source_key", kind).physical();
                    var physical = new ValidationPhysicalIdentity(actual.sourceId(), DATABASE, actual.schema(), sourceTable, actual.keyFields(), actual.actualKey());
                    var issues = evaluated.problem() == null ? List.<ValidationRootCheck.Finding>of() : List.of(new ValidationRootCheck.Finding(
                            evaluated.problem().kind() == ValidationPredicate.ProblemKind.MISSING_FIELD ? ValidationRootCheck.Kind.SOURCE_FIELD_MISSING
                                    : ValidationRootCheck.Kind.SOURCE_FIELD_INVALID, new ValidationRootCheck.Location(observation.root(), evaluated.problem().path())));
                    return new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(disposition, working, Optional.of(physical)), issues);
                }, original -> {
                    var actual = key(original, "target_key", "mongodb").physical();
                    return new ValidationRootReader.Target(original,
                            new ValidationPhysicalIdentity(actual.sourceId(), DATABASE, null, targetTable, actual.keyFields(), actual.actualKey()));
                }, (pair, root) -> ValidationFieldCheck.check(observation, List.of(), pair.source().working(), pair.target().working(),
                        pair.sourceSample(), pair.targetSample(), generated, String::equals, new ValidationValueComparison.TraversalLimits(LIMITS, 10000), root));
        for (var handle : handles) assertThat(handle.workersExited().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
        return checked;
    }

    private static ValidationReadHandle rootLookup(String kind, ConnectorRef artifact, Map<String, Object> settings, String table,
            String keyField, ValidationKey logical, ExecutorService queryIo, ExecutorService decodeIo) {
        var binding = new ValidationKeyBinding("root", List.of(new ValidationKeyBinding.Component(List.of(keyField),
                ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), LIMITS);
        return PdkValidationSession.lookup(kind, artifact, settings, "retained-" + kind + "-artifact",
                new PdkValidationSession.Lookup(table, List.of(), List.of(), 2, binding, logical),
                (model, codecs) -> new PdkValidationRows(model, codecs, path -> {
                    if (path.size() == 1 && List.of(keyField, "amount", "price", "fractional").contains(path.getFirst()))
                        return PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER);
                    if (path.equals(List.of("local_time"))) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.LOCAL_DATE_TIME);
                    if (path.equals(List.of("body"))) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.JSON);
                    return PdkValidationRows.Rule.AUTO;
                }, LIMITS), (model, codecs, key) -> Map.of(keyField, key.parts().getFirst().value()),
                () -> {}, queryIo, decodeIo, 1, LIMITS);
    }

    private static void verifyLookups(GenericContainer<?> jdbc, boolean mysql, String kind, ConnectorRef sourceRef,
            Map<String, Object> sourceSettings, ConnectorRef targetRef, String uri) throws Exception {
        // Native equality is admitted for this controlled exact-numeric and binary-collation fixture.
        // This is not a blanket text-collation or portable-to-native guarantee for other models.
        String labelType = mysql ? "VARCHAR(40) COLLATE utf8mb4_0900_bin" : "VARCHAR(40) COLLATE \"C\"";
        sql(jdbc, mysql, "CREATE TABLE lookup_records (record_key BIGINT NOT NULL, amount DECIMAL(30,0), label " + labelType + ");"
                + "INSERT INTO lookup_records VALUES "
                + "(1," + EXACT + ",' O''Brien '),(2," + EXACT + ",' O''Brien '),(3," + EXACT + ",' O''Brien '),"
                + "(4," + (EXACT - 1) + ",' O''Brien '),(5," + EXACT + ",'O''Brien'),"
                + "(6," + EXACT + ",' O''BRIEN '),(7," + (EXACT + 1) + ",' O''Brien ');");
        try (var client = MongoClients.create(uri)) {
            client.getDatabase(DATABASE).getCollection("lookup_records").insertMany(
                    List.of(7L, 6L, 5L, 4L, 3L, 2L, 1L).stream().map(id -> new Document("record_key", id)
                            .append("amount", id == 4 ? EXACT - 1 : id == 7 ? EXACT + 1 : EXACT)
                            .append("label", id == 5 ? "O'Brien" : id == 6 ? " O'BRIEN " : " O'Brien ")).toList());
        }
        for (boolean source : List.of(true, false)) {
            String connector = source ? kind : "mongodb";
            ConnectorRef artifact = source ? sourceRef : targetRef;
            Map<String, Object> settings = source ? sourceSettings : Map.of("uri", uri, "database", DATABASE, "isUri", true);
            assertThat(lookupIds(connector, artifact, settings, EXACT)).containsExactlyInAnyOrder(1L, 2L, 3L);
            assertThat(lookupIds(connector, artifact, settings, EXACT - 1)).containsExactly(4L);
            assertThat(lookupIds(connector, artifact, settings, EXACT + 1)).containsExactly(7L);
            assertThat(lookupIds(connector, artifact, settings, EXACT + 10)).isEmpty();
        }
        assertThat(sql(jdbc, mysql, "SELECT COUNT(*) FROM lookup_records;").trim()).isEqualTo("7");
    }

    private static List<Long> lookupIds(String kind, ConnectorRef artifact, Map<String, Object> settings, long amount) {
        var binding = new ValidationKeyBinding("lookup", List.of(
                new ValidationKeyBinding.Component(List.of("amount"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number")),
                new ValidationKeyBinding.Component(List.of("label"), ValidationKey.Kind.TEXT, Set.of("text"))), Set.of(), LIMITS);
        var logical = new ValidationKey(List.of(ValidationKey.Part.number(amount), ValidationKey.Part.text(" O'Brien ")));
        var lookup = new PdkValidationSession.Lookup("lookup_records", List.of(), List.of("record_key", "amount", "label"), 2, binding, logical);
        var ids = new ArrayList<Long>();
        var projection = ValidationStaticMap.compile("price", new TransformBody.MapProjection(
                Map.of("price", FieldRule.rename("amount"))), LIMITS);
        var predicate = ValidationPredicateCompiler.compile("eligible", "after.price >= 9007199254740993",
                new ValidationPredicate.Limits(LIMITS, 2000, false));
        var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
                new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(102))), List.of()), 1);
        try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor();
             var cursor = PdkValidationSession.lookup(kind, artifact, settings, "retained-" + kind + "-artifact", lookup,
                     (model, codecs) -> new PdkValidationRows(model, codecs, path ->
                             PdkValidationRows.Rule.of(path.equals(List.of("label")) ? PdkValidationRows.Shape.TEXT : PdkValidationRows.Shape.EXACT_NUMBER), LIMITS),
                     (model, codecs, key) -> Map.of("amount", key.parts().get(0).value(), "label", key.parts().get(1).value()),
                     () -> {}, queryIo, decodeIo, 1, LIMITS)) {
            var relation = ValidationRelationReader.read(observation, "relation", "native-lookup-v1", cursor,
                    new ValidationRelationRows.Limits(32768, LIMITS), Clock.systemUTC(), () -> {}, original -> {
                        long id = ((Number) value(original.fields().get("record_key"))).longValue();
                        ids.add(id);
                        assertThat(value(original.fields().get("label"))).isEqualTo(" O'Brien ");
                        var working = projection.apply(original);
                        var participation = predicate.evaluate(working);
                        assertThat(participation.problem()).isNull();
                        var physical = new ValidationPhysicalIdentity(kind, DATABASE, kind.equals("postgres") ? "public" : null,
                                "lookup_records", List.of(List.of("record_key")), new ValidationKey(List.of(ValidationKey.Part.number(id))));
                        return new ValidationRelationReader.Evaluation(participation.matches() ? ValidationRelationRows.Disposition.INCLUDED
                                : ValidationRelationRows.Disposition.EXCLUDED, working, Optional.of(physical));
                    });
            long included = amount >= EXACT ? ids.size() : 0;
            assertThat(relation.included()).isEqualTo(included);
            assertThat(relation.excluded()).isEqualTo(ids.size() - included);
            assertThat(relation.indeterminate()).isZero();
            assertThat(relation.includedSamples()).hasSize((int) Math.min(included, 2));
            relation.includedSamples().forEach(sample -> {
                assertThat(sample.original().fields()).containsKey("amount").doesNotContainKey("price");
                assertThat(sample.working().fields()).containsKey("price").doesNotContainKey("amount");
                assertThat(sample.physical().orElseThrow().table()).isEqualTo("lookup_records");
            });
            try (var root = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN,
                    ValidationCheckEvent.Participation.SHOULD_EXIST, ValidationRootCheck.TargetPresence.PRESENT,
                    new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 4))) {
                var location = new ValidationRootCheck.Location(observation.root(), List.of("child"));
                var checked = ValidationNestSingleSource.check(relation, ValidationNestSingleSource.Relation.GATHER,
                        Optional.empty(), location, List.of(location), List.of(), root);
                assertThat(checked.state()).isEqualTo(included == 0 ? ValidationNestSingleSource.State.EMPTY
                        : included == 1 ? ValidationNestSingleSource.State.UNIQUE : ValidationNestSingleSource.State.BLOCKED);
                // Target content and the full root are outside this source-query fixture.
            }
        }
        return ids;
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
