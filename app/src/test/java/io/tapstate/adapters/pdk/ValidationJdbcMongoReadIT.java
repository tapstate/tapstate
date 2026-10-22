package io.tapstate.adapters.pdk;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ValidationPredicateCompiler;
import io.tapstate.core.dsl.ValidationDeclarations;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.TransformResource;
import io.tapstate.app.ValidationLinearRules;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
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
import io.tapstate.runtime.engine.validation.ValidationNestRelations;
import io.tapstate.runtime.engine.validation.ValidationNestLookupBinding;
import io.tapstate.runtime.engine.validation.ValidationNestObjectProjection;
import io.tapstate.runtime.engine.validation.ValidationNestTree;
import io.tapstate.runtime.engine.validation.ValidationNestArraySourceRead;
import io.tapstate.core.validation.ValidationNestArrayMembers;
import io.tapstate.core.validation.ValidationNestArrayFieldCheck;
import io.tapstate.core.validation.ValidationNestTargetArray;
import io.tapstate.runtime.engine.validation.ValidationRootReader;
import io.tapstate.runtime.engine.validation.ValidationStaticMap;
import io.tapstate.runtime.engine.validation.ValidationReadScope;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
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
import static io.tapstate.core.validation.ValidationRootCheck.Kind.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real independent JDBC/Mongo reads; no pipeline execution, strict window or general admission proof. */
@RequiresDocker
class ValidationJdbcMongoReadIT {
    private static final String DATABASE = "validation_reads";
    private static final String PASSWORD = "validation-fixture-only";
    private static final long EXACT = 9_007_199_254_740_993L;
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(8192, 100, 12);
    // Nested values retain per-value type and codec provenance, in addition to the small raw document.
    private static final ValidationValues.Limits ARRAY_LIMITS = new ValidationValues.Limits(32768, 100, 12);

    @Test @Timeout(120)
    void mysqlNativeReadPreservesDecimalsAndCompletesBothMergeTails() throws Exception { verify(true); }

    @Test @Timeout(120)
    void postgresNativeReadPreservesDecimalsAndCompletesBothMergeTails() throws Exception { verify(false); }

    @Test @Timeout(120)
    void mysqlNativeArraysKeepAllMembersAndRootProblems() throws Exception { verifyArrays(true); }

    @Test @Timeout(120)
    void postgresNativeArraysKeepAllMembersAndRootProblems() throws Exception { verifyArrays(false); }

    @Test @Timeout(120)
    void mysqlNativeMixedTreeKeepsObjectArrayScopes() throws Exception { verifyMixedTree(true); }

    @Test @Timeout(120)
    void postgresNativeMixedTreeKeepsObjectArrayScopes() throws Exception { verifyMixedTree(false); }

    private void verifyMixedTree(boolean mysql) throws Exception {
        String kind = mysql ? "mysql" : "postgres";
        var sourceRef = artifact(kind); var targetRef = artifact("mongodb");
        int port = mysql ? 3306 : 5432;
        try (var jdbc = new GenericContainer<>(DockerImageName.parse(mysql ? "mysql:8.0" : "postgres:16"))
                    .withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75));
             var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            if (mysql) jdbc.withEnv("MYSQL_ROOT_PASSWORD", PASSWORD).withEnv("MYSQL_DATABASE", DATABASE)
                    .withEnv("MYSQL_USER", "validation").withEnv("MYSQL_PASSWORD", PASSWORD)
                    .withCommand("--innodb-buffer-pool-size=64M", "--max-connections=20")
                    .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n", 1));
            else jdbc.withEnv("POSTGRES_PASSWORD", PASSWORD).withEnv("POSTGRES_USER", "validation").withEnv("POSTGRES_DB", DATABASE)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
            jdbc.start(); mongo.start();
            sql(jdbc, mysql, "CREATE TABLE mixed_roots (id BIGINT PRIMARY KEY, customer_id BIGINT, shipping_id BIGINT, label VARCHAR(40));"
                    + "CREATE TABLE mixed_customers (id BIGINT PRIMARY KEY, name VARCHAR(40));"
                    + "CREATE TABLE mixed_items (id BIGINT PRIMARY KEY, customer_id BIGINT, product_id BIGINT, amount BIGINT);"
                    + "CREATE INDEX mixed_parent_lookup ON mixed_items(customer_id);"
                    + "CREATE TABLE mixed_products (id BIGINT PRIMARY KEY, name VARCHAR(40));"
                    + "CREATE TABLE mixed_shipping (id BIGINT PRIMARY KEY, label VARCHAR(40));"
                    + "INSERT INTO mixed_roots VALUES (" + EXACT + ",9,77,'root'),(2,9,77,'root');"
                    + "INSERT INTO mixed_customers VALUES (9,'Ada');"
                    + "INSERT INTO mixed_items VALUES (11,9,501,5),(12,9,502,6);"
                    + "INSERT INTO mixed_products VALUES (501,'P1'),(502,'P2');"
                    + "INSERT INTO mixed_shipping VALUES (77,'ship');");
            var sourceSettings = new LinkedHashMap<String, Object>();
            sourceSettings.put("host", jdbc.getHost()); sourceSettings.put("port", jdbc.getMappedPort(port));
            sourceSettings.put("database", DATABASE); sourceSettings.put(mysql ? "username" : "user", "validation");
            sourceSettings.put("password", PASSWORD); sourceSettings.put("timezone", "+00:00"); sourceSettings.put("oldVersionTimezone", false);
            if (mysql) sourceSettings.put("addtionalString", "useSSL=false&allowPublicKeyRetrieval=true"); else sourceSettings.put("schema", "public");
            String uri = mongo.getReplicaSetUrl(DATABASE);
            try (var client = MongoClients.create(uri)) {
                client.getDatabase(DATABASE).getCollection("mixed_targets").insertMany(List.of(mixedDocument(EXACT, false), mixedDocument(2, true)));
            }
            var targetSettings = Map.<String, Object>of("uri", uri, "database", DATABASE, "isUri", true);
            try (var queryIo = Executors.newFixedThreadPool(2); var decodeIo = Executors.newFixedThreadPool(2)) {
                var good = mixedRootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings, EXACT, queryIo, decodeIo);
                assertThat(good.observedAgreement()).isTrue(); assertThat(good.reads()).hasSize(7);
                var bad = mixedRootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings, 2, queryIo, decodeIo);
                assertThat(bad.counts()).containsExactly(entry(TARGET_VALUE, 2L)); assertThat(bad.reads()).hasSize(7);
                assertThat(bad.findings().keySet()).anySatisfy(finding -> {
                    assertThat(finding.location().scope().rootKey()).isEqualTo(new ValidationKey(List.of(ValidationKey.Part.number(2))));
                    assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("customer", "items"),
                            new ValidationKey(List.of(ValidationKey.Part.number(11)))));
                    assertThat(finding.location().fieldPath()).containsExactly("product", "name");
                }).anySatisfy(finding -> {
                    assertThat(finding.location().scope().ancestors()).isEmpty();
                    assertThat(finding.location().fieldPath()).containsExactly("shipping", "label");
                });
            }
            for (var table : Map.of("mixed_roots", "2", "mixed_customers", "1", "mixed_items", "2", "mixed_products", "2", "mixed_shipping", "1").entrySet())
                assertThat(sql(jdbc, mysql, "SELECT COUNT(*) FROM " + table.getKey() + ";").trim()).isEqualTo(table.getValue());
            System.out.printf("Native mixed tree %s: roots=2 reads=14, source tables unchanged%n", kind);
        }
    }

    private static Document mixedDocument(long root, boolean wrong) {
        var first = new Document("id", 11L).append("customer_id", 9L).append("product_id", 501L).append("amount", 5L)
                .append("product", new Document("id", 501L).append("name", wrong ? "wrong" : "P1"));
        var second = new Document("id", 12L).append("customer_id", 9L).append("product_id", 502L).append("amount", 6L)
                .append("product", new Document("id", 502L).append("name", "P2"));
        return new Document("id", root).append("customer_id", 9L).append("shipping_id", 77L).append("label", "root")
                .append("customer", new Document("id", 9L).append("name", "Ada").append("items", List.of(second, first)))
                .append("shipping", new Document("id", 77L).append("label", wrong ? "wrong" : "ship"));
    }

    private static ValidationRootCheck mixedRootRead(String kind, ConnectorRef sourceRef, Map<String, Object> sourceSettings,
            ConnectorRef targetRef, Map<String, Object> targetSettings, long id, ExecutorService queryIo, ExecutorService decodeIo) throws Exception {
        var key = new ValidationKey(List.of(ValidationKey.Part.number(id)));
        var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "native-mixed-" + kind, ValidationMode.FULL, "fixed-mixed-tree", 1, "rules"), new ValidationRoot("mixed_roots", "output", key, List.of()), 1);
        var product = new Embed("mixed_products", Map.of("id", "product_id"), EmbedAs.OBJECT, "product", List.of("id"), null, null, null, List.of());
        var items = new Embed("mixed_items", Map.of("customer_id", "id"), EmbedAs.ARRAY, "items", List.of("id"), null, null, null, List.of(product));
        var customer = new Embed("mixed_customers", Map.of("id", "customer_id"), EmbedAs.OBJECT, "customer", List.of("id"), null, null, null, List.of(items));
        var shipping = new Embed("mixed_shipping", Map.of("id", "shipping_id"), EmbedAs.OBJECT, "shipping", List.of("id"), null, null, null, List.of());
        var plan = ValidationNestRelations.compile("mixed", new NestRoot("mixed_roots", List.of("id"), null, null, List.of(customer, shipping)),
                Set.of("mixed_roots", "mixed_customers", "mixed_items", "mixed_products", "mixed_shipping"), Map.of(), new ValidationNestRelations.Limits(16, 8, 65536));
        var identity = new ValidationKeyBinding("mixed-id", List.of(objectComponent("id")), Set.of(), ARRAY_LIMITS);
        var identityMap = ValidationStaticMap.compile("mixed-input", new TransformBody.MapProjection(Map.of()), ARRAY_LIMITS);
        var handles = new ArrayList<ValidationReadHandle>(); var ids = new java.util.concurrent.atomic.AtomicInteger();
        try (var cleanup = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(8), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
             var scope = new ValidationReadScope(observation, cleanup, () -> {}, 8)) {
            var bindings = new LinkedHashMap<List<String>, ValidationNestTree.Binding>();
            bindMixedRelations(kind, sourceRef, sourceSettings, plan.children(), identity, identityMap, bindings, scope, handles, ids, queryIo, decodeIo);
            var generated = ValidationFieldCheck.generatedId(observation.run(), "mixed_roots", "output", true, false, false,
                    value -> value instanceof ValidationValues.Typed typed && typed.logicalType().equals("text") && typed.value() instanceof String);
            var tree = new ValidationNestTree(new ValidationNestObjectProjection.Scope(observation.run(), "mixed_roots", "output"), plan,
                    new ValidationNestObjectProjection.RootKeys(identity, identity), bindings,
                    new ValidationNestTree.Limits(ARRAY_LIMITS, 32 * 1024 * 1024, 64 * 1024 * 1024, 100_000, 20, 100, 8, 100,
                            new ValidationRootCheck.Limits(4 * 1024 * 1024, 100, 100, 100, 10)), Set.of("array", "object", "json"), String::equals, generated);
            var result = tree.read(observation,
                    new ValidationRootReader.Query("mixed-source-" + id, "native-mixed-v1", () -> openMixedRead(kind, sourceRef, sourceSettings,
                            "mixed_roots", identity, key, scope, handles, queryIo, decodeIo)),
                    new ValidationRootReader.Query("mixed-target-" + id, "native-mixed-v1", () -> openMixedRead("mongodb", targetRef, targetSettings,
                            "mixed_targets", identity, key, scope, handles, queryIo, decodeIo)), Clock.systemUTC(), scope::requireActive,
                    (context, raw) -> mixedRow(kind, "mixed_roots", identity, identityMap, raw),
                    raw -> new ValidationRootReader.Target(raw, objectPhysical("mongodb", "mixed_targets", identity, raw)));
            assertThat(handles).hasSize(7);
            for (var handle : handles) {
                assertThat(handle.workersExited().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
                assertThat(handle.rowsReleased().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            }
            scope.close(); assertThat(scope.drained().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            return result;
        }
    }

    private static void bindMixedRelations(String kind, ConnectorRef artifact, Map<String, Object> settings,
            List<ValidationNestRelations.Relation> relations, ValidationKeyBinding identity, ValidationStaticMap identityMap,
            Map<List<String>, ValidationNestTree.Binding> bindings, ValidationReadScope scope, List<ValidationReadHandle> handles,
            java.util.concurrent.atomic.AtomicInteger ids, ExecutorService queryIo, ExecutorService decodeIo) {
        for (var relation : relations) {
            var physical = new ValidationKeyBinding("mixed-relation", relation.join().stream().map(pair -> objectComponent(pair.child())).toList(), Set.of(), ARRAY_LIMITS);
            var lookup = new ValidationNestLookupBinding("mixed", relation, relation.join().stream().map(pair -> objectComponent(pair.parent())).toList(), physical, ARRAY_LIMITS);
            bindings.put(relation.treePath(), new ValidationNestTree.Binding(lookup, request ->
                    new ValidationRootReader.Query("mixed-child-" + ids.incrementAndGet(), "native-mixed-v1", () ->
                            openMixedRead(kind, artifact, settings, relation.alias(), physical, request.key(), scope, handles, queryIo, decodeIo)),
                    (context, raw) -> mixedRow(kind, relation.alias(), identity, identityMap, raw), relation.shape() == EmbedAs.ARRAY
                            ? Optional.of(new ValidationNestTree.ArrayKeys(identity, identity, identity, Optional.of(identity))) : Optional.empty()));
            bindMixedRelations(kind, artifact, settings, relation.children(), identity, identityMap, bindings, scope, handles, ids, queryIo, decodeIo);
        }
    }
    private static ValidationNestTree.Row mixedRow(String kind, String table, ValidationKeyBinding identity, ValidationStaticMap identityMap, ValidationValues.Row raw) {
        var view = identityMap.apply(raw, List.of());
        return new ValidationNestTree.Row(new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                view.row(), Optional.of(objectPhysical(kind, table, identity, raw))), List.of()), view);
    }
    private static ValidationReadHandle openMixedRead(String kind, ConnectorRef artifact, Map<String, Object> settings, String table,
            ValidationKeyBinding binding, ValidationKey key, ValidationReadScope scope, List<ValidationReadHandle> handles,
            ExecutorService queryIo, ExecutorService decodeIo) {
        var handle = scope.open(table, guard -> PdkValidationSession.lookup(kind, artifact, settings, "retained-" + kind + "-artifact",
                new PdkValidationSession.Lookup(table, List.of(), List.of(), 2, binding, key),
                (model, codecs) -> new PdkValidationRows(model, codecs, path -> {
                    if (path.getLast().equals("items")) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.ARRAY);
                    if (Set.of("customer", "shipping", "product").contains(path.getLast())) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.OBJECT);
                    if (Set.of("id", "customer_id", "shipping_id", "product_id", "amount").contains(path.getLast())) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER);
                    return PdkValidationRows.Rule.AUTO;
                }, ARRAY_LIMITS), (model, codecs, logical) -> {
                    var match = new LinkedHashMap<String, Object>();
                    for (int i = 0; i < binding.components().size(); i++) match.put(binding.components().get(i).path().getFirst(), logical.parts().get(i).value());
                    return match;
                }, guard, queryIo, decodeIo, 1, ARRAY_LIMITS));
        handles.add(handle); return handle;
    }

    private void verifyArrays(boolean mysql) throws Exception {
        String kind = mysql ? "mysql" : "postgres";
        var sourceRef = artifact(kind); var targetRef = artifact("mongodb");
        int port = mysql ? 3306 : 5432;
        try (var jdbc = new GenericContainer<>(DockerImageName.parse(mysql ? "mysql:8.0" : "postgres:16"))
                    .withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75));
             var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            if (mysql) jdbc.withEnv("MYSQL_ROOT_PASSWORD", PASSWORD).withEnv("MYSQL_DATABASE", DATABASE)
                    .withEnv("MYSQL_USER", "validation").withEnv("MYSQL_PASSWORD", PASSWORD)
                    .withCommand("--innodb-buffer-pool-size=64M", "--max-connections=20")
                    .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n", 1));
            else jdbc.withEnv("POSTGRES_PASSWORD", PASSWORD).withEnv("POSTGRES_USER", "validation").withEnv("POSTGRES_DB", DATABASE)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
            jdbc.start(); mongo.start();
            sql(jdbc, mysql, "CREATE TABLE array_roots (id BIGINT PRIMARY KEY, label VARCHAR(40));"
                    + "CREATE TABLE array_children (id BIGINT NOT NULL, root_id BIGINT NOT NULL, sku VARCHAR(40), value VARCHAR(40));"
                    + "CREATE INDEX array_parent_lookup ON array_children(root_id);"
                    + "INSERT INTO array_roots VALUES (" + EXACT + ",'root'),(2,'root'),(3,'root'),(4,'root'),(5,'root'),(6,'root'),(7,'root'),(8,'root'),(9,'root'),(10,'root');"
                    + "INSERT INTO array_children VALUES (11," + EXACT + ",'same','A'),(12," + EXACT + ",'same','B'),(13," + EXACT + ",'same','C'),"
                    + "(14," + EXACT + ",'same','D'),(15," + EXACT + ",'same','E'),(16," + EXACT + ",'same','F'),"
                    + "(21,2,'same','A'),(41,4,'same','A'),(42,4,'same','B'),(51,5,'same','first'),(51,5,'same','second'),"
                    + "(61,6,'same','A'),(71,7,'same','A'),(81,8,'same','A'),(91,9,'same','first'),(91,9,'same','second'),"
                    + "(101,10,'same','first'),(101,10,'same','second');");
            var sourceSettings = new LinkedHashMap<String, Object>();
            sourceSettings.put("host", jdbc.getHost()); sourceSettings.put("port", jdbc.getMappedPort(port));
            sourceSettings.put("database", DATABASE); sourceSettings.put(mysql ? "username" : "user", "validation");
            sourceSettings.put("password", PASSWORD); sourceSettings.put("timezone", "+00:00"); sourceSettings.put("oldVersionTimezone", false);
            if (mysql) sourceSettings.put("addtionalString", "useSSL=false&allowPublicKeyRetrieval=true");
            else sourceSettings.put("schema", "public");
            String uri = mongo.getReplicaSetUrl(DATABASE);
            try (var client = MongoClients.create(uri)) {
                var roots = new ArrayList<Document>();
                roots.add(arrayDocument(EXACT, "root", List.of(arrayItem(16, EXACT, "F"), arrayItem(15, EXACT, "E"), arrayItem(14, EXACT, "D"),
                        arrayItem(13, EXACT, "C"), arrayItem(12, EXACT, "B"), arrayItem(11, EXACT, "A"))));
                roots.add(arrayDocument(2, "root", List.of(arrayItem(21, 2, "wrong"))));
                roots.add(arrayDocument(3, "root", List.of()));
                roots.add(arrayDocument(4, "root", List.of(arrayItem(42, 4, "B"), arrayItem(43, 4, "extra"))));
                roots.add(arrayDocument(5, "wrong", List.of(arrayItem(51, 5, "unproved"))));
                roots.add(arrayDocument(6, "root", List.of(arrayItem(61, 6, "A"), arrayItem(61, 6, "A"))));
                roots.add(new Document("id", 7L).append("label", "wrong"));
                roots.add(arrayDocument(8, "root", List.of(arrayItem(81, 8, "A"))));
                roots.add(arrayDocument(10, "root", List.of(arrayItem(101, 10, "unproved"))));
                roots.add(arrayDocument(10, "root", List.of(arrayItem(101, 10, "unproved"))));
                client.getDatabase(DATABASE).getCollection("array_targets").insertMany(roots);
            }
            var settings = Map.<String, Object>of("uri", uri, "database", DATABASE, "isUri", true);
            var results = new LinkedHashMap<Long, Map<ValidationRootCheck.Kind, Long>>();
            try (var queryIo = Executors.newFixedThreadPool(2); var decodeIo = Executors.newFixedThreadPool(2)) {
                verifyArrayBudgetRejection(targetRef, settings, queryIo, decodeIo);
                for (long id : List.of(EXACT, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)) {
                    var result = arrayRootRead(kind, sourceRef, sourceSettings, targetRef, settings, id, queryIo, decodeIo);
                    assertThat(result.reads()).hasSize(3);
                    assertThat(result.observedAgreement()).as("agreement for array root %s", id).isEqualTo(id == EXACT || id == 3 || id == 8);
                    if (id == 2) assertThat(result.findings().keySet()).singleElement().satisfies(finding -> {
                        assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("items"),
                                new ValidationKey(List.of(ValidationKey.Part.number(21)))));
                        assertThat(finding.location().fieldPath()).containsExactly("value");
                    });
                    results.put(id, result.counts());
                }
            }
            assertThat(results).isEqualTo(Map.of(EXACT, Map.of(), 2L, Map.of(TARGET_VALUE, 1L), 3L, Map.of(),
                    4L, Map.of(TARGET_MISSING, 1L, TARGET_EXTRA, 1L), 5L, Map.of(SOURCE_DUPLICATE, 1L, TARGET_VALUE, 1L),
                    6L, Map.of(TARGET_DUPLICATE, 1L), 7L, Map.of(TARGET_STRUCTURE, 1L, TARGET_VALUE, 1L), 8L, Map.of(),
                    9L, Map.of(SOURCE_DUPLICATE, 1L, TARGET_MISSING, 1L), 10L, Map.of(SOURCE_DUPLICATE, 1L, TARGET_DUPLICATE, 1L)));
            assertThat(sql(jdbc, mysql, "SELECT COUNT(*) FROM array_roots;").trim()).isEqualTo("10");
            assertThat(sql(jdbc, mysql, "SELECT COUNT(*) FROM array_children;").trim()).isEqualTo("18");
        }
    }

    private static Document arrayDocument(long id, String label, List<Document> items) {
        return new Document("id", id).append("label", label).append("items", items);
    }
    private static Document arrayItem(long id, long parent, String value) {
        return new Document("id", id).append("root_id", parent).append("sku", "same").append("value", value);
    }

    private static ValidationRootCheck arrayRootRead(String kind, ConnectorRef sourceRef, Map<String, Object> sourceSettings,
            ConnectorRef targetRef, Map<String, Object> targetSettings, long id, ExecutorService queryIo, ExecutorService decodeIo) throws Exception {
        var rootKey = new ValidationKey(List.of(ValidationKey.Part.number(id)));
        var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "native-arrays-" + kind, ValidationMode.FULL, "fixed-array-tree", 1, "rules"),
                new ValidationRoot("array_roots", "output", rootKey, List.of()), 1);
        var identity = new ValidationKeyBinding("array-identity", List.of(objectComponent("id")), Set.of(), ARRAY_LIMITS);
        var parentKey = new ValidationKeyBinding("array-parent", List.of(objectComponent("root_id")), Set.of(), ARRAY_LIMITS);
        var embed = new Embed("array_children", Map.of("root_id", "id"), EmbedAs.ARRAY, "items", List.of("id"), List.of("id"), null, null, List.of());
        var plan = ValidationNestRelations.compile("arrays", new NestRoot("root", List.of("id"), null, null, List.of(embed)),
                Set.of("root", "array_children"), Map.of(), new ValidationNestRelations.Limits(8, 8, 65536));
        var lookup = new ValidationNestLookupBinding("arrays", plan.children().getFirst(), List.of(objectComponent("id")), parentKey, ARRAY_LIMITS);
        var emptyMap = ValidationStaticMap.compile("array-parent", new TransformBody.MapProjection(Map.of()), ARRAY_LIMITS);
        var handles = new ArrayList<ValidationReadHandle>();
        var sourceFacts = new ValidationNestArrayMembers.Source[1];
        String sourceRead = "array-root-source-" + id;
        try (var cleanup = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(8), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
             var scope = new ValidationReadScope(observation, cleanup, () -> {}, 8)) {
            var generated = ValidationFieldCheck.generatedId(observation.run(), "array_roots", "output", true, false, false,
                    value -> value instanceof ValidationValues.Typed typed && typed.logicalType().equals("text") && typed.value() instanceof String);
            var result = ValidationRootReader.read(observation, ValidationMethod.FULL_COLUMN,
                    new ValidationRootReader.Query(sourceRead, "native-array-v1", () -> openArrayRead(kind, sourceRef, sourceSettings,
                            "array_roots", identity, rootKey, scope, handles, queryIo, decodeIo)),
                    new ValidationRootReader.Query("array-root-target-" + id, "native-array-v1", () -> openArrayRead("mongodb", targetRef, targetSettings,
                            "array_targets", identity, rootKey, scope, handles, queryIo, decodeIo)),
                    new ValidationRelationRows.Limits(65536, ARRAY_LIMITS), new ValidationRootCheck.Limits(1048576, 100, 100, 20, 8), Clock.systemUTC(), scope::requireActive,
                    raw -> {
                        var physical = objectPhysical(kind, "array_roots", identity, raw);
                        var children = ValidationNestArraySourceRead.read(observation, lookup, emptyMap.apply(raw, List.of()),
                                new ValidationRootCheck.SampledSide(physical, raw, sourceRead), request ->
                                        new ValidationRootReader.Query("array-children-" + id, "native-array-v1", () -> openArrayRead(kind, sourceRef, sourceSettings,
                                                "array_children", parentKey, request.key(), scope, handles, queryIo, decodeIo)),
                                new ValidationNestArraySourceRead.Limits(new ValidationRelationRows.Limits(65536, ARRAY_LIMITS),
                                        new ValidationNestArrayMembers.Limits(1048576, 100, 100, ARRAY_LIMITS), 100, 20), Clock.systemUTC(), scope::requireActive,
                                (context, child) -> {
                                    var actual = objectPhysical(kind, "array_children", identity, child);
                                    var key = ((ValidationKeyBinding.Bound) identity.read(child)).key();
                                    return new ValidationNestArraySourceRead.Classified(new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(
                                            ValidationRelationRows.Disposition.INCLUDED, child, Optional.of(actual)), List.of()),
                                            Optional.of(new ValidationNestArrayMembers.Member(key, key, actual, child)));
                                }, (context, source) -> {
                                    assertThat(context.parent()).isEqualTo(observation);
                                    assertThat(source.uncertainty()).isEmpty(); assertThat(source.evidence()).isEqualTo(ValidationRootReader.SourceEvidence.empty());
                                });
                        sourceFacts[0] = children.source().orElseThrow();
                        return new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED, raw, Optional.of(physical)),
                                List.of(), ValidationRootReader.SourceEvidence.fromArraySource(observation, sourceFacts[0], 1048576));
                    }, raw -> {
                        if (id == EXACT) System.out.printf("Native array fixture %s: retainedBytes=%d nodes=%d depth=%d%n", kind,
                                raw.accountedBytes(), raw.nodes(), raw.depth());
                        if (raw.fields().containsKey("items")) assertThat(raw.fields().get("items")).isInstanceOfSatisfying(ValidationValues.Typed.class,
                                typed -> { assertThat(typed.logicalType()).isEqualTo("array"); assertThat(typed.value()).isInstanceOf(List.class); });
                        return new ValidationRootReader.Target(raw, objectPhysical("mongodb", "array_targets", identity, raw));
                    }, (pair, root) -> {
                        try (var members = ValidationNestArrayMembers.forTarget(sourceFacts[0])) {
                            var physical = new ValidationCheckEvent.Side(pair.target().physical().orElseThrow(), pair.target().original(), 1,
                                    ValidationCheckEvent.GroupStatus.COMPLETE, pair.targetRead());
                            var checked = ValidationNestArrayFieldCheck.check(observation, List.of("items"), pair.source().working(), physical,
                                    pair.target().working(), identity, Optional.of(identity), Set.of("array", "object", "json"),
                                    new ValidationNestTargetArray.Limits(ARRAY_LIMITS, 100, 100000), members, root, group -> {
                                        var source = group.sourceSamples().getFirst(); var target = group.targetSamples().getFirst();
                                        ValidationFieldCheck.check(new ValidationObservation(observation.run(), group.scope(), observation.revision()), List.of(),
                                                source.values(), target.values(), Optional.of(new ValidationRootCheck.SampledSide(source.physical(), source.original(), sourceFacts[0].read().readId())),
                                                Optional.of(new ValidationRootCheck.SampledSide(target.physical(), target.original(), pair.targetRead().readId())),
                                                Optional.empty(), String::equals, new ValidationValueComparison.TraversalLimits(ARRAY_LIMITS, 100000), root);
                                    });
                            ValidationFieldCheck.checkComposed(observation, pair.source().working(), pair.target().working(), pair.sourceSample(), pair.targetSample(),
                                    generated, String::equals, new ValidationValueComparison.TraversalLimits(ARRAY_LIMITS, 100000), List.of(), Set.of(), List.of(checked), root);
                        }
                    });
            for (var handle : handles) {
                assertThat(handle.workersExited().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
                assertThat(handle.rowsReleased().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            }
            scope.close(); assertThat(scope.drained().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            return result;
        }
    }

    private static ValidationReadHandle openArrayRead(String kind, ConnectorRef artifact, Map<String, Object> settings, String table,
            ValidationKeyBinding binding, ValidationKey key, ValidationReadScope scope, List<ValidationReadHandle> handles,
            ExecutorService queryIo, ExecutorService decodeIo) {
        var handle = scope.open(table, guard -> PdkValidationSession.lookup(kind, artifact, settings, "retained-" + kind + "-artifact",
                new PdkValidationSession.Lookup(table, List.of(), List.of(), 2, binding, key),
                arrayAdmission(ARRAY_LIMITS), (model, codecs, logical) -> {
                    var match = new LinkedHashMap<String, Object>();
                    for (int i = 0; i < binding.components().size(); i++) match.put(binding.components().get(i).path().getFirst(), logical.parts().get(i).value());
                    return match;
                }, guard, queryIo, decodeIo, 1, ARRAY_LIMITS));
        handles.add(handle); return handle;
    }

    private static PdkValidationSession.ReadAdmission arrayAdmission(ValidationValues.Limits limits) {
        return (model, codecs) -> new PdkValidationRows(model, codecs, path -> {
            if (path.equals(List.of("items"))) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.ARRAY);
            if (Set.of("id", "root_id").contains(path.getLast())) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER);
            return PdkValidationRows.Rule.AUTO;
        }, limits);
    }

    private static void verifyArrayBudgetRejection(ConnectorRef target, Map<String, Object> settings,
            ExecutorService queryIo, ExecutorService decodeIo) throws Exception {
        var binding = new ValidationKeyBinding("array-budget-key", List.of(objectComponent("id")), Set.of(), ARRAY_LIMITS);
        var key = new ValidationKey(List.of(ValidationKey.Part.number(EXACT)));
        try (var handle = PdkValidationSession.lookup("mongodb", target, settings, "retained-mongodb-artifact",
                new PdkValidationSession.Lookup("array_targets", List.of(), List.of(), 2, binding, key), arrayAdmission(LIMITS),
                (model, codecs, logical) -> Map.of("id", logical.parts().getFirst().value()), () -> {}, queryIo, decodeIo, 1, LIMITS)) {
            Throwable primary = catchThrowable(handle::next);
            assertThat(primary).isInstanceOfSatisfying(TapstateException.class, failure -> {
                assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED);
                assertThat(failure.args()).containsEntry("limit", "bytes");
            });
            handle.close();
            Throwable exited = catchThrowable(() -> handle.workersExited().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS));
            assertThat(exited).isInstanceOf(java.util.concurrent.ExecutionException.class).hasCauseInstanceOf(TapstateException.class);
            assertThat(((TapstateException) exited.getCause()).code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED);
            assertThat(handle.rowsReleased().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            var pending = new java.util.ArrayDeque<Throwable>(); pending.add(primary); pending.add(exited);
            var seen = new java.util.IdentityHashMap<Throwable, Boolean>();
            for (int i = 0; i < 64 && !pending.isEmpty(); i++) {
                var error = pending.removeFirst(); if (seen.put(error, true) != null) continue;
                assertThat(error.getClass().getName()).isNotEqualTo("com.mongodb.MongoInterruptedException");
                if (error.getCause() != null) pending.addLast(error.getCause());
                for (var suppressed : error.getSuppressed()) pending.addLast(suppressed);
            }
            assertThat(pending).isEmpty();
        }
    }

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
            var projectionBody = new TransformBody.MapProjection(Map.of(
                    "target_key", FieldRule.rename("source_key"), "fractional", FieldRule.drop(), "payload", FieldRule.drop(),
                    "local_time", FieldRule.drop(), "body", FieldRule.drop()));
            var projection = ValidationStaticMap.compile("fixed-output", projectionBody, LIMITS);
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
                                var sourceIdentity = new ValidationKeyBinding("source-root", List.of(new ValidationKeyBinding.Component(List.of("source_key"),
                ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), LIMITS);
        var projectedIdentity = new ValidationKeyBinding("projected-root", List.of(new ValidationKeyBinding.Component(List.of("target_key"),
                ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), LIMITS);
        var identity = new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), observation.root().outputId(),
                sourceIdentity, projectedIdentity);
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
            verifyRootReads(jdbc, mysql, kind, sourceRef, sourceSettings, targetRef, uri, projectionBody);
            verifyObjectReads(jdbc, mysql, kind, sourceRef, sourceSettings, targetRef, uri);
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
            Map<String, Object> sourceSettings, ConnectorRef targetRef, String uri, TransformBody.MapProjection projectionBody) throws Exception {
        var targetSettings = Map.<String, Object>of("uri", uri, "database", DATABASE, "isUri", true);
        var rules = linearRules(kind, "source_records", projectionBody, Optional.empty());
        try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor()) {
            verifyBorrowedReadBudget(kind, sourceRef, sourceSettings, queryIo, decodeIo);
            var actual = new ArrayList<String>();
            for (long id = 1; id <= 6; id++) {
                var result = rootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings, "source_records", "target_records", id,
                        rules, queryIo, decodeIo);
                actual.add(id + ":" + (result.observedAgreement() ? "EQUAL" : result.counts()));
                assertThat(result.reads()).hasSize(2);
            }
            assertThat(actual).containsExactly("1:EQUAL", "2:{TARGET_VALUE=1}", "3:{TARGET_MISSING=1}",
                    "4:{TARGET_EXTRA=1}", "5:{TARGET_VALUE=1}", "6:{TARGET_EXTRA=1}");
            for (FieldRule keyRule : List.of(FieldRule.literal(99), FieldRule.drop())) {
                var invalid = linearRules(kind, "source_records", new TransformBody.MapProjection(Map.of("target_key", keyRule)), Optional.empty());
                var targetOpens = new java.util.concurrent.atomic.AtomicInteger();
                assertThatThrownBy(() -> rootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings,
                        "source_records", "target_records", 1, invalid, queryIo, decodeIo, targetOpens))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> {
                            assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED);
                            assertThat(failure.args()).containsEntry("field", List.of("target_key"))
                                    .containsEntry("rule", keyRule instanceof FieldRule.Drop
                                            ? "identity.projected-key-unavailable" : "identity.projected-key-changed");
                        });
                assertThat(targetOpens).hasValue(0);
            }
        }
        sql(jdbc, mysql, "CREATE TABLE root_check_records (source_key BIGINT NOT NULL, amount DECIMAL(30,0));"
                + "INSERT INTO root_check_records VALUES (7," + (EXACT - 1) + "),(7," + EXACT + "),"
                + "(8," + EXACT + "),(8," + EXACT + "),(9,NULL),(10," + EXACT + "),(11," + (EXACT - 1) + ");");
        try (var observer = MongoClients.create(uri)) {
            observer.getDatabase(DATABASE).getCollection("root_check_records").insertMany(
                    List.of(7L, 8L, 9L, 10L, 10L, 11L).stream().map(id -> new Document("target_key", id).append("price", EXACT)).toList());
        }
        var filtered = linearRules(kind, "root_check_records", new TransformBody.MapProjection(
                Map.of("target_key", FieldRule.rename("source_key"), "price", FieldRule.rename("amount"))),
                Optional.of("after.price >= 9007199254740993"));
        try (var queryIo = Executors.newSingleThreadExecutor(); var decodeIo = Executors.newSingleThreadExecutor()) {
            var actual = new ArrayList<String>();
            for (long id = 7; id <= 11; id++) {
                var result = rootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings, "root_check_records", "root_check_records", id,
                        filtered, queryIo, decodeIo);
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

    private static void verifyObjectReads(GenericContainer<?> jdbc, boolean mysql, String kind, ConnectorRef sourceRef,
            Map<String, Object> sourceSettings, ConnectorRef targetRef, String uri) throws Exception {
        sql(jdbc, mysql, "CREATE TABLE nest_roots (id BIGINT PRIMARY KEY, customer_id BIGINT, label VARCHAR(40));"
                + "CREATE TABLE nest_customers (id BIGINT NOT NULL, region_id BIGINT, name VARCHAR(40));"
                + "CREATE INDEX customer_lookup ON nest_customers(id);"
                + "CREATE TABLE nest_regions (id BIGINT PRIMARY KEY, label VARCHAR(40));"
                + "INSERT INTO nest_roots VALUES (" + EXACT + ",10,'root'),(2,10,'root'),(3,NULL,'root'),(4,99,'root'),(5,20,'root'),(6,10,'root');"
                + "INSERT INTO nest_customers VALUES (10,100,'Ada'),(20,100,'first'),(20,100,'second');"
                + "INSERT INTO nest_regions VALUES (100,'north');"
                + "CREATE TABLE nest_conflict_roots (id BIGINT PRIMARY KEY, customer_id BIGINT, label VARCHAR(40), customer VARCHAR(40));"
                + "INSERT INTO nest_conflict_roots VALUES (7,NULL,'root','legacy');");
        try (var mongo = MongoClients.create(uri)) {
            var documents = new ArrayList<Document>();
            for (long id : List.of(EXACT, 2L, 3L, 4L, 5L, 6L, 7L)) {
                Long customer = id == 3 || id == 7 ? null : id == 4 ? 99L : id == 5 ? 20L : 10L;
                var document = new Document("id", id).append("customer_id", customer)
                        .append("label", id == 4 || id == 7 ? "wrong" : "root");
                if (id == 4 || id == 5 || id == 7) document.append("customer", new Document("unproved", true));
                else if (id != 3) document.append("customer", new Document("id", 10L).append("region_id", 100L).append("name", "Ada")
                        .append("region", new Document("id", 100L).append("label", id == 2 ? "wrong" : "north")));
                documents.add(document);
            }
            mongo.getDatabase(DATABASE).getCollection("nest_targets").insertMany(documents);
        }
        var targetSettings = Map.<String, Object>of("uri", uri, "database", DATABASE, "isUri", true);
        // A parent can remain backpressured while its independent child is read: each live read owns
        // separate query/decode capacity. These controlled numeric fixtures do not admit other models.
        try (var queryIo = Executors.newFixedThreadPool(2); var decodeIo = Executors.newFixedThreadPool(2)) {
            var results = new LinkedHashMap<Long, Map<ValidationRootCheck.Kind, Long>>();
            for (long id : List.of(EXACT, 2L, 3L, 4L, 5L, 6L, 7L)) {
                var result = objectRootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings,
                        id == 7 ? "nest_conflict_roots" : "nest_roots", id, queryIo, decodeIo);
                results.put(id, result.counts());
                assertThat(result.observedAgreement()).as("agreement for root %s", id).isEqualTo(id == EXACT || id == 3 || id == 6);
                int reads = id == 3 || id == 7 ? 2 : id == 4 || id == 5 ? 3 : 4;
                assertThat(result.reads()).hasSize(reads);
                if (id == 2) assertThat(result.findings().keySet()).singleElement().satisfies(finding ->
                        assertThat(finding.location().fieldPath()).containsExactly("customer", "region", "label"));
            }
            assertThat(results).isEqualTo(Map.of(EXACT, Map.of(), 2L, Map.of(TARGET_VALUE, 1L), 3L, Map.of(),
                    4L, Map.of(SOURCE_REFERENCE_MISSING, 1L, TARGET_VALUE, 1L), 5L, Map.of(SOURCE_DUPLICATE, 1L), 6L, Map.of(),
                    7L, Map.of(SOURCE_FIELD_CONFLICT, 1L, TARGET_VALUE, 1L)));
        }
        assertThat(sql(jdbc, mysql, "SELECT COUNT(*) FROM nest_roots;").trim()).isEqualTo("6");
        assertThat(sql(jdbc, mysql, "SELECT COUNT(*) FROM nest_customers;").trim()).isEqualTo("3");
    }

    private static ValidationRootCheck objectRootRead(String kind, ConnectorRef sourceRef, Map<String, Object> sourceSettings,
            ConnectorRef targetRef, Map<String, Object> targetSettings, String rootTable, long id,
            ExecutorService queryIo, ExecutorService decodeIo) throws Exception {
        var rootKey = new ValidationKey(List.of(ValidationKey.Part.number(id)));
        var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "native-objects-" + kind, ValidationMode.FULL, "fixed-object-tree", 1, "rules"),
                new ValidationRoot(rootTable, "output", rootKey, List.of()), 1);
        var region = new Embed("nest_regions", Map.of("id", "region_id"), EmbedAs.OBJECT, "region", List.of("id"), null, null, null, List.of());
        var customer = new Embed("nest_customers", Map.of("id", "customer_id"), EmbedAs.OBJECT, "customer", List.of("id"), null, null, null, List.of(region));
        var plan = ValidationNestRelations.compile("objects", new NestRoot("root", List.of("id"), null, null, List.of(customer)),
                Set.of("root", "nest_customers", "nest_regions"), Map.of(), new ValidationNestRelations.Limits(8, 8, 65536));
        var identity = new ValidationKeyBinding("native-object-key", List.of(objectComponent("id")), Set.of(), LIMITS);
        var emptyMap = ValidationStaticMap.compile("object-input", new TransformBody.MapProjection(Map.of()), LIMITS);
        var handles = new ArrayList<ValidationReadHandle>();
        try (var cleanup = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(8), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
             var scope = new ValidationReadScope(observation, cleanup, () -> {}, 8)) {
            var bindings = new LinkedHashMap<List<String>, ValidationNestObjectProjection.Binding>();
            bindObjectRelations(kind, sourceRef, sourceSettings, plan.children(), observation, identity, bindings, scope, handles, queryIo, decodeIo);
            var projection = new ValidationNestObjectProjection(new ValidationNestObjectProjection.Scope(observation.run(), rootTable, "output"), plan,
                    new ValidationNestObjectProjection.RootKeys(identity, identity), bindings,
                    new ValidationNestObjectProjection.Limits(LIMITS, 8 * 1024 * 1024, 64 * 1024 * 1024, 8, 100, 8));
            String sourceRead = "object-source-" + id;
            var generated = ValidationFieldCheck.generatedId(observation.run(), rootTable, "output", true, false, false,
                    value -> value instanceof ValidationValues.Typed typed && typed.logicalType().equals("text") && typed.value() instanceof String);
            var result = ValidationRootReader.read(observation, ValidationMethod.FULL_COLUMN,
                    new ValidationRootReader.Query(sourceRead, "native-object-v1", () -> openObjectRead(kind, sourceRef, sourceSettings,
                            rootTable, identity, rootKey, scope, handles, queryIo, decodeIo)),
                    new ValidationRootReader.Query("object-target-" + id, "native-object-v1", () -> openObjectRead("mongodb", targetRef, targetSettings,
                            "nest_targets", identity, rootKey, scope, handles, queryIo, decodeIo)),
                    new ValidationRelationRows.Limits(65536, LIMITS), new ValidationRootCheck.Limits(1048576, 100, 100, 20, 8), Clock.systemUTC(), scope::requireActive,
                    original -> {
                        var physical = objectPhysical(kind, rootTable, identity, original);
                        var view = projection.read(observation, new ValidationRootCheck.SampledSide(physical, original, sourceRead),
                                emptyMap.apply(original, List.of()), ValidationRootReader.SourceEvidence.empty(), Clock.systemUTC(), scope::requireActive);
                        return new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                                view.view().row(), Optional.of(physical)), List.of(), view.evidence());
                    }, actual -> new ValidationRootReader.Target(actual, objectPhysical("mongodb", "nest_targets", identity, actual)),
                    (pair, root) -> ValidationFieldCheck.checkPartial(observation, List.of(), pair.source().working(), pair.target().working(),
                            pair.sourceSample(), pair.targetSample(), generated, String::equals, new ValidationValueComparison.TraversalLimits(LIMITS, 100000),
                            pair.sourceEvidence().issues().stream().flatMap(issue -> issue.affected().stream())
                                    .map(ValidationRootCheck.Location::fieldPath).distinct().toList(), root));
            for (var handle : handles) {
                assertThat(handle.workersExited().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
                assertThat(handle.rowsReleased().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            }
            scope.close(); assertThat(scope.drained().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            return result;
        }
    }

    private static void bindObjectRelations(String kind, ConnectorRef artifact, Map<String, Object> settings,
            List<ValidationNestRelations.Relation> relations, ValidationObservation observation, ValidationKeyBinding identity,
            Map<List<String>, ValidationNestObjectProjection.Binding> bindings, ValidationReadScope scope, List<ValidationReadHandle> handles,
            ExecutorService queryIo, ExecutorService decodeIo) {
        for (var relation : relations) {
            var physical = new ValidationKeyBinding("native-relation", relation.join().stream().map(pair -> objectComponent(pair.child())).toList(), Set.of(), LIMITS);
            var lookup = new ValidationNestLookupBinding("objects", relation, relation.join().stream().map(pair -> objectComponent(pair.parent())).toList(), physical, LIMITS);
            bindings.put(relation.treePath(), new ValidationNestObjectProjection.Binding(lookup, request ->
                    new ValidationRootReader.Query("object-child-" + observation.root().rootKey().parts().getFirst().value() + "-" + String.join("-", relation.treePath()),
                            "native-object-v1", () -> openObjectRead(kind, artifact, settings, relation.alias(), physical, request.key(), scope, handles, queryIo, decodeIo)),
                    (context, raw) -> new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                            raw, Optional.of(objectPhysical(kind, relation.alias(), identity, raw))), List.of())));
            bindObjectRelations(kind, artifact, settings, relation.children(), observation, identity, bindings, scope, handles, queryIo, decodeIo);
        }
    }

    private static ValidationReadHandle openObjectRead(String kind, ConnectorRef artifact, Map<String, Object> settings, String table,
            ValidationKeyBinding binding, ValidationKey key, ValidationReadScope scope, List<ValidationReadHandle> handles,
            ExecutorService queryIo, ExecutorService decodeIo) {
        var handle = scope.open(table, guard -> PdkValidationSession.lookup(kind, artifact, settings, "retained-" + kind + "-artifact",
                new PdkValidationSession.Lookup(table, List.of(), List.of(), 2, binding, key),
                (model, codecs) -> new PdkValidationRows(model, codecs, path -> {
                    if (Set.of("id", "customer_id", "region_id").contains(path.getLast())) return PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER);
                    return PdkValidationRows.Rule.AUTO;
                }, LIMITS), (model, codecs, logical) -> {
                    var match = new LinkedHashMap<String, Object>();
                    for (int i = 0; i < binding.components().size(); i++) match.put(binding.components().get(i).path().getFirst(), logical.parts().get(i).value());
                    return match;
                }, guard, queryIo, decodeIo, 1, LIMITS));
        handles.add(handle); return handle;
    }
    private static ValidationKeyBinding.Component objectComponent(String field) {
        return new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"));
    }
    private static ValidationPhysicalIdentity objectPhysical(String kind, String table, ValidationKeyBinding binding, ValidationValues.Row row) {
        var actual = binding.read(row);
        assertThat(actual).isInstanceOf(ValidationKeyBinding.Bound.class);
        return new ValidationPhysicalIdentity(kind + "-fixture", DATABASE, kind.equals("postgres") ? "public" : null, table,
                binding.components().stream().map(ValidationKeyBinding.Component::path).toList(), ((ValidationKeyBinding.Bound) actual).key());
    }

    private static ValidationLinearRules linearRules(String kind, String table, TransformBody.MapProjection projection,
            Optional<String> predicate) {
        var parser = new DslParser();
        String filter = predicate.map(expression -> "  - { id: eligible, from: projected, type: filter, expr: '" + expression + "' }\n").orElse("");
        var pipeline = (PipelineResource) parser.parse("version: tapstate/v1\nkind: pipeline\nid: native_validation\nsource: read_source\n"
                + "transforms:\n  - { id: projected, use: mapping, from: read_source." + table + " }\n" + filter
                + "serve: { from: " + (predicate.isPresent() ? "eligible" : "projected")
                + ", sync: [{ id: output, source: read_target }] }\n");
        Resource source = parser.parse("version: tapstate/v1\nkind: source\nid: read_source\nconnector: " + kind
                + "\nconfig: {}\nmode: cdc\ntables: [" + table + "]\n");
        Resource target = parser.parse("version: tapstate/v1\nkind: source\nid: read_target\nconnector: mongodb\nconfig: {}\n");
        var mapping = new TransformResource("mapping", null, projection, null);
        var declarations = ValidationDeclarations.inspect(pipeline, Map.of("read_source", source, "read_target", target, "mapping", mapping),
                new ValidationDeclarations.Output(ValidationDeclarations.OutputKind.SYNC, "output"), ValidationDeclarations.Limits.defaults());
        return ValidationLinearRules.compile(declarations,
                new ValidationLinearRules.Limits(new ValidationPredicate.Limits(LIMITS, 2000, false), 16, 65536));
    }

    private static void verifyBorrowedReadBudget(String kind, ConnectorRef sourceRef, Map<String, Object> settings,
            ExecutorService queryIo, ExecutorService decodeIo) throws Exception {
        var logical = new ValidationKey(List.of(ValidationKey.Part.number(1)));
        var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "native-borrowed-" + kind, ValidationMode.FULL, "fixed-projection", 1, "rules"),
                new ValidationRoot("mapping", "output", logical, List.of()), 1);
        // This reservation witnesses handle ownership; it does not certify opaque driver allocations.
        var amount = new ValidationLocalBudget.Amount(1024 * 1024, 4, 1, 1);
        try (var cleanup = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, java.util.concurrent.TimeUnit.SECONDS,
                    new java.util.concurrent.ArrayBlockingQueue<>(1), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
                var budget = new ValidationLocalBudget("cluster", "node", "boot", amount, 1, () -> {});
                var run = budget.admit(observation.run(), amount, () -> {});
                var scope = new ValidationReadScope(observation, cleanup, () -> {}, 1)) {
            var reserved = run.reserve(amount);
            scope.drained().whenComplete((ignored, error) -> reserved.close());
            var handle = scope.open("source_records", guard -> rootLookup(kind, sourceRef, settings,
                    "source_records", "source_key", logical, queryIo, decodeIo, guard));
            try (var row = handle.next().orElseThrow()) {
                assertThat(row.values().fields()).containsKey("source_key");
                scope.close(); run.close();
                handle.workersExited().handle((ignored, error) -> null).toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(handle.rowsReleased().toCompletableFuture().isDone()).isFalse();
                assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
                assertThat(run.released().toCompletableFuture().isDone()).isFalse();
                assertThat(budget.snapshot().reserved()).isEqualTo(amount);
            }
            scope.drained().handle((ignored, error) -> null).toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(run.released().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            assertThat(budget.snapshot().reserved()).isEqualTo(new ValidationLocalBudget.Amount(0, 0, 0, 0));
        }
    }

    private static ValidationRootCheck rootRead(String kind, ConnectorRef sourceRef, Map<String, Object> sourceSettings,
            ConnectorRef targetRef, Map<String, Object> targetSettings, String sourceTable, String targetTable, long id,
            ValidationLinearRules rules, ExecutorService queryIo, ExecutorService decodeIo) throws Exception {
        return rootRead(kind, sourceRef, sourceSettings, targetRef, targetSettings, sourceTable, targetTable, id, rules,
                queryIo, decodeIo, new java.util.concurrent.atomic.AtomicInteger());
    }

    private static ValidationRootCheck rootRead(String kind, ConnectorRef sourceRef, Map<String, Object> sourceSettings,
            ConnectorRef targetRef, Map<String, Object> targetSettings, String sourceTable, String targetTable, long id,
            ValidationLinearRules rules, ExecutorService queryIo, ExecutorService decodeIo,
            java.util.concurrent.atomic.AtomicInteger targetOpens) throws Exception {
        var logical = new ValidationKey(List.of(ValidationKey.Part.number(id)));
        var observation = new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "native-roots-" + kind, ValidationMode.FULL, "fixed-projection", 1, "rules"),
                new ValidationRoot("mapping", "output", logical, List.of()), 1);
        var sourceIdentity = new ValidationKeyBinding("source-root", List.of(new ValidationKeyBinding.Component(List.of("source_key"),
                ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), LIMITS);
        var projectedIdentity = new ValidationKeyBinding("projected-root", List.of(new ValidationKeyBinding.Component(List.of("target_key"),
                ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), LIMITS);
        var identity = new ValidationLinearRules.Identity(observation.run(), observation.root().mappingId(), observation.root().outputId(),
                sourceIdentity, projectedIdentity);
        var generated = ValidationFieldCheck.generatedId(observation.run(), "mapping", "output", true, false, false,
                value -> value instanceof ValidationValues.Typed typed && typed.logicalType().equals("text") && typed.value() instanceof String);
        var handles = new ArrayList<ValidationReadHandle>();
        try (var cleanup = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(2), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
                var scope = new ValidationReadScope(observation, cleanup, () -> {}, 2)) {
        var checked = ValidationRootReader.read(observation, ValidationMethod.FULL_COLUMN,
                new ValidationRootReader.Query("source", "native-root-v1", () -> {
                    var handle = scope.open(sourceTable, guard ->
                            rootLookup(kind, sourceRef, sourceSettings, sourceTable, "source_key", logical, queryIo, decodeIo, guard));
                    handles.add(handle); return handle;
                }),
                new ValidationRootReader.Query("target", "native-root-v1", () -> {
                    targetOpens.incrementAndGet();
                    var handle = scope.open(targetTable, guard ->
                            rootLookup("mongodb", targetRef, targetSettings, targetTable, "target_key", logical, queryIo, decodeIo, guard));
                    handles.add(handle); return handle;
                }),
                new ValidationRelationRows.Limits(65536, LIMITS), new ValidationRootCheck.Limits(1024 * 1024, 100, 100, 20, 4),
                Clock.systemUTC(), scope::requireActive, original -> {
                    var actual = key(original, "source_key", kind).physical();
                    var physical = new ValidationPhysicalIdentity(actual.sourceId(), DATABASE, actual.schema(), sourceTable, actual.keyFields(), actual.actualKey());
                    return rules.classify(original, physical, observation, identity, scope::requireActive);
                }, original -> {
                    var actual = key(original, "target_key", "mongodb").physical();
                    return new ValidationRootReader.Target(original,
                            new ValidationPhysicalIdentity(actual.sourceId(), DATABASE, null, targetTable, actual.keyFields(), actual.actualKey()));
                }, (pair, root) -> ValidationFieldCheck.check(observation, List.of(), pair.source().working(), pair.target().working(),
                        pair.sourceSample(), pair.targetSample(), generated, String::equals, new ValidationValueComparison.TraversalLimits(LIMITS, 10000), root));
        for (var handle : handles) {
            assertThat(handle.workersExited().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
            assertThat(handle.rowsReleased().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
        }
        scope.close();
        assertThat(scope.drained().toCompletableFuture().get(5, java.util.concurrent.TimeUnit.SECONDS)).isNull();
        return checked;
        }
    }

    private static ValidationReadHandle rootLookup(String kind, ConnectorRef artifact, Map<String, Object> settings, String table,
            String keyField, ValidationKey logical, ExecutorService queryIo, ExecutorService decodeIo, Runnable requireActive) {
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
                requireActive, queryIo, decodeIo, 1, LIMITS);
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
