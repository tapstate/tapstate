package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Authorship, partial edits and safe Source reads preserve the execution policy the author wrote. */
class CloudExecutionPreservationTest {

    private static final String USER_ID = "83eb8e82-8178-460f-a93f-b3f415659fd7";
    private static final String URI = "mongodb://reader:fixture-password@db.example:27017/orders";
    private static final Map<String, Object> CONFIG = Map.of("uri", URI, "auth_source", "admin");
    private static final ExecutionSpec SOURCE_EXECUTION = new ExecutionSpec(3, new BatchSpec(64, "25ms"));
    private static final ExecutionSpec VIEW_EXECUTION = new ExecutionSpec(6, new BatchSpec(128, "1s"));
    private static final ExecutionSpec REPLACEMENT_EXECUTION = new ExecutionSpec(null, new BatchSpec(256, "2s"));

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: orders_source
            metadata: { description: Orders }
            connector: mongodb
            config: { uri: "mongodb://reader:fixture-password@db.example:27017/orders", auth_source: admin }
            mode: cdc
            tables: [ orders ]
            srs: { enabled: true }
            execution: { parallelism: 3, batch: { max_records: 64, max_wait: 25ms } }
            experimental: {}
            """;

    private static final String VIEW = """
            version: tapstate/v1
            kind: view
            id: orders_view
            metadata: { description: Orders }
            primary_key: order_id
            execution: { parallelism: 6, batch: { max_records: 128, max_wait: 1s } }
            """;

    private static final String PATCH = """
            version: tapstate/v1
            kind: source
            id: orders_source
            connector: mongodb
            """;

    private final DslParser parser = new DslParser();
    private final CanonicalWriter writer = new CanonicalWriter();

    @ParameterizedTest
    @ValueSource(strings = { "source", "view" })
    void cloudAttributionKeepsExecutionWhenCreatingTheResource(String kind) {
        Resource submitted = parser.parse(yaml(kind));
        ExecutionSpec expected = expectedExecution(kind);
        assertThat(execution(submitted)).isEqualTo(expected);

        Resource attributed = ResourceAttributionPolicy.managedCloud().attribute(USER_ID, submitted, null);

        assertThat(attributed.metadata().cloud()).isTrue();
        assertThat(attributed.metadata().userId()).isEqualTo(USER_ID);
        assertThat(execution(attributed)).isEqualTo(expected);
        assertThat(execution(parser.parse(writer.write(attributed)))).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = { "source", "view" })
    void cloudAttributionKeepsSubmittedExecutionWhenRetainingTheOriginalAuthor(String kind) {
        Resource existing = parser.parse(withCloudAuthor(yaml(kind)));
        Resource submitted = parser.parse(yaml(kind)
                .replace("description: Orders", "description: Edited")
                .replace("parallelism: 3, batch: { max_records: 64, max_wait: 25ms }",
                        "batch: { max_records: 256, max_wait: 2s }")
                .replace("parallelism: 6, batch: { max_records: 128, max_wait: 1s }",
                        "batch: { max_records: 256, max_wait: 2s }"));
        assertThat(execution(submitted)).isEqualTo(REPLACEMENT_EXECUTION);

        Resource updated = ResourceAttributionPolicy.managedCloud()
                .attribute("another-verified-user", submitted, existing);

        assertThat(updated.metadata().description()).isEqualTo("Edited");
        assertThat(updated.metadata().cloud()).isTrue();
        assertThat(updated.metadata().userId()).isEqualTo(USER_ID);
        assertThat(execution(updated)).isEqualTo(REPLACEMENT_EXECUTION);
        assertThat(execution(parser.parse(writer.write(updated)))).isEqualTo(REPLACEMENT_EXECUTION);
    }

    @Test
    void cloudApplyPersistsSourceAndStandaloneViewExecution() {
        Fixture fixture = new Fixture(true);

        ApplyResult result = fixture.apply.apply(USER_ID, List.of(draft(SOURCE), draft(VIEW)));

        assertThat(result.outcomes()).hasSize(2)
                .allSatisfy(outcome -> assertThat(outcome.change()).isEqualTo(ArtifactOutcome.Change.CREATED));
        SourceResource source = fixture.source();
        ViewResource view = (ViewResource) fixture.store.get("orders_view").orElseThrow();
        assertThat(source.execution()).isEqualTo(SOURCE_EXECUTION);
        assertThat(view.execution()).isEqualTo(VIEW_EXECUTION);
        assertThat(source.config()).isEqualTo(CONFIG);
        assertThat(source.metadata().userId()).isEqualTo(USER_ID);
        assertThat(view.metadata().userId()).isEqualTo(USER_ID);
        assertThat(fixture.store.canonical("orders_source")).contains("execution:", "max_wait: 25ms");
        assertThat(fixture.store.canonical("orders_view")).contains("execution:", "parallelism: 6");
    }

    @ParameterizedTest(name = "cloud={0}")
    @ValueSource(booleans = { false, true })
    void aPartialSourceEditKeepsAnOmittedExecution(boolean cloud) {
        Fixture fixture = seeded(cloud);

        ApplyResult result = fixture.apply.apply(USER_ID,
                List.of(draft(PATCH + "metadata: { description: Edited }\n")));

        assertChange(result, ArtifactOutcome.Change.UPDATED);
        assertThat(fixture.source().metadata().description()).isEqualTo("Edited");
        assertSourcePolicy(fixture.source(), SOURCE_EXECUTION);
        assertThat(fixture.store.writes).isEqualTo(1);
    }

    @ParameterizedTest(name = "cloud={0}")
    @ValueSource(booleans = { false, true })
    void declaringEveryOlderSourceFieldStillKeepsAnOmittedExecution(boolean cloud) {
        Fixture fixture = seeded(cloud);
        String submitted = """
                version: tapstate/v1
                kind: source
                id: orders_source
                metadata: { description: Edited }
                connector: mongodb
                config: { uri: "mongodb://reader:fixture-password@db.example:27017/orders", auth_source: admin }
                mode: cdc
                tables: [ orders ]
                srs: { enabled: true }
                experimental: {}
                """;

        ApplyPlan plan = fixture.apply.plan(List.of(draft(submitted)));
        assertThat(((SourceResource) plan.artifacts().getFirst().resource()).execution())
                .as("declaring the older fields is still a partial edit when execution is omitted")
                .isEqualTo(SOURCE_EXECUTION);
        assertChange(fixture.apply.apply(USER_ID, List.of(draft(submitted))), ArtifactOutcome.Change.UPDATED);
        assertSourcePolicy(fixture.source(), SOURCE_EXECUTION);
    }

    @ParameterizedTest(name = "cloud={0}")
    @ValueSource(booleans = { false, true })
    void aPartialSourceEditReplacesTheWholeExplicitExecution(boolean cloud) {
        Fixture fixture = seeded(cloud);
        String submitted = PATCH + "execution: { batch: { max_records: 256, max_wait: 2s } }\n";

        assertChange(fixture.apply.apply(USER_ID, List.of(draft(submitted))), ArtifactOutcome.Change.UPDATED);

        assertSourcePolicy(fixture.source(), REPLACEMENT_EXECUTION);
        assertThat(fixture.source().execution().parallelism())
                .as("an explicit execution block replaces the old block instead of merging its fields")
                .isNull();
        assertChange(fixture.apply.apply(USER_ID, List.of(draft(submitted))), ArtifactOutcome.Change.UNCHANGED);
        assertThat(fixture.store.writes).isEqualTo(1);
    }

    @ParameterizedTest(name = "cloud={0}")
    @ValueSource(booleans = { false, true })
    void anExplicitEmptyExecutionClearsTheStoredExecution(boolean cloud) {
        Fixture fixture = seeded(cloud);
        String submitted = PATCH + "execution: {}\n";
        assertThat(((SourceResource) parser.parse(submitted)).execution()).isNull();

        assertChange(fixture.apply.apply(USER_ID, List.of(draft(submitted))), ArtifactOutcome.Change.UPDATED);

        assertSourcePolicy(fixture.source(), null);
        assertThat(fixture.store.canonical("orders_source")).doesNotContain("execution:");
        assertChange(fixture.apply.apply(USER_ID, List.of(draft(submitted))), ArtifactOutcome.Change.UNCHANGED);
        assertThat(fixture.store.writes).isEqualTo(1);
    }

    @ParameterizedTest(name = "cloud={0}")
    @ValueSource(booleans = { false, true })
    void anExplicitNullExecutionIsRejectedWithoutChangingTheStoredSource(boolean cloud) {
        Fixture fixture = seeded(cloud);
        String original = fixture.store.canonical("orders_source");

        assertThatThrownBy(() -> fixture.apply.apply(USER_ID,
                List.of(draft(PATCH + "execution: null\n"))))
                .isInstanceOfSatisfying(DslException.class,
                        error -> assertThat(error.code()).isEqualTo(DslError.ILLEGAL_VALUE));

        assertThat(fixture.store.canonical("orders_source")).isEqualTo(original);
        assertSourcePolicy(fixture.source(), SOURCE_EXECUTION);
        assertThat(fixture.store.writes).isZero();
        assertThat(fixture.audit).isEmpty();
    }

    @ParameterizedTest(name = "cloud={0}")
    @ValueSource(booleans = { false, true })
    void anOrdinarySourceReadKeepsExecutionAndCanBeReappliedWithoutWriting(boolean cloud) {
        Fixture fixture = seeded(cloud);
        String original = fixture.store.canonical("orders_source");
        String originalHash = CanonicalHash.of(fixture.source());
        ArtifactQueryService query = new ArtifactQueryService(fixture.store);

        String projection = new SourceReadProjection().canonicalForRead(fixture.source());
        StoredArtifact read = query.get("orders_source").orElseThrow();
        assertThat(read.canonicalForm()).isEqualTo(projection)
                .contains("execution:", "parallelism: 3", "max_records: 64", "max_wait: 25ms")
                .doesNotContain("config:", "fixture-password", "db.example", "auth_source:");
        SourceResource displayed = (SourceResource) parser.parse(read.canonicalForm());
        assertThat(displayed.config()).isEmpty();
        assertThat(displayed.execution()).isEqualTo(SOURCE_EXECUTION);
        assertThat(read.contentHash()).isEqualTo(originalHash);
        assertThat(query.list()).singleElement().satisfies(row -> {
            assertThat(row.canonicalForm()).isEqualTo(read.canonicalForm());
            assertThat(row.contentHash()).isEqualTo(originalHash);
        });
        assertThat(query.list("source")).singleElement().satisfies(row ->
                assertThat(row.canonicalForm()).isEqualTo(read.canonicalForm()));
        assertSourcePolicy((SourceResource) query.getResource("orders_source").orElseThrow().resource(),
                SOURCE_EXECUTION);

        assertChange(fixture.apply.apply(USER_ID, List.of(draft(read.canonicalForm()))),
                ArtifactOutcome.Change.UNCHANGED);

        assertSourcePolicy(fixture.source(), SOURCE_EXECUTION);
        assertThat(fixture.store.canonical("orders_source")).isEqualTo(original);
        assertThat(CanonicalHash.of(fixture.source())).isEqualTo(originalHash);
        assertThat(fixture.store.writes).isZero();
        assertThat(fixture.audit).isEmpty();
    }

    private Fixture seeded(boolean cloud) {
        Fixture fixture = new Fixture(cloud);
        SourceResource stored = (SourceResource) parser.parse(cloud ? withCloudAuthor(SOURCE) : SOURCE);
        assertSourcePolicy(stored, SOURCE_EXECUTION);
        // Seed an existing authoritative record so a broken create cannot mask an independent edit/read defect.
        fixture.store.seed(stored);
        return fixture;
    }

    private static void assertSourcePolicy(SourceResource source, ExecutionSpec expected) {
        assertThat(source.config()).isEqualTo(CONFIG);
        assertThat(source.mode()).isEqualTo(SourceMode.CDC);
        assertThat(source.tables()).containsExactly(TableRef.literal("orders"));
        assertThat(source.srsEnabled()).isTrue();
        assertThat(source.execution()).isEqualTo(expected);
    }

    private static void assertChange(ApplyResult result, ArtifactOutcome.Change expected) {
        assertThat(result.outcomes()).singleElement().satisfies(outcome ->
                assertThat(outcome.change()).isEqualTo(expected));
    }

    private static ArtifactDraft draft(String yaml) {
        return new ArtifactDraft(null, yaml);
    }

    private static String yaml(String kind) {
        return "source".equals(kind) ? SOURCE : VIEW;
    }

    private static ExecutionSpec expectedExecution(String kind) {
        return "source".equals(kind) ? SOURCE_EXECUTION : VIEW_EXECUTION;
    }

    private static ExecutionSpec execution(Resource resource) {
        return resource instanceof SourceResource source ? source.execution() : ((ViewResource) resource).execution();
    }

    private static String withCloudAuthor(String yaml) {
        return yaml.replace("metadata: { description: Orders }",
                "metadata: { description: Orders, cloud: true, user_id: " + USER_ID + " }");
    }

    private static final class Fixture {
        private final CanonicalStore store = new CanonicalStore();
        private final List<AuditRecord> audit = new ArrayList<>();
        private final ApplyService apply;

        private Fixture(boolean cloud) {
            apply = new ApplyService(TapstateCatalog::load, store,
                    new AuditGate(audit::add, Clock.systemUTC()), new EmptySchemaStore(),
                    PlanAdvisories.none(), SchemaDerivation.none(), null,
                    cloud ? ResourceAttributionPolicy.managedCloud() : ResourceAttributionPolicy.onPrem(),
                    cloud ? StateDatabasePolicy.CLOUD : StateDatabasePolicy.ON_PREM);
        }

        private SourceResource source() {
            return (SourceResource) store.get("orders_source").orElseThrow();
        }
    }

    /** Stores canonical text and reparses it on read, including the conditions copied by partial edits. */
    private static final class CanonicalStore implements ArtifactStore {
        private final DslParser parser = new DslParser();
        private final CanonicalWriter writer = new CanonicalWriter();
        private final Map<String, String> byId = new LinkedHashMap<>();
        private int writes;

        private void seed(Resource resource) {
            byId.put(resource.id(), writer.write(resource));
        }

        private String canonical(String id) {
            return byId.get(id);
        }

        @Override
        public void saveAll(List<Resource> resources) {
            saveAll(resources, Map.of());
        }

        @Override
        public Optional<String> saveAll(List<Resource> resources, Map<String, String> expectedHashes) {
            for (var expected : expectedHashes.entrySet()) {
                Resource stored = get(expected.getKey()).orElse(null);
                if (stored == null || !CanonicalHash.of(stored).equals(expected.getValue())) {
                    return Optional.of(expected.getKey());
                }
            }
            Map<String, String> staged = new LinkedHashMap<>();
            for (Resource resource : resources) {
                staged.put(resource.id(), writer.write(resource));
            }
            byId.putAll(staged);
            writes += resources.size();
            return Optional.empty();
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id)).map(parser::parse);
        }

        @Override
        public List<Resource> list() {
            return byId.values().stream().map(parser::parse).toList();
        }
    }
}
