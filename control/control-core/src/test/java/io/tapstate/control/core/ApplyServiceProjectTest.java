package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.ProjectLabel;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Apply from a project: what it labels, what it refuses, and what it leaves alone. The store is in
 * memory so each rule is seen on its own; the same rules over HTTP and a real store are the
 * integration cases in the app module.
 */
class ApplyServiceProjectTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-09-28T08:00:00Z"), ZoneOffset.UTC);

    private final InMemoryArtifactStore store = new InMemoryArtifactStore();
    private final ApplyService service = new ApplyService(
            TapstateCatalog::load, store, new AuditGate(record -> { }, FIXED_CLOCK), new EmptySchemaStore(),
            PlanAdvisories.none(), SchemaDerivation.none());

    private static String source(String id, String labels) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                %sconnector: mysql
                config: { host: 10.30.0.5, database: orders, username: u, password: p }
                mode: cdc
                tables: [ orders ]
                """.formatted(id, labels);
    }

    private static String source(String id) {
        return source(id, "");
    }

    private static ArtifactDraft draft(String content) {
        return new ArtifactDraft("file.tap.yml", content);
    }

    @Test
    @DisplayName("a resource stored with no project is adopted by the first project to apply it, out loud")
    void anUnownedResourceIsClaimedWithAWarning() {
        service.apply("author", List.of(draft(source("orders_db"))));

        ApplyResult result = service.apply("author", List.of(draft(source("orders_db"))), "orders_sync");

        assertThat(ProjectLabel.of(store.get("orders_db").orElseThrow())).isEqualTo("orders_sync");
        assertThat(result.warnings()).extracting(ValidationDiagnostic::code)
                .contains(ArtifactError.PROJECT_CLAIMED.code());
        assertThat(ArtifactError.PROJECT_CLAIMED.severity()).isEqualTo(io.tapstate.core.common.Severity.WARNING);
    }

    @Test
    @DisplayName("a resource new to the server is labelled with no warning")
    void aNewResourceIsLabelledQuietly() {
        ApplyResult result = service.apply("author", List.of(draft(source("orders_db"))), "orders_sync");

        assertThat(ProjectLabel.of(store.get("orders_db").orElseThrow())).isEqualTo("orders_sync");
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    @DisplayName("an id owned by another project refuses the whole batch and writes nothing")
    void anotherProjectsIdRefusesTheBatch() {
        service.apply("author", List.of(draft(source("orders_db"))), "orders_sync");
        Resource before = store.get("orders_db").orElseThrow();

        TapstateException refused = catchThrowableOfType(TapstateException.class, () -> service.apply("author",
                List.of(draft(source("billing_db")), draft(source("orders_db"))), "billing"));

        assertThat(refused.code()).isEqualTo(ArtifactError.PROJECT_ID_TAKEN);
        assertThat(refused.args()).containsEntry("owner", "orders_sync").containsEntry("project", "billing")
                .containsEntry("id", "orders_db").containsEntry("kind", "source");
        assertThat(store.get("billing_db")).isEmpty();
        assertThat(CanonicalHash.of(store.get("orders_db").orElseThrow())).isEqualTo(CanonicalHash.of(before));
    }

    @Test
    @DisplayName("a hand-written project label naming another project is refused, naming the file")
    void aHandWrittenLabelMustAgree() {
        DslException refused = catchThrowableOfType(DslException.class, () -> service.apply("author",
                List.of(draft(source("orders_db", "metadata: { labels: { project: billing } }\n"))),
                "orders_sync"));

        assertThat(refused.code()).isEqualTo(DslError.RESERVED_LABEL);
        assertThat(refused.args()).containsEntry("value", "billing").containsEntry("expected", "orders_sync");
        assertThat(refused.source()).isEqualTo("file.tap.yml");
        assertThat(store.get("orders_db")).isEmpty();
    }

    @Test
    @DisplayName("nothing is applied as the Default project by name; it holds what carries no label")
    void theDefaultProjectCannotBeNamedOnApply() {
        DslException refused = catchThrowableOfType(DslException.class, () -> service.apply("author",
                List.of(draft(source("orders_db"))), "default"));

        assertThat(refused.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(refused.path()).isEqualTo("project");
        assertThat(store.get("orders_db")).isEmpty();
    }

    @Test
    @DisplayName("a hand-written project label that agrees is accepted")
    void aHandWrittenLabelThatAgreesIsFine() {
        service.apply("author",
                List.of(draft(source("orders_db", "metadata: { labels: { project: orders_sync } }\n"))),
                "orders_sync");

        assertThat(ProjectLabel.of(store.get("orders_db").orElseThrow())).isEqualTo("orders_sync");
    }

    @Test
    @DisplayName("an edit that names no project keeps the project the stored resource belongs to")
    void anEditFromAClientWithoutProjectsKeepsTheOwner() {
        service.apply("author", List.of(draft(source("orders_db"))), "orders_sync");

        service.apply("author", List.of(draft(source("orders_db").replace("10.30.0.5", "10.30.0.6"))));

        Resource stored = store.get("orders_db").orElseThrow();
        assertThat(ProjectLabel.of(stored)).isEqualTo("orders_sync");
    }

    @Test
    @DisplayName("an unchanged re-apply from the owning project is a no-op")
    void reApplyingIsANoOp() {
        service.apply("author", List.of(draft(source("orders_db"))), "orders_sync");

        ApplyResult again = service.apply("author", List.of(draft(source("orders_db"))), "orders_sync");

        assertThat(again.outcomes()).extracting(ArtifactOutcome::change)
                .containsExactly(ArtifactOutcome.Change.UNCHANGED);
    }

    private static final class InMemoryArtifactStore implements ArtifactStore {
        final Map<String, Resource> byId = new LinkedHashMap<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(r -> byId.put(r.id(), r));
        }

        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
            for (Map.Entry<String, String> precondition : expectedContentHashes.entrySet()) {
                Resource stored = byId.get(precondition.getKey());
                if (stored == null || !CanonicalHash.of(stored).equals(precondition.getValue())) {
                    return Optional.of(precondition.getKey());
                }
            }
            saveAll(artifacts);
            return Optional.empty();
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(byId.values());
        }
    }
}
