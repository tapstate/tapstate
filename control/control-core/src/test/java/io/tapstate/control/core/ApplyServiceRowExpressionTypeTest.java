package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.DslError;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Apply judges a batch's row expressions against the columns its sources were discovered to hold.
 * This is where the type check has anything to work with: the offline check sees only the document,
 * while apply can reach the schema store and read what discovery resolved each column to.
 *
 * <p>The refusal has to land before anything is written. An apply that refused the pipeline but
 * stored it anyway would leave exactly the artifact the refusal exists to keep out.
 */
class ApplyServiceRowExpressionTypeTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-07-15T10:15:30Z"), ZoneOffset.UTC);

    private static final String SOURCE = source("[ orders ]");

    /** The source document, varying only in which tables it selects — the rest is fixed noise. */
    private static String source(String tables) {
        return """
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: mysql
                config: { host: 10.10.0.5, username: u, password: p }
                mode: cdc
                tables: %s
                """.formatted(tables);
    }

    private final InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
    private final InMemorySchemaStore schemas = new InMemorySchemaStore();
    private final ApplyService service = new ApplyService(
            TapstateCatalog::load, artifacts, new AuditGate(record -> { }, FIXED_CLOCK), schemas,
            PlanAdvisories.none());

    private static String pipeline(String expr) {
        return pipeline("orders", expr);
    }

    /** The filter step reads {@code from}, which is a table name or a regex only a connection resolves. */
    private static String pipeline(String from, String expr) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: orders_out
                source: src_orders
                transforms:
                  - { id: keep, from: [%s], type: filter, expr: "%s" }
                serve:
                  from: keep
                  sync: [ { id: out, source: src_orders, write_mode: upsert } ]
                """.formatted(from, expr);
    }

    private List<ArtifactDraft> batch(String expr) {
        return batch(SOURCE, expr);
    }

    private List<ArtifactDraft> batch(String source, String expr) {
        return List.of(new ArtifactDraft("src_orders.tap.yml", source),
                new ArtifactDraft("orders_out.tap.yml", pipeline(expr)));
    }

    private List<ArtifactDraft> batch(String source, String from, String expr) {
        return List.of(new ArtifactDraft("src_orders.tap.yml", source),
                new ArtifactDraft("orders_out.tap.yml", pipeline(from, expr)));
    }

    /** Records a whole connection's discovery: several tables, as one discovery of one connection. */
    private void discovered(String connectionId, SourceTable... tables) {
        schemas.save(new DiscoveredSourceModel(connectionId, "mysql", 0L, new SourceModel(List.of(tables))));
    }

    private static SourceTable table(String name, String column, TapstateType type) {
        // Keyed on its first column, because these pipelines upsert and an upsert needs a key. What
        // is under test here is the type verdict; a keyless table would be refused before reaching it.
        return new SourceTable(
                name, List.of(new SourceField(column, column + "_native", type)), List.of(column),
                List.of());
    }

    /** Records the discovery of one table's columns for a connection, as discovery would have. */
    private void discovered(String connectionId, String table, Map<String, TapstateType> columns) {
        discovered(connectionId, "mysql", table, columns);
    }

    /** The same, for a discovery that ran through a named connector rather than the source's own. */
    private void discovered(
            String connectionId, String connectorId, String table, Map<String, TapstateType> columns) {
        List<SourceField> fields = new ArrayList<>();
        columns.forEach((name, type) -> fields.add(new SourceField(name, name + "_native", type)));
        // Keyed on its first column, for the same reason as table(): these pipelines upsert.
        List<String> key = fields.isEmpty() ? List.of() : List.of(fields.get(0).name());
        schemas.save(new DiscoveredSourceModel(connectionId, connectorId, 0L,
                new SourceModel(List.of(new SourceTable(table, fields, key, List.of())))));
    }

    @Test
    @DisplayName("an expression computing on a column whose type cannot survive it is refused by apply")
    void applyRefusesAnUnsupportedColumnType() {
        discovered("src_orders", "orders", Map.of("amount", TapstateType.DECIMAL));

        DslException thrown = catchThrowableOfType(DslException.class,
                () -> service.apply("tester", batch("after.amount * 2 > 0")));

        assertThat(thrown.code()).isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED);
        assertThat(thrown.args()).containsEntry("column", "amount");
    }

    @Test
    @DisplayName("a refused apply writes nothing, so the refused pipeline is not left stored")
    void aRefusedApplyStoresNothing() {
        discovered("src_orders", "orders", Map.of("amount", TapstateType.DECIMAL));

        catchThrowableOfType(DslException.class, () -> service.apply("tester", batch("after.amount * 2 > 0")));

        assertThat(artifacts.get("orders_out")).isEmpty();
        assertThat(artifacts.get("src_orders")).isEmpty();
        assertThat(artifacts.saved).isEmpty();
    }

    /**
     * The write-free validate verb plans the batch, so it reaches this check too and an author asking
     * "would this apply?" is told the same no. The verb answers with a diagnostic rather than letting
     * the refusal escape, which is what keeps a refused expression a reported verdict instead of a
     * server fault. Moving the check onto the write path alone would silently take it out of this
     * answer, so the case is pinned on the verb rather than only on apply.
     */
    @Test
    @DisplayName("the write-free validate verb reports the same refusal, as a diagnostic and not a throw")
    void validateReportsTheRefusalAsADiagnostic() {
        discovered("src_orders", "orders", Map.of("amount", TapstateType.DECIMAL));

        ArtifactValidationResult result = service.validate(batch("after.amount * 2 > 0"));

        assertThat(result.valid()).isFalse();
        assertThat(result.diagnostics()).singleElement().satisfies(diagnostic -> {
            assertThat(diagnostic.code()).isEqualTo(DslError.ROW_EXPRESSION_TYPE_UNSUPPORTED.code());
            assertThat(diagnostic.params()).containsEntry("column", "amount");
        });
        assertThat(artifacts.saved).isEmpty();
    }

    @Test
    @DisplayName("the same expression over a lossless numeric column applies and is stored")
    void applyAcceptsALosslessColumnType() {
        discovered("src_orders", "orders", Map.of("amount", TapstateType.INT64));

        assertThatCode(() -> service.apply("tester", batch("after.amount * 2 > 0")))
                .doesNotThrowAnyException();
        assertThat(artifacts.get("orders_out")).isPresent();
    }

    @Test
    @DisplayName("reading a row field from a source apply cannot find a model for is refused, naming it")
    void anUndiscoveredSourceIsRefused() {
        DslException thrown = catchThrowableOfType(DslException.class,
                () -> service.apply("tester", batch("after.amount * 2 > 0")));

        assertThat(thrown.code()).isEqualTo(DslError.ROW_EXPRESSION_NEEDS_DISCOVERY);
        assertThat(thrown.args()).containsEntry("source", "src_orders");
        assertThat(artifacts.saved).isEmpty();
    }

    @Test
    @DisplayName("an expression reading no row field applies without any discovery")
    void anExpressionWithoutRowAccessNeedsNoDiscovery() {
        assertThatCode(() -> service.apply("tester", batch("op == 'i'"))).doesNotThrowAnyException();
    }

    /**
     * The column here is one the expression survives, so a model that was consulted would let this
     * apply through. It is refused instead, which is the only outcome that shows the model was not
     * consulted at all - the assertion would pass on a green run if it merely named some other code.
     *
     * <p>A model carries the types the connector that produced it declares, and a different connector
     * spells its types differently. So a model discovered through one connector says nothing about a
     * source now configured to read through another, even where both kept the connection's id.
     */
    @Test
    @DisplayName("a model discovered through a different connector does not count as this source's")
    void aModelDiscoveredThroughAnotherConnectorIsNotConsulted() {
        discovered("src_orders", "mongodb", "orders", Map.of("amount", TapstateType.INT64));

        DslException thrown = catchThrowableOfType(DslException.class,
                () -> service.apply("tester", batch("after.amount * 2 > 0")));

        assertThat(thrown.code()).isEqualTo(DslError.ROW_EXPRESSION_NEEDS_DISCOVERY);
        assertThat(thrown.args()).containsEntry("source", "src_orders");
        assertThat(artifacts.saved).isEmpty();
    }

    /**
     * A batch of endpoints alone is an ordinary thing to apply, and only a pipeline can carry a row
     * expression - so there is nothing for a discovered model to be judged against. Asserting on the
     * reads rather than on the outcome is the point: the outcome is the same either way, which is
     * exactly why a store round trip per source would be paid here forever without anyone noticing.
     */
    @Test
    @DisplayName("a batch carrying no pipeline consults the schema store not at all")
    void aBatchWithoutAPipelineReadsNoSchema() {
        service.apply("tester", List.of(new ArtifactDraft("src_orders.tap.yml", SOURCE)));

        assertThat(schemas.reads).isZero();
    }

    /**
     * Two tables of one source typing a column differently is not a conflict to resolve — it is two
     * tables, each with its own column. The step reads {@code orders}, where the column is a number
     * and the comparison holds, and nothing about the archive table bears on it. Pooling them would
     * have to call the column unresolved and refuse this, which is the shape most real databases take
     * (one name, two tables, two types) rather than a corner of it.
     */
    @Test
    @DisplayName("two tables of one source typing a column differently do not pool into an unresolved one")
    void columnsAreNotPooledAcrossTheTablesOfOneSource() {
        discovered("src_orders",
                table("orders", "amount", TapstateType.INT64),
                table("orders_archive", "amount", TapstateType.STRING));

        assertThatCode(() -> service.apply(
                "tester", batch(source("[ orders, orders_archive ]"), "after.amount > 0")))
                .doesNotThrowAnyException();
        assertThat(artifacts.get("orders_out")).isPresent();
    }

    /**
     * Discovery runs per connection, so the stored model carries every table the connection can see —
     * not the ones this source selected. Folding all of them together would let a table the source
     * never reads decide the type of a column it does: the conflict here is with {@code sessions},
     * which the source does not select, and {@code orders.id} resolved perfectly well on its own.
     *
     * <p>This is the common case rather than a corner: a column named {@code id} or {@code status}
     * typed differently in two unrelated tables of one database is ordinary, and folding the whole
     * connection in would refuse most expressions on most real databases.
     */
    @Test
    @DisplayName("a conflict in a table the source does not select leaves the columns it does read resolved")
    void aConflictInAnUnselectedTableDoesNotReachTheSource() {
        discovered("src_orders",
                table("orders", "id", TapstateType.INT64),
                table("sessions", "id", TapstateType.STRING));

        assertThatCode(() -> service.apply("tester", batch("after.id > 0"))).doesNotThrowAnyException();
        assertThat(artifacts.get("orders_out")).isPresent();
    }

    /**
     * A step that names the table it reads is judged on that table. The model here carries no table by
     * that name — discovery may predate it, or the connector may report qualified names — and a table
     * the model does not carry reads the same way a column it does not carry does: the model is what
     * the last discovery found, and its silence is not evidence of a problem.
     */
    @Test
    @DisplayName("a step naming a table the model does not carry has nothing to be judged against")
    void aStepNamingATableTheModelDoesNotCarryIsNotJudged() {
        discovered("src_orders", table("legacy_orders", "amount", TapstateType.DECIMAL));

        assertThatCode(() -> service.apply("tester", batch("after.amount * 2 > 0")))
                .doesNotThrowAnyException();
    }

    /**
     * A Source exposes only its selected tables. When a stale discovery no longer carries any of
     * them, a dynamic {@code from:} cannot reach tables outside that declared scope merely because
     * they are still present in the connection-level model.
     */
    @Test
    @DisplayName("an unresolvable from: excludes every table outside the Source selection")
    void anUnresolvableReferenceExcludesTablesOutsideTheSourceSelection() {
        discovered("src_orders", table("legacy_orders", "amount", TapstateType.DECIMAL));

        assertThatCode(() -> service.apply(
                "tester", batch(source("[ orders ]"), "/.*/", "after.amount * 2 > 0")))
                .doesNotThrowAnyException();
        assertThat(artifacts.get("orders_out")).isPresent();
    }

    @Test
    @DisplayName("a regex selector picks the discovered tables it matches, and leaves the rest out")
    void aRegexSelectorSelectsTheTablesItMatches() {
        discovered("src_orders",
                table("ord_2026", "id", TapstateType.INT64),
                table("sessions", "id", TapstateType.STRING));

        assertThatCode(() -> service.apply("tester", batch(source("[ /ord_.*/ ]"), "after.id > 0")))
                .doesNotThrowAnyException();
    }

    // ---- doubles -------------------------------------------------------------------------

    private static final class InMemoryArtifactStore implements ArtifactStore {
        final Map<String, Resource> byId = new LinkedHashMap<>();
        final List<Resource> saved = new ArrayList<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            saved.addAll(artifacts);
            artifacts.forEach(r -> byId.put(r.id(), r));
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

    private static final class InMemorySchemaStore implements SchemaStore {
        private final Map<String, DiscoveredSourceModel> byConnection = new HashMap<>();
        /** Reads, counted: a real store answers each over the network, so an unneeded one is a cost. */
        int reads;

        @Override
        public void save(DiscoveredSourceModel discovered) {
            byConnection.put(discovered.connectionId(), discovered);
        }

        @Override
        public Optional<DiscoveredSourceModel> get(String connectionId) {
            reads++;
            return Optional.ofNullable(byConnection.get(connectionId));
        }
    }
}
