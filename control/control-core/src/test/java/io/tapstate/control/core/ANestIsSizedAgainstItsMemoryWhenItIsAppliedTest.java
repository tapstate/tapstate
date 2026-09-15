package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The row counts discovery took are what sizing reads, and what it has to say arrives on the batch that
 * was applied — as a warning beside the outcome, never as a reason the batch did not go through.
 *
 * <p>This is the whole chain end to end: a count taken at discovery, stored, read back when a batch is
 * planned, carried into the rule as the tables the sources hold, and returned as a coded finding. Each
 * hop can drop the count without anything failing — a table with no count sizes as a table with no rows,
 * which is silence rather than an error — so the chain is worth one test that crosses all of it.
 */
class ANestIsSizedAgainstItsMemoryWhenItIsAppliedTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-08-09T10:15:30Z"), ZoneOffset.UTC);
    private static final long DEPLOYMENT_BUDGET = 100_000L;

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ customers, orders, lines ]
            """;

    /** customers → orders → lines: orders has children, so it holds state of its own. */
    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: orders_doc
            source: src_orders
            transforms:
              - id: doc
                type: nest
                from: { c: customers, o: orders, l: lines }
                root:
                  from: c
                  key: [customer_id]
                  embed:
                    - from: o
                      on: { customer_id: customer_id }
                      as: array
                      path: orders
                      arrayKey: [order_id]
                      embed:
                        - from: l
                          on: { order_id: order_id }
                          as: array
                          path: lines
                          arrayKey: [line_id]
            serve:
              from: doc
              sync: [ { id: out, source: src_orders, write_mode: upsert } ]
            """;

    private final InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
    private final InMemorySchemaStore schemas = new InMemorySchemaStore();
    private final ApplyService service = new ApplyService(
            TapstateCatalog::load, artifacts, new AuditGate(record -> { }, FIXED_CLOCK), schemas,
            new NestSizingAdvisories(DEPLOYMENT_BUDGET), SchemaDerivation.none());

    /** Records what discovery found: three tables, each with the row count it reported (null = none). */
    private void discovered(Long customers, Long orders, Long lines) {
        List<SourceTable> tables = new ArrayList<>();
        tables.add(table("customers", "customer_id", customers));
        tables.add(table("orders", "order_id", orders));
        tables.add(table("lines", "line_id", lines));
        schemas.save(new DiscoveredSourceModel("src_orders", "mysql", 0L, new SourceModel(tables)));
    }

    private static SourceTable table(String name, String key, Long rows) {
        List<SourceField> fields = List.of(
                new SourceField("customer_id", "varchar", TapstateType.STRING),
                new SourceField("order_id", "varchar", TapstateType.STRING),
                new SourceField("line_id", "varchar", TapstateType.STRING));
        return new SourceTable(name, fields, List.of(key), List.of(), rows);
    }

    private static List<ArtifactDraft> batch() {
        return List.of(new ArtifactDraft("source.yaml", SOURCE), new ArtifactDraft("pipeline.yaml", PIPELINE));
    }

    private static ValidationDiagnostic withCode(List<ValidationDiagnostic> warnings, String code) {
        return warnings.stream().filter(w -> w.code().equals(code)).findFirst().orElse(null);
    }

    @Test
    void aNestFarOverItsBudgetIsWarnedAboutRatherThanRefused() {
        discovered(9_000_000L, 1_000L, 1_000L);

        ArtifactValidationResult result = service.validate(batch());

        assertThat(result.valid()).as("over budget is a cost, not a reason to refuse").isTrue();
        assertThat(result.diagnostics()).as("nothing was refused, so nothing is a refusal reason").isEmpty();
        ValidationDiagnostic warning =
                withCode(result.warnings(), "nest.state-far-exceeds-memory-budget");
        assertThat(warning).as("the oversized level is reported").isNotNull();
        assertThat(warning.params())
                .containsEntry("pipeline", "orders_doc")
                .containsEntry("embedPath", "$root")
                .containsEntry("estimatedEntries", 9_000_000L)
                .containsEntry("budget", DEPLOYMENT_BUDGET)
                .containsEntry("multiple", 90L);
    }

    @Test
    void theCountTakenAtDiscoveryIsTheOneSizedAgainst() {
        // The same tree, sized only by what the counts say: nothing else about the batch changed.
        discovered(1_000L, 9_000_000L, 1_000L);

        List<ValidationDiagnostic> warnings = service.validate(batch()).warnings();

        ValidationDiagnostic warning = withCode(warnings, "nest.state-far-exceeds-memory-budget");
        assertThat(warning).isNotNull();
        assertThat(warning.params()).containsEntry("embedPath", "orders");
    }

    @Test
    void aNestThatFitsInMemoryIsAppliedWithNothingToSay() {
        // lines is far past the line and stays silent: a leaf is elements inside a document rather than
        // entries of a level of its own. Over the line rather than merely over the budget, so this keeps
        // failing if leaves ever start being counted as levels.
        discovered(1_000L, 1_000L, 5_000_000L);

        ArtifactValidationResult result = service.validate(batch());

        assertThat(result.valid()).isTrue();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    void aTableDiscoveryNeverCountedIsReportedAsAGapRatherThanSizedAsEmpty() {
        discovered(null, 1_000L, 1_000L);

        List<ValidationDiagnostic> warnings = service.validate(batch()).warnings();

        ValidationDiagnostic gap = withCode(warnings, "nest.capacity-estimate-incomplete");
        assertThat(gap).as("a level nobody could size says so").isNotNull();
        assertThat(gap.params()).containsEntry("embedPath", "$root").containsEntry("counted", 0L);
        assertThat(withCode(warnings, "nest.state-far-exceeds-memory-budget"))
                .as("an uncounted table is never reported as fitting").isNull();
    }

    @Test
    void theWarningsReachTheVerbThatWritesTheBatchTooNotOnlyTheOneThatChecksIt() {
        discovered(9_000_000L, 1_000L, 1_000L);

        ApplyResult applied = service.apply("tester", batch());

        assertThat(withCode(applied.warnings(), "nest.state-far-exceeds-memory-budget"))
                .as("apply carries what validate would have said").isNotNull();
        assertThat(applied.outcomes()).as("and the batch was still written").hasSize(2);
    }

    /** An artifact store that answers from memory, so a batch can be applied without one. */
    private static final class InMemoryArtifactStore implements ArtifactStore {
        private final Map<String, Resource> byId = new java.util.LinkedHashMap<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
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

    /** A schema store that answers from memory, standing in for the one discovery writes to. */
    private static final class InMemorySchemaStore implements SchemaStore {
        private final Map<String, DiscoveredSourceModel> byConnection = new HashMap<>();

        @Override
        public void save(DiscoveredSourceModel discovered) {
            byConnection.put(discovered.connectionId(), discovered);
        }

        @Override
        public Optional<DiscoveredSourceModel> get(String connectionId) {
            return Optional.ofNullable(byConnection.get(connectionId));
        }
    }
}
