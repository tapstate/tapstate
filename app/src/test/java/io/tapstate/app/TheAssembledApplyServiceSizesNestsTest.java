package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.LivePipelines;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.ArtifactValidationResult;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.ValidationDiagnostic;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateType;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

/**
 * The assembly hands the apply verb a rule that actually sizes nests, and hands it the budget this
 * process runs on.
 *
 * <p>Worth a test of its own because the wiring is one argument in one factory method, and getting it
 * wrong is silent in both directions: pass the empty pass and every apply reports nothing, which reads
 * exactly like a batch with nothing wrong; pass a number that is not the running one and every estimate
 * is judged against a budget no pipeline will run on. Neither shows up as a failure anywhere else — the
 * channel was quiet before this and would simply go on being quiet.
 */
class TheAssembledApplyServiceSizesNestsTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-08-09T10:15:30Z"), ZoneOffset.UTC);

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_orders
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ customers, orders, lines ]
            """;

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

    private final InMemorySchemaStore schemas = new InMemorySchemaStore();

    /**
     * The apply service exactly as the assembly builds it, over the budget {@code settings} carries.
     * A null stands for the deployment that runs no engine, where nothing defines those settings.
     */
    private ApplyService assembled(NestSettings settings) {
        ConnectorCatalogView catalog = new ConnectorCatalogView(
                TapstateCatalog.load(), new NoRegisteredConnectors(), specStore(), registry());
        // A real lifecycle reading rather than none, so this stays a test of the assembly as it is
        // built: nothing here is running, so the refusal it carries never fires and sizing is what is
        // measured.
        return new ControlPlaneConfiguration().applyService(
                new InMemoryArtifactStore(), catalog,
                new AuditGate(record -> { }, FIXED_CLOCK), schemas, settings,
                new LivePipelines(new InMemoryDesiredStore(), new InMemoryStateStore()));
    }

    /** One customer table of {@code rows} rows, plus the two the tree also reads. */
    private void discovered(long customerRows) {
        List<SourceField> fields = List.of(
                new SourceField("customer_id", "varchar", TapstateType.STRING),
                new SourceField("order_id", "varchar", TapstateType.STRING),
                new SourceField("line_id", "varchar", TapstateType.STRING));
        schemas.save(new DiscoveredSourceModel("src_orders", "mysql", 0L, new SourceModel(List.of(
                new SourceTable("customers", fields, List.of("customer_id"), List.of(), customerRows),
                new SourceTable("orders", fields, List.of("order_id"), List.of(), 1_000L),
                new SourceTable("lines", fields, List.of("line_id"), List.of(), 1_000L)))));
    }

    private static ValidationDiagnostic sizingWarning(ArtifactValidationResult result) {
        return result.warnings().stream()
                .filter(w -> w.code().equals("nest.state-far-exceeds-memory-budget"))
                .findFirst()
                .orElse(null);
    }

    private static List<ArtifactDraft> batch() {
        return List.of(new ArtifactDraft("source.yaml", SOURCE), new ArtifactDraft("pipeline.yaml", PIPELINE));
    }

    @Test
    void theApplyVerbTheAssemblyBuildsReportsAnOversizedNest() {
        discovered(9_000_000L);

        ArtifactValidationResult result = assembled(NestSettings.defaults()).validate(batch());

        assertThat(result.valid()).isTrue();
        assertThat(sizingWarning(result))
                .as("the assembly wired a rule that sizes, not the pass that finds nothing")
                .isNotNull();
    }

    @Test
    void theBudgetThisProcessRunsOnIsTheOneEstimatesAreJudgedAgainst() {
        // The same tree and the same counts either side of a budget change. Reading the running settings
        // rather than a copy is what makes a deployment that raised its budget stop being warned.
        discovered(9_000_000L);

        ValidationDiagnostic onDefault = sizingWarning(assembled(NestSettings.defaults()).validate(batch()));
        ValidationDiagnostic onRaised = sizingWarning(assembled(
                NestSettings.defaults().withEntriesHeldInMemory(9_000_000L)).validate(batch()));

        assertThat(onDefault).as("far over the budget it was started with").isNotNull();
        assertThat(onDefault.params())
                .containsEntry("budget", NestSettings.DEFAULT_ENTRIES_HELD_IN_MEMORY);
        assertThat(onRaised).as("and within a budget raised to hold it").isNull();
    }

    @Test
    void aNestThatFitsIsAppliedWithNothingToSay() {
        discovered(1_000L);

        assertThat(assembled(NestSettings.defaults()).validate(batch()).warnings()).isEmpty();
    }

    @Test
    void aControlPlaneRunningNoEngineStillComesUpAndStillSizes() {
        // The bean carrying nest settings belongs to the engine's substrate, and a deployment can run the
        // control plane without one. Demanding it would take the whole control plane down in exactly the
        // shape that never nests anything locally — an artifact endpoint refusing to start because of a
        // rule that only advises. It sizes against the built-in default instead, which is the number a
        // nest runs on wherever it does run, absent a deployment saying otherwise.
        discovered(9_000_000L);

        ArtifactValidationResult result = assembled(null).validate(batch());

        assertThat(result.valid()).isTrue();
        assertThat(sizingWarning(result)).as("still sized, on the built-in default").isNotNull();
        assertThat(sizingWarning(result).params())
                .containsEntry("budget", NestSettings.DEFAULT_ENTRIES_HELD_IN_MEMORY);
    }

    /** A catalog store with nothing registered, so the merged view is the bundled snapshot alone. */
    private static final class NoRegisteredConnectors implements ConnectorCatalogStore {
        @Override
        public void upsert(ConnectorCatalogEntry entry) {
        }

        @Override
        public Optional<ConnectorCatalogEntry> get(String connectorId) {
            return Optional.empty();
        }

        @Override
        public List<ConnectorCatalogEntry> list() {
            return List.of();
        }
    }

    /** Never reached: the merged view reads the bundled snapshot and the catalog store, nothing else. */
    private static ConnectorSpecStore specStore() {
        return new ConnectorSpecStore() {
            @Override
            public void put(String connectorId, byte[] spec) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<byte[]> get(String connectorId) {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** Never reached either, for the same reason. */
    private static ConnectorRegistry registry() {
        return new ConnectorRegistry() {
            @Override
            public RegistrationOutcome register(String connectorId, String pdkApiVersion,
                    RegistrationSource source, byte[] artifact) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<ConnectorRegistration> list() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<byte[]> artifact(String contentHash) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasArtifact(String contentHash) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
