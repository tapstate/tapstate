package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.ArtifactValidationResult;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.LivePipelines;
import io.tapstate.control.core.SchemaDerivation;
import io.tapstate.control.core.ValidationDiagnostic;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateType;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
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
 * A source column whose own name holds a dot is applied, and applying it says so.
 *
 * <p>A relational source is free to name a column {@code price.usd}, and syncing it to a document store
 * puts that name down as a top-level key. The store then reads the same characters two ways: as a key it
 * holds, and as a step into a {@code price} that is not there. Every ordinary way of addressing the
 * column takes the second reading - a read by path answers nothing, and no index can be declared over it
 * at all, because an index key is written in that same path syntax. The column is present, correct, and
 * unreachable by the spelling anyone would try first.
 *
 * <p>What makes it a defect rather than a property of the store is that nothing anywhere says it.
 * Discovery reports the column, the batch applies, the pipeline runs, the rows arrive - and an empty
 * answer to a read for that column is indistinguishable from a column that happens to hold nothing. The
 * one moment the product can see this coming is the moment the batch is applied: the column name is
 * already in the discovered model of the source, so nothing has to be run and no query has to be waited
 * for to know that this name will not address itself once written.
 *
 * <p>Reported, not refused. A batch carrying such a column is applied - the name is legal in the source
 * and legal in the store, the rows are written correctly, and refusing here would also refuse every
 * pipeline whose data is already sitting in a collection this way. The finding travels in the advisory
 * column beside the outcomes, which is the channel that exists for exactly this: something worth telling
 * the author, that is not grounds to turn the batch away.
 *
 * <p>The assembly's own apply verb rather than one built here, because what is under test is whether the
 * product says anything - and a rule this test wired itself would report on a batch no deployment judges
 * that way. An ordinary column name goes through the same batch first, so a pass that says something
 * about every apply is not mistaken for one that noticed the dot.
 */
class AColumnNamedWithADotIsReportedWhenItIsAppliedTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(Instant.parse("2026-08-09T10:15:30Z"), ZoneOffset.UTC);

    /** The measured case: legal in the source, and a key that does not address itself once written. */
    private static final String DOTTED_COLUMN = "price.usd";

    /** The same column named the way that stays reachable, so the batch is the only thing that differs. */
    private static final String PLAIN_COLUMN = "price_usd";

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config: { host: 10.10.0.5, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    /** A document store, which is where the name stops addressing itself. */
    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: orders_dest
            connector: mongodb
            config: { uri: "mongodb://localhost:27017/t" }
            """;

    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: orders_sync
            source: orders_src
            serve:
              from: orders
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    private final InMemorySchemaStore schemas = new InMemorySchemaStore();

    @Test
    void aColumnWhoseNameHoldsADotIsAppliedAndReportedOn() {
        discovered(PLAIN_COLUMN);
        assertThat(assembled().validate(batch()).warnings())
                .as("an ordinary column name is nothing to report, so a report on one would not be about the dot")
                .isEmpty();

        discovered(DOTTED_COLUMN);
        ArtifactValidationResult result = assembled().validate(batch());

        assertThat(result.valid())
                .as("the name is legal on both sides and the rows are written correctly: this is a cost, "
                        + "not a reason to refuse the batch")
                .isTrue();
        assertThat(namesTheColumn(result.warnings(), DOTTED_COLUMN))
                .as("applying a batch that syncs a column named `%s` into a document store reported "
                        + "nothing about it - the column arrives unaddressable by the read anyone would "
                        + "write for it, and unindexable, and the only warnings were %s",
                        DOTTED_COLUMN, result.warnings())
                .isTrue();
    }

    /**
     * Whether any finding names the column. Which code it is filed under is left open - a warning that
     * does not say which column it is about is one nobody can act on, and that is the part worth pinning.
     */
    private static boolean namesTheColumn(List<ValidationDiagnostic> warnings, String column) {
        return warnings.stream().anyMatch(warning -> warning.params().values().stream()
                .anyMatch(value -> String.valueOf(value).contains(column)));
    }

    /** Records the source as discovery reports it: one table, carrying the column under test. */
    private void discovered(String pricecolumn) {
        SourceTable orders = new SourceTable("orders",
                List.of(new SourceField("id", "bigint", TapstateType.INT64),
                        new SourceField(pricecolumn, "decimal", TapstateType.DECIMAL)),
                List.of("id"), List.of(), 1_000L);
        schemas.save(new DiscoveredSourceModel("orders_src", "mysql", 0L, new SourceModel(List.of(orders))));
    }

    private static List<ArtifactDraft> batch() {
        return List.of(new ArtifactDraft("source.yaml", SOURCE), new ArtifactDraft("target.yaml", TARGET),
                new ArtifactDraft("pipeline.yaml", PIPELINE));
    }

    /** The apply verb exactly as the assembly builds it, carrying whatever advisory rules it wires. */
    private ApplyService assembled() {
        ConnectorCatalogView catalog = new ConnectorCatalogView(
                TapstateCatalog.load(), new NoRegisteredConnectors(), specStore(), registry());
        return new ControlPlaneConfiguration().applyService(
                new InMemoryArtifactStore(), catalog,
                new AuditGate(record -> { }, FIXED_CLOCK), schemas, NestSettings.defaults(),
                SchemaDerivation.none(),
                new LivePipelines(new InMemoryDesiredStore(), new InMemoryStateStore()));
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
