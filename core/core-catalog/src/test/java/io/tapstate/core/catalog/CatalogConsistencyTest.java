package io.tapstate.core.catalog;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tapstate.core.model.SourceMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the real bundled catalog the build tool generated into main resources — the artifact the
 * runtime ships. This runs in every build (no connectors checkout, no PDK), so it is the always-on
 * guard that the checked-in catalog is internally consistent: the index and the per-connector entries
 * agree, every entry reconstructs, and the well-known connectors are present. A broken or empty
 * regeneration fails here. Assertions are by floor and by known id, not by exact count, so adding a
 * connector does not break this test.
 */
class CatalogConsistencyTest {

    private final TapstateCatalog catalog = TapstateCatalog.load();

    @Test
    void loadsAndKeepsTheIndexAndEntriesInStep() {
        // load() rejects a duplicate index id; this confirms every index id has exactly one entry.
        assertThat(catalog.ids()).doesNotHaveDuplicates();
        assertThat(catalog.all()).hasSameSizeAs(catalog.ids());
    }

    @Test
    void carriesTheWholeOfficialConnectorSet() {
        // A floor with headroom, not an exact count, so adding/removing a few connectors does not break
        // this — but a gross truncation (the real failure this guards) drops well below it. The exact
        // empty-modes-still-shipped guarantee is locked deterministically by the assembler unit tests
        // (a not-derived connector is emitted as an entry) and by the byte-lock in the refresh job.
        assertThat(catalog.ids()).hasSizeGreaterThan(70);
        assertThat(catalog.ids()).contains("mysql", "kafka", "mongodb", "doris");
    }

    @Test
    void reconstructsEveryEntryWithItsId() {
        for (String id : catalog.ids()) {
            ConnectorCatalogEntry entry = catalog.byId(id);
            assertThat(entry.id()).isEqualTo(id);
            assertThat(entry.group()).isNotNull();
            // Both come from the index head via the loader; blank here means the bundled catalog
            // shipped with no provenance at all, which is the state the head shape exists to prevent.
            assertThat(entry.provenance().specSha()).isNotBlank();
            assertThat(entry.provenance().capabilitySha()).isNotBlank();
        }
    }

    /**
     * The connectors this repository declares the modes of, pinned by name and by count — but no
     * longer by value. The values live in the overlay now, as the single copy; what stays here is the
     * set of ids, which is the anchor that keeps this test from going vacuous.
     *
     * <p>Spelled out rather than read off the artifact, and that is the whole point. A test phrased as
     * "every entry sourced from the overlay still has modes" reads the population off the same thing
     * it is checking, so when the overlay empties there is nothing left to check and it passes —
     * green at exactly the moment it should fire. Naming the ids moves that knowledge out of the
     * artifact and into the test.
     *
     * <p>Named for what it is: a checked-in baseline that grows by explicit commit as connectors are
     * declared, not "the official set" — that is the register path's set, a different thing.
     */
    private static final List<String> EXPECTED_OVERLAY_IDS = List.of(
            "GitHub", "activemq", "ali1688", "aws-clickhouse", "dws", "feishu-bitable",
            "file-stream", "greenplum", "highgo", "huawei-gauss-db", "hubspot", "kafka",
            "kafka_enhanced", "lark-approval", "lark-doc", "metabase", "mongodb3", "rabbitmq",
            "rocketmq", "salesforce", "selectdb", "shein", "temu", "vastbase", "yashandb",
            "zoho-crm");

    @Test
    void keepsTheModesOnlyOurOwnDeclarationSupplies() {
        // Pinning the count as well as the ids: shrinking the list is the cheapest way to make a
        // failing run go green, and it would leave a test that still looks like it guards the whole set.
        assertThat(EXPECTED_OVERLAY_IDS).as("the pinned set must stay whole").hasSize(26);
        assertThat(catalog.ids()).containsAll(EXPECTED_OVERLAY_IDS);

        for (String id : EXPECTED_OVERLAY_IDS) {
            ConnectorCatalogEntry entry = catalog.byId(id);
            assertThat(entry.modes())
                    .as("connector '%s' lost the modes only our own declaration supplies - a "
                            + "regeneration with the overlay missing empties them", id)
                    .isNotEmpty();
            assertThat(entry.provenance().modeSource().values())
                    .as("connector '%s' is no longer sourced from the overlay - its modes came back "
                            + "from somewhere else, which is the drift this pins", id)
                    .containsOnly(ModeSource.OVERLAY);
        }
    }

    @Test
    void theOverlayDeclaresExactlyTheBaselineSet() {
        // Both directions. Missing one is a declaration dropped; an extra one is a declaration added
        // without anybody saying so, and the second is the one a one-directional check waves through.
        assertThat(ConnectorOverlay.load().ids())
                .containsExactlyInAnyOrderElementsOf(EXPECTED_OVERLAY_IDS);
    }

    @Test
    void derivesModesForADatabaseConnector() {
        ConnectorCatalogEntry mysql = catalog.byId("mysql");
        assertThat(mysql.group()).isEqualTo(ConnectorGroup.DATABASE);
        assertThat(mysql.modes()).contains(io.tapstate.core.model.SourceMode.SNAPSHOT,
                io.tapstate.core.model.SourceMode.CDC);
        assertThat(mysql.sink().capable()).isTrue();
    }

    @Test
    void mysqlPortKeepsItsNumericConnectorType() {
        ConfigField port = catalog.byId("mysql").config().stream()
                .filter(field -> field.name().equals("port"))
                .findFirst()
                .orElseThrow();

        assertThat(port.type()).isEqualTo(ConfigType.NUMBER);
    }

    @Test
    void derivesModesForPostgres() {
        // Both modes must come from the capability probe rather than a spec declaration. A
        // hand-written declaration would satisfy an assertion on modes alone while proving nothing
        // about the jar being readable, which is the thing that was broken: an encrypted artifact is
        // not a zip, so it cannot be classloaded to probe at all.
        ConnectorCatalogEntry postgres = catalog.byId("postgres");
        assertThat(postgres.group()).isEqualTo(ConnectorGroup.DATABASE);
        assertThat(postgres.modes()).contains(SourceMode.SNAPSHOT, SourceMode.CDC);
        assertThat(postgres.provenance().modeSource())
                .containsEntry(SourceMode.SNAPSHOT, ModeSource.DERIVED)
                .containsEntry(SourceMode.CDC, ModeSource.DERIVED);
    }

    @Test
    void everyIndexedIdIsResolvable() {
        List<String> ids = catalog.ids();
        assertThat(ids).allSatisfy(id -> assertThat(catalog.byId(id)).isNotNull());
    }
}
