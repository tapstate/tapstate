package io.tapstate.core.catalog;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The release's officially supported connector set. Pinned exactly, because a silent addition here is
 * a support promise nobody made, and both the register path and the authoring surfaces read this one
 * list to decide what they offer and accept.
 */
class OfficialConnectorsTest {

    @Test
    void pinsTheConnectorsThisReleaseSupports() {
        // Written out in full rather than counted or matched by prefix. The release verifies the
        // database kinds themselves; each managed variant is accepted by an explicit support decision.
        assertThat(OfficialConnectors.IDS).containsExactly(
                "mysql", "aliyun-rds-mysql", "aws-rds-mysql", "polar-db-mysql", "mysql-pxc",
                "postgres", "aliyun-rds-postgres", "aliyun-adb-postgres", "polar-db-postgres",
                "tencent-db-postgres",
                "mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb", "oracle", "sqlserver",
                "db2");
    }

    @Test
    void databaseKindsKeepTheirDeclaredOrder() {
        assertThat(OfficialConnectors.IDS_BY_DATABASE_KIND.keySet())
                .containsExactly("mysql", "postgres", "mongodb", "oracle", "sqlserver");
    }

    @Test
    void callersCannotChangeTheSupportedSetOrItsGrouping() {
        assertThatThrownBy(() -> OfficialConnectors.IDS_BY_DATABASE_KIND.put("other", List.of("other")))
                .isInstanceOf(UnsupportedOperationException.class);
        OfficialConnectors.IDS_BY_DATABASE_KIND.values().forEach(ids ->
                assertThatThrownBy(() -> ids.add("other"))
                        .isInstanceOf(UnsupportedOperationException.class));
        assertThatThrownBy(() -> OfficialConnectors.IDS.add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> OfficialConnectors.SOURCE_ONLY_IDS.add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> OfficialConnectors.PREVIEW_IDS.add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void whatACatalogCarriesKeepsThisListsOrder() {
        // A menu and the refusal that names the same connectors should read the same way round, so
        // the order comes from here rather than from however the catalog happens to be sorted.
        assertThat(OfficialConnectors.presentIn(TapstateCatalog.load()))
                .containsExactlyElementsOf(OfficialConnectors.IDS);
    }

    @Test
    void membershipIsAskedOfTheSameList() {
        assertThat(OfficialConnectors.isOfficial("mysql")).isTrue();
        assertThat(OfficialConnectors.isOfficial("kafka")).isFalse();
    }

    @Test
    void pinsThePreviewIdsAndKeepsThemOutOfEveryVerifiedKind() {
        // A preview is accepted without a verified database behind it. Listed under a kind as well, it
        // would quietly pick up that kind's verification promise, which no release lane keeps for it.
        assertThat(OfficialConnectors.PREVIEW_IDS).containsExactly("db2");
        OfficialConnectors.IDS_BY_DATABASE_KIND.values().forEach(ids ->
                assertThat(ids).doesNotContainAnyElementsOf(OfficialConnectors.PREVIEW_IDS));
        assertThat(OfficialConnectors.IDS).endsWith(OfficialConnectors.PREVIEW_IDS.toArray(String[]::new));
        assertThat(OfficialConnectors.isOfficial("db2")).isTrue();
    }

    @Test
    void pinsTheConnectorsSupportedAsASourceOnly() {
        // Pinned like the supported set: adding an id withdraws a target role users may rely on, and
        // removing one grants a target role nobody has certified.
        assertThat(OfficialConnectors.SOURCE_ONLY_IDS).containsExactly("db2");
        assertThat(OfficialConnectors.isSourceOnly("db2")).isTrue();
        assertThat(OfficialConnectors.isSourceOnly("mysql")).isFalse();
    }

    @Test
    void aSourceOnlyIdIsOneThisReleaseSupports() {
        // A source-only id outside the supported set could never be registered, so its boundary would
        // describe a connector no deployment can hold.
        assertThat(OfficialConnectors.IDS).containsAll(OfficialConnectors.SOURCE_ONLY_IDS);
    }

    @Test
    void theBundledCatalogOffersNoSourceOnlyConnectorAsATarget() {
        TapstateCatalog catalog = TapstateCatalog.load();
        for (String id : OfficialConnectors.SOURCE_ONLY_IDS) {
            ConnectorCatalogEntry entry = catalog.byId(id);
            assertThat(entry.sink().capable()).as("sink capability of source-only %s", id).isFalse();
            assertThat(entry.modes()).as("source modes of source-only %s", id)
                    .contains(io.tapstate.core.model.SourceMode.SNAPSHOT, io.tapstate.core.model.SourceMode.CDC);
        }
    }
}
