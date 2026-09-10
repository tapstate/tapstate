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
                "mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb");
    }

    @Test
    void databaseKindsKeepTheirDeclaredOrder() {
        assertThat(OfficialConnectors.IDS_BY_DATABASE_KIND.keySet())
                .containsExactly("mysql", "postgres", "mongodb");
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
}
