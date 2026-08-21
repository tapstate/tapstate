package io.tapstate.adapters.mongostore;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The aggregated store port binds each store concern to its own distinct, named storage area (a
 * collection, or a GridFS bucket for the connector registry). The driver-bound aggregation itself is
 * witnessed against a real replica-set by {@link MongoStorePortIT}; this pins the layout
 * deterministically, so a silent rename that would collide two concerns onto one name fails the build
 * here rather than corrupting a live store.
 */
class MongoStorePortTest {

    @Test
    void bindsEachConcernToItsOwnDistinctNamedStorage() {
        assertThat(List.of(
                MongoStorePort.ARTIFACTS,
                MongoStorePort.PIPELINE_STATE,
                MongoStorePort.PIPELINE_DESIRED,
                MongoStorePort.PIPELINE_OBSERVATION,
                MongoStorePort.CONNECTIONS,
                MongoStorePort.SOURCE_SCHEMAS,
                MongoStorePort.CONNECTOR_ARTIFACTS,
                MongoStorePort.CONNECTOR_CATALOG,
                MongoStorePort.CONNECTOR_SPECS,
                MongoStorePort.CONNECTION_TEST_RESULTS,
                MongoStorePort.SRS_META,
                MongoAuthStores.CLUSTER_IDENTITY))
                .doesNotHaveDuplicates()
                .containsExactly("artifacts", "pipeline_state", "pipeline_desired", "pipeline_observation",
                        "connections", "source_schemas", "connector_artifacts", "connector_catalog",
                        "connector_specs", "connection_test_results", "srs_meta", "cluster_identity");
    }

    /**
     * Operator state is the one area whose contents cannot be worked out again from anything else that
     * survived, and the promise its port makes is that a write having returned means the change is
     * durable. Acknowledging on the primary alone reports a write done that a lost primary then takes
     * with it, after the frontier has already advanced past the change on the strength of it - a loss
     * nothing reports. Pinned here for the same reason the layout above is: a silent relaxation to a
     * faster concern would fail the build rather than quietly weaken what a returning write means.
     */
    @Test
    void acknowledgesOperatorStateOnAMajorityThatHasJournalledIt() {
        assertThat(MongoStorePort.NEST_STATE_WRITE_CONCERN.getWObject()).isEqualTo("majority");
        assertThat(MongoStorePort.NEST_STATE_WRITE_CONCERN.getJournal()).isTrue();
    }
}
