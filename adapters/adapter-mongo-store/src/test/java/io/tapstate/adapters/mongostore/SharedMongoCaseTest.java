package io.tapstate.adapters.mongostore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Suite admission is checked before a container can be constructed or started. */
class SharedMongoCaseTest {

    @Test
    void anAuditedSuiteCanRegisterWithoutAnActiveInvocation() {
        SharedMongoCase fixture = SharedMongoCase.forSuite(MongoArtifactStoreIT.class);
        assertThat(fixture).isNotNull();
        assertThatThrownBy(fixture::getReplicaSetUrl)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No test invocation owns this fixture");
        assertThatThrownBy(() -> fixture.database("tapstate"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("No test invocation owns this fixture");
    }

    @Test
    void theAtomicSaveFailureSuiteCannotShareAReplicaSet() {
        refuses(MongoKeyedStateStoreSaveIfAbsentIT.class);
        refuses(MongoConnectorRegistryIT.class);
    }

    @Test
    void theLogFailureSuiteCannotShareAReplicaSet() {
        refuses(MongoSrsLogStoreIT.class);
    }

    @Test
    void theTtlMonitorSuiteCannotShareAReplicaSet() {
        refuses(RateHistoryExpiresByAgeIT.class);
    }

    @Test
    void theTopologySuiteCannotShareAReplicaSet() {
        refuses(MongoConnectionReplicaSetIT.class);
    }

    @Test
    void theTransactionLockSuiteCannotShareAReplicaSet() {
        refuses(SourceConfigKeyringRetirementIT.class);
    }

    @Test
    void anUnknownSuiteCannotShareAReplicaSet() {
        refuses(SharedMongoCaseTest.class);
    }

    private static void refuses(Class<?> suite) {
        assertThatThrownBy(() -> SharedMongoCase.forSuite(suite))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("This suite has not been audited for a shared replica set: " + suite.getName());
    }
}
