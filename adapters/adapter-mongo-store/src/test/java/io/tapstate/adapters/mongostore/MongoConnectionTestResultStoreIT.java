package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the connection-test-result store against a real Mongo replica-set: a saved result reads
 * back equal through the real bson encode / decode (overall outcome, plus a passed item and a failed
 * item carrying every diagnostic), an untested connection reads back empty, and a re-test of the same
 * connection replaces the stored result in place (last write wins) rather than accumulating documents.
 * Where Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a
 * green build that ran nothing.
 */
@RequiresDocker
class MongoConnectionTestResultStoreIT {


    @RegisterExtension
    private static final SharedMongoCase REPLICA_SET =
            SharedMongoCase.forSuite(MongoConnectionTestResultStoreIT.class);

    private static ConnectionTestResult failedResult() {
        return new ConnectionTestResult(
                "conn-orders",
                "mongodb",
                ConnectionTestResult.Outcome.FAILED,
                List.of(
                        new ConnectionTestItem("Connection", ConnectionTestItem.Status.PASSED, null, null, null, null),
                        new ConnectionTestItem(
                                "Login",
                                ConnectionTestItem.Status.FAILED,
                                "authentication failed for user 'sync'",
                                "SCRAM-SHA-256 negotiation rejected",
                                "verify the user exists in the admin database",
                                "11000")),
                1783939200000L);
    }

    @Test
    void savedResultReadsBackEqualThroughRealBson() {
        withStore((store, collection) -> {
            ConnectionTestResult result = failedResult();
            store.save(result);

            assertThat(store.find("conn-orders")).contains(result);
        });
    }

    @Test
    void findReturnsEmptyForAnUntestedConnection() {
        withStore((store, collection) -> assertThat(store.find("never-tested")).isEmpty());
    }

    @Test
    void reTestReplacesTheStoredResultInPlace() {
        withStore((store, collection) -> {
            store.save(failedResult());
            ConnectionTestResult passed = new ConnectionTestResult(
                    "conn-orders",
                    "mongodb",
                    ConnectionTestResult.Outcome.PASSED,
                    List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.PASSED, null, null, null, null)),
                    1783939300000L);
            store.save(passed);

            assertThat(collection.countDocuments()).isEqualTo(1);
            assertThat(store.find("conn-orders")).contains(passed);
        });
    }

    private interface StoreTest {
        void run(MongoConnectionTestResultStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh result store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection =
                    client.getDatabase(REPLICA_SET.database("tapstate")).getCollection("connection_test_results");
            collection.drop();
            test.run(new MongoConnectionTestResultStore(collection), collection);
        }
    }
}
