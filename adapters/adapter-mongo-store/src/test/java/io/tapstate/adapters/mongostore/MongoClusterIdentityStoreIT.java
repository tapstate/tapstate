package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

/** Witnesses that a newly constructed server service reads the same persisted cluster identity. */
@RequiresDocker
class MongoClusterIdentityStoreIT {

    @RegisterExtension
    private static final SharedMongoCase REPLICA_SET =
            SharedMongoCase.forSuite(MongoClusterIdentityStoreIT.class);

    @Test
    void identitySurvivesReconstructionAndFirstWriterWins() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var collection = client.getDatabase(REPLICA_SET.database("tapstate")).getCollection(MongoAuthStores.CLUSTER_IDENTITY);
            collection.drop();

            MongoClusterIdentityStore firstProcess = new MongoClusterIdentityStore(collection);
            assertThat(firstProcess.createIfAbsent(new ClusterIdentity("FIRST")).clusterId()).isEqualTo("FIRST");

            MongoClusterIdentityStore restartedProcess = new MongoClusterIdentityStore(collection);
            assertThat(restartedProcess.createIfAbsent(new ClusterIdentity("SECOND")).clusterId()).isEqualTo("FIRST");
            assertThat(restartedProcess.find()).contains(new ClusterIdentity("FIRST"));
            assertThat(collection.countDocuments()).isEqualTo(1);
        }
    }
}
