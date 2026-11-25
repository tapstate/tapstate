package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** The ACTIVE set is created once and every later topology is a monotonic revision CAS. */
@RequiresDocker
class MongoClusterMembershipStoreIT {

    @RegisterExtension
    private static final SharedMongoCase REPLICA_SET =
            SharedMongoCase.forSuite(MongoClusterMembershipStoreIT.class);

    @Test
    void bootstrapHasOneWinnerAndTopologyRevisionsCannotBeRebasedByAStaleWriter() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var collection = client.getDatabase(REPLICA_SET.database("tapstate")).getCollection(MongoStorePort.CLUSTER_MEMBERSHIP);
            collection.drop();
            MongoClusterMembershipStore store = new MongoClusterMembershipStore(collection);

            ClusterMembership first = store.createIfAbsent("cluster-a", Set.of("a", "b", "c"));
            ClusterMembership loser = store.createIfAbsent("cluster-a", Set.of("x", "y", "z"));
            ClusterMembership second = store.compareAndSet(
                    "cluster-a", first.revision(), Set.of("a", "b", "c", "d")).orElseThrow();

            assertThat(loser).isEqualTo(first);
            assertThat(second.revision()).isEqualTo(2);
            assertThat(second.activeNodeIds()).containsExactlyInAnyOrder("a", "b", "c", "d");
            assertThat(store.compareAndSet("cluster-a", first.revision(), Set.of("a", "b"))).isEmpty();
            assertThat(store.read("cluster-a")).contains(second);
            assertThat(collection.countDocuments()).isEqualTo(1);
        }
    }
}
