package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the connector registry against a real Mongo replica-set through the real GridFS store: a
 * registered artifact reads back with its identity and its exact bytes, re-registering the same bytes
 * is a content-hash no-op that stores no second copy, list returns every registration, and an unknown
 * hash has no bytes. Where Docker is absent this aborts on a developer machine and fails in CI, where
 * a skip would be a green build that ran nothing.
 */
@RequiresDocker
class MongoConnectorRegistryIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static byte[] jar(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void registerStoresTheRegistrationAndRetrievableBytes() {
        withRegistry(registry -> {
            byte[] jar = jar("mysql-connector-bytes");
            RegistrationOutcome outcome = registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar);

            assertThat(outcome.newlyRegistered()).isTrue();
            ConnectorRegistration registration = outcome.registration();
            assertThat(registration.connectorId()).isEqualTo("mysql");
            assertThat(registration.pdkApiVersion()).isEqualTo("1.3.5");
            assertThat(registration.source()).isEqualTo(RegistrationSource.REGISTER);
            assertThat(registry.artifact(registration.contentHash()).orElseThrow()).isEqualTo(jar);
        });
    }

    @Test
    void reRegisteringTheSameBytesIsAContentHashNoOp() {
        withRegistry(registry -> {
            byte[] jar = jar("mysql-connector-bytes");
            RegistrationOutcome first = registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar);
            RegistrationOutcome again = registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar);

            assertThat(first.newlyRegistered()).isTrue();
            assertThat(again.newlyRegistered()).isFalse();
            // the no-op returns what is stored (the original SEED source), and stores no second copy
            assertThat(again.registration()).isEqualTo(first.registration());
            assertThat(registry.list()).hasSize(1);
        });
    }

    @Test
    void findAnswersAboutOneConnectorAndIsEmptyForAnUnregisteredOne() {
        withRegistry(registry -> {
            registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar("mysql-bytes"));
            registry.register("mongodb", "1.3.5", RegistrationSource.REGISTER, jar("mongodb-bytes"));

            assertThat(registry.findAll("mysql"))
                    .singleElement()
                    .satisfies(found -> assertThat(found.connectorId()).isEqualTo("mysql"));
            assertThat(registry.findAll("postgres")).isEmpty();
        });
    }

    @Test
    void findAllReportsBothArtifactsAndInAStableOrderWhenAnIdCarriesTwo() {
        // One artifact per id is the intended state and a register refuses a second - but two concurrent
        // registers can both pass that check before either stores, and an operator can write out of band.
        // Both have to be reported: a register that saw only one could call a duplicate "already
        // registered", and a read face that saw only one would call an unloadable connector available.
        // And the order has to hold across calls, or anything downstream flips for no observable reason.
        withRegistry(registry -> {
            registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar("mysql-bytes-one"));
            registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, jar("mysql-bytes-two"));

            List<ConnectorRegistration> first = registry.findAll("mysql");

            // Both are reported - a caller that must refuse the duplicate has to be able to see it.
            assertThat(first).hasSize(2);
            assertThat(registry.list()).hasSize(2);
            // And reported in the same order every time, so nothing downstream flips between calls.
            assertThat(registry.findAll("mysql")).containsExactlyElementsOf(first);
            assertThat(registry.findAll("mysql")).containsExactlyElementsOf(first);
        });
    }

    @Test
    void listReturnsEveryRegisteredConnector() {
        withRegistry(registry -> {
            registry.register("mysql", "1.3.5", RegistrationSource.SEED, jar("mysql-bytes"));
            registry.register("postgres", "1.3.5", RegistrationSource.REGISTER, jar("postgres-bytes"));

            assertThat(registry.list())
                    .extracting(ConnectorRegistration::connectorId)
                    .containsExactlyInAnyOrder("mysql", "postgres");
        });
    }

    @Test
    void artifactForAnUnknownHashIsEmpty() {
        withRegistry(registry -> assertThat(registry.artifact("deadbeef")).isEmpty());
    }

    private interface RegistryTest {
        void run(MongoConnectorRegistry registry);
    }

    /** Runs a test body against a fresh registry over a clean GridFS bucket on the real replica-set. */
    private static void withRegistry(RegistryTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("tapstate");
            GridFSBucket bucket = GridFSBuckets.create(database, "connector_artifacts");
            bucket.drop();
            test.run(new MongoConnectorRegistry(bucket));
        }
    }
}
