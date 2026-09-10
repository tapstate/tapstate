package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.User;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The live half of the collection registry: after every store this product has has actually written
 * something, a real server must be holding nothing the registry does not declare.
 *
 * <p>This is what an architecture rule cannot see. That rule pins where a handle is taken; it says
 * nothing about what a database ends up holding, and two of the ways a collection appears never go
 * through a handle at all — a GridFS bucket materializes as two collections the driver names itself,
 * and a changeset that builds an index materializes the collection it indexes. Both look exactly like
 * an undeclared collection to anybody reading the registry, so the registry has to account for them
 * and this is what proves it does.
 *
 * <p>Every store is driven, rather than the collections being created directly, because a test that
 * created them from the registry would be comparing the registry with itself.
 */
@RequiresDocker
class SystemCollectionsReconciliationIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final DslParser PARSER = new DslParser();
    private static final String DATABASE = "reconciliation";
    private static final Instant WHEN = Instant.parse("2026-09-02T00:00:00Z");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static final String ORDERS = """
            version: tapstate/v1
            kind: source
            id: orders
            connector: mysql
            config:
              host: localhost
            """;

    @Test
    void aLiveStoreHoldsNothingTheRegistryDoesNotDeclare() {
        String uri = REPLICA_SET.getReplicaSetUrl();
        String storeUri = uri.substring(0, uri.lastIndexOf('/') + 1) + DATABASE;
        try (MongoConnection connection =
                new MongoConnection(new MongoConnectionSettings(storeUri, null, Duration.ofSeconds(5)))) {
            connection.verify();
            writeThroughEveryStore(new MongoStorePort(connection), new MongoAuthStores(connection));

            try (MongoClient raw = MongoClients.create(uri)) {
                List<String> live = new ArrayList<>();
                raw.getDatabase(DATABASE).listCollectionNames().into(live);
                Set<String> declared = SystemCollections.physicalNamesIn(SystemCollections.Database.STORE);

                // Positive control: a run that wrote nothing would satisfy the subset below and report
                // a registry nobody checked. Sixteen of the seventeen store-side rows are written here;
                // the seventeenth is created by the first changeset rather than by any store.
                assertThat(live)
                        .as("positive control: the stores must actually have created their collections")
                        .hasSizeGreaterThanOrEqualTo(16);
                // The GridFS pair is the case the registry has to expand a row for, so name it: a
                // registry that stopped expanding would fail here and nowhere else.
                assertThat(live).contains(
                        MongoStorePort.CONNECTOR_ARTIFACTS + ".files",
                        MongoStorePort.CONNECTOR_ARTIFACTS + ".chunks");
                assertThat(live)
                        .as("a collection with no row is a collection nothing knows the shape or the "
                                + "evolution rule of; add its row rather than widening this")
                        .isSubsetOf(declared);

                List<String> liveNest = new ArrayList<>();
                raw.getDatabase(MongoStorePort.NEST_STATE_DATABASE).listCollectionNames().into(liveNest);
                assertThat(liveNest)
                        .as("positive control: operator state and its dead letters must have been written")
                        .hasSizeGreaterThanOrEqualTo(2);
                assertThat(liveNest).isSubsetOf(
                        SystemCollections.physicalNamesIn(SystemCollections.Database.NEST));
            }
        }
    }

    /** One write through every sub-store, which is what makes each collection exist at all. */
    private static void writeThroughEveryStore(MongoStorePort port, MongoAuthStores auth) {
        port.artifacts().save(PARSER.parse(ORDERS));
        port.state().create("orders_sync", "{\"phase\":\"snapshot\"}", WHEN);
        port.desired().save(new DesiredState("orders_sync", PipelineState.RUNNING, "rev-abc"));
        port.observations().save(new Observation("orders_sync", PipelineState.RUNNING,
                Map.of(), Map.of(), Map.of("orders", "w7")));
        port.catalog().save(new ConnectionConfig("mysql-local", "mysql", Map.of("host", "localhost")));
        port.schemas().save(new DiscoveredSourceModel("mysql-local", "mysql", 1783998000000L,
                new SourceModel(List.of(new SourceTable("orders", List.of(), List.of(), List.of())))));
        port.connectors().register("mysql", "1.3.5", RegistrationSource.SEED,
                "mysql-connector-bytes".getBytes(StandardCharsets.UTF_8));
        port.connectorSpecs().put("spec-hash",
                "{\"properties\":{\"id\":\"mysql\"}}".getBytes(StandardCharsets.UTF_8));
        port.connectionTestResults().save(new ConnectionTestResult("mysql-local", "mysql",
                ConnectionTestResult.Outcome.PASSED,
                List.of(new ConnectionTestItem("Connection", ConnectionTestItem.Status.PASSED,
                        null, null, null, null)),
                1783939200000L));
        port.meta().create("orders@mysql-1", "7d");
        port.keyedState().save("nest.orders_sync.assemble.items", "k1",
                "held-child".getBytes(StandardCharsets.UTF_8));
        port.nestDeadLetters().record(new NestDeadLetterRecord("nest.orders_sync.assemble.items",
                "[\"items\"]#[1]~i", "mysql-a", "1:1", 0L, 9_000L, Map.of("id", 1)));

        auth.users().save(new User("admin", "hash", "ADMIN"));
        auth.tokens().save(new TokenRecord("tok-1", "WRITE", "hash-abc", false, WHEN));
        auth.sessions().save(new SessionRecord("s01", "sha256-fixture", "admin", "ADMIN",
                "urn:tapstate:cluster:01J5FIXTURE", false, WHEN, WHEN,
                WHEN.plusSeconds(30L * 24 * 3600), WHEN.plusSeconds(90L * 24 * 3600)));
        auth.audit().record(new AuditRecord(WHEN, "admin", "artifact.apply", "orders"));
        auth.clusterIdentity().createIfAbsent(new ClusterIdentity("01J5FIXTURE"));
    }
}
