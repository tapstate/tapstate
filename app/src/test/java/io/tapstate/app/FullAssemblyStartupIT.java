package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.control.core.ApplyService;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole platform assembled as one JVM process — the L1 {@code --role=all} shape — brought up over
 * a real store. Booting the real {@code @SpringBootApplication} with the store enabled activates every
 * assembly configuration at once, so all six architectural rings come up in a single application
 * context: the store bridge, the control-core plane over it, the PDK connector plane, the embedded
 * Hazelcast member, the Jet engine over that member, and the desired-to-actual convergence loop. This
 * witnesses that the independently developed rings collapse into one coherent process and that the
 * liveness probe answers over the full assembly — a scenario no other test co-boots: the control-plane
 * assembly checks import only the store and control configurations (no member/engine/convergence), and
 * every full-application boot elsewhere runs with the store disabled, which gates four of the six
 * rings off. Where Docker is absent this aborts on a developer machine and fails in CI, where a skip
 * would be a green build that ran nothing.
 */
@RequiresDocker
class FullAssemblyStartupIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private ConfigurableApplicationContext context;
    private String database;

    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void roleAllBringsUpEverySixRingBeanInOneJvmAndServesHealth() {
        int port = startFullAssembly();

        // The one context is live, and every architectural ring has a working bean in it: booting the whole
        // application with the store enabled is the --role=all assembly, so these are wired together rather
        // than in the hand-picked subsets the other store-enabled checks assemble.
        assertThat(context.isRunning()).isTrue();
        assertThat(context.getBean(StorePort.class)).isNotNull();            // adapters -> spi.store bridge
        assertThat(context.getBean(SrsMetaStore.class)).isNotNull();         // the SRS meta store
        assertThat(context.getBean(ApplyService.class)).isNotNull();         // a control-core service
        assertThat(context.getBean(ConnectorProvisioner.class)).isNotNull(); // the adapter-pdk connector plane
        assertThat(context.getBean(Engine.class)).isNotNull();               // the Jet engine over the member
        assertThat(context.getBean(PipelineConverger.class)).isNotNull();    // desired -> actual convergence
        assertThat(context.getBean(ConvergenceDriver.class)).isNotNull();    // the scheduled convergence driver

        // The embedded Hazelcast member is the runtime backbone every store-enabled subset test omits; under
        // the full assembly it comes up and is running.
        HazelcastInstance member = context.getBean(HazelcastInstance.class);
        assertThat(member.getLifecycleService().isRunning()).isTrue();

        // The liveness probe answers over the full assembly, not merely the control-plane-only subset.
        RestClient client = RestClient.create("http://127.0.0.1:" + port);
        String health = client.get().uri("/healthz").retrieve().body(String.class);
        assertThat(health).isEqualTo("ok");
    }

    /**
     * What the whole assembly leaves behind in the store, reconciled against the registry that declares
     * every collection this product keeps.
     *
     * <p>Done here rather than only over the store adapter because this is the one boot that brings up
     * all six rings at once: a collection created by the member, the engine or the convergence loop is
     * invisible to a test that drives the stores alone, and undeclared is exactly how it would look to
     * anybody reading the registry afterwards.
     *
     * <p>The version endpoint is checked in the same run for the same reason -- what it reports about
     * the store is only true if a real start actually migrated one.
     */
    @Test
    void theFullAssemblyLeavesNothingInTheStoreThatNothingDeclares() {
        int port = startFullAssembly();

        List<String> live = new ArrayList<>();
        try (MongoClient raw = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            raw.getDatabase(database).listCollectionNames().into(live);
        }
        assertThat(live)
                .as("positive control: a full boot must have written something, or the reconciliation "
                        + "below is over an empty set")
                .isNotEmpty();
        assertThat(live)
                .as("a collection with no row is one nothing knows the shape or the evolution rule of")
                .isSubsetOf(SystemCollections.physicalNamesIn(SystemCollections.Database.STORE));

        @SuppressWarnings("unchecked")
        Map<String, Object> version = RestClient.create("http://localhost:" + port)
                .get().uri("/version").retrieve().body(Map.class);
        assertThat(version).containsEntry("dslVersions", List.of("tapstate/v1"));
        assertThat(version)
                .as("the store a real start opened has been brought to the version this build knows")
                .containsEntry("dataVersion", MigrationRunner.SUPPORTED_VERSION);
    }

    private int startFullAssembly(String... extraProperties) {
        // A per-run database on the shared class container keeps runs independent.
        database = "assembly_all_" + Long.toUnsignedString(System.nanoTime(), 16);
        List<String> properties = new ArrayList<>(List.of(
                "tapstate.store.mongo.enabled=true",
                "tapstate.store.mongo.uri=" + REPLICA_SET.getReplicaSetUrl(database),
                // the container speaks plaintext; TLS is opt-in, so no flag is needed here
                "tapstate.store.mongo.server-selection-timeout=5s"));
        properties.addAll(List.of(extraProperties));
        context = new SpringApplicationBuilder(Bootstrap.class)
                .properties(properties.toArray(String[]::new))
                // Both the address and the port are run arguments, not default properties: `properties(...)`
                // populates the lowest-ranked source Spring has, and the product's own application
                // configuration publishes 8080, so a port asked for there is silently overridden. The
                // address is the other half -- a free port alone binds the wildcard, and a wildcard bind
                // does not reserve 127.0.0.1:<port>, so a process already holding that port on the loopback
                // keeps receiving what this test sends.
                .run("--server.address=127.0.0.1", "--server.port=0");
        return ((WebServerApplicationContext) context).getWebServer().getPort();
    }
}
