package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.control.core.ApplyService;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.store.SrsStores;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorePort;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole platform assembled as one JVM process — the L1 {@code --role=all} shape — brought up over
 * a real store. Booting the real {@code @SpringBootApplication} with the store enabled activates every
 * assembly configuration after native storage admission, so all six architectural rings run under
 * one process: the store bridge, the control-core plane over it, the PDK connector plane, the embedded
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
    private NativeAppStorage storage;
    @TempDir Path work;

    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void roleAllBringsUpEverySixRingBeanInOneJvmAndServesHealth() {
        int port = startFullAssembly();
        var business=NativeAppStorage.business(context);

        // The admitted child is live, and every architectural ring has a working bean in it: booting the whole
        // application with the store enabled is the --role=all assembly, so these are wired together rather
        // than in the hand-picked subsets the other store-enabled checks assemble.
        assertThat(context.isRunning()).isTrue();
        assertThat(business.isRunning()).isTrue();
        assertThat(business.getBean(StorePort.class)).isNotNull();            // adapters -> spi.store bridge
        assertThat(business.getBean(SrsStores.class)).isNotNull();            // the native SRS stores
        assertThat(business.getBean(ApplyService.class)).isNotNull();         // a control-core service
        assertThat(business.getBean(ConnectorProvisioner.class)).isNotNull(); // the adapter-pdk connector plane
        assertThat(business.getBean(Engine.class)).isNotNull();               // the Jet engine over the member
        assertThat(business.getBean(PipelineConverger.class)).isNotNull();    // desired -> actual convergence
        assertThat(business.getBean(ConvergenceDriver.class)).isNotNull();    // the scheduled convergence driver

        // The embedded Hazelcast member is the runtime backbone every store-enabled subset test omits; under
        // the full assembly it comes up and is running.
        HazelcastInstance member = business.getBean(HazelcastInstance.class);
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

        assertThat(NativeAppStorage.business(context).isRunning()).isTrue();
        try (MongoClient raw = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var config=raw.getDatabase(storage.configurationDatabase()).listCollectionNames().into(new ArrayList<>());
            var cache=raw.getDatabase(storage.cacheDatabase()).listCollectionNames().into(new ArrayList<>());
            assertThat(config).as("native CONFIG must contain actual installation facts").isNotEmpty()
                    .isSubsetOf(SystemCollections.physicalNamesIn(SystemCollections.Database.CONFIG));
            assertThat(cache).as("native CACHE must contain actual admitted runtime facts").isNotEmpty()
                    .isSubsetOf(SystemCollections.physicalNamesIn(SystemCollections.Database.CACHE));
            assertThat(raw.getDatabase(storage.masterDatabase()).listCollectionNames().into(new ArrayList<>()))
                    .as("a boot without a pipeline creates no user target data").isEmpty();
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> version = RestClient.create("http://localhost:" + port)
                .get().uri("/version").retrieve().body(Map.class);
        assertThat(version).containsEntry("dslVersions", List.of("tapstate/v1"));
        assertThat(version)
                .as("the store a real start opened has been brought to the version this build knows")
                .containsEntry("dataVersion", storage.schemaVersion());
    }

    private int startFullAssembly(String... extraProperties) {
        // A per-run database on the shared class container keeps runs independent.
        storage=NativeAppStorage.create(REPLICA_SET,"assembly",StorageBinding.Mode.OP,"assembly-"+java.util.UUID.randomUUID());
        List<String> properties = new ArrayList<>(List.of(
                "tapstate.store.mongo.enabled=true",
                "tapstate.store.mongo.uri=" + storage.configurationUri(),
                "tapstate.connectors.plugins-dir="+work.resolve("plugins"),
                "tapstate.connectors.seed-dir="+work.resolve("optional-seeds"),
                // the container speaks plaintext; TLS is opt-in, so no flag is needed here
                "tapstate.store.mongo.server-selection-timeout=5s"));
        properties.addAll(List.of(extraProperties));
        context = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .properties(properties.toArray(String[]::new))
                // Both the address and the port are run arguments, not default properties: `properties(...)`
                // populates the lowest-ranked source Spring has, and the product's own application
                // configuration publishes 8080, so a port asked for there is silently overridden. The
                // address is the other half -- a free port alone binds the wildcard, and a wildcard bind
                // does not reserve 127.0.0.1:<port>, so a process already holding that port on the loopback
                // keeps receiving what this test sends.
                .run("--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                        "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false");
        return ((WebServerApplicationContext) context).getWebServer().getPort();
    }
}
