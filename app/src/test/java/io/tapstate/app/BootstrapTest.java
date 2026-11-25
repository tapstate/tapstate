package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.pdk.PdkAdapter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootVersion;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The assembly root: the Spring application context starts and stops cleanly (validating Spring Boot
 * 4.1 on JDK 21), and the root records the adapter bridges it wires — the R7 exemption made real,
 * since the app is the only non-adapter module permitted to reference the adapters ring.
 */
class BootstrapTest {

    @Test
    void runsOnSpringBoot41() {
        // The service framework platform is Spring Boot 4.1.x. Pinning the running platform version
        // here fails the build on a silent downgrade or a stale pin. (Co-existence with the embedded
        // Hazelcast member is witnessed by HazelcastMemberTest, which boots the same context.)
        assertThat(SpringBootVersion.getVersion()).startsWith("4.1");
    }

    @Test
    void springApplicationContextStartsAndStops() {
        // This checks the Spring Boot substrate on JDK 21, not store connectivity, so the store
        // connection is disabled — its own wiring is covered by StoreStartupTest.
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(Bootstrap.class)
                .web(WebApplicationType.NONE)
                .properties("tapstate.store.mongo.enabled=false")
                .run()) {
            assertThat(context.isRunning()).isTrue();
        }
    }

    @Test
    void doesNotAutoConfigureAStoreClient() {
        // The Mongo driver is on the classpath (through the adapter), but Spring Boot must not
        // auto-configure its own client: the only store client is the controlled MongoConnection.
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(Bootstrap.class)
                .web(WebApplicationType.NONE)
                .properties("tapstate.store.mongo.enabled=false")
                .run()) {
            assertThat(context.getBeansOfType(com.mongodb.client.MongoClient.class)).isEmpty();
        }
    }

    @Test
    void assemblyRootRecordsTheAdapterBridges() {
        // Each bridge is explicitly owned by the production assembly.
        assertThat(Bootstrap.adapterBridges())
                .containsExactly(PdkAdapter.class, MongoConfigurationDomain.class, MongoCacheDomain.class);
    }
}
