package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.JsonReader;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.UserStore;
import java.net.URI;
import java.net.http.*;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import static org.assertj.core.api.Assertions.*;

/** The production entry point serves its minimum controls without constructing a data plane. */
class StorageMinimumStartupTest {
    private static ConfigurableApplicationContext context;
    private static URI base;
    @BeforeAll static void start() {
        context = new SpringApplicationBuilder(Bootstrap.class).web(WebApplicationType.SERVLET).run(
                "--server.port=0", "--tapstate.store.mongo.uri=", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.control.auth.jwt-secret=minimum-startup-test-key");
        base = URI.create("http://127.0.0.1:" + ((WebServerApplicationContext)context).getWebServer().getPort());
    }
    @AfterAll static void close() { if (context != null) context.close(); }
    @Test void configurationAbsenceDoesNotCreateRuntimeMembersStoresOrWorkers() {
        assertThat(context.getBean(StorageRoot.class).phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_REQUIRED);
        for (Class<?> type : new Class<?>[]{HazelcastInstance.class, Engine.class, PipelineConverger.class,
                ConvergenceDriver.class, NodeSessionLease.class, ActuationClaimRenewer.class, StorePort.class, UserStore.class}) {
            assertThat(context.getBeansOfType(type)).as(type.getSimpleName()).isEmpty();
        }
    }
    @Test void parentHttpMapperKeepsStrictSourceInputDeserialization() {
        var mapper = context.getBean(tools.jackson.databind.ObjectMapper.class);
        assertThatThrownBy(() -> mapper.readValue("{\"id\":\"source\",\"connector\":\"mongodb\",\"config\":{},\"unknownField\":[]}",
                io.tapstate.control.core.SourceInput.class)).isInstanceOf(tools.jackson.databind.exc.UnrecognizedPropertyException.class);
    }
    @Test void livenessRemainsAvailableAndLoginReportsItsMissingDependency() throws Exception {
        var client = HttpClient.newHttpClient();
        assertThat(client.send(HttpRequest.newBuilder(base.resolve("/healthz")).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        var response = client.send(HttpRequest.newBuilder(base.resolve("/auth/login")).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"sentinel-password\"}")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(((Map<?,?>)JsonReader.parse(response.body())).get("code")).isEqualTo("control.storage-unavailable");
        assertThat(response.body()).doesNotContain("sentinel-password");
    }
    @Test void directBusinessRequestsCannotConstructTheRuntimeOrUseTheOldBootstrapAlias() throws Exception {
        var client = HttpClient.newHttpClient();
        var response = client.send(HttpRequest.newBuilder(base.resolve("/api/storage/status")).header("Authorization", "Bearer old-jwt")
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{}")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).contains("control.storage-unavailable");
        var bootstrap = client.send(HttpRequest.newBuilder(base.resolve("/auth/bootstrap")).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"admin\",\"password\":\"sentinel-password\"}")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(bootstrap.statusCode()).isEqualTo(403);
        assertThat(context.getBeansOfType(HazelcastInstance.class)).isEmpty();
    }
}
