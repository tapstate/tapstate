package io.tapstate.app;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Calls the installed SDK's real HTTP transport against a bounded local provider witness. */
class CloudConfigurationTransportTest {
    private static final String SECRET = "fixture-service-secret";
    @SuppressWarnings("unchecked")
    private static Map<String,Object> fixture() throws Exception {
        try (var input = CloudConfigurationTransportTest.class.getResourceAsStream("/cloud-configuration-initialization-v1.json")) {
            return (Map<String,Object>)JsonReader.parse(new String(Objects.requireNonNull(input).readAllBytes(), StandardCharsets.UTF_8));
        }
    }
    private static final class Provider implements AutoCloseable {
        final HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        final Map<String,Object> contract = fixture();
        final AtomicInteger requests = new AtomicInteger(), redirected = new AtomicInteger();
        final AtomicReference<Map<?,?>> body = new AtomicReference<>();
        final AtomicReference<String> authorization = new AtomicReference<>(), path = new AtomicReference<>();
        int status = 200;
        Object response;
        final CloudSdkBridge bridge;
        Provider() throws Exception {
            server.createContext("/", exchange -> {
                requests.incrementAndGet(); path.set(exchange.getRequestURI().getPath());
                authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                body.set((Map<?,?>)JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
                Object answer = response == null ? contract.get(path.get().endsWith("/complete") ? "completeResponse" : "claimResponse") : response;
                byte[] bytes = JsonWriter.write(answer).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                if (status == 302) exchange.getResponseHeaders().set("Location", base() + "/unexpected");
                exchange.sendResponseHeaders(status, bytes.length); exchange.getResponseBody().write(bytes); exchange.close();
            });
            server.createContext("/unexpected", exchange -> { redirected.incrementAndGet(); exchange.sendResponseHeaders(500, -1); exchange.close(); });
            server.start();
            var properties = new CloudProperties(); properties.setBaseUrl(base()); properties.setToken(SECRET);
            properties.setClusterId((String)contract.get("clusterId")); properties.setAtlasUri("mongodb://fixture/config");
            bridge = new CloudSdkBridge(CloudRuntimeSettings.resolve(properties));
        }
        String base() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        Map<?,?> claim() { return (Map<?,?>)contract.get("claimRequest"); }
        Map<?,?> completion() { return (Map<?,?>)contract.get("completeRequest"); }
        CloudSdkBridge.ConfigurationInitialization invokeClaim() { return bridge.claimConfiguration((String)claim().get("bootId"), (String)claim().get("configurationHash")); }
        @Override public void close() { server.stop(0); }
    }
    @Test void claimAndCompletionConsumeTheVersionedEnvelopeWithoutSendingConfigurationSecrets() throws Exception {
        try (var provider = new Provider()) {
            var grant = provider.invokeClaim();
            assertThat(grant.creationGranted()).isTrue(); assertThat(grant.phase()).isEqualTo("CLAIMED");
            assertThat(provider.body.get()).isEqualTo(provider.claim());
            assertThat(provider.path.get()).isEqualTo(provider.contract.get("claimPath"));
            assertThat(provider.authorization.get()).isEqualTo("Bearer " + SECRET);
            provider.bridge.completeConfiguration((String)provider.completion().get("operationId"), (String)provider.completion().get("configurationHash"));
            assertThat(provider.body.get()).isEqualTo(provider.completion());
            assertThat(provider.path.get()).isEqualTo(provider.contract.get("completePath"));
            assertThat(provider.requests).hasValue(2);
            assertThat(JsonWriter.write(provider.body.get())).doesNotContain("mongodb://", SECRET);
        }
    }
    @Test void unknownWritesAndRedirectsAreNotRepeated() throws Exception {
        try (var provider = new Provider()) {
            provider.status = 503;
            assertThatThrownBy(provider::invokeClaim).isInstanceOf(TapstateException.class);
            assertThat(provider.requests).hasValue(1);
            provider.status = 302;
            assertThatThrownBy(provider::invokeClaim).isInstanceOf(TapstateException.class);
            assertThat(provider.requests).hasValue(2); assertThat(provider.redirected).hasValue(0);
        }
    }
    @Test void anotherScopeOrInvalidPublicationNeverGrantsConfigurationCreation() throws Exception {
        try (var provider = new Provider()) {
            var response = (Map<?,?>)provider.contract.get("claimResponse");
            @SuppressWarnings("unchecked") var original = (Map<String,Object>)response.get("data");
            for (var mutation : List.<Map<String,Object>>of(Map.of("clusterId", "other"), Map.of("configurationHash", "b".repeat(64)),
                    Map.of("creationGranted", "true"), Map.of("phase", "AVAILABLE"), Map.of("operationId", "not-an-operation"))) {
                var data = new LinkedHashMap<>(original); data.putAll(mutation); provider.response = Map.of("code", "ok", "data", data);
                assertThatThrownBy(provider::invokeClaim).isInstanceOf(TapstateException.class);
            }
            provider.response = response;
            assertThatThrownBy(() -> provider.bridge.completeConfiguration((String)provider.completion().get("operationId"),
                    (String)provider.completion().get("configurationHash"))).isInstanceOf(TapstateException.class);
        }
    }
}
