package io.tapstate.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.SampleSourceCredentialsProvider;
import io.tapstate.core.common.TapstateException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class CloudSampleSourceCredentialsProviderTest {
    private HttpServer server;
    private AtomicReference<String> authorization;
    private AtomicReference<String> availabilityBody;
    private AtomicReference<String> credentialsBody;
    private CloudSampleSourceCredentialsProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        authorization = new AtomicReference<>();
        availabilityBody = new AtomicReference<>("{\"code\":\"ok\",\"data\":{\"available\":true}}");
        credentialsBody = new AtomicReference<>(
                "{\"code\":\"ok\",\"data\":{\"host\":\"sample.internal\",\"password\":\"secret-value\"}}");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/api/clusters/cluster-1/sample-sources/availability", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = availabilityBody.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/v1/api/clusters/cluster-1/sample-sources/credentials", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = credentialsBody.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        CloudProperties properties = new CloudProperties();
        properties.setBaseUrl("http://127.0.0.1:" + server.getAddress().getPort());
        properties.setToken("cluster-runtime-token");
        properties.setAtlasUri("mongodb://127.0.0.1:27017/metadata");
        properties.setClusterId("cluster-1");
        provider = new CloudSampleSourceCredentialsProvider(
                CloudRuntimeSettings.resolve(properties), JsonMapper.builder().build());
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop(0);
    }

    @Test
    void fetchesCredentialsOnlyThroughTheClusterServerIdentity() {
        assertTrue(provider.available());
        SampleSourceCredentialsProvider.Credentials credentials = provider.fetch();

        assertEquals("sample.internal", credentials.host());
        assertEquals("secret-value", credentials.password());
        assertEquals("Bearer cluster-runtime-token", authorization.get());
        assertFalse(credentials.toString().contains("secret-value"));
    }

    @Test
    void malformedJsonFailsClosedWithoutExposingParserInput() {
        String secret = "sample-json-password-sentinel";
        String malformed = "{\"password\":\"" + secret + "\",";
        availabilityBody.set(malformed);
        credentialsBody.set(malformed);

        assertFalse(provider.available());
        TapstateException failure = assertThrows(TapstateException.class, provider::fetch);
        assertEquals(ControlError.UNREACHABLE, failure.code());
        assertTrue(failure.args().isEmpty());
        assertNull(failure.getCause());
        assertFalse(failure.toString().contains(secret));
    }
}
