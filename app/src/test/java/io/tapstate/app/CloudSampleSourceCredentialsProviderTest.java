package io.tapstate.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.control.core.SampleSourceCredentialsProvider;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CloudSampleSourceCredentialsProviderTest {
    private HttpServer server;
    private AtomicReference<String> authorization;
    private CloudSampleSourceCredentialsProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        authorization = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/api/clusters/cluster-1/sample-sources/availability", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = "{\"code\":\"ok\",\"data\":{\"available\":true}}"
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.createContext("/v1/api/clusters/cluster-1/sample-sources/credentials", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            byte[] body = "{\"code\":\"ok\",\"data\":{\"host\":\"sample.internal\",\"password\":\"secret-value\"}}"
                    .getBytes(StandardCharsets.UTF_8);
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
                CloudRuntimeSettings.resolve(properties), new ObjectMapper());
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
}
