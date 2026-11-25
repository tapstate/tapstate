package io.tapstate.app;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorCatalogHttpTest {
    @Test
    void aDirectoryBeforeTheManifestMustBeEmpty() throws IOException {
        assertThat(ConnectorCatalogHttp.authentication(jarWithLeadingDirectory(new byte[0]))).isEqualTo("Beta");
        byte[] jar = jarWithLeadingDirectory(new byte[]{1});
        assertThatThrownBy(() -> ConnectorCatalogHttp.authentication(jar))
                .isInstanceOf(IOException.class).hasMessage("Connector directory entry contains data");
    }

    private static byte[] jarWithLeadingDirectory(byte[] contents) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry("META-INF/"));
            zip.write(contents);
            zip.closeEntry();
            zip.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
            zip.write("Manifest-Version: 1.0\r\nAuthentication: Beta\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }

    @Test
    void aPublishedArtifactCanBeProbedForAvailabilityAndStageWithoutDownloadingItsBody() throws Exception {
        var manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Authentication", "Beta");
        var bytes = new ByteArrayOutputStream();
        try (var stream = new JarOutputStream(bytes, manifest)) {
            stream.finish();
        }
        var range = new AtomicReference<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/available.jar", exchange -> {
            try (exchange) {
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    range.set(exchange.getRequestHeaders().getFirst("Range"));
                    exchange.sendResponseHeaders(206, bytes.size());
                    bytes.writeTo(exchange.getResponseBody());
                }
            }
        });
        server.createContext("/full.jar", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(200, bytes.size());
                bytes.writeTo(exchange.getResponseBody());
            }
        });
        server.createContext("/missing.jar", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(404, -1);
            }
        });
        server.createContext("/unavailable.jar", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(503, -1);
            }
        });
        server.start();
        try {
            var client = new ConnectorCatalogHttp();
            var base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            var available = base.resolve("/available.jar");
            assertThat(client.available(available)).isTrue();
            assertThat(range.get()).isNull();
            assertThat(client.authentication(available)).isEqualTo("Beta");
            assertThat(range.get()).startsWith("bytes=0-");
            assertThat(client.authentication(base.resolve("/full.jar"))).isEqualTo("Beta");
            assertThat(client.read(base.resolve("/full.jar"))).isEqualTo(bytes.toByteArray());
            assertThat(client.available(base.resolve("/missing.jar"))).isFalse();
            assertThatThrownBy(() -> client.read(base.resolve("/missing.jar")))
                    .isInstanceOf(ConnectorReleaseCatalog.MissingResource.class);
            assertThatThrownBy(() -> client.available(base.resolve("/unavailable.jar")))
                    .isInstanceOf(IOException.class).hasMessageContaining("503");
        } finally {
            server.stop(0);
        }
    }
}
