package io.tapstate.cli;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.catalog.OfficialConnectors;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishedConnectorArtifactsTest {

    @Test
    void everyPublishedIdIsInsideTheSupportedConnectorBoundary() {
        assertThat(PublishedConnectorArtifacts.IDS).isSubsetOf(OfficialConnectors.IDS);
    }

    @Test
    void atlasDownloadIsPinnedToTheTestedShadedArtifact() throws Exception {
        URI source = PublishedConnectorArtifacts.artifact("mongodb-atlas", ignored -> null);
        assertThat(source).isEqualTo(URI.create("https://github.com/tapstate/tapstate/releases/download/"
                + "connectors-preview/mongodb-atlas-connector.jar"));
        byte[] bytes = {1, 2, 3};
        assertThat(PublishedConnectorArtifacts.download("mongodb-atlas", source, (from, expected) -> {
            assertThat(from).isEqualTo(source);
            assertThat(expected.bytes()).isEqualTo(19_764_088);
            assertThat(expected.sha256()).isEqualTo(
                    "2f70bfe42baafcadb0c2d88042e173156c714f508d5089ce7eb8eab7edaa8e18");
            return PublishedConnectorArtifacts.Fetched.verified(bytes);
        })).isEqualTo(bytes);
    }

    @Test
    void theHttpFetcherReturnsACompleteSuccessfulBody() throws Exception {
        byte[] jar = completeJar();
        HttpServer server = server(200, jar);
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/connector.jar");

            assertThat(PublishedConnectorArtifacts.download(
                    expected(jar), source, PublishedConnectorArtifacts.Fetcher.http())).isEqualTo(jar);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void theHttpFetcherRejectsATruncatedBodyWithADeclaredLength() {
        byte[] truncated = {(byte) 'P', (byte) 'K', 3, 4, 9};
        PublishedConnectorArtifacts.Artifact expected = expected(completeJar());
        HttpServer server = declaredLengthServer(expected.bytes(), truncated);
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/connector.jar");

            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    expected, source, PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("truncated");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void trustedMetadataRejectsATruncatedZipWithoutLengthMetadata() {
        byte[] truncated = {(byte) 'P', (byte) 'K', 3, 4, 9};

        assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                expected(completeJar()),
                URI.create("https://example.invalid/connector.jar"),
                (ignored, expected) -> PublishedConnectorArtifacts.Fetched.unverified(truncated)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("length does not match the published asset");
    }

    @Test
    void trustedMetadataRejectsSameLengthBytesWithTheWrongDigest() {
        byte[] expectedBytes = completeJar();
        byte[] corrupted = expectedBytes.clone();
        corrupted[corrupted.length - 1] ^= 1;

        assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                expected(expectedBytes),
                URI.create("https://example.invalid/connector.jar"),
                (ignored, expected) -> PublishedConnectorArtifacts.Fetched.unverified(corrupted)))
                .isInstanceOf(IOException.class)
                .hasMessage("connector artifact checksum does not match the published SHA-256");
    }

    @Test
    void theHttpFetcherRejectsAnOversizedDeclaredBodyBeforeReadingIt() {
        HttpServer server = declaredLengthServer(70L * 1024 * 1024, new byte[0]);
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/connector.jar");

            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    expected(completeJar()), source, PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("exceeds the 64 MiB connector artifact limit");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void theHttpFetcherBoundsAnOversizedChunkedBody() {
        HttpServer server = chunkedServer(64L * 1024 * 1024 + 1);
        PublishedConnectorArtifacts.Artifact maximum =
                new PublishedConnectorArtifacts.Artifact(64L * 1024 * 1024, "unused-after-size-refusal");
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/connector.jar");

            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    maximum, source, PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("exceeds the 64 MiB connector artifact limit");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void theHttpFetcherRejectsAnErrorResponseBeforeRegistration() {
        HttpServer server = server(404, "not found".getBytes());
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/connector.jar");

            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    expected(completeJar()), source, PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("HTTP 404");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void everyRedirectHopUsesTheSameBoundedDownloadPath() throws Exception {
        byte[] jar = completeJar();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/release", exchange -> {
            exchange.getResponseHeaders().add("Location", "/connector.jar");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/connector.jar", exchange -> {
            exchange.sendResponseHeaders(200, jar.length);
            exchange.getResponseBody().write(jar);
            exchange.close();
        });
        server.start();
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/release");

            assertThat(PublishedConnectorArtifacts.download(
                    expected(jar), source, PublishedConnectorArtifacts.Fetcher.http())).isEqualTo(jar);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aRedirectWithoutALocationAndAProtocolChangeAreRefused() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/missing", exchange -> {
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext("/downgrade", exchange -> {
            exchange.getResponseHeaders().add("Location", "https://example.invalid/connector.jar");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.start();
        try {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    expected(completeJar()), URI.create(base + "/missing"),
                    PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("HTTP 302 without a redirect location");
            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    expected(completeJar()), URI.create(base + "/downgrade"),
                    PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("connector download refused a protocol-changing redirect");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aRedirectLoopStopsAtTheDeclaredBound() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/loop", exchange -> {
            exchange.getResponseHeaders().add("Location", "/loop");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.start();
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/loop");

            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    expected(completeJar()), source, PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("too many connector download redirects");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void nativeDownloadsResolveTheStandardHttpsProxyAndNoProxyVariables() throws Exception {
        Map<String, String> env = Map.of(
                "https_proxy", "http://proxy.example:8443",
                "NO_PROXY", "localhost,.internal.example");

        Proxy proxied = PublishedConnectorArtifacts.proxyFor(
                URI.create("https://github.com/file.jar"), env::get);
        Proxy bypassed = PublishedConnectorArtifacts.proxyFor(
                URI.create("https://db.internal.example/file.jar"), env::get);

        assertThat(proxied.type()).isEqualTo(Proxy.Type.HTTP);
        assertThat((InetSocketAddress) proxied.address())
                .extracting(InetSocketAddress::getHostString, InetSocketAddress::getPort)
                .containsExactly("proxy.example", 8443);
        assertThat(bypassed).isEqualTo(Proxy.NO_PROXY);
    }

    @Test
    void proxyDefaultsByTypeAndNoProxyAcceptsAHostWithItsPort() throws Exception {
        Proxy socks = PublishedConnectorArtifacts.proxyFor(
                URI.create("https://github.com/file.jar"),
                Map.of("ALL_PROXY", "socks5://proxy.example")::get);
        Proxy http = PublishedConnectorArtifacts.proxyFor(
                URI.create("https://github.com/file.jar"),
                Map.of("HTTPS_PROXY", "proxy.example")::get);
        Proxy bypassed = PublishedConnectorArtifacts.proxyFor(
                URI.create("https://github.com/file.jar"),
                Map.of("HTTPS_PROXY", "http://proxy.example:8443", "NO_PROXY", "github.com:443")::get);

        assertThat(socks.type()).isEqualTo(Proxy.Type.SOCKS);
        assertThat(((InetSocketAddress) socks.address()).getPort()).isEqualTo(1080);
        assertThat(http.type()).isEqualTo(Proxy.Type.HTTP);
        assertThat(((InetSocketAddress) http.address()).getPort()).isEqualTo(80);
        assertThat(bypassed).isEqualTo(Proxy.NO_PROXY);
    }

    @Test
    void noProxyPortQualifiersAndBracketedIpv6MatchTheDestinationExactly() throws Exception {
        Map<String, String> mismatchedPort = Map.of(
                "HTTPS_PROXY", "http://proxy.example:8443",
                "NO_PROXY", "mirror.example:9443");
        Map<String, String> matchingIpv6 = Map.of(
                "HTTPS_PROXY", "http://proxy.example:8443",
                "NO_PROXY", "[2001:db8::1]:443");
        Map<String, String> mismatchedIpv6 = Map.of(
                "HTTPS_PROXY", "http://proxy.example:8443",
                "NO_PROXY", "[2001:db8::1]:9443");

        assertThat(PublishedConnectorArtifacts.proxyFor(
                URI.create("https://mirror.example/file.jar"), mismatchedPort::get).type())
                .isEqualTo(Proxy.Type.HTTP);
        assertThat(PublishedConnectorArtifacts.proxyFor(
                URI.create("https://[2001:db8::1]/file.jar"), matchingIpv6::get))
                .isEqualTo(Proxy.NO_PROXY);
        assertThat(PublishedConnectorArtifacts.proxyFor(
                URI.create("https://[2001:db8::1]/file.jar"), mismatchedIpv6::get).type())
                .isEqualTo(Proxy.Type.HTTP);
    }

    @Test
    void unknownAssetsAndMalformedDownloadConfigurationAreRefused() {
        assertThatThrownBy(() -> PublishedConnectorArtifacts.artifact("not-published", name -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("no published connector asset for not-published");
        assertThatThrownBy(() -> PublishedConnectorArtifacts.base(
                Map.of("TAPSTATE_CONNECTORS_URL", "https://[broken")::get))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("connector download endpoint must be an https URL");
        assertThatThrownBy(() -> PublishedConnectorArtifacts.proxyFor(
                URI.create("https://github.com/file.jar"),
                Map.of("HTTPS_PROXY", "https://[broken")::get))
                .isInstanceOf(IOException.class)
                .hasMessage("the configured download proxy is not a valid URL");
    }

    private static HttpServer server(int status, byte[] body) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/connector.jar", exchange -> {
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException failed) {
            throw new AssertionError("could not start the HTTP test server", failed);
        }
    }

    private static HttpServer declaredLengthServer(long declaredLength, byte[] body) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/connector.jar", exchange -> {
                exchange.sendResponseHeaders(200, declaredLength);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException failed) {
            throw new AssertionError("could not start the HTTP test server", failed);
        }
    }

    private static HttpServer chunkedServer(long bytes) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/connector.jar", exchange -> {
                exchange.sendResponseHeaders(200, 0);
                byte[] chunk = new byte[8192];
                long remaining = bytes;
                try {
                    while (remaining > 0) {
                        int length = (int) Math.min(chunk.length, remaining);
                        exchange.getResponseBody().write(chunk, 0, length);
                        remaining -= length;
                    }
                } catch (IOException clientStoppedAtTheLimit) {
                    // The bounded client is expected to close before the oversized response completes.
                } finally {
                    exchange.close();
                }
            });
            server.start();
            return server;
        } catch (IOException failed) {
            throw new AssertionError("could not start the HTTP test server", failed);
        }
    }

    private static byte[] completeJar() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (JarOutputStream jar = new JarOutputStream(bytes)) {
                jar.putNextEntry(new JarEntry("connector.txt"));
                jar.write("connector".getBytes());
                jar.closeEntry();
            }
            return bytes.toByteArray();
        } catch (IOException failed) {
            throw new AssertionError("could not create the connector jar fixture", failed);
        }
    }

    private static PublishedConnectorArtifacts.Artifact expected(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return new PublishedConnectorArtifacts.Artifact(
                    bytes.length, HexFormat.of().formatHex(digest.digest(bytes)));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new AssertionError("SHA-256 is required by the Java platform", unavailable);
        }
    }
}
