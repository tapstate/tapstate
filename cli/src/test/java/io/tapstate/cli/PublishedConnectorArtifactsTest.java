package io.tapstate.cli;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.catalog.OfficialConnectors;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishedConnectorArtifactsTest {

    @Test
    void everyPublishedIdIsInsideTheSupportedConnectorBoundary() {
        assertThat(PublishedConnectorArtifacts.IDS).isSubsetOf(OfficialConnectors.IDS);
    }

    @Test
    void theHttpFetcherReturnsACompleteSuccessfulBody() throws Exception {
        byte[] jar = {(byte) 'P', (byte) 'K', 3, 4, 9};
        HttpServer server = server(200, jar);
        try {
            URI source = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/connector.jar");

            assertThat(PublishedConnectorArtifacts.download(
                    source, PublishedConnectorArtifacts.Fetcher.http())).isEqualTo(jar);
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
                    source, PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("HTTP 404");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void everyRedirectHopUsesTheSameBoundedDownloadPath() throws Exception {
        byte[] jar = {(byte) 'P', (byte) 'K', 3, 4, 9};
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
                    source, PublishedConnectorArtifacts.Fetcher.http())).isEqualTo(jar);
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
                    URI.create(base + "/missing"), PublishedConnectorArtifacts.Fetcher.http()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("HTTP 302 without a redirect location");
            assertThatThrownBy(() -> PublishedConnectorArtifacts.download(
                    URI.create(base + "/downgrade"), PublishedConnectorArtifacts.Fetcher.http()))
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
                    source, PublishedConnectorArtifacts.Fetcher.http()))
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
}
