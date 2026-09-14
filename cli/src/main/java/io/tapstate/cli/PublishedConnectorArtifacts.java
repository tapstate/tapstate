package io.tapstate.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URLConnection;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;

/** Connector jars available from the floating {@code connectors-preview} release. */
final class PublishedConnectorArtifacts {

    private static final String RELEASES = "https://github.com/tapstate/tapstate/releases";
    private static final String CONNECTORS_PATH = "/download/connectors-preview";
    private static final String ENV_CONNECTORS_URL = "TAPSTATE_CONNECTORS_URL";
    private static final String ENV_BASE_URL = "TAPSTATE_BASE_URL";

    /** Exact public ids with a matching {@code <id>-connector.jar} release asset. */
    static final List<String> IDS = List.of("mysql", "mongodb", "postgres", "oracle", "sqlserver");

    /** Fetches one complete response body; tests replace the network at this seam. */
    interface Fetcher {
        byte[] fetch(URI from) throws IOException;

        static Fetcher http() {
            return http(name -> null);
        }

        /** Uses the standard proxy environment explicitly, including in a native image. */
        static Fetcher http(UnaryOperator<String> env) {
            return from -> fetchFollowingRedirects(from, env);
        }

        private static byte[] fetchFollowingRedirects(URI from, UnaryOperator<String> env) throws IOException {
            URI current = from;
            String originalScheme = from.getScheme();
            for (int redirects = 0; redirects <= 5; redirects++) {
                Proxy proxy = proxyFor(current, env);
                URLConnection opened = proxy == Proxy.NO_PROXY
                        ? current.toURL().openConnection() : current.toURL().openConnection(proxy);
                if (!(opened instanceof HttpURLConnection connection)) {
                    throw new IOException("download endpoint is not HTTP");
                }
                connection.setInstanceFollowRedirects(false);
                connection.setConnectTimeout(20_000);
                connection.setReadTimeout(300_000);
                connection.setRequestProperty("Accept", "application/java-archive, application/octet-stream");
                try {
                    int status = connection.getResponseCode();
                    if (status == HttpURLConnection.HTTP_OK) {
                        try (InputStream body = connection.getInputStream()) {
                            return body.readAllBytes();
                        }
                    }
                    if (status < 300 || status >= 400) {
                        throw new IOException("HTTP " + status);
                    }
                    String location = connection.getHeaderField("Location");
                    if (location == null || location.isBlank()) {
                        throw new IOException("HTTP " + status + " without a redirect location");
                    }
                    URI next = current.resolve(location);
                    if (!originalScheme.equalsIgnoreCase(next.getScheme())) {
                        throw new IOException("connector download refused a protocol-changing redirect");
                    }
                    current = next;
                } finally {
                    connection.disconnect();
                }
            }
            throw new IOException("too many connector download redirects");
        }
    }

    private PublishedConnectorArtifacts() {
    }

    static boolean contains(String id) {
        return IDS.contains(id);
    }

    static String jarName(String id) {
        return id + "-connector.jar";
    }

    static URI artifact(String id, UnaryOperator<String> env) {
        if (!contains(id)) {
            throw new IllegalArgumentException("no published connector asset for " + id);
        }
        return URI.create(base(env) + jarName(id));
    }

    /** Resolves the shared mirror settings used by both registration and the local development stack. */
    static String base(UnaryOperator<String> env) {
        String explicit = env.apply(ENV_CONNECTORS_URL);
        String base;
        if (explicit != null && !explicit.isBlank()) {
            base = explicit;
        } else {
            String releases = env.apply(ENV_BASE_URL);
            base = (releases == null || releases.isBlank() ? RELEASES : releases) + CONNECTORS_PATH;
        }
        String normalized = base.endsWith("/") ? base : base + "/";
        URI endpoint;
        try {
            endpoint = URI.create(normalized);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("connector download endpoint must be an https URL", invalid);
        }
        if (!"https".equalsIgnoreCase(endpoint.getScheme()) || endpoint.getHost() == null) {
            throw new IllegalArgumentException("connector download endpoint must be an https URL");
        }
        return normalized;
    }

    /** Fetches and rejects an error page before its bytes can reach the server registration endpoint. */
    static byte[] download(URI source, Fetcher fetcher) throws IOException {
        byte[] artifact = fetcher.fetch(source);
        if (artifact == null || artifact.length < 4
                || artifact[0] != 'P' || artifact[1] != 'K' || artifact[2] != 3 || artifact[3] != 4) {
            throw new IOException("the response is not a connector jar");
        }
        return artifact;
    }

    /** Resolves the conventional proxy variables because native Java does not inherit them itself. */
    static Proxy proxyFor(URI destination, UnaryOperator<String> env) throws IOException {
        String noProxy = first(env, "NO_PROXY", "no_proxy");
        if (bypasses(destination.getHost(), noProxy)) {
            return Proxy.NO_PROXY;
        }
        String configured = "https".equalsIgnoreCase(destination.getScheme())
                ? first(env, "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy")
                : first(env, "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy");
        if (configured == null) {
            return Proxy.NO_PROXY;
        }
        try {
            URI endpoint = URI.create(configured.contains("://") ? configured : "http://" + configured);
            if (endpoint.getHost() == null) {
                throw new IllegalArgumentException();
            }
            Proxy.Type type = endpoint.getScheme().toLowerCase(Locale.ROOT).startsWith("socks")
                    ? Proxy.Type.SOCKS : Proxy.Type.HTTP;
            int port = endpoint.getPort() >= 0 ? endpoint.getPort() : (type == Proxy.Type.SOCKS ? 1080 : 80);
            return new Proxy(type, InetSocketAddress.createUnresolved(endpoint.getHost(), port));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("the configured download proxy is not a valid URL", invalid);
        }
    }

    private static String first(UnaryOperator<String> env, String... names) {
        for (String name : names) {
            String value = env.apply(name);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static boolean bypasses(String host, String noProxy) {
        if (host == null || noProxy == null || noProxy.isBlank()) {
            return false;
        }
        for (String entry : noProxy.split(",")) {
            String candidate = entry.strip();
            if (candidate.equals("*")) {
                return true;
            }
            int port = candidate.lastIndexOf(':');
            if (port > 0 && candidate.indexOf(':') == port) {
                candidate = candidate.substring(0, port);
            }
            String suffix = candidate.startsWith(".") ? candidate.substring(1) : candidate;
            if (!suffix.isEmpty() && (host.equalsIgnoreCase(suffix)
                    || host.toLowerCase(Locale.ROOT).endsWith("." + suffix.toLowerCase(Locale.ROOT)))) {
                return true;
            }
        }
        return false;
    }
}
