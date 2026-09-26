package io.tapstate.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;

/** Connector jars available from the floating {@code connectors-preview} release. */
final class PublishedConnectorArtifacts {

    private static final String RELEASES = "https://github.com/tapstate/tapstate/releases";
    private static final String CONNECTORS_PATH = "/download/connectors-preview";
    private static final String ENV_CONNECTORS_URL = "TAPSTATE_CONNECTORS_URL";
    private static final String ENV_BASE_URL = "TAPSTATE_BASE_URL";

    /** Current preview artifacts fit below 51 MiB; 64 MiB bounds both declared and chunked responses. */
    private static final long MAX_ARTIFACT_BYTES = 64L * 1024 * 1024;
    private static final int COPY_BUFFER_BYTES = 8192;

    private static final Map<String, Artifact> ARTIFACTS = Map.of(
            "mysql", new Artifact(47_131_206,
                    "34def7fca33fdf80f9d7e5561b2a2d9b7a2733fd06da9974359bbb77066ca0e2"),
            "mongodb", new Artifact(20_400_364,
                    "7fbdbf1ef2965053c9c5e6d28c0c21c3aa2818938ab758b965cc278d61e0c7b4"),
            "mongodb-atlas", new Artifact(19_764_088,
                    "2f70bfe42baafcadb0c2d88042e173156c714f508d5089ce7eb8eab7edaa8e18"),
            "aws-rds-mysql", new Artifact(47_043_627,
                    "c70999b64201fbcfbe7b34acf9f6fa1b1d8aa357b3db770bbcab980deed9b399"),
            "postgres", new Artifact(52_771_466,
                    "535aaaed34594dcc9e2dd1937efc4b91865911f11de624aca555b59a609053ee"),
            "oracle", new Artifact(36_837_471,
                    "5e273f48ff9ee935881db3863bbbf165e91d86edb9aae3f929f1b835b13c3151"),
            "sqlserver", new Artifact(16_232_120,
                    "3254471003dbb5cb6512610efae34bc35f4d4a77c8c02cfbf9fc883659a4c826"));

    /** Exact public ids with a matching {@code <id>-connector.jar} release asset and trusted digest. */
    static final List<String> IDS = List.copyOf(ARTIFACTS.keySet());

    /** Fetches one complete response body; tests replace the network at this seam. */
    interface Fetcher {
        Fetched fetch(URI from, Artifact expected) throws IOException;

        static Fetcher http() {
            return http(name -> null);
        }

        /** Uses the standard proxy environment explicitly, including in a native image. */
        static Fetcher http(UnaryOperator<String> env) {
            return (from, expected) -> fetchFollowingRedirects(from, expected, env);
        }

        private static Fetched fetchFollowingRedirects(
                URI from, Artifact expected, UnaryOperator<String> env) throws IOException {
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
                        return readVerifiedArtifact(connection, expected);
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

    /** Expected release bytes; changing a floating asset requires updating this metadata in the CLI. */
    record Artifact(long bytes, String sha256) {
    }

    /** Carries whether a transport seam already checked the trusted length and digest. */
    static final class Fetched {
        private final byte[] bytes;
        private final boolean verified;

        private Fetched(byte[] bytes, boolean verified) {
            this.bytes = bytes;
            this.verified = verified;
        }

        static Fetched unverified(byte[] bytes) {
            return new Fetched(bytes, false);
        }

        static Fetched verified(byte[] bytes) {
            return new Fetched(bytes, true);
        }

        byte[] bytes() {
            return bytes;
        }

        boolean verified() {
            return verified;
        }
    }

    private PublishedConnectorArtifacts() {
    }

    static boolean contains(String id) {
        return ARTIFACTS.containsKey(id);
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
    static byte[] download(String id, URI source, Fetcher fetcher) throws IOException {
        Artifact expected = ARTIFACTS.get(id);
        if (expected == null) {
            throw new IllegalArgumentException("no published connector asset for " + id);
        }
        return download(expected, source, fetcher);
    }

    static byte[] download(Artifact expected, URI source, Fetcher fetcher) throws IOException {
        Fetched fetched = fetcher.fetch(source, expected);
        byte[] artifact = fetched == null ? null : fetched.bytes();
        if (artifact == null) {
            throw new IOException("connector artifact response was empty");
        }
        if (artifact.length > MAX_ARTIFACT_BYTES) {
            throw artifactTooLarge();
        }
        if (!fetched.verified()) {
            verifyArtifact(artifact, expected);
        }
        return artifact;
    }

    /** Resolves the conventional proxy variables because native Java does not inherit them itself. */
    static Proxy proxyFor(URI destination, UnaryOperator<String> env) throws IOException {
        String noProxy = first(env, "NO_PROXY", "no_proxy");
        if (bypasses(destination, noProxy)) {
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

    private static boolean bypasses(URI destination, String noProxy) {
        String host = normalizeHost(destination.getHost());
        if (host == null || noProxy == null || noProxy.isBlank()) {
            return false;
        }
        int destinationPort = effectivePort(destination);
        for (String entry : noProxy.split(",")) {
            String candidate = entry.strip();
            if (candidate.equals("*")) {
                return true;
            }
            NoProxyTarget target = NoProxyTarget.parse(candidate);
            if (target == null || target.port() != null && target.port() != destinationPort) {
                continue;
            }
            String suffix = target.host().startsWith(".") ? target.host().substring(1) : target.host();
            if (!suffix.isEmpty() && (host.equalsIgnoreCase(suffix)
                    || suffix.indexOf(':') < 0
                    && host.toLowerCase(Locale.ROOT).endsWith("." + suffix.toLowerCase(Locale.ROOT)))) {
                return true;
            }
        }
        return false;
    }

    private static Fetched readVerifiedArtifact(HttpURLConnection connection, Artifact expected) throws IOException {
        long declaredLength = connection.getContentLengthLong();
        if (declaredLength > MAX_ARTIFACT_BYTES) {
            throw artifactTooLarge();
        }
        if (declaredLength >= 0 && declaredLength != expected.bytes()) {
            throw unexpectedLength(expected.bytes(), declaredLength);
        }
        Path temporaryDirectory = Path.of(System.getProperty("java.io.tmpdir", "."));
        Path spool = Files.createTempFile(temporaryDirectory, "tapstate-connector-", ".jar");
        try {
            long received = 0;
            byte[] buffer = new byte[COPY_BUFFER_BYTES];
            MessageDigest digest = sha256Digest();
            try (InputStream body = connection.getInputStream(); OutputStream file = Files.newOutputStream(spool)) {
                while (true) {
                    long lastAllowedByte = Math.min(MAX_ARTIFACT_BYTES, expected.bytes());
                    int allowed = (int) Math.min(buffer.length, lastAllowedByte - received + 1);
                    int read = body.read(buffer, 0, allowed);
                    if (read < 0) {
                        break;
                    }
                    if (read == 0) {
                        continue;
                    }
                    received += read;
                    if (received > MAX_ARTIFACT_BYTES) {
                        throw artifactTooLarge();
                    }
                    if (received > expected.bytes()) {
                        throw unexpectedLength(expected.bytes(), received);
                    }
                    digest.update(buffer, 0, read);
                    file.write(buffer, 0, read);
                }
            }
            if (declaredLength >= 0 && received != declaredLength) {
                throw new IOException("connector artifact was truncated: expected "
                        + declaredLength + " bytes but received " + received);
            }
            if (received != expected.bytes()) {
                throw unexpectedLength(expected.bytes(), received);
            }
            verifyDigest(expected, HexFormat.of().formatHex(digest.digest()));
            return Fetched.verified(Files.readAllBytes(spool));
        } finally {
            Files.deleteIfExists(spool);
        }
    }

    private static void verifyArtifact(byte[] artifact, Artifact expected) throws IOException {
        if (artifact.length != expected.bytes()) {
            throw unexpectedLength(expected.bytes(), artifact.length);
        }
        MessageDigest digest = sha256Digest();
        verifyDigest(expected, HexFormat.of().formatHex(digest.digest(artifact)));
    }

    private static void verifyDigest(Artifact expected, String actual) throws IOException {
        if (!expected.sha256().equals(actual)) {
            throw new IOException("connector artifact checksum does not match the published SHA-256");
        }
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
    }

    private static IOException unexpectedLength(long expected, long actual) {
        return new IOException("connector artifact length does not match the published asset: expected "
                + expected + " bytes but received " + actual);
    }

    private static IOException artifactTooLarge() {
        return new IOException("response exceeds the 64 MiB connector artifact limit");
    }

    private static int effectivePort(URI destination) {
        if (destination.getPort() >= 0) {
            return destination.getPort();
        }
        if ("https".equalsIgnoreCase(destination.getScheme())) {
            return 443;
        }
        if ("http".equalsIgnoreCase(destination.getScheme())) {
            return 80;
        }
        return -1;
    }

    private static String normalizeHost(String host) {
        if (host != null && host.length() >= 2 && host.startsWith("[") && host.endsWith("]")) {
            return host.substring(1, host.length() - 1);
        }
        return host;
    }

    private record NoProxyTarget(String host, Integer port) {
        private static NoProxyTarget parse(String candidate) {
            if (candidate.isEmpty()) {
                return null;
            }
            if (candidate.startsWith("[")) {
                int close = candidate.indexOf(']');
                if (close < 0) {
                    return null;
                }
                String host = candidate.substring(1, close);
                String remainder = candidate.substring(close + 1);
                if (remainder.isEmpty()) {
                    return new NoProxyTarget(host, null);
                }
                if (!remainder.startsWith(":")) {
                    return null;
                }
                Integer port = parsePort(remainder.substring(1));
                return port == null ? null : new NoProxyTarget(host, port);
            }
            int firstColon = candidate.indexOf(':');
            int lastColon = candidate.lastIndexOf(':');
            if (firstColon >= 0 && firstColon == lastColon) {
                Integer port = parsePort(candidate.substring(lastColon + 1));
                return port == null ? null : new NoProxyTarget(candidate.substring(0, lastColon), port);
            }
            return new NoProxyTarget(candidate, null);
        }

        private static Integer parsePort(String value) {
            try {
                int port = Integer.parseInt(value);
                return port >= 0 && port <= 65_535 ? port : null;
            } catch (NumberFormatException invalid) {
                return null;
            }
        }
    }
}
