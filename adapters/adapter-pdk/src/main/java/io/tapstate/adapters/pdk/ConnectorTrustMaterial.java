package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.*;

/** Owned connector material derived from a persisted CA bundle; never a second source of connection truth. */
public final class ConnectorTrustMaterial implements AutoCloseable {
    private final String connector;
    private final Map<String,Object> settings;
    private final Path directory;
    private ConnectorTrustMaterial(String connector, Map<String,Object> settings, Path directory) {
        this.connector = connector; this.settings = Collections.unmodifiableMap(settings); this.directory = directory;
    }
    public static ConnectorTrustMaterial open(String connector, Map<String,Object> original) {
        var settings = new LinkedHashMap<>(original);
        if (!Set.of("mongodb", "mongodb-atlas").contains(connector) || !settings.containsKey("tlsCaPem")) return new ConnectorTrustMaterial(connector, settings, null);
        Object raw = settings.remove("tlsCaPem");
        if (!(raw instanceof String pem)
                || pem.getBytes(StandardCharsets.UTF_8).length > 64 * 1024 || pem.toUpperCase(Locale.ROOT).contains("PRIVATE KEY")
                || List.of("sslCA", "sslKey", "sslKeyPassword").stream().anyMatch(settings::containsKey)
                || Boolean.FALSE.equals(settings.get("ssl")) || Boolean.FALSE.equals(settings.get("sslValidate"))) throw invalid(connector);
        try {
            String uri = settings.get("uri") instanceof String text ? text : null;
            if (uri != null && new java.net.URI(uri).getRawQuery() != null) {
                for (String pair : new java.net.URI(uri).getRawQuery().split("[&;]")) {
                    String[] parts = pair.split("=", 2);
                    String key = java.net.URLDecoder.decode(parts[0], StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
                    String value = parts.length == 2 ? java.net.URLDecoder.decode(parts[1], StandardCharsets.UTF_8).toLowerCase(Locale.ROOT) : "";
                    if ((Set.of("tls", "ssl").contains(key) && !value.equals("true"))
                            || (Set.of("tlsinsecure", "tlsallowinvalidcertificates", "tlsallowinvalidhostnames", "sslinvalidhostnameallowed").contains(key) && !value.equals("false"))
                            || Set.of("tlscafile", "tlscertificatekeyfile", "tlscertificatekeyfilepassword").contains(key)) throw invalid(connector);
                }
            }
            var certificates = CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
            if (certificates.isEmpty()) throw invalid(connector);
            for (var certificate : certificates) ((X509Certificate)certificate).checkValidity();
        } catch (java.security.cert.CertificateException | java.net.URISyntaxException | IllegalArgumentException invalid) { throw invalid(connector); }
        Path directory = null;
        try {
            directory = Files.createTempDirectory("tapstate-connector-trust-", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            Path ca = Files.createFile(directory.resolve("ca.pem"), PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.writeString(ca, pem, StandardCharsets.UTF_8);
            settings.put("ssl", true); settings.put("sslValidate", true); settings.put("sslCA", ca.toString());
            return new ConnectorTrustMaterial(connector, settings, directory);
        } catch (IOException | UnsupportedOperationException failed) {
            if (directory != null) new ConnectorTrustMaterial(connector, settings, directory).close();
            throw invalid(connector);
        }
    }
    public Map<String,Object> settings() { return settings; }
    @Override public void close() {
        if (directory == null) return;
        try { Files.deleteIfExists(directory.resolve("ca.pem")); Files.deleteIfExists(directory); }
        catch (IOException failed) { throw new TapstateException(ConnectorError.TLS_MATERIAL_CLEANUP_FAILED, Map.of("connector", connector), null); }
    }
    @Override public String toString() { return "ConnectorTrustMaterial[redacted]"; }
    private static TapstateException invalid(String connector) { return new TapstateException(ConnectorError.TLS_TRUST_INVALID, Map.of("connector", connector), null); }
}
