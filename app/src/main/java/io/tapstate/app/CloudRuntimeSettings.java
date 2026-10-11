package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoDatabaseNames;
import io.tapstate.core.common.TapstateException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * Validated Cloud startup settings. No single value enables Cloud behavior: all four values must be
 * present, while their complete absence preserves the existing on-prem behavior. This keeps an
 * on-prem deployment that deliberately points its ordinary Mongo setting at Atlas from being
 * mistaken for a managed Cloud Cluster.
 *
 * <p>The token and Atlas URI are intentionally omitted from {@link #toString()} because both may carry
 * credentials.
 */
final class CloudRuntimeSettings {

    private static final int MAX_DATABASE_NAME_BYTES = 63;
    private static final String OPERATOR_SUFFIX = "_operator";
    private static final String VIEWS_SUFFIX = "_views";

    enum Mode {
        ON_PREM,
        CLOUD
    }

    private final Mode mode;
    private final URI baseUrl;
    private final String token;
    private final String atlasUri;
    private final String clusterId;
    private final String operatorStateDatabase;
    private final String viewsDatabase;

    private CloudRuntimeSettings(Mode mode, URI baseUrl, String token, String atlasUri, String clusterId,
            String operatorStateDatabase, String viewsDatabase) {
        this.mode = Objects.requireNonNull(mode, "mode");
        this.baseUrl = baseUrl;
        this.token = token;
        this.atlasUri = atlasUri;
        this.clusterId = clusterId;
        this.operatorStateDatabase = operatorStateDatabase;
        this.viewsDatabase = viewsDatabase;
    }

    static CloudRuntimeSettings resolve(CloudProperties properties) {
        return resolve(properties, null);
    }

    /** The SDK's unprefixed Cluster ID is a fallback only when the canonical setting is absent. */
    static CloudRuntimeSettings resolve(CloudProperties properties, String sdkClusterId) {
        Objects.requireNonNull(properties, "properties");
        String clusterId = properties.getClusterId() != null ? properties.getClusterId() : sdkClusterId;
        boolean any = properties.getBaseUrl() != null
                || properties.getToken() != null
                || properties.getAtlasUri() != null
                || clusterId != null;
        if (!any) {
            return new CloudRuntimeSettings(Mode.ON_PREM, null, null, null, null, null, null);
        }
        if (!hasText(properties.getBaseUrl())
                || !hasText(properties.getToken())
                || !hasText(properties.getAtlasUri())
                || !hasText(clusterId)) {
            throw new TapstateException(BootError.CLOUD_CONFIG_INCOMPLETE, Map.of(), null);
        }
        URI baseUrl = parseBaseUrl(properties.getBaseUrl());
        String atlasUri = properties.getAtlasUri().trim();
        if (!(atlasUri.startsWith("mongodb://") || atlasUri.startsWith("mongodb+srv://"))) {
            throw new TapstateException(BootError.CLOUD_ATLAS_URI_INVALID, Map.of(), null);
        }
        String metadataDatabase = metadataDatabase(atlasUri);
        return new CloudRuntimeSettings(Mode.CLOUD, baseUrl, properties.getToken(), atlasUri, clusterId.trim(),
                derivedDatabase(metadataDatabase, OPERATOR_SUFFIX),
                derivedDatabase(metadataDatabase, VIEWS_SUFFIX));
    }

    private static URI parseBaseUrl(String raw) {
        try {
            URI parsed = new URI(raw.trim());
            String scheme = parsed.getScheme();
            if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                    || parsed.getHost() == null
                    || parsed.getUserInfo() != null
                    || parsed.getQuery() != null
                    || parsed.getFragment() != null) {
                throw new IllegalArgumentException("not an absolute HTTP(S) base URL");
            }
            String normalized = parsed.toString();
            while (normalized.endsWith("/")) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            return URI.create(normalized);
        } catch (URISyntaxException | IllegalArgumentException invalid) {
            // URI parser diagnostics echo the rejected input. The base URL is configuration and may be
            // malformed precisely because somebody accidentally embedded userinfo, so never attach that
            // diagnostic to an operator-visible failure or log chain.
            throw new TapstateException(BootError.CLOUD_BASE_URL_INVALID, Map.of(), null);
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    Mode mode() {
        return mode;
    }

    boolean cloud() {
        return mode == Mode.CLOUD;
    }

    URI baseUrl() {
        return baseUrl;
    }

    String token() {
        return token;
    }

    String atlasUri() {
        return atlasUri;
    }

    String clusterId() {
        return clusterId;
    }

    String metadataUri(String onPremUri) {
        return cloud() ? atlasUri : onPremUri;
    }

    String operatorStateDatabase(String onPremDatabase) {
        return cloud() ? operatorStateDatabase : onPremDatabase;
    }

    String viewsDatabase(String onPremDatabase) {
        return cloud() ? viewsDatabase : onPremDatabase;
    }

    private static String metadataDatabase(String atlasUri) {
        try {
            String path = new URI(atlasUri).getRawPath();
            if (path == null || path.length() <= 1 || path.indexOf('/', 1) >= 0) {
                throw new IllegalArgumentException("missing database");
            }
            // URLDecoder implements form semantics for '+', while this is a URI path. Preserve a
            // literal plus before decoding percent escapes so a path cannot silently name another DB.
            String database = URLDecoder.decode(path.substring(1).replace("+", "%2B"), StandardCharsets.UTF_8);
            if (database.isBlank()) throw new IllegalArgumentException("missing database");
            if (!MongoDatabaseNames.isValid(database)) throw new IllegalArgumentException("invalid database");
            return database;
        } catch (URISyntaxException | IllegalArgumentException invalid) {
            // Driver diagnostics quote connection strings and can therefore carry credentials.
            throw new TapstateException(BootError.CLOUD_ATLAS_URI_INVALID, Map.of(), null);
        }
    }

    private static String derivedDatabase(String metadataDatabase, String suffix) {
        String derived = metadataDatabase + suffix;
        if (derived.getBytes(StandardCharsets.UTF_8).length > MAX_DATABASE_NAME_BYTES) {
            throw new TapstateException(BootError.CLOUD_ATLAS_URI_INVALID, Map.of(), null);
        }
        return derived;
    }

    @Override
    public String toString() {
        return "CloudRuntimeSettings[mode=" + mode + ", baseUrl=" + baseUrl
                + ", token=<redacted>, atlasUri=<redacted>]";
    }
}
