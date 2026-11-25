package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The four external values that distinguish a managed Cloud runtime from an on-prem runtime.
 * Spring's relaxed binding accepts the same keys from an application properties file, environment
 * variables, or JVM system properties.
 */
@ConfigurationProperties("tapstate.cloud")
public final class CloudProperties {

    /** Base URL of the Global Control Plane API used by the embedded Cloud client. */
    private String baseUrl;

    /** Opaque Cluster credential passed to the SDK for code exchange and outbound status reporting. */
    private String token;

    /** MongoDB Atlas connection string for this Cluster's own metadata store. */
    private String atlasUri;

    /** Cloud-assigned Cluster identity; never inferred from a login JWT. */
    private String clusterId;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getAtlasUri() {
        return atlasUri;
    }

    public void setAtlasUri(String atlasUri) {
        this.atlasUri = atlasUri;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }
}
