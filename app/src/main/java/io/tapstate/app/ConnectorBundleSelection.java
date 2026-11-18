package io.tapstate.app;

import io.tapstate.control.restapi.RuntimeVersion;
import io.tapstate.core.common.JsonWriter;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Build-time connector selection using the same Maven releases and host compatibility as the service. */
public final class ConnectorBundleSelection {
    private static final List<String> CLOUD = List.of("mongodb", "mysql", "postgres", "mongodb-atlas");
    private static final List<String> ON_PREM =
            List.of("mongodb", "mysql", "postgres", "mongodb-atlas", "oracle", "sqlserver", "aws-rds-mysql", "dummy");

    private ConnectorBundleSelection() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) throw new IllegalArgumentException("Expected one connector bundle profile");
        var fetch = new ConnectorCatalogHttp();
        URI configuration = ConnectorCatalogBackground.CONFIGURATION;
        var support = ConnectorSupportManifest.read(
                new String(fetch.read(configuration.resolve("supported-connectors.json")), StandardCharsets.UTF_8));
        var repositories = ConnectorReleaseCatalog.repositories(
                new String(fetch.read(configuration.resolve("repositories.json")), StandardCharsets.UTF_8));
        var selected = select(args[0], support, repositories, RuntimeVersion.current(),
                ConnectorCatalogBackground.hostApiVersion(), fetch);
        System.out.println(JsonWriter.write(Map.of("schemaVersion", 1, "connectors", selected.stream()
                .map(ConnectorBundleSelection::asJson).toList())));
    }

    static List<ConnectorReleaseCatalog.Download> select(
            String profile, ConnectorSupportManifest support, Map<String, ConnectorReleaseCatalog.Repository> repositories,
            String engineVersion, String hostApiVersion, ConnectorReleaseCatalog.Fetch fetch)
            throws IOException, InterruptedException {
        var required = switch (profile) {
            case "cloud" -> CLOUD;
            case "onprem" -> ON_PREM;
            default -> throw new IllegalArgumentException("Unknown connector bundle profile");
        };
        Map<String, ConnectorSupportManifest.Entry> manifest = new LinkedHashMap<>();
        for (String id : required) {
            var entry = support.entries().get(id);
            if (entry == null) throw new IOException("Required connector is outside the official support manifest: " + id);
            manifest.put(id, entry);
        }
        var downloads = ConnectorReleaseCatalog.resolve(new ConnectorSupportManifest(support.version(), manifest),
                repositories, Map.of(), engineVersion, hostApiVersion, fetch);
        for (String id : required) {
            if (!downloads.containsKey(id)) throw new IOException("No compatible published JAR is available for " + id);
        }
        return required.stream().map(downloads::get).toList();
    }

    private static Map<String, String> asJson(ConnectorReleaseCatalog.Download download) {
        var target = download.release().coordinate();
        return Map.of("id", download.connectorId(), "groupId", target.groupId(), "artifactId", target.artifactId(),
                "version", target.version(), "pdkApiVersion", download.release().pdkApiVersion(),
                "sha256", download.sha256(), "url", download.jar().toString());
    }
}
