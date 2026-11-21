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
        if (args.length == 0 || args.length != 1 && !"op".equals(args[0])) {
            throw new IllegalArgumentException("Expected a connector bundle profile and optional OP connector IDs");
        }
        List<String> required = "op".equals(args[0])
                ? java.util.stream.Stream.concat(java.util.stream.Stream.of("mongodb"),
                        java.util.Arrays.stream(args).skip(1)).distinct().toList()
                : profileIds(args[0]);
        var fetch = new ConnectorCatalogHttp();
        URI configuration = ConnectorCatalogBackground.CONFIGURATION;
        var support = ConnectorSupportManifest.read(
                new String(fetch.read(configuration.resolve("supported-connectors.json")), StandardCharsets.UTF_8));
        var repositories = ConnectorReleaseCatalog.repositories(
                new String(fetch.read(configuration.resolve("repositories.json")), StandardCharsets.UTF_8));
        var selected = select(required, support, repositories, RuntimeVersion.current(),
                ConnectorCatalogBackground.hostApiVersion(), fetch);
        System.out.println(JsonWriter.write(Map.of("schemaVersion", 1, "connectors", selected.stream()
                .map(ConnectorBundleSelection::asJson).toList())));
    }

    static List<ConnectorReleaseCatalog.Download> select(
            String profile, ConnectorSupportManifest support, Map<String, ConnectorReleaseCatalog.Repository> repositories,
            String engineVersion, String hostApiVersion, ConnectorReleaseCatalog.Fetch fetch)
            throws IOException, InterruptedException {
        return select(profileIds(profile), support, repositories, engineVersion, hostApiVersion, fetch);
    }

    private static List<String> profileIds(String profile) {
        return switch (profile) {
            case "cloud" -> CLOUD;
            case "onprem" -> ON_PREM;
            default -> throw new IllegalArgumentException("Unknown connector bundle profile");
        };
    }

    private static List<ConnectorReleaseCatalog.Download> select(
            List<String> required, ConnectorSupportManifest support,
            Map<String, ConnectorReleaseCatalog.Repository> repositories,
            String engineVersion, String hostApiVersion, ConnectorReleaseCatalog.Fetch fetch)
            throws IOException, InterruptedException {
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
