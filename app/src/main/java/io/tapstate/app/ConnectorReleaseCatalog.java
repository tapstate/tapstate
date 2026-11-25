package io.tapstate.app;

import io.tapstate.core.common.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/** Resolves the official catalog against one immutable set of repository settings. */
final class ConnectorReleaseCatalog {
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

    interface Fetch {
        byte[] read(URI uri) throws IOException, InterruptedException;

        boolean available(URI uri) throws IOException, InterruptedException;

        String authentication(URI jar) throws IOException, InterruptedException;
    }

    record Repository(URI baseUrl, MavenConnectorCatalog.Coordinate bom) {
        Repository {
            if (baseUrl.getHost() == null || baseUrl.getUserInfo() != null || baseUrl.getQuery() != null
                    || baseUrl.getFragment() != null
                    || !Set.of("http", "https").contains(baseUrl.getScheme())) {
                throw new IllegalArgumentException("Invalid Maven repository URL");
            }
            if (!baseUrl.getPath().endsWith("/")) baseUrl = URI.create(baseUrl + "/");
        }
    }

    /** Captured by value so a simultaneous catalog refresh cannot retarget an in-flight download. */
    record Download(String connectorId, MavenConnectorCatalog.Release release, URI jar, String sha256) {
    }

    private ConnectorReleaseCatalog() {
    }

    static Map<String, Repository> repositories(String json) throws IOException {
        try {
            if (!(JsonReader.parse(json) instanceof Map<?, ?> root)
                    || !Long.valueOf(1).equals(root.get("schemaVersion"))
                    || !(root.get("repositories") instanceof Map<?, ?> entries) || entries.isEmpty()) {
                throw new IllegalArgumentException("Invalid connector repository configuration");
            }
            var repositories = new LinkedHashMap<String, Repository>();
            for (var entry : entries.entrySet()) {
                if (!(entry.getKey() instanceof String id) || id.isBlank()
                        || !(entry.getValue() instanceof Map<?, ?> row) || !(row.get("bom") instanceof Map<?, ?> bom)) {
                    throw new IllegalArgumentException("Invalid connector repository entry");
                }
                repositories.put(id, new Repository(URI.create(required(row, "baseUrl")), new MavenConnectorCatalog.Coordinate(
                        required(bom, "groupId"), required(bom, "artifactId"), required(bom, "version"))));
            }
            return Map.copyOf(repositories);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid connector repository configuration", invalid);
        }
    }

    static Map<String, Download> resolve(
            ConnectorSupportManifest support, Map<String, Repository> repositories, Map<String, Download> previous,
            String engineVersion, String hostApiVersion, Fetch fetch) throws IOException, InterruptedException {
        var downloads = new LinkedHashMap<String, Download>();
        for (var supported : support.entries().values()) {
            if (!repositories.containsKey(supported.repository())) {
                throw new IOException("No repository configured for " + supported.id());
            }
        }
        for (var repository : repositories.entrySet()) {
            List<ConnectorSupportManifest.Entry> entries = support.entries().values().stream()
                    .filter(entry -> entry.repository().equals(repository.getKey()))
                    .filter(entry -> support.admitted(entry.id(), engineVersion).isPresent())
                    .toList();
            if (!entries.isEmpty()) {
                resolveRepository(repository.getValue(), entries, previous, hostApiVersion, fetch, downloads);
            }
        }
        return Map.copyOf(downloads);
    }

    private static void resolveRepository(
            Repository repository, List<ConnectorSupportManifest.Entry> entries, Map<String, Download> previous,
            String hostApiVersion, Fetch fetch, Map<String, Download> downloads) throws IOException, InterruptedException {
        var candidates = new ArrayList<>(readBom(repository, repository.bom(), fetch));
        if (entries.stream().anyMatch(entry -> requiresHistory(candidates, entry, hostApiVersion))) {
            var bom = repository.bom();
            var metadata = fetch.read(bom.metadata(repository.baseUrl()));
            for (String version : MavenConnectorCatalog.readBomVersions(new ByteArrayInputStream(metadata), bom)) {
                candidates.addAll(readBom(repository,
                        new MavenConnectorCatalog.Coordinate(bom.groupId(), bom.artifactId(), version), fetch));
            }
        }
        for (var entry : entries) {
            Optional<MavenConnectorCatalog.Release> release = MavenConnectorCatalog
                    .compatibleReleases(candidates, entry.artifactId(), hostApiVersion).stream().findFirst();
            if (release.isPresent()) {
                readDownload(repository, entry.id(), release.get(), previous.get(entry.id()), fetch)
                        .ifPresent(download -> downloads.put(entry.id(), download));
            }
        }
    }

    private static boolean requiresHistory(List<MavenConnectorCatalog.Release> releases,
                                           ConnectorSupportManifest.Entry entry, String hostApiVersion) {
        boolean isPublished = releases.stream()
                .anyMatch(release -> release.coordinate().artifactId().equals(entry.artifactId()));
        return isPublished && MavenConnectorCatalog.compatibleReleases(releases, entry.artifactId(), hostApiVersion)
                .stream().noneMatch(release -> MavenConnectorCatalog.sameGeneration(release.pdkApiVersion(), hostApiVersion));
    }

    private static List<MavenConnectorCatalog.Release> readBom(
            Repository repository, MavenConnectorCatalog.Coordinate bom, Fetch fetch) throws IOException, InterruptedException {
        return MavenConnectorCatalog.read(
                new ByteArrayInputStream(fetch.read(bom.file(repository.baseUrl(), "pom"))), bom);
    }

    private static Optional<Download> readDownload(
            Repository repository, String id, MavenConnectorCatalog.Release release,
            Download previous, Fetch fetch) throws IOException, InterruptedException {
        var coordinate = release.coordinate();
        URI jar = coordinate.file(repository.baseUrl(), "jar");
        byte[] response;
        try {
            response = fetch.read(coordinate.file(repository.baseUrl(), "jar.sha256"));
        } catch (MissingResource missing) {
            return Optional.empty();
        }
        String digest = new String(response, StandardCharsets.US_ASCII).strip();
        if (!SHA256.matcher(digest).matches()) throw new IOException("Invalid SHA-256 for " + coordinate);
        if (previous != null && previous.release().coordinate().equals(coordinate) && !previous.sha256().equals(digest)) {
            throw new IOException("Conflicting SHA-256 for " + coordinate);
        }
        return fetch.available(jar) ? Optional.of(new Download(id, release, jar, digest)) : Optional.empty();
    }

    private static String required(Map<?, ?> row, String field) {
        if (row.get(field) instanceof String value && !value.isBlank()) {
            return value;
        }
        throw new IllegalArgumentException("Missing repository field: " + field);
    }

    static final class MissingResource extends IOException {
        MissingResource(URI uri) {
            super("Maven resource not found: " + uri);
        }
    }
}
