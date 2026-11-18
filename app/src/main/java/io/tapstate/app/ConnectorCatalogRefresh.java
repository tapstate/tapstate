package io.tapstate.app;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;

import static io.tapstate.app.ConnectorReleaseCatalog.Download;

/** The local support and remote downloads visible to one running service. */
final class ConnectorCatalogRefresh {
    record Catalog(ConnectorSupportManifest support, Map<String, Download> downloads, Instant refreshedAt) {
    }

    private final ConnectorSupportSnapshot snapshot;
    private final URI supportUri;
    private final URI repositoriesUri;
    private final String engineVersion;
    private final String hostApiVersion;
    private final ConnectorReleaseCatalog.Fetch fetch;
    private final Clock clock;
    private volatile Catalog catalog;

    ConnectorCatalogRefresh(ConnectorSupportSnapshot snapshot, String bundledSupport, URI supportUri,
                            URI repositoriesUri, String engineVersion, String hostApiVersion,
                            ConnectorReleaseCatalog.Fetch fetch, Clock clock) throws IOException {
        this.snapshot = snapshot;
        this.supportUri = supportUri;
        this.repositoriesUri = repositoriesUri;
        this.engineVersion = engineVersion;
        this.hostApiVersion = hostApiVersion;
        this.fetch = fetch;
        this.clock = clock;
        this.catalog = new Catalog(snapshot.restore(bundledSupport), Map.of(), null);
    }

    Catalog current() {
        return catalog;
    }

    synchronized Catalog refresh() throws IOException, InterruptedException {
        Catalog original = catalog;
        String json = new String(fetch.read(supportUri), StandardCharsets.UTF_8);
        var incoming = ConnectorSupportManifest.read(json);
        boolean newer = incoming.version() > original.support().version();
        var support = newer ? incoming : original.support();
        var repositories = ConnectorReleaseCatalog.repositories(
                new String(fetch.read(repositoriesUri), StandardCharsets.UTF_8));
        var downloads = ConnectorReleaseCatalog.resolve(
                support, repositories, original.downloads(), engineVersion, hostApiVersion, fetch);
        if (newer) snapshot.replace(json);
        catalog = new Catalog(support, downloads, clock.instant());
        return catalog;
    }
}
