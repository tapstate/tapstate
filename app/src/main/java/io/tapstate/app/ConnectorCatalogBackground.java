package io.tapstate.app;

import io.tapstate.control.restapi.RuntimeVersion;
import io.tapstate.core.common.JsonReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Owns the periodic catalog refresh separately from pipeline and capture scheduling. */
final class ConnectorCatalogBackground implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectorCatalogBackground.class);
    static final URI CONFIGURATION = URI.create(
            "https://raw.githubusercontent.com/tapstate/tapstate/connector-config/deploy/connectors/");
    private static final String SUPPORT = "supported-connectors.json";

    private final ConnectorCatalogHttp http = new ConnectorCatalogHttp();
    private final ConnectorCatalogRefresh catalog;
    private volatile boolean reachable;
    private final String engineVersion = RuntimeVersion.current();
    private final java.util.concurrent.ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("connector-catalog").factory());

    ConnectorCatalogBackground(Path dataDirectory, Clock clock) {
        try {
            catalog = new ConnectorCatalogRefresh(new ConnectorSupportSnapshot(dataDirectory), resource(SUPPORT),
                    CONFIGURATION.resolve(SUPPORT), CONFIGURATION.resolve("repositories.json"),
                    engineVersion, hostApiVersion(), http, clock);
        } catch (IOException invalid) {
            throw new IllegalStateException("The bundled connector support manifest is invalid", invalid);
        }
    }

    List<String> admittedIds() {
        var support = catalog.current().support();
        return support.entries().keySet().stream()
                .filter(id -> support.admitted(id, engineVersion).isPresent())
                .toList();
    }

    boolean permitsRole(String id, String role) {
        var entry = catalog.current().support().entries().get(id);
        return entry == null || entry.supportsRole(role);
    }

    void start() {
        long interval = Duration.ofMinutes(30).toSeconds();
        executor.scheduleWithFixedDelay(this::refreshNow, 0, interval, TimeUnit.SECONDS);
    }

    ConnectorCatalogRefresh.Catalog current() { return catalog.current(); }

    boolean reachable() { return reachable; }

    synchronized void refreshNow() {
        try {
            catalog.refresh();
            reachable = true;
        } catch (IOException unavailable) {
            reachable = current().downloads().values().stream().findFirst().map(previous -> {
                try {
                    return http.available(previous.jar());
                } catch (IOException offline) {
                    return false;
                }
            }).orElse(false);
            LOG.debug("Could not refresh the connector catalog. Keeping the last successful catalog.", unavailable);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    static String hostApiVersion() throws IOException {
        if (JsonReader.parse(resource("pdk-version.json")) instanceof Map<?, ?> manifest
                && manifest.get("pdkApiVersion") instanceof String version) {
            return version;
        }
        throw new IllegalStateException("The packaged host manifest has no PDK API version");
    }

    private static String resource(String resource) throws IOException {
        try (var stream = ConnectorCatalogBackground.class.getResourceAsStream("/META-INF/tapstate/" + resource)) {
            if (stream == null) throw new IllegalStateException("Missing packaged " + resource);
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
