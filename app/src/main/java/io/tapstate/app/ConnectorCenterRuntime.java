package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.adapters.pdk.PdkApiLevels;
import io.tapstate.control.core.ConnectorCatalogError;
import io.tapstate.control.core.ConnectorCenter;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorArtifactMetadata;
import io.tapstate.spi.store.ConnectorArtifactMetadata.Origin;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;

/** Connects the catalog and artifact installer to authenticated operations. */
final class ConnectorCenterRuntime implements ConnectorCenter, AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ConnectorCenterRuntime.class);
    private static final int MAX_ACTIVE = 4;
    private static final int MAX_OPERATIONS = 128;
    private final ConnectorCatalogBackground remote;
    private final LocalConnectorArtifacts local;
    private final boolean cloud;
    private final ConnectorCatalogHttp http = new ConnectorCatalogHttp();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, Operation> operations = new LinkedHashMap<>();
    private final Map<String, String> localStages = new ConcurrentHashMap<>();

    ConnectorCenterRuntime(ConnectorCatalogBackground remote, LocalConnectorArtifacts local, boolean cloud) {
        this.remote = remote;
        this.local = local;
        this.cloud = cloud;
    }

    @Override
    public Map<String, Object> catalog() {
        var catalog = remote.current();
        boolean downloadAvailable = remote.reachable();
        var support = catalog.support().entries();
        var inventory = local.inventory();
        var installed = inventory.versions();
        var defaults = inventory.defaults().stream().collect(Collectors.toMap(artifact -> artifact.catalog().id(),
                artifact -> artifact));
        localStages.keySet().retainAll(defaults.values().stream()
                .map(ConnectorArtifactMetadata::sha256).collect(Collectors.toSet()));
        var ids = new TreeSet<String>(remote.admittedIds(catalog.support()));
        installed.forEach(artifact -> ids.add(artifact.catalog().id()));
        ids.remove("dummy");
        var rows = new ArrayList<Map<String, Object>>();
        for (String id : ids) {
            var versions = installed.stream().filter(artifact -> artifact.catalog().id().equals(id))
                    .map(ConnectorCenterRuntime::version).toList();
            var item = support.get(id);
            var download = catalog.downloads().get(id);
            var current = defaults.get(id);
            var row = new LinkedHashMap<String, Object>();
            row.put("id", id);
            row.put("name", item == null ? id : item.name());
            if (item != null) row.put("roles", item.roles());
            row.put("installedVersions", versions);
            row.put("downloadable", download != null && downloadAvailable);
            if (current != null) {
                row.put("defaultSha256", current.sha256());
                if (current.catalog().icon() != null) row.put("icon", current.catalog().icon());
            }
            row.put("updateAvailable", current != null && download != null
                    && PdkApiLevels.compareArtifactVersions(download.release().coordinate().version(), current.version()) > 0);
            String stage = current == null
                    ? releaseStage(catalog.authentication().get(id))
                    : localStages.computeIfAbsent(current.sha256(), this::readLocalStage);
            row.put("properties", Map.of("authentication", stage == null ? "Alpha" : stage));
            if (download != null) {
                var target = download.release().coordinate();
                row.put("downloadVersion", target.version());
                row.put("downloadHash", download.sha256());
            }
            rows.add(row);
        }
        var view = new LinkedHashMap<String, Object>();
        view.put("connectors", rows);
        view.put("downloadCenterAvailable", downloadAvailable);
        view.put("uploadAvailable", !cloud);
        view.put("supportVersion", catalog.support().version());
        if (catalog.refreshedAt() != null) view.put("refreshedAt", catalog.refreshedAt().toString());
        return view;
    }

    private String readLocalStage(String sha256) {
        try {
            return releaseStage(local.manifestAuthentication(sha256));
        } catch (IOException unreadable) {
            LOG.debug("Could not read connector manifest for {}", sha256, unreadable);
            return null;
        }
    }

    static String releaseStage(String authentication) {
        return switch (authentication == null ? "" : authentication.strip().toLowerCase(Locale.ROOT)) {
            case "ga" -> "GA";
            case "beta" -> "Beta";
            default -> "Alpha";
        };
    }

    private static Map<String, Object> version(ConnectorArtifactMetadata artifact) {
        return Map.of("version", artifact.version(), "sha256", artifact.sha256(), "sizeBytes", artifact.sizeBytes(),
                "pdkApiVersion", artifact.pdkApiVersion(), "origin", artifact.origin().name());
    }

    @Override
    public Map<String, Object> refresh() {
        remote.refreshNow();
        return catalog();
    }

    @Override
    public Map<String, Object> download(String id, String expectedSha256) {
        if (id.equals("dummy")) throw notFound(id);
        var download = remote.current().downloads().get(id);
        if (expectedSha256 != null && (download == null || !expectedSha256.equals(download.sha256()))) {
            throw new TapstateException(ConnectorCatalogError.DOWNLOAD_TARGET_CHANGED, Map.of("connector", id), null);
        }
        if (download == null) throw notFound(id);
        if (!remote.reachable()) throw unavailable();
        var operation = begin(id, -1);
        workers.submit(() -> {
            try (var response = http.transfer(download.jar())) {
                operation.total = response.length();
                try (var staged = local.stage(response.body(), amount -> operation.bytes = amount)) {
                    install(operation, staged, Origin.DOWNLOAD, id, download.sha256());
                }
            } catch (IOException failure) {
                operation.fail(ControlError.UNREACHABLE.code());
            } catch (TapstateException failure) {
                operation.fail(failure.code().code());
            } catch (RuntimeException bug) {
                operation.fail(ControlError.UNREACHABLE.code());
                LOG.error("Connector download operation failed", bug);
            }
        });
        return operation.view();
    }

    @Override
    public Map<String, Object> upload(InputStream artifact, long size, String expectedConnectorId) {
        if (cloud) throw new TapstateException(ConnectorError.CLOUD_UPLOAD_DISABLED, Map.of(), null);
        var operation = begin(null, size);
        LocalConnectorArtifacts.StagedArtifact staged;
        try {
            staged = local.stage(artifact, amount -> operation.bytes = amount);
        } catch (TapstateException failed) {
            operation.fail(failed.code().code());
            return operation.view();
        }
        operation.phase = "INSTALLING";
        try {
            workers.submit(() -> {
                try (staged) {
                    install(operation, staged, Origin.UPLOAD, expectedConnectorId, null);
                } catch (TapstateException failure) {
                    operation.fail(failure.code().code());
                } catch (RuntimeException bug) {
                    operation.fail(ControlError.UNREACHABLE.code());
                    LOG.error("Connector upload operation failed", bug);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException closing) {
            staged.close();
            operation.fail(ControlError.UNREACHABLE.code());
        }
        return operation.view();
    }

    private static void install(Operation operation, LocalConnectorArtifacts.StagedArtifact staged,
                                Origin origin, String id, String hash) {
        operation.phase = "INSTALLING";
        var outcome = staged.install(origin, id, hash);
        var registered = outcome.registration();
        operation.pdkApiVersion = registered.pdkApiVersion();
        operation.newlyRegistered = outcome.newlyRegistered();
        operation.connector = registered.connectorId();
        operation.hash = registered.contentHash();
        operation.phase = "INSTALLED";
    }

    private synchronized Operation begin(String id, long size) {
        if (operations.values().stream().filter(op -> !op.terminal()).count() >= MAX_ACTIVE) throw unavailable();
        while (operations.size() >= MAX_OPERATIONS) {
            String oldest = operations.values().stream().filter(Operation::terminal).findFirst().orElseThrow().id;
            operations.remove(oldest);
        }
        var operation = new Operation(id, size);
        operations.put(operation.id, operation);
        return operation;
    }

    @Override
    public synchronized Map<String, Object> operation(String id) {
        var result = operations.get(id);
        if (result == null) throw notFound(id);
        return result.view();
    }

    private static TapstateException notFound(String id) {
        return new TapstateException(ConnectorCatalogError.NOT_FOUND, Map.of("connector", id), null);
    }

    private static TapstateException unavailable() {
        return new TapstateException(ControlError.UNREACHABLE, Map.of(), null);
    }

    @Override
    public void close() {
        workers.shutdownNow();
    }

    private static final class Operation {
        private final String id = UUID.randomUUID().toString();
        private volatile String phase = "TRANSFERRING";
        private volatile long bytes;
        private volatile long total;
        private volatile String connector;
        private volatile String hash;
        private volatile String error;
        private volatile String pdkApiVersion;
        private volatile boolean newlyRegistered;

        Operation(String connector, long total) {
            this.connector = connector;
            this.total = total;
        }

        boolean terminal() {
            return phase.equals("INSTALLED") || phase.equals("FAILED");
        }

        void fail(String code) {
            error = code;
            phase = "FAILED";
        }

        Map<String, Object> view() {
            var status = new LinkedHashMap<String, Object>();
            status.put("operationId", id);
            status.put("state", phase);
            status.put("bytesTransferred", bytes);
            if (total >= 0) status.put("totalBytes", total);
            if (connector != null) status.put("connectorId", connector);
            if (hash != null) status.put("sha256", hash);
            if (error != null) status.put("errorCode", error);
            if (pdkApiVersion != null) status.put("pdkApiVersion", pdkApiVersion);
            if ("INSTALLED".equals(phase)) status.put("newlyRegistered", newlyRegistered);
            return status;
        }
    }
}
