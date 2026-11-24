package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.BundledConnectorIcon;
import io.tapstate.spi.store.ConnectorArtifactMetadata;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ContentHash;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.annotation.AnnotationFormatError;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipException;

/** Owns incremental registration of the immutable image and the on-premises drop directory. */
final class LocalConnectorRuntime implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(LocalConnectorRuntime.class);
    private final LocalConnectorArtifacts local;
    private final Path seed;
    private final Path drop;
    private final Maintenance maintenance;
    private final Map<Path, RejectedDrop> rejectedDrops = new HashMap<>();
    private Thread worker;

    @FunctionalInterface
    interface Maintenance {
        void run() throws IOException;
    }

    LocalConnectorRuntime(LocalConnectorArtifacts local, Path seed, Path drop, Maintenance maintenance) {
        this.local = local;
        this.seed = seed;
        this.drop = drop;
        this.maintenance = maintenance;
    }

    LocalConnectorArtifacts artifacts() {
        return local;
    }

    List<SeedOutcome> initialize(CloudConnectorSeedReadiness readiness) {
        try {
            List<SeedOutcome> results = new ArrayList<>();
            var manifest = bundleManifest(seed);
            if (Files.isRegularFile(manifest)) {
                for (Object value : bundleEntries(manifest)) {
                    Map<?, ?> entry = (Map<?, ?>) value;
                    String id = (String) entry.get("id");
                    Path file = seed.resolve(id + "-connector.jar");
                    var outcome = local.registerBundle(file, id, (String) entry.get("sha256"), (long) entry.get("bytes"));
                    results.add(new SeedOutcome.Seeded(file, outcome));
                }
            } else if (readiness != null) {
                for (String name : readiness.selectedFileNames()) {
                    Path file = seed.resolve(name);
                    String id = name.substring(0, name.length() - "-connector.jar".length());
                    try (var bytes = Files.newInputStream(file)) {
                        results.add(new SeedOutcome.Seeded(file,
                                local.registerBundle(file, id, ContentHash.of(bytes), Files.size(file))));
                    }
                }
            } else if (!seed.equals(drop)) {
                for (Path file : jars(seed)) {
                    try (var bytes = Files.newInputStream(file)) {
                        results.add(new SeedOutcome.Seeded(file, local.install(bytes,
                                ConnectorArtifactMetadata.Origin.DROP_DIRECTORY, null, null)));
                    } catch (io.tapstate.core.common.TapstateException rejected) {
                        results.add(new SeedOutcome.Failed(file, rejected));
                    }
                }
            }
            local.restore();
            if (drop != null) pollDrops();
            worker = Thread.ofVirtual().name("connector-artifact-maintenance").start(this::maintain);
            return List.copyOf(results);
        } catch (IOException unavailable) {
            if (readiness != null) throw CloudConnectorSeedReadiness.sweepUnavailable();
            throw new UncheckedIOException(unavailable);
        }
    }

    static Set<String> bundledFileNames(Path seed) {
        var manifest = bundleManifest(seed);
        if (!Files.isRegularFile(manifest)) return null;
        try {
            return bundleEntries(manifest).stream()
                    .map(value -> ((Map<?, ?>) value).get("id") + "-connector.jar")
                    .collect(Collectors.toUnmodifiableSet());
        } catch (IOException unavailable) {
            throw new UncheckedIOException(unavailable);
        }
    }

    private static Path bundleManifest(Path seed) {
        return seed.toAbsolutePath().normalize().resolveSibling("release").resolve("connectors.lock.json");
    }

    private static List<?> bundleEntries(Path manifest) throws IOException {
        try {
            var payload = (Map<?, ?>) JsonReader.parse(Files.readString(manifest));
            if (!(payload.get("connectors") instanceof List<?> records)) throw new IOException("Invalid release manifest");
            return records;
        } catch (IllegalArgumentException | ClassCastException invalid) {
            throw new IOException("Invalid release manifest", invalid);
        }
    }

    private static List<Path> jars(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) return List.of();
        try (var paths = Files.list(directory)) {
            return paths.filter(file -> Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)
                    && file.getFileName().toString().endsWith(".jar")).sorted().toList();
        }
    }

    private volatile boolean closed;

    private void maintain() {
        while (!closed) {
            try {
                Thread.sleep(10_000);
                if (closed) return;
                if (drop != null) pollDrops();
                maintenance.run();
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException | UncheckedIOException unavailable) {
                LOG.warn("Could not complete local connector artifact maintenance", unavailable);
            } catch (io.tapstate.core.common.TapstateException refused) {
                LOG.warn("Local connector artifact maintenance deferred ({})", refused.code().code());
            }
        }
    }

    void pollDrops() throws IOException {
        var files = jars(drop);
        rejectedDrops.keySet().retainAll(Set.copyOf(files));
        for (Path file : files) {
            pollDrop(file);
        }
    }

    private void pollDrop(Path file) {
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException unavailable) {
            LOG.warn("Could not read dropped connector {}", file.getFileName(), unavailable);
            return;
        }
        if (!attributes.isRegularFile()) return;
        var stamp = new DropStamp(attributes.fileKey(), attributes.size(), attributes.lastModifiedTime());
        var rejected = rejectedDrops.get(file);
        if (rejected != null && rejected.stamp().equals(stamp)
                && (rejected.unlistedId() == null || !local.admits(rejected.unlistedId()))) return;
        rejectedDrops.remove(file);
        try {
            local.installDrop(file);
        } catch (TapstateException failure) {
            if (failure.code() == ConnectorError.NOT_OFFICIAL
                    && failure.args().get("connector") instanceof String id) {
                rejectedDrops.put(file, new RejectedDrop(stamp, id));
            } else if (invalidArtifact(failure)) {
                rejectedDrops.put(file, new RejectedDrop(stamp, null));
            }
            LOG.warn("Could not install dropped connector {} ({})", file.getFileName(), failure.code().code());
        }
    }

    private static boolean invalidArtifact(TapstateException failure) {
        return failure.code() instanceof ConnectorError code && switch (code) {
            case NO_CONNECTOR_CLASS, AMBIGUOUS_CONNECTOR_CLASS, API_LEVEL_UNKNOWN -> true;
            case ARTIFACT_UNREADABLE -> failure.getCause() instanceof ZipException;
            case LOAD_FAILED -> failure.getCause() instanceof LinkageError
                    || failure.getCause() instanceof AnnotationFormatError;
            default -> false;
        };
    }

    private record DropStamp(Object fileKey, long size, FileTime modified) {}

    private record RejectedDrop(DropStamp stamp, String unlistedId) {}

    ConnectorCatalogStore view(ConnectorCatalogStore underlying, ConnectorCatalogBackground support) {
        return new ConnectorCatalogStore() {
            @Override public void upsert(ConnectorCatalogEntry entry) { underlying.upsert(entry); }
            @Override public List<ConnectorCatalogEntry> list() {
                return local.catalog().stream().map(ConnectorArtifactMetadata::catalog)
                        .map(entry -> permitted(entry, support)).toList();
            }
            @Override public Optional<ConnectorCatalogEntry> get(String id) {
                return local.findAll(id).stream().findFirst()
                        .flatMap(row -> underlying.artifact(id, row.contentHash())).map(ConnectorArtifactMetadata::catalog)
                        .map(entry -> permitted(entry, support));
            }
            @Override public Optional<BundledConnectorIcon> bundledIcon(String id, String hash) {
                return underlying.artifact(id, hash).flatMap(ConnectorArtifactMetadata::icon);
            }
            @Override public Optional<ConnectorArtifactMetadata> artifact(String id, String hash) {
                return underlying.artifact(id, hash);
            }
        };
    }

    private static ConnectorCatalogEntry permitted(ConnectorCatalogEntry entry, ConnectorCatalogBackground support) {
        boolean source = support.permitsRole(entry.id(), "source");
        boolean target = support.permitsRole(entry.id(), "target");
        if (source && target) return entry;
        var noSink = new io.tapstate.core.catalog.SinkCapability(false, List.of());
        return new ConnectorCatalogEntry(entry.id(), entry.name(), entry.displayName(), entry.icon(), entry.group(),
                source ? entry.modes() : List.of(), entry.discovery(), target ? entry.sink() : noSink,
                target && entry.pushOut(), entry.config(), entry.provenance());
    }

    @Override public void close() {
        closed = true;
        if (worker != null) worker.interrupt();
    }
}
