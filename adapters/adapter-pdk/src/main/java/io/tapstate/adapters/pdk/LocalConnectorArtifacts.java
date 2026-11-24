package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorArtifactHoldings;
import io.tapstate.spi.store.ConnectorArtifactMetadata;
import io.tapstate.spi.store.ConnectorArtifactMetadata.Origin;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistrar;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.RequiredNoteOrigin;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.JarFile;

import static io.tapstate.adapters.pdk.ConnectorArtifactRegistrar.*;

/** Verified local installation and exact-artifact loading for a single member. */
public final class LocalConnectorArtifacts implements ConnectorProvisioner, ConnectorRegistrar {
    private record Installed(ConnectorArtifactMetadata metadata, Path file) {
        ConnectorRegistration registration() {
            return new ConnectorRegistration(metadata.catalog().id(), metadata.sha256(), metadata.pdkApiVersion(),
                    metadata.origin() == Origin.BUNDLE ? RegistrationSource.SEED : RegistrationSource.REGISTER);
        }
    }

    private static final Comparator<Installed> RELEASE_ORDER = Comparator
            .comparing((Installed value) -> value.metadata().version(), PdkApiLevels::compareArtifactVersions)
            .thenComparing(value -> value.metadata().sha256());

    private final Path directory;
    private final ConnectorInstallRecords records;
    private final ConnectorIntrospector introspector;
    private final PdkCapabilityDeriver deriver;
    private final ConnectorCatalogStore catalog;
    private final ConnectorSpecStore specs;
    private final ConnectorArtifactHoldings holdings;
    private final String nodeId;
    @FunctionalInterface
    public interface LegacyArtifact {
        boolean copyTo(String hash, java.io.OutputStream output) throws IOException;
    }
    private final LegacyArtifact legacy;
    private final Supplier<List<String>> admission;
    private final String hostApi;
    private final Map<String, Installed> installed = new ConcurrentHashMap<>();
    private final Map<String, Path> bundled = new java.util.HashMap<>();

    public LocalConnectorArtifacts(Path directory, ConnectorIntrospector introspector, ConnectorCatalogStore catalog,
                                   ConnectorSpecStore specs, ConnectorArtifactHoldings holdings, String nodeId,
                                   LegacyArtifact legacy, Supplier<List<String>> admission,
                                   String hostApi) {
        this.directory = directory;
        this.records = new ConnectorInstallRecords(directory);
        this.introspector = introspector;
        this.deriver = new PdkCapabilityDeriver(this);
        this.catalog = catalog;
        this.specs = specs;
        this.holdings = holdings;
        this.nodeId = nodeId;
        this.legacy = legacy;
        this.admission = admission;
        this.hostApi = hostApi;
    }

    @Override
    public RegistrationOutcome register(byte[] artifact, RegistrationSource source) {
        return install(new ByteArrayInputStream(artifact), source == RegistrationSource.SEED ? Origin.DROP_DIRECTORY : Origin.UPLOAD,
                null, ContentHash.of(artifact));
    }

    public RegistrationOutcome install(InputStream input, Origin origin, String expectedId, String expectedHash) {
        try (var staged = stage(input, amount -> {})) {
            return staged.install(origin, expectedId, expectedHash);
        }
    }

    public StagedArtifact stage(InputStream input, java.util.function.LongConsumer transferred) {
        try {
            Files.createDirectories(directory);
            Path staged = Files.createTempFile(directory, ".connector-", ".jar");
            boolean complete = false;
            try {
                try (var file = FileChannel.open(staged, StandardOpenOption.WRITE);
                        var output = Channels.newOutputStream(file)) {
                    byte[] buffer = new byte[128 * 1024];
                    long total = 0;
                    for (int read; (read = input.read(buffer)) >= 0;) {
                        total += read;
                        if (total > 256L * 1024 * 1024) throw artifactUnreadable(staged);
                        output.write(buffer, 0, read);
                        transferred.accept(total);
                    }
                    file.force(true);
                }
                complete = true;
                return new StagedArtifact(staged);
            } finally {
                if (!complete) Files.deleteIfExists(staged);
            }
        } catch (IOException error) {
            throw loadFailure("upload");
        }
    }

    public final class StagedArtifact implements AutoCloseable {
        private final Path file;

        private StagedArtifact(Path file) { this.file = file; }

        public RegistrationOutcome install(Origin origin, String expectedId, String expectedHash) {
            try {
                return installStaged(file, origin, expectedId, expectedHash);
            } catch (IOException error) {
                throw loadFailure(expectedId == null ? "upload" : expectedId);
            }
        }

        @Override public void close() {
            discard(file);
        }
    }

    private static void discard(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException failure) {
            LOG.warn("Could not delete a temporary connector artifact");
        }
    }

    public boolean admits(String connectorId) {
        return admission.get().contains(connectorId);
    }

    public RegistrationOutcome installDrop(Path artifact) {
        try (var input = Files.newInputStream(artifact)) {
            var result = install(input, Origin.DROP_DIRECTORY, null, null);
            Files.delete(artifact);
            return result;
        } catch (IOException error) {
            throw artifactUnreadable(artifact);
        }
    }

    public synchronized RegistrationOutcome registerBundle(Path file, String id, String sha256, long sizeBytes) {
        try {
            if (!Files.isRegularFile(file) || Files.size(file) != sizeBytes) throw loadFailure(id);
            var current = reusable(id, sha256, sizeBytes);
            boolean newlyRegistered = current.isEmpty();
            if (newlyRegistered) {
                verifyHash(file, id, sha256);
                var inspected = inspect(file, Origin.BUNDLE, id, false);
                current = Optional.of(publish(file, inspected, sha256, sizeBytes));
            }
            Installed selected = new Installed(current.orElseThrow(), file);
            Installed previous = installed.putIfAbsent(sha256, selected);
            bundled.put(sha256, file);
            return new RegistrationOutcome(selected.registration(), previous == null && newlyRegistered);
        } catch (IOException error) {
            throw loadFailure(id);
        }
    }

    public synchronized void restore() throws IOException {
        if (Files.isDirectory(directory)) {
            try (var paths = Files.list(directory)) {
                for (Path file : paths.filter(p -> p.getFileName().toString().matches("[0-9a-f]{64}\\.jar")).toList()) {
                    String hash = file.getFileName().toString().substring(0, 64);
                    try {
                        restore(file, hash);
                    } catch (TapstateException unreadable) {
                        LOG.warn("Local connector artifact {} is unavailable ({})", hash, unreadable.code().code());
                    }
                }
            }
        }
        for (var held : holdings.forNode(nodeId)) {
            if (!installed.containsKey(held.sha256())) holdings.withdraw(nodeId, held.sha256());
        }
    }

    private void restore(Path file, String hash) throws IOException {
        Optional<ConnectorInstallRecords.Installation> known = readRecord(hash);
        var previous = known.orElse(null);
        if (!Files.isRegularFile(file) || previous != null && Files.size(file) != previous.sizeBytes()) {
            throw loadFailure(previous == null ? hash : previous.connectorId());
        }
        if (previous != null) {
            var kept = reusable(previous.connectorId(), hash, previous.sizeBytes());
            if (kept.isPresent()) {
                if (previous.state() != ConnectorInstallRecords.State.COMPLETE) records.save(hash, previous.complete());
                installed.put(hash, new Installed(kept.get(), file));
                return;
            }
        }
        installStaged(file, previous == null ? Origin.DROP_DIRECTORY : previous.origin(),
                previous == null ? null : previous.connectorId(), hash);
    }

    private synchronized RegistrationOutcome installStaged(Path staged, Origin origin, String expectedId,
                                                           String expectedHash) throws IOException {
        String hash = hashOf(staged);
        if (expectedHash != null && !hash.equals(expectedHash)) throw loadFailure(expectedId == null ? hash : expectedId);
        var prior = installed.get(hash);
        if (prior != null && prior.file().equals(localFile(hash))) {
            if (expectedId != null && !expectedId.equals(prior.registration().connectorId())) throw loadFailure(expectedId);
            if (Files.isRegularFile(prior.file()) && hash.equals(hashOf(prior.file()))) {
                return new RegistrationOutcome(prior.registration(), false);
            }
        }
        return finish(staged, origin, expectedId, hash, prior);
    }

    private RegistrationOutcome finish(Path staged, Origin origin, String expectedId, String hash,
                                       Installed previous) throws IOException {
        var existing = readRecord(hash).orElse(null);
        var inspected = inspect(staged, origin, expectedId, existing == null
                && origin != Origin.LEGACY_GRIDFS && origin != Origin.BUNDLE && origin != Origin.DOWNLOAD);
        if (existing != null && (!existing.connectorId().equals(inspected.id())
                || !existing.version().equals(inspected.version()))) throw loadFailure(inspected.id());
        long size = Files.size(staged);
        var record = new ConnectorInstallRecords.Installation(inspected.id(), inspected.version(), inspected.origin(),
                size, ConnectorInstallRecords.State.PENDING);
        Path destination = localFile(hash);
        records.save(hash, record);
        if (!staged.equals(destination)
                && (!Files.isRegularFile(destination) || !hash.equals(hashOf(destination)))) {
            Files.move(staged, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        var metadata = reusable(inspected.id(), hash, size).orElse(null);
        if (metadata == null) metadata = publish(destination, inspected, hash, size);
        records.save(hash, record.complete());
        Installed selected = new Installed(metadata, previous != null && previous.metadata().origin() == Origin.BUNDLE
                ? previous.file() : destination);
        installed.put(hash, selected);
        return new RegistrationOutcome(selected.registration(), previous == null);
    }

    private Optional<ConnectorInstallRecords.Installation> readRecord(String hash) {
        try {
            return records.read(hash);
        } catch (IOException invalid) {
            LOG.warn("Connector installation record {} is unreadable. Validating the original JAR.", hash);
            return Optional.empty();
        }
    }

    private Optional<ConnectorArtifactMetadata> reusable(String id, String hash, long size) {
        return catalog.artifact(id, hash)
                .filter(value -> value.sizeBytes() == size && PdkApiLevels.allows(hostApi, value.pdkApiVersion()))
                .filter(value -> specs.has(value.catalog().provenance().specContentHash()))
                .filter(value -> holdings.forNode(nodeId).contains(new ConnectorArtifactHoldings.Held(hash, id, value.version())));
    }

    private record Inspection(String id, String version, Origin origin, IntrospectedConnector details, Object spec) {}

    private Inspection inspect(Path file, Origin origin, String expectedId, boolean checkAdmission) throws IOException {
        IntrospectedConnector details = introspector.introspect(List.of(file));
        Object spec = parseSpecTree(details, file);
        String id = declaredId(spec, details, file);
        if (expectedId != null && !expectedId.equals(id)) throw loadFailure(expectedId);
        if (checkAdmission) {
            List<String> supported = admission.get();
            if (!supported.contains(id)) throw new TapstateException(ConnectorError.NOT_OFFICIAL,
                    Map.of("connector", id, "official", String.join(", ", supported)), null);
        }
        if (!PdkApiLevels.allows(hostApi, details.pdkApiVersion())) {
            throw new TapstateException(ConnectorError.API_LEVEL_UNKNOWN,
                    Map.of("connector", id, "version", String.valueOf(details.pdkApiVersion())), null);
        }
        try (var jar = new JarFile(file.toFile())) {
            var manifest = jar.getManifest();
            String version = manifest == null ? null : manifest.getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VERSION);
            if (version == null || version.isBlank()) throw artifactUnreadable(file);
            return new Inspection(id, version, origin, details, spec);
        }
    }

    private ConnectorArtifactMetadata publish(Path file, Inspection inspection, String hash, long size) {
        String id = inspection.id();
        var details = inspection.details();
        byte[] rawSpec = details.spec().getBytes(StandardCharsets.UTF_8);
        String specHash = ContentHash.of(rawSpec);
        var entry = catalogRow(details, inspection.spec(), specHash, deriver.derive(id, reference(file, hash, details)));
        if (!specs.has(specHash)) specs.put(specHash, rawSpec);
        var metadata = new ConnectorArtifactMetadata(entry, hash, inspection.version(), details.pdkApiVersion(),
                inspection.origin(), size, bundledIcon(file, entry.icon()));
        catalog.upsertArtifact(metadata);
        holdings.publish(nodeId, new ConnectorArtifactHoldings.Held(hash, id, inspection.version()));
        return metadata;
    }

    @Override
    public synchronized ConnectorRef resolve(String connectorId) {
        return load(selected(connectorId).orElseThrow(() -> notRegistered(connectorId)));
    }

    @Override
    public synchronized ConnectorRef resolve(String connectorId, String hash) {
        Installed value = installed.get(hash);
        if (value == null || !value.registration().connectorId().equals(connectorId)) throw localUnavailable(connectorId, hash);
        return load(value);
    }

    @Override
    public synchronized ConnectorRef resolveRetained(RequiredNoteStore notes, RequiredNoteScope scope, RequiredNoteOrigin origin) {
        boolean ready = notes.localArtifactReady(scope, origin);
        String id = origin.connectorId();
        String hash = origin.artifactHash();
        if (!ready) {
            ensureOriginalInstalled(id, hash);
            var reference = resolve(id, hash);
            retain(reference);
            notes.markLocalArtifactReady(scope, origin);
            return reference;
        }
        var value = installed.get(hash);
        if (value == null || !value.registration().connectorId().equals(id)) throw localUnavailable(id, hash);
        try {
            if (!value.file().equals(localFile(hash))) verifyHash(localFile(hash), id, hash);
            return load(value);
        } catch (TapstateException unreadable) {
            throw localUnavailable(id, hash);
        }
    }

    private synchronized void ensureOriginalInstalled(String id, String hash) {
        if (installed.containsKey(hash)) return;
        Path original = localFile(hash);
        try {
            if (Files.isRegularFile(original)) {
                installStaged(original, Origin.LEGACY_GRIDFS, id, hash);
                return;
            }
            Files.createDirectories(directory);
            Path staged = Files.createTempFile(directory, ".legacy-connector-", ".jar");
            try {
                try (var file = FileChannel.open(staged, StandardOpenOption.WRITE)) {
                    if (!legacy.copyTo(hash, Channels.newOutputStream(file))) throw loadFailure(id);
                    file.force(true);
                }
                installStaged(staged, Origin.LEGACY_GRIDFS, id, hash);
            } finally {
                Files.deleteIfExists(staged);
            }
        } catch (IOException error) {
            throw loadFailure(id);
        }
    }

    @Override
    public synchronized <T> T withArtifactRetention(Supplier<T> publication) {
        return publication.get();
    }

    /**
     * Retires idle loaders independently of disk retention. Old bytes remain protected by exact durable
     * references. The same monitor covers local opens, installation and the publication of new references.
     */
    public synchronized void reclaim(Set<String> eligibleIds, Supplier<Set<String>> retainedArtifacts) throws IOException {
        for (var entry : installed.values()) {
            if (eligibleIds.contains(entry.registration().connectorId())) {
                ConnectorClassLoader.retireUnused(entry.file());
            }
        }

        Set<String> retained = null;
        for (String hash : records.hashes()) {
            var record = readRecord(hash).orElse(null);
            if (record == null || record.state() != ConnectorInstallRecords.State.COMPLETE
                    || !eligibleIds.contains(record.connectorId())) continue;
            var latest = selected(record.connectorId()).orElse(null);
            if (latest == null || latest.metadata().sha256().equals(hash)) continue;

            Path file = localFile(hash);
            Path bundle = bundled.get(hash);
            if (!ConnectorClassLoader.retireUnused(file)
                    || bundle != null && !ConnectorClassLoader.retireUnused(bundle)) continue;
            if (retained == null) retained = retainedArtifacts.get();
            if (retained.contains(hash)) continue;
            reclaim(hash, record, file, bundle);
        }
    }

    private void reclaim(String hash, ConnectorInstallRecords.Installation record, Path file, Path bundle)
            throws IOException {
        boolean bundledCopy = bundle != null && Files.isRegularFile(bundle);
        if (!bundledCopy) holdings.withdraw(nodeId, hash);
        try {
            Files.deleteIfExists(file);
        } catch (IOException failure) {
            if (!bundledCopy && Files.isRegularFile(file)) {
                try {
                    holdings.publish(nodeId, new ConnectorArtifactHoldings.Held(hash, record.connectorId(), record.version()));
                } catch (RuntimeException restore) {
                    failure.addSuppressed(restore);
                }
            }
            throw failure;
        }
        if (bundledCopy) installed.computeIfPresent(hash, (key, old) -> new Installed(old.metadata(), bundle));
        else installed.remove(hash);
        records.remove(hash);
    }

    @Override
    public synchronized void retain(ConnectorRef ref) {
        String hash = ref.contentHash();
        var value = installed.get(hash);
        if (value == null) throw loadFailure(hash);
        String id = value.registration().connectorId();
        Path retained = localFile(hash);
        if (Files.isRegularFile(retained) && hash.equals(hashOf(retained)) && readRecord(hash)
                .filter(record -> record.state() == ConnectorInstallRecords.State.COMPLETE).isPresent()) return;
        try (var input = Files.newInputStream(value.file())) {
            install(input, value.metadata().origin(), id, hash);
        } catch (IOException error) {
            throw loadFailure(id);
        }
    }

    @Override
    public void markBound(RequiredNoteStore notes, RequiredNoteScope scope, RequiredNoteOrigin origin) {
        notes.markLocalArtifactReady(scope, origin);
    }

    @Override
    public Optional<String> artifactVersion(String connectorId, String sha256) {
        return Optional.ofNullable(installed.get(sha256))
                .filter(value -> value.metadata().catalog().id().equals(connectorId))
                .map(value -> value.metadata().version());
    }

    public synchronized List<ConnectorArtifactMetadata> catalog() {
        return list().stream().map(row -> installed.get(row.contentHash()).metadata()).toList();
    }

    public List<ConnectorArtifactMetadata> versions() {
        return installed.values().stream().filter(v -> Files.isRegularFile(v.file()))
                .sorted(RELEASE_ORDER.reversed()).map(Installed::metadata).toList();
    }

    public record Inventory(List<ConnectorArtifactMetadata> versions, List<ConnectorArtifactMetadata> defaults) { }

    public synchronized Inventory inventory() {
        return new Inventory(versions(), catalog());
    }

    public synchronized String manifestAuthentication(String sha256) throws IOException {
        var artifact = installed.get(sha256);
        if (artifact == null) return null;
        try (var jar = new JarFile(artifact.file().toFile())) {
            var manifest = jar.getManifest();
            return manifest == null ? null : manifest.getMainAttributes().getValue("Authentication");
        }
    }

    private Optional<Installed> selected(String id) {
        return installed.values().stream().filter(value -> value.registration().connectorId().equals(id))
                .filter(value -> Files.isRegularFile(value.file()))
                .filter(value -> PdkApiLevels.allows(hostApi, value.metadata().pdkApiVersion())).max(RELEASE_ORDER);
    }

    private List<ConnectorRegistration> list() {
        return installed.values().stream().map(value -> value.registration().connectorId()).distinct().sorted()
                .flatMap(id -> selected(id).stream()).map(Installed::registration).toList();
    }

    public List<ConnectorRegistration> findAll(String id) {
        return selected(id).map(Installed::registration).stream().toList();
    }

    private boolean hasArtifact(String hash) {
        var value = installed.get(hash);
        return value != null && Files.isRegularFile(value.file());
    }

    public ConnectorRegistry registryView() {
        return new ConnectorRegistry() {
            @Override public List<ConnectorRegistration> list() { return LocalConnectorArtifacts.this.list(); }
            @Override public List<ConnectorRegistration> findAll(String id) { return LocalConnectorArtifacts.this.findAll(id); }
            @Override public boolean hasArtifact(String hash) { return LocalConnectorArtifacts.this.hasArtifact(hash); }
            @Override public Optional<byte[]> artifact(String hash) {
                throw new UnsupportedOperationException("Installed artifacts are loaded directly from disk");
            }
            @Override public RegistrationOutcome register(String id, String version, RegistrationSource source, byte[] bytes) {
                throw new UnsupportedOperationException("Use verified local installation");
            }
        };
    }

    @Override
    public String cacheIdentity(String id) {
        return selected(id).orElseThrow(() -> notRegistered(id)).metadata().sha256();
    }

    private Path localFile(String hash) {
        return directory.resolve(hash + ".jar");
    }

    private ConnectorRef load(Installed value) {
        String id = value.registration().connectorId();
        String hash = value.metadata().sha256();
        verifyHash(value.file(), id, hash);
        return reference(value.file(), hash, introspector.introspect(List.of(value.file())));
    }

    private ConnectorRef reference(Path file, String hash, IntrospectedConnector details) {
        return new ConnectorRef(List.of(file), details.className(), details.pdkApiVersion(), null, details.spec(), hash,
                false, this::acquireLoader);
    }

    private synchronized ConnectorClassLoader.SharedUse acquireLoader(List<Path> classpath) {
        if (!Files.isRegularFile(classpath.getFirst())) throw loadFailure(classpath.getFirst().getFileName().toString());
        return ConnectorClassLoader.acquire(classpath);
    }

    private static void verifyHash(Path file, String id, String sha256) {
        if (!Files.isRegularFile(file) || !sha256.equals(hashOf(file))) throw loadFailure(id);
    }

    private static TapstateException loadFailure(String id) {
        return new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", id), null);
    }

    private static TapstateException localUnavailable(String id, String hash) {
        return new TapstateException(ConnectorError.LOCAL_ARTIFACT_UNAVAILABLE, Map.of("connector", id, "hash", hash), null);
    }

    private static TapstateException notRegistered(String id) {
        return new TapstateException(ConnectorError.NOT_REGISTERED, Map.of("connector", id), null);
    }

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(LocalConnectorArtifacts.class);
}
