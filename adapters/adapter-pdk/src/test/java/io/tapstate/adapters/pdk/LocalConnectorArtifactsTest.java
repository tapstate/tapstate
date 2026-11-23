package io.tapstate.adapters.pdk;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorArtifactHoldings;
import io.tapstate.spi.store.ConnectorArtifactMetadata;
import io.tapstate.spi.store.ConnectorArtifactMetadata.Origin;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.RequiredNoteOrigin;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.RequiredNoteScope;
import io.tapstate.spi.store.RequiredNoteStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalConnectorArtifactsTest {
    @TempDir
    Path work;

    @Test
    void distinctVersionsRemainAvailableByHashAfterAnOfflineCacheRebuild() throws IOException {
        var cache = new Cache();
        var installer = installer(cache, (hash, output) -> { throw new AssertionError("Unexpected GridFS"); });
        Path original = artifact("2.9.3");
        Path replacement = artifact("2.9.12");
        var old = install(installer, original);
        var next = install(installer, replacement);

        assertThat(installer.resolve("orders").contentHash()).isEqualTo(next);
        assertThat(installer.resolve("orders", old).contentHash()).isEqualTo(old);
        assertThat(cache.byHash).hasSize(2);

        var rebuilt = installer(new Cache(), (hash, output) -> { throw new AssertionError("Unexpected GridFS"); });
        rebuilt.restore();
        assertThat(rebuilt.versions()).extracting(ConnectorArtifactMetadata::sha256).containsExactly(next, old);
        assertThat(rebuilt.resolve("orders", old).contentHash()).isEqualTo(old);
    }

    @Test
    void aLegacyBindingMigratesOnceAndLosingThatLocalCopyCannotReadGridFsAgain() throws IOException {
        Path original = artifact("2.9.3");
        String hash = ContentHash.of(Files.readAllBytes(original));
        var cache = new Cache();
        var copies = new AtomicInteger();
        LocalConnectorArtifacts.LegacyArtifact legacy = (requested, output) -> {
            assertThat(requested).isEqualTo(hash);
            copies.incrementAndGet();
            Files.copy(original, output);
            return true;
        };
        var ready = new AtomicBoolean();
        var notes = (RequiredNoteStore) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{RequiredNoteStore.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "localArtifactReady" -> ready.get();
                    case "markLocalArtifactReady" -> {
                        assertThat(Files.exists(work.resolve("plugins").resolve(hash + ".jar"))).isTrue();
                        assertThat(cache.byHash).containsKey(hash);
                        assertThat(cache.forNode("node")).extracting(ConnectorArtifactHoldings.Held::sha256).contains(hash);
                        assertThat(new ConnectorInstallRecords(work.resolve("plugins")).read(hash))
                                .get().extracting(ConnectorInstallRecords.Installation::state).isEqualTo(ConnectorInstallRecords.State.COMPLETE);
                        ready.set(true);
                        yield null;
                    }
                    default -> throw new AssertionError(method.getName());
                });
        var owner = new RequiredNoteOwner(1, "pdk.chain.chain", RequiredNoteOwner.Kind.CHAIN, "chain", 0);
        var scope = new RequiredNoteScope(owner, 1);
        var origin = new RequiredNoteOrigin("orders", hash, new byte[]{1});
        var installer = installer(cache, legacy);

        assertThat(installer.resolveRetained(notes, scope, origin).contentHash()).isEqualTo(hash);
        assertThat(ready).isTrue();
        assertThat(copies).hasValue(1);

        var restarted = installer(cache, legacy);
        restarted.restore();
        assertThat(restarted.resolveRetained(notes, scope, origin).contentHash()).isEqualTo(hash);
        Files.delete(work.resolve("plugins").resolve(hash + ".jar"));
        assertThatThrownBy(() -> restarted.resolveRetained(notes, scope, origin)).isInstanceOfSatisfying(
                TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ConnectorError.LOCAL_ARTIFACT_UNAVAILABLE);
                    assertThat(failure.args()).containsEntry("hash", hash);
                });
        assertThat(copies).hasValue(1);
    }

    @Test
    void reclaimsOnlyAnEligibleUnreferencedOldInstallationAndReopensIdenticalBytes() throws Exception {
        var cache = new Cache();
        var runtime = installer(cache, (hash, output) -> { throw new AssertionError("Unexpected GridFS"); });
        Path original = artifact("2.9.3");
        String old = install(runtime, original);
        String latest = install(runtime, artifact("2.9.12"));
        var oldRef = runtime.resolve("orders", old);
        Path oldJar = work.resolve("plugins").resolve(old + ".jar");
        Path oldRecord = work.resolve("plugins").resolve(old + ".json");
        var eligible = Set.of("orders");

        ConnectorClassLoader originalLoader;
        try (var first = oldRef.acquireLoader(); var second = oldRef.acquireLoader()) {
            originalLoader = first.loader();
            first.close();
            runtime.reclaim(eligible, () -> Set.of());
            assertThat(oldJar).exists();
            assertThat(oldRecord).exists();
            assertThat(second.loader()).isSameAs(originalLoader);
        }

        runtime.reclaim(Set.of(), Set::of);
        assertThat(oldJar).exists();
        try (var check = oldRef.acquireLoader()) {
            assertThat(check.loader()).isSameAs(originalLoader);
        }

        runtime.reclaim(eligible, () -> Set.of(old));
        assertThat(oldJar).exists();
        assertThat(oldRecord).exists();
        try (var check = oldRef.acquireLoader()) {
            assertThat(check.loader()).isNotSameAs(originalLoader);
        }

        runtime.reclaim(eligible, Set::of);
        assertThat(oldJar).doesNotExist();
        assertThat(oldRecord).doesNotExist();
        assertThat(cache.forNode("node")).extracting(ConnectorArtifactHoldings.Held::sha256).containsExactly(latest);
        assertThat(runtime.resolve("orders").contentHash()).isEqualTo(latest);
        assertThatThrownBy(oldRef::acquireLoader).isInstanceOf(TapstateException.class);

        assertThat(install(runtime, original)).isEqualTo(old);
        try (var reopened = runtime.resolve("orders", old).acquireLoader()) {
            assertThat(reopened.loader()).isNotSameAs(originalLoader);
        }
    }

    @Test
    void sweepWaitsForDurableReferencePublication() throws Exception {
        var runtime = installer(new Cache(), (hash, output) -> { throw new AssertionError("Unexpected GridFS"); });
        String old = install(runtime, artifact("2.9.3"));
        install(runtime, artifact("2.9.12"));
        var entered = new java.util.concurrent.CountDownLatch(1);
        var scanning = new java.util.concurrent.CountDownLatch(1);
        var retained = new AtomicBoolean();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var writer = executor.submit(() -> runtime.withArtifactRetention(() -> {
                runtime.retain(runtime.resolve("orders", old));
                entered.countDown();
                try {
                    assertThat(scanning.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                retained.set(true);
                return null;
            }));
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var sweep = executor.submit(() -> {
                scanning.countDown();
                runtime.reclaim(Set.of("orders"), () -> retained.get() ? Set.of(old) : Set.of());
                return null;
            });
            writer.get(5, java.util.concurrent.TimeUnit.SECONDS);
            sweep.get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertThat(runtime.resolve("orders", old).contentHash()).isEqualTo(old);
    }

    @Test
    void startupCompletesInterruptedInstallsAndLeavesCompletedRecordsAlone() throws IOException {
        var cache = new Cache();
        LocalConnectorArtifacts.LegacyArtifact legacy = (hash, output) -> {
            throw new AssertionError("Unexpected GridFS");
        };
        Path source = artifact("2.9.3");
        String hash = ContentHash.of(Files.readAllBytes(source));
        Path plugins = Files.createDirectories(work.resolve("plugins"));
        var records = new ConnectorInstallRecords(plugins);
        var pending = new ConnectorInstallRecords.Installation("orders", "2.9.3", Origin.UPLOAD,
                Files.size(source), ConnectorInstallRecords.State.PENDING);

        records.save(hash, pending);
        var incomplete = installer(cache, legacy);
        incomplete.restore();
        assertThat(incomplete.versions()).isEmpty();
        assertThat(cache.byHash).isEmpty();

        Files.copy(source, plugins.resolve(hash + ".jar"));
        var moved = installer(cache, legacy);
        moved.restore();
        assertThat(moved.versions()).extracting(ConnectorArtifactMetadata::sha256).containsExactly(hash);
        assertThat(cache.forNode("node")).extracting(ConnectorArtifactHoldings.Held::sha256).containsExactly(hash);
        assertThat(records.read(hash)).contains(pending.complete());
        var originalRegistration = cache.byHash.get(hash);

        records.save(hash, pending);
        installer(cache, legacy).restore();
        assertThat(records.read(hash)).contains(pending.complete());
        assertThat(cache.byHash.get(hash)).isSameAs(originalRegistration);

        Path record = plugins.resolve(hash + ".json");
        Files.setLastModifiedTime(record, java.nio.file.attribute.FileTime.fromMillis(1000));
        var completedAt = Files.getLastModifiedTime(record);
        installer(cache, legacy).restore();
        assertThat(Files.getLastModifiedTime(record)).isEqualTo(completedAt);
        assertThat(cache.byHash.get(hash)).isSameAs(originalRegistration);

        Files.writeString(record, "interrupted");
        installer(cache, legacy).restore();
        assertThat(records.read(hash)).contains(new ConnectorInstallRecords.Installation(
                "orders", "2.9.3", Origin.DROP_DIRECTORY, Files.size(source), ConnectorInstallRecords.State.COMPLETE));
    }

    private LocalConnectorArtifacts installer(Cache cache, LocalConnectorArtifacts.LegacyArtifact legacy) {
        return new LocalConnectorArtifacts(work.resolve("plugins"), new ConnectorIntrospector(), cache,
                cache.specs, cache, "node", legacy, () -> List.of("orders"), "2.0.11-SNAPSHOT");
    }

    private static String install(LocalConnectorArtifacts installer, Path artifact) throws IOException {
        try (var input = Files.newInputStream(artifact)) {
            return installer.install(input, Origin.UPLOAD, "orders", null).registration().contentHash();
        }
    }

    private Path artifact(String version) throws IOException {
        Path original = Synthetic.seedableOrdersConnector(work.resolve("build-" + version));
        Map<String, byte[]> entries = new HashMap<>();
        try (var jar = new JarFile(original.toFile())) {
            for (var entry : java.util.Collections.list(jar.entries())) {
                if (!entry.isDirectory() && !entry.getName().equals(JarFile.MANIFEST_NAME)) {
                    try (var bytes = jar.getInputStream(entry)) {
                        entries.put(entry.getName(), bytes.readAllBytes());
                    }
                }
            }
        }
        return SyntheticJar.jarWithEntries(work.resolve("jar-" + version), entries,
                Map.of("PDK-API-Version", "2.9", "Implementation-Version", version));
    }

    private static final class Cache implements ConnectorCatalogStore, ConnectorArtifactHoldings {
        private final Map<String, ConnectorArtifactMetadata> byHash = new HashMap<>();
        private final Map<String, Held> possession = new HashMap<>();
        private final InMemoryConnectorSpecStore specs = new InMemoryConnectorSpecStore();

        @Override public void upsert(ConnectorCatalogEntry entry) { throw new AssertionError("Unexpected legacy upsert"); }
        @Override public Optional<ConnectorCatalogEntry> get(String id) { return Optional.empty(); }
        @Override public List<ConnectorCatalogEntry> list() { return List.of(); }
        @Override public void upsertArtifact(ConnectorArtifactMetadata value) { byHash.put(value.sha256(), value); }
        @Override public Optional<ConnectorArtifactMetadata> artifact(String id, String hash) {
            return Optional.ofNullable(byHash.get(hash)).filter(value -> id.equals(value.catalog().id()));
        }
        @Override public void publish(String nodeId, Held artifact) { possession.put(artifact.sha256(), artifact); }
        @Override public List<Held> forNode(String nodeId) { return List.copyOf(possession.values()); }
        @Override public void withdraw(String nodeId, String hash) { possession.remove(hash); }
    }
}
