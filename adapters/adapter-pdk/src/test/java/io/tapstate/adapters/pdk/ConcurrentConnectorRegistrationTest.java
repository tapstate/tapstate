package io.tapstate.adapters.pdk;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;

/** A hash-addressed registry must not turn concurrent current registrations into two active versions. */
class ConcurrentConnectorRegistrationTest {
    @Test
    void sameIdWithDifferentBytesPublishesOnlyOneArtifact(@TempDir Path work) throws Exception {
        Path first = Synthetic.seedableMysqlConnector(work.resolve("fixture"));
        Path second = Files.copy(first, work.resolve("second.jar"));
        // A trailing byte changes the address while leaving the declared connector and JAR readable.
        Files.write(second, new byte[] {42}, StandardOpenOption.APPEND);
        String firstHash;
        String secondHash;
        try (var input = Files.newInputStream(first)) { firstHash = ContentHash.of(input); }
        try (var input = Files.newInputStream(second)) { secondHash = ContentHash.of(input); }
        var paths = java.util.Map.of(firstHash, first, secondHash, second);
        var checks = new AtomicInteger();
        var firstCheck = new CountDownLatch(1);
        var bothChecks = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var registrations = new ConcurrentHashMap<String, ConnectorRegistration>();
        var registry = new ConnectorRegistry() {
            @Override public List<ConnectorRegistration> list() { return List.copyOf(registrations.values()); }
            @Override public List<ConnectorRegistration> findAll(String id) {
                var original = list().stream().filter(row -> row.connectorId().equals(id)).toList();
                if (checks.incrementAndGet() <= 2) {
                    firstCheck.countDown(); bothChecks.countDown();
                    try { assertThat(release.await(10, TimeUnit.SECONDS)).isTrue(); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
                }
                return original;
            }
            @Override public RegistrationOutcome register(String id, String version, RegistrationSource source, byte[] bytes) {
                throw new AssertionError("Registration must stream");
            }
            @Override public RegistrationOutcome register(String id, String version, RegistrationSource source,
                    String hash, long length, InputStream bytes) throws IOException {
                assertThat(ContentHash.of(bytes)).isEqualTo(hash);
                var row = new ConnectorRegistration(id, hash, version, source);
                var winner = registrations.putIfAbsent(hash, row);
                return new RegistrationOutcome(winner == null ? row : winner, winner == null);
            }
            @Override public Optional<byte[]> artifact(String hash) { throw new AssertionError("Artifact reads must stream"); }
            @Override public boolean copyArtifactTo(String hash, java.io.OutputStream target) throws IOException {
                if (!registrations.containsKey(hash)) return false;
                try (var input = Files.newInputStream(paths.get(hash))) { input.transferTo(target); }
                return true;
            }
            @Override public boolean hasArtifact(String hash) { return registrations.containsKey(hash); }
        };
        var catalog = new ConnectorCatalogStore() {
            @Override public void upsert(ConnectorCatalogEntry row) { }
            @Override public Optional<ConnectorCatalogEntry> get(String id) { return Optional.empty(); }
            @Override public List<ConnectorCatalogEntry> list() { return List.of(); }
        };
        var specs = new ConnectorSpecStore() {
            @Override public void put(String hash, byte[] bytes) { }
            @Override public Optional<byte[]> get(String hash) { return Optional.empty(); }
        };
        var registrar = new ConnectorArtifactRegistrar(registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of()), catalog, specs);
        try (var callers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var a = callers.submit(() -> register(registrar, first));
            assertThat(firstCheck.await(5, TimeUnit.SECONDS)).isTrue();
            var b = callers.submit(() -> register(registrar, second));
            // Under the correct ID lock, the second check cannot enter until the first publishes.
            // Under the defect, both snapshots are empty; this barrier makes that race deterministic.
            bothChecks.await(2, TimeUnit.SECONDS);
            release.countDown();
            var outcomes = List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS));
            assertThat(outcomes.stream().filter(RegistrationOutcome.class::isInstance).count()).isEqualTo(1);
            assertThat(outcomes.stream().filter(TapstateException.class::isInstance).toList()).singleElement()
                    .satisfies(failure -> assertThat(((TapstateException) failure).code()).isEqualTo(ConnectorError.REGISTRATION_CONFLICT));
        } finally { release.countDown(); }
        assertThat(registry.findAll("mysql")).hasSize(1);
        var resolved = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), work.resolve("plugins"))
                .resolve("mysql");
        assertThat(resolved.contentHash()).isEqualTo(firstHash);
    }

    private static Object register(ConnectorArtifactRegistrar registrar, Path artifact) {
        try { return registrar.register(artifact, RegistrationSource.SEED); }
        catch (TapstateException refusal) { return refusal; }
    }
}
