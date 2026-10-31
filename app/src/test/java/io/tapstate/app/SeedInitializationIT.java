package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.PdkCapabilityDeriver;
import io.tapstate.adapters.pdk.RegistryConnectorProvisioner;
import io.tapstate.adapters.pdk.SeedConnectorSweep;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.assertThat;

/** Paired local replica-set measurements over the actual immutable eight-artifact release. */
@RequiresDocker
class SeedInitializationIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @TempDir Path work;

    @Test
    void localOpStorageRetainsBothIntegrityGatesWhileAvoidingStagingDownloads() throws Exception {
        Path seeds = CloudConnectorTestInputs.seedDirectory();
        byte[] lock = java.nio.file.Files.readAllBytes(seeds.resolveSibling("release").resolve("connectors.lock.json"));
        long bytes = 443_262_490L;
        int cpu = Math.max(1, Runtime.getRuntime().availableProcessors());
        // Timing is a retained observation, never a machine-speed pass/fail threshold.
        int trials = Math.max(1, Integer.getInteger("tapstate.test.seed-trials", 1));
        for (int trial = 0; trial < trials; trial++) {
            for (boolean reuseSeed : List.of(false, true)) {
                String mode = reuseSeed ? "cpu-seed" : "serial-registry";
                var storage = NativeAppStorage.create(MONGO, "seed_perf", StorageBinding.Mode.OP,
                        "seed-perf-" + java.util.UUID.randomUUID());
                var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                        storage.cacheUri(), storage.cacheDatabase(), null);
                try (var domain = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "seed-perf")) {
                    var scope = storage.preparation().businessScope();
                    var registry = new MeteredRegistry(domain.connectors(scope, () -> {}));
                    var catalog = domain.connectorCatalog(scope, () -> {});
                    var specs = domain.connectorSpecs(scope, () -> {});
                    Path plugins = work.resolve(mode + "-" + trial);
                    for (String state : List.of("cold", "warm", "cold-local-cache")) {
                        if (state.equals("cold-local-cache")) plugins = work.resolve(mode + "-" + trial + "-restart");
                        var introspector = new ConnectorIntrospector();
                        var provisioner = new RegistryConnectorProvisioner(registry, introspector, plugins, reuseSeed ? seeds : null);
                        var deriver = new PdkCapabilityDeriver(provisioner);
                        var registrar = new ConnectorArtifactRegistrar(registry, introspector, deriver, catalog, specs);
                        registry.reset();
                        long start = System.nanoTime();
                        var gate = new CloudConnectorSeedReadiness(seeds, plugins, lock, registry, catalog, specs, deriver);
                        long checked = System.nanoTime();
                        var outcomes = new SeedConnectorSweep(registrar, reuseSeed ? cpu : 1).sweep(seeds);
                        long registered = System.nanoTime();
                        gate.verifyRegistrations(outcomes);
                        long ready = System.nanoTime();
                        assertThat(outcomes).hasSize(8).allSatisfy(outcome -> assertThat(outcome).isInstanceOf(SeedOutcome.Seeded.class));
                        assertThat(registry.list()).hasSize(8);
                        long expectedRead = switch (state) {
                            case "cold" -> bytes * (reuseSeed ? 1 : 2);
                            case "warm" -> bytes * 2;
                            default -> bytes * (reuseSeed ? 2 : 3);
                        };
                        assertThat(registry.readBytes.get()).as(mode + "/" + state).isEqualTo(expectedRead);
                        assertThat(registry.uploadBytes.get()).isEqualTo(state.equals("cold") ? bytes : 0);
                        System.out.printf("SEED_MEASUREMENT mode=%s trial=%d state=%s workers=%d preflight_ms=%d registration_ms=%d final_ms=%d total_ms=%d upload_ms=%d readback_ms=%d upload_bytes=%d readback_bytes=%d%n",
                                mode, trial, state, reuseSeed ? cpu : 1, millis(checked - start), millis(registered - checked),
                                millis(ready - registered), millis(ready - start), millis(registry.uploadNanos.get()),
                                millis(registry.readNanos.get()), registry.uploadBytes.get(), registry.readBytes.get());
                    }
                }
            }
        }
    }

    private static long millis(long nanos) { return nanos / 1_000_000; }

    private static final class MeteredRegistry implements ConnectorRegistry {
        private final ConnectorRegistry delegate;
        final AtomicLong readBytes = new AtomicLong();
        final AtomicLong uploadBytes = new AtomicLong();
        final AtomicLong readNanos = new AtomicLong();
        final AtomicLong uploadNanos = new AtomicLong();
        MeteredRegistry(ConnectorRegistry delegate) { this.delegate = delegate; }
        void reset() { readBytes.set(0); uploadBytes.set(0); readNanos.set(0); uploadNanos.set(0); }
        @Override public RegistrationOutcome register(String id, String version, RegistrationSource source, byte[] bytes) {
            throw new AssertionError("Registration must stream");
        }
        @Override public RegistrationOutcome register(String id, String version, RegistrationSource source,
                String hash, long length, InputStream bytes) throws IOException {
            boolean existing = delegate.hasArtifact(hash);
            long start = System.nanoTime();
            try {
                var outcome = delegate.register(id, version, source, hash, length, bytes);
                if (!existing && outcome.newlyRegistered()) uploadBytes.addAndGet(length);
                return outcome;
            } finally { uploadNanos.addAndGet(System.nanoTime() - start); }
        }
        @Override public List<ConnectorRegistration> list() { return delegate.list(); }
        @Override public List<ConnectorRegistration> findAll(String id) { return delegate.findAll(id); }
        @Override public boolean hasArtifact(String hash) { return delegate.hasArtifact(hash); }
        @Override public Optional<byte[]> artifact(String hash) { throw new AssertionError("Readback must stream"); }
        @Override public boolean copyArtifactTo(String hash, OutputStream output) throws IOException {
            long start = System.nanoTime();
            try {
                return delegate.copyArtifactTo(hash, new OutputStream() {
                    @Override public void write(int value) throws IOException { output.write(value); readBytes.incrementAndGet(); }
                    @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                        output.write(bytes, offset, length); readBytes.addAndGet(length);
                    }
                });
            } finally { readNanos.addAndGet(System.nanoTime() - start); }
        }
    }
}
