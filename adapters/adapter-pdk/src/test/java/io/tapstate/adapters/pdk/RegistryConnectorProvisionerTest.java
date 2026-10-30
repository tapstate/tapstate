package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.RegistrationSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The real connector provisioner: {@link RegistryConnectorProvisioner} resolves a connector id against
 * the distribution store, stages its registered bytes into a content-addressed on-disk cache
 * ({@code <hash>.jar}), and introspects the staged jar into the {@link ConnectorRef} the bridge loads
 * from. Staging is content-addressed, so a second resolve reuses the cached jar rather than fetching the
 * bytes again. An id that resolves to no artifact — or to more than one — is refused with a coded
 * connector-domain exception rather than loaded blindly. Synthetic connector jars compiled at test time
 * stand in for real connector dist jars; an in-memory registry stands in for the Mongo/GridFS store.
 */
class RegistryConnectorProvisionerTest {

    @Test
    void resolvesARegisteredConnectorToARefStagedInTheContentAddressedCache(@TempDir Path dir) throws IOException {
        Path cacheDir = dir.resolve("plugins");
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);

        ConnectorRef ref = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir)
                .resolve("orders");

        assertThat(ref.className()).isEqualTo("synthetic.OrdersConnector");
        assertThat(ref.pdkApiVersion()).isEqualTo("1.3.5");
        assertThat(ref.requiredLevel()).isNull();
        assertThat(ref.classpath()).hasSize(1);
        Path staged = ref.classpath().get(0);
        assertThat(staged.getParent()).isEqualTo(cacheDir);
        assertThat(staged.getFileName().toString()).endsWith(".jar");
        assertThat(Files.readAllBytes(staged)).isEqualTo(artifact);
    }

    /**
     * An artifact certified for one instance to serve several writers resolves to a ref that says so and names
     * its bytes; an artifact with no certification, or another's, runs one instance per writer.
     */
    @Test
    void resolvesAnArtifactCertifiedForSharingToARefThatSharesIt(@TempDir Path dir) throws IOException {
        Path cacheDir = dir.resolve("plugins");
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        String hash = registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact)
                .registration().contentHash();
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        ConnectorRef uncertified = provisioner.resolve("orders");
        registry.shareSafe.put("some-other-bytes", "v1");
        ConnectorRef certifiedForOthers = provisioner.resolve("orders");
        registry.shareSafe.put(hash, "v1");
        ConnectorRef certified = provisioner.resolve("orders");

        assertThat(uncertified.shareSafe()).isFalse();
        assertThat(certifiedForOthers.shareSafe()).isFalse();
        assertThat(certified.shareSafe()).isTrue();
        assertThat(certified.contentHash()).isEqualTo(hash);
    }

    @Test
    void reusesTheStagedArtifactOnASecondResolveInsteadOfFetchingBytesAgain(@TempDir Path dir) throws IOException {
        Path cacheDir = dir.resolve("plugins");
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        Path first = provisioner.resolve("orders").classpath().get(0);
        Path second = provisioner.resolve("orders").classpath().get(0);

        assertThat(second).isEqualTo(first);
        assertThat(registry.artifactCalls).isEqualTo(1);
    }

    @Test
    void refusesAByteChangedButStillReadableCachedConnectorOnResolve(@TempDir Path dir) throws Exception {
        Path seed = Synthetic.annotatedConnector(dir);
        byte[] artifact = Files.readAllBytes(seed);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        var registration = registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact).registration();
        Path cacheDir = dir.resolve("plugins");
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);
        ConnectorRef original = provisioner.resolve("orders");
        Path cached = original.classpath().get(0);
        assertThat(original.contentHash()).isEqualTo(registration.contentHash());
        assertThat(cached).isEqualTo(cacheDir.resolve(registration.contentHash() + ".jar"));
        assertThat(Files.readAllBytes(cached)).isEqualTo(artifact);

        Files.write(cached, new byte[] {0}, StandardOpenOption.APPEND);

        assertThat(Files.size(cached)).isEqualTo(artifact.length + 1L);
        assertThat(ContentHash.of(Files.readAllBytes(cached))).isNotEqualTo(registration.contentHash());
        try (JarFile jar = new JarFile(cached.toFile())) {
            var spec = jar.getJarEntry("orders-spec.json");
            assertThat(spec).isNotNull();
            try (var stream = jar.getInputStream(spec)) {
                assertThat(new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8))
                        .isEqualTo(original.spec());
            }
        }
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(cached))) {
            assertThat(loader.loadConnectorClass(original.className()).getName())
                    .isEqualTo("synthetic.OrdersConnector");
        }
        assertThat(Files.readAllBytes(seed)).isEqualTo(artifact);
        assertThat(registry.list()).containsExactly(registration);
        assertThat(registry.artifact(registration.contentHash())).hasValue(artifact);

        assertThatThrownBy(() -> provisioner.resolve("orders"))
                .as("a readable cached JAR with bytes different from its content address must be refused")
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void refusesACorruptCacheEvenWhenItsLengthIsUnchanged(@TempDir Path dir) throws IOException {
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), dir.resolve("plugins"));
        Path cached = provisioner.resolve("orders").classpath().get(0);
        byte[] corrupted = new byte[artifact.length];
        Files.write(cached, corrupted);

        assertSafeLoadFailure(() -> provisioner.resolve("orders"));

        assertThat(Files.readAllBytes(cached)).isEqualTo(corrupted);
        assertThat(registry.artifactCalls).as("a damaged cache is refused rather than replaced from the store")
                .isEqualTo(1);
    }

    @Test
    void refusesACacheLargerThanTheHeapWithoutExhaustingMemory(@TempDir Path dir) throws Exception {
        Path output = dir.resolve("limited-heap.log");
        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                CachedArtifactHeapProbe.class.getName(), dir.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS))
                    .as("limited-heap cache validation completes").isTrue();
            assertThat(process.exitValue())
                    .withFailMessage("Limited-heap cache validation failed:%n%s", Files.readString(output))
                    .isZero();
        } finally {
            process.destroyForcibly();
        }
    }

    /** Runs the corrupt-cache refusal in a JVM whose heap is smaller than the cached artifact. */
    public static final class CachedArtifactHeapProbe {
        public static void main(String[] args) throws IOException {
            Path cacheDir = Files.createDirectories(Path.of(args[0]).resolve("plugins"));
            InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
            String hash = registry.register("orders", "1.3.5", RegistrationSource.SEED, new byte[] {1})
                    .registration().contentHash();
            Path cached = cacheDir.resolve(hash + ".jar");
            long cacheSize = 96L * 1024 * 1024;
            // A sparse file supplies the oversized corruption without allocating its bytes in the test.
            try (FileChannel file = FileChannel.open(cached, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                file.position(cacheSize - 1);
                file.write(ByteBuffer.wrap(new byte[] {0}));
            }
            assertThat(Files.size(cached)).isEqualTo(cacheSize).isGreaterThan(Runtime.getRuntime().maxMemory());
            RegistryConnectorProvisioner provisioner =
                    new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

            assertSafeLoadFailure(() -> provisioner.resolve("orders"));

            assertThat(registry.artifactCalls).isZero();
            assertThat(Files.size(cached)).isEqualTo(cacheSize);
        }
    }

    @Test
    void refusesAnUnreadableCacheWithoutPathsOrCauses(@TempDir Path dir) throws IOException {
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        String hash = registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact)
                .registration().contentHash();
        Path cacheDir = dir.resolve("plugins");
        Path cached = Files.createDirectories(cacheDir.resolve(hash + ".jar"));
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        assertSafeLoadFailure(() -> provisioner.resolve("orders"));

        assertThat(cached).isDirectory();
        assertThat(registry.artifactCalls).isZero();
    }

    @Test
    void materializesAMissingCacheAgainFromValidatedStoreBytes(@TempDir Path dir) throws IOException {
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), dir.resolve("plugins"));
        ConnectorRef first = provisioner.resolve("orders");
        Files.delete(first.classpath().get(0));

        ConnectorRef restored = provisioner.resolve("orders");

        assertThat(restored).isEqualTo(first);
        assertThat(Files.readAllBytes(restored.classpath().get(0))).isEqualTo(artifact);
        assertThat(registry.artifactCalls).isEqualTo(2);
    }

    @Test
    void refusesMismatchedStoreBytesBeforePublishingTheCache(@TempDir Path dir) throws IOException {
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        String hash = registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact)
                .registration().contentHash();
        registry.replaceArtifact(hash, java.util.Arrays.copyOf(artifact, artifact.length + 1));
        Path cacheDir = dir.resolve("plugins");
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        assertSafeLoadFailure(() -> provisioner.resolve("orders"));

        assertThat(cacheDir).isEmptyDirectory();
        assertThat(registry.artifactCalls).isEqualTo(1);
    }

    @Test
    void refusesAnUnwritableStagingDirectoryWithoutPathsOrCauses(@TempDir Path dir) throws IOException {
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);
        Path cacheDir = Files.createFile(dir.resolve("not-a-directory")).resolve("plugins");
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        assertSafeLoadFailure(() -> provisioner.resolve("orders"));

        assertThat(cacheDir.getParent()).isRegularFile();
        assertThat(Files.size(cacheDir.getParent())).isZero();
    }

    @Test
    void refusesACacheAnotherStagerPlacedWithDifferentBytes(@TempDir Path dir) throws IOException {
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        byte[] changed = java.util.Arrays.copyOf(artifact, artifact.length + 1);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        String hash = registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact)
                .registration().contentHash();
        Path cacheDir = dir.resolve("plugins");
        Path target = cacheDir.resolve(hash + ".jar");
        AtomicReference<Object> placedFile = new AtomicReference<>();
        registry.onArtifactFetch = () -> {
            try {
                Files.createDirectories(cacheDir);
                Files.write(target, changed);
                placedFile.set(fileKey(target));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        assertSafeLoadFailure(() -> provisioner.resolve("orders"));

        assertThat(Files.readAllBytes(target)).isEqualTo(changed);
        assertThat(fileKey(target)).isEqualTo(placedFile.get());
        try (var files = Files.list(cacheDir)) {
            assertThat(files.toList()).containsExactly(target);
        }
    }

    @Test
    void concurrentResolvesStageOnceAndShareTheValidCache(@TempDir Path dir) throws Exception {
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), dir.resolve("plugins"));
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> { start.await(); return provisioner.resolve("orders"); });
            var second = workers.submit(() -> { start.await(); return provisioner.resolve("orders"); });
            start.countDown();

            ConnectorRef firstRef = first.get(10, TimeUnit.SECONDS);
            ConnectorRef secondRef = second.get(10, TimeUnit.SECONDS);

            assertThat(secondRef).isEqualTo(firstRef);
            assertThat(registry.artifactCalls).isEqualTo(1);
            assertThat(Files.readAllBytes(firstRef.classpath().get(0))).isEqualTo(artifact);
            assertThat(ConnectorClassLoader.shared(secondRef.classpath()))
                    .isSameAs(ConnectorClassLoader.shared(firstRef.classpath()));
        }
    }

    @Test
    void anArtifactAnotherStagerPlacedFirstIsKeptAndKeepsItsLoader(@TempDir Path dir) throws IOException {
        Path cacheDir = dir.resolve("plugins");
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);
        Path target = cacheDir.resolve(ContentHash.of(artifact) + ".jar");
        AtomicReference<Object> placedFile = new AtomicReference<>();
        AtomicReference<ConnectorClassLoader> placedLoader = new AtomicReference<>();
        // Another stager - a second process on this cache - finishes while this resolve is fetching the
        // bytes, and a connector is already opened from what it placed.
        registry.onArtifactFetch = () -> {
            try {
                Files.createDirectories(cacheDir);
                Files.write(target, artifact);
                Files.setLastModifiedTime(target, FileTime.fromMillis(1_000_000_000_000L));
                placedFile.set(fileKey(target));
                placedLoader.set(ConnectorClassLoader.shared(List.of(target)));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        ConnectorRef ref = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir)
                .resolve("orders");

        assertThat(ref.classpath()).containsExactly(target);
        assertThat(placedFile.get()).as("this file system names a file by a key").isNotNull();
        assertThat(fileKey(target)).as("the file the other stager placed, not one renamed over it")
                .isEqualTo(placedFile.get());
        assertThat(ConnectorClassLoader.shared(ref.classpath()))
                .as("the loader a connector was already opened through")
                .isSameAs(placedLoader.get());
    }

    @Test
    void refusesAConnectorIdWithNoRegisteredArtifact(@TempDir Path dir) {
        RegistryConnectorProvisioner provisioner = new RegistryConnectorProvisioner(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(), dir.resolve("plugins"));

        assertThatThrownBy(() -> provisioner.resolve("ghost"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException ce = (TapstateException) e;
                    assertThat(ce.code()).isEqualTo(ConnectorError.NOT_REGISTERED);
                    assertThat(ce.args()).containsEntry("connector", "ghost");
                });
    }

    @Test
    void refusesAConnectorIdThatResolvesToMoreThanOneRegisteredArtifact(@TempDir Path dir) throws IOException {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED,
                Files.readAllBytes(Synthetic.annotatedConnector(dir)));
        registry.register("orders", "2.0.8", RegistrationSource.REGISTER,
                Files.readAllBytes(Synthetic.annotatedEmittingConnector(dir)));
        RegistryConnectorProvisioner provisioner = new RegistryConnectorProvisioner(
                registry, new ConnectorIntrospector(), dir.resolve("plugins"));

        assertThatThrownBy(() -> provisioner.resolve("orders"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException ce = (TapstateException) e;
                    assertThat(ce.code()).isEqualTo(ConnectorError.AMBIGUOUS_REGISTRATION);
                    assertThat(ce.args()).containsEntry("connector", "orders").containsKey("artifacts");
                });
    }

    @Test
    void refusesARegistrationWhoseArtifactBytesAreMissing(@TempDir Path dir) {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.addDanglingRegistration("orders", "0000feedface", "1.3.5");
        RegistryConnectorProvisioner provisioner = new RegistryConnectorProvisioner(
                registry, new ConnectorIntrospector(), dir.resolve("plugins"));

        assertThatThrownBy(() -> provisioner.resolve("orders"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(ConnectorError.LOAD_FAILED));
    }

    @Test
    void retainedContextSelectsItsExactRegisteredHashEvenWhenNormalResolutionIsAmbiguous(@TempDir Path dir) throws IOException {
        var registry = new InMemoryConnectorRegistry();
        String retained = registry.register("orders", "1.3.5", RegistrationSource.SEED,
                Files.readAllBytes(Synthetic.annotatedConnector(dir))).registration().contentHash();
        registry.register("orders", "2.0.8", RegistrationSource.REGISTER,
                Files.readAllBytes(Synthetic.annotatedEmittingConnector(dir)));
        var provisioner = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), dir.resolve("plugins"));
        assertThatThrownBy(() -> provisioner.resolve("orders")).isInstanceOf(TapstateException.class);
        var exact = provisioner.resolve("orders", retained);
        assertThat(exact.contentHash()).isEqualTo(retained);
        assertThat(exact.className()).isEqualTo("synthetic.OrdersConnector");
    }

    @Test
    void retainedResolutionRefusesMissingRegistrationBytesAndChangedArtifacts(@TempDir Path dir) {
        var registry = new InMemoryConnectorRegistry();
        String hash = "a".repeat(64);
        var provisioner = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), dir.resolve("plugins"));
        assertThatThrownBy(() -> provisioner.resolve("orders", hash)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ConnectorError.NOT_REGISTERED));
        registry.addDanglingRegistration("orders", hash, "2.0.8");
        assertSafeLoadFailure(() -> provisioner.resolve("orders", hash));
        registry.replaceArtifact(hash, new byte[]{1, 2});
        assertSafeLoadFailure(() -> provisioner.resolve("orders", hash));
    }

    @Test
    void requiresItsCollaborators(@TempDir Path dir) {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        ConnectorIntrospector introspector = new ConnectorIntrospector();
        Path cacheDir = dir.resolve("plugins");

        assertThatNullPointerException().isThrownBy(
                () -> new RegistryConnectorProvisioner(null, introspector, cacheDir));
        assertThatNullPointerException().isThrownBy(
                () -> new RegistryConnectorProvisioner(registry, null, cacheDir));
        assertThatNullPointerException().isThrownBy(
                () -> new RegistryConnectorProvisioner(registry, introspector, null));
    }

    private static Object fileKey(Path file) throws IOException {
        return Files.readAttributes(file, BasicFileAttributes.class).fileKey();
    }

    private static void assertSafeLoadFailure(Runnable resolve) {
        assertThatThrownBy(resolve::run).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(ConnectorError.LOAD_FAILED);
            assertThat(failure.args()).isEqualTo(Map.of("connector", "orders"));
            assertThat(failure).hasNoCause().hasMessage("connector.load-failed {connector=orders}");
        });
    }

}
