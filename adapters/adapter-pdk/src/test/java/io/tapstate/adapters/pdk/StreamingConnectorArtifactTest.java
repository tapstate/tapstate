package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamingConnectorArtifactTest {
    @Test void preservesOnlySafeUnknownCleanupDiagnosticsAcrossTheRegistrarBoundary(@TempDir Path dir) throws IOException {
        Path jar = Synthetic.seedableMysqlConnector(dir);
        String hash;
        try (var input = Files.newInputStream(jar)) { hash = ContentHash.of(input); }
        String upload = "bc5352ad-87ad-42b6-a0c8-c61926b29c91";
        var registry = new StreamingRegistry(jar) {
            @Override public RegistrationOutcome register(String id, String version, RegistrationSource source,
                                                           String expectedHash, long length, InputStream bytes) throws IOException {
                var failure = new IOException("private-sentinel");
                failure.addSuppressed(new TapstateException(IoError.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED,
                        Map.of("hash", hash, "upload", upload), new IOException("private-sentinel")));
                failure.addSuppressed(new IOException("private-sentinel"));
                throw failure;
            }
        };
        var registrar = new ConnectorArtifactRegistrar(registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of()), new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore());
        assertThatThrownBy(() -> registrar.register(jar, RegistrationSource.SEED))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ConnectorError.ARTIFACT_UNREADABLE);
                    assertThat(failure.getCause()).isNull(); assertThat(failure.getSuppressed()).hasSize(1);
                    assertThat(failure.getSuppressed()[0]).isInstanceOfSatisfying(TapstateException.class, cleanup -> {
                        assertThat(cleanup.code()).isEqualTo(IoError.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED);
                        assertThat(cleanup.args()).containsExactlyInAnyOrderEntriesOf(Map.of("hash", hash, "upload", upload));
                        assertThat(cleanup.getCause()).isNull(); assertThat(cleanup.getSuppressed()).isEmpty();
                    });
                });
    }

    @Test void registersAndLoadsAnArtifactLargerThanTheHeap(@TempDir Path dir) throws Exception {
        Path small = Synthetic.seedableMysqlConnector(dir.resolve("small"));
        Path large = dir.resolve("large.jar");
        byte[] block = new byte[8192];
        int blocks = 96 * 1024 * 1024 / block.length;
        CRC32 crc = new CRC32();
        for (int i = 0; i < blocks; i++) crc.update(block);
        try (var out = new ZipOutputStream(Files.newOutputStream(large));
             var in = new ZipInputStream(Files.newInputStream(small))) {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                out.putNextEntry(new ZipEntry(entry.getName())); in.transferTo(out); out.closeEntry();
            }
            ZipEntry padding = new ZipEntry("padding.bin");
            padding.setMethod(ZipEntry.STORED); padding.setSize((long) blocks * block.length); padding.setCrc(crc.getValue());
            out.putNextEntry(padding);
            for (int i = 0; i < blocks; i++) out.write(block);
            out.closeEntry();
        }
        Path output = dir.resolve("limited-heap.log");
        ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx64m", "-XX:+UseSerialGC", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                HeapProbe.class.getName(), dir.toString());
        for (String key : List.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")) builder.environment().remove(key);
        Process child = builder.redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertThat(child.waitFor(45, TimeUnit.SECONDS)).as("bounded artifact transfer completes").isTrue();
            assertThat(child.exitValue()).withFailMessage("Limited-heap transfer failed:%n%s", Files.readString(output)).isZero();
        } finally { child.destroyForcibly(); }
    }

    public static final class HeapProbe {
        public static void main(String[] args) throws Exception {
            Path dir = Path.of(args[0]); Path jar = dir.resolve("large.jar");
            assertThat(Files.size(jar)).isGreaterThan(Runtime.getRuntime().maxMemory());
            var registry = new StreamingRegistry(jar);
            var rows = new InMemoryConnectorCatalogStore();
            var registrar = new ConnectorArtifactRegistrar(registry, new ConnectorIntrospector(),
                    id -> new ConnectorCapabilities(Set.of("batch_read_function")), rows, new InMemoryConnectorSpecStore());
            RegistrationOutcome result = registrar.register(jar, RegistrationSource.SEED);
            assertThat(result.registration().connectorId()).isEqualTo("mysql");
            var provisioner = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), dir.resolve("cache"));
            Path loaded = provisioner.resolve("mysql").classpath().getFirst();
            assertThat(Files.size(loaded)).isEqualTo(Files.size(jar));
            try (var input = Files.newInputStream(loaded)) {
                assertThat(ContentHash.of(input)).isEqualTo(result.registration().contentHash());
            }
            assertThat(provisioner.resolve("mysql").classpath()).containsExactly(loaded);
            assertThat(registry.downloads).isEqualTo(1);
        }
    }

    private static class StreamingRegistry implements ConnectorRegistry {
        private final Path original;
        private ConnectorRegistration registration;
        private int downloads;
        StreamingRegistry(Path original) { this.original = original; }
        @Override public RegistrationOutcome register(String id, String version, RegistrationSource source, byte[] bytes) {
            throw new AssertionError("Registration must not materialize the whole artifact");
        }
        public RegistrationOutcome register(String id, String version, RegistrationSource source,
                                            String hash, long length, InputStream bytes) throws IOException {
            assertThat(length).isEqualTo(Files.size(original));
            assertThat(ContentHash.of(bytes)).isEqualTo(hash);
            registration = new ConnectorRegistration(id, hash, version, source);
            return new RegistrationOutcome(registration, true);
        }
        @Override public List<ConnectorRegistration> list() { return registration == null ? List.of() : List.of(registration); }
        @Override public Optional<byte[]> artifact(String hash) { throw new AssertionError("Download must stream"); }
        public boolean copyArtifactTo(String hash, OutputStream target) throws IOException {
            assertThat(hash).isEqualTo(registration.contentHash()); downloads++;
            try (var input = Files.newInputStream(original)) { input.transferTo(target); }
            return true;
        }
        @Override public boolean hasArtifact(String hash) { return registration != null && registration.contentHash().equals(hash); }
    }
}
