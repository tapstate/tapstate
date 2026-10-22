package io.tapstate.app;

import io.tapstate.adapters.pdk.SyntheticSinkJars;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.StorageBinding;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import static org.assertj.core.api.Assertions.*;

/** Drives an actual isolated test JAR through installed registration and testing, without a CACHE or member. */
class InstalledConnectorTestsTest {
    @Test void installedRegistrationAvoidsAnExtraWholeArtifactTransferCopy(@TempDir Path work) throws Exception {
        Path artifact = SyntheticSinkJars.registeredStorageProbe(work.resolve("build"), work.resolve("trace"));
        Path seeds = Files.createDirectory(work.resolve("seeds"));
        byte[] block = new byte[8192];
        int blocks = 200 * 1024 * 1024 / block.length;
        CRC32 crc = new CRC32();
        for (int i = 0; i < blocks; i++) crc.update(block);
        try (var out = new ZipOutputStream(Files.newOutputStream(seeds.resolve("mongodb.jar")));
             var input = new ZipInputStream(Files.newInputStream(artifact))) {
            ZipEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                out.putNextEntry(new ZipEntry(entry.getName())); input.transferTo(out); out.closeEntry();
            }
            var padding = new ZipEntry("padding.bin"); padding.setMethod(ZipEntry.STORED);
            padding.setSize((long) blocks * block.length); padding.setCrc(crc.getValue());
            out.putNextEntry(padding);
            for (int i = 0; i < blocks; i++) out.write(block);
            out.closeEntry();
        }
        Path output = work.resolve("installed-heap.log");
        var command = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx600m", "-XX:+UseSerialGC", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                InstalledHeapProbe.class.getName(), work.toString());
        for (String key : List.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS")) command.environment().remove(key);
        Process child = command.redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertThat(child.waitFor(45, TimeUnit.SECONDS)).as("installed connector preparation finishes").isTrue();
            assertThat(child.exitValue()).withFailMessage("Installed connector heap probe failed:%n%s", Files.readString(output)).isZero();
        } finally { child.destroyForcibly(); }
    }

    public static final class InstalledHeapProbe {
        public static void main(String[] args) {
            Path work = Path.of(args[0]);
            var properties = new ConnectorPluginProperties();
            properties.setSeedDir(work.resolve("seeds")); properties.setPluginsDir(work.resolve("plugins"));
            var target = new InstalledConnectorTests(properties, java.time.Clock.systemUTC()).target();
            var connection = connection("accepted");
            var identity = target.identity(connection);
            target.test(connection, target.definition(connection, "views"), identity);
            assertThat(work.resolve("trace")).hasContent("mongodb://fixture/accepted\n");
        }
    }

    @Test void minimumRootTestsItsRegisteredArtifactWithoutConstructingBusiness(@TempDir Path work) throws Exception {
        Path trace = work.resolve("connection-tests");
        Path artifact = SyntheticSinkJars.registeredStorageProbe(work.resolve("build"), trace);
        Path seeds = Files.createDirectory(work.resolve("seeds")); Files.copy(artifact, seeds.resolve("mongodb.jar"));
        try (var context = new SpringApplicationBuilder(Bootstrap.class).web(WebApplicationType.NONE)
                .environment(CloudFixtureEnvironment.isolated()).run("--tapstate.store.mongo.uri=", "--SDK_STATUS_SENDER_ENABLED=false",
                        "--tapstate.control.auth.jwt-secret=installed-connector-test-key", "--logging.level.root=ERROR",
                        "--tapstate.connectors.seed-dir=" + seeds, "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"))) {
            var target = context.getBean(InstalledConnectorTests.class).target();
            var accepted = connection("accepted");
            var original = target.identity(accepted);
            assertThat(original.artifactHash()).isEqualTo(ContentHash.of(Files.readAllBytes(artifact)));
            assertThat(trace).doesNotExist();
            target.test(accepted, target.definition(accepted, "views"), original);
            var refused = connection("refused");
            assertThatThrownBy(() -> target.test(refused, target.definition(refused, "views"), original))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_TEST_FAILED));
            assertThat(Files.readAllLines(trace)).isEqualTo(List.of("mongodb://fixture/accepted", "mongodb://fixture/refused"));
            assertThat(context.getBean(StorageRoot.class).runtime().current()).isEmpty();
            assertThat(context.getBean(StorageRoot.class).runtime().business()).isEmpty();
            assertThat(context.getBeansOfType(com.hazelcast.core.HazelcastInstance.class)).isEmpty();
        }
    }
    private static StorageBinding.Connection connection(String database) {
        return new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://fixture/" + database, database, null);
    }
}
