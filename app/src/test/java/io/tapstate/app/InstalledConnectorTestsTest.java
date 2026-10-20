package io.tapstate.app;

import io.tapstate.adapters.pdk.SyntheticSinkJars;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.StorageBinding;
import java.nio.file.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import static org.assertj.core.api.Assertions.*;

/** Drives an actual isolated test JAR through installed registration and testing, without a CACHE or member. */
class InstalledConnectorTestsTest {
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
