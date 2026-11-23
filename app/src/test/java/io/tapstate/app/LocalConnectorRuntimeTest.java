package io.tapstate.app;

import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class LocalConnectorRuntimeTest {
    @TempDir
    Path root;

    @Test
    void startupAdmitsOnlyTheLockedBundleBeforeApplyingLocalDrops() throws Exception {
        Path seeds = Files.createDirectory(root.resolve("connectors"));
        Path drops = Files.createDirectory(root.resolve("drop"));
        Path bundle = Files.writeString(seeds.resolve("mongodb-connector.jar"), "bundle fixture");
        Files.writeString(seeds.resolve("unlisted-connector.jar"), "unlisted fixture");
        Path drop = Files.writeString(drops.resolve("postgres-connector.jar"), "drop fixture");
        Files.writeString(drops.resolve("partial.tmp"), "in progress");
        String hash = ContentHash.of(Files.readAllBytes(bundle));
        Path manifest = Files.createDirectory(root.resolve("release")).resolve("connectors.lock.json");
        Files.writeString(manifest, """
                {"connectors":[{"id":"mongodb","sha256":"%s","bytes":%d}]}
                """.formatted(hash, Files.size(bundle)));

        var local = mock(LocalConnectorArtifacts.class);
        var registered = new RegistrationOutcome(new ConnectorRegistration("mongodb", hash, "2.0.11", RegistrationSource.SEED), true);
        when(local.registerBundle(bundle, "mongodb", hash, Files.size(bundle))).thenReturn(registered);
        try (var runtime = new LocalConnectorRuntime(local, seeds, drops, () -> {})) {
            assertThat(LocalConnectorRuntime.bundledFileNames(seeds)).containsExactly("mongodb-connector.jar");
            assertThat(runtime.initialize(null)).containsExactly(new SeedOutcome.Seeded(bundle, registered));
            var operations = inOrder(local);
            operations.verify(local).registerBundle(bundle, "mongodb", hash, Files.size(bundle));
            operations.verify(local).restore();
            operations.verify(local).installDrop(drop);
            operations.verifyNoMoreInteractions();
        }
    }

    @Test
    void anUnreadableBundleManifestRefusesStartupBeforeRegisteringAnything() throws Exception {
        Path seeds = Files.createDirectory(root.resolve("connectors"));
        Path manifest = Files.createDirectory(root.resolve("release")).resolve("connectors.lock.json");
        Files.writeString(manifest, "{\"connectors\":{}}");
        var local = mock(LocalConnectorArtifacts.class);
        try (var runtime = new LocalConnectorRuntime(local, seeds, null, () -> {})) {
            assertThatThrownBy(() -> LocalConnectorRuntime.bundledFileNames(seeds))
                    .isInstanceOf(UncheckedIOException.class);
            assertThatThrownBy(() -> runtime.initialize(null)).isInstanceOf(UncheckedIOException.class);
            verifyNoInteractions(local);
        }
    }
}
