package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.SinkCapability;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceMode;
import io.tapstate.spi.store.ConnectorArtifactMetadata;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    void aCloudPreflightSelectionUsesOnlyItsApprovedFilesWhenTheBundleLockIsElsewhere() throws Exception {
        Path seeds = Files.createDirectory(root.resolve("cloud-files"));
        Path approved = Files.writeString(seeds.resolve("mongodb-connector.jar"), "approved fixture");
        Files.writeString(seeds.resolve("unselected-connector.jar"), "outside the selected preflight");
        String hash = ContentHash.of(Files.readAllBytes(approved));
        var local = mock(LocalConnectorArtifacts.class);
        var readiness = mock(CloudConnectorSeedReadiness.class);
        when(readiness.selectedFileNames()).thenReturn(java.util.Set.of("mongodb-connector.jar"));
        var registered = new RegistrationOutcome(
                new ConnectorRegistration("mongodb", hash, "2.0.11", RegistrationSource.SEED), true);
        when(local.registerBundle(approved, "mongodb", hash, Files.size(approved))).thenReturn(registered);
        try (var runtime = new LocalConnectorRuntime(local, seeds, null, () -> {})) {
            assertThat(LocalConnectorRuntime.bundledFileNames(seeds)).isNull();
            assertThat(runtime.initialize(readiness)).containsExactly(new SeedOutcome.Seeded(approved, registered));
            var operations = inOrder(local);
            operations.verify(local).registerBundle(approved, "mongodb", hash, Files.size(approved));
            operations.verify(local).restore();
            operations.verifyNoMoreInteractions();
        }
    }

    @Test
    void droppedAdmissionDoesNotHideTheOtherOfflineSeed() throws Exception {
        Path seeds = Files.createDirectory(root.resolve("offline-seeds"));
        Path rejected = Files.writeString(seeds.resolve("a-unlisted.jar"), "unlisted");
        Path admitted = Files.writeString(seeds.resolve("b-supported.jar"), "supported");
        String hash = ContentHash.of(Files.readAllBytes(admitted));
        var registered = new RegistrationOutcome(
                new ConnectorRegistration("mongodb", hash, "2.0.11", RegistrationSource.REGISTER), true);
        var local = mock(LocalConnectorArtifacts.class);
        var failure = new TapstateException(ConnectorError.NOT_OFFICIAL, Map.of("connector", "unlisted"), null);
        when(local.install(any(), eq(ConnectorArtifactMetadata.Origin.DROP_DIRECTORY), isNull(), isNull()))
                .thenThrow(failure).thenReturn(registered);
        try (var runtime = new LocalConnectorRuntime(local, seeds, null, () -> {})) {
            assertThat(runtime.initialize(null)).containsExactly(
                    new SeedOutcome.Failed(rejected, failure), new SeedOutcome.Seeded(admitted, registered));
            assertThat(rejected).exists();
        }
    }

    @Test
    void anInvalidDropIsRetriedOnlyWhenTheFileChanges() throws IOException {
        Path drop = Files.writeString(root.resolve("broken.jar"), "invalid JAR");
        var local = mock(LocalConnectorArtifacts.class);
        when(local.installDrop(drop)).thenThrow(new TapstateException(
                ConnectorError.ARTIFACT_UNREADABLE, Map.of(), new ZipException("invalid JAR")));

        try (var runtime = new LocalConnectorRuntime(local, root, root, () -> {})) {
            runtime.pollDrops();
            runtime.pollDrops();
            verify(local).installDrop(drop);

            Files.writeString(drop, "replacement dropped JAR");
            runtime.pollDrops();
            verify(local, times(2)).installDrop(drop);
        }
    }

    @Test
    void anUnlistedDropIsRetriedWhenItsConnectorIsAdmitted() throws IOException {
        Path drop = Files.writeString(root.resolve("mysql.jar"), "connector JAR");
        var local = mock(LocalConnectorArtifacts.class);
        when(local.installDrop(drop)).thenThrow(new TapstateException(
                ConnectorError.NOT_OFFICIAL, Map.of("connector", "mysql"), null)).thenAnswer(call -> {
                    Files.delete(drop);
                    return null;
                });

        try (var runtime = new LocalConnectorRuntime(local, root, root, () -> {})) {
            runtime.pollDrops();
            runtime.pollDrops();
            verify(local).installDrop(drop);
            assertThat(drop).exists();

            when(local.admits("mysql")).thenReturn(true);
            runtime.pollDrops();
            verify(local, times(2)).installDrop(drop);
            assertThat(drop).doesNotExist();
        }
    }

    @Test
    void aTransientDropFailureIsRetriedWithoutChangingTheFile() throws IOException {
        Path drop = Files.writeString(root.resolve("pending.jar"), "connector JAR");
        var local = mock(LocalConnectorArtifacts.class);
        when(local.installDrop(drop)).thenThrow(new TapstateException(
                ConnectorError.ARTIFACT_UNREADABLE, Map.of(), new IOException("temporary read failure")));
        try (var runtime = new LocalConnectorRuntime(local, root, root, () -> {})) {
            runtime.pollDrops();
            runtime.pollDrops();
            verify(local, times(2)).installDrop(drop);
        }
    }

    @Test
    void supportRoleChangesUpdateTheInstalledCatalogWithoutReinstallingTheJar() {
        var local = mock(LocalConnectorArtifacts.class);
        var support = mock(ConnectorCatalogBackground.class);
        var catalog = mock(ConnectorCatalogStore.class);
        String id = "mongodb";
        String hash = "a".repeat(64);
        var sink = new SinkCapability(true, List.of());
        var entry = new ConnectorCatalogEntry(id, id, id, null, null, List.of(SourceMode.CDC),
                null, sink, true, List.of(), null);
        var installed = new ConnectorArtifactMetadata(entry, hash, "2.11.0", "2.0.11",
                ConnectorArtifactMetadata.Origin.BUNDLE, 100, Optional.empty());
        when(local.catalog()).thenReturn(List.of(installed));
        when(local.findAll(id)).thenReturn(List.of(new ConnectorRegistration(id, hash, "2.0.11", RegistrationSource.SEED)));
        when(catalog.artifact(id, hash)).thenReturn(Optional.of(installed));
        try (var runtime = new LocalConnectorRuntime(local, root, null, () -> {})) {
            var visible = runtime.view(catalog, support);
            when(support.permitsRole(id, "source")).thenReturn(true);
            when(support.permitsRole(id, "target")).thenReturn(false);
            var sourceOnly = visible.get(id).orElseThrow();
            assertThat(sourceOnly.modes()).containsExactly(SourceMode.CDC);
            assertThat(sourceOnly.sink().capable()).isFalse();
            assertThat(sourceOnly.pushOut()).isFalse();

            when(support.permitsRole(id, "source")).thenReturn(false);
            when(support.permitsRole(id, "target")).thenReturn(true);
            assertThat(visible.list()).singleElement().satisfies(targetOnly -> {
                assertThat(targetOnly.modes()).isEmpty();
                assertThat(targetOnly.sink()).isEqualTo(sink);
                assertThat(targetOnly.pushOut()).isTrue();
            });
            assertThat(visible.artifact(id, hash)).contains(installed);
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
