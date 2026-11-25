package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.RegistrationSource;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The startup seed sweep: every connector jar in the seed directory goes through the same
 * register-if-absent path an explicit register uses, the files stay where they are, and one
 * defective artifact never stops the rest from registering — so shipping jars in a seed directory
 * and restarting nodes (or several nodes sweeping concurrently) is harmless by construction.
 */
class SeedConnectorSweepTest {

    @Test
    void registersIndependentArtifactsConcurrentlyAndReportsInFileOrder(@TempDir Path seeds, @TempDir Path work) throws Exception {
        Files.copy(Synthetic.seedableMysqlConnector(work.resolve("mysql")), seeds.resolve("b.jar"));
        Files.copy(Synthetic.seedableMongodbConnector(work.resolve("mongo")), seeds.resolve("a.jar"));
        var started = new java.util.concurrent.CountDownLatch(2);
        io.tapstate.spi.store.ConnectorRegistry registry = new io.tapstate.spi.store.ConnectorRegistry() {
            @Override public io.tapstate.spi.store.RegistrationOutcome register(String id, String version,
                    RegistrationSource source, byte[] bytes) { throw new AssertionError("Registration must stream"); }
            @Override public io.tapstate.spi.store.RegistrationOutcome register(String id, String version,
                    RegistrationSource source, String hash, long length, java.io.InputStream bytes) throws java.io.IOException {
                started.countDown();
                try {
                    assertThat(started.await(3, java.util.concurrent.TimeUnit.SECONDS))
                            .as("independent registrations overlap instead of waiting for the first transfer").isTrue();
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.io.InterruptedIOException(); }
                assertThat(io.tapstate.spi.store.ContentHash.of(bytes)).isEqualTo(hash);
                return new io.tapstate.spi.store.RegistrationOutcome(new ConnectorRegistration(id, hash, version, source), true);
            }
            @Override public List<ConnectorRegistration> list() { return List.of(); }
            @Override public java.util.Optional<byte[]> artifact(String hash) { throw new AssertionError("Download must stream"); }
            @Override public boolean hasArtifact(String hash) { return false; }
        };
        var catalog = new io.tapstate.spi.store.ConnectorCatalogStore() {
            @Override public void upsert(io.tapstate.core.catalog.ConnectorCatalogEntry row) { }
            @Override public java.util.Optional<io.tapstate.core.catalog.ConnectorCatalogEntry> get(String id) { return java.util.Optional.empty(); }
            @Override public List<io.tapstate.core.catalog.ConnectorCatalogEntry> list() { return List.of(); }
        };
        var specs = new io.tapstate.spi.store.ConnectorSpecStore() {
            @Override public void put(String hash, byte[] bytes) { }
            @Override public java.util.Optional<byte[]> get(String hash) { return java.util.Optional.empty(); }
        };
        var registrar = new ConnectorArtifactRegistrar(registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of()), catalog, specs);

        var outcomes = new SeedConnectorSweep(registrar, 2).sweep(seeds);

        assertThat(outcomes).extracting(outcome -> outcome.artifact().getFileName().toString())
                .containsExactly("a.jar", "b.jar");
    }

    @Test
    void anExplicitSelectionNeverRegistersOrInspectsOtherSeedFiles(@TempDir Path work, @TempDir Path seedDir) throws Exception {
        Files.copy(Synthetic.seedableMongodbConnector(work), seedDir.resolve("mongodb.jar"));
        Files.writeString(seedDir.resolve("db2.jar"), "unselected unreadable artifact");
        var registry = new InMemoryConnectorRegistry();

        var outcomes = sweepOver(registry).sweep(seedDir, Set.of("mongodb.jar"));

        assertThat(outcomes).hasSize(1).allSatisfy(outcome -> assertThat(outcome).isInstanceOf(SeedOutcome.Seeded.class));
        assertThat(registry.list()).extracting(ConnectorRegistration::connectorId).containsExactly("mongodb");
    }

    @Test
    void registersEveryConnectorJarInTheSeedDirectory(@TempDir Path work, @TempDir Path seedDir) throws Exception {
        Files.copy(Synthetic.seedableMongodbConnector(work), seedDir.resolve("mongodb.jar"));
        Files.copy(Synthetic.seedableMysqlConnector(work), seedDir.resolve("mysql.jar"));
        Files.writeString(seedDir.resolve("README.txt"), "not a connector");
        Files.createDirectory(seedDir.resolve("nested.jar"));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        List<SeedOutcome> outcomes = sweepOver(registry).sweep(seedDir);

        assertThat(outcomes).hasSize(2);
        assertThat(outcomes).allSatisfy(outcome -> assertThat(outcome).isInstanceOf(SeedOutcome.Seeded.class));
        // Deterministic reporting: artifacts are swept sorted by file name.
        assertThat(outcomes.get(0).artifact().getFileName().toString()).isEqualTo("mongodb.jar");
        assertThat(outcomes.get(1).artifact().getFileName().toString()).isEqualTo("mysql.jar");
        assertThat(registry.list())
                .extracting(ConnectorRegistration::connectorId, ConnectorRegistration::pdkApiVersion,
                        ConnectorRegistration::source)
                .containsExactlyInAnyOrder(
                        tuple("mongodb", null, RegistrationSource.SEED),
                        tuple("mysql", "1.3.5", RegistrationSource.SEED));
    }

    @Test
    void refusesAConnectorOutsideTheAcceptedSetAndKeepsSweepingTheRest(@TempDir Path work, @TempDir Path seedDir)
            throws Exception {
        // A seed directory is the deployment's, not the release's: the shipped stack mounts a directory
        // the user owns and stages jars there. So the accepted set has to bound this route too, or it
        // bounds only the artifacts that happen to arrive over the wire. The refusal is one contained
        // per-jar failure — a jar the deployment may not register must not stop the ones it may.
        Files.copy(Synthetic.seedableOrdersConnector(work), seedDir.resolve("orders.jar"));
        Files.copy(Synthetic.seedableMysqlConnector(work), seedDir.resolve("zz-mysql.jar"));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        List<SeedOutcome> outcomes = sweepOver(registry).sweep(seedDir);

        assertThat(outcomes).hasSize(2);
        SeedOutcome.Failed refused = (SeedOutcome.Failed) outcomes.get(0);
        assertThat(refused.artifact().getFileName().toString()).isEqualTo("orders.jar");
        assertThat(refused.cause()).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) refused.cause()).code()).isEqualTo(ConnectorError.NOT_OFFICIAL);
        assertThat(outcomes.get(1)).isInstanceOf(SeedOutcome.Seeded.class);
        // Refused means refused: nothing of it is stored, and the accepted jar behind it still registered.
        assertThat(registry.list())
                .extracting(ConnectorRegistration::connectorId)
                .containsExactly("mysql");
    }

    @Test
    void reSweepingTheSameDirectoryIsIdempotent(@TempDir Path work, @TempDir Path seedDir) throws Exception {
        Files.copy(Synthetic.seedableMysqlConnector(work), seedDir.resolve("mysql.jar"));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        SeedConnectorSweep sweep = sweepOver(registry);

        sweep.sweep(seedDir);
        List<SeedOutcome> again = sweep.sweep(seedDir);

        assertThat(again).hasSize(1);
        SeedOutcome.Seeded seeded = (SeedOutcome.Seeded) again.get(0);
        assertThat(seeded.outcome().newlyRegistered()).isFalse();
        assertThat(registry.list()).hasSize(1);
    }

    @Test
    void aDefectiveArtifactIsReportedAndDoesNotStopTheSweep(@TempDir Path work, @TempDir Path seedDir)
            throws Exception {
        Files.copy(Synthetic.jarWithoutConnectorClass(work), seedDir.resolve("a-defective.jar"));
        Files.copy(Synthetic.seedableMysqlConnector(work), seedDir.resolve("mysql.jar"));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        List<SeedOutcome> outcomes = sweepOver(registry).sweep(seedDir);

        assertThat(outcomes).hasSize(2);
        SeedOutcome.Failed failed = (SeedOutcome.Failed) outcomes.get(0);
        assertThat(failed.cause()).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failed.cause()).code()).isEqualTo(ConnectorError.NO_CONNECTOR_CLASS);
        assertThat(outcomes.get(1)).isInstanceOf(SeedOutcome.Seeded.class);
        assertThat(registry.list()).extracting(ConnectorRegistration::connectorId).containsExactly("mysql");
    }

    @Test
    void anUnreadableArtifactIsReportedAndDoesNotStopTheSweep(@TempDir Path work, @TempDir Path seedDir)
            throws Exception {
        Files.write(seedDir.resolve("a-garbage.jar"), new byte[] {0x13, 0x37});
        Files.copy(Synthetic.seedableMysqlConnector(work), seedDir.resolve("mysql.jar"));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        List<SeedOutcome> outcomes = sweepOver(registry).sweep(seedDir);

        assertThat(outcomes).hasSize(2);
        SeedOutcome.Failed failed = (SeedOutcome.Failed) outcomes.get(0);
        assertThat(failed.cause()).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(ConnectorError.ARTIFACT_UNREADABLE);
            assertThat(error.args()).containsEntry("artifact", "a-garbage.jar");
            assertThat(error).hasCauseInstanceOf(java.util.zip.ZipException.class);
        });
        assertThat(registry.list()).extracting(ConnectorRegistration::connectorId).containsExactly("mysql");
    }

    @Test
    void sweepsJarExtensionsCaseInsensitively(@TempDir Path work, @TempDir Path seedDir) throws Exception {
        Files.copy(Synthetic.seedableMysqlConnector(work), seedDir.resolve("MYSQL.JAR"));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        List<SeedOutcome> outcomes = sweepOver(registry).sweep(seedDir);

        assertThat(outcomes).hasSize(1);
        assertThat(outcomes.get(0)).isInstanceOf(SeedOutcome.Seeded.class);
        assertThat(registry.list()).extracting(ConnectorRegistration::connectorId).containsExactly("mysql");
    }

    @Test
    void aSeedPathThatIsAFileIsRefusedNotSilentlyEmpty(@TempDir Path dir) throws Exception {
        // A misconfigured seed path pointing at a file must be loud: an empty sweep here would
        // silently drop every shipped connector.
        Path file = Files.writeString(dir.resolve("connectors"), "a stray file shadowing the seed directory");

        assertThatThrownBy(() -> sweepOver(new InMemoryConnectorRegistry()).sweep(file))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(NotDirectoryException.class);
    }

    @Test
    void aMissingSeedDirectoryIsAnEmptySweep(@TempDir Path dir) {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        List<SeedOutcome> outcomes = sweepOver(registry).sweep(dir.resolve("absent"));

        assertThat(outcomes).isEmpty();
        assertThat(registry.list()).isEmpty();
    }

    @Test
    void anEmptySeedDirectoryIsAnEmptySweep(@TempDir Path seedDir) {
        assertThat(sweepOver(new InMemoryConnectorRegistry()).sweep(seedDir)).isEmpty();
    }

    @Test
    void requiresItsCollaboratorsAndArguments() {
        assertThatNullPointerException().isThrownBy(() -> new SeedConnectorSweep(null));

        SeedConnectorSweep sweep = sweepOver(new InMemoryConnectorRegistry());
        assertThatNullPointerException().isThrownBy(() -> sweep.sweep(null));
    }

    private static SeedConnectorSweep sweepOver(InMemoryConnectorRegistry registry) {
        CapabilityDeriver deriver = id -> new ConnectorCapabilities(Set.of("batch_read_function"));
        return new SeedConnectorSweep(new ConnectorArtifactRegistrar(
                registry, new ConnectorIntrospector(), deriver,
                new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore()));
    }
}
