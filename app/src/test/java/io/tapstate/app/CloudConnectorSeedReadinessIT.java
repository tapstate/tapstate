package io.tapstate.app;

import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.jar.JarFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual locked PDK JARs, real local metadata, and Spring readiness; no real Cloud or database data plane. */
@RequiresDocker
class CloudConnectorSeedReadinessIT {
    private static final Set<String> IDS = Set.of(
            "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql");

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir Path work;

    @Test
    void allSevenRealArtifactsRegisterBeforeReadyAndRemainIdempotentAcrossRestart() {
        Path seeds = CloudConnectorTestInputs.seedDirectory();
        String uri = database("complete");
        var ready = new AtomicInteger();
        List<String> hashes;
        try (var context = builder(ready).run(arguments(uri, true, seeds))) {
            assertThat(ready.get()).isEqualTo(1);
            var registry = context.getBean(ConnectorRegistry.class);
            assertThat(registry.list()).extracting(registration -> registration.connectorId())
                    .containsExactlyInAnyOrderElementsOf(IDS);
            assertThat(context.getBean(ConnectorCatalogView.class).summaries())
                    .extracting(summary -> summary.id()).containsExactlyInAnyOrderElementsOf(IDS);
            hashes = registry.list().stream().map(registration -> registration.contentHash()).sorted().toList();
        }
        try (var restarted = builder(ready).run(arguments(uri, true, seeds))) {
            assertThat(ready.get()).isEqualTo(2);
            assertThat(restarted.getBean(ConnectorRegistry.class).list().stream()
                    .map(registration -> registration.contentHash()).sorted()).containsExactlyElementsOf(hashes);
        }
    }

    @Test
    void aMissingCloudJarRefusesReadyEvenThoughEveryOtherJarIsValid() throws Exception {
        Path seeds = copyRelease();
        Files.delete(seeds.resolve("mongodb-atlas-connector.jar"));
        refusesReady(database("missing"), seeds);
    }

    @Test
    void aCorruptCloudJarRefusesReadyWithoutWeakeningTheOtherSixInputs() throws Exception {
        Path seeds = copyRelease();
        Path atlas = seeds.resolve("mongodb-atlas-connector.jar");
        // Break only the owned hard link, then create the damaged file. Never modify the shared input.
        Files.delete(atlas);
        Files.write(atlas, new byte[] {0x13, 0x37});
        refusesReady(database("corrupt"), seeds);
    }

    @Test
    void aValidButByteChangedPluginCacheCannotBecomeReadyOnRestart() throws Exception {
        String uri = database("changed_cache");
        var ready = new AtomicInteger();
        Path seeds = CloudConnectorTestInputs.seedDirectory();
        String atlasHash;
        try (var context = builder(ready).run(arguments(uri, true, seeds))) {
            atlasHash = context.getBean(ConnectorRegistry.class).findAll("mongodb-atlas").getFirst().contentHash();
        }
        Path cache = work.resolve("plugins").resolve(atlasHash + ".jar");
        Files.write(cache, new byte[] {0x42}, StandardOpenOption.APPEND);
        try (var stillValid = new JarFile(cache.toFile())) {
            assertThat(stillValid.getJarEntry("atlas-spec.json")).isNotNull();
        }
        ready.set(0);
        refusesReady(uri, seeds, ready);
    }

    @Test
    void aRegistrationConflictIsFatalForCloudButDoesNotEmitApplicationReady() {
        String uri = database("conflict");
        var ready = new AtomicInteger();
        try (var onPrem = builder(ready).run(arguments(uri, false, work.resolve("optional-seeds")))) {
            onPrem.getBean(ConnectorRegistry.class).register(
                    "mysql", "2.0.5-SNAPSHOT", RegistrationSource.SEED, new byte[] {1, 2, 3});
        }
        ready.set(0);
        refusesReady(uri, CloudConnectorTestInputs.seedDirectory(), ready);
    }

    @Test
    void onPremStillStartsWithMissingOrDefectiveOptionalSeeds() throws Exception {
        var ready = new AtomicInteger();
        try (var missing = builder(ready).run(arguments(database("onprem_missing"), false, work.resolve("absent")))) {
            assertThat(missing.getBean(ConnectorRegistry.class).list()).isEmpty();
            assertThat(ready.get()).isEqualTo(1);
        }
        Path defective = Files.createDirectories(work.resolve("defective"));
        Files.write(defective.resolve("garbage.jar"), new byte[] {0x13, 0x37});
        try (var damaged = builder(ready).run(arguments(database("onprem_defective"), false, defective))) {
            assertThat(damaged.getBean(ConnectorRegistry.class).list()).isEmpty();
            assertThat(ready.get()).isEqualTo(2);
        }
    }

    private void refusesReady(String uri, Path seeds) {
        refusesReady(uri, seeds, new AtomicInteger());
    }

    private void refusesReady(String uri, Path seeds, AtomicInteger ready) {
        assertThatThrownBy(() -> {
            try (var unexpected = builder(ready).run(arguments(uri, true, seeds))) {
                throw new AssertionError("Cloud started without its required connector release");
            }
        }).satisfies(failure -> {
            Throwable cause = failure;
            while (!(cause instanceof TapstateException) && cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertThat(cause).isInstanceOfSatisfying(TapstateException.class,
                    coded -> assertThat(coded.code()).isEqualTo(BootError.CLOUD_CONNECTORS_INVALID));
        });
        assertThat(ready.get()).as("failed seed startup never emits ApplicationReadyEvent").isZero();
    }

    private static SpringApplicationBuilder builder(AtomicInteger ready) {
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .listeners(event -> {
                    if (event instanceof ApplicationReadyEvent) { ready.incrementAndGet(); }
                });
    }

    private String[] arguments(String uri, boolean cloud, Path seeds) {
        List<String> args = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.store.mongo.uri=" + uri,
                "--tapstate.store.mongo.operator-state-database=seed_ops_" + Long.toUnsignedString(System.nanoTime(), 16),
                "--tapstate.connectors.seed-dir=" + seeds,
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins")));
        if (cloud) {
            args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example.invalid",
                    "--tapstate.cloud.token=cloud-seed-fixture-token", "--tapstate.cloud.atlas-uri=" + uri,
                    "--tapstate.cloud.cluster-id=cloud-seed-cluster"));
        }
        return args.toArray(String[]::new);
    }

    private static String database(String prefix) {
        return MONGO.getReplicaSetUrl("seed_" + prefix + "_" + Long.toUnsignedString(System.nanoTime(), 16));
    }

    private Path copyRelease() throws IOException {
        Path original = CloudConnectorTestInputs.seedDirectory().getParent();
        Path root = Files.createDirectory(work.resolve("release-copy"));
        try (var files = Files.walk(original)) {
            for (Path input : files.toList()) {
                Path destination = root.resolve(original.relativize(input));
                if (Files.isDirectory(input)) {
                    Files.createDirectories(destination);
                } else {
                    Files.createLink(destination, input);
                }
            }
        }
        return root.resolve("connectors");
    }
}
