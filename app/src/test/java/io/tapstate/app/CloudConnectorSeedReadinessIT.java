package io.tapstate.app;

import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import java.time.Duration;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual locked PDK JARs, real local metadata, and Spring readiness; no real Cloud or database data plane. */
@RequiresDocker
class CloudConnectorSeedReadinessIT {
    private static final Set<String> IDS = Set.of(
            "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql", "db2");

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir Path work;

    @Test
    void allLockedRealArtifactsRegisterBeforeReadyAndRemainIdempotentAcrossRestart() {
        Path seeds = CloudConnectorTestInputs.seedDirectory();
        var storage = database("complete",true);
        var ready = new AtomicInteger();
        List<String> hashes;
        try (var context = builder(ready).run(arguments(storage, true, seeds))) {
            assertThat(ready.get()).isEqualTo(1);
            var business=NativeAppStorage.business(context);
            var registry = business.getBean(ConnectorRegistry.class);
            assertThat(registry.list()).extracting(registration -> registration.connectorId())
                    .containsExactlyInAnyOrderElementsOf(IDS);
            assertThat(business.getBean(ConnectorCatalogView.class).summaries())
                    .extracting(summary -> summary.id()).containsExactlyInAnyOrderElementsOf(IDS);
            hashes = registry.list().stream().map(registration -> registration.contentHash()).sorted().toList();
        }
        try (var restarted = builder(ready).run(arguments(storage, true, seeds))) {
            assertThat(ready.get()).isEqualTo(2);
            assertThat(NativeAppStorage.business(restarted).getBean(ConnectorRegistry.class).list().stream()
                    .map(registration -> registration.contentHash()).sorted()).containsExactlyElementsOf(hashes);
        }
    }

    @Test
    void aMissingCloudJarRefusesBusinessEvenThoughEveryOtherJarIsValid() throws Exception {
        Path seeds = copyRelease();
        Files.delete(seeds.resolve("mongodb-atlas-connector.jar"));
        refusesBusiness(database("missing",true), seeds);
    }

    @Test
    void aCorruptCloudJarRefusesBusinessWithoutWeakeningTheOtherLockedInputs() throws Exception {
        Path seeds = copyRelease();
        Path atlas = seeds.resolve("mongodb-atlas-connector.jar");
        // Replace only the owned copy with the damaged file. Never modify the shared input.
        Files.delete(atlas);
        Files.write(atlas, new byte[] {0x13, 0x37});
        refusesBusiness(database("corrupt",true), seeds);
    }

    @Test
    void aValidButByteChangedPluginCacheCannotBecomeReadyOnRestart() throws Exception {
        var storage = database("changed_cache",true);
        var ready = new AtomicInteger();
        Path seeds = CloudConnectorTestInputs.seedDirectory();
        String atlasHash;
        try (var context = builder(ready).run(arguments(storage, true, seeds))) {
            atlasHash = NativeAppStorage.business(context).getBean(ConnectorRegistry.class).findAll("mongodb-atlas").getFirst().contentHash();
        }
        Path cache = work.resolve("plugins").resolve(atlasHash + ".jar");
        Files.write(cache, new byte[] {0x42}, StandardOpenOption.APPEND);
        try (var stillValid = new JarFile(cache.toFile())) {
            assertThat(stillValid.getJarEntry("atlas-spec.json")).isNotNull();
        }
        ready.set(0);
        refusesBusiness(storage, seeds, ready);
    }

    @Test
    void aRegistrationConflictRefusesCloudBusinessWithoutChangingItsInstallationMode() {
        var storage = database("conflict",true);
        var ready = new AtomicInteger();
        var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,storage.cacheUri(),storage.cacheDatabase(),null);
        try(var cache=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"seed-conflict-fixture")) {
            cache.verifyBusinessReady(storage.preparation().businessScope());
            cache.connectors(storage.preparation().businessScope(),() -> {}).register(
                    "mysql", "2.0.5-SNAPSHOT", RegistrationSource.SEED, new byte[] {1, 2, 3});
        }
        refusesBusiness(storage, CloudConnectorTestInputs.seedDirectory(), ready);
    }

    @Test
    void onPremStillStartsWithMissingOrDefectiveOptionalSeeds() throws Exception {
        var ready = new AtomicInteger();
        try (var missing = builder(ready).run(arguments(database("onprem_missing",false), false, work.resolve("absent")))) {
            assertThat(NativeAppStorage.business(missing).getBean(ConnectorRegistry.class).list()).isEmpty();
            assertThat(ready.get()).isEqualTo(1);
        }
        Path defective = Files.createDirectories(work.resolve("defective"));
        Files.write(defective.resolve("garbage.jar"), new byte[] {0x13, 0x37});
        try (var damaged = builder(ready).run(arguments(database("onprem_defective",false), false, defective))) {
            assertThat(NativeAppStorage.business(damaged).getBean(ConnectorRegistry.class).list()).isEmpty();
            assertThat(ready.get()).isEqualTo(2);
        }
    }

    private void refusesBusiness(NativeAppStorage storage, Path seeds) {
        refusesBusiness(storage, seeds, new AtomicInteger());
    }

    private void refusesBusiness(NativeAppStorage storage, Path seeds, AtomicInteger ready) {
        try(var context=builder(ready).run(arguments(storage,true,seeds))) {
            var root=context.getBean(StorageRoot.class);
            assertThat(ready.get()).as("the diagnostic/auth root remains available").isEqualTo(1);
            assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
            assertThat(root.runtime().business()).isEmpty();
            // Retry the actual child assembly to retain the exact release-validation diagnosis.
            assertThatThrownBy(() -> root.runtime().startBusiness(root.runtime().current().orElseThrow(),
                    bound -> new NativeBusinessContext(root,bound,context)))
                    .isInstanceOfSatisfying(TapstateException.class,
                            coded -> assertThat(coded.code()).isEqualTo(BootError.CLOUD_CONNECTORS_INVALID));
            assertThat(root.runtime().business()).isEmpty();
            assertThat(com.hazelcast.core.Hazelcast.getAllHazelcastInstances()).isEmpty();
        }
    }

    private static SpringApplicationBuilder builder(AtomicInteger ready) {
        return new SpringApplicationBuilder(Bootstrap.class)
                .initializers(CloudConnectorTestInputs::reportCapabilityFailures).environment(CloudFixtureEnvironment.isolated())
                .listeners(event -> {
                    if (event instanceof ApplicationReadyEvent) { ready.incrementAndGet(); }
                });
    }

    private String[] arguments(NativeAppStorage storage, boolean cloud, Path seeds) {
        String uri=storage.configurationUri();
        List<String> args = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.store.mongo.uri=" + uri,
                "--tapstate.connectors.seed-dir=" + seeds,
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins")));
        if (cloud) {
            args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example.invalid",
                    "--tapstate.cloud.token=cloud-seed-fixture-token", "--tapstate.cloud.atlas-uri=" + uri,
                    "--tapstate.cloud.cluster-id="+storage.clusterId()));
        }
        return args.toArray(String[]::new);
    }

    private static NativeAppStorage database(String prefix,boolean cloud) {
        return NativeAppStorage.create(MONGO,"seed_"+prefix,cloud?StorageBinding.Mode.CLOUD:StorageBinding.Mode.OP,
                "cloud-seed-"+java.util.UUID.randomUUID());
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
                    // The release and JUnit's temporary directory can be on different filesystems.
                    Files.copy(input, destination);
                }
            }
        }
        return root.resolve("connectors");
    }
}
