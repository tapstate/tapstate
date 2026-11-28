package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.core.model.SourceResource;
import io.tapstate.e2e.Await;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.ContainerLabels;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** A retained storage installation with no administrator must reopen first-administrator setup. */
@RequiresDocker
class ARetainedInstallationReopensFirstAdministratorSetupIT {

    @TempDir Path work;

    @Test
    void publishesFirstAdministratorEntryAfterRestart() {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            ContainerLabels.configure(mongo);
            mongo.start();
            var files = retainCompletedInstallation(mongo);

            try (var context = new SpringApplicationBuilder(Bootstrap.class).run(
                    "--spring.config.location=optional:classpath:/application.properties",
                    "--tapstate.installation.require-credential=true",
                    "--tapstate.installation.directory=" + files.directory(),
                    "--tapstate.control.auth.jwt-secret=retained-installation-e2e-signing-key",
                    "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                    "--tapstate.connectors.seed-dir=" + work.resolve("seeds"),
                    "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                    "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR")) {
                var port = ((WebServerApplicationContext) context).getWebServer().getPort();
                var client = RestClient.create("http://127.0.0.1:" + port);
                awaitFirstAdministratorEntry(client);
                try (var database = MongoClients.create(mongo.getReplicaSetUrl("metadata"))) {
                    assertThat(database.getDatabase("metadata").getCollection("users").countDocuments()).isZero();
                }
            }
        }
    }

    private InstallationFiles retainCompletedInstallation(MongoDBContainer mongo) {
        var files = InstallationFiles.initialize(work.resolve("installation"));
        var config = endpoint(mongo, "metadata");
        var cache = endpoint(mongo, "cache");
        var stage = files.stage();
        var scope = stage.scope();
        var hash = OnPremInstallation.configurationHash(config);
        files.saveConnection(config.uri(), null);
        files.saveStage(stage.withHandoff(InstallationFiles.Handoff.CREATE_STARTED, hash));
        try (var metadata = MongoConfigurationBootstrap.prepare(OnPremInstallation.settings(files, new MongoProperties()),
                     scope.clusterId(), scope.installationId(), scope.operationId(), scope.generation());
                var cacheDomain = MongoCacheDomain.connect(cache, Duration.ofSeconds(5), "retained-e2e")) {
            var views = new SourceResource("views", null, "mongodb",
                    Map.of("isUri", true, "uri", mongo.getReplicaSetUrl("views")), null, null, null, null);
            var committed = metadata.bindings().commitWithFirstCache(scope, StorageBinding.Mode.OP, cache, views);
            cacheDomain.prepareFirstAuth(committed.firstCreation().orElseThrow());
            cacheDomain.prepareInitialBusiness(committed.preparation(), () -> {});
            metadata.bindings().complete(scope);
        }
        files.saveStage(stage.withHandoff(InstallationFiles.Handoff.CONFIGURED, hash));
        return files;
    }

    private static StorageBinding.Connection endpoint(MongoDBContainer mongo, String database) {
        return new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                mongo.getReplicaSetUrl(database), database, null);
    }

    private static void awaitFirstAdministratorEntry(RestClient client) {
        var entry = new AtomicReference<>();
        Await.until("first-administrator entry for the retained installation", Duration.ofSeconds(30), () -> {
            var response = client.get().uri("/auth/storage-entry").retrieve().body(Map.class);
            assertThat(response).isNotNull();
            entry.set(response.get("entry"));
            return "FIRST_ADMIN_REQUIRED".equals(entry.get());
        }, () -> String.valueOf(entry.get()));
    }
}
