package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateViewOptions;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.StorageSetupService;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertChannelPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RequiresDocker
class OnPremInstallationBootstrapIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @TempDir Path temporary;

    @Test void aLoadedConfigurationFinishesItsRetainedFeaturePreparationBeforeHandoff() {
        withRetainedPreparation(f -> {
            var rules = SystemCollections.ALERT_RULES.on(f.database()).find().into(new ArrayList<>());
            var marker = alertMarker(f.database());
            f.installation().resumePreparation();
            assertThat(f.configuration().alertSchemaVersion()).isEqualTo(7);
            assertThat(f.files().stage().handoff()).isEqualTo(InstallationFiles.Handoff.CONFIGURED);
            assertThat(alertMarker(f.database())).containsEntry("operationId", marker.get("operationId"))
                    .containsEntry("authorityPreparationOperationId", marker.get("authorityPreparationOperationId"));
            assertThat(SystemCollections.ALERT_RULES.on(f.database()).find().into(new ArrayList<>())).isEqualTo(rules);
            assertThat(f.database().getCollection("unrelated_user_data").find().first()).containsEntry("retained", true);
            var completed = alertMarker(f.database());
            f.installation().resumePreparation();
            assertThat(alertMarker(f.database())).isEqualTo(completed);
        });
    }

    @Test void damagedFeatureResourcesCannotCompleteTheOriginalHandoff() {
        withRetainedPreparation(f -> {
            f.database().createView(SystemCollections.ALERT_NODE_LIFECYCLES.collectionName(), "unrelated_user_data",
                    List.of(), new CreateViewOptions());
            var original = alertMarker(f.database());
            assertThatThrownBy(f.installation()::resumePreparation).isInstanceOf(TapstateException.class);
            assertThat(f.files().stage().handoff()).isEqualTo(InstallationFiles.Handoff.CREATE_STARTED);
            assertThat(alertMarker(f.database())).isEqualTo(original);
        });
    }

    @Test void aConfiguredInstallationDoesNotPrepareFeatureStorageDuringOrdinaryResume() {
        withRetainedPreparation(f -> {
            var original = alertMarker(f.database());
            f.files().saveStage(f.files().stage().withHandoff(InstallationFiles.Handoff.CONFIGURED,
                    f.files().stage().configurationHash()));
            f.installation().resumePreparation();
            assertThat(alertMarker(f.database())).isEqualTo(original);
            assertThat(f.configuration().alertSchemaVersion()).isEqualTo(4);
            var operation = f.files().stage().scope().operationId();
            assertThat(f.configuration().prepareAlertStorage(operation)).isEqualTo(7);
            var completed = alertMarker(f.database());
            assertThat(f.configuration().prepareAlertStorage(operation)).isEqualTo(7);
            assertThat(alertMarker(f.database())).isEqualTo(completed);
        });
    }

    private void withRetainedPreparation(Consumer<Fixture> action) {
        String name = "installer_bootstrap_" + UUID.randomUUID().toString().replace("-", "");
        var files = InstallationFiles.initialize(temporary.resolve(name));
        var scope = files.stage().scope();
        var uri = MONGO.getReplicaSetUrl(name);
        var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri, name, null);
        var settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
        files.saveConnection(uri, null);
        files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CREATE_STARTED,
                OnPremInstallation.configurationHash(connection)));
        try (var client = MongoClients.create(uri)) {
            var database = client.getDatabase(name);
            try {
                try (var prepared = MongoConfigurationBootstrap.prepareWithAlerts(settings, scope.clusterId(), scope.installationId(),
                        scope.operationId(), scope.generation(), new AlarmKey.Scope(scope.installationId(), scope.clusterId()),
                        AlertChannelPolicy.Deployment.ON_PREM)) {
                    assertThat(prepared.alertSchemaVersion()).isEqualTo(7);
                }
                // The supported intermediate capability retained before node feature preparation commits.
                var marker = alertMarker(database);
                marker.put("installedVersion", 4);
                marker.put("queryPreparation", new Document("operationId", scope.operationId()).append("phase", "READY")
                        .append("nextIndex", 5).append("indexContractVersion", 1));
                SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(new Document("_id", "config"),
                        new Document("$set", new Document("alerting", marker)));
                SystemCollections.ALERT_NODE_LIFECYCLES.on(database).drop();
                database.getCollection("unrelated_user_data").insertOne(new Document("_id", "retained").append("retained", true));
                try (var configuration = MongoConfigurationDomain.openExisting(settings)) {
                    assertThat(configuration.alertSchemaVersion()).isEqualTo(4);
                    var root = mock(StorageRoot.class);
                    var runtime = mock(StorageRuntime.class);
                    when(root.mode()).thenReturn(StorageBinding.Mode.OP);
                    when(root.runtime()).thenReturn(runtime);
                    when(runtime.configuration()).thenReturn(Optional.of(configuration));
                    when(runtime.requireConfiguration()).thenReturn(configuration);
                    try (var installation = new OnPremInstallation(root, files, new MongoProperties(),
                            mock(StorageSetupService.DefaultTarget.class), Clock.systemUTC())) {
                        action.accept(new Fixture(installation, files, configuration, database));
                    }
                }
            } finally { database.drop(); }
        }
    }

    private static Document alertMarker(MongoDatabase database) {
        return new Document(SystemCollections.SCHEMA_MIGRATIONS.on(database).find(new Document("_id", "config"))
                .first().get("alerting", Document.class));
    }

    private record Fixture(OnPremInstallation installation, InstallationFiles files,
            MongoConfigurationDomain configuration, MongoDatabase database) {}
}
