package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.BuiltinAlarmTypes;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RequiresDocker
class CloudInitialAlertRulesIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test void aNewCloudConfigurationAlreadyContainsTheFiveEnabledScopedRules() {
        String database = "cloud_initial_alerts_" + UUID.randomUUID().toString().replace("-", "");
        var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database), null, Duration.ofSeconds(5));
        var properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example.test"); properties.setToken("fixture-machine-token");
        properties.setClusterId("initial-alert-cluster"); properties.setAtlasUri(settings.uri());
        var cloud = CloudRuntimeSettings.resolve(properties);
        var bridge = mock(CloudSdkBridge.class);
        var scope = new AlarmKey.Scope("initial-alert-organization", cloud.clusterId());
        when(bridge.alertScope()).thenReturn(java.util.Optional.of(scope));
        when(bridge.claimConfiguration(anyString(), anyString())).thenAnswer(call ->
                new CloudSdkBridge.ConfigurationInitialization(cloud.clusterId(), "a9c7faf0-c2a4-437a-a698-767c515e37ec", "CLAIMED", call.getArgument(1), true));
        try (var initializer = new CloudConfigurationInitialization(settings, cloud, bridge, BootId.fresh());
                var configuration = initializer.open(); var observer = MongoClients.create(settings.uri())) {
            var rows = observer.getDatabase(database).getCollection("alert_rules").find().into(new ArrayList<>());
            assertThat(rows).hasSize(5);
            assertThat(rows).extracting(row -> row.get("body", Document.class).getString("typeId"))
                    .containsExactlyInAnyOrderElementsOf(BuiltinAlarmTypes.catalog().types().stream().map(type -> type.id()).toList());
            for (var row : rows) {
                assertThat(row.get("_id", Document.class)).containsEntry("authorityId", scope.authorityId()).containsEntry("clusterId", scope.clusterId());
                assertThat(row.getString("deployment")).isEqualTo("CLOUD_MANAGED");
                assertThat(row.get("body", Document.class)).containsEntry("enabled", true);
                assertThat(row.getLong("recordRevision")).isEqualTo(1L);
            }
        } finally {
            try (var observer = MongoClients.create(settings.uri())) { observer.getDatabase(database).drop(); }
        }
    }
}
