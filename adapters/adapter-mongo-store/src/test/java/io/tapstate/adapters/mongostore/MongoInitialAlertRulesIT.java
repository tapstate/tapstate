package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoInitialAlertRulesIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @ParameterizedTest
    @EnumSource(value = AlertChannelPolicy.Deployment.class, names = {"ON_PREM", "CLOUD_MANAGED"})
    void creationUsesCatalogDefaultsAndRetryNeverRepairsOrOverwritesRules(AlertChannelPolicy.Deployment deployment) {
        String database = "initial_rules_" + UUID.randomUUID().toString().replace("-", "");
        var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database), null, Duration.ofSeconds(5));
        var scope = new AlarmKey.Scope(deployment == AlertChannelPolicy.Deployment.ON_PREM ? "installation" : "organization", "cluster");
        var catalog = BuiltinAlarmTypes.catalog();
        var expectedNames = java.util.Map.of(
                "pipeline.failed", "Pipeline execution failed",
                "pipeline.observation-stale", "Pipeline observation stale",
                "source.connection-failed", "Source connection failed",
                "cluster.node-unavailable", "Cluster node unavailable",
                "cluster.storage-unavailable", "Required storage unavailable");
        try (var observer = MongoClients.create(settings.uri())) {
            var raw = observer.getDatabase(database);
            try (var configuration = MongoConfigurationBootstrap.prepareWithAlerts(settings, "cluster", "installation", "setup", 1, scope, deployment)) {
                var rows = SystemCollections.ALERT_RULES.on(raw).find().into(new ArrayList<>());
                assertThat(rows).hasSize(5);
                assertThat(rows.stream().mapToInt(MongoAlertConfigurationCodec::size).sum()).isLessThan(16 * 1024);
                for (var row : rows) {
                    String id = row.get("_id", Document.class).getString("id");
                    var rule = (Rule) MongoAlertConfigurationCodec.read(Kind.RULE, scope, id, row).definition();
                    var type = catalog.types().stream().filter(value -> value.id().equals(rule.typeId())).findFirst().orElseThrow();
                    assertThat(rule.name()).isEqualTo(expectedNames.get(rule.typeId()));
                    assertThat(rule.header().identity().id()).isEqualTo("builtin-" + rule.typeId());
                    assertThat(rule.header().deployment()).isEqualTo(deployment);
                    assertThat(rule.bindingRevision()).isEqualTo(1L);
                    assertThat(rule.parameters()).isEqualTo(type.validatedParameters(java.util.Map.of()));
                    assertThat(rule.enabled()).isEqualTo(type.enabledByDefault());
                    assertThat(rule.severityOverride()).isNull();
                    assertThat(rule.effectiveSeverity(catalog)).isEqualTo(type.defaultSeverity());
                    assertThat(rule.resources()).isEqualTo(new Resources(type.resourceKinds(), true, java.util.Set.of()));
                }
                var store = configuration.alertDefinitions(scope, catalog);
                var first = (Rule) MongoAlertConfigurationCodec.read(Kind.RULE, scope,
                        rows.getFirst().get("_id", Document.class).getString("id"), rows.getFirst()).definition();
                var changed = new Rule(first.header().next(first.header().createdAt().plusSeconds(1), "administrator"),
                        "Edited and disabled", first.typeId(), first.typeVersion(), first.resources(), first.parameters(),
                        AlarmSeverity.MINOR, false, first.metric()).enrolled(first);
                configuration.alertTransaction(new MongoReadBudget(Duration.ofSeconds(5)), session -> store.stage(session, first, changed));
                var second = (Rule) MongoAlertConfigurationCodec.read(Kind.RULE, scope,
                        rows.get(1).get("_id", Document.class).getString("id"), rows.get(1)).definition();
                var retired = new Rule(second.header().retire(second.header().createdAt().plusSeconds(1), "administrator"),
                        second.name(), second.typeId(), second.typeVersion(), second.resources(), second.parameters(),
                        second.severityOverride(), second.enabled(), second.metric()).enrolled(second);
                configuration.alertTransaction(new MongoReadBudget(Duration.ofSeconds(5)), session -> store.stage(session, second, retired));
                SystemCollections.ALERT_RULES.on(raw).deleteOne(new Document("_id", rows.get(2).get("_id")));
            }
            List<Document> retained = SystemCollections.ALERT_RULES.on(raw).find().into(new ArrayList<>());
            for (int attempt = 0; attempt < 3; attempt++) {
                try (var retry = MongoConfigurationBootstrap.prepareWithAlerts(settings, "cluster", "installation", "setup", 1, scope, deployment);
                        var resumed = MongoConfigurationBootstrap.resume(settings, "cluster", "installation", "setup", 1);
                        var reopened = MongoConfigurationDomain.open(settings, "cluster")) {
                    assertThat(SystemCollections.ALERT_RULES.on(raw).find().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(retained);
                }
            }
        } finally {
            try (var observer = MongoClients.create(settings.uri())) { observer.getDatabase(database).drop(); }
        }
    }
}
