package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** The original native node writes expectation before monitoring, then intent before an orderly whole-app close. */
@RequiresDocker
class NativeNodeLifecycleIT {
    @Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @TempDir Path work;
    @Test void aNodeWithoutAnyRuleRetainsItsOwnOrderlyStopAcrossReopen() {
        exercise(false);
    }
    @Test void aFailedBusinessOwnerCannotTurnItsTeardownIntoAPlannedStop() {
        exercise(true);
    }
    private void exercise(boolean failClosed) {
        var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"native_node", "node-admin", "isolated-node-password");
        try(var observer=MongoClients.create(storage.configurationUri())) {
            var db=observer.getDatabase(storage.configurationDatabase());
            var rows=db.getCollection("alert_node_lifecycles");
            try(var application=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR",
                    "--tapstate.store.mongo.uri="+storage.configurationUri(),"--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=2","--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),
                    "--tapstate.connectors.plugins-dir="+work.resolve("plugins"),"--tapstate.control.auth.jwt-secret=isolated-node-test-signing")) {
                NativeAppStorage.awaitBusiness(application);
                await().atMost(Duration.ofSeconds(15)).untilAsserted(()->assertThat(rows.countDocuments()).isOne());
                var original=rows.find().first();assertThat(original).containsEntry("phase","EXPECTED");
                assertThat(db.getCollection("alert_rules").countDocuments()).isZero();
                assertThat(db.getCollection("alert_instances").countDocuments()).isZero();
                if(failClosed) {
                    var root=application.getBean(StorageRoot.class);
                    root.runtime().business().orElseThrow().close();
                    assertThat(rows.find().first()).containsEntry("phase","EXPECTED");
                }
            } finally {
                var retained=rows.find().first();assertThat(retained).isNotNull();
                if(failClosed)assertThat(retained).containsEntry("phase","EXPECTED").doesNotContainKey("commandId");
                else assertThat(retained).containsEntry("phase","RETIRING").containsEntry("reason","ORDERLY_SHUTDOWN");
                try(var domain=io.tapstate.adapters.mongostore.MongoConfigurationDomain.open(
                        new io.tapstate.adapters.mongostore.MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5)),storage.clusterId())) {
                    assertThat(domain.alertSchemaVersion()).isEqualTo(7);
                    assertThat(rows.find().first()).isEqualTo(retained);
                }
                observer.getDatabase(storage.configurationDatabase()).drop();observer.getDatabase(storage.cacheDatabase()).drop();observer.getDatabase(storage.masterDatabase()).drop();
            }
        }
    }
}
