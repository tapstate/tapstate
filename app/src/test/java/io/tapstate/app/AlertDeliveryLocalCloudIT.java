package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

/** Explicit local dependency witness. An absent owned environment is never reported as a passed integration. */
@RequiresDocker
@EnabledIfSystemProperty(named="tapstate.test.alert-cloud-local-root",matches=".+")
class AlertDeliveryLocalCloudIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void actualLocalControlPlaneAuthenticatesStoresDeduplicatesEvaluatesAndDelivers()throws Exception {
        var root=Path.of(System.getProperty("tapstate.test.alert-cloud-local-root"));var json=new com.fasterxml.jackson.databind.ObjectMapper();
        var launch=json.readTree(root.resolve("launch.json").toFile());
        assertThat(launch.path("source").asText()).isEqualTo("650082bdf4180d6e5da679c5040522d4855ff6a1");
        var origin=URI.create("https://localhost:"+launch.path("cloudPort").asInt());String org="112345678901234567890123",cluster="012345678901234567890123";
        var ssl=AlertChannels.trust(Files.readAllBytes(root.resolve("tls-cert.pem")));
        try(var mongo=MongoClients.create("mongodb://127.0.0.1:"+launch.path("mongoPort").asInt()+"/?directConnection=true&replicaSet=rs0")){
            String database=launch.path("database").asText();assertThat(database).startsWith("w4_alert_t6_control_20261028");
            var db=mongo.getDatabase(database);var events=db.getCollection("events");var alerts=db.getCollection("alerts");var deliveries=db.getCollection("notification_deliveries");
            assertThat(events.countDocuments()).isZero();assertThat(deliveries.countDocuments()).isZero();
            var policy=db.getCollection("alert_policies");var policyId=new Document("_id",new ObjectId("312345678901234567890123"));
            assertThat(policy.find(policyId).first().getList("recipients",String.class)).containsExactly("fixture@alert.invalid");
            long smtpBaseline=Files.exists(root.resolve("smtp-receipts.jsonl"))?Files.readAllLines(root.resolve("smtp-receipts.jsonl")).size():0;
            var evidence=new LinkedHashMap<String,Object>();var remoteIds=new ArrayList<String>();
            AlertEvaluationHostIT.withFixture(false,true,new AlarmKey.Scope(org,cluster),f->{
                var properties=new CloudProperties();properties.setBaseUrl(origin.toString());properties.setToken("isolated-machine-token");properties.setAtlasUri("mongodb://config.invalid/config");properties.setClusterId(cluster);var cloud=CloudRuntimeSettings.resolve(properties);
                var planner=new AlertNotificationPlanner(f.store(),f.clock(),"local-cloud-planner",origin,ACTIVE);f.evaluator().withPlanner(planner);bad(f);planner.tick();
                var store=f.configuration().alerts(f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("cloud-sdk",2,AlertChannelPolicy.Family.CLOUD_SDK))),BuiltinAlarmTypes.catalog(),f.clock());
                var factory=new AlertChannels(AlertChannelPolicy.Deployment.CLOUD_MANAGED,cloud,f.clock(),ACTIVE,org,host->new InetAddress[]{InetAddress.getLoopbackAddress()},ssl);
                try(var worker=new AlertDeliveryRuntime(store,f.clock(),"local-cloud-sender",ACTIVE,factory,a->{throw new AssertionError("managed test authority");})){
                    var original=f.store().dueIntents(T.plusSeconds(1),null,10).rows().getFirst().value();worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                    var first=store.intent(original.frozen().identity(),null).orElseThrow().value();assertThat(first.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);String eventId=first.state().acceptedReceipt().remoteId();remoteIds.add(eventId);
                    assertThat(events.find(new Document("_id",new ObjectId(eventId))).first().getObjectId("orgId").toHexString()).isEqualTo(org);
                    await().atMost(Duration.ofSeconds(15)).until(()->deliveries.countDocuments(new Document("status","SENT"))==1);
                    assertThat(deliveries.countDocuments()).isOne();assertThat(alerts.countDocuments()).isOne();
                    // This explicit receiver-side dedup probe is separate from the production worker's single attempt.
                    var destination=AlertDeliveryRuntime.destination(first.frozen());
                    try(var probe=new CloudAlertSdkChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED,destination,origin,"isolated-machine-token",org,new InetAddress[]{InetAddress.getLoopbackAddress()},ssl,f.clock())){
                        var replay=probe.send(AlertDeliveryRuntime.request(first,first.state().attempts().getFirst().permit()));
                        assertThat(replay.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(replay.receipt().remoteId()).isEqualTo(eventId);
                    }catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
                    assertThat(events.countDocuments()).isOne();assertThat(alerts.countDocuments()).isOne();assertThat(deliveries.countDocuments()).isOne();
                    policy.updateOne(policyId,new Document("$set",new Document("eventTypes",List.of("cluster.never.*"))));
                    f.now().set(T.plusSeconds(1));recover(f);planner.tick();worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                    assertThat(events.countDocuments()).isEqualTo(2);assertThat(alerts.countDocuments()).isOne();assertThat(deliveries.countDocuments()).isOne();
                    var utc=LocalTime.ofInstant(T.plusSeconds(2),ZoneOffset.UTC);String start=utc.minusHours(1).withSecond(0).withNano(0).toString(),end=utc.plusHours(1).withSecond(0).withNano(0).toString();
                    policy.updateOne(policyId,new Document("$set",new Document("eventTypes",List.of("cluster.alert.*")).append("quietHoursStart",start).append("quietHoursEnd",end)));
                    f.now().set(T.plusSeconds(2));assertThat(f.evaluator().accept(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)),3,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
                    planner.tick();worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                    assertThat(events.countDocuments()).isEqualTo(3);assertThat(alerts.countDocuments(new Document("status","SUPPRESSED"))).isOne();assertThat(deliveries.countDocuments()).isOne();
                    assertThat(worker.occupied()).isZero();
                    evidence.put("clusterWorkerAttemptsForFirstIntent",first.state().attemptsConsumed());evidence.put("acceptedSource",first.state().acceptedReceipt().source());
                }
            });
            try(var client=HttpClient.newBuilder().sslContext(ssl).followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(3)).build()){
                var denied=client.send(HttpRequest.newBuilder(origin.resolve("/v1/api/clusters/ffffffffffffffffffffffff/events")).timeout(Duration.ofSeconds(5)).header("Authorization","Bearer isolated-machine-token").header("Content-Type","application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"cluster.alert.test\",\"subject\":\"isolated invalid scope\",\"severity\":\"INFO\",\"dedupeKey\":\"wrong-scope\"}")).build(),HttpResponse.BodyHandlers.discarding());
                assertThat(denied.statusCode()).isEqualTo(401);
            }
            assertThat(Files.readAllLines(root.resolve("smtp-receipts.jsonl"))).hasSize((int)smtpBaseline+1);
            evidence.put("smtpReceiptsBeforeRun",smtpBaseline);
            evidence.put("controlPlaneSource",launch.path("source").asText());evidence.put("events",events.countDocuments());evidence.put("alerts",alerts.countDocuments());evidence.put("suppressedAlerts",alerts.countDocuments(new Document("status","SUPPRESSED")));evidence.put("sentDeliveries",deliveries.countDocuments(new Document("status","SENT")));evidence.put("smtpReceipts",1);evidence.put("firstRemoteEventIds",remoteIds);
            evidence.put("scope","isolated local real control-plane/SDK/CONFIG/SMTP; no human inbox or full retained Cluster runtime acceptance");
            json.writerWithDefaultPrettyPrinter().writeValue(root.resolve("verification.json").toFile(),evidence);
        }
    }
}
