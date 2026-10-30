package io.tapstate.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.alerting.NotificationChannel;
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

/** Opt-in protocol and policy witness against an existing, isolated local control plane. */
@EnabledIfSystemProperty(named="tapstate.test.alert-cloud-matrix-root",matches=".+")
class AlertCloudEventMatrixIT {
    static final ObjectMapper JSON=new ObjectMapper();
    static final String SOURCE="650082bdf4180d6e5da679c5040522d4855ff6a1";
    @Test void fiveTypesAndThreeNotificationKindsKeepClusterIdentityAndPolicyResultsSeparate()throws Exception {
        var root=Path.of(System.getProperty("tapstate.test.alert-cloud-matrix-root"));
        var launch=JSON.readTree(root.resolve("launch.json").toFile());
        assertThat(launch.path("source").asText()).isEqualTo(SOURCE);
        String database=launch.path("database").asText();assertThat(database).startsWith("w4_alert_t10_control_");
        var origin=URI.create("https://localhost:"+launch.path("cloudPort").asInt());
        var ssl=AlertChannels.trust(Files.readAllBytes(root.resolve("tls-cert.pem")));
        var org=new ObjectId();var clusters=List.of(new ObjectId(),new ObjectId());
        var policyId=new ObjectId();var now=Instant.now();var ids=new HashSet<String>();var keys=new HashSet<String>();
        var evidence=new ArrayList<Map<String,Object>>();var controlIds=new ArrayList<String>();long maxCallNanos=0;
        long smtpBefore=Files.exists(root.resolve("smtp-receipts.jsonl"))?Files.readAllLines(root.resolve("smtp-receipts.jsonl")).size():0;
        try(var mongo=MongoClients.create("mongodb://127.0.0.1:"+launch.path("mongoPort").asInt()+"/?directConnection=true&replicaSet=rs0")){
            var db=mongo.getDatabase(database);var events=db.getCollection("events");var alerts=db.getCollection("alerts");var deliveries=db.getCollection("notification_deliveries");
            assertThat(events.countDocuments()).isZero();assertThat(deliveries.countDocuments()).isZero();
            for(int i=0;i<clusters.size();i++)db.getCollection("clusters").insertOne(new Document("_id",clusters.get(i)).append("orgId",org)
                    .append("name","matrix-"+i).append("dnsLabel","matrix-"+clusters.get(i).toHexString()).append("region","isolated")
                    .append("state","READY").append("sdkStaticToken","isolated-matrix-token-"+i).append("createdAt",Date.from(now)).append("updatedAt",Date.from(now))
                    .append("_class","io.tapstate.cloud.cluster.Cluster"));
            var policy=db.getCollection("alert_policies");policy.insertOne(new Document("_id",policyId).append("orgId",org).append("name","Isolated matrix")
                    .append("enabled",true).append("eventTypes",List.of("cluster.alert.*")).append("minSeverity","INFO").append("recipients",List.of("fixture@alert.invalid"))
                    .append("channel","EMAIL").append("cooldownSeconds",0L).append("timezone","UTC").append("createdAt",Date.from(now)).append("updatedAt",Date.from(now)));
            for(int clusterIndex=0;clusterIndex<clusters.size();clusterIndex++){
                String cluster=clusters.get(clusterIndex).toHexString();
                var destination=new NotificationChannel.Destination(new AlarmKey.Scope(org.toHexString(),cluster),"cloud-sdk","managed-target","logical-target","binding-a");
                try(var channel=new CloudAlertSdkChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED,destination,origin,"isolated-matrix-token-"+clusterIndex,org.toHexString(),
                        new InetAddress[]{InetAddress.getLoopbackAddress()},ssl,Clock.systemUTC())){
                    for(var type:BuiltinAlarmTypes.catalog().types())for(var kind:List.of(DeliveryKey.Kind.FAULT,DeliveryKey.Kind.REMINDER,DeliveryKey.Kind.RECOVERY)){
                        var request=request(origin,destination,type,kind,kind==DeliveryKey.Kind.REMINDER?2:1,now);
                        long start=System.nanoTime();var accepted=channel.send(request);maxCallNanos=Math.max(maxCallNanos,System.nanoTime()-start);
                        assertThat(accepted.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(accepted.implementation()).isEqualTo("sdk-event-center");
                        assertThat(ids.add(accepted.receipt().remoteId())).isTrue();
                        var stored=events.find(new Document("_id",new ObjectId(accepted.receipt().remoteId()))).first();assertThat(stored).isNotNull();
                        assertThat(stored.getObjectId("orgId")).isEqualTo(org);assertThat(stored.getString("source")).isEqualTo("cluster");
                        var payload=stored.get("payload",Document.class);assertThat(payload.getString("clusterId")).isEqualTo(cluster);
                        assertThat(payload.getString("alarmTypeId")).isEqualTo(type.id());assertThat(payload.getString("notificationKind")).isEqualTo(kind.name().toLowerCase(Locale.ROOT));
                        assertThat(stored.getString("severity")).isEqualTo(CloudAlertEventWire.cloudSeverity(kind,type.defaultSeverity()));
                        assertThat(keys.add(stored.getString("dedupeKey"))).isTrue();
                        // A receiver-side replay probe is not an extra automatic worker attempt.
                        var duplicate=channel.send(request);assertThat(duplicate.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                        assertThat(duplicate.receipt().remoteId()).isEqualTo(accepted.receipt().remoteId());assertThat(channel.activeCalls()).isZero();
                        evidence.add(new LinkedHashMap<>(Map.of("cluster",cluster,"type",type.id(),"kind",kind.name(),"eventId",accepted.receipt().remoteId(),"dedupeKey",stored.getString("dedupeKey"))));
                    }
                    if(clusterIndex==0){
                        var type=BuiltinAlarmTypes.catalog().types().getFirst();
                        policy.updateOne(new Document("_id",policyId),new Document("$set",new Document("eventTypes",List.of("cluster.never.*"))));
                        var unmatched=request(origin,destination,type,DeliveryKey.Kind.FAULT,90,now);var unmatchedResult=channel.send(unmatched);
                        assertThat(unmatchedResult.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);controlIds.add(unmatchedResult.receipt().remoteId());
                        assertThat(alerts.countDocuments(new Document("eventId",new ObjectId(controlIds.getFirst())))).isZero();
                        policy.updateOne(new Document("_id",policyId),new Document("$set",new Document("eventTypes",List.of("cluster.alert.*"))));
                        // Making the already stored event eligible must not reevaluate policy on replay.
                        var replay=channel.send(unmatched);assertThat(replay.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                        assertThat(replay.receipt().remoteId()).isEqualTo(controlIds.getFirst());
                        assertThat(alerts.countDocuments(new Document("eventId",new ObjectId(controlIds.getFirst())))).isZero();
                        var utc=LocalTime.now(ZoneOffset.UTC);
                        policy.updateOne(new Document("_id",policyId),new Document("$set",new Document("eventTypes",List.of("cluster.alert.*"))
                                .append("quietHoursStart",utc.minusHours(1).withSecond(0).withNano(0).toString()).append("quietHoursEnd",utc.plusHours(1).withSecond(0).withNano(0).toString())));
                        var quiet=channel.send(request(origin,destination,type,DeliveryKey.Kind.FAULT,91,now));
                        assertThat(quiet.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);controlIds.add(quiet.receipt().remoteId());
                        policy.updateOne(new Document("_id",policyId),new Document("$unset",new Document("quietHoursStart","").append("quietHoursEnd","")));
                    }
                }
            }
            assertThat(events.countDocuments()).isEqualTo(32);assertThat(alerts.countDocuments()).isEqualTo(31);
            assertThat(alerts.countDocuments(new Document("status","SUPPRESSED"))).isOne();
            await().atMost(Duration.ofSeconds(30)).until(()->deliveries.countDocuments(new Document("status","SENT"))==30);
            assertThat(deliveries.countDocuments()).isEqualTo(30);
            var smtp=new ArrayList<com.fasterxml.jackson.databind.JsonNode>();
            for(var line:Files.readAllLines(root.resolve("smtp-receipts.jsonl")).subList((int)smtpBefore,(int)smtpBefore+30))smtp.add(JSON.readTree(line));
            for(var row:evidence){
                var eventId=new ObjectId((String)row.get("eventId"));var matched=new Document("eventId",eventId).append("policyId",policyId);
                assertThat(alerts.countDocuments(matched)).isOne();var alert=alerts.find(matched).first();
                assertThat(alert.getString("status")).isNotEqualTo("SUPPRESSED");
                var deliveryFilter=new Document("alertId",alert.getObjectId("_id"));assertThat(deliveries.countDocuments(deliveryFilter)).isOne();
                var delivery=deliveries.find(deliveryFilter).first();assertThat(delivery.getString("status")).isEqualTo("SENT");
                assertThat(delivery.getObjectId("orgId")).isEqualTo(org);assertThat(delivery.getString("recipient")).isEqualTo("fixture@alert.invalid");
                var matchingSmtp=smtp.stream().filter(receipt->receipt.path("subject").asText().equals(delivery.getString("subject"))).toList();
                assertThat(matchingSmtp).hasSize(1);row.put("alertId",alert.getObjectId("_id").toHexString());row.put("deliveryId",delivery.getObjectId("_id").toHexString());
                row.put("smtpSha256",matchingSmtp.getFirst().path("sha256").asText());
            }
            assertThat(alerts.countDocuments(new Document("eventId",new ObjectId(controlIds.getFirst())))).isZero();
            var quietFilter=new Document("eventId",new ObjectId(controlIds.getLast())).append("policyId",policyId);assertThat(alerts.countDocuments(quietFilter)).isOne();
            var quietAlert=alerts.find(quietFilter).first();assertThat(quietAlert.getString("status")).isEqualTo("SUPPRESSED");
            assertThat(deliveries.countDocuments(new Document("alertId",quietAlert.getObjectId("_id")))).isZero();
            try(var client=HttpClient.newBuilder().sslContext(ssl).followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(3)).build()){
                for(int i=0;i<2;i++){
                    var denied=client.send(HttpRequest.newBuilder(origin.resolve("/v1/api/clusters/"+clusters.get(1-i).toHexString()+"/events")).timeout(Duration.ofSeconds(5))
                            .header("Authorization","Bearer isolated-matrix-token-"+i).header("Content-Type","application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"cluster.alert.test\",\"severity\":\"INFO\",\"subject\":\"Wrong Cluster\",\"dedupeKey\":\"wrong-scope\"}")).build(),HttpResponse.BodyHandlers.discarding());
                    assertThat(denied.statusCode()).isEqualTo(401);
                }
            }
            assertThat(events.countDocuments()).isEqualTo(32);
            assertThat(Files.readAllLines(root.resolve("smtp-receipts.jsonl"))).hasSize((int)smtpBefore+30);
            assertThat(Duration.ofNanos(maxCallNanos)).isLessThan(Duration.ofSeconds(30));
            JSON.writerWithDefaultPrettyPrinter().writeValue(root.resolve("matrix-verification.json").toFile(),Map.of(
                    "source",SOURCE,"events",32,"distinctBusinessKeys",keys.size(),"sentDeliveries",30,"suppressedAlerts",1,"smtpReceipts",30,
                    "maxObservedCallMillis",maxCallNanos/1_000_000,"rows",evidence,"controlEventIds",controlIds,
                    "scope","Real SDK protocol and local control-plane policy/SMTP only; no durable runtime worker or human acceptance claim"));
        }
    }
    static NotificationChannel.Request request(URI origin,NotificationChannel.Destination destination,AlarmTypeCatalog.Type type,DeliveryKey.Kind kind,long round,Instant now){
        var alarm=new AlarmKey(destination.scope(),new AlarmKey.Resource(type.resourceKinds().iterator().next(),"matrix-resource","incarnation-a"),type.id(),"matrix-rule");
        var key=new DeliveryKey(DeliveryKey.NotificationRound.forEpisode(alarm,1,"managed-route","route-binding",kind,round),"cloud-sdk",destination.receiverIncarnation(),destination.targetId());
        return new NotificationChannel.Request(destination,NotificationChannel.Purpose.BUSINESS,"matrix-"+key.notification().subjectId()+"-"+kind+"-"+round,"attempt-1",Instant.now().plusSeconds(30),
                "application/json",CloudAlertEventWire.encode(origin,destination,key,alarm,now,type.defaultSeverity(),"Matrix "+destination.scope().clusterId()+":"+type.id()+":"+kind+":"+round,false));
    }
}
