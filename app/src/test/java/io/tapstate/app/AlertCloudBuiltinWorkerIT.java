package io.tapstate.app;

import com.hazelcast.cluster.*;
import com.hazelcast.core.HazelcastInstance;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.capture.SourceOperationObserver;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.AlertNodeLifecycleStore.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.*;
import java.nio.file.*;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/** Classified producer evidence traverses real CONFIG, planner, worker, SDK and local event-center policy. */
@RequiresDocker
@EnabledIfSystemProperty(named="tapstate.test.alert-cloud-worker-root",matches=".+")
class AlertCloudBuiltinWorkerIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    static final Runnable ACTIVE=()->{};
    @Test void fiveBuiltinTypesPersistOriginalFaultReminderAndRecoveryReceiptsForTwoClusters()throws Exception{
        var root=Path.of(System.getProperty("tapstate.test.alert-cloud-worker-root"));var json=new ObjectMapper();
        var launch=json.readTree(root.resolve("launch.json").toFile());
        assertThat(launch.path("source").asText()).isEqualTo("650082bdf4180d6e5da679c5040522d4855ff6a1");
        String database=launch.path("database").asText();assertThat(database).startsWith("w4_alert_t10_control_");
        var origin=URI.create("https://localhost:"+launch.path("cloudPort").asInt());var ssl=AlertChannels.trust(Files.readAllBytes(root.resolve("tls-cert.pem")));
        var org=new ObjectId();var rows=new ArrayList<Map<String,Object>>();
        long smtpBefore=Files.readAllLines(root.resolve("smtp-receipts.jsonl")).size();
        try(var mongo=MongoClients.create("mongodb://127.0.0.1:"+launch.path("mongoPort").asInt()+"/?directConnection=true&replicaSet=rs0")){
            var db=mongo.getDatabase(database);var policy=new ObjectId();
            db.getCollection("alert_policies").insertOne(new Document("_id",policy).append("orgId",org).append("name","Durable worker matrix").append("enabled",true)
                    .append("eventTypes",List.of("cluster.alert.*")).append("minSeverity","INFO").append("recipients",List.of("fixture@alert.invalid"))
                    .append("channel","EMAIL").append("cooldownSeconds",0L).append("timezone","UTC").append("createdAt",new Date()).append("updatedAt",new Date()));
            for(int n=0;n<2;n++){
                var cluster=new ObjectId();String token="isolated-worker-token-"+cluster.toHexString();
                db.getCollection("clusters").insertOne(new Document("_id",cluster).append("orgId",org).append("name","worker-"+n).append("dnsLabel","worker-"+cluster.toHexString())
                        .append("region","isolated").append("state","READY").append("sdkStaticToken",token).append("createdAt",new Date()).append("updatedAt",new Date()).append("_class","io.tapstate.cloud.cluster.Cluster"));
                AlertEvaluationHostIT.withFixture(false,true,new AlarmKey.Scope(org.toHexString(),cluster.toHexString()),f->exercise(f,origin,ssl,token,rows));
            }
            var events=db.getCollection("events");var alerts=db.getCollection("alerts");var deliveries=db.getCollection("notification_deliveries");
            assertThat(rows).hasSize(30);assertThat(rows.stream().map(r->r.get("eventId")).distinct()).hasSize(30);
            assertThat(events.countDocuments(new Document("orgId",org))).isEqualTo(30);
            await().atMost(Duration.ofSeconds(30)).until(()->deliveries.countDocuments(new Document("orgId",org).append("status","SENT"))==30);
            var smtp=new ArrayList<com.fasterxml.jackson.databind.JsonNode>();var receiptLines=Files.readAllLines(root.resolve("smtp-receipts.jsonl"));
            assertThat(receiptLines).hasSize((int)smtpBefore+30);for(var line:receiptLines.subList((int)smtpBefore,receiptLines.size()))smtp.add(json.readTree(line));
            for(var row:rows){
                var eventId=new ObjectId((String)row.get("eventId"));var event=events.find(new Document("_id",eventId)).first();
                assertThat(event.getObjectId("orgId")).isEqualTo(org);assertThat(event.get("payload",Document.class).getString("clusterId")).isEqualTo(row.get("cluster"));
                assertThat(event.getString("type")).isEqualTo("cluster.alert."+row.get("type")+"."+row.get("kind").toString().toLowerCase(Locale.ROOT));
                var alertFilter=new Document("eventId",eventId).append("policyId",policy);assertThat(alerts.countDocuments(alertFilter)).isOne();var alert=alerts.find(alertFilter).first();
                var deliveryFilter=new Document("alertId",alert.getObjectId("_id"));assertThat(deliveries.countDocuments(deliveryFilter)).isOne();var delivery=deliveries.find(deliveryFilter).first();
                assertThat(delivery.getString("status")).isEqualTo("SENT");String alertId=alert.getObjectId("_id").toHexString();
                var actual=smtp.stream().filter(s->s.path("alertIds").isArray()
                        &&java.util.stream.StreamSupport.stream(s.path("alertIds").spliterator(),false).anyMatch(id->id.asText().equals(alertId))).toList();
                assertThat(actual).hasSize(1);row.put("alertId",alertId);row.put("remoteDeliveryId",delivery.getObjectId("_id").toHexString());row.put("smtpSha256",actual.getFirst().path("sha256").asText());
            }
            json.writerWithDefaultPrettyPrinter().writeValue(root.resolve("worker-verification.json").toFile(),Map.of("controlPlaneSource",launch.path("source").asText(),"rows",rows,"events",30,"smtpAccepted",30,
                    "scope","Actual CONFIG/planner/worker/SDK/local policy with classified original producer seams and controlled membership; no real outage, three-process or human acceptance claim"));
        }
    }
    static void exercise(AlertEvaluationHostIT.Fixture f,URI origin,SSLContext ssl,String token,List<Map<String,Object>> rows){
        f.now().set(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));var scope=f.storeHost().scope();var setup=f.storeHost().setup();
        var cache=f.configuration().bindings().cachePreparation().orElseThrow().authScope();var runtimeRef=new AtomicReference<PipelineAlertRuntime>();
        var signals=new BuiltinResourceSignals(runtimeRef::get,()->f.configuration().bindings().load().orElseThrow(),cache,ACTIVE,f.clock());
        var properties=new CloudProperties();properties.setBaseUrl(origin.toString());properties.setToken(token);properties.setClusterId(scope.clusterId());properties.setAtlasUri(f.settings().uri());
        var cloud=CloudRuntimeSettings.resolve(properties);String managed=AlertNotificationPlanner.managedBinding(origin,scope,setup);
        var host=new AlertStore.Host(scope,setup,AlertChannelPolicy.Deployment.CLOUD_MANAGED,ACTIVE,new AlertIntent.SystemRoute("managed-events",managed,1),
                new AlertIntent.SdkTarget(managed,scope.clusterId(),managed),signals::resourceEvidence,AlertChannels.capabilities(AlertChannelPolicy.Deployment.CLOUD_MANAGED,cloud));
        var catalog=BuiltinAlarmTypes.catalog();var store=f.configuration().alerts(host,catalog,f.clock());var rules=new LinkedHashMap<String,AlertConfiguration.Rule>();
        var access=AlertStore.Access.cloud("operator",scope.authorityId(),"ADMIN",ACTIVE,ACTIVE);
        for(var type:catalog.types()){
            var h=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"worker-"+type.id()),host.deployment(),f.clock().instant(),"operator");
            var rule=new AlertConfiguration.Rule(h,type.id(),type.id(),type.version(),new AlertConfiguration.Resources(type.resourceKinds(),true,Set.of()),
                    type.id().equals("pipeline.observation-stale")?Map.of("max-age",java.math.BigDecimal.valueOf(5),"startup-grace",java.math.BigDecimal.ZERO):Map.of(),null,true,null).validated(catalog).enrolled(null);
            assertThat(store.saveDefinition(null,rule,access)).isEqualTo(AlertStore.Outcome.STAGED);rules.put(type.id(),rule);
        }
        var evaluator=new AlertEvaluationHost(store,catalog,host.deployment(),f.clock(),"builtin-worker",ACTIVE);
        var planner=new AlertNotificationPlanner(store,f.clock(),"builtin-planner",origin,ACTIVE);evaluator.withPlanner(planner);
        var claims=f.cache().workloadClaims(f.business());var ownClaim=claims.read(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.NODE_SESSION,"node-a")).orElseThrow().claim();
        var peerClaim=claims.acquire(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.NODE_SESSION,"node-b"),new WorkloadOwner("node-b","boot-b"),1,Duration.ofSeconds(60)).claim();
        var own=new Registration(new Key(scope,"node-a","boot-a"),setup,cache,WorkloadClaimFence.from(ownClaim),UUID.randomUUID().toString(),1);
        var peer=new Registration(new Key(scope,"node-b","boot-b"),setup,cache,WorkloadClaimFence.from(peerClaim),UUID.randomUUID().toString(),1);
        var lifecycle=f.configuration().alertNodeLifecycles(scope,setup,ACTIVE);lifecycle.register(peer,f.clock().instant(),ACTIVE);
        var member=mock(HazelcastInstance.class);var cluster=mock(Cluster.class);var membership=mock(ClusterMembershipGate.class);
        var ownMember=ClusterNodeAlertSourceTest.member(own);var peerMember=ClusterNodeAlertSourceTest.member(peer);var live=new LinkedHashSet<Member>(List.of(ownMember,peerMember));
        when(member.getCluster()).thenReturn(cluster);when(cluster.addMembershipListener(any())).thenReturn(UUID.randomUUID());when(cluster.getMembers()).thenAnswer(c->Set.copyOf(live));
        when(membership.committed()).thenReturn(new ClusterMembership(scope.clusterId(),1,Set.of("node-a","node-b")));
        when(membership.visibleNodeIds()).thenAnswer(c->live.stream().map(ClusterMembershipGate::stableIdOf).collect(java.util.stream.Collectors.toSet()));
        f.configuration().artifacts().create(new SourceResource("erp",null,"mongodb",Map.of("uri","mongodb://source.invalid/fixture"),null,null,null,null));
        var channels=new AlertChannels(host.deployment(),cloud,f.clock(),ACTIVE,scope.authorityId(),hostname->new InetAddress[]{InetAddress.getLoopbackAddress()},ssl);
        var source=signals.source(f.configuration().artifacts().sourceBinding("erp").orElseThrow(),"worker-matrix",ACTIVE,ACTIVE);
        try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(store,evaluator,catalog,null,null,Set.of("pipeline-lifecycle","pipeline-observations","source-operational-health","cluster-storage-health","cluster-member-health")),
                    f.configuration().intents(),(intent,fence)->Optional.of(ACTIVE),ACTIVE,f.clock());
                var node=new ClusterNodeAlertSource(member,membership,runtime,signals,ACTIVE,f.clock(),()->lifecycle,()->own,registration->ACTIVE);
                var worker=new AlertDeliveryRuntime(store,f.clock(),"builtin-sender",ACTIVE,channels,authority->{throw new AssertionError("Cloud cannot continue channel tests");})){
            runtimeRef.set(runtime);runtime.tick();node.tick();drain(runtime);live.remove(peerMember);f.now().set(f.clock().instant().plusSeconds(1));
            for(int phase=0;phase<3;phase++){
                if(phase==1)f.now().set(f.clock().instant().plusSeconds(3601));if(phase==2){f.now().set(f.clock().instant().plusSeconds(1));live.add(peerMember);}
                boolean good=phase==2;int sequence=phase+1;
                var failed=new AlertEvaluationHost.Binding(rules.get("pipeline.failed"),new AlarmKey.Resource("pipeline","orders","1"),"pipeline-matrix",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
                assertThat(evaluator.accept(failed,sequence,new AlertInput.Conditions(f.clock().instant(),good?AlertSignal.Condition.GOOD:AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
                var stale=new AlertEvaluationHost.Binding(rules.get("pipeline.observation-stale"),failed.resource(),"freshness-matrix",0,ACTIVE,()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
                assertThat(evaluator.accept(stale,sequence,new AlertInput.Freshness(f.clock().instant(),true,f.clock().instant().minusSeconds(60),good?f.clock().instant():f.clock().instant().minusSeconds(10),Duration.ZERO,Duration.ofSeconds(5)))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
                source.completed("connection",SourceOperationObserver.Capability.STREAM_READ,good?null:new SQLException("Controlled connection evidence","08001"));
                signals.cacheOperation(true,good?null:new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","controlled-cache-write"),null));node.tick();drain(runtime);
                assertThat(store.active(null,100,f.reads()).total()).as("active builtin heads in phase %s",phase).isEqualTo(good?0:5);
                int expected=(phase+1)*5;
                planner.tick();
                await().atMost(Duration.ofSeconds(15)).untilAsserted(()->{worker.tick();assertThat(worker.failure()).as("dispatch health").isNull();
                    var retained=store.deliveries(null,100,null,null,f.reads()).rows();assertThat(retained).hasSize(expected);
                    assertThat(retained.stream().map(r->r.value().frozen().members().getFirst().evaluation().key().typeId()+":"+r.value().state().summary()+":"+r.value().state().attemptsConsumed()+":"
                            +(r.value().state().attempts().isEmpty()?"none":r.value().state().attempts().getFirst().protocol())).toList())
                            .allMatch(row->row.contains(":ACCEPTED:1:"));});
            }
            var records=store.deliveries(null,100,null,null,f.reads()).rows();assertThat(records).hasSize(15);
            var expectedKinds=new TreeMap<String,Long>();for(var type:catalog.types())for(var kind:List.of(DeliveryKey.Kind.FAULT,DeliveryKey.Kind.REMINDER,DeliveryKey.Kind.RECOVERY))expectedKinds.put(type.id()+":"+kind,1L);
            var actualKinds=new TreeMap<String,Long>();for(var record:records){assertThat(record.value().frozen().members()).hasSize(1);
                actualKinds.merge(record.value().frozen().members().getFirst().evaluation().key().typeId()+":"+((AlertIntent.Business)record.value().frozen().identity()).key().notification().kind(),1L,Long::sum);}
            assertThat(actualKinds).containsExactlyInAnyOrderEntriesOf(expectedKinds);
            for(var r:records){var v=r.value();var identity=(AlertIntent.Business)v.frozen().identity();var receipt=v.state().acceptedReceipt();
                assertThat(receipt.source()).isEqualTo("sdk-event-center");assertThat(v.state().attempts()).hasSize(1);
                var originalReceipt=v.state().attempts().getFirst().receipt();assertThat(originalReceipt).isNotNull();
                assertThat(originalReceipt.source()).isEqualTo(receipt.source());assertThat(originalReceipt.remoteId()).isEqualTo(receipt.remoteId());
                rows.add(new LinkedHashMap<>(Map.of("cluster",scope.clusterId(),"type",v.frozen().members().getFirst().evaluation().key().typeId(),"kind",identity.key().notification().kind().name(),
                        "deliveryId",identity.deliveryId(),"attemptId",v.state().attempts().getFirst().permit().attemptId(),"eventId",receipt.remoteId())));
            }
            assertThat(store.active(null,100,f.reads()).total()).isZero();
        }finally{source.close();}
    }
    static void drain(PipelineAlertRuntime runtime){await().atMost(Duration.ofSeconds(10)).until(()->runtime.health().queuedInputs()==0);}
}
