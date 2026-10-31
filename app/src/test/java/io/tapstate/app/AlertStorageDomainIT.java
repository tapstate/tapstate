package io.tapstate.app;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

/** Independent servers and authenticated principals exercise loss without substituting a new authoritative ledger. */
@RequiresDocker
class AlertStorageDomainIT {
    @Container static final MongoDBContainer CONFIG=new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container static final MongoDBContainer CACHE=new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @BeforeAll static void principals() {
        principal(CONFIG,"config-owner","config-fixture-password");
        principal(CACHE,"cache-owner","cache-fixture-password");
    }
    private static void principal(MongoDBContainer server,String user,String password) {
        try(var client=MongoClients.create(server.getReplicaSetUrl())) {
            client.getDatabase("admin").runCommand(new Document("createUser",user).append("pwd",password)
                    .append("roles",List.of(new Document("role","root").append("db","admin"))));
        }
    }
    private static String authenticated(MongoDBContainer server,String database,boolean config) {
        var uri=server.getReplicaSetUrl(database).replace("mongodb://",config?"mongodb://config-owner:config-fixture-password@":"mongodb://cache-owner:cache-fixture-password@");
        return uri+(uri.contains("?")?"&":"?")+"authSource=admin";
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void cacheLossPreservesOriginalFactsWithoutCreatingANewIdentityOrLedger(boolean cloud) {
        String suffix=UUID.randomUUID().toString().replace("-","");
        String configUri=authenticated(CONFIG,"alert_separate_config_"+suffix,true),cacheUri=authenticated(CACHE,"alert_separate_cache_"+suffix,false);
        assertThat(new ConnectionString(configUri).getHosts()).isNotEqualTo(new ConnectionString(cacheUri).getHosts());
        assertThat(new ConnectionString(configUri).getCredential().getUserName()).isNotEqualTo(new ConnectionString(cacheUri).getCredential().getUserName());
        AlertEvaluationHostIT.withFixture(false,cloud,AlertEvaluationHostIT.SCOPE,configUri,cacheUri,f->{
            if(!cloud)route(f,Duration.ZERO);
            var planner=new AlertNotificationPlanner(f.store(),f.clock(),"separate-planner",cloud?URI.create("https://cloud.example.test"):null,ACTIVE);
            f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var capability=cloud?new AlertChannelPolicy.Capability("cloud-sdk",2,AlertChannelPolicy.Family.CLOUD_SDK):new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL);
            var store=f.configuration().alerts(f.storeHost().withChannels(Set.of(capability)),BuiltinAlarmTypes.catalog(),f.clock());
            var channels=new AlertDeliveryRuntimeIT.Channels(store);
            channels.answer=request->new NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,NotificationChannel.Action.RETRY,null,NotificationChannel.Phase.DEADLINE,0,"fixture-unknown");
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,store,channels,"separate-sender")) {
                worker.tick();AlertDeliveryRuntimeIT.settled(worker);
            }
            var original=store.deliveries(null,100,null,null,f.reads()).rows();
            assertThat(original).hasSize(cloud?1:2).allSatisfy(row->{
                assertThat(row.value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
                assertThat(row.value().state().attemptsConsumed()).isOne();
            });
            int calls=channels.calls.get();
            try(var config=MongoClients.create(configUri);var cache=MongoClients.create(cacheUri)) {
                var configDb=config.getDatabase(new ConnectionString(configUri).getDatabase());
                var cacheDb=cache.getDatabase(new ConnectionString(cacheUri).getDatabase());
                var before=alertRows(configDb);
                var setup=f.configuration().bindings().load().orElseThrow();
                cacheDb.drop();
                assertThatThrownBy(()->f.cache().workloadClaims(f.business()).readPipeline(f.intent().scope())).isInstanceOf(TapstateException.class);
                try(var reopened=MongoConfigurationDomain.open(f.settings(),f.storeHost().scope().clusterId())) {
                    var retained=reopened.alerts(f.storeHost().withChannels(Set.of(capability)),BuiltinAlarmTypes.catalog(),f.clock());
                    assertThat(reopened.bindings().load().orElseThrow()).isEqualTo(setup);
                    assertThat(retained.deliveries(null,100,null,null,f.reads()).rows()).isEqualTo(original);
                    assertThat(retained.active(null,10,f.reads()).total()).isOne();
                    assertThat(retained.planning().schedules()).isNotEmpty();
                    assertThat(retained.maintenance().orElseThrow().inventory().logicalBytes()).isPositive();
                }
                assertThat(alertRows(configDb)).isEqualTo(before);
                assertThat(cacheDb.listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(channels.calls).hasValue(calls);
            }
        });
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void missingConfigRefusesEvaluationAndAnActualWorkerWhileTheIndependentCacheRemainsIntact(boolean cloud) {
        String suffix=UUID.randomUUID().toString().replace("-","");
        String configUri=authenticated(CONFIG,"alert_lost_config_"+suffix,true),cacheUri=authenticated(CACHE,"alert_intact_cache_"+suffix,false);
        AlertEvaluationHostIT.withFixture(false,cloud,AlertEvaluationHostIT.SCOPE,configUri,cacheUri,f->{
            if(!cloud)route(f,Duration.ZERO);
            var planner=new AlertNotificationPlanner(f.store(),f.clock(),"before-config-loss",cloud?URI.create("https://cloud.example.test"):null,ACTIVE);
            f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var capability=cloud?new AlertChannelPolicy.Capability("cloud-sdk",2,AlertChannelPolicy.Family.CLOUD_SDK):new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL);
            var store=f.configuration().alerts(f.storeHost().withChannels(Set.of(capability)),BuiltinAlarmTypes.catalog(),f.clock());
            assertThat(store.dueIntents(T.plusSeconds(1),null,10).rows()).hasSize(cloud?1:2);
            var channels=new AlertDeliveryRuntimeIT.Channels(store);
            try(var config=MongoClients.create(configUri);var cache=MongoClients.create(cacheUri)) {
                var configDb=config.getDatabase(new ConnectionString(configUri).getDatabase());
                var cacheDb=cache.getDatabase(new ConnectionString(cacheUri).getDatabase());
                var before=rows(cacheDb,false);assertThat(before.get("workload_claims")).isNotEmpty();
                configDb.drop();
                try(var worker=AlertDeliveryRuntimeIT.runtime(f,store,channels,"missing-config-worker")) {
                    worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                    assertThat(worker.failure()).isNotNull();assertThat(channels.calls).hasValue(0);
                }
                assertThatThrownBy(()->f.evaluator().accept(f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE)),2,
                        new AlertInput.Conditions(T.plusSeconds(1),AlertSignal.Condition.GOOD,null))).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->MongoConfigurationDomain.open(f.settings(),f.storeHost().scope().clusterId())).isInstanceOf(TapstateException.class);
                assertThat(configDb.listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(rows(cacheDb,false)).isEqualTo(before);
            }
        });
    }
    private static Map<String,List<Document>> alertRows(MongoDatabase database) {
        var rows=rows(database,true);assertThat(rows.keySet()).hasSize(12);return rows;
    }
    private static Map<String,List<Document>> rows(MongoDatabase database,boolean alertsOnly) {
        var rows=new TreeMap<String,List<Document>>();
        for(String name:database.listCollectionNames())if(!alertsOnly||name.startsWith("alert_"))
            rows.put(name,database.getCollection(name).find().sort(new Document("_id",1)).into(new ArrayList<>()));
        return rows;
    }
}
