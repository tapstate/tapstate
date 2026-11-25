package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** Aggregate CACHE heartbeat deferral retains exact CONFIG evidence and never accepts a refused fault. */
@RequiresDocker
class AlertCacheHeartbeatIT {
    @Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;

    @ParameterizedTest @ValueSource(booleans={false,true})
    void aPeerHeartbeatDoesNotWriteOrInventAcceptanceBeforeNormalOwnershipTransfer(boolean cloud) {
        var scope=cloud?new AlarmKey.Scope("a".repeat(24),"b".repeat(24)):AlertEvaluationHostIT.SCOPE;
        AlertEvaluationHostIT.withFixture(false,cloud,scope,f->{
            var original=f.storeHost();var cache=f.configuration().bindings().cachePreparation().orElseThrow().authScope();
            var resource=new AlarmKey.Resource("cluster-storage","cache",cache.cacheIncarnation()+":"+cache.bindingVersion());
            var host=new AlertStore.Host(original.scope(),original.setup(),original.deployment(),()->{},original.managedRoute(),original.managedTarget(),
                    key->resource.equals(key)?Optional.of(new AlertStore.StorageEvidence(resource,AlertStore.StorageRole.CACHE,cache,null)):Optional.empty(),original.availableChannels());
            var store=f.configuration().alerts(host,BuiltinAlarmTypes.catalog(),f.clock());
            var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"cache-heartbeat"),host.deployment(),f.clock().instant(),"operator");
            var rule=new AlertConfiguration.Rule(header,"Required CACHE health","cluster.storage-unavailable",1,
                    new AlertConfiguration.Resources(Set.of("cluster-storage"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog()).enrolled(null);
            var access=cloud?AlertStore.Access.cloud("operator",scope.authorityId(),"ADMIN",()->{},()->{}):AlertNotificationPlannerIT.access();
            assertThat(store.saveDefinition(null,rule,access)).isEqualTo(AlertStore.Outcome.STAGED);
            var digest=BuiltinResourceSignals.digest(cache.clusterId(),cache.cacheIncarnation(),Long.toString(cache.bindingVersion()),Long.toString(cache.authGeneration()));
            var binding=new AlertEvaluationHost.Binding(rule,resource,digest,0,()->{});
            var owner=new AlertEvaluationHost(store,BuiltinAlarmTypes.catalog(),host.deployment(),f.clock(),"cache-owner",()->{});
            var peer=new AlertEvaluationHost(store,BuiltinAlarmTypes.catalog(),host.deployment(),f.clock(),"cache-peer",()->{});
            assertThat(owner.accept(binding,0,new AlertInput.Operational(f.clock().instant(),digest,OperationalEvidence.Capability.CACHE_READ,true)))
                    .isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            f.now().set(AlertEvaluationHostIT.T.plusSeconds(3));
            try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(store,peer,BuiltinAlarmTypes.catalog(),null,null,
                    Set.of("cluster-storage-health","pipeline-lifecycle")),f.configuration().intents(),(key,claim)->Optional.empty(),()->{},f.clock());
                var client=MongoClients.create(f.settings().uri())) {
                runtime.tick();drained(runtime);assertThat(runtime.health().degraded()).isFalse();
                var db=client.getDatabase(new com.mongodb.ConnectionString(f.settings().uri()).getDatabase());
                var version=db.getCollection("configuration_version").find().first();
                var before=store.state(binding.key(),f.reads()).orElseThrow();
                runtime.resourceInput(resource,rule.typeId(),0,digest,()->{},new AlertInput.Operational(f.clock().instant(),digest,OperationalEvidence.Capability.CACHE_READ,true));
                drained(runtime);assertThat(runtime.health().degraded()).isFalse();assertThat(runtime.health().rejected()).isZero();
                assertThat(store.state(binding.key(),f.reads())).contains(before);
                assertThat(db.getCollection("configuration_version").find().first()).isEqualTo(version);
                f.now().set(AlertEvaluationHostIT.T.plusSeconds(4));
                runtime.resourceInput(resource,rule.typeId(),0,digest,()->{},new AlertInput.Operational(f.clock().instant(),digest,OperationalEvidence.Capability.CACHE_READ,false));
                drained(runtime);assertThat(runtime.health().degraded()).isTrue();assertThat(runtime.health().rejected()).isOne();
                assertThat(store.state(binding.key(),f.reads())).contains(before);
                f.now().set(AlertEvaluationHostIT.T.plusSeconds(31));
                runtime.resourceInput(resource,rule.typeId(),0,digest,()->{},new AlertInput.Operational(f.clock().instant(),digest,OperationalEvidence.Capability.CACHE_READ,false));
                drained(runtime);var transferred=store.state(binding.key(),f.reads()).orElseThrow().value();
                assertThat(transferred.owner().ownerId()).isEqualTo("cache-peer");
                assertThat(transferred.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
            }
        });
    }
    private static void drained(PipelineAlertRuntime runtime){await().atMost(Duration.ofSeconds(10)).until(()->runtime.health().queuedInputs()==0);}
}
