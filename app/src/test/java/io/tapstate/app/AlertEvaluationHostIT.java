package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@RequiresDocker
class AlertEvaluationHostIT {
    @Container static final MongoDBContainer MONGO=new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    static final Instant T=AlertEvaluationHostTest.T;
    static final Runnable ACTIVE=()->{};
    static final Duration TTL=Duration.ofSeconds(60);
    static final AlarmKey.Scope SCOPE=new AlarmKey.Scope("installation-a","cluster-a");
    static final WorkloadOwner OWNER=new WorkloadOwner("node-a","boot-a");
    record Fixture(MongoConfigurationDomain configuration,MongoCacheDomain cache,MongoConnectionSettings settings,
            CacheBusinessScope business,PipelineIntentSnapshot intent,WorkloadClaim claim,AlertStore store,
            AlertStore.Host storeHost,AlertConfiguration.Rule rule,AlertEvaluationHost evaluator,Clock clock,AtomicReference<Instant> now){
        AlertEvaluationHost.Binding binding(java.util.function.Supplier<AlertStore.EvaluationProof> proof){
            return new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("pipeline","orders","1"),"original-producer",0,ACTIVE,proof);
        }
        AlertStore.ReadAccess reads(){return new AlertStore.ReadAccess(true,Set.of(),ACTIVE);}
    }
    @Test void realConfigurationAcceptsAndReopensStateUnderItsOriginalPreparedPipeline() {
        withFixture(f->{
            var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent,ACTIVE));
            assertThat(f.evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null)))
                    .isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            var retained=f.store.state(binding.key(),f.reads()).orElseThrow();
            assertThat(retained.value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
            try(var reopened=MongoConfigurationDomain.open(f.settings,SCOPE.clusterId())){
                var view=reopened.alerts(f.storeHost,BuiltinAlarmTypes.catalog(),f.clock);
                assertThat(view.state(binding.key(),f.reads()).orElseThrow()).isEqualTo(retained);
                assertThat(view.evaluationStates(null,1).rows()).containsExactly(retained);
            }
        });
    }
    @Test void stalePipelineIntentIsRejectedInsideTheConfigurationTransaction() {
        withFixture(f->{
            var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent,ACTIVE));
            assertThat(f.configuration.intents().compareAndSet(f.intent.scope(),f.intent.intentRevision(),
                    new DesiredState("orders",PipelineState.STOPPED,f.intent.desired().revision()))).isTrue();
            assertThatThrownBy(()->f.evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null)))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.store.state(binding.key(),f.reads())).isEmpty();
        });
    }
    @Test void expirationAtTheFinalPureGuardRollsBackTheAlreadyStagedState() {
        withFixture(f->{
            var calls=new AtomicInteger();
            var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent,()->{
                if(calls.incrementAndGet()==3)throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            }));
            assertThatThrownBy(()->f.evaluator.accept(binding,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null)))
                    .isInstanceOf(TapstateException.class);
            assertThat(calls).hasValue(3);assertThat(f.store.state(binding.key(),f.reads())).isEmpty();
            assertThat(f.store.active(null,10,f.reads()).total()).isZero();
        });
    }
    @Test void asynchronousProductionFeedUsesActualConfigIntentAndCacheLeaseWithoutAnyExporter() {
        withFixture(f->{
            try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(f.store,f.evaluator),f.configuration.intents(),(scope,expected)->{
                long asked=System.nanoTime();var reading=f.cache.workloadClaims(f.business).readPipeline(scope);
                if(reading.isEmpty()||!WorkloadClaimFence.from(reading.orElseThrow().claim()).equals(expected))return Optional.empty();
                long budget=reading.orElseThrow().leaseRemaining().toNanos();
                return Optional.of(()->{if(System.nanoTime()-asked>=budget)throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);});
            },ACTIVE,f.clock)){
                runtime.tick();await().atMost(Duration.ofSeconds(10)).until(()->!runtime.health().degraded());
                var pass=mock(PipelineConvergencePass.class);when(pass.scope()).thenReturn(f.intent.scope());
                when(pass.intent()).thenReturn(f.intent);when(pass.claim()).thenReturn(WorkloadClaimFence.from(f.claim));
                runtime.published(pass,new Observation("orders",PipelineState.FAILED,Map.of(),Map.of(),Map.of(),null,T,List.of()),
                        new IllegalStateException("password=isolated-secret-must-not-survive"));
                await().atMost(Duration.ofSeconds(10)).until(()->f.store.active(null,10,f.reads()).total()==1);
                var head=f.store.active(null,10,f.reads()).rows().getFirst().value();
                assertThat(head.episode().diagnostics().status()).isEqualTo(AlertStateRecord.DiagnosticStatus.CAPTURED);
                assertThat(head.episode().diagnostics().history().first().trace().text()).doesNotContain("isolated-secret");
                assertThat(head.binding().ruleIdentity()).isEqualTo(f.rule.header().identity());
                assertThat(f.store.dueIntents(T.plusSeconds(1),null,10).rows()).isEmpty();
            }
        });
    }
    static void withFixture(java.util.function.Consumer<Fixture> test){withFixture(false,test);}
    static void withFixture(boolean withSource,java.util.function.Consumer<Fixture> test){withFixture(withSource,false,test);}
    static void withFixture(boolean withSource,boolean cloud,java.util.function.Consumer<Fixture> test){withFixture(withSource,cloud,SCOPE,test);}
    static void withFixture(boolean withSource,boolean cloud,AlarmKey.Scope fixtureScope,java.util.function.Consumer<Fixture> test){
        String suffix=UUID.randomUUID().toString().replace("-","");String configName="alert_host_config_"+suffix,cacheName="alert_host_cache_"+suffix;
        withFixture(withSource,cloud,fixtureScope,MONGO.getReplicaSetUrl(configName),MONGO.getReplicaSetUrl(cacheName),test);
    }
    static void withFixture(boolean withSource,boolean cloud,AlarmKey.Scope fixtureScope,String configUri,String cacheUri,java.util.function.Consumer<Fixture> test){
        var deployment=cloud?AlertChannelPolicy.Deployment.CLOUD_MANAGED:AlertChannelPolicy.Deployment.ON_PREM;
        String configName=new com.mongodb.ConnectionString(configUri).getDatabase(),cacheName=new com.mongodb.ConnectionString(cacheUri).getDatabase();
        try(var client=MongoClients.create(configUri);var cacheClient=MongoClients.create(cacheUri)){
            var configDb=client.getDatabase(configName);var cacheDb=cacheClient.getDatabase(cacheName);
            var settings=new MongoConnectionSettings(configUri,null,Duration.ofSeconds(5));
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,cacheUri,cacheName,null);
            try(var configuration=MongoConfigurationBootstrap.prepare(settings,fixtureScope.clusterId(),"installation-a","setup-a",1);
                    var cache=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"alert-host-test")){
                var setup=new StorageSetupScope(fixtureScope.clusterId(),"installation-a","setup-a",1);
                var master=new SourceResource(cloud?"atlas-store":"views",null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                var committed=configuration.bindings().commitWithFirstCache(setup,cloud?StorageBinding.Mode.CLOUD:StorageBinding.Mode.OP,connection,master);
                var business=committed.preparation().businessScope();cache.prepareFirstAuth(committed.firstCreation().orElseThrow());cache.prepareInitialBusiness(committed.preparation(),ACTIVE);
                var pipeline=new PipelineResource("orders",null,withSource?List.of(SourceRef.spec("views",true)):List.of(),null,null,null,null,null);configuration.artifacts().create(pipeline);
                var scope=new PipelineIntentScope(fixtureScope.clusterId(),business.bindingVersion(),business.cacheIncarnation(),"orders",1);
                configuration.intents().compareAndSet(scope,0,new DesiredState("orders",PipelineState.RUNNING,CanonicalHash.of(pipeline)));
                var claims=cache.workloadClaims(business);var node=claims.acquire(new WorkloadClaimKey(fixtureScope.clusterId(),WorkloadClaimType.NODE_SESSION,"node-a"),OWNER,1,TTL).claim();
                var now=new AtomicReference<>(T);Clock clock=new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return now.get();}};
                new PipelineFirstStatePreparer(business,configuration.preparations(),cache.pipelinePreparations(business,ACTIVE),
                        ()->Optional.of(WorkloadClaimFence.from(node)),ACTIVE,clock,TTL).prepare(scope);
                assertThat(configuration.pipelineStates().read("orders").orElseThrow().phase()).isEqualTo(PipelineStateMapping.Phase.ACTIVE);
                var intent=configuration.intents().loadForActuation("orders").orElseThrow();var claim=claims.acquirePipeline(scope,OWNER,1,TTL).claim();
                var token=new TokenRecord("operator","ADMIN","isolated-test-hash",false,T);configuration.tokens().save(token);
                var access=new AlertStore.Access(new AlertTestAuthority(AlertTestAuthority.Mechanism.MACHINE_TOKEN,"installation-a","operator","ADMIN",token.authenticationBinding(),null),ACTIVE,ACTIVE,()->"");
                var storeHost=cloud?new AlertStore.Host(fixtureScope,setup,deployment,ACTIVE,new AlertIntent.SystemRoute("managed-route","managed-binding",1),new AlertIntent.SdkTarget("managed-target",fixtureScope.clusterId(),"binding-a"),r->Optional.empty(),Set.of()):new AlertStore.Host(fixtureScope,setup,deployment,ACTIVE);
                var store=configuration.alerts(storeHost,BuiltinAlarmTypes.catalog(),clock);
                if(cloud)access=AlertStore.Access.cloud("operator",fixtureScope.authorityId(),"ADMIN",ACTIVE,ACTIVE);
                var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,fixtureScope,"rule"),deployment,T,"operator");
                var rule=new AlertConfiguration.Rule(header,"Failure","pipeline.failed",1,new AlertConfiguration.Resources(Set.of("pipeline"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
                assertThat(store.saveDefinition(null,rule,access)).isEqualTo(AlertStore.Outcome.STAGED);
                test.accept(new Fixture(configuration,cache,settings,business,intent,claim,store,storeHost,rule,
                        new AlertEvaluationHost(store,BuiltinAlarmTypes.catalog(),deployment,clock,"host-a",ACTIVE),clock,now));
            }finally{configDb.drop();cacheDb.drop();}
        }
    }
}
