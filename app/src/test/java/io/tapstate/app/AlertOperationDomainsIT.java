package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import io.tapstate.testsupport.RequiresDocker;
import static org.assertj.core.api.Assertions.*;

/** Exact CONFIG domain keys survive restart and cannot borrow another access domain's recovery. */
@RequiresDocker
class AlertOperationDomainsIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    private record SourceCase(AlertConfiguration.Rule rule,SourceBindingIdentity identity,AlarmKey.Resource resource) { }
    private SourceCase source(AlertEvaluationHostIT.Fixture f) {return source(f,Map.of());}
    private SourceCase source(AlertEvaluationHostIT.Fixture f,Map<String,java.math.BigDecimal> parameters) {
        f.configuration().artifacts().create(new SourceResource("erp",null,"test",Map.of("host","unused.invalid"),null,null,null,null));
        var id=f.configuration().artifacts().sourceBinding("erp").orElseThrow().identity();
        var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,f.storeHost().scope(),"source-rule"),
                AlertChannelPolicy.Deployment.ON_PREM,AlertEvaluationHostIT.T,"operator");
        var rule=new AlertConfiguration.Rule(header,"Source access","source.connection-failed",1,
                new AlertConfiguration.Resources(Set.of("source"),true,Set.of()),parameters,null,true,null).validated(BuiltinAlarmTypes.catalog());
        assertThat(f.store().saveDefinition(null,rule,AlertNotificationPlannerIT.access())).isEqualTo(AlertStore.Outcome.STAGED);
        return new SourceCase(rule,id,new AlarmKey.Resource("source","erp",id.incarnation()));
    }
    private AlertEvaluationHost.Binding binding(SourceCase source,String domain) {
        return new AlertEvaluationHost.Binding(source.rule(),source.resource(),"binding-"+domain,source.identity().bindingRevision(),()->{},domain);
    }
    private AlertInput.Operational input(java.time.Instant at,String domain,boolean success) {
        return new AlertInput.Operational(at,"a".repeat(64),OperationalEvidence.Capability.STREAM_READ,success,domain);
    }
    @Test void anotherConnectionCannotRecoverAnOriginalFailureAndOriginalDomainSurvivesHostRestart() {
        AlertEvaluationHostIT.withFixture(f->{
            var source=source(f);String first="1".repeat(64),second="2".repeat(64);
            var a=binding(source,first);var b=binding(source,second);
            assertThat(f.evaluator().accept(a,1,input(f.clock().instant(),first,true))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
            assertThat(f.store().state(a.key(),f.reads())).isEmpty();
            assertThat(f.evaluator().accept(a,1,input(f.clock().instant(),first,false))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            f.now().set(f.clock().instant().plusSeconds(1));
            assertThat(f.evaluator().accept(b,1,input(f.clock().instant(),second,true))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
            var current=f.store().state(a.key(),f.reads()).orElseThrow().value();
            assertThat(current.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
            assertThat(current.operationalEvidence().version()).isEqualTo(2);
            assertThat(f.store().originalRule(current.snapshot().rule(),f.reads())).contains(source.rule());
            f.now().set(AlertEvaluationHostIT.T.plusSeconds(31));
            var restarted=new AlertEvaluationHost(f.store(),BuiltinAlarmTypes.catalog(),AlertChannelPolicy.Deployment.ON_PREM,f.clock(),"owner-a",()->{});
            assertThat(restarted.accept(a,2,input(f.clock().instant(),first,true))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            assertThat(f.store().state(a.key(),f.reads()).orElseThrow().value().snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
            assertThat(f.store().state(b.key(),f.reads())).isEmpty();
        });
    }
    @Test void aSixtyFifthRetainedDomainIsRefusedWithoutChangingTheOriginalSixtyFour() {
        AlertEvaluationHostIT.withFixture(f->{
            var source=source(f);
            for(int i=0;i<64;i++) {
                String token=String.format("%064x",i);
                assertThat(f.evaluator().accept(binding(source,token),1,input(f.clock().instant(),token,false))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            }
            String extra="f".repeat(64);
            assertThatThrownBy(()->f.evaluator().accept(binding(source,extra),1,input(f.clock().instant(),extra,false)))
                    .isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.code()).isEqualTo(AlertError.INVALID_CONFIGURATION));
            assertThat(f.store().state(binding(source,extra).key(),f.reads())).isEmpty();
            assertThat(f.store().active(null,100,f.reads()).total()).isEqualTo(64);
        });
    }
    @Test void proofLossAtTheConfigBoundaryCannotCommitARecoveryThenRejectItAfterward() {
        AlertEvaluationHostIT.withFixture(f->{
            var source=source(f);String token="b".repeat(64);var original=binding(source,token);
            f.evaluator().accept(original,1,input(f.clock().instant(),token,false));
            var before=f.store().state(original.key(),f.reads()).orElseThrow();
            var live=new java.util.concurrent.atomic.AtomicBoolean(true);
            Runnable proof=()->{if(!live.get())throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);};
            var guarded=(AlertStore)java.lang.reflect.Proxy.newProxyInstance(AlertStore.class.getClassLoader(),new Class<?>[]{AlertStore.class},(object,method,args)->{
                if(method.getName().equals("commit"))live.set(false);
                try{return method.invoke(f.store(),args);}catch(java.lang.reflect.InvocationTargetException failure){throw failure.getCause();}
            });
            var host=new AlertEvaluationHost(guarded,BuiltinAlarmTypes.catalog(),AlertChannelPolicy.Deployment.ON_PREM,f.clock(),"host-a",()->{});
            var guardedBinding=new AlertEvaluationHost.Binding(source.rule(),source.resource(),original.producer(),source.identity().bindingRevision(),proof,token);
            f.now().set(f.clock().instant().plusSeconds(1));
            assertThatThrownBy(()->host.accept(guardedBinding,2,input(f.clock().instant(),token,true))).isInstanceOf(TapstateException.class);
            assertThat(f.store().state(original.key(),f.reads()).orElseThrow()).isEqualTo(before);
            assertThat(before.value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        });
    }

    @Test void changingTheSourceBindingRetiresOnlyOldDomainsWithoutARecovery() {bindingReplacement("firing");}
    @Test void pendingAndResolvedDomainsRetireWithoutInventingAnEpisodeOrChangingRecoveryHistory() {
        bindingReplacement("pending");bindingReplacement("resolved");
    }
    private void bindingReplacement(String phase) {
        AlertEvaluationHostIT.withFixture(f->{
            var source=source(f,phase.equals("pending")?Map.of("trigger-for",java.math.BigDecimal.TEN):Map.of());
            String token="c".repeat(64);var original=binding(source,token);
            f.evaluator().accept(original,1,input(f.clock().instant(),token,false));
            if(phase.equals("resolved")) {
                f.now().set(f.clock().instant().plusSeconds(1));f.evaluator().accept(original,2,input(f.clock().instant(),token,true));
            }
            f.configuration().artifacts().save(new SourceResource("erp",null,"test",Map.of("host","replacement.invalid"),null,null,null,null));
            var changed=f.configuration().artifacts().sourceBinding("erp").orElseThrow().identity();
            assertThat(changed.incarnation()).isEqualTo(source.identity().incarnation());
            assertThat(changed.bindingRevision()).isEqualTo(source.identity().bindingRevision()+1);
            String currentToken="d".repeat(64);
            var current=new AlertEvaluationHost.Binding(source.rule(),source.resource(),"new-binding",changed.bindingRevision(),()->{},currentToken);
            f.now().set(f.clock().instant().plusSeconds(1));
            f.evaluator().accept(current,1,input(f.clock().instant(),currentToken,false));
            try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(f.store(),f.evaluator(),BuiltinAlarmTypes.catalog(),null,null,Set.of("source-operational-health","pipeline-lifecycle")),
                    f.configuration().intents(),(scope,fence)->Optional.empty(),()->{},f.clock())) {
                runtime.tick();
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(()->runtime.health().queuedInputs()==0);
                var old=f.store().state(original.key(),f.reads()).orElseThrow().value();
                assertThat(old.retiredAt()).isNotNull();
                if(phase.equals("pending"))assertThat(old.episode()).isNull();
                else assertThat(old.episode().value().endReason()).isEqualTo(phase.equals("resolved")?AlertSnapshot.EndReason.RECOVERED:AlertSnapshot.EndReason.SUPERSEDED);
                assertThat(f.store().state(current.key(),f.reads()).orElseThrow().value().snapshot().status()).isEqualTo(phase.equals("pending")?AlertSnapshot.Status.PENDING:AlertSnapshot.Status.FIRING);
                assertThat(runtime.health().degraded()).isFalse();
            }
        });
    }

    @Test void obsoleteAggregateEvidenceIsPreservedWithoutBlockingTheCurrentDomain() {
        AlertEvaluationHostIT.withFixture(f->{
            var source=source(f);var legacy=new AlertEvaluationHost.Binding(source.rule(),source.resource(),"legacy",source.identity().bindingRevision(),()->{});
            f.evaluator().accept(legacy,1,new AlertInput.Operational(f.clock().instant(),"e".repeat(64),OperationalEvidence.Capability.STREAM_READ,false));
            f.configuration().artifacts().save(new SourceResource("erp",null,"test",Map.of("host","replacement.invalid"),null,null,null,null));
            var current=f.configuration().artifacts().sourceBinding("erp").orElseThrow().identity();
            String token="f".repeat(64);f.now().set(f.clock().instant().plusSeconds(1));
            var currentBinding=new AlertEvaluationHost.Binding(source.rule(),source.resource(),"current",current.bindingRevision(),()->{},token);
            f.evaluator().accept(currentBinding,1,input(f.clock().instant(),token,false));
            try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(f.store(),f.evaluator(),BuiltinAlarmTypes.catalog(),null,null,Set.of("source-operational-health","pipeline-lifecycle")),
                    f.configuration().intents(),(scope,fence)->Optional.empty(),()->{},f.clock())) {
                runtime.tick();org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).until(()->runtime.health().queuedInputs()==0);
                var old=f.store().state(legacy.key(),f.reads()).orElseThrow().value();
                assertThat(old.retiredAt()).isNull();assertThat(old.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
                assertThat(old.operationalEvidence().failedCapabilities()).isNotZero();
                assertThat(runtime.health().degraded()).isTrue();
                assertThat(f.store().state(currentBinding.key(),f.reads()).orElseThrow().value().retiredAt()).isNull();
            }
        });
    }

}
