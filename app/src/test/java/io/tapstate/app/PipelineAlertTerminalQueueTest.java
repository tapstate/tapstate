package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PipelineAlertTerminalQueueTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void aSlowStoppedTickBatchCannotConsumeEveryFinalPublicationSlot(boolean refresh) throws Exception {
        var f=new PipelineAlertRuntimeTest();var catalog=BuiltinAlarmTypes.catalog();
        var original=AlertEvaluationHostTest.definition("pipeline.failed");var rules=new ArrayList<AlertConfiguration.Rule>();
        for(var type:catalog.types()) {
            var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,
                    original.header().identity().scope(),"builtin-"+type.id()),AlertChannelPolicy.Deployment.ON_PREM,PipelineAlertRuntimeTest.T,"operator");
            rules.add(new AlertConfiguration.Rule(header,"Rule "+type.id(),type.id(),type.version(),
                    new AlertConfiguration.Resources(type.resourceKinds(),true,Set.of()),type.validatedParameters(Map.of()),null,true,null).validated(catalog).enrolled(null));
        }
        when(f.fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(new ArrayList<AlertConfiguration.Definition>(rules),null));
        var providers=new HashSet<String>();catalog.types().forEach(type->providers.add(type.providerId()));
        Clock clock=new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return f.fixture.now.updateAndGet(value->value.plusNanos(1));}};
        var entered=new CountDownLatch(2);var release=new CountDownLatch(1);var blocking=new AtomicBoolean();
        doAnswer(call->{
            if(blocking.get()){entered.countDown();assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}
            return Optional.ofNullable(f.rows.get(call.getArgument(0)));
        }).when(f.fixture.store).state(any(),any());
        try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(f.fixture.store,f.fixture.host,catalog,null,null,providers),
                f.intentStore,(scope,claim)->{
                    if(blocking.get()){entered.countDown();try{assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}}
                    return Optional.of(()->{});
                },()->{},clock)) {
            try {
                runtime.tick();await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
                assertThat(rules).hasSize(7);assertThat(rules).allSatisfy(rule->{assertThat(rule.enabled()).isTrue();assertThat(rule.metric()).isNull();});
                PipelineConvergencePass target=null;
                for(int i=0;i<70;i++) {
                    var pass=f.pass("stop-"+i);var intent=new PipelineIntentSnapshot(pass.scope(),1,new DesiredState(pass.scope().pipelineId(),PipelineState.STOPPED,"definition"));
                    when(pass.intent()).thenReturn(intent);f.intents.put(pass.scope().pipelineId(),intent);runtime.expecting(pass);if(i==0)target=pass;
                }
                assertThat(runtime.needsFrequentTerminalSamples(target.scope(),PipelineState.STOPPED)).isFalse();
                blocking.set(true);runtime.tick();assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                if(refresh) {
                    long reads=mockingDetails(f.fixture.store).getInvocations().stream().filter(call->call.getMethod().getName().equals("evaluationSnapshot")).count();
                    runtime.tick();
                    long after=mockingDetails(f.fixture.store).getInvocations().stream().filter(call->call.getMethod().getName().equals("evaluationSnapshot")).count();
                    boolean frequent=runtime.needsFrequentTerminalSamples(target.scope(),PipelineState.STOPPED);
                    assertThat(after).isEqualTo(reads);assertThat(frequent).isFalse();return;
                }
                var observation=PipelineAlertRuntimeTest.observation(target.scope().pipelineId(),PipelineAlertRuntimeTest.T,PipelineState.STOPPED);
                var receipt=runtime.terminalPublished(target,TerminalPublication.from(target,7,observation),observation,null,true);
                assertThat(f.rows).isEmpty();assertThat(receipt.status()).isEqualTo(TerminalPublication.Status.PENDING);
                long rejected=runtime.health().rejected();for(int i=0;i<5;i++)runtime.tick();
                assertThat(runtime.health().rejected()).isEqualTo(rejected);
                blocking.set(false);release.countDown();
                await().atMost(Duration.ofSeconds(10)).until(()->{runtime.tick();return receipt.status()==TerminalPublication.Status.COMPLETE;});
                assertThat(receipt.status()).isEqualTo(TerminalPublication.Status.COMPLETE);
                assertThat(f.rows.values()).anySatisfy(row->assertThat(row.value().snapshot().rule().key().resource().id()).isEqualTo("stop-0"));
            } finally {blocking.set(false);release.countDown();}
        }
    }
    @Test void deferredTimerTurnsReachBothResourcesAndPipelines() throws Exception {
        var f=new PipelineAlertRuntimeTest();var catalog=BuiltinAlarmTypes.catalog();
        var scope=AlertEvaluationHostTest.definition("pipeline.failed").header().identity().scope();
        var rules=new ArrayList<AlertConfiguration.Definition>();var providers=new HashSet<String>();
        for(var type:catalog.types()) {
            var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"builtin-"+type.id()),AlertChannelPolicy.Deployment.ON_PREM,PipelineAlertRuntimeTest.T,"operator");
            rules.add(new AlertConfiguration.Rule(header,"Rule",type.id(),type.version(),new AlertConfiguration.Resources(type.resourceKinds(),true,Set.of()),type.validatedParameters(Map.of()),null,true,null).validated(catalog).enrolled(null));
            providers.add(type.providerId());
        }
        when(f.fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(rules,null));
        var entered=new CountDownLatch(2);var release=new CountDownLatch(1);var blocking=new AtomicBoolean();
        Set<String> visited=ConcurrentHashMap.newKeySet();
        java.util.function.Consumer<String> probe=id->{visited.add(id);if(blocking.get()){entered.countDown();try{assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}}};
        var evaluator=spy(f.fixture.host);
        doAnswer(call->{probe.accept(((AlertEvaluationHost.Binding)call.getArgument(0)).resource().id());return call.callRealMethod();}).when(evaluator).tick(any(AlertEvaluationHost.Binding.class));
        doAnswer(call->{probe.accept(((AlertEvaluationHost.Binding)call.getArgument(0)).resource().id());return call.callRealMethod();}).when(evaluator).tick(any(AlertEvaluationHost.Binding.class),any());
        try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(f.fixture.store,evaluator,catalog,null,null,providers),f.intentStore,(original,claim)->{probe.accept(original.pipelineId());return Optional.of(()->{});},()->{},f.fixture.clock)) {
            try {
                runtime.tick();await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
                for(int i=0;i<70;i++) {
                    String sourceId="source-"+i;
                    runtime.resourceInput(new AlarmKey.Resource("source",sourceId,"one"),"source.connection-failed",1,"producer",()->{},()->probe.accept(sourceId),new AlertInput.Operational(PipelineAlertRuntimeTest.T,"a".repeat(64),OperationalEvidence.Capability.INITIALIZE,false,"b".repeat(64),null));
                    await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
                    var pass=f.pass("stop-"+i);var intent=new PipelineIntentSnapshot(pass.scope(),1,new DesiredState(pass.scope().pipelineId(),PipelineState.STOPPED,"definition"));
                    when(pass.intent()).thenReturn(intent);f.intents.put(pass.scope().pipelineId(),intent);runtime.expecting(pass);
                }
                visited.clear();f.fixture.now.set(PipelineAlertRuntimeTest.T.plusSeconds(300));
                blocking.set(true);runtime.tick();assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                blocking.set(false);release.countDown();
                for(int turn=0;turn<6;turn++) {
                    await().atMost(Duration.ofSeconds(3)).until(()->runtime.health().queuedInputs()==0);
                    runtime.tick();
                }
                assertThat(visited.stream().filter(id->id.startsWith("source-")).count()).isEqualTo(70);
                assertThat(visited.stream().filter(id->id.startsWith("stop-")).count()).isEqualTo(70);
                assertThat(runtime.health().queuedInputs()).isLessThanOrEqualTo(64);
            }finally{blocking.set(false);release.countDown();}
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void onlyPipelineOwnedOperationalReportsRetainTheOriginalGuardMarker(boolean owned) {
        var f=new PipelineAlertRuntimeTest();var catalog=BuiltinAlarmTypes.catalog();var type=catalog.types().stream().filter(value->value.id().equals("source.connection-failed")).findFirst().orElseThrow();
        var scope=AlertEvaluationHostTest.definition("pipeline.failed").header().identity().scope();
        var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"source-rule"),AlertChannelPolicy.Deployment.ON_PREM,PipelineAlertRuntimeTest.T,"operator");
        var rule=new AlertConfiguration.Rule(header,"Source",type.id(),type.version(),new AlertConfiguration.Resources(type.resourceKinds(),true,Set.of()),type.validatedParameters(Map.of()),null,true,null).validated(catalog).enrolled(null);
        when(f.fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(List.of(rule),null));
        var originalReads=new AtomicInteger();
        try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(f.fixture.store,f.fixture.host,catalog,null,null,Set.of(type.providerId())),f.intentStore,(scopeValue,claim)->Optional.of(()->{}),()->{},f.fixture.clock)) {
            runtime.tick();await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
            var pass=f.pass("orders");runtime.expecting(pass);
            var dependency=owned?runtime.pipelineGuard(new ExecutionFence(pass.scope(),pass.claim()),originalReads::incrementAndGet):null;
            runtime.resourceInput(new AlarmKey.Resource("source","erp","one"),type.id(),1,"producer",owned?dependency:originalReads::incrementAndGet,()->{},
                    new AlertInput.Operational(PipelineAlertRuntimeTest.T,"a".repeat(64),OperationalEvidence.Capability.INITIALIZE,false,"b".repeat(64),null));
            if(dependency!=null)dependency.close();
            await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
            var commits=mockingDetails(f.fixture.store).getInvocations().stream().filter(call->call.getMethod().getName().equals("commit"))
                    .map(call->(AlertStore.Commit)call.getArgument(0)).toList();
            assertThat(commits).isNotEmpty();
            for(var commit:commits) {
                var proof=commit.state().proof();assertThat(proof).isNotNull();
                assertThat(proof.requireCurrent() instanceof AlertStore.EvaluationProof.PipelineOperationGuard).isEqualTo(owned);
                int before=originalReads.get();proof.requireCurrent().run();
                assertThat(originalReads.get()).isEqualTo(before+(owned?1:0));
            }
        }
    }

}
