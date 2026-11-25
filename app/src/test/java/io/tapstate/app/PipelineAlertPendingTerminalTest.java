package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PipelineAlertPendingTerminalTest {
    @ParameterizedTest @ValueSource(booleans={false,true})
    void aPendingStoppedFinalDoesNotAcquireAnotherReplaceableTimer(boolean timerFails) throws Exception {
        exercise(timerFails,"steady");
    }
    @ParameterizedTest @ValueSource(strings={"changed-rule","real-publication","running","accepted-timer-healthy","accepted-timer-failure","final-failure"})
    void pendingDeferralPreservesUncoveredWork(String mode) throws Exception {
        exercise(mode.equals("accepted-timer-failure"),mode);
    }
    private void exercise(boolean timerFails,String mode) throws Exception {
        var f=new PipelineAlertRuntimeTest();var catalog=BuiltinAlarmTypes.catalog();
        var scope=AlertEvaluationHostTest.definition("pipeline.failed").header().identity().scope();
        var rules=new ArrayList<AlertConfiguration.Definition>();var providers=new HashSet<String>();
        for(var type:catalog.types()) {
            var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"builtin-"+type.id()),AlertChannelPolicy.Deployment.ON_PREM,PipelineAlertRuntimeTest.T,"operator");
            rules.add(new AlertConfiguration.Rule(header,"Rule",type.id(),type.version(),new AlertConfiguration.Resources(type.resourceKinds(),true,Set.of()),type.validatedParameters(Map.of()),null,true,null).validated(catalog).enrolled(null));providers.add(type.providerId());
        }
        when(f.fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(rules,null));
        var finalEntered=new CountDownLatch(1);var finalRelease=new CountDownLatch(1);
        var timerEntered=new CountDownLatch(1);var timerRelease=new CountDownLatch(1);var commits=new AtomicInteger();var timerCalls=new AtomicInteger();
        doAnswer(call->{
            if(commits.incrementAndGet()==1){finalEntered.countDown();assertThat(finalRelease.await(5,TimeUnit.SECONDS)).isTrue();}
            if(mode.equals("final-failure")&&commits.get()==1)throw new io.tapstate.core.common.TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","controlled final failure"),null);
            AlertStore.Commit command=call.getArgument(0);var change=command.state();
            f.rows.compute(change.after().snapshot().rule().key(),(key,before)->new AlertStore.Stored<>(before==null?1:before.revision()+1,change.after()));
            return AlertStore.Outcome.STAGED;
        }).when(f.fixture.store).commit(any());
        Clock clock=new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return f.fixture.now.updateAndGet(value->value.plusNanos(1));}};
        try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(f.fixture.store,f.fixture.host,catalog,null,null,providers),f.intentStore,(original,claim)->{
            boolean timer=StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().equals(PipelineAlertRuntime.class.getName())&&frame.getMethodName().contains("pipelineTimerOffer")));
            if(timer){timerCalls.incrementAndGet();timerEntered.countDown();try{assertThat(timerRelease.await(5,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
                if(timerFails)throw new io.tapstate.core.common.TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","controlled timer read failure"),null);}
            return Optional.of(()->{});
        },()->{},clock)) {
            try {
                runtime.tick();await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
                var pass=f.pass("orders");var actual=mode.equals("running")?PipelineState.RUNNING:PipelineState.STOPPED;
                var intent=new PipelineIntentSnapshot(pass.scope(),1,new DesiredState("orders",actual,"definition"));
                when(pass.intent()).thenReturn(intent);f.intents.put("orders",intent);runtime.expecting(pass);
                boolean previous=mode.startsWith("accepted-timer-");
                if(previous){runtime.tick();assertThat(timerEntered.await(2,TimeUnit.SECONDS)).isTrue();}
                var observation=PipelineAlertRuntimeTest.observation("orders",PipelineAlertRuntimeTest.T,actual);
                TerminalPublication.Receipt receipt;
                if(actual==PipelineState.RUNNING){runtime.published(pass,observation,null);receipt=TerminalPublication.failed();}
                else receipt=runtime.terminalPublished(pass,TerminalPublication.from(pass,7,observation),observation,null,true);
                if(previous){for(int i=0;i<5;i++)runtime.tick();timerRelease.countDown();}
                assertThat(finalEntered.await(2,TimeUnit.SECONDS)).isTrue();
                if(actual==PipelineState.STOPPED)assertThat(receipt.status()).isEqualTo(mode.equals("accepted-timer-failure")?TerminalPublication.Status.FAILED:TerminalPublication.Status.PENDING);
                if(mode.equals("changed-rule")){
                    var original=(AlertConfiguration.Rule)rules.getFirst();var changed=new AlertConfiguration.Rule(original.header().next(clock.instant(),"operator"),"Edited",original.typeId(),original.typeVersion(),original.resources(),original.parameters(),original.severityOverride(),original.enabled(),original.metric()).validated(catalog).enrolled(null);
                    var replacements=new ArrayList<>(rules);replacements.set(0,changed);
                    when(f.fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(replacements,null));
                    runtime.tick();await().atMost(Duration.ofSeconds(2)).until(()->receipt.status()==TerminalPublication.Status.FAILED);
                }
                if(mode.equals("real-publication"))runtime.published(pass,PipelineAlertRuntimeTest.observation("orders",clock.instant(),actual),null);
                for(int i=0;i<5;i++)runtime.tick();
                finalRelease.countDown();
                if(mode.equals("final-failure")){
                    await().atMost(Duration.ofSeconds(2)).until(()->receipt.status()==TerminalPublication.Status.FAILED);
                    runtime.tick();
                } else await().atMost(Duration.ofSeconds(2)).until(()->commits.get()>=(actual==PipelineState.RUNNING?1:2));
                if(!mode.equals("steady"))assertThat(timerEntered.await(2,TimeUnit.SECONDS)).isTrue();
                timerRelease.countDown();await().atMost(Duration.ofSeconds(3)).until(()->runtime.health().queuedInputs()==0);
                boolean failure=List.of("changed-rule","real-publication","running","accepted-timer-failure","final-failure").contains(mode);
                assertThat(receipt.status()).isEqualTo(failure?TerminalPublication.Status.FAILED:TerminalPublication.Status.COMPLETE);
                assertThat(receipt.seal()).isEqualTo(!failure);
                assertThat(timerCalls).hasValue(mode.equals("steady")?0:mode.equals("accepted-timer-failure")?2:1);
                if(mode.equals("real-publication"))assertThat(commits.get()).isGreaterThanOrEqualTo(3);
            }finally{finalRelease.countDown();timerRelease.countDown();}
        }
    }
}
