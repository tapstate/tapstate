package io.tapstate.app;

import io.tapstate.adapters.alerting.NotificationRenderer;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.lang.reflect.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

/** Controlled render failures join real CONFIG transactions; wire-size classification is verified by the renderer tests. */
@RequiresDocker
class AlertRenderingContractIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @ParameterizedTest @ValueSource(booleans={false,true})
    void renderFailureAndOriginalRoundProgressRemainAtomicAcrossFailureAndUnknownCommit(boolean committed){
        AlertEvaluationHostIT.withFixture(f->{var route=route(f,Duration.ZERO);var receiver=(AlertReceiver)f.store().definition(AlertConfiguration.Kind.RECEIVER,"receiver",access()).orElseThrow();
            var email=(AlertReceiver.Email)receiver.transport();var single=new AlertReceiver.Email(email.host(),email.port(),email.encryption(),email.authentication(),email.username(),email.from(),List.of("a@example.test"),email.trust());
            f.store().saveReceiver(receiver,receiver.edit(receiver.name(),true,single,null,T,"operator"),null,false,access());
            var interrupted=new AtomicBoolean();var originalKey=new AtomicReference<AlertIntent.Identity>();
            var store=(AlertStore)Proxy.newProxyInstance(AlertStore.class.getClassLoader(),new Class<?>[]{AlertStore.class},(proxy,method,args)->{
                boolean fail=method.getName().equals("commit")&&args[0] instanceof AlertStore.Commit command
                        &&command.intents().stream().anyMatch(change->change.after().frozen().content().renderingFailed())&&interrupted.compareAndSet(false,true);
                if(fail){var command=(AlertStore.Commit)args[0];originalKey.set(command.intents().getFirst().after().frozen().identity());
                    if(committed)assertThat(invoke(f.store(),method,args)).isEqualTo(AlertStore.Outcome.STAGED);
                    throw new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail",committed?"controlled-unknown-commit":"controlled-before-commit"),null);
                }return invoke(f.store(),method,args);
            });
            var fails=new AtomicBoolean(true);var actual=new NotificationRenderer();var controlled=mock(NotificationRenderer.class);
            when(controlled.render(any(),any())).thenAnswer(call->fails.get()?new NotificationRenderer.Result(null,NotificationRenderer.Failure.PAYLOAD_BUDGET)
                    :actual.render(call.getArgument(0),call.getArgument(1)));
            var first=new AlertNotificationPlanner(store,f.clock(),"render-original",null,ACTIVE);org.springframework.test.util.ReflectionTestUtils.setField(first,"renderer",controlled);
            f.evaluator().withPlanner(first);bad(f);var version=f.store().planning().version();
            assertThatThrownBy(first::tick).isInstanceOf(TapstateException.class);assertThat(interrupted).isTrue();
            var after=f.store().planning();assertThat(after.intents()).hasSize(committed?1:0);
            assertThat(after.schedules().stream().filter(row->row.value().batch()==null).map(row->row.value().clock().round()).toList()).containsExactly(committed?1L:0L);
            if(!committed)assertThat(after.version()).isEqualTo(version);
            assertThat(f.store().active(null,10,f.reads()).total()).isOne();
            f.now().set(T.plusSeconds(31));var resumed=new AlertNotificationPlanner(f.store(),f.clock(),"render-resumed",null,ACTIVE);org.springframework.test.util.ReflectionTestUtils.setField(resumed,"renderer",controlled);
            resumed.tick();resumed.tick();var row=f.store().intent(originalKey.get(),null).orElseThrow();
            assertThat(f.store().deliveries(null,100,null,null,f.reads()).rows()).hasSize(1);assertThat(row.value().state().stopReason()).isEqualTo(AlertIntent.StopReason.RENDER_FAILED);
            assertThat(row.value().state().attemptsConsumed()).isZero();assertThat(row.value().state().payload()).isNull();
            var senderStore=sendingStore(f);var channels=new AlertDeliveryRuntimeIT.Channels(senderStore);
            try(var worker=AlertDeliveryRuntimeIT.runtime(f,senderStore,channels,"render-terminal")){worker.tick();AlertDeliveryRuntimeIT.settled(worker);assertThat(channels.calls).hasValue(0);}
            assertThatThrownBy(()->AlertDeliveryProgress.manual(row.value(),f.clock().instant())).isInstanceOf(TapstateException.class);
            f.now().set(T.plusSeconds(32));recover(f);resumed.tick();resumed.tick();
            var failures=f.store().deliveries(null,100,null,null,f.reads()).rows();assertThat(failures).hasSize(2).allSatisfy(value->{
                assertThat(value.value().frozen().content().renderingFailed()).isTrue();assertThat(value.value().state().attemptsConsumed()).isZero();});
            fails.set(false);f.now().set(T.plusSeconds(33));var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            assertThat(f.evaluator().accept(binding,3,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
            resumed.tick();var all=f.store().deliveries(null,100,null,null,f.reads()).rows();assertThat(all).hasSize(3);
            assertThat(all.stream().filter(value->!value.value().frozen().content().renderingFailed())).hasSize(1);
            assertThat(f.store().intent(originalKey.get(),null).orElseThrow().value().frozen()).isEqualTo(row.value().frozen());
        });
    }
    private static Object invoke(AlertStore store,Method method,Object[] args)throws Throwable{
        try{return method.invoke(store,args);}catch(InvocationTargetException failure){throw failure.getCause();}
    }
}
