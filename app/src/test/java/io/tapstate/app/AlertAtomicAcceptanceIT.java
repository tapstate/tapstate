package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static io.tapstate.app.AlertNotificationPlannerIT.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class AlertAtomicAcceptanceIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void anExplicitUnmatchedRouteStillAcceptsStateWithoutAnyNotificationLedgerWork(){
        AlertEvaluationHostIT.withFixture(f->{var route=route(f,Duration.ZERO);
            var unmatched=new AlertConfiguration.Match(route.match().resources(),Set.of("pipeline.observation-stale"),Set.of());
            f.store().saveDefinition(route,route.edit(route.name(),unmatched,true,route.channels(),route.groupBy(),route.grouping(),route.inhibition(),T,"operator"),access());
            var planner=planner(f,"no-eligible-route");f.evaluator().withPlanner(planner);bad(f);planner.tick();
            assertThat(f.store().active(null,10,f.reads()).total()).isOne();
            var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            assertThat(f.store().history(binding.key(),null,10,f.reads()).rows()).hasSize(1);
            assertThat(f.store().planning().schedules()).isEmpty();assertThat(intents(f)).isEmpty();
        });
    }
    @Test void aLostSuccessfulStateAndPendingPlanReplyReplaysOnlyTheOriginalEpisodeAndTargetKeys(){
        AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);var injected=new AtomicBoolean();
            var wrapper=(AlertStore)Proxy.newProxyInstance(AlertStore.class.getClassLoader(),new Class<?>[]{AlertStore.class},(proxy,method,args)->{
                try{var result=method.invoke(f.store(),args);
                    if(method.getName().equals("commit")&&args[0] instanceof AlertStore.Commit commit&&commit.state()!=null&&injected.compareAndSet(false,true)){
                        assertThat(result).isEqualTo(AlertStore.Outcome.STAGED);
                        throw new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","controlled-lost-successful-commit-reply"),null);
                    }
                    return result;
                }catch(InvocationTargetException failure){throw failure.getCause();}
            });
            var planner=new AlertNotificationPlanner(wrapper,f.clock(),"original-acceptance",null,ACTIVE);
            var evaluator=new AlertEvaluationHost(wrapper,BuiltinAlarmTypes.catalog(),f.storeHost().deployment(),f.clock(),"original-evaluator",ACTIVE).withPlanner(planner);
            var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));var input=new AlertInput.Conditions(T,AlertSignal.Condition.BAD,null);
            assertThatThrownBy(()->evaluator.accept(binding,1,input)).isInstanceOf(TapstateException.class);assertThat(injected).isTrue();
            var original=f.store().state(binding.key(),f.reads()).orElseThrow();
            assertThat(original.value().snapshot().episode().number()).isOne();assertThat(f.store().planning().schedules()).hasSize(2);assertThat(intents(f)).isEmpty();
            assertThat(evaluator.accept(binding,1,input)).isEqualTo(AlertEvaluationHost.Result.IGNORED);
            assertThat(f.store().state(binding.key(),f.reads()).orElseThrow()).isEqualTo(original);
            f.now().set(T.plusSeconds(31));planner(f,"reopened-acceptance").tick();var ids=intents(f).stream().map(row->row.value().frozen().identity()).toList();
            assertThat(ids).hasSize(2).doesNotHaveDuplicates();planner(f,"another-owner").tick();
            assertThat(intents(f).stream().map(row->row.value().frozen().identity())).containsExactlyInAnyOrderElementsOf(ids);
            assertThat(f.store().history(binding.key(),null,10,f.reads()).rows()).hasSize(1);
        });
    }
}
