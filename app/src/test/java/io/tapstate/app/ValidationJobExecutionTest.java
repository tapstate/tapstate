package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.jet.core.*;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.assertj.core.api.Assertions.*;

class ValidationJobExecutionTest {
    private static final Map<String,State> STATES=new ConcurrentHashMap<>();
    @Test @Timeout(35) void completedJetDoesNotInventBusinessCompletionOrReaderDrain() throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();state.finish.set(true);var execution=fixture.execution(state);execution.start();
            await(()->state.exited.get());
            assertThat(execution.progress().businessComplete()).isFalse();assertThat(execution.progress().readersReleased()).isFalse();
            state.complete.set(true);state.readers.set(true);assertThat(execution.progress().businessComplete()).isTrue();
            await(()->execution.progress().processorsExited());execution.releaseFactories();
        }
    }
    @Test @Timeout(35) void cancellationSignalsLocallyAndMaintainsOnlyTheOriginalJob() throws Exception {
        try(var fixture=new Fixture()) {
            var state=new State();var execution=fixture.execution(state);execution.start();await(()->state.entered.get());
            String original=execution.jobName();execution.cancel();assertThat(state.cancelled).isTrue();
            execution.maintain();await(()->execution.progress().processorsExited());
            assertThat(execution.jobName()).isEqualTo(original);assertThat(execution.progress().readersReleased()).isFalse();
            assertThatThrownBy(execution::releaseFactories).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            state.readers.set(true);execution.releaseFactories();
        }
    }
    private static final class State implements ValidationJobExecution.Work {
        final AtomicBoolean finish=new AtomicBoolean(),entered=new AtomicBoolean(),exited=new AtomicBoolean(),complete=new AtomicBoolean(),readers=new AtomicBoolean(),cancelled=new AtomicBoolean();
        public boolean businessComplete(){return complete.get();}public boolean readersReleased(){return readers.get();}public Throwable failure(){return null;}public void cancel(){cancelled.set(true);}
    }
    private static final class Probe extends AbstractProcessor {
        final String id;Probe(String id){this.id=id;}
        @Override protected void init(Context context){STATES.get(id).entered.set(true);}
        @Override public boolean complete(){return STATES.get(id).finish.get();}
        @Override public void close(){STATES.get(id).exited.set(true);}
    }
    private static final class Factory implements ProcessorSupplier {
        private static final long serialVersionUID=1L;final String id;Factory(String id){this.id=id;}
        public Collection<? extends Processor> get(int count){return List.of(new Probe(id));}
    }
    private static final class Fixture implements AutoCloseable {
        final HazelcastInstance member;final ValidationProcessorRegistry registry;final List<String> ids=new ArrayList<>();
        Fixture(){
            var config=new Config().setClusterName("validation-job-execution-"+UUID.randomUUID());config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
            config.setProperty("hazelcast.logging.type","none").setProperty("hazelcast.phone.home.enabled","false").setProperty("hazelcast.shutdownhook.enabled","false")
                    .setProperty("hazelcast.operation.thread.count","2").setProperty("hazelcast.operation.generic.thread.count","2").setProperty("hazelcast.io.input.thread.count","1").setProperty("hazelcast.io.output.thread.count","1").setProperty("hazelcast.partition.count","17");
            config.getNetworkConfig().setPort(0).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface("127.0.0.1");var join=config.getNetworkConfig().getJoin();join.getAutoDetectionConfig().setEnabled(false);join.getMulticastConfig().setEnabled(false);join.getTcpIpConfig().setEnabled(false);
            member=Hazelcast.newHazelcastInstance(config);registry=new ValidationProcessorRegistry(member,()->{});
        }
        ValidationJobExecution execution(State state){
            String id=UUID.randomUUID().toString();ids.add(id);STATES.put(id,state);
            var identity=new ValidationRunIdentity(new ValidationOwner("cluster","flow","node","boot",1,1,1),"task",id,ValidationMode.FULL,"spec",1,"rules");
            var processors=registry.register(identity,Set.of("check"),()->{});var local=member.getCluster().getLocalMember();
            var graph=new ValidationJobGraph(identity,local.getAddress(),local.getUuid(),new ValidationJobGraph.Limits(1,0,1),processors);graph.vertex("check",new Factory(id));
            return new ValidationJobExecution(identity,graph,processors,member.getJet(),state,()->{},()->{});
        }
        public void close(){registry.close();member.shutdown();ids.forEach(STATES::remove);}
    }
    private static void await(java.util.function.BooleanSupplier ready)throws Exception{long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(8);while(!ready.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(10);assertThat(ready.getAsBoolean()).isTrue();}
}
