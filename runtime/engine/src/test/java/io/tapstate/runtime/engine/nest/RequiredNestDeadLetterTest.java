package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.*;
import java.io.*;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RequiredNestDeadLetterTest {
    private static final PipelineIntentScope SCOPE=new PipelineIntentScope("cluster",2,"cache","pipeline",7);
    private static final OperatorPlacement PLACE=OperatorMapCoordinates.nest(SCOPE,"extra-cache","nest",List.of("items"),OperatorMapCoordinates.NestMap.STATE,null);
    private static OperatorMapBinding binding(String execution){return new OperatorMapBinding(PLACE,execution,execution.repeat(64));}
    private static OperatorRunManifest manifest(OperatorMapBinding binding){return new OperatorRunManifest("pipeline",binding.executionIdentity(),List.of(binding));}
    private static NestVertex vertex(OperatorMapBinding binding){return new NestVertex(List.of("items"),"items",binding.mapName(),List.of("id"),List.of("parent"),List.of());}
    private static ReleasedChild released(){return new ReleasedChild(new NestElement(new ElementRef(List.of("items","children"),1,List.of(42),null),Map.of("id",42),new SourceOrder(9,12),Map.of("orders",new ChainPosition(new SourceOrder(9,12),"token"))),Duration.ofMillis(15));}

    @Test void serializedChannelKeepsOriginalPositionsAndStableConfigCoordinatesAcrossExecutions()throws Exception {
        var f=new Fixture();var original=binding("a");f.maps.install(List.of(original));
        var channel=copy(new RequiredNestDeadLetter(manifest(original),() -> 50L)).bind(f.member,(namespace,count) -> f.gauge.set(count));
        channel.unassemblable(vertex(original),released());
        var context=f.store.context;var fact=f.store.fact;
        assertThat(context).isEqualTo(new RequiredDeadLetterContext("cluster",2,"pipeline","nest",PLACE.pathId(),7,PLACE.mapName()));
        assertThat(fact.cacheIncarnation()).isEqualTo("cache");assertThat(fact.positions()).isEqualTo(released().child().positions());
        assertThat(fact.record().namespace()).isEqualTo(PLACE.mapName());assertThat(fact.record().row()).containsEntry("id",42);
        assertThat(fact.record().heldForMillis()).isEqualTo(15);assertThat(fact.record().discardedAt()).isEqualTo(50);
        assertThat(f.store.expected).containsExactly(0L);assertThat(f.gauge.get()).isEqualTo(1);
        var next=binding("b");f.maps.install(List.of(next));
        var restarted=new RequiredNestDeadLetter(manifest(next),() -> 60L).bind(f.member,(namespace,count) -> f.gauge.set(count));
        restarted.unassemblable(vertex(next),released());assertThat(f.store.context).isEqualTo(context);assertThat(f.store.expected).containsExactly(0L,1L);
        int reads=f.store.reads;assertCode(() -> channel.unassemblable(vertex(original),released()),IoError.WORKLOAD_CLAIM_FENCED);assertThat(f.store.reads).isEqualTo(reads);
    }
    @Test void failedCasNeverReadsAgainOrAdvancesTheDiscardGauge() {
        var f=new Fixture();var original=binding("a");f.maps.install(List.of(original));f.store.accept=false;
        var channel=new RequiredNestDeadLetter(manifest(original)).bind(f.member,(namespace,count) -> f.gauge.set(count));
        assertCode(() -> channel.unassemblable(vertex(original),released()),IoError.REQUIRED_DEAD_LETTER_CONFLICT);
        assertThat(f.store.reads).isEqualTo(1);assertThat(f.store.expected).containsExactly(0L);assertThat(f.gauge.get()).isZero();assertThat(f.store.fact).isNull();
    }
    @Test void replacingTheOriginalMapDuringReadRefusesBeforeTheConfigWrite() {
        var f=new Fixture();var original=binding("a");f.maps.install(List.of(original));
        var channel=new RequiredNestDeadLetter(manifest(original)).bind(f.member,(namespace,count) -> f.gauge.set(count));
        f.store.afterRead=() -> f.maps.install(List.of(binding("b")));
        assertCode(() -> channel.unassemblable(vertex(original),released()),IoError.WORKLOAD_CLAIM_FENCED);
        assertThat(f.store.reads).isEqualTo(1);assertThat(f.store.expected).isEmpty();assertThat(f.gauge.get()).isZero();
    }
    @Test void anotherPathOrHotRunCannotUseThisNamespace() {
        var f=new Fixture();var original=binding("a");f.maps.install(List.of(original));var channel=new RequiredNestDeadLetter(manifest(original)).bind(f.member,(namespace,count) -> f.gauge.set(count));
        var wrong=new NestVertex(List.of("other"),"other",original.mapName(),List.of("id"),List.of(),List.of());
        assertCode(() -> channel.unassemblable(wrong,released()),IoError.WORKLOAD_CLAIM_FENCED);
        assertCode(() -> channel.unassemblable(vertex(binding("b")),released()),IoError.WORKLOAD_CLAIM_FENCED);assertThat(f.store.reads).isZero();
    }
    @Test void missingProviderOrUninstalledOriginalNeverUsesLegacyDeadLetterState() {
        var f=new Fixture();var original=binding("a");f.context.clear();f.context.put(DurableNestDeadLetter.USER_CONTEXT_KEY,new Object());
        assertCode(() -> new RequiredNestDeadLetter(manifest(original)).bind(f.member,(namespace,count) -> f.gauge.set(count)),IoError.WORKLOAD_CLAIM_FENCED);
        var uninstalled=new Fixture();assertCode(() -> new RequiredNestDeadLetter(manifest(original)).bind(uninstalled.member,(namespace,count) -> uninstalled.gauge.set(count)),IoError.WORKLOAD_CLAIM_FENCED);
        assertThat(uninstalled.store.reads).isZero();
    }
    @Test void anUnboundChannelCannotAcknowledgeAReleasedChild() {
        assertCode(() -> new RequiredNestDeadLetter(manifest(binding("a"))).unassemblable(vertex(binding("a")),released()),IoError.WORKLOAD_CLAIM_FENCED);
    }
    private static void assertCode(Runnable action,io.tapstate.core.common.TapstateErrorCode expected){assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(expected));}
    private static RequiredNestDeadLetter copy(RequiredNestDeadLetter value)throws Exception{var bytes=new ByteArrayOutputStream();try(var out=new ObjectOutputStream(bytes)){out.writeObject(value);}try(var in=new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))){return(RequiredNestDeadLetter)in.readObject();}}
    private static final class Fixture {
        final Map<String,Object> context=new ConcurrentHashMap<>();final Store store=new Store();final AtomicLong gauge=new AtomicLong();
        final HazelcastInstance member=(HazelcastInstance)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{HazelcastInstance.class},(proxy,method,args) -> switch(method.getName()){case "getUserContext" -> context;case "getName" -> "required-letter-test";case "getDistributedObjects" -> List.of();default -> throw new AssertionError(method.getName());});
        final OperatorMapBindings maps;
        Fixture(){var config=new Config();RequiredNestDeadLetter.bindTo(config,(original,guard) -> store.guardedBy(guard));context.putAll(config.getUserContext());maps=OperatorMapBindings.bindTo(member,(original,guard) -> (KeyedStateStore)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{KeyedStateStore.class},(proxy,method,args) -> {throw new AssertionError("dead letters must not use operator payload state");}));}
    }
    private static final class Store implements RequiredDeadLetterStore {
        int reads;boolean accept=true;Runnable afterRead=() -> {};RequiredDeadLetterContext context;RequiredDeadLetterFact fact;long revision;
        final List<Long> expected=new ArrayList<>();
        public RequiredDeadLetterStore guardedBy(Runnable active){var self=this;return new RequiredDeadLetterStore(){
            public RequiredDeadLetterStore guardedBy(Runnable next){return self.guardedBy(() -> {active.run();next.run();});}
            public Optional<RequiredDeadLetterSnapshot> find(RequiredDeadLetterContext context,String element){active.run();var result=self.find(context,element);active.run();return result;}
            public List<RequiredDeadLetterSnapshot> read(RequiredDeadLetterContext context,int limit){throw new AssertionError("running callback must not scan diagnostics");}
            public boolean record(RequiredDeadLetterContext context,long expected,RequiredDeadLetterFact fact){active.run();var result=self.record(context,expected,fact);active.run();return result;}
            public void dropNamespace(RequiredDeadLetterContext context,String operation){throw new AssertionError("running callback must not retire diagnostics");}
        };}
        public Optional<RequiredDeadLetterSnapshot> find(RequiredDeadLetterContext context,String element){reads++;afterRead.run();return fact==null?Optional.empty():Optional.of(new RequiredDeadLetterSnapshot(revision,fact));}
        public List<RequiredDeadLetterSnapshot> read(RequiredDeadLetterContext context,int limit){throw new AssertionError("no list");}
        public boolean record(RequiredDeadLetterContext context,long expected,RequiredDeadLetterFact fact){this.expected.add(expected);if(!accept)return false;assertThat(expected).isEqualTo(revision);this.context=context;this.fact=fact;revision++;return true;}
        public void dropNamespace(RequiredDeadLetterContext context,String operation){throw new AssertionError("no retirement");}
    }
}
