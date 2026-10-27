package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Necessary observations keep their stable CONFIG identity; only original run coordinates cross the job boundary. */
public final class RequiredNestDeadLetter implements NestDeadLetter {
    private static final long serialVersionUID=1L;
    private static final String KEY="tapstate.nest.required-dead-letters";
    @FunctionalInterface public interface Provider {
        RequiredDeadLetterStore open(OperatorMapBinding original,Runnable requireBindingActive);
    }
    private final OperatorRunManifest manifest;
    private final NestClock clock;
    public RequiredNestDeadLetter(OperatorRunManifest manifest){this(manifest,NestClock.SYSTEM);}
    public RequiredNestDeadLetter(OperatorRunManifest manifest,NestClock clock){this.manifest=Objects.requireNonNull(manifest,"manifest");this.clock=Objects.requireNonNull(clock,"clock");}
    public static void bindTo(Config config,Provider provider){if(config.getUserContext().putIfAbsent(KEY,Objects.requireNonNull(provider,"provider"))!=null)throw fenced();}
    @Override public NestDeadLetter bind(HazelcastInstance member){return bind(member,new JetNestDeadLetterGauge());}
    NestDeadLetter bind(HazelcastInstance member,NestDeadLetterGauge gauge) {
        if(!(member.getUserContext().get(KEY)instanceof Provider provider))throw fenced();
        var maps=OperatorMapBindings.resolve(member,true);var targets=new HashMap<String,Target>();
        for(var binding:manifest.bindings()) {
            var placement=binding.placement();
            if(placement.resource()!=OperatorPlacement.Resource.NEST_STATE || !placement.namespace().equals("state"))continue;
            var guard=maps.guard(binding);guard.run();
            var store=Objects.requireNonNull(provider.open(binding,guard),"required dead-letter store").guardedBy(guard);guard.run();
            var scope=placement.scope();
            var context=new RequiredDeadLetterContext(scope.clusterId(),scope.bindingVersion(),scope.pipelineId(),placement.stepId(),placement.pathId(),scope.stateGeneration(),placement.mapName());
            targets.put(binding.mapName(),new Target(placement,context,store,guard));
        }
        return new Bound(Map.copyOf(targets),clock,gauge);
    }
    @Override public void unassemblable(NestVertex from,ReleasedChild released){throw fenced();}
    private record Target(OperatorPlacement placement,RequiredDeadLetterContext context,RequiredDeadLetterStore store,Runnable guard){}
    private static final class Bound implements NestDeadLetter {
        private static final long serialVersionUID=1L;
        private final Map<String,Target> targets;
        private final NestClock clock;
        private final NestDeadLetterGauge gauge;
        private final Map<String,AtomicLong> counts=new ConcurrentHashMap<>();
        Bound(Map<String,Target> targets,NestClock clock,NestDeadLetterGauge gauge){this.targets=targets;this.clock=clock;this.gauge=gauge;}
        @Override public void unassemblable(NestVertex from,ReleasedChild released) {
            var target=targets.get(Objects.requireNonNull(from,"from").mapName());
            if(target==null || !target.placement().pathId().equals(OperatorMapCoordinates.path(from.pathId())))throw fenced();
            var child=Objects.requireNonNull(released,"released").child();var context=target.context();
            String element=NestStateKeys.nameOf(child.ref().pathId(),child.ref().elementKey());
            var record=new NestDeadLetterRecord(context.namespace(),element,String.join(",",child.positions().keySet()),child.order().epoch()+":"+child.order().seq(),released.heldFor().toMillis(),clock.millis(),child.fields());
            var fact=new RequiredDeadLetterFact(target.placement().scope().cacheIncarnation(),child.positions(),record);
            target.guard().run();var before=target.store().find(context,element);target.guard().run();
            long revision=before.map(RequiredDeadLetterSnapshot::revision).orElse(0L);
            if(!target.store().record(context,revision,fact))throw new TapstateException(IoError.REQUIRED_DEAD_LETTER_CONFLICT,Map.of(),null);
            target.guard().run();gauge.handedOver(from.mapName(),counts.computeIfAbsent(from.mapName(),ignored -> new AtomicLong()).incrementAndGet());
        }
    }
    private static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
}
