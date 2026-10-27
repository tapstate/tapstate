package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.join.JoinStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;
import java.lang.reflect.Proxy;

class OperatorMapBindingsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","orders",3);
    private static OperatorPlacement placement(OperatorPlacement.Resource resource,String namespace) { return new OperatorPlacement(SCOPE,"cache",resource,"step","root",namespace); }
    private static OperatorMapBinding binding(OperatorPlacement place,int attempt) { return new OperatorMapBinding(place,"attempt-"+attempt,String.format("%064x",attempt)); }
    private static HazelcastInstance member() { return member(() -> {}); }
    private static HazelcastInstance member(Runnable enumeration) {
        var context = new ConcurrentHashMap<String,Object>();
        return (HazelcastInstance)Proxy.newProxyInstance(HazelcastInstance.class.getClassLoader(),new Class<?>[]{HazelcastInstance.class},(proxy,method,args) -> switch(method.getName()) {
            case "getUserContext" -> context;
            case "getDistributedObjects" -> { enumeration.run(); yield List.of(); }
            case "toString" -> "operator-member";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new AssertionError("unexpected member operation: "+method.getName());
        });
    }
    private static void fenced(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED)); }

    @Test void hotNamesChangeByAttemptWhileColdCoordinatesAndConfigurationStayFixed() {
        var place = placement(OperatorPlacement.Resource.NEST_STATE,"root"); var first = binding(place,1); var second = binding(place,2);
        assertThat(first.mapName()).isNotEqualTo(second.mapName()); assertThat(first.configurationPattern()).isEqualTo(second.configurationPattern());
        assertThat(first.placement()).isEqualTo(second.placement());
    }

    @Test void everyMapStoreCallUsesTheOriginalColdNamespace() {
        var member = member(); var cold = new RecordingStore(); var original = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var registry = OperatorMapBindings.bindTo(member,(binding,active) -> { active.run(); return cold; }); registry.install(List.of(original));
        var hot = registry.nest(original.mapName(),"cache");
        hot.load(original.mapName(),"key"); hot.loadAll(original.mapName(),List.of("key")); hot.save(original.mapName(),"key",new byte[]{1});
        hot.saveIfAbsent(original.mapName(),"key",new byte[]{1}); hot.delete(original.mapName(),"key"); hot.count(original.mapName()); hot.dropNamespace(original.mapName());
        assertThat(cold.calls).hasSize(7);
        assertThat(cold.calls).extracting(Call::namespace).containsOnly(original.placement().mapName());
        assertThat(cold.calls).extracting(Call::operation).containsExactly("load","loadAll","save","saveIfAbsent","delete","count","drop");
        fenced(() -> hot.save("foreign", "key",new byte[]{1}));
    }

    @Test void aFinalPreparationCallbackCannotReviveTheOriginalBindingItRetired() {
        var member = member(); var original = binding(placement(OperatorPlacement.Resource.JOIN_FACTS,"facts"),1);
        var registry = OperatorMapBindings.bindTo(member,(binding,active) -> new RecordingStore()); registry.install(List.of(original));
        var old = registry.join(original.mapName());
        fenced(() -> registry.install(List.of(original),() -> assertThat(registry.release(original)).isTrue()));
        fenced(() -> registry.join(original.mapName())); fenced(() -> old.count(original.mapName()));
    }

    @Test void oldHandlesAndLateReleasesCannotTouchTheNewSlot() {
        var member = member(); var cold = new RecordingStore(); var place = placement(OperatorPlacement.Resource.JOIN_FACTS,"facts");
        var first = binding(place,1); var second = binding(place,2);
        var registry = OperatorMapBindings.bindTo(member,(binding,active) -> { active.run(); return cold; });
        registry.install(List.of(first)); var old = registry.join(first.mapName()); registry.install(List.of(second));
        fenced(() -> old.save(first.mapName(),"key",new byte[]{1})); fenced(() -> old.delete(first.mapName(),"key"));
        fenced(() -> registry.join(first.mapName())); assertThat(registry.release(first)).isFalse();
        registry.join(second.mapName()).save(second.mapName(),"key",new byte[]{2}); assertThat(cold.calls).extracting(Call::operation).containsExactly("save");
    }

    @Test void aReleasedLatestSlotCannotBeResurrectedByTheSameOriginalInstall() {
        var original = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var registry = OperatorMapBindings.bindTo(member(),(binding,active) -> new RecordingStore());
        registry.install(List.of(original)); var old = registry.nest(original.mapName(),null);
        assertThat(registry.release(original)).isTrue(); assertThat(registry.release(original)).isFalse();
        fenced(() -> registry.install(List.of(original))); fenced(() -> old.load(original.mapName(),"key"));
    }

    @Test void aReentrantRetirementOfTheSameSlotCannotBeReportedAsSuccessfulInstallation() {
        var original = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var added = binding(placement(OperatorPlacement.Resource.JOIN_FACTS,"facts"),1);
        var owner = new AtomicReference<OperatorMapBindings>(); var opens = new AtomicInteger(); var addedGuard = new AtomicReference<Runnable>();
        var registry = OperatorMapBindings.bindTo(member(),(binding,active) -> {
            if (opens.incrementAndGet() == 2) assertThat(owner.get().release(original)).isTrue();
            if (binding.equals(added)) addedGuard.set(active);
            return new RecordingStore();
        }); owner.set(registry); registry.install(List.of(original));
        fenced(() -> registry.install(List.of(original,added))); fenced(() -> registry.nest(original.mapName(),null));
        fenced(() -> registry.join(added.mapName())); fenced(() -> addedGuard.get().run());
    }

    @Test void theFinalMapLivenessCallbackCannotReplaceTheOriginalSlotBeforePublication() {
        var place = placement(OperatorPlacement.Resource.NEST_STATE,"root"); var original = binding(place,1); var later = binding(place,2);
        var owner = new AtomicReference<OperatorMapBindings>(); var calls = new AtomicInteger(); var outerGuard = new AtomicReference<Runnable>();
        var member = member(() -> { if (calls.incrementAndGet() == 2) owner.get().install(List.of(later)); });
        var registry = OperatorMapBindings.bindTo(member,(binding,active) -> { if (binding.equals(original)) outerGuard.set(active); return new RecordingStore(); });
        owner.set(registry); fenced(() -> registry.install(List.of(original)));
        fenced(() -> outerGuard.get().run()); assertThat(registry.nest(later.mapName(),null)).isNotNull();
    }

    @Test void everyOriginalSlotIsCapturedBeforeTheFirstMapLivenessCallback() {
        var first = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var second = binding(placement(OperatorPlacement.Resource.JOIN_FACTS,"facts"),1); var later = binding(second.placement(),2);
        var owner = new AtomicReference<OperatorMapBindings>(); var calls = new AtomicInteger();
        var member = member(() -> { if (calls.incrementAndGet() == 1) owner.get().install(List.of(later)); });
        var registry = OperatorMapBindings.bindTo(member,(binding,active) -> new RecordingStore()); owner.set(registry);
        fenced(() -> registry.install(List.of(first,second)));
        assertThat(registry.join(later.mapName())).isNotNull(); fenced(() -> registry.nest(first.mapName(),null));
    }

    @Test void callbackReentryCannotOverwriteTheLaterSlotAndFailedCandidatesAreRetired() {
        var member = member(); var place = placement(OperatorPlacement.Resource.NEST_STATE,"root"); var first = binding(place,1); var later = binding(place,2);
        var owner = new AtomicReference<OperatorMapBindings>(); var once = new AtomicBoolean(); var refusedGuard = new AtomicReference<Runnable>();
        var registry = OperatorMapBindings.bindTo(member,(binding,active) -> {
            if (binding.equals(first)) { refusedGuard.set(active); if (once.compareAndSet(false,true)) owner.get().install(List.of(later)); }
            return new RecordingStore();
        }); owner.set(registry);
        fenced(() -> registry.install(List.of(first))); fenced(() -> refusedGuard.get().run());
        assertThat(registry.nest(later.mapName(),null)).isNotNull(); fenced(() -> registry.nest(first.mapName(),null));
    }

    @Test void candidatesOnOneMemberAreAtomicWhenTheSecondProviderRefuses() {
        var first = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var second = binding(placement(OperatorPlacement.Resource.JOIN_FACTS,"facts"),1); var guard = new AtomicReference<Runnable>();
        var registry = OperatorMapBindings.bindTo(member(),(binding,active) -> {
            if (binding.equals(second)) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            guard.set(active); return new RecordingStore();
        });
        fenced(() -> registry.install(List.of(first,second))); fenced(() -> registry.nest(first.mapName(),null)); fenced(() -> guard.get().run());
    }

    @Test void aMemberCannotReplaceItsCacheOwnerAndClosingDoesNotEnableALegacyFallback() {
        var member = member(); var registry = OperatorMapBindings.bindTo(member,(binding,active) -> new RecordingStore());
        fenced(() -> OperatorMapBindings.bindTo(member,(binding,active) -> new RecordingStore()));
        registry.close(); assertThat(OperatorMapBindings.resolve(member,true)).isSameAs(registry);
        fenced(() -> registry.install(List.of(binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1))));
    }

    @Test void kindDatabaseAndMissingDescriptorsRefuseInsteadOfChoosingAnotherStore() {
        var member = member(); var first = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var joined = binding(placement(OperatorPlacement.Resource.JOIN_FACTS,"facts"),1);
        var registry = OperatorMapBindings.bindTo(member,(binding,active) -> new RecordingStore()); registry.install(List.of(first,joined));
        fenced(() -> registry.nest(first.mapName(),"other")); fenced(() -> registry.join(first.mapName()));
        fenced(() -> registry.nest(joined.mapName(),null)); fenced(() -> registry.join("missing"));
    }

    @Test void requiredFactoriesNeverUseAnOtherwiseAvailableLegacyStore() {
        var member = member(); var legacy = new RecordingStore();
        NestStateMapStoreFactory.bindTo(member,legacy); JoinStateMapStoreFactory.bindTo(member,legacy);
        var properties = new Properties(); properties.setProperty(OperatorMapBindings.SCOPED_PROPERTY,"true");
        for (var loader : List.of(new NestStateMapStoreFactory().newMapStore("nest.example",properties),
                new JoinStateMapStoreFactory().newMapStore("join.example",properties))) {
            fenced(() -> ((MapLoaderLifecycleSupport)loader).init(member,new Properties(),"ignored"));
        }
        assertThat(legacy.calls).isEmpty();
        properties.setProperty(OperatorMapBindings.SCOPED_PROPERTY,"false"); fenced(() -> OperatorMapBindings.required(properties));
    }
    @Test void anInitializingHandleDoesNoColdIoUntilItsCompleteOriginalDescriptorIsAdmitted() {
        var original = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1); var cold = new RecordingStore(); var opens = new AtomicInteger();
        var registry = OperatorMapBindings.bindTo(member(),(binding,active) -> { opens.incrementAndGet(); return cold; });
        var waiting = registry.initializingNest(original.mapName(),"cache");
        fenced(() -> waiting.load(original.mapName(),"key")); fenced(() -> waiting.save(original.mapName(),"key",new byte[]{1}));
        assertThat(opens.get()).isZero(); assertThat(cold.calls).isEmpty(); assertThat(registry.awaitingMapNames()).containsExactly(original.mapName());
        registry.attachExisting(List.of(original)); waiting.save(original.mapName(),"key",new byte[]{1});
        assertThat(registry.awaitingMapNames()).isEmpty(); assertThat(opens.get()).isOne();
        assertThat(cold.calls).containsExactly(new Call("save",original.placement().mapName()));
        var next = binding(original.placement(),2); registry.install(List.of(next));
        fenced(() -> waiting.load(original.mapName(),"key")); fenced(() -> registry.attachExisting(List.of(original)));
        assertThat(registry.nest(next.mapName(),null)).isNotNull();
    }

    @Test void restorationRequiresALocalScopedHandleWithTheOriginalKindAndDatabase() {
        var original = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1); var opens = new AtomicInteger();
        var registry = OperatorMapBindings.bindTo(member(),(binding,active) -> { opens.incrementAndGet(); return new RecordingStore(); });
        fenced(() -> registry.attachExisting(List.of(original)));
        registry.initializingJoin(original.mapName()); fenced(() -> registry.attachExisting(List.of(original)));
        var other = binding(placement(OperatorPlacement.Resource.NEST_STATE,"other"),1);
        registry.initializingNest(other.mapName(),"wrong"); fenced(() -> registry.attachExisting(List.of(other)));
        assertThat(opens.get()).isZero();
    }

    @Test void waitingHandlesCannotObserveAHalfPublishedBatchOrKeepFailedCandidatesAlive() {
        var first = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var second = binding(placement(OperatorPlacement.Resource.JOIN_FACTS,"facts"),1);
        var firstHandle = new AtomicReference<KeyedStateStore>(); var firstGuard = new AtomicReference<Runnable>();
        var registry = OperatorMapBindings.bindTo(member(),(binding,active) -> {
            if (binding.equals(first)) firstGuard.set(active);
            if (binding.equals(second)) { fenced(() -> firstHandle.get().load(first.mapName(),"key")); throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
            return new RecordingStore();
        });
        firstHandle.set(registry.initializingNest(first.mapName(),null)); var secondHandle = registry.initializingJoin(second.mapName());
        fenced(() -> registry.attachExisting(List.of(first,second))); fenced(() -> firstGuard.get().run());
        fenced(() -> firstHandle.get().load(first.mapName(),"key")); fenced(() -> secondHandle.load(second.mapName(),"key"));
        assertThat(registry.awaitingMapNames()).containsExactlyInAnyOrder(first.mapName(),second.mapName());
    }

    @Test void closingOrRetiringAnOriginalHandleNeverLetsItsLaterRestorationRebindIt() {
        var original = binding(placement(OperatorPlacement.Resource.NEST_STATE,"root"),1);
        var registry = OperatorMapBindings.bindTo(member(),(binding,active) -> new RecordingStore());
        var waiting = registry.initializingNest(original.mapName(),null); registry.attachExisting(List.of(original));
        assertThat(registry.release(original)).isTrue(); fenced(() -> waiting.count(original.mapName()));
        registry.initializingNest(original.mapName(),null); fenced(() -> registry.attachExisting(List.of(original)));
        registry.close(); fenced(() -> registry.attachExisting(List.of(original))); fenced(() -> waiting.count(original.mapName()));
    }

    private record Call(String operation,String namespace) {}
    private static final class RecordingStore implements KeyedStateStore {
        final List<Call> calls = new ArrayList<>();
        @Override public Optional<byte[]> load(String ns,String key) { calls.add(new Call("load",ns)); return Optional.empty(); }
        @Override public Map<String,byte[]> loadAll(String ns,Collection<String> keys) { calls.add(new Call("loadAll",ns)); return Map.of(); }
        @Override public void save(String ns,String key,byte[] value) { calls.add(new Call("save",ns)); }
        @Override public Optional<byte[]> saveIfAbsent(String ns,String key,byte[] value) { calls.add(new Call("saveIfAbsent",ns)); return Optional.empty(); }
        @Override public void delete(String ns,String key) { calls.add(new Call("delete",ns)); }
        @Override public void dropNamespace(String ns) { calls.add(new Call("drop",ns)); }
        @Override public long count(String ns) { calls.add(new Call("count",ns)); return 0; }
    }

}
