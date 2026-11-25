package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.test.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.runtime.engine.ReplayFloor;
import java.io.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static io.tapstate.runtime.engine.nest.NestFixtures.*;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.*;
import static org.assertj.core.api.Assertions.*;

class ADeferredParkingHandoffStaysDurableTest {
    private static final NestVertex VERTEX = NestTopology.compile("p", "doc",
            nest("customer", List.of("customer_id"), tracking(embed("policy", "customer_id", "customer_id",
                    EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))))), tables())
            .vertexAt(List.of("policies"));
    private static final List<Object> KEY = List.of("P2");
    private static final ParkedSubtree.At PARKED = new ParkedSubtree.At(VERTEX.pathId(), KEY);

    @Test void aFailedReceivingWriteCannotDeleteTheOnlyDurableParkedCopy() throws Exception {
        var state = new ColdState(); var parking = new HeapNestStore<ParkedSubtree>(); var out = new TestOutbox(16);
        var receiver = receiver(state, parking, out, 10);
        feed(receiver, 0, renamed()); out.drainQueueAndReset(0, new ArrayList<>(), false);
        feed(receiver, 0, deleted()); out.drainQueueAndReset(0, new ArrayList<>(), false);
        parking.save(PARKED, new ParkedSubtree(List.of(child("K1")))); state.fail = true;
        assertThatThrownBy(receiver::tryProcess).isInstanceOf(IllegalStateException.class);
        assertThat(parking.load(PARKED)).isNotNull();
        state.fail = false;
        var restarted = receiver(state, parking, new TestOutbox(16), 10);
        feed(restarted, 0, renamed());
        assertThat(state.load(KEY).waiting()).hasSize(1);
    }

    @Test void idleReceivingCannotExceedTheOriginalPendingLimitSilently() throws Exception {
        var state = new ColdState(); var parking = new HeapNestStore<ParkedSubtree>(); var out = new TestOutbox(16);
        var receiver = receiver(state, parking, out, 1);
        feed(receiver, 0, renamed()); feed(receiver, 0, deleted());
        feed(receiver, 1, Envelope.insert(4, "claim", row("claim_id", "K0", "policy_id", "P2"), null).withOrder(at(4)));
        out.drainQueueAndReset(0, new ArrayList<>(), false);
        parking.save(PARKED, new ParkedSubtree(List.of(child("K1"))));
        assertThatThrownBy(receiver::tryProcess).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(NestError.PENDING_LIMIT_EXCEEDED));
        assertThat(state.load(KEY).pending()).isEqualTo(2);
    }

    @Test void aLiveReceivingHandoffSurvivesBackpressureAndColdRestart() throws Exception {
        var state = new ColdState(); var parking = new HeapNestStore<ParkedSubtree>(); var out = new TestOutbox(1);
        var receiver = receiver(state, parking, out, 10);
        feed(receiver, 0, renamed()); out.drainQueueAndReset(0, new ArrayList<>(), false);
        parking.save(PARKED, new ParkedSubtree(List.of(child("K1"), child("K2"))));
        receiver.tryProcess();
        assertThat(parking.load(PARKED)).isNull();
        var restartedOut = new TestOutbox(16); var restarted = receiver(state, parking, restartedOut, 10);
        feed(restarted, 0, renamed());
        var recovered = new ArrayList<Object>(); restartedOut.drainQueueAndReset(0, recovered, false);
        assertThat(recovered).anySatisfy(item -> assertThat(((KeyedElement) item).element().fields()).containsEntry("claim_id", "K2"));
    }

    @Test void aRetryAfterTheParkingRemovalFailsKeepsOneReceivingObligation() throws Exception {
        var state = new ColdState(); var underlying = new HeapNestStore<ParkedSubtree>();
        var refuse = new java.util.concurrent.atomic.AtomicBoolean(true);
        NestStore<ParkedSubtree> parking = new NestStore<>() {
            @Override public ParkedSubtree load(Object key) { return underlying.load(key); }
            @Override public void save(Object key, ParkedSubtree value) { underlying.save(key, value); }
            @Override public void remove(Object key) {
                if (refuse.getAndSet(false)) throw new IllegalStateException("parking removal refused");
                underlying.remove(key);
            }
            @Override public long count() { return underlying.count(); }
        };
        var out = new TestOutbox(16); var receiver = receiver(state, parking, out, 1);
        feed(receiver, 0, renamed()); out.drainQueueAndReset(0, new ArrayList<>(), false);
        parking.save(PARKED, new ParkedSubtree(List.of(child("K1"))));
        assertThatThrownBy(receiver::tryProcess).isInstanceOf(IllegalStateException.class);
        assertThat(state.load(KEY).pending()).isEqualTo(1);
        var restarted = receiver(state, parking, new TestOutbox(16), 1);
        feed(restarted, 0, renamed());
        assertThat(state.load(KEY).pending()).isEqualTo(1);
        assertThat(parking.load(PARKED)).isNull();
    }

    @Test void aColdRestartMustFindAnOutstandingHandoffWithoutAnotherEventForItsKey() throws Exception {
        var state = new ColdState(); var parking = new HeapNestStore<ParkedSubtree>(); var out = new TestOutbox(1);
        var floors = new HashMap<String,io.tapstate.core.event.SourceOrder>();
        var axes = io.tapstate.runtime.engine.ChainAxes.assign(List.of("policy", "claim"));
        int twin = VERTEX.inbound().stream().filter(NestInbound::carriesDepartures).findFirst().orElseThrow().ordinal();
        Map<Integer,List<String>> edges = Map.of(0,List.of("policy"),1,List.of("claim"),twin,List.of("policy"));
        var first = new ResolverProcessor(VERTEX,state,(from,child) -> {},axes,edges,
                chain -> Optional.ofNullable(floors.get(chain)),NestClock.SYSTEM,NestSettings.defaults(),parking);
        first.init(out,new TestProcessorContext()); feed(first,0,renamed());
        out.drainQueueAndReset(0,new ArrayList<>(),false);
        var watermark = new com.hazelcast.jet.core.Watermark(io.tapstate.runtime.engine.FrontierOrders.pack("policy",at(50)),axes.axisOf("policy"));
        first.tryProcessWatermark(0,watermark); first.tryProcessWatermark(twin,watermark);
        out.drainQueueAndReset(0,new ArrayList<>(),false);
        parking.save(PARKED,new ParkedSubtree(List.of(child("K1"),child("K2"))));
        first.tryProcess();
        assertThat(parking.load(PARKED)).isNull();
        assertThat(state.load(KEY).pending()).isEqualTo(2);
        floors.put("policy",at(50));
        var restartedOut = new TestOutbox(16);
        var restarted = new ResolverProcessor(VERTEX,state,(from,child) -> {},axes,edges,
                chain -> Optional.ofNullable(floors.get(chain)),NestClock.SYSTEM,NestSettings.defaults(),parking);
        restarted.init(restartedOut,new TestProcessorContext());
        // The old parent is below the confirmed floor. The production startup input discovers only
        // pending handoffs and routes internal wakes, with no new source row for this key.
        var recoveryOut = new TestOutbox(16);
        var recovery = new NestReleaseRecovery(state::pendingReleases);
        recovery.init(recoveryOut, new TestProcessorContext());
        assertThat(recovery.complete()).isTrue();
        var wakes = new ArrayList<Object>(); recoveryOut.drainQueueAndReset(0, wakes, false);
        for (Object wake : wakes) feed(restarted, VERTEX.inbound().size(), wake);
        for (int i = 0; i < 3; i++) restarted.tryProcess();
        var recovered = new ArrayList<Object>(); restartedOut.drainQueueAndReset(0,recovered,false);
        assertThat(recovered).anySatisfy(item -> assertThat(((KeyedElement)item).element().fields()).containsEntry("claim_id","K2"));
    }

    @Test void aDiscoveredHandoffCannotTurnIntoNewEmptyStateAfterItsColdRowDisappears() throws Exception {
        var state = new ColdState(); var parking = new HeapNestStore<ParkedSubtree>();
        var first = receiver(state,parking,new TestOutbox(16),10);
        feed(first,1,Envelope.insert(1,"claim",row("claim_id","K1","policy_id","P2"),null).withOrder(at(1)));
        feed(first,0,renamed());
        var recoveryOut = new TestOutbox(16); var recovery = new NestReleaseRecovery(state::pendingReleases);
        recovery.init(recoveryOut,new TestProcessorContext()); assertThat(recovery.complete()).isTrue();
        var wakes = new ArrayList<Object>(); recoveryOut.drainQueueAndReset(0,wakes,false); assertThat(wakes).hasSize(1);
        state.remove(KEY);
        var restarted = receiver(state,parking,new TestOutbox(16),10);
        assertThatThrownBy(() -> feed(restarted,VERTEX.inbound().size(),wakes.getFirst()))
                .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE));
        assertThat(state.load(KEY)).isNull(); assertThat(state.count()).isZero();
    }

    private static ResolverProcessor receiver(ColdState state, NestStore<ParkedSubtree> parking,
            TestOutbox out, long limit) throws Exception {
        var processor = new ResolverProcessor(VERTEX, state, (from, child) -> {}, null, null,
                ReplayFloor.NONE, NestClock.SYSTEM, NestSettings.defaults().withPendingLimit(VERTEX.mapName(), limit), parking);
        processor.init(out, new TestProcessorContext()); return processor;
    }
    private static NestElement child(String id) {
        return new NestElement(new ElementRef(List.of("policies", "claims"), KEY, List.of(id), null),
                row("claim_id", id, "policy_id", "P2"), at(1), Map.of());
    }
    private static Envelope renamed() {
        return Envelope.update(2, "policy", row("policy_id", "P1", "customer_id", "C1", "policy_no", "PN1"),
                row("policy_id", "P2", "customer_id", "C1", "policy_no", "PN1"), null).withOrder(at(2));
    }
    private static Envelope deleted() {
        return Envelope.delete(3, "policy", row("policy_id", "P2", "customer_id", "C1", "policy_no", "PN1"), null).withOrder(at(3));
    }
    private static void feed(ResolverProcessor processor, int ordinal, Object event) {
        var inbox = new TestInbox(); inbox.queue().add(event); processor.process(ordinal, inbox);
    }
    private static final class ColdState implements NestStore<ResolverState> {
        private final Map<Object, byte[]> entries = new LinkedHashMap<>();
        private final Map<String, byte[]> pending = new TreeMap<>(); boolean fail;
        List<io.tapstate.spi.store.NestReleaseStore.PendingKey> pendingReleases(String after) {
            return pending.entrySet().stream().filter(entry -> after == null || entry.getKey().compareTo(after) > 0)
                    .limit(io.tapstate.spi.store.NestReleaseStore.MAX_PAGE_KEYS)
                    .map(entry -> new io.tapstate.spi.store.NestReleaseStore.PendingKey(entry.getKey(),entry.getValue())).toList();
        }
        @Override public ResolverState load(Object key) {
            var bytes = entries.get(key); if (bytes == null) return null;
            try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes))) { return (ResolverState) in.readObject(); }
            catch (Exception failure) { throw new AssertionError(failure); }
        }
        @Override public void save(Object key, ResolverState state) {
            if (fail) throw new IllegalStateException("receiving write refused");
            var bytes = new ByteArrayOutputStream();
            try (var out = new ObjectOutputStream(bytes)) { out.writeObject(state); }
            catch (IOException failure) { throw new AssertionError(failure); }
            entries.put(key, bytes.toByteArray());
            if (state.releases().isEmpty()) pending.remove(NestStateKeys.nameOf(key));
            else {
                var encoded = new ByteArrayOutputStream();
                try (var out = new ObjectOutputStream(encoded)) { out.writeObject(key); }
                catch (IOException failure) { throw new AssertionError(failure); }
                pending.put(NestStateKeys.nameOf(key), encoded.toByteArray());
            }
        }
        @Override public void remove(Object key) { entries.remove(key); pending.remove(NestStateKeys.nameOf(key)); }
        @Override public long count() { return entries.size(); }
    }
}
