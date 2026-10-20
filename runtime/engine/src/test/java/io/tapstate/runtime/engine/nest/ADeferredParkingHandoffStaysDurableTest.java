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
        recoverHandoffs(state, restarted);
        // The old parent is below the confirmed floor, so no event is delivered to this key again.
        for (int i = 0; i < 3; i++) restarted.tryProcess();
        var recovered = new ArrayList<Object>(); restartedOut.drainQueueAndReset(0,recovered,false);
        assertThat(recovered).anySatisfy(item -> assertThat(((KeyedElement)item).element().fields()).containsEntry("claim_id","K2"));
    }

    private static void recoverHandoffs(ColdState state, ResolverProcessor receiver) throws Exception {
        var out = new TestOutbox(16);
        var source = new NestHandoffRecovery(state::pending);
        source.init(out, new TestProcessorContext());
        boolean complete;
        do {
            complete = source.complete();
            var keys = new ArrayList<Object>();
            out.drainQueueAndReset(0, keys, false);
            for (Object key : keys) feed(receiver, VERTEX.inbound().size(), key);
        } while (!complete);
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
        private record Row(byte[] state, byte[] pendingKey) { }
        private final Map<Object, Row> entries = new LinkedHashMap<>(); boolean fail;
        @Override public ResolverState load(Object key) {
            var row = entries.get(key); if (row == null) return null;
            try (var in = new ObjectInputStream(new ByteArrayInputStream(row.state()))) { return (ResolverState) in.readObject(); }
            catch (Exception failure) { throw new AssertionError(failure); }
        }
        @Override public void save(Object key, ResolverState state) {
            if (fail) throw new IllegalStateException("receiving write refused");
            var bytes = new ByteArrayOutputStream();
            try (var out = new ObjectOutputStream(bytes)) { out.writeObject(state); }
            catch (IOException failure) { throw new AssertionError(failure); }
            entries.put(key, new Row(bytes.toByteArray(), state.releases().isEmpty() ? null : NestHandoffRecovery.encodeKey(key)));
        }
        List<io.tapstate.spi.store.NestStateStore.Pending> pending(String after, int limit) {
            return entries.entrySet().stream().filter(entry -> entry.getValue().pendingKey() != null)
                    .map(entry -> new io.tapstate.spi.store.NestStateStore.Pending(
                            NestStateKeys.nameOf(entry.getKey()), entry.getValue().pendingKey()))
                    .filter(row -> after == null || row.cursor().compareTo(after) > 0)
                    .sorted(Comparator.comparing(io.tapstate.spi.store.NestStateStore.Pending::cursor))
                    .limit(limit).toList();
        }
        @Override public void remove(Object key) { entries.remove(key); }
        @Override public long count() { return entries.size(); }
    }
}
