package io.tapstate.app;

import io.tapstate.runtime.srs.*;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeSnapshotOnlyRunTest {
    private static final CacheBusinessScope SCOPE=new CacheBusinessScope("cluster",1,"cache");
    private static final CaptureConfig CONFIG=new CaptureConfig("fixture",Map.of(),List.of("orders","quiet"));

    @Test void boundedRowsUseTheAllocatedOrderWithoutOpeningSrsOrSamplingACdcSeam() throws Exception {
        var chains=mock(SrsChainStore.class); var models=mock(SrsModelStore.class); var log=mock(SrsLogStore.class); var consumers=mock(SrsConsumerStore.class);
        var rows=new CopyOnWriteArrayList<Envelope>(); var loaded=new CopyOnWriteArrayList<String>(); var port=new Port();
        try(var unit=new NativeSrsCaptureRunUnit(SCOPE,new SrsStores(chains,models,log,consumers),mock(HazelcastInstance.class))) {
            var run=unit.beginSnapshotOnly(CONFIG,17,port,handoff(rows,loaded),() -> {});
            assertThat(run.awaitLoaded(Duration.ofSeconds(3))).isTrue(); assertThat(run.failure()).isEmpty();
            assertThat(rows).hasSize(2).allSatisfy(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(17)));
            assertThat(loaded).containsExactly("orders","quiet"); assertThat(run.snapshotCount()).isEqualTo(2);
            assertThat(run.chainId()).isEmpty(); assertThat(run.cdcSubscription()).isEmpty();
            unit.stopSnapshot(run); unit.stopSnapshot(run); assertThat(port.closed).hasValue(1);
        }
        verifyNoInteractions(chains,models,log,consumers);
    }
    @Test void theBackgroundLoadReturnsBeforeItsBoundedHandoffCanAcceptAllRows() throws Exception {
        var buffer=new SnapshotBuffer(1); buffer.declareSnapshot("original","ring"); var port=new Port();
        try(var unit=unit()) {
            var run=unit.beginSnapshotOnly(CONFIG,3,port,buffer.handoff("original","ring"),() -> {});
            assertThat(run.awaitLoaded(Duration.ofMillis(25))).isFalse();
            long deadline=System.nanoTime()+Duration.ofSeconds(3).toNanos(); List<Envelope> first=List.of();
            while(first.isEmpty() && System.nanoTime()<deadline) { first=buffer.drain("original","ring"); if(first.isEmpty())Thread.sleep(5); }
            assertThat(first).hasSize(1); assertThat(run.awaitLoaded(Duration.ofSeconds(3))).isTrue();
            assertThat(buffer.drain("original","ring")).hasSize(1); assertThat(run.failure()).isEmpty();
        }
        assertThat(port.closed).hasValue(1);
    }
    @Test void guardLossCannotPublishTableCompletionAfterTheLastAllowedRow() throws Exception {
        var active=new AtomicBoolean(true); var port=new Port(); var loaded=new CopyOnWriteArrayList<String>();
        try(var unit=unit()) {
            var run=unit.beginSnapshotOnly(CONFIG,3,port,new CaptureHandoff() {
                public void accept(Envelope row) { active.set(false); }
                public void loaded(String table) { loaded.add(table); }
            },() -> { if(!active.get())throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); });
            assertThat(run.awaitLoaded(Duration.ofSeconds(3))).isTrue();
            assertThat(run.failure()).hasValueSatisfying(error -> assertThat(error).isInstanceOf(TapstateException.class));
            assertThat(loaded).isEmpty(); assertThat(port.closed).hasValue(1);
        }
    }
    private static NativeSrsCaptureRunUnit unit() {
        return new NativeSrsCaptureRunUnit(SCOPE,new SrsStores(mock(SrsChainStore.class),mock(SrsModelStore.class),mock(SrsLogStore.class),mock(SrsConsumerStore.class)),mock(HazelcastInstance.class));
    }
    private static CaptureHandoff handoff(List<Envelope> rows,List<String> loaded) {
        return new CaptureHandoff() { public void accept(Envelope row) { rows.add(row); } public void loaded(String table) { loaded.add(table); } };
    }
    private static final class Port implements CapturePort,SnapshotOnlyCapture {
        final AtomicInteger closed=new AtomicInteger();
        public CaptureBatch snapshotOnly(CaptureConfig config) {
            var rows=List.of(Envelope.read(1,"orders",Map.of("id",1),null),Envelope.read(1,"orders",Map.of("id",2),null)).iterator();
            return new CaptureBatch() {
                public boolean hasNext() { return rows.hasNext(); } public Envelope next() { return rows.next(); }
                public Optional<SourcePosition> seam() { throw new AssertionError("bounded-only reads have no CDC seam"); }
                public void close() { closed.incrementAndGet(); }
            };
        }
        public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError("use the bounded-only capability"); }
        public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) { throw new AssertionError("bounded-only reads have no CDC tail"); }
        public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
        public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
    }
}
