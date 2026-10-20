package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.AwaitedLoad;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ScopedSrsSinkAckFactoryTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",3,"cache","pipeline",7);
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),new WorkloadOwner("node","boot"),2,5,9);
    private static final SrsConsumerRun RUN = SrsConsumerRun.of(SCOPE,new SrsConsumerMembership(
            new SrsConsumerContext(new SrsConsumerKey("chain","pipeline","source"),7),4,false,Set.of("orders"),FENCE));
    private static ScopedSrsSinkAckFactory factory() { return new ScopedSrsSinkAckFactory(Map.of("source.orders",new ScopedSrsSinkAckFactory.Source(RUN,"orders")),"original-run"); }

    @Test void sharedAndPrivateStreamsKeepSeparateWriterStoresAfterFactorySerialization() throws Exception {
        var f = new Fixture(); var privateStore = mock(DirectChannelStore.class);
        var context = new DirectChannelContext(SCOPE,"private-channel","private-source","private-capture");
        var capture = new WorkloadClaimFence(new WorkloadClaimKey(SCOPE.clusterId(),WorkloadClaimType.CAPTURE,context.captureId()),FENCE.owner(),1,0,1);
        var window = new DirectCaptureWindow(9,capture,FENCE,"anchor",0,0,null,null);
        var cursor = new ConsumerOffset(context.consumerId(),Map.of(),new ChainPosition(SourceOrder.snapshotRow(9),"anchor"),List.of(),null,0,Map.of(),ConsumerProgressKind.DIRECT_SOURCE);
        var opened = new DirectChannelProgress(context,Set.of("other"),1,window,null,cursor,null);
        var original = new ExecutionFence(SCOPE,FENCE); var privateRun = DirectSourceRun.of(original,opened);
        var sharedViews = (SrsStoreViews)f.context.get(SrsStoreViews.USER_CONTEXT_KEY);
        f.context.put(SrsStoreViews.USER_CONTEXT_KEY,new SrsStoreViews() {
            @Override public SrsStores srs(CacheBusinessScope scope,Runnable guard) { return sharedViews.srs(scope,guard); }
            @Override public DirectChannelStore directChannels(CacheBusinessScope scope,Runnable guard) {
                assertThat(scope).isEqualTo(RUN.cacheScope()); guard.run(); return privateStore;
            }
        });
        when(privateStore.load(context)).thenReturn(opened);
        when(privateStore.beginWriterRun(any(),anyLong(),anyString(),anyMap(),any())).thenReturn(true);
        when(privateStore.advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any())).thenReturn(true);
        var factory = new ScopedSrsSinkAckFactory(Map.of("source.orders",new ScopedSrsSinkAckFactory.Source(RUN,"orders")),
                Map.of("private.other",new ScopedDirectSinkAckFactory.Source(privateRun,"other")),"mixed-run");
        var bytes = new ByteArrayOutputStream(); try (var out = new ObjectOutputStream(bytes)) { out.writeObject(factory); }
        SinkAckFactory copy; try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { copy = (SinkAckFactory)in.readObject(); }
        copy.beginRun(f.member,Map.of("source.orders",List.of("writer"),"private.other",List.of("writer")),FENCE);
        verify(f.store).beginWriterRun(RUN.context(),RUN.membershipRevision(),"mixed-run",Map.of("orders",List.of("writer")),FENCE);
        verify(privateStore).beginWriterRun(context,9,"mixed-run",Map.of("other",List.of("writer")),FENCE);
        clearInvocations(f.store,privateStore);
        var ack = copy.resolve(f.member).forWriter("writer"); var point = new ChainPosition(new SourceOrder(9,12),"actual");
        ack.advance("source.orders",point,FENCE);
        verify(f.store).advanceWriter(RUN.context(),RUN.membershipRevision(),"mixed-run","orders","writer",new WriterProgress(point.order(),point),FENCE);
        verify(privateStore,never()).advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any());
        clearInvocations(f.store,privateStore);
        ack.advance("private.other",point,FENCE); ack.bounded("private.other",new SourceOrder(9,20),FENCE);
        verify(privateStore).advanceWriter(context,9,"mixed-run","other","writer",new WriterProgress(point.order(),point),FENCE);
        verify(privateStore).advanceWriter(context,9,"mixed-run","other","writer",new WriterProgress(new SourceOrder(9,20),null),FENCE);
        verify(f.store,never()).advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any());
    }

    @Test void deserializedFactoryBindsOnlyTheOriginalMembershipAndFullClaimOnItsActualMember() throws Exception {
        var f = new Fixture(); var original = factory(); var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(original); }
        SinkAckFactory copy;
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { copy = (SinkAckFactory)in.readObject(); }
        copy.beginRun(f.member,Map.of("source.orders",List.of("writer")),FENCE);
        var ack = copy.resolve(f.member).forWriter("writer"); var position = new ChainPosition(new SourceOrder(9,12),"token");
        ack.advance("source.orders",position,FENCE); ack.bounded("source.orders",new SourceOrder(9,20),FENCE);
        verify(f.store).beginWriterRun(RUN.context(),4,"original-run",Map.of("orders",List.of("writer")),FENCE);
        verify(f.store).advanceWriter(RUN.context(),4,"original-run","orders","writer",new WriterProgress(position.order(),position),FENCE);
        verify(f.store).advanceWriter(RUN.context(),4,"original-run","orders","writer",new WriterProgress(new SourceOrder(9,20),null),FENCE);
        var guards = ArgumentCaptor.forClass(ExecutionFence.class); verify(f.authorization,atLeastOnce()).require(guards.capture());
        assertThat(guards.getAllValues()).allSatisfy(fence -> assertThat(fence).isEqualTo(new ExecutionFence(SCOPE,FENCE)));
    }
    @Test void storeRefusalCannotBeConvertedToAnAcknowledgedOrRetriedNewerRun() {
        var f = new Fixture(); when(f.store.advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any())).thenReturn(false);
        var ack = factory().resolve(f.member).forWriter("writer");
        assertThatThrownBy(() -> ack.advance("source.orders",new ChainPosition(new SourceOrder(1,2),"t2"),FENCE)).satisfies(ScopedSrsSinkAckFactoryTest::refused);
        verify(f.store,never()).find(any()); verify(f.store,never()).attach(any(),anyLong(),anySet(),any());
    }
    @Test void missingTypedStoreOrScopedAuthorizationNeverFallsBackToLegacyNoOp() {
        var f = new Fixture(); f.context.remove(SrsStoreViews.USER_CONTEXT_KEY); f.context.put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY,mock(SrsMetaStore.class));
        assertThatThrownBy(() -> factory().resolve(f.member)).satisfies(ScopedSrsSinkAckFactoryTest::refused);
        var unscoped = new Fixture(); unscoped.context.put(ExecutionAuthorization.USER_CONTEXT_KEY,ExecutionAuthorization.unfenced());
        assertThatThrownBy(() -> factory().beginRun(unscoped.member,Map.of("source.orders",List.of("writer")))).satisfies(ScopedSrsSinkAckFactoryTest::refused);
        verify(unscoped.store,never()).beginWriterRun(any(),anyLong(),anyString(),anyMap(),any());
    }
    @Test void lateLocalGuardLossAndAnotherSuppliedClaimRefuseBeforeDurableWrites() {
        var f = new Fixture(); var ack = factory().resolve(f.member).forWriter("writer");
        var other = new WorkloadClaimFence(FENCE.key(),new WorkloadOwner("other","boot"),2,5,9);
        assertThatThrownBy(() -> ack.bounded("source.orders",new SourceOrder(1,5),other)).satisfies(ScopedSrsSinkAckFactoryTest::refused);
        f.active.set(false);
        assertThatThrownBy(() -> ack.advance("source.orders",new ChainPosition(new SourceOrder(1,5),"t5"),FENCE)).satisfies(ScopedSrsSinkAckFactoryTest::refused);
        verify(f.store,never()).advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any());
    }
    @Test void writerNamesAndStreamsMustBelongToTheCompiledRoster() {
        var f = new Fixture(); var factory = factory(); var ack = factory.resolve(f.member);
        assertThatThrownBy(() -> ack.advance("source.orders",new ChainPosition(new SourceOrder(1,1),"t1"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> ack.forWriter("writer").bounded("uncompiled",new SourceOrder(1,1))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> factory.beginRun(f.member,Map.of("other",List.of("writer")),FENCE)).isInstanceOf(IllegalStateException.class);
        verify(f.store,never()).advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any());
    }
    @Test void loadBarrierUsesItsOwnPinnedRunAndCompletedStateWithoutBorrowingReadAhead() {
        var f = new Fixture(); var awaited = new AwaitedLoad("sink","source.orders",List.of("writer"));
        var landings = factory().loadLandings(f.member);
        f.held.set(progress(4,List.of(),null)); assertThat(landings.stillLanding(List.of(awaited))).containsExactly(awaited);
        var point = new WriterProgress(SourceOrder.snapshotRow(7),null);
        var writers = new SrsConsumerWriterRun(4,FENCE,"original-run",Map.of("orders",List.of("writer","slow")),Map.of("orders",Map.of("writer",point)));
        f.held.set(progress(4,List.of(),writers)); assertThat(landings.stillLanding(List.of(awaited))).isEmpty();
        assertThat(landings.stillLanding(List.of(new AwaitedLoad("sink","source.orders",List.of("writer","slow"))))).hasSize(1);
        f.held.set(progress(4,List.of("orders"),null)); assertThat(landings.stillLanding(List.of(awaited))).isEmpty();
        f.held.set(progress(5,List.of("orders"),null)); assertThatThrownBy(() -> landings.stillLanding(List.of(awaited))).satisfies(ScopedSrsSinkAckFactoryTest::refused);
    }
    @Test void operatorReplayFloorUsesOnlyTheSameOriginalTableSinkConfirmation() throws Exception {
        var f = new Fixture(); var source = new ScopedSrsSinkAckFactory.Source(RUN,"orders");
        var factory = new ScopedSrsReplayFloorFactory(Map.of("source.orders",source));
        var bytes = new ByteArrayOutputStream(); try(var out=new ObjectOutputStream(bytes)) { out.writeObject(factory); }
        io.tapstate.runtime.engine.ReplayFloorFactory copy;
        try(var in=new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { copy=(io.tapstate.runtime.engine.ReplayFloorFactory)in.readObject(); }
        var floors=copy.resolve(f.member); f.held.set(progress(4,List.of(),null));
        assertThat(floors.of("source.orders")).isEmpty();
        var prior=f.held.get(); var old=prior.cursor(); var table=new ChainPosition(new SourceOrder(7,10),"table-token");
        var cursor=new ConsumerOffset(old.pipelineId(),Map.of("orders",99L),new ChainPosition(new SourceOrder(7,50),"aggregate"),
                old.snapshotCompletedTables(),old.cdcStartPosition(),old.snapshotEpoch(),Map.of("orders",table),ConsumerProgressKind.SRS);
        f.held.set(new SrsConsumerProgress(RUN.context(),4,2,cursor,prior.snapshot()));
        assertThat(floors.of("source.orders")).contains(table.order()); assertThat(floors.of("uncompiled")).isEmpty();
        f.active.set(false); assertThatThrownBy(() -> floors.of("source.orders")).satisfies(ScopedSrsSinkAckFactoryTest::refused);
    }

    private static SrsConsumerProgress progress(long membership,List<String> complete,SrsConsumerWriterRun writer) {
        var pin = new SrsSnapshotPin(7,"owner-seam",Map.of("orders",new SrsModelKey("chain","orders","cache",1,0)));
        var cursor = new ConsumerOffset(RUN.context().key().consumerId(),Map.of("orders",99L),null,complete,"owner-seam",7,Map.of(),ConsumerProgressKind.SRS);
        return new SrsConsumerProgress(RUN.context(),membership,1,cursor,pin,writer);
    }
    private static void refused(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED)); }
    private static final class Fixture {
        final HazelcastInstance member = mock(HazelcastInstance.class);
        final java.util.concurrent.ConcurrentMap<String,Object> context = new ConcurrentHashMap<>();
        final SrsConsumerStore store = mock(SrsConsumerStore.class);
        final ExecutionAuthorization authorization = mock(ExecutionAuthorization.class);
        final AtomicBoolean active = new AtomicBoolean(true);
        final AtomicReference<SrsConsumerProgress> held = new AtomicReference<>();
        Fixture() {
            when(member.getUserContext()).thenReturn(context);
            when(authorization.require(any())).thenAnswer(call -> { if (!active.get()) throw new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline","pipeline"),null); return FENCE; });
            context.put(ExecutionAuthorization.USER_CONTEXT_KEY,authorization);
            SrsStoreViews views = (scope,guard) -> {
                assertThat(scope).isEqualTo(RUN.cacheScope()); guard.run();
                return new SrsStores(mock(SrsChainStore.class),mock(SrsModelStore.class),mock(SrsLogStore.class),store);
            };
            context.put(SrsStoreViews.USER_CONTEXT_KEY,views);
            when(store.guardedBy(any())).thenReturn(store);
            when(store.beginWriterRun(any(),anyLong(),anyString(),anyMap(),any())).thenReturn(true);
            when(store.advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any())).thenReturn(true);
            when(store.progress(any())).thenAnswer(call -> { assertThat(call.getArgument(0,SrsConsumerContext.class)).isEqualTo(RUN.context()); return Optional.ofNullable(held.get()); });
        }
    }
}
