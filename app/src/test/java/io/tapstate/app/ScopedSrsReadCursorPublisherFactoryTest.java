package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.srs.SrsReadCursorPublisherFactory;
import io.tapstate.spi.store.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ScopedSrsReadCursorPublisherFactoryTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",3,"cache","pipeline",7);
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),new WorkloadOwner("node","boot"),2,5,9);
    private static final SrsConsumerRun RUN = SrsConsumerRun.of(SCOPE,new SrsConsumerMembership(
            new SrsConsumerContext(new SrsConsumerKey("chain","pipeline","source"),7),4,false,Set.of("orders"),FENCE));
    @Test void serializedReaderUsesOriginalScopeAndMembershipAndCannotTreatReadAheadAsConfirmation() throws Exception {
        var original = new ScopedSrsReadCursorPublisherFactory(RUN,"orders"); var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(original); }
        SrsReadCursorPublisherFactory copied;
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { copied = (SrsReadCursorPublisherFactory)in.readObject(); }
        var f = new Fixture(); f.held.set(progress(4,5L,null)); copied.resolve(f.member).accept(99);
        verify(f.store).advanceReadSequence(RUN.context(),4,"orders",99,FENCE);
        assertThat(copied.recoverySequence(f.member)).hasValue(5); assertThat(copied.confirmedPosition(f.member)).isEmpty();
        verify(f.store,never()).advanceWriter(any(),anyLong(),anyString(),anyString(),anyString(),any(),any());
        when(f.models.requireEvent(MODEL,RING,12,CAPTURE)).thenReturn(ACTUAL);
        copied.modelVerifier(f.member,RING).verify(item(),12);verify(f.models).requireEvent(MODEL,RING,12,CAPTURE);
    }
    @Test void epochEndBoundsAndMissingFloorsNeverBecomePhysicalRecoveryPositions() {
        var f = new Fixture(); var reader = new ScopedSrsReadCursorPublisherFactory(RUN,"orders");
        f.held.set(progress(4,12L,new ChainPosition(new SourceOrder(1,Long.MAX_VALUE),null)));
        assertThat(reader.confirmedPosition(f.member)).isEmpty(); assertThat(reader.recoverySequence(f.member)).hasValue(12);
        f.held.set(progress(4,null,null)); assertThatThrownBy(() -> reader.recoverySequence(f.member)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::refused);
        var position = new ChainPosition(new SourceOrder(1,12),"t12"); f.held.set(progress(4,12L,position));
        assertThat(reader.confirmedPosition(f.member)).contains(position);
        assertThatThrownBy(() -> new ScopedSrsReadCursorPublisherFactory(RUN,"unselected").confirmedPosition(f.member)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::refused);
    }
    @Test void replacedMembershipAndLostLocalGuardCannotReadOrWriteThroughTheOldBinding() {
        var f = new Fixture(); var reader = new ScopedSrsReadCursorPublisherFactory(RUN,"orders"); var publish = reader.resolve(f.member);
        f.held.set(progress(5,9L,null)); assertThatThrownBy(() -> reader.recoverySequence(f.member)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::refused);
        when(f.store.advanceReadSequence(any(),anyLong(),anyString(),anyLong(),any())).thenReturn(false);
        assertThatThrownBy(() -> publish.accept(20)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::refused);
        clearInvocations(f.store); f.active.set(false);
        assertThatThrownBy(() -> publish.accept(21)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::refused);
        verify(f.store,never()).advanceReadSequence(any(),anyLong(),anyString(),anyLong(),any());
    }
    private static final String RING = "ring-original";
    private static final WorkloadClaimFence CAPTURE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.CAPTURE,"capture"),new WorkloadOwner("capture-node","capture-boot"),6,2,9);
    private static final SrsModelKey MODEL = new SrsModelKey("chain","orders","cache",3,2);
    private static final SrsModel ACTUAL = new SrsModel(MODEL,RING,new SourceTable("orders",List.of(new SourceField("id","int")),List.of("id"),List.of()),10);
    private static io.tapstate.runtime.srs.SrsItem item() { return new io.tapstate.runtime.srs.SrsItem(null,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",1),2,CAPTURE,3); }

    @Test void eventModelVerifierUsesTheOriginalConsumerAndHistoricalCaptureContext() {
        var f=new Fixture();f.held.set(progress(4,5L,null));when(f.models.requireEvent(MODEL,RING,12,CAPTURE)).thenReturn(ACTUAL);
        new ScopedSrsReadCursorPublisherFactory(RUN,"orders").modelVerifier(f.member,RING).verify(item(),12);
        verify(f.models).requireEvent(MODEL,RING,12,CAPTURE);verify(f.models,never()).load(any());verify(f.store,never()).advanceReadSequence(any(),anyLong(),anyString(),anyLong(),any());
    }
    @Test void eventModelVerifierPropagatesMissingModelWithoutFallingBackToLatest() {
        var f=new Fixture();f.held.set(progress(4,5L,null));when(f.models.requireEvent(MODEL,RING,12,CAPTURE)).thenThrow(modelUnavailable());
        assertThatThrownBy(() -> new ScopedSrsReadCursorPublisherFactory(RUN,"orders").modelVerifier(f.member,RING).verify(item(),12)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::missingModel);
        verify(f.models,never()).load(any());
    }
    @Test void eventModelVerifierRejectsAReplacedMembership() {
        var f=new Fixture();f.held.set(progress(4,5L,null));var verifier=new ScopedSrsReadCursorPublisherFactory(RUN,"orders").modelVerifier(f.member,RING);
        f.held.set(progress(5,5L,null));assertThatThrownBy(() -> verifier.verify(item(),12)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::refused);
        verify(f.models,never()).requireEvent(any(),anyString(),anyLong(),any());
    }
    @Test void eventModelVerifierRechecksAuthorizationAfterTheModelRead() {
        var f=new Fixture();f.held.set(progress(4,5L,null));when(f.models.requireEvent(MODEL,RING,12,CAPTURE)).thenAnswer(call -> {f.active.set(false);return ACTUAL;});
        assertThatThrownBy(() -> new ScopedSrsReadCursorPublisherFactory(RUN,"orders").modelVerifier(f.member,RING).verify(item(),12)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::refused);
    }
    @Test void eventModelVerifierDoesNotAdoptAWrongReturnedModel() {
        var f=new Fixture();f.held.set(progress(4,5L,null));when(f.models.requireEvent(MODEL,RING,12,CAPTURE)).thenReturn(new SrsModel(new SrsModelKey("chain","orders","cache",3,1),RING,ACTUAL.table(),1));
        assertThatThrownBy(() -> new ScopedSrsReadCursorPublisherFactory(RUN,"orders").modelVerifier(f.member,RING).verify(item(),12)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::missingModel);
    }
    @Test void eventModelVerifierAcceptsActualVersionZero() {
        var f=new Fixture();f.held.set(progress(4,5L,null));var key=new SrsModelKey("chain","orders","cache",3,0);
        var actual=new SrsModel(key,RING,ACTUAL.table(),0);when(f.models.requireEvent(key,RING,0,CAPTURE)).thenReturn(actual);
        var event=new io.tapstate.runtime.srs.SrsItem(null,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",1),0,CAPTURE,3);
        new ScopedSrsReadCursorPublisherFactory(RUN,"orders").modelVerifier(f.member,RING).verify(event,0);verify(f.models).requireEvent(key,RING,0,CAPTURE);
    }
    @Test void eventModelVerifierRejectsMissingCaptureIdentityBeforeLookingUpAModel() {
        var f=new Fixture();f.held.set(progress(4,5L,null));var verifier=new ScopedSrsReadCursorPublisherFactory(RUN,"orders").modelVerifier(f.member,RING);
        for(var event:List.of(new io.tapstate.runtime.srs.SrsItem(null,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",1),0,null,3),
                new io.tapstate.runtime.srs.SrsItem(null,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",1),0,CAPTURE,0))) {
            assertThatThrownBy(() -> verifier.verify(event,0)).satisfies(ScopedSrsReadCursorPublisherFactoryTest::missingModel);
        }
        verify(f.models,never()).requireEvent(any(),anyString(),anyLong(),any());
    }
    private static TapstateException modelUnavailable() { return new TapstateException(IoError.SRS_EVENT_MODEL_UNAVAILABLE,Map.of("ring",RING,"epoch","3","version","2"),null); }
    private static void missingModel(Throwable failure) {assertThat(failure).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.SRS_EVENT_MODEL_UNAVAILABLE));}

    private static SrsConsumerProgress progress(long membership,Long floor,ChainPosition confirmed) {
        var cursor = new ConsumerOffset(RUN.context().key().consumerId(),Map.of("orders",99L),null,List.of(),null,0,
                confirmed == null ? Map.of() : Map.of("orders",confirmed),ConsumerProgressKind.SRS);
        return new SrsConsumerProgress(RUN.context(),membership,1,cursor,null,null,floor == null ? Map.of() : Map.of("orders",floor));
    }
    private static void refused(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED)); }
    private static final class Fixture {
        final HazelcastInstance member = mock(HazelcastInstance.class);
        final SrsConsumerStore store = mock(SrsConsumerStore.class);
        final SrsModelStore models = mock(SrsModelStore.class);
        final AtomicReference<SrsConsumerProgress> held = new AtomicReference<>();
        final AtomicBoolean active = new AtomicBoolean(true);
        Fixture() {
            var context = new ConcurrentHashMap<String,Object>(); when(member.getUserContext()).thenReturn(context);
            var authorization = mock(ExecutionAuthorization.class);
            when(authorization.require(any())).thenAnswer(call -> {
                assertThat(call.getArgument(0,ExecutionFence.class)).isEqualTo(new ExecutionFence(SCOPE,FENCE));
                if (!active.get()) throw new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,Map.of("pipeline","pipeline"),null); return FENCE;
            });
            context.put(ExecutionAuthorization.USER_CONTEXT_KEY,authorization);
            SrsStoreViews views = (scope,guard) -> { assertThat(scope).isEqualTo(RUN.cacheScope()); guard.run(); return new SrsStores(mock(SrsChainStore.class),models,mock(SrsLogStore.class),store); };
            context.put(SrsStoreViews.USER_CONTEXT_KEY,views);
            when(models.guardedBy(any())).thenReturn(models);
            when(store.guardedBy(any())).thenReturn(store); when(store.progress(RUN.context())).thenAnswer(call -> Optional.ofNullable(held.get()));
            when(store.advanceReadSequence(any(),anyLong(),anyString(),anyLong(),any())).thenReturn(true);
        }
    }
}
