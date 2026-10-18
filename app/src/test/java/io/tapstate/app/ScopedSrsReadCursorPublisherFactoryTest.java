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
    private static SrsConsumerProgress progress(long membership,Long floor,ChainPosition confirmed) {
        var cursor = new ConsumerOffset(RUN.context().key().consumerId(),Map.of("orders",99L),null,List.of(),null,0,
                confirmed == null ? Map.of() : Map.of("orders",confirmed),ConsumerProgressKind.SRS);
        return new SrsConsumerProgress(RUN.context(),membership,1,cursor,null,null,floor == null ? Map.of() : Map.of("orders",floor));
    }
    private static void refused(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED)); }
    private static final class Fixture {
        final HazelcastInstance member = mock(HazelcastInstance.class);
        final SrsConsumerStore store = mock(SrsConsumerStore.class);
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
            SrsStoreViews views = (scope,guard) -> { assertThat(scope).isEqualTo(RUN.cacheScope()); guard.run(); return new SrsStores(mock(SrsChainStore.class),mock(SrsModelStore.class),mock(SrsLogStore.class),store); };
            context.put(SrsStoreViews.USER_CONTEXT_KEY,views);
            when(store.guardedBy(any())).thenReturn(store); when(store.progress(RUN.context())).thenAnswer(call -> Optional.ofNullable(held.get()));
            when(store.advanceReadSequence(any(),anyLong(),anyString(),anyLong(),any())).thenReturn(true);
        }
    }
}
