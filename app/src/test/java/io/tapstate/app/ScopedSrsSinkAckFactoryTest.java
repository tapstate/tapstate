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

    @Test void aReaderAfterAWriteCannotBorrowAnOlderUnfinishedSnapshot() throws Exception {
        var reads = new SrsConsumerProgressReads(); var original = progress(4,List.of(),null);
        var latest = new AtomicReference<>(original); var captured = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1); var lateEntered = new java.util.concurrent.CountDownLatch(1);
        var threads = new java.util.concurrent.CopyOnWriteArrayList<Thread>();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2, task -> {
            var thread = new Thread(task); threads.add(thread); return thread;
        })) {
            var first = executor.submit(() -> reads.read(RUN,() -> {
                var snapshot = latest.get(); captured.countDown();
                try { assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                return snapshot;
            }));
            try {
                assertThat(captured.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var written = progress(4,List.of("orders"),null); latest.set(written);
                var later = executor.submit(() -> { lateEntered.countDown(); return reads.read(RUN,latest::get); });
                assertThat(lateEntered.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); awaitParked(threads);
                release.countDown();
                assertThat(first.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(original);
                assertThat(later.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(written);
            } finally { release.countDown(); }
        }
    }

    @Test void queuedReadersShareOnlyANewTransactionForTheSameOriginalRun() throws Exception {
        var f = new Fixture(); f.held.set(progress(4,List.of(),null));
        var first = ScopedSrsConsumerAccess.bind(f.member,RUN); var second = ScopedSrsConsumerAccess.bind(f.member,RUN);
        var third = ScopedSrsConsumerAccess.bind(f.member,RUN);
        var entered = new java.util.concurrent.CountDownLatch(3); var release = new java.util.concurrent.CountDownLatch(1);
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        when(f.store.progress(RUN.context())).thenAnswer(call -> {
            reads.incrementAndGet(); assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); return Optional.of(f.held.get());
        });
        var threads = new java.util.concurrent.CopyOnWriteArrayList<Thread>();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(3, task -> {
            var thread = new Thread(task,"consumer-progress-reader-"+threads.size()); threads.add(thread); return thread;
        })) {
            var a = executor.submit(() -> { entered.countDown(); return first.progress(); });
            var b = executor.submit(() -> { entered.countDown(); return second.progress(); });
            var c = executor.submit(() -> { entered.countDown(); return third.progress(); });
            try {
                assertThat(entered.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                awaitParked(threads,3);
                assertThat(reads).hasValue(1);
            } finally { release.countDown(); }
            assertThat(a.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(f.held.get());
            assertThat(b.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(f.held.get());
            assertThat(c.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(f.held.get()); assertThat(reads).hasValue(2);
            f.held.set(progress(4,List.of("orders"),null));
            assertThat(first.progress()).isSameAs(f.held.get()); assertThat(reads).hasValue(3);
        }
    }

    @Test void aSharedReadCannotOutliveEitherCallersOriginalAuthorization() throws Exception {
        var f = new Fixture(); f.held.set(progress(4,List.of(),null));
        var first = ScopedSrsConsumerAccess.bind(f.member,RUN); var second = ScopedSrsConsumerAccess.bind(f.member,RUN);
        var entered = new java.util.concurrent.CountDownLatch(1); var release = new java.util.concurrent.CountDownLatch(1);
        when(f.store.progress(RUN.context())).thenAnswer(call -> {
            entered.countDown(); assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); return Optional.of(f.held.get());
        });
        var threads = new java.util.concurrent.CopyOnWriteArrayList<Thread>();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(2, task -> {
            var thread = new Thread(task); threads.add(thread); return thread;
        })) {
            var a = executor.submit(first::progress);
            try {
                assertThat(entered.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var b = executor.submit(second::progress);
                awaitParked(threads); verify(f.store,times(1)).progress(RUN.context());
                f.active.set(false); release.countDown();
                for (var read : List.of(a,b)) assertThatThrownBy(() -> read.get(10,java.util.concurrent.TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class)
                        .satisfies(failure -> refused(failure.getCause()));
            } finally { release.countDown(); }
        }
    }

    @Test void aQueuedFollowerKeepsItsOwnGuardBeforeJoiningAndAfterAnotherCallersRead() throws Exception {
        var f = new Fixture(); var original = progress(4,List.of(),null); f.held.set(original);
        var first = ScopedSrsConsumerAccess.bind(f.member,RUN); var leader = ScopedSrsConsumerAccess.bind(f.member,RUN);
        var permitted = new AtomicBoolean(); var unused = mock(SrsConsumerStore.class);
        var follower = new ScopedSrsConsumerAccess.Bound(RUN,unused,first.models(),() -> {
            if (!permitted.get()) throw ScopedSrsConsumerAccess.refused(RUN);
        },first.reads());
        var entered = new java.util.concurrent.CountDownLatch(1); var release = new java.util.concurrent.CountDownLatch(1);
        when(f.store.progress(RUN.context())).thenAnswer(call -> {
            entered.countDown(); assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); return Optional.of(original);
        });
        var threads = new java.util.concurrent.CopyOnWriteArrayList<Thread>();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(3, task -> {
            var thread = new Thread(task); threads.add(thread); return thread;
        })) {
            var a = executor.submit(first::progress);
            try {
                assertThat(entered.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(follower::progress).satisfies(ScopedSrsSinkAckFactoryTest::refused);
                var b = executor.submit(leader::progress); awaitParked(threads);
                permitted.set(true); var c = executor.submit(follower::progress); awaitParked(threads,3);
                permitted.set(false); release.countDown();
                assertThat(a.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(original);
                assertThat(b.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(original);
                assertThatThrownBy(() -> c.get(10,java.util.concurrent.TimeUnit.SECONDS))
                        .isInstanceOf(java.util.concurrent.ExecutionException.class).satisfies(failure -> refused(failure.getCause()));
                verify(f.store,times(2)).progress(RUN.context()); verifyNoInteractions(unused);
            } finally { release.countDown(); }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"coded", "wrapped", "programmer"})
    void anInFlightFailureKeepsItsExactCauseAndDoesNotRemainCached(String kind) throws Exception {
        var reads = new SrsConsumerProgressReads();
        var coded = new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain","chain"),null);
        Throwable failure = switch (kind) {
            case "coded" -> coded;
            case "wrapped" -> new java.util.concurrent.CompletionException(coded);
            default -> new AssertionError("programmer failure");
        };
        var entered = new java.util.concurrent.CountDownLatch(3); var release = new java.util.concurrent.CountDownLatch(1);
        var lookups = new java.util.concurrent.atomic.AtomicInteger();
        java.util.function.Supplier<SrsConsumerProgress> lookup = () -> {
            lookups.incrementAndGet();
            try { assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            if (failure instanceof Error error) throw error;
            throw (RuntimeException)failure;
        };
        var threads = new java.util.concurrent.CopyOnWriteArrayList<Thread>();
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(3, task -> {
            var thread = new Thread(task); threads.add(thread); return thread;
        })) {
            var a = executor.submit(() -> { entered.countDown(); return reads.read(RUN,lookup); });
            var b = executor.submit(() -> { entered.countDown(); return reads.read(RUN,lookup); });
            var c = executor.submit(() -> { entered.countDown(); return reads.read(RUN,lookup); });
            try {
                assertThat(entered.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                awaitParked(threads,3); assertThat(lookups).hasValue(1);
            } finally { release.countDown(); }
            for (var result : List.of(a,b,c)) assertThatThrownBy(() -> result.get(10,java.util.concurrent.TimeUnit.SECONDS))
                    .isInstanceOf(java.util.concurrent.ExecutionException.class)
                    .satisfies(thrown -> assertThat(thrown.getCause()).isSameAs(failure));
            assertThat(lookups).hasValue(2);
        }
        var after = progress(4,List.of("orders"),null);
        assertThat(reads.read(RUN,() -> after)).isSameAs(after);
    }

    private static void awaitParked(List<Thread> threads) {
        awaitParked(threads,2);
    }
    private static void awaitParked(List<Thread> threads, int count) {
        long deadline = System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
        while ((threads.size()!=count || threads.stream().anyMatch(thread -> thread.getState()!=Thread.State.WAITING
                && thread.getState()!=Thread.State.TIMED_WAITING)) && System.nanoTime()<deadline)
            java.util.concurrent.locks.LockSupport.parkNanos(1_000_000);
        assertThat(threads).hasSize(count).allSatisfy(thread -> assertThat(thread.getState()).isIn(Thread.State.WAITING,Thread.State.TIMED_WAITING));
    }

    @Test void anotherScopeMembershipOrClaimCannotBorrowAnUnfinishedReadAndEveryFailureIsRetriedFresh() throws Exception {
        var reads = new SrsConsumerProgressReads(); var original = progress(4,List.of(),null);
        var next = SrsConsumerRun.of(SCOPE,new SrsConsumerMembership(RUN.context(),5,false,Set.of("orders"),FENCE));
        var claim = new WorkloadClaimFence(FENCE.key(),FENCE.owner(),FENCE.claimGeneration(),6,FENCE.topologyRevision());
        var nextClaim = SrsConsumerRun.of(SCOPE,new SrsConsumerMembership(RUN.context(),4,false,Set.of("orders"),claim));
        var scope = new PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),"new-cache",SCOPE.pipelineId(),SCOPE.stateGeneration());
        var nextScope = SrsConsumerRun.of(scope,new SrsConsumerMembership(RUN.context(),4,false,Set.of("orders"),FENCE));
        var entered = new java.util.concurrent.CountDownLatch(4); var release = new java.util.concurrent.CountDownLatch(1);
        java.util.function.Supplier<SrsConsumerProgress> lookup = () -> {
            entered.countDown();
            try { assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            return original;
        };
        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(4)) {
            var results = List.of(RUN,next,nextClaim,nextScope).stream().map(run -> executor.submit(() -> reads.read(run,lookup))).toList();
            try { assertThat(entered.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
            finally { release.countDown(); }
            for (var result : results) assertThat(result.get(10,java.util.concurrent.TimeUnit.SECONDS)).isSameAs(original);
        }
        var failure = new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain","chain"),null);
        assertThatThrownBy(() -> reads.read(RUN,() -> { throw failure; })).isSameAs(failure);
        var bug = new AssertionError("programmer failure");
        assertThatThrownBy(() -> reads.read(RUN,() -> { throw bug; })).isSameAs(bug);
        assertThat(reads.read(RUN,() -> original)).isSameAs(original);
    }

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
