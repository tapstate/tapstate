package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SrsSnapshotPreparationTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","pipeline",3);
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),new WorkloadOwner("node","boot"),1,2,1);
    private static final SrsConsumerRun RUN = SrsConsumerRun.of(SCOPE,new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey("chain","pipeline","source"),3),4,false,Set.of("orders","quiet"),FENCE));
    private static SourceTable table(String name) { return new SourceTable(name,List.of(new SourceField("id","int")),List.of("id"),List.of(),999L); }
    private static SrsModel model(String table,long version) { return new SrsModel(new SrsModelKey("chain",table,"cache",1,version),"ring-"+table,table(table),0); }
    private static CaptureConfig config() { return new CaptureConfig("connector",Map.of(),List.of("orders","quiet")); }

    @Test void actualSessionModelsArePinnedBeforeEitherTableReadsAndTheLoadUsesThatReceipt() {
        var f = new Fixture(); var owner = f.owner(); var port = new Port(f,owner);
        try (var load = SnapshotPhase.openPrepared(port,config(),owner)) {
            assertThat(f.pinned).isNotNull(); assertThat(f.sourceReads).isOne(); assertThat(load.tailSeam()).isEqualTo("sampled-seam");
            var rows = new ArrayList<Envelope>(); var loaded = new ArrayList<String>();
            assertThat(load.read(rows::add,loaded::add)).isEqualTo(2);
            assertThat(rows).allSatisfy(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(7)));
            assertThat(loaded).containsExactly("orders","quiet");
        }
        assertThat(f.sourceReads).isEqualTo(2); assertThat(f.pinCalls).isOne(); assertThat(f.progressReads).isOne();
        assertThat(f.pinned.models()).containsEntry("orders",model("orders",2).key()).containsEntry("quiet",model("quiet",2).key());
        assertThat(f.closedSessions).isOne();
    }
    @Test void resumedSnapshotKeepsExactSavedReferencesAndAnchorWithoutQueryingNewHeads() {
        var f = new Fixture(); var saved = new SrsSnapshotPin(5,"original-seam",Map.of("orders",model("orders",2).key(),"quiet",model("quiet",2).key()));
        f.original = f.progress(saved,List.of("quiet")); f.forbidHeads = true;
        var owner = f.owner(); var port = new Port(f,owner);
        try (var load = SnapshotPhase.openPrepared(port,config(),owner)) {
            assertThat(port.selected).containsExactly("orders"); assertThat(owner.request().snapshotEpoch()).isEqualTo(5);
            assertThat(owner.request().replayAnchor()).isEqualTo(new SourcePosition("original-seam"));
            assertThat(load.tailSeam()).isEqualTo("original-seam");
            var rows = new ArrayList<Envelope>(); load.read(rows::add,ignored -> {});
            assertThat(rows).singleElement().satisfies(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(5)));
        }
        assertThat(f.pinned).isEqualTo(saved); assertThat(f.headReads).isZero(); assertThat(f.exactReads).isOne();
    }
    @Test void sourceSchemaMismatchAndMissingPublishedModelsPreventEveryRowRead() {
        var wrong = new Fixture(); wrong.actual.put("orders",new SourceTable("orders",List.of(new SourceField("changed","string")),List.of(),List.of()));
        var owner = wrong.owner();
        assertThatThrownBy(() -> SnapshotPhase.openPrepared(new Port(wrong,owner),config(),owner)).satisfies(SrsSnapshotPreparationTest::unavailable);
        assertThat(wrong.pinCalls).isZero(); assertThat(wrong.sourceReads).isZero(); assertThat(wrong.closedSessions).isOne();
        var missing = new Fixture(); missing.published = false; var pending = missing.owner();
        assertThatThrownBy(() -> SnapshotPhase.openPrepared(new Port(missing,pending),config(),pending)).satisfies(SrsSnapshotPreparationTest::unavailable);
        assertThat(missing.sourceReads).isZero(); assertThat(missing.pinCalls).isZero();
    }
    @Test void pinConflictNeverRereadsARevisionAndGuardLossNeverAcceptsRows() {
        var f = new Fixture(); var owner = f.owner(); f.currentRevision = 9;
        assertThatThrownBy(() -> SnapshotPhase.openPrepared(new Port(f,owner),config(),owner)).satisfies(SrsSnapshotPreparationTest::unavailable);
        assertThat(f.sourceReads).isZero(); assertThat(f.progressReads).isOne(); assertThat(f.pinCalls).isOne();
        assertThatThrownBy(owner::accepted).satisfies(SrsSnapshotPreparationTest::unavailable);
        var lost = new Fixture(); var abandoned = lost.owner(); lost.active.set(false);
        assertThatThrownBy(() -> abandoned.prepare(abandoned.request(),new SourcePosition("seam"),lost.actual)).isInstanceOf(TapstateException.class);
        assertThat(lost.pinCalls).isZero(); assertThat(lost.headReads).isZero();
    }
    @Test void missingPreparedCapabilityOrReceiptNeverFallsBackAndFailedCleanupStillClosesTheSession() {
        var f = new Fixture(); var owner = f.owner();
        assertThatThrownBy(() -> SnapshotPhase.openPrepared(new PlainPort(),config(),owner)).satisfies(SrsSnapshotPreparationTest::unavailable);
        var cleanupOwner = f.owner(); var port = new Port(f,cleanupOwner); port.skipPreparation = true; port.failBatchClose = true;
        assertThatThrownBy(() -> SnapshotPhase.openPrepared(port,config(),cleanupOwner)).isInstanceOfSatisfying(TapstateException.class,error -> {
            assertThat(error.code()).isEqualTo(IoError.SRS_CONSUMER_UNAVAILABLE); assertThat(error.getSuppressed()).hasSize(1);
        });
        assertThat(f.closedSessions).isOne(); assertThat(f.pinCalls).isZero();
    }
    @Test void aCompletedSnapshotOpensNoConnectorAndPartialMetadataCannotPin() {
        var f = new Fixture(); var saved = new SrsSnapshotPin(5,"original",Map.of("orders",model("orders",2).key(),"quiet",model("quiet",2).key()));
        f.original = f.progress(saved,List.of("orders","quiet")); var owner = f.owner();
        try (var load = SnapshotPhase.openPrepared(new PlainPort(),config(),owner)) { assertThat(load.readsAnything()).isFalse(); }
        assertThat(f.pinCalls).isZero();
        var incomplete = new Fixture(); var pending = incomplete.owner(); pending.beginRead();
        assertThatThrownBy(() -> pending.prepare(pending.request(),new SourcePosition("seam"),Map.of("orders",table("orders")))).satisfies(SrsSnapshotPreparationTest::unavailable);
        assertThat(incomplete.pinCalls).isZero();
    }
    @Test void aPreparedOwnerCannotAuthorizeAnotherSessionsReadWithoutItsOwnCallback() {
        var f = new Fixture(); var owner = f.owner();
        try (var first = SnapshotPhase.openPrepared(new Port(f,owner),config(),owner)) { assertThat(first.readsAnything()).isTrue(); }
        var unpreparedSecond = new Port(f,owner); unpreparedSecond.skipPreparation = true;
        assertThatThrownBy(() -> SnapshotPhase.openPrepared(unpreparedSecond,config(),owner)).satisfies(SrsSnapshotPreparationTest::unavailable);
        assertThat(unpreparedSecond.selected).isNull(); assertThat(f.closedSessions).isOne();
        int pinCalls = f.pinCalls;
        assertThatThrownBy(() -> owner.prepare(owner.request(),new SourcePosition("late"),f.actual)).satisfies(SrsSnapshotPreparationTest::unavailable);
        assertThat(f.pinCalls).isEqualTo(pinCalls);
    }
    @Test void cleanupRethrowingThePrimaryFailureCannotMaskItOrSkipSessionRelease() {
        var f = new Fixture(); var owner = f.owner(); var port = new Port(f,owner); port.skipPreparation = true;
        var failure = new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain","chain"),null); port.sameFailure = failure;
        assertThatThrownBy(() -> SnapshotPhase.openPrepared(port,config(),owner)).isSameAs(failure);
        assertThat(f.closedSessions).isOne();
    }

    @Test void closingALoadPreservesTheSessionFailureAndStillClosesItsBatch() {
        var f = new Fixture(); var owner = f.owner(); var port = new Port(f,owner);
        var load = SnapshotPhase.openPrepared(port,config(),owner);
        var failure = new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain","chain"),null);
        port.sessionFailure = failure; port.failBatchClose = true;
        assertThatThrownBy(load::close).isSameAs(failure).satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
        assertThat(f.closedSessions).isOne();
    }

    @Test void nativeUnitDrainsPreparedPrivateSnapshotWithoutOpeningOrOwningACaptureTail() throws Exception {
        var f = new Fixture(); f.nativeRings = true; var rows = new java.util.concurrent.CopyOnWriteArrayList<Envelope>();
        var completed = new java.util.concurrent.CopyOnWriteArrayList<String>();
        try (var unit = new NativeSrsCaptureRunUnit(RUN.cacheScope(),new SrsStores(f,f,f,f),unusedMember())) {
            var run = unit.beginSnapshot(RUN,config(),7,owner -> new Port(f,owner),new CaptureHandoff() {
                @Override public void accept(Envelope row) { rows.add(row); }
                @Override public void loaded(String table) { completed.add(table); }
            },() -> { if (!f.active.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); });
            assertThat(run.awaitLoaded(java.time.Duration.ofSeconds(3))).isTrue(); assertThat(run.failure()).isEmpty();
            assertThat(rows).hasSize(2).allSatisfy(row -> assertThat(row.position().order()).isEqualTo(SourceOrder.snapshotRow(7)));
            assertThat(completed).containsExactly("orders","quiet"); assertThat(run.cdcSubscription()).isEmpty();
            assertThat(run.health().receivedRows()).containsEntry("orders",Map.of("r",1L)).containsEntry("quiet",Map.of("r",1L));
            long bytes = rows.stream().mapToLong(io.tapstate.core.event.PayloadBytes::of).sum();
            assertThat(run.health().receivedBytes().values().stream().mapToLong(Long::longValue).sum()).isEqualTo(bytes);
            unit.stopSnapshot(run); unit.stopSnapshot(run); assertThat(f.closedSessions).isOne();
            assertThat(f.pinCalls).isOne();
        }
    }

    @Test void revocationAfterOneTableCannotOpenTheNextTableOrReadAnotherRow() {
        var f = new Fixture(); var owner = f.owner();
        try (var load = SnapshotPhase.openPrepared(new Port(f,owner),config(),owner)) {
            assertThatThrownBy(() -> load.read(row -> {},table -> f.active.set(false))).isInstanceOf(TapstateException.class);
        }
        assertThat(f.sourceReads).isOne(); assertThat(f.closedSessions).isOne();
    }

    @Test void losingAuthorityAfterSnapshotRegistrationReleasesTheUnreturnedRun() throws Exception {
        var f = new Fixture(); f.nativeRings = true; var unitRef = new java.util.concurrent.atomic.AtomicReference<NativeSrsCaptureRunUnit>();
        try (var unit = new NativeSrsCaptureRunUnit(RUN.cacheScope(),new SrsStores(f,f,f,f),unusedMember())) {
            unitRef.set(unit);
            assertThatThrownBy(() -> unit.beginSnapshot(RUN,config(),7,owner -> new Port(f,owner),CaptureHandoff.of(row -> {
                throw new AssertionError("a refused start cannot deliver a row");
            }),() -> {
                try { if (!registered(unitRef.get()).isEmpty()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
                catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
            })).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(registered(unit)).isEmpty(); assertThat(f.closedSessions).isOne();
        }
    }
    private static java.util.Collection<?> registered(NativeSrsCaptureRunUnit unit) throws ReflectiveOperationException {
        var field = NativeSrsCaptureRunUnit.class.getDeclaredField("snapshots"); field.setAccessible(true);
        return (java.util.Collection<?>) field.get(unit);
    }
    private static com.hazelcast.core.HazelcastInstance unusedMember() {
        return (com.hazelcast.core.HazelcastInstance) java.lang.reflect.Proxy.newProxyInstance(
                com.hazelcast.core.HazelcastInstance.class.getClassLoader(),new Class<?>[]{com.hazelcast.core.HazelcastInstance.class},
                (proxy,method,args) -> { throw new AssertionError("a private snapshot must not use the capture member"); });
    }

    private static void unavailable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.SRS_CONSUMER_UNAVAILABLE)); }
    private static class PlainPort implements CapturePort {
        @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError("unprepared snapshot must not open"); }
        @Override public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) { throw new AssertionError("no CDC in a Pipeline pin"); }
        @Override public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
        @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError("no latest discovery"); }
    }
    private static final class Port extends PlainPort implements SnapshotSession.PreparedProvider {
        final Fixture f; final SrsSnapshotPreparation owner; List<String> selected; boolean prepared,skipPreparation,failBatchClose; RuntimeException sameFailure,sessionFailure;
        Port(Fixture f,SrsSnapshotPreparation owner) { this.f = f; this.owner = owner; }
        @Override public SnapshotSession snapshotSession(CaptureConfig config) { throw new AssertionError("prepared session required"); }
        @Override public SnapshotSession snapshotSession(CaptureConfig config,SnapshotPreparation request) {
            selected = config.streams(); assertThat(request).isEqualTo(owner.request());
            return new SnapshotSession() {
                @Override public CaptureBatch read(String table) {
                    if (!prepared && !skipPreparation) {
                        var actual = new HashMap<String,SourceTable>(); selected.forEach(name -> actual.put(name,f.actual.get(name)));
                        owner.prepare(request,new SourcePosition("sampled-seam"),actual); prepared = true;
                    }
                    if (!skipPreparation) { assertThat(f.pinned).isNotNull(); f.sourceReads++; }
                    var rows = List.of(Envelope.read(1,table,Map.of("id",1),Map.of())).iterator();
                    return new CaptureBatch() {
                        @Override public Optional<SourcePosition> seam() { if (sameFailure != null) throw sameFailure; return Optional.of(new SourcePosition("sampled-seam")); }
                        @Override public boolean hasNext() { return rows.hasNext(); }
                        @Override public Envelope next() { return rows.next(); }
                        @Override public void close() { if (sameFailure != null) throw sameFailure; if (failBatchClose) throw new IllegalStateException("fixture batch cleanup failed"); }
                    };
                }
                @Override public void close() { f.closedSessions++; if (sessionFailure != null) throw sessionFailure; }
            };
        }
    }
    private static final class Fixture implements SrsConsumerStore,SrsModelStore,SrsChainStore,SrsLogStore {
        final Map<String,SourceTable> actual = new HashMap<>(Map.of("orders",table("orders"),"quiet",table("quiet")));
        final AtomicBoolean active = new AtomicBoolean(true); int pinCalls,progressReads,headReads,exactReads,sourceReads,closedSessions;
        long currentRevision = 4; boolean published = true,forbidHeads,nativeRings; SrsSnapshotPin pinned; SrsConsumerProgress original = progress(null,List.of());
        SrsConsumerProgress progress(SrsSnapshotPin snapshot,List<String> complete) {
            var cursor = new ConsumerOffset(RUN.context().key().consumerId(),Map.of("orders",-1L,"quiet",-1L),null,complete,
                    snapshot == null ? null : snapshot.sourcePosition(),snapshot == null ? 0 : snapshot.snapshotEpoch(),Map.of(),ConsumerProgressKind.SRS);
            return new SrsConsumerProgress(RUN.context(),4,4,cursor,snapshot);
        }
        SrsSnapshotPreparation owner() {
            return new SrsSnapshotPreparation((scope,guard) -> { assertThat(scope).isEqualTo(RUN.cacheScope()); guard.run(); return new SrsStores(this,this,this,this); },RUN,
                    Map.of("orders","ring-orders","quiet","ring-quiet"),7,() -> { if (!active.get()) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null); });
        }
        @Override public Fixture guardedBy(Runnable guard) { guard.run(); return this; }
        @Override public Optional<SrsConsumerProgress> progress(SrsConsumerContext context) { assertThat(context).isEqualTo(RUN.context()); progressReads++; return Optional.of(original); }
        @Override public boolean pinSnapshot(SrsConsumerContext context,long membership,long revision,SrsSnapshotPin pin,WorkloadClaimFence fence) {
            assertThat(context).isEqualTo(RUN.context()); assertThat(membership).isEqualTo(4); assertThat(revision).isEqualTo(4); assertThat(fence).isEqualTo(FENCE); pinCalls++;
            if (revision != currentRevision) return false; pinned = pin; return true;
        }
        @Override public Optional<Map<String,SrsModel>> publishedForNewSnapshot(String chain,Map<String,String> rings) {
            if (forbidHeads) throw new AssertionError("a saved reference must not consult current heads"); headReads++;
            if (!published) return Optional.empty(); var answer = new HashMap<String,SrsModel>(); rings.forEach((table,ring) -> answer.put(table,nativeRings ? new SrsModel(model(table,2).key(),SrsRingbuffer.ringName("chain",table),table(table),0) : model(table,2))); return Optional.of(answer);
        }
        @Override public Optional<SrsModel> load(SrsModelKey key) { exactReads++; assertThat(key.modelVersion()).isEqualTo(2); return Optional.of(model(key.table(),2)); }
        @Override public SrsModel requireEvent(SrsModelKey key,String ring,long sequence,WorkloadClaimFence fence) { throw new AssertionError("snapshot pinning must not resolve an event range"); }
        @Override public SrsModel publish(SrsModel model,WorkloadClaimFence fence) { throw new AssertionError("Pipeline pin cannot publish a model"); }
        @Override public Optional<SrsConsumerMembership> find(SrsConsumerKey key) { throw new AssertionError(); }
        @Override public SrsConsumerTopology attached(String chain) { throw new AssertionError(); }
        @Override public Optional<ConsumerOffset> cursor(SrsConsumerContext context) { throw new AssertionError(); }
        @Override public boolean beginWriterRun(SrsConsumerContext context,long revision,String id,Map<String,List<String>> expected,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public boolean advanceWriter(SrsConsumerContext context,long revision,String id,String table,String writer,WriterProgress progress,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public boolean advanceReadSequence(SrsConsumerContext context,long revision,String table,long sequence,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public boolean startReadingAfter(SrsConsumerContext context,long membership,long revision,String table,long sequence,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public boolean startReadingAfter(SrsConsumerContext c,long m,long p,Map<String,Long> arrivals,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public Optional<SrsConsumerMembership> attach(SrsConsumerContext context,long revision,Set<String> tables,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public Optional<SrsConsumerMembership> detach(SrsConsumerContext context,long revision,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public SrsChainState prepare(String chain,String retention,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public Optional<SrsChainState> load(String chain) { throw new AssertionError(); }
        @Override public SrsChainState requestTables(String chain,Set<String> tables,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public OptionalLong openEpoch(String chain,long epoch,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public boolean publishServingTables(String chain,long epoch,Set<String> tables,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public Optional<SrsChainState> checkpoint(String chain,long revision,SrsCaptureCheckpoint checkpoint,WorkloadClaimFence fence) { throw new AssertionError(); }
        @Override public void store(String ring,long sequence,SrsLogRecord record) { throw new AssertionError(); }
        @Override public void storeAll(String ring,long sequence,List<SrsLogRecord> records) { throw new AssertionError(); }
        @Override public Optional<SrsLogRecord> load(String ring,long sequence) { throw new AssertionError(); }
        @Override public long largestSequence(String ring) { throw new AssertionError(); }
        @Override public void trim(String ring,long sequence) { throw new AssertionError(); }
    }
}
