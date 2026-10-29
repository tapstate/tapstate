package io.tapstate.runtime.srs;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.*;
import io.tapstate.spi.capture.*;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;

class SrsCaptureWriterTest {
    private static HazelcastInstance member;
    private static final DispatchLog SHARED_LOG = new DispatchLog();
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster", 1, "cache");
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "capture"), new WorkloadOwner("node", "boot"), 4, 0, 1);
    @BeforeAll static void startMember() {
        var config = new Config().setClusterName("native-producer-" + UUID.randomUUID());
        config.setProperty("hazelcast.phone.home.enabled", "false"); config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.setProperty("hazelcast.operation.thread.count", "2"); config.setProperty("hazelcast.operation.generic.thread.count", "2");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false); config.getJetConfig().setEnabled(false);
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(SrsItem.class).setImplementation(new SrsItemSerializer()));
        config.addRingBufferConfig(new RingbufferConfig("srs.*").setCapacity(2).setBackupCount(0).setInMemoryFormat(InMemoryFormat.OBJECT)
                .setRingbufferStoreConfig(new RingbufferStoreConfig().setEnabled(true).setFactoryImplementation(new SrsLogRingbufferStoreFactory(SHARED_LOG))));
        member = Hazelcast.newHazelcastInstance(config);
    }
    @AfterAll static void stopMember() { if (member != null) member.shutdown(); }
    private static SourceTable table(String name) { return new SourceTable(name, List.of(new SourceField("id", "int")), List.of("id"), List.of(), 99L); }
    private static void code(Throwable error, IoError code) { assertThat(error).isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code)); }

    @Test void actualModelsPrecedeEventsAndCheckpointIncludesQuietTablesAndActualPerTableSequences() {
        var f = new Fixture(); var writer = f.writer(); var callback = new AtomicReference<CaptureListener>();
        try (var source = writer.start(f.port(writer, callback, true), f.config(), CaptureStart.present(), new CaptureHealth())) {
            var initial = f.saved.getFirst();
            assertThat(initial.position()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(2), "seam"));
            assertThat(initial.tables()).hasSize(2).allSatisfy((name, at) -> { assertThat(at.model().captureEpoch()).isEqualTo(2); assertThat(at.sequence()).isEqualTo(-1); });
            callback.get().onBatch(List.of(Envelope.insert(1, "orders", Map.of("id", 1), null),
                    Envelope.insert(2, "orders", Map.of("id", 2), null)), Optional.of(new SourcePosition("batch-1")));
            callback.get().onBatch(List.of(Envelope.insert(3, "quiet", Map.of("id", 3), null)), Optional.of(new SourcePosition("batch-2")));
            var last = f.saved.getLast();
            assertThat(last.position()).isEqualTo(new ChainPosition(new SourceOrder(2, 2), "batch-2"));
            assertThat(last.tables().get("orders").sequence()).isEqualTo(1); assertThat(last.tables().get("quiet").sequence()).isZero();
            assertThat(f.log.rows.values()).hasSize(2).allSatisfy(rows -> assertThat(rows.values()).isNotEmpty().allSatisfy(row -> {
                assertThat(row.schemaVer()).isZero(); assertThat(row.epoch()).isEqualTo(2); assertThat(row.captureFence()).isEqualTo(FENCE);
            }));
            assertThat(f.published).hasSize(2).allSatisfy((key, model) -> assertThat(model.table().approximateRowCount()).isNull());
        }
        assertThat(f.loadCalls).isZero();
    }
    @Test void aSnapshotPublishesOnlyItsOwedSubsetBeforeTheCdcSessionPublishesTheCompleteSelection() {
        var f = new Fixture(); var writer = f.writer();
        writer.prepareSnapshotTables(Map.of("orders", f.actual.get("orders")));
        assertThat(f.published).hasSize(1).allSatisfy((key,model) -> assertThat(key.table()).isEqualTo("orders"));
        var callback = new AtomicReference<CaptureListener>();
        try (var source = writer.start(f.port(writer,callback,true),f.config(),CaptureStart.present(),new CaptureHealth())) {
            assertThat(f.published).hasSize(2);
            callback.get().onBatch(List.of(Envelope.insert(1,"quiet",Map.of("id",1),null)),Optional.of(new SourcePosition("next")));
            assertThat(f.saved.getLast().tables().get("quiet").sequence()).isZero();
        }
    }
    @Test void aSnapshotSubsetCannotPublishAnUnservedTableOrReplaceAnAlreadyHeldShape() {
        var f = new Fixture(); var writer = f.writer();
        assertThatThrownBy(() -> writer.prepareSnapshotTables(Map.of("other",table("other"))))
                .satisfies(error -> code(error,IoError.SRS_CHECKPOINT_UNPROVEN));
        assertThat(f.published).isEmpty();
        writer.prepareSnapshotTables(Map.of("orders",f.actual.get("orders")));
        var held = Map.copyOf(f.published);
        assertThatThrownBy(() -> writer.prepareSnapshotTables(Map.of("orders",new SourceTable("orders",List.of(new SourceField("changed","string")),List.of(),List.of()))))
                .satisfies(error -> code(error,IoError.SRS_MODEL_UNAVAILABLE));
        assertThat(f.published).isEqualTo(held); writer.close();
    }
    @Test void snapshotReceiptCannotReplaceTheCdcSessionsOwnMetadataPreparation() {
        var f = new Fixture(); var writer = f.writer(); writer.prepareSnapshotTables(f.actual);
        assertThatThrownBy(() -> writer.start(f.port(writer, new AtomicReference<>(), false), f.config(), CaptureStart.present(), new CaptureHealth()))
                .satisfies(error -> code(error, IoError.SRS_CHECKPOINT_UNPROVEN));
        assertThat(f.saved).isEmpty(); assertThat(f.log.rows).isEmpty();
    }
    @Test void oneUnprovedDdlRefusesTheWholeBatchBeforeAnyEarlierRowOrCheckpoint() {
        var f = new Fixture(); var writer = f.writer(); var callback = new AtomicReference<CaptureListener>();
        try (var source = writer.start(f.port(writer, callback, true), f.config(), CaptureStart.present(), new CaptureHealth())) {
            assertThatThrownBy(() -> callback.get().onBatch(List.of(Envelope.insert(1, "orders", Map.of("id", 1), null),
                    Envelope.ddl(2, "quiet", Map.of("origin", "alter table"))), Optional.of(new SourcePosition("unsafe"))))
                    .satisfies(error -> code(error, IoError.SRS_MODEL_UNAVAILABLE));
            assertThat(f.log.rows).isEmpty(); assertThat(f.saved).hasSize(1);
            assertThatThrownBy(() -> callback.get().onBatch(List.of(), Optional.of(new SourcePosition("late"))))
                    .satisfies(error -> code(error, IoError.CACHE_BUSINESS_FENCED));
        }
    }
    @Test void checkpointConflictNeverAdoptsAnotherRevisionOrLetsTheCallbackContinue() {
        var f = new Fixture(); var writer = f.writer(); var callback = new AtomicReference<CaptureListener>();
        try (var source = writer.start(f.port(writer, callback, true), f.config(), CaptureStart.present(), new CaptureHealth())) {
            f.conflict = true;
            assertThatThrownBy(() -> callback.get().onBatch(List.of(Envelope.insert(1, "orders", Map.of("id", 1), null)), Optional.of(new SourcePosition("uncommitted"))))
                    .satisfies(error -> code(error, IoError.SRS_CHECKPOINT_UNPROVEN));
            assertThat(f.saved).hasSize(1); assertThat(f.loadCalls).isZero(); assertThat(f.log.largestSequence(f.ring("orders"))).isZero();
            assertThatThrownBy(() -> callback.get().onBatch(List.of(), Optional.of(new SourcePosition("retry"))))
                    .satisfies(error -> code(error, IoError.CACHE_BUSINESS_FENCED));
        }
    }
    @Test void closeDuringTheFirstPiecePreventsEveryLaterPieceOfTheSameBatch() {
        var f = new Fixture(); var writer = f.writer(); var callback = new AtomicReference<CaptureListener>();
        try (var source = writer.start(f.port(writer, callback, true), f.config(), CaptureStart.present(), new CaptureHealth())) {
            f.log.afterBatch = writer::close;
            var rows = java.util.stream.IntStream.range(0, 5).mapToObj(id -> Envelope.insert(id, "orders", Map.of("id", id), null)).toList();
            assertThatThrownBy(() -> callback.get().onBatch(rows, Optional.empty()))
                    .satisfies(error -> code(error, IoError.CACHE_BUSINESS_FENCED));
            assertThat(f.log.rows.get(f.ring("orders"))).hasSize(2);
            assertThat(f.saved).hasSize(1);
        }
    }
    @Test void changedDiscoveryCannotRepublishModelsOverTheOriginalSourceResumeHistory() {
        var f = new Fixture(); var old = new SrsModelKey(f.chain, "orders", "cache", 1, 0);
        f.published.put(old, new SrsModel(old, f.ring("orders"), new SourceTable("orders", List.of(new SourceField("old", "string")), List.of(), List.of()), 0));
        f.opened = f.state(4, new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 3), "original"), Map.of("orders", new SrsTableCheckpoint(old, 3))));
        var writer = f.writer();
        assertThatThrownBy(() -> writer.prepare(f.actual)).satisfies(error -> code(error, IoError.SRS_MODEL_UNAVAILABLE));
        assertThat(f.published).hasSize(1); assertThat(f.saved).isEmpty();
        assertThatThrownBy(() -> writer.start(f.port(writer, new AtomicReference<>(), true), f.config(), CaptureStart.present(), new CaptureHealth()))
                .satisfies(error -> code(error, IoError.SRS_CHECKPOINT_UNPROVEN));
    }

    @Test void anIntactCompleteBaselineMismatchIsTypedBeforeAnyNewEpochModelIsPublished() {
        var f = new Fixture(); var refs = new TreeMap<String, SrsTableCheckpoint>();
        for (String name : f.actual.keySet()) {
            var key = new SrsModelKey(f.chain, name, "cache", 1, 0);
            var old = name.equals("orders") ? new SourceTable(name, List.of(new SourceField("old", "string")), List.of(), List.of()) : f.actual.get(name);
            f.published.put(key, new SrsModel(key, f.ring(name), old, 0)); refs.put(name, new SrsTableCheckpoint(key, -1));
        }
        f.opened = f.state(1, new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 1), "old-cut"), refs));
        var before = Map.copyOf(f.published);
        assertThatThrownBy(() -> f.writer().prepare(f.actual)).isInstanceOfSatisfying(SrsBaselineModelMismatch.class,
                failure -> assertThat(failure.chainId()).isEqualTo(f.chain));
        assertThat(f.published).isEqualTo(before); assertThat(f.saved).isEmpty();
    }

    @Test void aPendingFullUsesItsFixedModelsAndCannotCheckpointAtStreamOnStart() {
        var f = new Fixture(); var oldRefs = new TreeMap<String, SrsTableCheckpoint>(); var newRefs = new TreeMap<String, SrsModelKey>();
        for (String name : f.actual.keySet()) {
            var old = new SrsModelKey(f.chain, name, "cache", 1, 0);
            f.published.put(old, new SrsModel(old, f.ring(name), new SourceTable(name,
                    List.of(new SourceField("previous", "string")), List.of(), List.of()), 0));
            oldRefs.put(name, new SrsTableCheckpoint(old, -1));
            var current = new SrsModelKey(f.chain, name, "cache", 2, 0);
            f.published.put(current, new SrsModel(current, f.ring(name), f.actual.get(name), 0)); newRefs.put(name, current);
        }
        var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 1), "old-cut"), oldRefs);
        var full = new SrsFreshFull(new SrsFreshFullOrigin("explicit-full", new PipelineIntentScope("cluster", 1, "cache", "flow", 2),
                UUID.randomUUID().toString(), "f".repeat(64), "input"), 2, newRefs);
        f.opened = new SrsChainState(f.chain, "capture", null, 2, f.actual.keySet(), f.actual.keySet(), 1, checkpoint, Instant.EPOCH, 1, 1,
                new SourceResumeOverride("fixed-new-cut", 1, Instant.EPOCH), full);
        var writer = f.writer(); var callback = new AtomicReference<CaptureListener>();
        try (var source = writer.start(f.port(writer, callback, true), f.config(), CaptureStart.resume(new SourcePosition("fixed-new-cut")), new CaptureHealth())) {
            assertThat(f.saved).isEmpty();
            callback.get().onBatch(List.of(Envelope.insert(1, "orders", Map.of("id", 1), null)), Optional.of(new SourcePosition("actual-batch")));
            assertThat(f.saved).hasSize(1); assertThat(f.saved.getFirst().positionEditsRestricted()).isTrue();
            assertThat(f.saved.getFirst().position().token()).isEqualTo("actual-batch");
            assertThat(f.saved.getFirst().tables()).containsKeys("orders", "quiet");
            assertThat(f.opened.checkpoint()).isEqualTo(checkpoint);
        }
    }

    @Test void aLaterWriterPreservesThePersistedPositionRestrictionInItsExpectedCheckpoint() {
        var f = new Fixture(); var refs = new TreeMap<String, SrsTableCheckpoint>();
        for (String name : f.actual.keySet()) {
            var key = new SrsModelKey(f.chain, name, "cache", 1, 0);
            f.published.put(key, new SrsModel(key, f.ring(name), f.actual.get(name), 0)); refs.put(name, new SrsTableCheckpoint(key, -1));
        }
        f.opened = f.state(1, new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 1), "old-cut"), refs, true));
        var writer = f.writer();
        try (var source = writer.start(f.port(writer, new AtomicReference<>(), true), f.config(), CaptureStart.resume(new SourcePosition("old-cut")), new CaptureHealth())) {
            assertThat(f.saved).hasSize(1); assertThat(f.saved.getFirst().positionEditsRestricted()).isTrue();
        }
    }
    @Test void resumedIntroductionUsesDurableHighwaterAndKeepsTheExactOldSourceToken() {
        var f = new Fixture(); var refs = new LinkedHashMap<String, SrsTableCheckpoint>();
        for (String name : f.actual.keySet()) {
            var key = new SrsModelKey(f.chain, name, "cache", 1, 0);
            f.published.put(key, new SrsModel(key, f.ring(name), table(name), 0)); refs.put(name, new SrsTableCheckpoint(key, 4));
            f.log.high.put(f.ring(name), 9L);
        }
        f.opened = f.state(7, new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 5), "original"), refs));
        var writer = f.writer(); var callback = new AtomicReference<CaptureListener>();
        try (var source = writer.start(f.port(writer, callback, true), f.config(), CaptureStart.resume(new SourcePosition("original")), new CaptureHealth())) {
            for (String name : f.actual.keySet()) {
                var model = f.published.get(new SrsModelKey(f.chain, name, "cache", 2, 0));
                assertThat(model.ddlSequence()).isEqualTo(10);
                assertThat(f.saved.getFirst().tables().get(name).sequence()).isEqualTo(9);
            }
            assertThat(f.saved.getFirst().position().token()).isEqualTo("original");
            callback.get().onBatch(List.of(Envelope.insert(1, "orders", Map.of("id", 1), null)), Optional.of(new SourcePosition("next")));
            assertThat(f.saved.getLast().tables().get("orders").sequence()).isEqualTo(10);
        }
    }
    @Test void originalPublicationRetryKeepsItsFirstSequenceAndCloseFencesAllLateWork() {
        var f = new Fixture(); var writer = f.writer(); writer.prepare(f.actual);
        var held = Map.copyOf(f.published); f.log.high.put(f.ring("orders"), 20L); writer.prepare(f.actual);
        assertThat(f.published).isEqualTo(held); writer.close();
        assertThatThrownBy(() -> writer.prepare(f.actual)).satisfies(error -> code(error, IoError.CACHE_BUSINESS_FENCED));
        assertThat(f.saved).isEmpty();
    }
    @Test void nativeRunUnitOpensPreparedModelsAndOnlyAcknowledgesItsDurableCaptureCheckpoint() {
        var f = new Fixture(); f.actual.keySet().forEach(name -> SHARED_LOG.stores.put(f.ring(name),f.log));
        var callback = new AtomicReference<CaptureListener>(); var told = new ArrayList<String>(); var stopped = new AtomicBoolean();
        try (var unit = new NativeSrsCaptureRunUnit(SCOPE,new SrsStores(f.chains,f.models,SHARED_LOG,unusedConsumers),member)) {
            var tail = unit.startTail(SCOPE,f.opened,FENCE,f.config(),CaptureStart.present(),writer -> new CapturePort() {
                @Override public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener) {
                    writer.prepare(f.actual); callback.set(listener); ((CaptureStartedListener)listener).onStart(new SourcePosition("seam"));
                    return new Subscription() {
                        @Override public void acknowledge(SourcePosition position) { told.add(position.token()); }
                        @Override public void close() { stopped.set(true); }
                    };
                }
                @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError(); }
                @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
                @Override public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
            },() -> { if (!f.active.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); });
            assertThat(f.published).hasSize(2); assertThat(told).containsExactly("seam");
            callback.get().onBatch(List.of(Envelope.insert(1,"orders",Map.of("id",1),null)),Optional.of(new SourcePosition("durable")));
            assertThat(f.saved.getLast().position().token()).isEqualTo("durable");
            tail.close(); tail.close(); assertThat(stopped).isTrue();
            assertThatThrownBy(() -> callback.get().onBatch(List.of(),Optional.of(new SourcePosition("late"))))
                    .satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED));
        }
    }

    @Test void explicitResumeUsesTheRequestedTokenAndPublishesOnlyTheNewActualCheckpoint() {
        var f = new Fixture(); var previous = new LinkedHashMap<String,SrsTableCheckpoint>();
        for (String table : f.actual.keySet()) {
            var key = new SrsModelKey(f.chain,table,SCOPE.cacheIncarnation(),1,0);
            previous.put(table, new SrsTableCheckpoint(key,-1)); f.published.put(key,new SrsModel(key,f.ring(table),f.actual.get(table),0));
        }
        var checkpoint = new SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1),"old-confirmed"),previous);
        f.opened = new SrsChainState(f.chain,"capture",null,2,f.actual.keySet(),f.actual.keySet(),1,checkpoint,Instant.EPOCH,0,0,
                new SourceResumeOverride("requested",1,Instant.EPOCH));
        var writer = f.writer(); assertThat(f.saved).isEmpty();
        try (var source = writer.start(f.port(writer,new AtomicReference<>(),true),f.config(),CaptureStart.resume(new SourcePosition("requested")),new CaptureHealth())) {
            assertThat(f.saved).hasSize(1); assertThat(f.saved.getFirst().position()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(2),"requested"));
            assertThat(f.opened.checkpoint()).isEqualTo(checkpoint);
        }
    }

    @Test void aFailedTailRetainsItsOriginalPortCloseUntilCleanupIsProvedEvenAfterGuardLoss() {
        var f=new Fixture();var released=new AtomicBoolean();var closes=new java.util.concurrent.atomic.AtomicInteger();
        Runnable owner=() -> {if(!f.active.get())throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null);};
        var mismatch=new SrsBaselineModelMismatch(f.chain,"orders");
        class Opening implements CapturePort,AutoCloseable {
            public Subscription cdc(CaptureConfig config,CaptureStart start,CaptureListener listener){throw mismatch;}
            public CaptureBatch snapshot(CaptureConfig config){throw new AssertionError();}
            public ConnectionReport testConnection(CaptureConfig config){throw new AssertionError();}
            public DiscoveredSchema discoverSchema(CaptureConfig config){throw new AssertionError();}
            public void close(){closes.incrementAndGet();if(!released.get())throw new TapstateException(CaptureError.STILL_ENDING,Map.of("captureId","capture"),null);}
        }
        try(var unit=new NativeSrsCaptureRunUnit(SCOPE,new SrsStores(f.chains,f.models,SHARED_LOG,unusedConsumers),member)) {
            try {
                assertThatThrownBy(() -> unit.startTail(SCOPE,f.opened,FENCE,f.config(),CaptureStart.present(),writer -> new Opening(),owner)).isSameAs(mismatch);
                assertThat(closes).hasValue(1);assertThat(mismatch.getSuppressed()).singleElement().isInstanceOf(TapstateException.class);
                f.active.set(false);
                assertThatThrownBy(() -> unit.stopOpenings(owner)).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(CaptureError.STILL_ENDING));
                assertThat(closes).hasValue(2);
            } finally {released.set(true);unit.stopOpenings(owner);}
            assertThat(closes).hasValue(3);unit.stopOpenings(owner);assertThat(closes).hasValue(3);
        }
    }

    private static final class Fixture {
        final String chain = "chain-" + UUID.randomUUID(); final AtomicBoolean active = new AtomicBoolean(true);
        final Map<String, SourceTable> actual = Map.of("orders", table("orders"), "quiet", table("quiet"));
        final Map<SrsModelKey, SrsModel> published = new LinkedHashMap<>(); final List<SrsCaptureCheckpoint> saved = new ArrayList<>();
        final Log log = new Log(); boolean conflict; int loadCalls; SrsChainState opened = state(0, null);
        SrsChainState state(long revision, SrsCaptureCheckpoint checkpoint) { return new SrsChainState(chain, "capture", null, 2, actual.keySet(), actual.keySet(), revision, checkpoint, checkpoint == null ? null : Instant.EPOCH, 0, 0); }
        CaptureConfig config() { return new CaptureConfig("connector", Map.of(), List.copyOf(actual.keySet())); }
        String ring(String table) { return SrsRingbuffer.ringName(chain, table); }
        SrsCaptureWriter writer() {
            for (String name : actual.keySet()) SHARED_LOG.stores.put(ring(name), log);
            return new SrsCaptureWriter(SCOPE, opened, FENCE, new SrsStores(chains, models, SHARED_LOG, unusedConsumers), member, () -> {
                if (!active.get()) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null);
            });
        }
        CapturePort port(SrsCaptureWriter writer, AtomicReference<CaptureListener> callback, boolean prepare) {
            return new CapturePort() {
                @Override public CaptureBatch snapshot(CaptureConfig config) { throw new AssertionError(); }
                @Override public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
                    if (prepare) writer.prepare(actual); callback.set(listener);
                    ((CaptureStartedListener)listener).onStart(start instanceof CaptureStart.Resume resume ? resume.position() : new SourcePosition("seam"));
                    return () -> {};
                }
                @Override public DiscoveredSchema discoverSchema(CaptureConfig config) { throw new AssertionError(); }
                @Override public ConnectionReport testConnection(CaptureConfig config) { throw new AssertionError(); }
            };
        }
        final SrsModelStore models = new SrsModelStore() {
            @Override public SrsModelStore guardedBy(Runnable guard) { return this; }
            @Override public SrsModel publishInitial(SrsModelKey key, String ring, SourceTable actual, WorkloadClaimFence fence) {
                SrsModel held = published.get(key);
                return publish(new SrsModel(key, ring, actual, held == null ? log.largestSequence(ring) + 1 : held.ddlSequence()), fence);
            }
            @Override public SrsModel publish(SrsModel model, WorkloadClaimFence fence) { assertThat(fence).isEqualTo(FENCE); published.putIfAbsent(model.key(), model); return published.get(model.key()); }
            @Override public Optional<SrsModel> load(SrsModelKey key) { return Optional.ofNullable(published.get(key)); }
            @Override public Optional<Map<String, SrsModel>> publishedForNewSnapshot(String chain, Map<String, String> rings) { throw new AssertionError("no latest lookup"); }
            @Override public SrsModel requireEvent(SrsModelKey key, String ring, long seq, WorkloadClaimFence fence) { throw new AssertionError(); }
        };
        final SrsChainStore chains = new SrsChainStore() {
            @Override public SrsChainStore guardedBy(Runnable guard) { return this; }
            @Override public Optional<SrsChainState> checkpoint(String id, long expected, SrsCaptureCheckpoint value, WorkloadClaimFence fence) {
                assertThat(id).isEqualTo(chain); assertThat(fence).isEqualTo(FENCE); assertThat(expected).isEqualTo(opened.checkpointRevision() + saved.size());
                if (conflict) return Optional.empty(); saved.add(value); return Optional.of(state(expected + 1, value));
            }
            @Override public Optional<SrsChainState> load(String chain) { loadCalls++; return Optional.of(saved.isEmpty() ? opened : state(opened.checkpointRevision()+saved.size(),saved.getLast())); }
            @Override public SrsChainState prepare(String c, String r, WorkloadClaimFence f) { throw new AssertionError(); }
            @Override public SrsChainState requestTables(String c, Set<String> t, WorkloadClaimFence f) { throw new AssertionError(); }
            @Override public OptionalLong openEpoch(String c, long e, WorkloadClaimFence f) { throw new AssertionError(); }
            @Override public boolean publishServingTables(String c, long e, Set<String> t, WorkloadClaimFence f) { throw new AssertionError(); }
        };
    }
    private static final class Log implements SrsLogStore {
        final Map<String, Long> high = new ConcurrentHashMap<>(); final Map<String, Map<Long, SrsLogRecord>> rows = new ConcurrentHashMap<>();
        Runnable afterBatch = () -> {};
        @Override public void store(String ring, long seq, SrsLogRecord record) { rows.computeIfAbsent(ring, ignored -> new ConcurrentHashMap<>()).put(seq, record); high.merge(ring, seq, Math::max); }
        @Override public void storeAll(String ring, long first, List<SrsLogRecord> values) { for (int i = 0; i < values.size(); i++) store(ring, first + i, values.get(i)); afterBatch.run(); }
        @Override public Optional<SrsLogRecord> load(String ring, long seq) { return Optional.ofNullable(rows.getOrDefault(ring, Map.of()).get(seq)); }
        @Override public long largestSequence(String ring) { return high.getOrDefault(ring, -1L); }
        @Override public void trim(String ring, long seq) { throw new AssertionError(); }
    }
    private static final class DispatchLog implements SrsLogStore {
        final Map<String, Log> stores = new ConcurrentHashMap<>();
        @Override public void store(String r, long s, SrsLogRecord v) { stores.get(r).store(r, s, v); }
        @Override public void storeAll(String r, long s, List<SrsLogRecord> v) { stores.get(r).storeAll(r, s, v); }
        @Override public Optional<SrsLogRecord> load(String r, long s) { return stores.get(r).load(r, s); }
        @Override public long largestSequence(String r) { return stores.get(r).largestSequence(r); }
        @Override public void trim(String r, long s) { throw new AssertionError(); }
    }
    private static final SrsConsumerStore unusedConsumers = new SrsConsumerStore() {
        @Override public SrsConsumerStore guardedBy(Runnable g) { g.run(); return this; }
        @Override public Optional<SrsConsumerMembership> find(SrsConsumerKey k) { throw new AssertionError(); }
        @Override public SrsConsumerTopology attached(String c) { throw new AssertionError(); }
        @Override public Optional<ConsumerOffset> cursor(SrsConsumerContext c) { throw new AssertionError(); }
        @Override public Optional<SrsConsumerProgress> progress(SrsConsumerContext c) { throw new AssertionError(); }
        @Override public boolean pinSnapshot(SrsConsumerContext c,long m,long p,SrsSnapshotPin s,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean beginWriterRun(SrsConsumerContext c,long m,String r,Map<String,List<String>> w,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean advanceWriter(SrsConsumerContext c,long m,String r,String t,String w,WriterProgress p,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean startReadingAfter(SrsConsumerContext c,long m,long p,String t,long s,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean startReadingAfter(SrsConsumerContext c,long m,long p,Map<String,Long> arrivals,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public boolean advanceReadSequence(SrsConsumerContext c,long m,String t,long s,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public Optional<SrsConsumerMembership> attach(SrsConsumerContext c,long e,Set<String> t,WorkloadClaimFence f) { throw new AssertionError(); }
        @Override public Optional<SrsConsumerMembership> detach(SrsConsumerContext c,long e,WorkloadClaimFence f) { throw new AssertionError(); }
    };
}
