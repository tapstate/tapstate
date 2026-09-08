package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Reading where a pipeline resumes from, and writing it back: the shape the reading takes, the refusals
 * that stand between a request and the record, and the fact that all of them are decided before anything
 * is written.
 */
class PipelinePositionServiceTest {

    private static final Instant WRITTEN_AT = Instant.parse("2026-09-03T10:12:44Z");
    private static final String CHAIN = "shop@mysql-1";
    private static final String OTHER_CHAIN = "billing@mysql-2";

    private final FakeArtifactStore artifacts = new FakeArtifactStore();
    private final FakeSrsMetaStore meta = new FakeSrsMetaStore();
    private final TestLifecycleStores.Desired desired = new TestLifecycleStores.Desired();
    private final TestLifecycleStores.State state = new TestLifecycleStores.State();
    private final RecordingAuditStore audit = new RecordingAuditStore();
    private final Map<String, List<PipelineChains.Chain>> chains = new HashMap<>();

    private final PipelinePositionService service = new PipelinePositionService(
            chains::get, meta, artifacts, new LivePipelines(desired, state),
            new AuditGate(audit, Clock.fixed(WRITTEN_AT, ZoneOffset.UTC)));

    // ------------------------------------------------------------------ reading

    @Test
    void reportsWhereEachChainResumesWithWhatIsNeededToEditIt() {
        onOneChain("orders_sync");
        meta.put(new SrsMeta(CHAIN, new ChainPosition(new SourceOrder(3L, 91201L), "mysql-bin.000004:154"),
                List.of(new ConsumerOffset("orders_sync", Map.of(),
                        new ChainPosition(new SourceOrder(3L, 91100L), "mysql-bin.000004:100")),
                        new ConsumerOffset("orders_audit", Map.of(), null)),
                null, List.of(), null, 3L, 0L, WRITTEN_AT));

        PipelinePosition.Chain chain = service.read("orders_sync").chains().getFirst();

        assertThat(chain.chainId()).isEqualTo(CHAIN);
        assertThat(chain.sourceId()).isEqualTo("shop_db");
        assertThat(chain.tables()).containsExactly("orders", "items");
        assertThat(chain.resumeFrom())
                .isEqualTo(new PipelinePosition.Point("mysql-bin.000004:154", 3L, 91201L));
        assertThat(chain.recordedAt()).isEqualTo("2026-09-03T10:12:44Z");
        assertThat(chain.sinkAcked())
                .isEqualTo(new PipelinePosition.Point("mysql-bin.000004:100", 3L, 91100L));
        // The other pipeline on the chain is named, because a write-back moves the chain for it too.
        assertThat(chain.sharedWith()).containsExactly("orders_audit");
    }

    @Test
    void listsAChainNothingHasReadYetRatherThanLeavingItOut() {
        onOneChain("orders_sync");

        PipelinePosition.Chain chain = service.read("orders_sync").chains().getFirst();

        assertThat(chain.chainId()).isEqualTo(CHAIN);
        assertThat(chain.resumeFrom()).isNull();
        assertThat(chain.recordedAt()).isNull();
        assertThat(chain.sinkAcked()).isNull();
    }

    /**
     * A position that was written back carries no ring coordinate, and the reading says so rather than
     * filling one in. Reported with a made-up coordinate it would read as a position the engine had
     * observed, which is the claim that would go into the next comparison.
     */
    @Test
    void reportsAWrittenBackPositionWithNoRingCoordinate() {
        onOneChain("orders_sync");
        meta.put(new SrsMeta(CHAIN, new ChainPosition(null, "mysql-bin.000001:4"),
                List.of(), null, List.of(), null, 3L, 0L, WRITTEN_AT));

        assertThat(service.read("orders_sync").chains().getFirst().resumeFrom())
                .isEqualTo(new PipelinePosition.Point("mysql-bin.000001:4", null, null));
    }

    // ------------------------------------------------------------ writing back

    @Test
    void movesTheChainToTheTokenTheDocumentComesBackWith() {
        onOneChain("orders_sync");
        atRest("orders_sync");
        meta.put(seeded("mysql-bin.000004:154"));

        PipelinePosition after = service.writeBack("alice", "orders_sync",
                new PipelinePosition("orders_sync",
                        List.of(PipelinePosition.Chain.resumingAt(CHAIN, "mysql-bin.000001:4"))));

        assertThat(meta.rewinds).containsExactly(Map.entry(CHAIN, "mysql-bin.000001:4"));
        // The reader's own path is never used for this: it would match nothing and report success.
        assertThat(meta.advances).isEmpty();
        assertThat(after.chains().getFirst().resumeFrom().token()).isEqualTo("mysql-bin.000001:4");
        assertThat(audit.records).extracting(AuditRecord::operationId).containsExactly("pipeline.set-position");
    }

    /**
     * A write-back that the next advance undoes is not one. The clamp on a source-read advance takes the
     * lowest position across the chain's consumers, and an ack recorded in an earlier generation ranks
     * below anything the resumed run has read — so it would be taken, and it sits ahead of where the
     * write-back put the offset. Letting the acks go is what leaves nothing to outrank the move.
     */
    @Test
    void letsGoOfTheAcksOnTheChainSoTheFirstAdvanceCannotUndoTheMove() {
        onOneChain("orders_sync");
        atRest("orders_sync");
        atRest("orders_audit");
        meta.put(new SrsMeta(CHAIN, new ChainPosition(new SourceOrder(3L, 91201L), "mysql-bin.000004:154"),
                List.of(new ConsumerOffset("orders_sync", Map.of("orders", 12L),
                                new ChainPosition(new SourceOrder(3L, 91100L), "mysql-bin.000004:100"),
                                List.of("orders")),
                        new ConsumerOffset("orders_audit", Map.of(),
                                new ChainPosition(new SourceOrder(3L, 5L), "mysql-bin.000004:5"))),
                null, List.of(), null, 3L, 0L, WRITTEN_AT));

        service.writeBack("alice", "orders_sync",
                new PipelinePosition("orders_sync",
                        List.of(PipelinePosition.Chain.resumingAt(CHAIN, "mysql-bin.000001:4"))));

        List<ConsumerOffset> after = meta.read(CHAIN).orElseThrow().consumerOffsets();
        // Every consumer on the chain, not just the one asked about: the move is the chain's, and any
        // surviving ack would clamp the next advance back over it.
        assertThat(after).allSatisfy(offset -> assertThat(offset.sinkAcked()).isNull());
        // What did happen is not unsaid: the read cursor and the finished initial loads stay, because
        // moving the tail says nothing about either.
        ConsumerOffset mine = after.stream()
                .filter(offset -> offset.pipelineId().equals("orders_sync")).findFirst().orElseThrow();
        assertThat(mine.perTableSeq()).containsEntry("orders", 12L);
        assertThat(mine.snapshotCompletedTables()).containsExactly("orders");
    }

    @Test
    void refusesWhileThePipelineItselfIsStillUp() {
        onOneChain("orders_sync");
        running("orders_sync");
        meta.put(seeded("mysql-bin.000004:154"));

        TapstateException refused = refuse("orders_sync", CHAIN, "mysql-bin.000001:4");

        assertThat(refused.code()).isEqualTo(PositionError.WRITE_BACK_WHILE_LIVE);
        assertThat(refused.args()).containsEntry("pipelines", List.of("orders_sync"));
        assertThat(meta.rewinds).isEmpty();
    }

    /**
     * The case a guard reading only the named pipeline waves through, and the reason this one reads the
     * whole chain. A chain is keyed by the source's physical coordinates and excludes the table subset,
     * so a second pipeline on the same database is on the same chain by construction — and a write-back
     * moves where <em>its</em> read picks up, not just this one's.
     */
    @Test
    void refusesWhileAnotherPipelineOnTheSameChainIsStillUp() {
        onOneChain("orders_sync");
        chains.put("orders_audit", List.of(new PipelineChains.Chain(CHAIN, "shop_db", List.of("orders"))));
        artifacts.put(pipeline("orders_audit", "shop_db"));
        atRest("orders_sync");
        running("orders_audit");
        meta.put(new SrsMeta(CHAIN, new ChainPosition(new SourceOrder(3L, 9L), "mysql-bin.000004:154"),
                List.of(new ConsumerOffset("orders_audit", Map.of(), null)),
                null, List.of(), null, 3L, 0L, WRITTEN_AT));

        TapstateException refused = refuse("orders_sync", CHAIN, "mysql-bin.000001:4");

        assertThat(refused.code()).isEqualTo(PositionError.WRITE_BACK_WHILE_LIVE);
        assertThat(refused.args()).containsEntry("pipelines", List.of("orders_audit"));
        assertThat(meta.rewinds).isEmpty();
    }

    /**
     * Pausing suspends the engine and nothing else — the capture goes on reading and goes on advancing
     * the offset being written. This is where the guard differs from the one an artifact edit passes,
     * which lets a paused pipeline through because both ways out of a pause re-read the definition.
     */
    @Test
    void refusesWhileThePipelineIsPausedBecauseTheCaptureIsStillReading() {
        onOneChain("orders_sync");
        state.put("orders_sync", PipelineState.PAUSED);
        desired.put("orders_sync", PipelineState.PAUSED);
        meta.put(seeded("mysql-bin.000004:154"));

        assertThat(refuse("orders_sync", CHAIN, "mysql-bin.000001:4").code())
                .isEqualTo(PositionError.WRITE_BACK_WHILE_LIVE);
        assertThat(meta.rewinds).isEmpty();
    }

    @Test
    void refusesAChainThePipelineDoesNotRead() {
        onOneChain("orders_sync");
        atRest("orders_sync");
        meta.put(seeded("mysql-bin.000004:154"));

        TapstateException refused = refuse("orders_sync", "someone-elses-chain", "mysql-bin.000001:4");

        assertThat(refused.code()).isEqualTo(PositionError.CHAIN_NOT_READ);
        assertThat(refused.args()).containsEntry("known", List.of(CHAIN));
        assertThat(meta.rewinds).isEmpty();
    }

    /**
     * A reading sent back changed is refused by name rather than dropped. Ignored, the caller is told the
     * write-back landed and finds their edit gone — and the acked position is the reading most likely to
     * be edited by someone who thinks it is where the read starts.
     */
    @Test
    void refusesAnEditToAReadingInsteadOfIgnoringIt() {
        onOneChain("orders_sync");
        atRest("orders_sync");
        meta.put(new SrsMeta(CHAIN, new ChainPosition(new SourceOrder(3L, 9L), "mysql-bin.000004:154"),
                List.of(new ConsumerOffset("orders_sync", Map.of(),
                        new ChainPosition(new SourceOrder(3L, 5L), "mysql-bin.000004:100"))),
                null, List.of(), null, 3L, 0L, WRITTEN_AT));

        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> service.writeBack("alice", "orders_sync",
                        new PipelinePosition("orders_sync", List.of(new PipelinePosition.Chain(
                                CHAIN, null, List.of(), PipelinePosition.Point.at("mysql-bin.000001:4"),
                                null, PipelinePosition.Point.at("mysql-bin.000009:1"), List.of())))));

        assertThat(refused.code()).isEqualTo(PositionError.FIELD_NOT_EDITABLE);
        assertThat(refused.args()).containsEntry("field", "sinkAcked");
        assertThat(meta.rewinds).isEmpty();
    }

    @Test
    void refusesADocumentSentBackUnchanged() {
        onOneChain("orders_sync");
        atRest("orders_sync");
        meta.put(seeded("mysql-bin.000004:154"));

        PipelinePosition unchanged = service.read("orders_sync");
        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> service.writeBack("alice", "orders_sync", unchanged));

        assertThat(refused.code()).isEqualTo(PositionError.NOTHING_TO_WRITE);
        assertThat(meta.rewinds).isEmpty();
    }

    /**
     * Two chains, the second one refused: the first must not have moved. Half a write-back is a state
     * nobody asked for, and the message the caller gets does not mention it.
     */
    @Test
    void movesNothingWhenAnyChainInTheRequestIsRefused() {
        chains.put("orders_sync", List.of(
                new PipelineChains.Chain(CHAIN, "shop_db", List.of("orders")),
                new PipelineChains.Chain(OTHER_CHAIN, "billing_db", List.of("invoices"))));
        artifacts.put(source("shop_db"));
        artifacts.put(source("billing_db"));
        artifacts.put(pipeline("orders_sync", "shop_db"));
        atRest("orders_sync");
        meta.put(seeded("mysql-bin.000004:154"));
        meta.put(new SrsMeta(OTHER_CHAIN, new ChainPosition(new SourceOrder(1L, 1L), "mysql-bin.000002:9"),
                List.of(), null, List.of(), null, 1L, 0L, WRITTEN_AT));

        TapstateException refused = catchThrowableOfType(TapstateException.class,
                () -> service.writeBack("alice", "orders_sync",
                        new PipelinePosition("orders_sync", List.of(
                                PipelinePosition.Chain.resumingAt(CHAIN, "mysql-bin.000001:4"),
                                new PipelinePosition.Chain(OTHER_CHAIN, "someone-else", List.of(),
                                        PipelinePosition.Point.at("mysql-bin.000002:1"), null, null,
                                        List.of())))));

        assertThat(refused.code()).isEqualTo(PositionError.FIELD_NOT_EDITABLE);
        assertThat(meta.rewinds).isEmpty();
    }

    // ------------------------------------------------------------------ setup

    private TapstateException refuse(String pipelineId, String chainId, String token) {
        return catchThrowableOfType(TapstateException.class,
                () -> service.writeBack("alice", pipelineId,
                        new PipelinePosition(pipelineId, List.of(
                                PipelinePosition.Chain.resumingAt(chainId, token)))));
    }

    private void onOneChain(String pipelineId) {
        chains.put(pipelineId, List.of(
                new PipelineChains.Chain(CHAIN, "shop_db", List.of("orders", "items"))));
        artifacts.put(source("shop_db"));
        artifacts.put(pipeline(pipelineId, "shop_db"));
    }

    private static SrsMeta seeded(String token) {
        return new SrsMeta(CHAIN, new ChainPosition(new SourceOrder(3L, 91201L), token),
                List.of(), null, List.of(), null, 3L, 0L, WRITTEN_AT);
    }

    private void atRest(String pipelineId) {
        state.put(pipelineId, PipelineState.STOPPED);
        desired.put(pipelineId, PipelineState.STOPPED);
    }

    private void running(String pipelineId) {
        state.put(pipelineId, PipelineState.RUNNING);
        desired.put(pipelineId, PipelineState.RUNNING);
    }

    private static SourceResource source(String id) {
        return new SourceResource(id, null, "mysql", Map.of(), null, null, null,
                new Srs(null, null, null, null, true), null);
    }

    private static PipelineResource pipeline(String id, String sourceId) {
        return new PipelineResource(id, null, List.of(SourceRef.bare(sourceId)), null, null, null, null, null);
    }

    /** An in-memory artifact store, enough for the reference graph the chain guard walks. */
    private static final class FakeArtifactStore implements ArtifactStore {
        private final Map<String, Resource> byId = new HashMap<>();

        void put(Resource resource) {
            byId.put(resource.id(), resource);
        }

        @Override
        public void saveAll(List<Resource> resources) {
            resources.forEach(this::put);
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(byId.values());
        }
    }

    /**
     * A meta store that keeps the records handed to it and records which write reached it. Keeping the
     * two writes apart is the point: a rewind routed through the advance would match nothing against a
     * real store and report success, so a double that answered both the same way would pass a
     * write-back that does not work.
     */
    private static final class FakeSrsMetaStore implements SrsMetaStore {
        private final Map<String, SrsMeta> records = new HashMap<>();
        final List<Map.Entry<String, String>> rewinds = new ArrayList<>();
        final List<Map.Entry<String, ChainPosition>> advances = new ArrayList<>();

        void put(SrsMeta record) {
            records.put(record.miningChainId(), record);
        }

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            return Optional.ofNullable(records.get(miningChainId));
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            rewinds.add(Map.entry(miningChainId, token));
            SrsMeta held = records.get(miningChainId);
            records.put(miningChainId, new SrsMeta(miningChainId, new ChainPosition(null, token),
                    held.consumerOffsets(), held.cdcStartPosition(), held.schemaHistory(),
                    held.retention(), held.epoch(), held.snapshotEpoch(), WRITTEN_AT));
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            advances.add(Map.entry(miningChainId, position));
        }

        @Override
        public void create(String miningChainId, String retention) {
            throw new UnsupportedOperationException("create");
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            SrsMeta held = records.get(miningChainId);
            List<ConsumerOffset> next = new ArrayList<>();
            for (ConsumerOffset existing : held.consumerOffsets()) {
                next.add(existing.pipelineId().equals(offset.pipelineId()) ? offset : existing);
            }
            records.put(miningChainId, new SrsMeta(miningChainId, held.sourceRead(), next,
                    held.cdcStartPosition(), held.schemaHistory(), held.retention(), held.epoch(),
                    held.snapshotEpoch(), held.sourceReadAt()));
        }

        @Override
        public void advanceConsumerReadSeq(
                String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException("advanceConsumerReadSeq");
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            throw new UnsupportedOperationException("advanceSinkAcked");
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            throw new UnsupportedOperationException("setCdcStart");
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException("openEpoch");
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException("appendSchemaVersion");
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
            throw new UnsupportedOperationException("markSnapshotComplete");
        }

        @Override
        public List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException("miningChainIdsWithConsumer");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException("detachConsumer");
        }

        @Override
        public void dropChain(String miningChainId) {
            throw new UnsupportedOperationException("dropChain");
        }
    }

    /** An audit store that captures every record written through it. */
    private static final class RecordingAuditStore implements AuditStore {
        final List<AuditRecord> records = new ArrayList<>();

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }
}
