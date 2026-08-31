package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Witnesses the SRS meta store's atomic operations against a real Mongo replica-set: a seed lands an
 * empty record, a re-seed is refused (insert-only, so the accumulated truth is never discarded), the
 * source read offset and cdc start position advance, a consumer cursor is inserted then replaced in
 * place by pipeline id, the schema history appends in order, and a mutate on an unseeded chain is an
 * ordering error. Where Docker is absent this aborts on a developer machine and fails in CI, where a
 * skip would be a green build that ran nothing.
 */
@RequiresDocker
class MongoSrsMetaStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final String CHAIN = "orders@mysql-1";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void createSeedsAnEmptyRecordAndReadReturnsIt() {
        withStore(store -> {
            store.create(CHAIN, "7d");

            SrsMeta seeded = store.read(CHAIN).orElseThrow();
            assertThat(seeded.miningChainId()).isEqualTo(CHAIN);
            assertThat(seeded.retention()).isEqualTo("7d");
            assertThat(seeded.sourceReadOffset()).isNull();
            assertThat(seeded.cdcStartPosition()).isNull();
            assertThat(seeded.consumerOffsets()).isEmpty();
            assertThat(seeded.schemaHistory()).isEmpty();
        });
    }

    @Test
    void readReturnsEmptyForAnUnminedChain() {
        withStore(store -> assertThat(store.read("never-mined")).isEmpty());
    }

    @Test
    void createIsInsertOnlyAndDoesNotDiscardAccumulatedTruth() {
        withStore(store -> {
            store.create(CHAIN, "7d");
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 500L), "gtid:aaa-1:500"));

            // a second seed must be refused: overwriting would discard the advanced offset. The
            // collision is a caller ordering error, surfaced bare like the unseeded-mutate error.
            assertThatThrownBy(() -> store.create(CHAIN, "30d")).isInstanceOf(IllegalStateException.class);

            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:500");
        });
    }

    @Test
    void advanceSourceReadOffsetPersistsTheOpaqueToken() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 900L), "gtid:aaa-1:900"));

            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:900");
        });
    }

    @Test
    void advanceSourceReadOffsetPersistsTheOrderBesideTheToken() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(3L, 42L), "gtid:aaa-1:42"));

            // Both halves survive the round trip. The token is what a read resumes from; the order is what
            // the next advance is ranked against, and a stored token whose order was dropped can no longer
            // be told from a rewind.
            assertThat(store.read(CHAIN).orElseThrow().sourceRead())
                    .isEqualTo(new ChainPosition(new SourceOrder(3L, 42L), "gtid:aaa-1:42"));
        });
    }

    @Test
    void sourceReadOffsetOnlyEverMovesForward() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 900L), "gtid:aaa-1:900"));

            // Lower sequence, same generation: a clamp to a consumer that is behind. Ignored.
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 500L), "gtid:aaa-1:500"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:900");

            // Lower generation: a stale writer from before a restart. Ignored.
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(0L, 9999L), "gtid:aaa-1:1"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:900");

            // The same position again: not an advance either, and not an error.
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 900L), "gtid:aaa-1:900"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:900");

            // Forward, same generation.
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 901L), "gtid:aaa-1:901"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-1:901");

            // Forward by generation, even though the sequence restarts: a rebuilt ring numbers from zero.
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(2L, 0L), "gtid:aaa-2:1"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-2:1");
        });
    }

    @Test
    void upsertConsumerOffsetInsertsThenReplacesInPlaceByPipelineId() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("p1", Map.of("orders", 10L), null));
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("p2", Map.of("orders", 20L), null));
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("p1", Map.of("orders", 99L), new ChainPosition(new SourceOrder(1, 99), "gtid:aaa-1:99")));

            List<ConsumerOffset> cursors = store.read(CHAIN).orElseThrow().consumerOffsets();
            assertThat(cursors).extracting(ConsumerOffset::pipelineId).containsExactlyInAnyOrder("p1", "p2");
            ConsumerOffset p1 = cursors.stream().filter(c -> c.pipelineId().equals("p1")).findFirst().orElseThrow();
            assertThat(p1.perTableSeq()).containsEntry("orders", 99L);
            assertThat(p1.sinkAckedSrcpos()).isEqualTo("gtid:aaa-1:99");
        });
    }

    @Test
    void advanceConsumerReadSeqAdvancesTheReadCursorWithoutClobberingTheSinkAckedPosition() {
        withStore(store -> {
            store.create(CHAIN, null);
            // The sink has acked a position for p1's cursor; the reader then advances its per-table read
            // cursor. The read cursor and the sink-ack are independent writers of one consumer record, so
            // the reader's advance must leave the acked position untouched.
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("p1", Map.of("orders", 5L), new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100")));

            store.advanceConsumerReadSeq(CHAIN, "p1", "orders", 42L);

            ConsumerOffset p1 = onlyConsumer(store);
            assertThat(p1.perTableSeq()).containsEntry("orders", 42L);
            assertThat(p1.sinkAckedSrcpos()).isEqualTo("gtid:aaa-1:100");
        });
    }

    @Test
    void advanceConsumerReadSeqCreatesTheCursorWhenTheConsumerHasNoneYet() {
        withStore(store -> {
            store.create(CHAIN, null);
            // A reader may advance before the pipeline's sink first acks: the deep set creates the consumer
            // entry, and its acked position stays absent until a sink writes one.
            store.advanceConsumerReadSeq(CHAIN, "p1", "orders", 7L);

            ConsumerOffset p1 = onlyConsumer(store);
            assertThat(p1.perTableSeq()).containsEntry("orders", 7L);
            assertThat(p1.sinkAckedSrcpos()).isNull();
        });
    }

    @Test
    void advanceSinkAckedSrcposAdvancesTheAckedPositionWithoutClobberingTheReadCursor() {
        withStore(store -> {
            store.create(CHAIN, null);
            // The reader has advanced p1's per-table cursor; the sink then acks a durable position. The read
            // cursor and the sink-ack are independent writers of one consumer record, so the sink's advance
            // must leave the read cursor untouched.
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("p1", Map.of("orders", 42L), null));

            store.advanceSinkAcked(CHAIN, "p1", new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100"));

            ConsumerOffset p1 = onlyConsumer(store);
            assertThat(p1.sinkAckedSrcpos()).isEqualTo("gtid:aaa-1:100");
            assertThat(p1.perTableSeq()).containsEntry("orders", 42L);
        });
    }

    @Test
    void advanceSinkAckedCreatesTheConsumerWhenItHasNoneYet() {
        withStore(store -> {
            store.create(CHAIN, null);
            // A sink may ack before the reader publishes any cursor: the deep set creates the consumer entry,
            // and its read cursor stays empty until a reader writes one.
            store.advanceSinkAcked(CHAIN, "p1", new ChainPosition(new SourceOrder(1, 7), "gtid:aaa-1:7"));

            ConsumerOffset p1 = onlyConsumer(store);
            assertThat(p1.sinkAckedSrcpos()).isEqualTo("gtid:aaa-1:7");
            assertThat(p1.perTableSeq()).isEmpty();
        });
    }

    @Test
    void setCdcStartPersistsTheSeamPositionAndItsGeneration() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.setCdcStart(CHAIN, "binlog.000042:1024", 3L);

            SrsMeta record = store.read(CHAIN).orElseThrow();
            assertThat(record.cdcStartPosition()).isEqualTo("binlog.000042:1024");
            assertThat(record.snapshotEpoch()).isEqualTo(3L);
        });
    }

    @Test
    void openEpochAllocatesTheNextGenerationAgainstTheRealStore() {
        withStore(store -> {
            store.create(CHAIN, null);
            assertThat(store.read(CHAIN).orElseThrow().epoch()).isZero();

            // The counter is advanced by the driver and read back after the write, so two members opening
            // the same chain cannot both come away with the same generation the way a read-add-write would.
            assertThat(store.openEpoch(CHAIN)).isEqualTo(1L);
            assertThat(store.openEpoch(CHAIN)).isEqualTo(2L);
            assertThat(store.read(CHAIN).orElseThrow().epoch()).isEqualTo(2L);
        });
    }

    @Test
    void openEpochLeavesTheSnapshotsPinnedGenerationAlone() {
        withStore(store -> {
            store.create(CHAIN, null);
            long running = store.openEpoch(CHAIN);
            store.setCdcStart(CHAIN, "binlog.000042:1024", running);

            store.openEpoch(CHAIN);

            // The restart that opens the next generation is exactly when a snapshot that had not drained
            // must keep the one it began in. Advancing both would hand a rerun's rows the newer generation
            // and let them overwrite changes the older one had already applied.
            SrsMeta record = store.read(CHAIN).orElseThrow();
            assertThat(record.epoch()).isEqualTo(2L);
            assertThat(record.snapshotEpoch()).isEqualTo(1L);
        });
    }

    @Test
    void aRecordWrittenBeforeGenerationsExistedReadsBackWithNoneOpened() {
        withStore(store -> {
            store.create(CHAIN, null);

            // The meta field set is append-only: a document an older build wrote carries neither
            // generation, and that has to read back as "no generation opened" rather than as corruption.
            SrsMeta record = store.read(CHAIN).orElseThrow();
            assertThat(record.epoch()).isZero();
            assertThat(record.snapshotEpoch()).isZero();
        });
    }

    @Test
    void markSnapshotCompleteIsPerTableAndIdempotentAgainstTheRealStore() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.markSnapshotComplete(CHAIN, "orders");
            store.markSnapshotComplete(CHAIN, "order_items");
            store.markSnapshotComplete(CHAIN, "orders");

            // One chain carries many tables, each snapshotted by its own capture run, so the mark is per
            // table. The re-mark exercises $addToSet against the real driver: set membership, so a replayed
            // or re-run snapshot of a table that is already marked adds nothing.
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables())
                    .containsExactly("orders", "order_items");
        });
    }

    @Test
    void appendSchemaVersionAppendsInOrder() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.appendSchemaVersion(CHAIN, new SchemaVersion(0, Map.of("id", "int"), 0));
            store.appendSchemaVersion(CHAIN, new SchemaVersion(1, Map.of("id", "int", "name", "string"), 12));

            List<SchemaVersion> history = store.read(CHAIN).orElseThrow().schemaHistory();
            assertThat(history).extracting(SchemaVersion::version).containsExactly(0L, 1L);
            assertThat(history.get(1).ddlSeq()).isEqualTo(12L);
            assertThat(history.get(1).schema()).containsEntry("name", "string");
        });
    }

    @Test
    void mutateOnAnUnminedChainIsAnOrderingError() {
        // every mutator requires the chain to have been seeded by create first; a mutate on an unseeded
        // chain is a caller ordering error, not a silent no-op.
        withStore(store -> {
            assertThatThrownBy(() -> store.advanceSourceReadOffset("nope", new ChainPosition(new SourceOrder(1L, 1L), "x")))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.upsertConsumerOffset("nope", new ConsumerOffset("p", Map.of(), null)))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.advanceConsumerReadSeq("nope", "p", "orders", 1L))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.advanceSinkAcked("nope", "p", new ChainPosition(new SourceOrder(1, 1), "gtid:aaa-1:1")))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.setCdcStart("nope", "x", 1L))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.appendSchemaVersion("nope", new SchemaVersion(0, Map.of(), 0)))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.markSnapshotComplete("nope", "orders"))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> store.openEpoch("nope"))
                    .isInstanceOf(IllegalStateException.class);
        });
    }

    @Test
    void detachConsumerRemovesOneCursorAndLeavesTheChainAndItsOtherConsumersByteForByte() {
        withStore(store -> {
            store.create(CHAIN, "7d");
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 500L), "gtid:aaa-1:500"));
            store.setCdcStart(CHAIN, "gtid:aaa-1:1", 1L);
            store.appendSchemaVersion(CHAIN, new SchemaVersion(0, Map.of("id", "int"), 0));
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("departing", Map.of("orders", 100L),
                    new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100")));
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("staying", Map.of("orders", 900L),
                    new ChainPosition(new SourceOrder(1, 900), "gtid:aaa-1:900")));

            store.detachConsumer(CHAIN, "departing");

            SrsMeta after = store.read(CHAIN).orElseThrow();
            // The chain record outlives its consumers: it is keyed by the chain, so removing it would be
            // cross-pipeline data loss, and everything on it that is not the departing cursor is untouched.
            assertThat(after.consumerOffsets())
                    .containsExactly(new ConsumerOffset("staying", Map.of("orders", 900L),
                            new ChainPosition(new SourceOrder(1, 900), "gtid:aaa-1:900")));
            assertThat(after.sourceReadOffset()).isEqualTo("gtid:aaa-1:500");
            assertThat(after.cdcStartPosition()).isEqualTo("gtid:aaa-1:1");
            assertThat(after.schemaHistory()).hasSize(1);
            assertThat(after.retention()).isEqualTo("7d");
        });
    }

    @Test
    void detachConsumerRemovesTheEntryOutrightRatherThanBlankingIt() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("departing", Map.of("orders", 100L),
                    new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100")));

            store.detachConsumer(CHAIN, "departing");

            // A cursor left present-but-empty would still be folded into the two minimums taken over every
            // consumer, which is exactly the permanent stall a detach exists to prevent.
            assertThat(store.read(CHAIN).orElseThrow().consumerOffsets()).isEmpty();
            assertThat(store.miningChainIdsWithConsumer("departing")).isEmpty();
        });
    }

    @Test
    void miningChainIdsWithConsumerNamesEveryChainThatCarriesThatConsumerAndNoOther() {
        withStore(store -> {
            store.create("chain-a", null);
            store.create("chain-b", null);
            store.create("chain-c", null);
            store.upsertConsumerOffset("chain-a", new ConsumerOffset("departing", Map.of(), null));
            store.upsertConsumerOffset("chain-b", new ConsumerOffset("departing", Map.of(), null));
            store.upsertConsumerOffset("chain-b", new ConsumerOffset("staying", Map.of(), null));
            store.upsertConsumerOffset("chain-c", new ConsumerOffset("staying", Map.of(), null));

            // Every chain it reads, not just the first: a departing consumer left on any one of them pins
            // that chain for everyone else on it.
            assertThat(store.miningChainIdsWithConsumer("departing"))
                    .containsExactlyInAnyOrder("chain-a", "chain-b");
            assertThat(store.miningChainIdsWithConsumer("never_joined")).isEmpty();
        });
    }

    @Test
    void detachConsumerIsIdempotentAndSilentOnAnUnseededChain() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("departing", Map.of(), null));

            // A detach states an end condition, so an absent cursor and an absent chain already satisfy it.
            // The advancing mutators refuse an unseeded chain; refusing here would abort a removal partway
            // and leave the consumer attached to the chains not yet reached.
            store.detachConsumer(CHAIN, "departing");
            store.detachConsumer(CHAIN, "departing");
            store.detachConsumer("never_seeded", "departing");

            assertThat(store.read(CHAIN).orElseThrow().consumerOffsets()).isEmpty();
            assertThat(store.read("never_seeded")).isEmpty();
        });
    }

    /** The single consumer cursor on the test chain — the shape the per-consumer advance tests read back. */
    private static ConsumerOffset onlyConsumer(MongoSrsMetaStore store) {
        List<ConsumerOffset> cursors = store.read(CHAIN).orElseThrow().consumerOffsets();
        assertThat(cursors).hasSize(1);
        return cursors.get(0);
    }

    private interface StoreTest {
        void run(MongoSrsMetaStore store) throws Exception;
    }

    /** Runs a test body against a fresh meta store over a clean srs_meta collection on the replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("srs_meta");
            collection.drop();
            test.run(new MongoSrsMetaStore(collection));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
