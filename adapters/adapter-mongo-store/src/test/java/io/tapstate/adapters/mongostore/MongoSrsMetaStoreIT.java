package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.tapstate.adapters.mongostore.StoredBytes.DOCUMENT_CEILING;
import static io.tapstate.adapters.mongostore.StoredBytes.bsonSize;
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
    private static final Instant WRITTEN_AT = Instant.parse("2026-09-03T10:12:44Z");

    /**
     * Columns of the wide table the history case drives: as wide as a table gets, so that a few hundred
     * versions of it weigh past what one document carries and the case reaches that size in writes a test
     * can afford rather than in thousands of them.
     */
    private static final int WIDE_COLUMNS = 1_000;

    /** The width of the table whose schema the damaged-history cases append versions of. */
    private static final int HISTORY_COLUMNS = 20;

    /**
     * Versions written straight into a record before a damaged-history case appends through the mutator:
     * enough of them, at that width, to weigh past what the record retains, so the trim is live throughout
     * and a bound that stopped bounding shows as a record that keeps growing.
     */
    private static final int SEEDED_VERSIONS = 900;

    /**
     * Where the element that is not a version goes. Among the newest, because that is where it decides
     * anything: at the oldest end the window has already closed by the time the walk arrives, so a record
     * damaged there cannot tell a bound that holds from one that does not.
     */
    private static final int DAMAGED_AT = 895;

    /** Versions appended through the store's own mutator once such a record is seeded. */
    private static final int APPENDED_VERSIONS = 40;

    /** What the size case leaves between the record it seeds and the ceiling. */
    private static final int UNDER_THE_CEILING = 4_096;

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

    /**
     * The narrow read answers exactly what the whole record says, on a chain carrying the two things that
     * make the two reads differ: several consumers, and a schema history behind them.
     *
     * <p>The narrow read exists to leave that history on the endpoint -- it is what the cdc write path
     * takes on every run of changes, and it never looks at the history. What must not come with the saving
     * is a different answer, and it is a real risk here rather than a theoretical one: the projection names
     * the field it keeps, so a rename that misses it would return no cursors at all, and a hot path told
     * there are no consumers writes an offset bounded by nothing.
     */
    @Test
    void theCursorReadAnswersTheSameAsTheWholeRecordOnAChainWithHistoryBehindIt() {
        withStore(store -> {
            store.create(CHAIN, "7d");
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("pipe-a", Map.of("orders", 7L),
                    new ChainPosition(new SourceOrder(1L, 3L), "tok-a")));
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("pipe-b", Map.of("orders", 4L, "items", 9L),
                    new ChainPosition(new SourceOrder(1L, 1L), "tok-b")));
            store.appendSchemaVersion(CHAIN, new SchemaVersion(1L, Map.of("id", "int"), 1L));
            store.appendSchemaVersion(CHAIN, new SchemaVersion(2L, Map.of("id", "int", "name", "varchar"), 2L));

            assertThat(store.consumerOffsets(CHAIN))
                    .containsExactlyInAnyOrderElementsOf(store.read(CHAIN).orElseThrow().consumerOffsets());
        });
    }

    @Test
    void theCursorReadIsEmptyForAnUnminedChain() {
        withStore(store -> assertThat(store.consumerOffsets("never-mined")).isEmpty());
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

            store.markSnapshotComplete(CHAIN, "p1", "orders");
            store.markSnapshotComplete(CHAIN, "p1", "order_items");
            store.markSnapshotComplete(CHAIN, "p1", "orders");

            // One chain carries many tables, each snapshotted by its own capture run, so the mark is per
            // table. The re-mark exercises $addToSet against the real driver: set membership, so a replayed
            // or re-run snapshot of a table that is already marked adds nothing.
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables("p1"))
                    .containsExactly("orders", "order_items");
        });
    }

    /**
     * Two pipelines on one chain keep separate completion sets against the real driver.
     *
     * <p>The dotted update path has to create the second consumer's entry rather than reach into the
     * first's, and a document-root write would satisfy the single-pipeline case above while failing this
     * one. That failure is the defect this exists for: a pipeline new to a shared chain reads another
     * pipeline's answer, skips a load it never did, and leaves its target short of every row of that
     * table -- run healthy, nothing logged.
     */
    @Test
    void markSnapshotCompleteIsPerPipelineAgainstTheRealStore() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.markSnapshotComplete(CHAIN, "p1", "orders");
            store.markSnapshotComplete(CHAIN, "p1", "order_items");
            store.markSnapshotComplete(CHAIN, "p2", "orders");

            SrsMeta record = store.read(CHAIN).orElseThrow();
            assertThat(record.snapshotCompletedTables("p1")).containsExactly("orders", "order_items");
            assertThat(record.snapshotCompletedTables("p2")).containsExactly("orders");
            assertThat(record.snapshotCompletedTables("never-a-consumer")).isEmpty();
        });
    }

    /**
     * Detaching a consumer takes its completion marks with it and leaves every other consumer's alone.
     *
     * <p>This is what makes a pipeline asked to re-read everything actually re-read it while others stay on
     * the chain. Before completion moved onto the consumer there was nowhere to clear it from without
     * deciding it on every other consumer's behalf, so it was left alone and the re-read never happened.
     */
    @Test
    void detachingAConsumerClearsOnlyItsOwnCompletionMarks() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.markSnapshotComplete(CHAIN, "leaving", "orders");
            store.markSnapshotComplete(CHAIN, "staying", "orders");

            store.detachConsumer(CHAIN, "leaving");

            SrsMeta record = store.read(CHAIN).orElseThrow();
            assertThat(record.snapshotCompletedTables("leaving")).isEmpty();
            assertThat(record.snapshotCompletedTables("staying")).containsExactly("orders");
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

    /**
     * A chain keeps recording schema changes after its history has passed what one document can carry, and
     * what survives is the newest run of versions.
     *
     * <p>The record is one document and the schema history is the only facet of it that grows for the life
     * of a chain, so the write that records a schema change is the write that has to make room for itself —
     * there is nothing else in the record to give up. This drives the store's own mutator past the point
     * where every version appended would fit in one document, and holds the shape of what is left: the
     * newest versions, contiguous, and more than the one just appended. A bound that kept only that one
     * would leave every change already read unresolvable, and a bound that dropped entries from anywhere
     * else would leave versions that never were adjacent looking as though they were.
     */
    @Test
    void aChainKeepsRecordingSchemaChangesPastTheCeilingItsWholeHistoryWouldHaveReached() {
        withStore(store -> {
            store.create(CHAIN, null);
            // Measured, not assumed: what one entry costs is a function of the table's width, so the count
            // that reaches the ceiling is derived from the entry's own stored bytes and the case keeps
            // meaning what it says if that shape ever moves.
            long entryBytes = bsonSize(MongoSrsMetaStore.toDocument(new SrsMeta(CHAIN, null, List.of(),
                    null, List.of(new SchemaVersion(1, wideSchema(), 1)), null, 0L, 0L, null)));
            int appended = (int) (3 * DOCUMENT_CEILING / (2 * entryBytes));
            assertThat(entryBytes * appended)
                    .as("the premise: %d versions of this shape are %d bytes of history between them, past "
                                    + "the %d-byte ceiling one document may take, so a chain that kept them "
                                    + "all would have stopped recording schema changes partway through",
                            appended, entryBytes * appended, DOCUMENT_CEILING)
                    .isGreaterThan(DOCUMENT_CEILING);

            for (int version = 1; version <= appended; version++) {
                store.appendSchemaVersion(CHAIN, new SchemaVersion(version, wideSchema(), version));
            }

            List<SchemaVersion> retained = store.read(CHAIN).orElseThrow().schemaHistory();
            List<Long> versions = retained.stream().map(SchemaVersion::version).toList();

            assertThat(versions)
                    .as("the oldest versions are dropped -- %d of the %d appended survive -- and what "
                                    + "survives is the newest run of them: a contiguous window of the "
                                    + "history, ending at the version just appended",
                            versions.size(), appended)
                    .hasSizeGreaterThan(1)
                    .hasSizeLessThan(appended)
                    .isEqualTo(newestRun(versions.size(), appended));
        });
    }

    /**
     * A chain whose record carries no schema history field at all still records its next schema change.
     *
     * <p>Nothing here writes such a record — {@code create} seeds the field and no path unsets it — so what
     * this holds is the repair case, and what getting it wrong costs. This is the only write that can
     * record a schema change, so an append that read a missing array as nothing it could work with would
     * not defer one change: it would leave the chain permanently unable to say that its source's schema
     * moved, and would say so as a store that could not be reached.
     */
    @Test
    void aRecordWithNoSchemaHistoryFieldStillRecordsItsNextSchemaChange() {
        withCollection((store, collection) -> {
            store.create(CHAIN, null);
            collection.updateOne(new Document("_id", CHAIN),
                    new Document("$unset", new Document("schemaHistory", "")));

            store.appendSchemaVersion(CHAIN, new SchemaVersion(7L, Map.of("id", "int"), 7L));

            assertThat(store.read(CHAIN).orElseThrow().schemaHistory())
                    .as("the change is recorded, on a record that had nowhere to record it")
                    .extracting(SchemaVersion::version)
                    .containsExactly(7L);
        });
    }

    /**
     * An element of the history that is not a version stops neither the append nor the bound.
     *
     * <p>Two ways a record reaches that state and one answer to both: an element left null, and one left as
     * something that is not a document at all. Asking either how much it weighs answers nothing — literally
     * nothing, for the first, and a refusal of the whole update for the second — and the difference between
     * the two is only how loudly the record breaks. A weight of nothing is the dangerous one: it compares
     * as under every budget, so the walk would keep every older entry, the trim would stop trimming, and
     * the history would go back to growing by an entry per change with nothing said, which is the state the
     * bound exists to end.
     *
     * <p>The control is the same drive over an undamaged record, because what is under witness is not that
     * the append survived — it is that the record is held to the same bound either way.
     */
    @Test
    void anElementThatIsNotAVersionStopsNeitherTheAppendNorTheBound() {
        withCollection((store, collection) -> {
            long clean = appendPast(store, collection, "undamaged", -1, null);
            long lastVersion = SEEDED_VERSIONS + APPENDED_VERSIONS;

            for (Object damage : new Object[] {null, "not a version"}) {
                String chain = "damaged-" + (damage == null ? "null" : "text");

                long damaged = appendPast(store, collection, chain, DAMAGED_AT, damage);

                assertThat(damaged)
                        .as("a record with an unsizable element among its newest entries is held to the "
                                        + "same bound as one without: %d bytes against the undamaged "
                                        + "record's %d, after the same %d appends",
                                damaged, clean, APPENDED_VERSIONS)
                        .isLessThanOrEqualTo(clean);
                assertThat(store.read(chain).orElseThrow().schemaHistory())
                        .as("and the record reads back, ending at the version just appended: the element "
                                + "that is not a version is gone, with everything older than it")
                        .extracting(SchemaVersion::version)
                        .endsWith(lastVersion);
            }
        });
    }

    /**
     * A write the endpoint refuses because the record it would leave behind is too large is reported as
     * exactly that, not as a store that could not be reached.
     *
     * <p>The trim bounds the one facet that grows for the life of a chain; it cannot bound the rest, and a
     * record can still arrive at the ceiling by what else it carries — at which point every mutator fails
     * on it. What that failure must not do is send whoever reads it to check a store that is healthy:
     * nothing is wrong with the store and no retry can help, and what has to change is how much is being
     * put in one document. The endpoint reports this as an ordinary command failure, so it is only told
     * apart by being named.
     */
    @Test
    void aWriteRefusedForTheSizeOfItsRecordIsReportedAsTheSizeItIs() {
        withCollection((store, collection) -> {
            store.create(CHAIN, retentionFillingTheRecord());
            assertThat(bsonSize(collection.find(new Document("_id", CHAIN)).first()))
                    .as("the premise: the seeded record is under the ceiling, so what follows is refused "
                            + "for what it would add rather than for what is already there")
                    .isLessThan(DOCUMENT_CEILING);

            assertThatThrownBy(() -> store.upsertConsumerOffset(CHAIN,
                    new ConsumerOffset("pipeline-1", cursorOverTables(300), null)))
                    .isInstanceOf(TapstateException.class)
                    .extracting(thrown -> ((TapstateException) thrown).code())
                    .isEqualTo(IoError.DOCUMENT_TOO_LARGE);
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
            assertThatThrownBy(() -> store.markSnapshotComplete("nope", "p1", "orders"))
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

    @Test
    void aWriteBackMovesTheOffsetBackWhereTheSameMoveThroughAnAdvanceWouldNot() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(3L, 900L), "gtid:aaa-3:900"));

            // The control group, and the reason the write-back cannot share the reader's path: asked to
            // go back through it, the store matches nothing and says nothing, because for a reader that
            // is the ordinary case of a clamp to a consumer sitting behind.
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 100L), "gtid:aaa-1:100"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("gtid:aaa-3:900");

            store.rewindSourceReadOffset(CHAIN, "gtid:aaa-1:100");

            // The token lands, and the order beside it goes: a written-back position names a spot in the
            // source's own log, and no ring here ever gave that spot a coordinate.
            assertThat(store.read(CHAIN).orElseThrow().sourceRead())
                    .isEqualTo(new ChainPosition(null, "gtid:aaa-1:100"));
        });
    }

    @Test
    void aWrittenBackOffsetAdmitsTheNextAdvanceWhateverGenerationItCarries() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(9L, 900L), "gtid:aaa-9:900"));

            store.rewindSourceReadOffset(CHAIN, "gtid:aaa-1:100");

            // The run that comes back opens a generation of its own and must not have to outrank the one
            // the write-back displaced. Left in place, that order would pin the chain until generation 9
            // came round again, with every advance in between silently dropped.
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 105L), "gtid:aaa-1:105"));

            assertThat(store.read(CHAIN).orElseThrow().sourceRead())
                    .isEqualTo(new ChainPosition(new SourceOrder(1L, 105L), "gtid:aaa-1:105"));
        });
    }

    @Test
    void writingBackAnOffsetOnAnUnseededChainIsAnOrderingError() {
        withStore(store -> assertThatThrownBy(
                () -> store.rewindSourceReadOffset("never_seeded", "gtid:aaa-1:1"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("never_seeded"));
    }

    @Test
    void bothWritesToTheReadOffsetRecordWhenTheyHappened() {
        withStoreAt(Clock.fixed(WRITTEN_AT, ZoneOffset.UTC), store -> {
            store.create(CHAIN, null);
            // A seeded chain has no offset, so there is no moment to report for one -- absence, not zero.
            assertThat(store.read(CHAIN).orElseThrow().sourceReadAt()).isNull();

            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(1L, 5L), "gtid:aaa-1:5"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadAt()).isEqualTo(WRITTEN_AT);

            store.rewindSourceReadOffset(CHAIN, "gtid:aaa-1:1");
            assertThat(store.read(CHAIN).orElseThrow().sourceReadAt()).isEqualTo(WRITTEN_AT);
        });
    }

    @Test
    void anOffsetStoredWithoutItsOrderReadsBackAsThePositionItIs() {
        withCollection((store, collection) -> {
            store.create(CHAIN, null);
            // The shape a record written before the order was stored carries, and the shape a write-back
            // leaves behind. Read as "nothing has read this chain" it would send the tail to the snapshot
            // seam and re-mine every change since, which is the loss the recorded token exists to stop.
            collection.updateOne(new Document("_id", CHAIN),
                    new Document("$set", new Document("sourceReadOffset", "gtid:aaa-1:77")));

            assertThat(store.read(CHAIN).orElseThrow().sourceRead())
                    .isEqualTo(new ChainPosition(null, "gtid:aaa-1:77"));
        });
    }

    /** The newest {@code kept} of the {@code appended} versions, in the order they were appended. */
    private static List<Long> newestRun(int kept, int appended) {
        List<Long> versions = new ArrayList<>();
        for (int version = appended - kept + 1; version <= appended; version++) {
            versions.add((long) version);
        }
        return versions;
    }

    /**
     * Seeds a chain with a history already past what the record retains — with one of its newest elements
     * replaced, where {@code damagedAt} names one — then appends versions through the store's own mutator
     * and answers what the record weighs afterwards. The seeding is a raw write because the mutator under
     * witness is the thing that trims: there is no way through it to build a record it has not already cut.
     */
    private static long appendPast(MongoSrsMetaStore store, MongoCollection<Document> collection,
            String chain, int damagedAt, Object damage) {
        store.create(chain, null);
        List<Object> seeded = new ArrayList<>(storedHistory(chain, SEEDED_VERSIONS));
        if (damagedAt >= 0) {
            seeded.set(damagedAt, damage);
        }
        collection.updateOne(new Document("_id", chain), new Document("$push",
                new Document("schemaHistory", new Document("$each", seeded))));
        for (int version = SEEDED_VERSIONS + 1; version <= SEEDED_VERSIONS + APPENDED_VERSIONS; version++) {
            store.appendSchemaVersion(chain,
                    new SchemaVersion(version, StoredBytes.schemaOfWidth(HISTORY_COLUMNS), version));
        }
        return bsonSize(collection.find(new Document("_id", chain)).first());
    }

    /** That many history entries, serialised as the store's own mapping serialises them. */
    private static List<Document> storedHistory(String chain, int versions) {
        List<SchemaVersion> history = new ArrayList<>();
        for (int version = 1; version <= versions; version++) {
            history.add(new SchemaVersion(version, StoredBytes.schemaOfWidth(HISTORY_COLUMNS), version));
        }
        return MongoSrsMetaStore.toDocument(
                        new SrsMeta(chain, null, List.of(), null, history, null, 0L, 0L, null))
                .getList("schemaHistory", Document.class);
    }

    /**
     * A retention string long enough to leave the seeded record just under the ceiling — measured off the
     * record the mapping writes without one, so it stays just under if the mapping ever gains a field.
     */
    private static String retentionFillingTheRecord() {
        long empty = bsonSize(MongoSrsMetaStore.toDocument(
                new SrsMeta(CHAIN, null, List.of(), null, List.of(), "", 0L, 0L, null)));
        return "r".repeat((int) (DOCUMENT_CEILING - empty - UNDER_THE_CEILING));
    }

    /** A consumer's per-table cursor over that many tables: more bytes than the record has room for. */
    private static Map<String, Long> cursorOverTables(int tables) {
        Map<String, Long> cursor = new LinkedHashMap<>();
        for (int table = 0; table < tables; table++) {
            cursor.put("table_" + table, (long) table);
        }
        return cursor;
    }

    /** A table's field schema at the width the history case drives. */
    private static Map<String, Object> wideSchema() {
        return StoredBytes.schemaOfWidth(WIDE_COLUMNS);
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

    private interface CollectionTest {
        void run(MongoSrsMetaStore store, MongoCollection<Document> collection) throws Exception;
    }

    /** Runs a test body against a fresh meta store over a clean srs_meta collection on the replica-set. */
    private static void withStore(StoreTest test) {
        withStoreAt(Clock.systemUTC(), test);
    }

    /** The same, with the clock the store stamps its writes from decided by the caller. */
    private static void withStoreAt(Clock clock, StoreTest test) {
        withCollection(clock, (store, collection) -> test.run(store));
    }

    /** The same again, handing over the collection too, for a case that has to write a raw document. */
    private static void withCollection(CollectionTest test) {
        withCollection(Clock.systemUTC(), test);
    }

    private static void withCollection(Clock clock, CollectionTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("srs_meta");
            collection.drop();
            test.run(new MongoSrsMetaStore(collection, clock), collection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
