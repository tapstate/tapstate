package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.DurableSourceRead;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.BsonDocument;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    void aConsumerWriteSplitsEveryLegacyCursorBeforeItAdvancesOne() {
        withCollection((store, collection) -> {
            ConsumerOffset advancing = new ConsumerOffset("p1", Map.of("orders", 10L),
                    new ChainPosition(new SourceOrder(1L, 10L), "gtid:aaa-1:10"));
            ConsumerOffset untouched = new ConsumerOffset("p2", Map.of("orders", 20L),
                    new ChainPosition(new SourceOrder(1L, 20L), "gtid:aaa-1:20"));
            collection.insertOne(MongoSrsMetaStore.toDocument(
                    new SrsMeta(CHAIN, null, List.of(advancing, untouched), List.of(), null)));

            store.advanceConsumerReadSeq(CHAIN, "p1", "orders", 11L);

            Document root = collection.find(new Document("_id", CHAIN)).first();
            assertThat(root).isNotNull();
            assertThat(root.get("consumerOffsets", Document.class)).isEmpty();
            assertThat(collection.find(new Document("miningChainId", CHAIN))
                    .into(new ArrayList<>()))
                    .hasSize(2);
            assertThat(store.read(CHAIN).orElseThrow().consumerOffsets()).containsExactlyInAnyOrder(
                    new ConsumerOffset("p1", Map.of("orders", 11L), advancing.sinkAcked()),
                    untouched);
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
            // A reader may advance before the pipeline's sink first acks: the deep max creates the consumer
            // entry, and its acked position stays absent until a sink writes one.
            store.advanceConsumerReadSeq(CHAIN, "p1", "orders", 7L);

            ConsumerOffset p1 = onlyConsumer(store);
            assertThat(p1.perTableSeq()).containsEntry("orders", 7L);
            assertThat(p1.sinkAckedSrcpos()).isNull();
        });
    }

    @Test
    void unreadRegistrationKeepsAdvancedCursorsAndNamesOnlySelectedTables() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceConsumerReadSeq(CHAIN, "nest", "bench_nest_items", -1L);
            store.advanceConsumerReadSeq(CHAIN, "nest", "bench_nest_items", 7L);
            store.advanceConsumerReadSeq(CHAIN, "nest", "bench_nest_items", -1L);
            store.advanceConsumerReadSeq(CHAIN, "join", "bench_join_orders", -1L);

            Map<String, ConsumerOffset> consumers = store.consumerOffsets(CHAIN).stream()
                    .collect(java.util.stream.Collectors.toMap(ConsumerOffset::pipelineId, offset -> offset));
            assertThat(consumers.get("nest").perTableSeq()).containsExactly(Map.entry("bench_nest_items", 7L));
            assertThat(consumers.get("join").perTableSeq()).containsExactly(Map.entry("bench_join_orders", -1L));
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
    void aChangeAckRaisesItsOwnTablesRingPlaceAndNeverLowersIt() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.advanceSinkAcked(CHAIN, "p1", "orders", new ChainPosition(new SourceOrder(1, 9), "t9"));
            store.advanceSinkAcked(CHAIN, "p1", "items", new ChainPosition(new SourceOrder(1, 4), "t4"));
            // Two members confirming at once can land out of order; the place in the ring only moves forward.
            store.advanceSinkAcked(CHAIN, "p1", "orders", new ChainPosition(new SourceOrder(1, 7), "t7"));
            // A snapshot row sits beneath every change and is no place in any ring.
            store.advanceSinkAcked(CHAIN, "p1", "orders", new ChainPosition(SourceOrder.snapshotRow(1), "s"));

            assertThat(store.ringDoneThrough(CHAIN, "p1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 9L, "items", 4L));
            assertThat(onlyConsumer(store).sinkAckedSrcpos())
                    .as("the chain's acked position is written in the same update, as it always was")
                    .isEqualTo("s");
        });
    }

    @Test
    void writerAwareProgressExposesOnlyTheSlowestSinkAgainstTheRealStore() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("sink-0", "sink-1")));

            store.advanceSinkWriterAcked(
                    CHAIN, "p1", "sink-0", "orders", new ChainPosition(new SourceOrder(1, 100), "t100"));

            assertThat(store.ringDoneThrough(CHAIN, "p1")).isEmpty();
            assertThat(onlyConsumer(store).sinkAcked()).isNull();

            store.advanceSinkWriterAcked(
                    CHAIN, "p1", "sink-1", "orders", new ChainPosition(new SourceOrder(1, 50), "t50"));

            assertThat(store.ringDoneThrough(CHAIN, "p1")).containsEntry("orders", 50L);
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("t50");

            store.markSinkWriterSnapshotComplete(CHAIN, "p1", "sink-0", "orders");
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables("p1")).isEmpty();

            store.markSinkWriterSnapshotComplete(CHAIN, "p1", "sink-1", "orders");
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables("p1"))
                    .containsExactly("orders");
        });
    }

    @Test
    void aLaterQuietTableConfirmationDoesNotSkipAnUnconfirmedRootChange() {
        withStore(store -> {
            String chain = "crm@postgres-526";
            String pipeline = "support_case_state";
            String writer = "view";
            store.create(chain, null);
            store.configureSinkWriters(chain, pipeline, Map.of(
                    "support_case", List.of(writer), "emailmessage", List.of(writer)));
            ChainPosition seam = new ChainPosition(SourceOrder.snapshotRow(1), "before-batch");
            for (String table : List.of("support_case", "emailmessage")) {
                store.advanceSinkWriterAcked(chain, pipeline, writer, table, seam);
                store.markSinkWriterSnapshotComplete(chain, pipeline, writer, table);
            }

            // Source order: two root changes, priority Low, priority High, then an email change.
            // The root ring has reached sequence 3, but its sink has confirmed only Low at sequence 2.
            store.advanceConsumerReadSeq(chain, pipeline, "support_case", 3L);
            store.advanceSinkWriterAcked(chain, pipeline, writer, "support_case",
                    new ChainPosition(new SourceOrder(1, 2), "after-low"));

            // This is the quiet table's first change: its independent ring sequence is 0 even though
            // its source position follows the root's still-unconfirmed High update at sequence 3.
            store.advanceConsumerReadSeq(chain, pipeline, "emailmessage", 0L);
            store.advanceSinkWriterAcked(chain, pipeline, writer, "emailmessage",
                    new ChainPosition(new SourceOrder(1, 0), "after-email"));
            assertThat(store.ringDoneThrough(chain, pipeline))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("support_case", 2L, "emailmessage", 0L));

            // With one consumer, the sink's aggregate is the source-read frontier used on restart.
            ChainPosition confirmed = store.read(chain).orElseThrow()
                    .consumerOffset(pipeline).orElseThrow().sinkAcked();
            store.advanceSourceReadOffset(chain, confirmed);
            assertThat(store.read(chain).orElseThrow().sourceReadOffset())
                    .as("a keep-state restart must resume before the unconfirmed priority High update; "
                            + "resuming after the later email change permanently skips it")
                    .isIn("before-batch", "after-low");
        });
    }

    @Test
    void sharedSourceNodesKeepTheirOwnConfirmedTablePositionsAfterTheStoreReopens() {
        withCollection((store, collection) -> {
            String first = SrsConsumerId.of("support_case_state", "case_source").value();
            String second = SrsConsumerId.of("support_case_state", "second_case_source").value();
            String other = SrsConsumerId.of("opportunity_state", "opportunity_source").value();
            Map<String, List<String>> plan = Map.of(
                    "support_case", List.of("view"), "emailmessage", List.of("view"));
            store.create(CHAIN, null);
            long generation = store.openEpoch(CHAIN);
            for (String consumer : List.of(first, second, other)) {
                store.configureSinkWriters(CHAIN, consumer, plan, ConsumerProgressKind.SRS);
            }

            // Mail is confirmed after High was read but before High's downstream effects finish.
            store.advanceConsumerReadSeq(CHAIN, first, "support_case", 3);
            store.advanceConsumerReadSeq(CHAIN, first, "emailmessage", 0);
            store.advanceSinkWriterAcked(CHAIN, first, "view", "support_case",
                    new ChainPosition(new SourceOrder(generation, 2), "after-low"));
            store.advanceSinkWriterAcked(CHAIN, first, "view", "emailmessage",
                    new ChainPosition(new SourceOrder(generation, 0), "after-mail"));
            store.advanceConsumerReadSeq(CHAIN, second, "support_case", 90);
            store.advanceSinkWriterAcked(CHAIN, second, "view", "support_case",
                    new ChainPosition(new SourceOrder(generation, 80), "second-source-confirmed"));
            store.advanceConsumerReadSeq(CHAIN, other, "emailmessage", 20);
            store.advanceSinkWriterAcked(CHAIN, other, "view", "emailmessage",
                    new ChainPosition(new SourceOrder(generation, 10), "other-pipeline-confirmed"));

            try (MongoClient reopenedClient = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
                MongoSrsMetaStore reopened = new MongoSrsMetaStore(reopenedClient,
                        reopenedClient.getDatabase(collection.getNamespace().getDatabaseName())
                                .getCollection(collection.getNamespace().getCollectionName()));
                SrsMeta persisted = reopened.read(CHAIN).orElseThrow();
                ConsumerOffset held = persisted.consumerOffset(first).orElseThrow();
                assertThat(held.progressKind()).isEqualTo(ConsumerProgressKind.SRS);
                assertThat(held.perTableSeq()).containsExactlyInAnyOrderEntriesOf(
                        Map.of("support_case", 3L, "emailmessage", 0L));
                assertThat(held.sinkAckedByTable()).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "support_case", new ChainPosition(new SourceOrder(generation, 2), "after-low"),
                        "emailmessage", new ChainPosition(new SourceOrder(generation, 0), "after-mail")));
                assertThat(held.sinkAcked())
                        .as("CDC-only table counters do not certify a single database recovery position")
                        .isNull();
                assertThat(reopened.ringDoneThrough(CHAIN, first))
                        .containsExactlyInAnyOrderEntriesOf(Map.of("support_case", 2L, "emailmessage", 0L));
                assertThat(persisted.consumerOffset(second).orElseThrow().sinkAckedByTable())
                        .containsExactly(Map.entry("support_case",
                                new ChainPosition(new SourceOrder(generation, 80), "second-source-confirmed")));
                assertThat(persisted.consumerOffset(other).orElseThrow().sinkAckedByTable())
                        .containsExactly(Map.entry("emailmessage",
                                new ChainPosition(new SourceOrder(generation, 10), "other-pipeline-confirmed")));
                assertThat(reopened.consumerOffsets(CHAIN)).containsExactlyInAnyOrderElementsOf(
                        persisted.consumerOffsets());
            }
        });
    }

    @Test
    void independentTableConfirmationsRetainOnlyAJointlyConfirmedSnapshotSeam() {
        withStore(store -> {
            for (ConsumerProgressKind kind : List.of(ConsumerProgressKind.LEGACY, ConsumerProgressKind.SRS)) {
                String chain = "snapshot-" + kind.name();
                String consumer = SrsConsumerId.of("support_case_state", "source-" + kind.name()).value();
                store.create(chain, null);
                store.configureSinkWriters(chain, consumer, Map.of(
                        "support_case", List.of("view"), "emailmessage", List.of("view")), kind);
                ChainPosition seam = new ChainPosition(SourceOrder.snapshotRow(7), "snapshot-seam");
                for (String table : List.of("support_case", "emailmessage")) {
                    store.advanceSinkWriterAcked(chain, consumer, "view", table, seam);
                    store.markSinkWriterSnapshotComplete(chain, consumer, "view", table);
                }
                store.advanceSinkWriterAcked(chain, consumer, "view", "support_case",
                        new ChainPosition(new SourceOrder(7, 2), "after-low"));
                store.advanceSinkWriterAcked(chain, consumer, "view", "emailmessage",
                        new ChainPosition(new SourceOrder(7, 0), "after-mail"));

                ConsumerOffset confirmed = store.read(chain).orElseThrow().consumerOffset(consumer).orElseThrow();
                assertThat(confirmed.sinkAcked()).as("coordinate system %s", kind).isEqualTo(seam);
                assertThat(confirmed.sinkAckedByTable()).containsExactlyInAnyOrderEntriesOf(Map.of(
                        "support_case", new ChainPosition(new SourceOrder(7, 2), "after-low"),
                        "emailmessage", new ChainPosition(new SourceOrder(7, 0), "after-mail")));
            }
        });
    }

    @Test
    void isolatedDirectChannelsAdvanceTheirSourceOrderWithoutMixingRecoveryRecords() {
        withStore(store -> {
            String firstChain = "crm@same-postgres:channel-one";
            String secondChain = "crm@same-postgres:channel-two";
            String first = SrsConsumerId.of("support_case_state", "case_source").value();
            String second = SrsConsumerId.of("support_case_state", "second_case_source").value();
            Map<String, List<String>> plan = Map.of(
                    "support_case", List.of("view"), "emailmessage", List.of("view"));
            store.create(firstChain, null);
            store.create(secondChain, null);
            store.configureSinkWriters(firstChain, first, plan, ConsumerProgressKind.DIRECT_SOURCE);
            store.configureSinkWriters(secondChain, second, plan, ConsumerProgressKind.DIRECT_SOURCE);

            store.advanceSinkWriterAcked(firstChain, first, "view", "support_case",
                    new ChainPosition(new SourceOrder(1, 1), "first-after-low"));
            store.advanceSinkWriterAcked(firstChain, first, "view", "emailmessage",
                    new ChainPosition(new SourceOrder(1, 3), "first-after-mail"));
            store.advanceSinkWriterAcked(secondChain, second, "view", "support_case",
                    new ChainPosition(new SourceOrder(1, 80), "second-root"));
            store.advanceSinkWriterAcked(secondChain, second, "view", "emailmessage",
                    new ChainPosition(new SourceOrder(1, 90), "second-mail"));
            store.advanceSourceReadOffset(firstChain, store.read(firstChain).orElseThrow()
                    .consumerOffset(first).orElseThrow().sinkAcked());
            store.advanceSourceReadOffset(secondChain, store.read(secondChain).orElseThrow()
                    .consumerOffset(second).orElseThrow().sinkAcked());

            assertThat(store.read(firstChain).orElseThrow().sourceReadOffset()).isEqualTo("first-after-low");
            assertThat(store.read(secondChain).orElseThrow().sourceReadOffset()).isEqualTo("second-root");
            store.advanceSinkWriterAcked(firstChain, first, "view", "support_case",
                    new ChainPosition(new SourceOrder(1, 2), "first-after-high"));
            store.advanceSourceReadOffset(firstChain, store.read(firstChain).orElseThrow()
                    .consumerOffset(first).orElseThrow().sinkAcked());

            assertThat(store.read(firstChain).orElseThrow().sourceReadOffset()).isEqualTo("first-after-high");
            assertThat(store.read(secondChain).orElseThrow().sourceReadOffset()).isEqualTo("second-root");
            assertThat(store.consumerOffsets(firstChain)).extracting(ConsumerOffset::pipelineId).containsExactly(first);
            assertThat(store.consumerOffsets(secondChain)).extracting(ConsumerOffset::pipelineId).containsExactly(second);
        });
    }

    @Test
    void directBatchRecoveryRetainsUnconfirmedHighAcrossStoreReopenAndAdvancesPastAQuietTable() {
        withCollection((store, collection) -> {
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();
            store.create(CHAIN, null);
            long epoch = store.openEpoch(CHAIN);
            store.configureSinkWriters(CHAIN, consumer, Map.of(
                    "support_case", List.of("view"), "emailmessage", List.of("view")),
                    ConsumerProgressKind.DIRECT_SOURCE);
            store.beginDirectCapture(CHAIN, consumer, epoch, "before-batch");
            ChainPosition low = new ChainPosition(new SourceOrder(epoch, 0), "after-low");
            ChainPosition high = new ChainPosition(new SourceOrder(epoch, 1), "after-high");
            ChainPosition mail = new ChainPosition(new SourceOrder(epoch, 2), "after-mail");
            store.recordDirectBatch(CHAIN, consumer, low, Map.of("support_case", 0L));
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case", low);
            store.recordDirectBatch(CHAIN, consumer, high, Map.of("support_case", 1L));
            store.recordDirectBatch(CHAIN, consumer, mail,
                    Map.of("support_case", 1L, "emailmessage", 2L));
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "emailmessage", mail);

            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("after-low");
            assertThat(store.read(CHAIN).orElseThrow().consumerOffset(consumer).orElseThrow().sinkAcked())
                    .isEqualTo(low);
            try (MongoClient reopenedClient = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
                MongoSrsMetaStore reopened = new MongoSrsMetaStore(reopenedClient,
                        reopenedClient.getDatabase(collection.getNamespace().getDatabaseName())
                                .getCollection(collection.getNamespace().getCollectionName()));
                // A repeated same-generation startup must keep both pending batch manifests.
                reopened.beginDirectCapture(CHAIN, consumer, epoch, "before-batch");
                assertThat(reopened.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("after-low");
                reopened.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case", high);
                assertThat(reopened.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("after-mail");

                ChainPosition laterRoot = new ChainPosition(new SourceOrder(epoch, 3), "later-root");
                reopened.recordDirectBatch(CHAIN, consumer, laterRoot,
                        Map.of("support_case", 3L, "emailmessage", 2L));
                reopened.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case", laterRoot);
                assertThat(reopened.read(CHAIN).orElseThrow().sourceReadOffset())
                        .as("mail need not change again to confirm a later root-only batch")
                        .isEqualTo("later-root");
                assertThat(reopened.read(CHAIN).orElseThrow().consumerOffset(consumer).orElseThrow().sinkAcked())
                        .isEqualTo(laterRoot);
            }
        });
    }

    @Test
    void directStreamGenerationRefusesOldProducersAndCannotCertifyNewBatchesWithOldWriterProgress() {
        withStore(store -> {
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();
            store.create(CHAIN, null);
            long oldEpoch = store.openEpoch(CHAIN);
            store.configureSinkWriters(CHAIN, consumer, Map.of(
                    "support_case", List.of("view"), "emailmessage", List.of("view")),
                    ConsumerProgressKind.DIRECT_SOURCE);
            store.beginDirectCapture(CHAIN, consumer, oldEpoch, "before-batch");
            ChainPosition low = new ChainPosition(new SourceOrder(oldEpoch, 0), "after-low");
            store.recordDirectBatch(CHAIN, consumer, low, Map.of("support_case", 0L));
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case", low);
            store.recordDirectBatch(CHAIN, consumer,
                    new ChainPosition(new SourceOrder(oldEpoch, 1), "unconfirmed-high"),
                    Map.of("support_case", 1L));

            long epoch = store.openEpoch(CHAIN);
            store.beginDirectCapture(CHAIN, consumer, epoch, "after-low");
            ChainPosition replayedHigh = new ChainPosition(new SourceOrder(epoch, 0), "after-high");
            store.recordDirectBatch(CHAIN, consumer, replayedHigh, Map.of("support_case", 0L));
            ChainPosition mail = new ChainPosition(new SourceOrder(epoch, 1), "after-mail");
            store.recordDirectBatch(CHAIN, consumer, mail,
                    Map.of("support_case", 0L, "emailmessage", 1L));
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "emailmessage", mail);
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case",
                    new ChainPosition(new SourceOrder(oldEpoch, 9), "old-late-confirmation"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("after-low");

            assertThatThrownBy(() -> store.beginDirectCapture(CHAIN, consumer, oldEpoch, "old-anchor"))
                    .isInstanceOf(TapstateException.class)
                    .satisfies(thrown -> assertThat(((TapstateException) thrown).code())
                            .isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThatThrownBy(() -> store.recordDirectBatch(CHAIN, consumer,
                    new ChainPosition(new SourceOrder(oldEpoch, 10), "old-late-batch"),
                    Map.of("support_case", 10L)))
                    .isInstanceOf(TapstateException.class)
                    .satisfies(thrown -> assertThat(((TapstateException) thrown).code())
                            .isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("after-low");
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case", replayedHigh);
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("after-mail");
        });
    }

    @Test
    void directStartRegistersItsInitialBoundaryAtomicallyWithoutResettingTheActiveStream() {
        withStore(store -> {
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();
            store.create(CHAIN, null);
            long epoch = store.openEpoch(CHAIN);
            store.configureSinkWriters(CHAIN, consumer, Map.of("support_case", List.of("view")),
                    ConsumerProgressKind.DIRECT_SOURCE);
            store.beginDirectCapture(CHAIN, consumer, epoch, null);
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isNull();
            store.beginDirectCapture(CHAIN, consumer, epoch, "initial-boundary");
            SrsMeta initial = store.read(CHAIN).orElseThrow();
            assertThat(initial.sourceReadOffset()).isEqualTo("initial-boundary");
            assertThat(initial.sourceRead()).isEqualTo(initial.consumerOffset(consumer).orElseThrow().sinkAcked());
            assertThat(initial.sourceReadDurable()).isFalse();

            ChainPosition pending = new ChainPosition(new SourceOrder(epoch, 0), "after-high");
            store.recordDirectBatch(CHAIN, consumer, pending, Map.of("support_case", 0L));
            store.beginDirectCapture(CHAIN, consumer, epoch, "later-unconfirmed-boundary");
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("initial-boundary");
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case", pending);
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("after-high");
        });
    }

    @Test
    void malformedDirectBatchLedgerRefusesProgressWithoutPublishingARecoveryCheckpoint() {
        withCollection((store, collection) -> {
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();
            List<Object> malformed = List.of("not-a-batch",
                    new Document("epoch", 1L).append("seq", 0L).append("token", "unconfirmed")
                            .append("targets", "not-a-document"),
                    new Document("epoch", 1L).append("token", "unconfirmed")
                            .append("targets", new Document()),
                    new Document("epoch", 1L).append("seq", 0L).append("token", "unconfirmed")
                            .append("targets", new Document("support_case", 1L)));
            for (int index = 0; index < malformed.size(); index++) {
                String chain = "malformed-direct-" + index;
                store.create(chain, null);
                long epoch = store.openEpoch(chain);
                store.configureSinkWriters(chain, consumer, Map.of("support_case", List.of("view")),
                        ConsumerProgressKind.DIRECT_SOURCE);
                store.beginDirectCapture(chain, consumer, epoch, "initial-boundary");
                collection.updateOne(new Document("miningChainId", chain).append("pipelineId", consumer),
                        new Document("$set", new Document("directBatches", List.of(malformed.get(index)))));

                assertThatThrownBy(() -> store.advanceSinkWriterAcked(chain, consumer, "view", "support_case",
                        new ChainPosition(new SourceOrder(epoch, 0), "after-high")))
                        .isInstanceOf(TapstateException.class)
                        .satisfies(thrown -> {
                            TapstateException refused = (TapstateException) thrown;
                            assertThat(refused.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                            assertThat(refused.args()).containsEntry("field", "directBatches");
                        });
                SrsMeta persisted = store.read(chain).orElseThrow();
                assertThat(persisted.sourceReadOffset()).isEqualTo("initial-boundary");
                assertThat(persisted.consumerOffset(consumer).orElseThrow().sinkAckedByTable()).isEmpty();
            }
        });
    }

    @Test
    void aCaptureCheckpointWaitsForTheRequestedTableUnionToBeServed() {
        withStore(store -> {
            store.create(CHAIN, null);
            long epoch = store.openEpoch(CHAIN);
            store.requestCaptureTables(CHAIN, List.of("support_case"));
            assertThat(store.publishCaptureTables(CHAIN, epoch, List.of("support_case"))).isTrue();
            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(epoch, 1), "before-union"),
                    List.of("support_case"));

            store.requestCaptureTables(CHAIN, List.of("emailmessage", "support_case"));
            assertThat(store.captureTables(CHAIN)).containsExactlyInAnyOrder("support_case", "emailmessage");
            assertThat(store.captureServingTables(CHAIN)).containsExactly("support_case");
            assertThat(store.publishCaptureTables(CHAIN, epoch, List.of("support_case"))).isFalse();
            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(epoch, 2), "narrow-read"),
                    List.of("support_case"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("before-union");

            assertThat(store.publishCaptureTables(CHAIN, epoch, List.of("support_case", "emailmessage"))).isTrue();
            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(epoch, 3), "union-durable"),
                    List.of("support_case", "emailmessage"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("union-durable");
            assertThat(store.read(CHAIN).orElseThrow().sourceReadDurable()).isTrue();
        });
    }

    @Test
    void aLateNarrowCallbackCannotCheckpointAfterTheWiderSubscriptionIsPublished() {
        withStore(store -> {
            store.create(CHAIN, null);
            long epoch = store.openEpoch(CHAIN);
            List<String> narrow = List.of("support_case");
            List<String> union = List.of("support_case", "emailmessage");
            store.requestCaptureTables(CHAIN, narrow);
            assertThat(store.publishCaptureTables(CHAIN, epoch, narrow)).isTrue();
            store.advanceCaptureCheckpoint(CHAIN,
                    new ChainPosition(new SourceOrder(epoch, 1), "before-union"), narrow);

            store.requestCaptureTables(CHAIN, union);
            assertThat(store.publishCaptureTables(CHAIN, epoch, union)).isTrue();
            store.advanceCaptureCheckpoint(CHAIN,
                    new ChainPosition(new SourceOrder(epoch, 2), "late-narrow-batch"), narrow);

            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset())
                    .as("publishing the new selection cannot certify an old callback's missing table")
                    .isEqualTo("before-union");
            store.advanceCaptureCheckpoint(CHAIN,
                    new ChainPosition(new SourceOrder(epoch, 3), "whole-union-batch"), union);
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("whole-union-batch");
        });
    }

    @Test
    void malformedRingRecoveryMarkersAreRefusedBeforeTheyCanBecomeFreshArrivals() {
        withCollection((store, collection) -> {
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();
            List<Object> malformed = new ArrayList<>(List.of("not-a-map",
                    new Document("support_case", "not-a-sequence"),
                    new Document("support_case", 2.5), new Document("support_case", -2L)));
            malformed.add(null);
            for (int index = 0; index < malformed.size(); index++) {
                String chain = "malformed-ring-marker-" + index;
                store.create(chain, null);
                long epoch = store.openEpoch(chain);
                store.configureSinkWriters(chain, consumer, Map.of("support_case", List.of("view")),
                        ConsumerProgressKind.SRS);
                store.advanceSinkWriterAcked(chain, consumer, "view", "support_case",
                        new ChainPosition(new SourceOrder(epoch, 2), "after-low"));
                Document key = new Document("miningChainId", chain).append("pipelineId", consumer);
                collection.updateOne(key,
                        new Document("$set", new Document("perTableRingDone", malformed.get(index))));

                assertThatThrownBy(() -> store.startRingAfter(chain, consumer, "support_case", 99L))
                        .isInstanceOf(TapstateException.class)
                        .satisfies(thrown -> {
                            TapstateException refused = (TapstateException) thrown;
                            assertThat(refused.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                            assertThat(refused.args()).containsEntry("field", "perTableRingDone");
                        });
                assertThat(collection.find(key).first().get("perTableRingDone"))
                        .as("a refusal keeps the unverified stored marker intact")
                        .isEqualTo(malformed.get(index));
            }
        });
    }

    @Test
    void aMissingRingMarkerUsesItsOwnConfirmedTablePositionInsteadOfTheCurrentTail() {
        withCollection((store, collection) -> {
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();
            store.create(CHAIN, null);
            long epoch = store.openEpoch(CHAIN);
            store.configureSinkWriters(CHAIN, consumer,
                    Map.of("support_case", List.of("view"), "emailmessage", List.of("view")),
                    ConsumerProgressKind.SRS);
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "support_case",
                    new ChainPosition(new SourceOrder(epoch, 2), "after-low"));
            store.advanceSinkWriterAcked(CHAIN, consumer, "view", "emailmessage",
                    new ChainPosition(new SourceOrder(epoch, 0), "after-mail"));
            Document key = new Document("miningChainId", CHAIN).append("pipelineId", consumer);
            collection.updateOne(key,
                    new Document("$unset", new Document("perTableRingDone.support_case", "")));

            store.startRingAfter(CHAIN, consumer, "support_case", 99L);

            assertThat(store.ringDoneThrough(CHAIN, consumer)).containsExactlyInAnyOrderEntriesOf(
                    Map.of("support_case", 2L, "emailmessage", 0L));
            assertThat(collection.find(key).first().get("perTableRingDone", Document.class))
                    .as("the real confirmation is used without inventing an arrival at the current tail")
                    .containsExactly(Map.entry("emailmessage", 0L));
            assertThat(store.read(CHAIN).orElseThrow().consumerOffset(consumer).orElseThrow()
                    .sinkAckedByTable().get("support_case").token()).isEqualTo("after-low");
        });
    }

    @Test
    void anOldCaptureCannotPublishOrCheckpointAfterANewGenerationOpens() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.requestCaptureTables(CHAIN, List.of("support_case", "emailmessage"));
            long oldEpoch = store.openEpoch(CHAIN);
            assertThat(store.publishCaptureTables(CHAIN, oldEpoch,
                    List.of("support_case", "emailmessage"))).isTrue();
            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(oldEpoch, 1), "old-durable"),
                    List.of("support_case", "emailmessage"));

            long newEpoch = store.openEpoch(CHAIN);
            assertThat(store.captureServingTables(CHAIN)).isEmpty();
            assertThat(store.publishCaptureTables(CHAIN, oldEpoch,
                    List.of("support_case", "emailmessage"))).isFalse();
            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(oldEpoch, 2), "old-late-write"),
                    List.of("support_case", "emailmessage"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("old-durable");

            assertThat(store.publishCaptureTables(CHAIN, newEpoch,
                    List.of("support_case", "emailmessage"))).isTrue();
            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(newEpoch, 0), "new-durable"),
                    List.of("support_case", "emailmessage"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("new-durable");
        });
    }

    @Test
    void onlyRecoverableCaptureWritesCertifyTheSourceCheckpointAsDurable() {
        withStore(store -> {
            store.create(CHAIN, null);
            long epoch = store.openEpoch(CHAIN);
            store.requestCaptureTables(CHAIN, List.of("orders"));
            store.publishCaptureTables(CHAIN, epoch, List.of("orders"));
            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(epoch, 1), "durable-one"),
                    List.of("orders"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadDurable()).isTrue();

            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(epoch, 0), "earlier-read"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadDurable()).isTrue();
            store.advanceSourceReadOffset(CHAIN, new ChainPosition(new SourceOrder(epoch, 2), "direct-read"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadDurable()).isFalse();

            store.advanceCaptureCheckpoint(CHAIN, new ChainPosition(new SourceOrder(epoch, 3), "durable-three"),
                    List.of("orders"));
            assertThat(store.read(CHAIN).orElseThrow().sourceReadDurable()).isTrue();
            store.rewindSourceReadOffset(CHAIN, "operator-selected-token");
            assertThat(store.read(CHAIN).orElseThrow().sourceReadDurable()).isFalse();
            assertThat(store.read(CHAIN).orElseThrow().sourceReadOffset()).isEqualTo("operator-selected-token");
        });
    }

    @Test
    void oldEmbeddedPipelineProgressIsRefusedInsteadOfGuessedToBelongToASourceNode() {
        withCollection((store, collection) -> {
            ConsumerOffset legacy = new ConsumerOffset("support_case_state",
                    Map.of("support_case", 3L, "emailmessage", 0L),
                    new ChainPosition(new SourceOrder(1, 0), "after-mail"),
                    List.of("support_case", "emailmessage"), "snapshot-seam", 1);
            collection.insertOne(MongoSrsMetaStore.toDocument(
                    new SrsMeta(CHAIN, null, List.of(legacy), List.of(), null)));
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();

            assertThatThrownBy(() -> store.configureSinkWriters(CHAIN, consumer, Map.of(
                    "support_case", List.of("view"), "emailmessage", List.of("view")), ConsumerProgressKind.SRS))
                    .isInstanceOf(TapstateException.class)
                    .satisfies(thrown -> {
                        TapstateException refused = (TapstateException) thrown;
                        assertThat(refused.code()).isEqualTo(IoError.SRS_PROGRESS_UNPROVEN);
                        assertThat(refused.args()).containsEntry("pipeline", "support_case_state");
                    });

            assertThat(store.read(CHAIN).orElseThrow().consumerOffsets()).containsExactly(legacy);
            assertThat(store.read(CHAIN).orElseThrow().consumerOffset(consumer)).isEmpty();
        });
    }

    @Test
    void oldWriterAmbiguityIsNotHiddenByAChangeToSourceScopedProgress() {
        withCollection((store, collection) -> {
            ConsumerOffset legacy = new ConsumerOffset("support_case_state", Map.of("support_case", 10L),
                    new ChainPosition(new SourceOrder(1, 10), "only-fast-writer-may-have-confirmed"));
            collection.insertOne(MongoSrsMetaStore.toDocument(
                    new SrsMeta(CHAIN, null, List.of(legacy), List.of(), null)));
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();

            assertThatThrownBy(() -> store.configureSinkWriters(CHAIN, consumer,
                    Map.of("support_case", List.of("fast", "slow")), ConsumerProgressKind.SRS))
                    .isInstanceOf(TapstateException.class)
                    .satisfies(thrown -> assertThat(((TapstateException) thrown).code())
                            .isEqualTo(IoError.SINK_WRITER_PROGRESS_AMBIGUOUS));

            assertThat(store.read(CHAIN).orElseThrow().consumerOffsets()).containsExactly(legacy);
        });
    }

    @Test
    void anOldUnreadRegistrationDoesNotFabricateConfirmedProgressForANewSourceNode() {
        withCollection((store, collection) -> {
            collection.insertOne(MongoSrsMetaStore.toDocument(new SrsMeta(CHAIN, null,
                    List.of(new ConsumerOffset("support_case_state", Map.of("support_case", -1L), null)),
                    List.of(), null)));
            String consumer = SrsConsumerId.of("support_case_state", "case_source").value();

            store.configureSinkWriters(CHAIN, consumer,
                    Map.of("support_case", List.of("view")), ConsumerProgressKind.SRS);

            ConsumerOffset registered = store.read(CHAIN).orElseThrow().consumerOffset(consumer).orElseThrow();
            assertThat(registered.progressKind()).isEqualTo(ConsumerProgressKind.SRS);
            assertThat(registered.perTableSeq()).isEmpty();
            assertThat(registered.sinkAcked()).isNull();
            assertThat(registered.sinkAckedByTable()).isEmpty();
            assertThat(store.ringDoneThrough(CHAIN, consumer)).isEmpty();
        });
    }

    @Test
    void scopedDetachRemovesOneNodeAndPipelineDetachRemovesAllOfItsRemainingNodes() {
        withCollection((store, collection) -> {
            String first = SrsConsumerId.of("support_case_state", "case_source").value();
            String second = SrsConsumerId.of("support_case_state", "case_copy").value();
            String other = SrsConsumerId.of("support_case_state_extra", "case_source").value();
            String otherChain = "other@postgres";
            // Embedded scoped records also have to acquire their owner identity during migration.
            collection.insertOne(MongoSrsMetaStore.toDocument(new SrsMeta(CHAIN, null,
                    List.of(new ConsumerOffset(first, Map.of("support_case", 2L), null),
                            new ConsumerOffset(second, Map.of("support_case", 3L), null),
                            new ConsumerOffset(other, Map.of("support_case", 4L), null)), List.of(), null)));
            store.create(otherChain, null);
            store.advanceConsumerReadSeq(otherChain, first, "support_case", 5);
            store.advanceConsumerReadSeq(otherChain, other, "support_case", 6);

            assertThat(store.miningChainIdsWithConsumer("support_case_state"))
                    .containsExactlyInAnyOrder(CHAIN, otherChain);
            store.detachConsumer(CHAIN, first);
            assertThat(store.consumerOffsets(CHAIN)).extracting(ConsumerOffset::pipelineId)
                    .containsExactlyInAnyOrder(second, other);
            store.detachConsumer(CHAIN, "support_case_state");
            assertThat(store.consumerOffsets(CHAIN)).extracting(ConsumerOffset::pipelineId).containsExactly(other);
            assertThat(store.miningChainIdsWithConsumer("support_case_state")).containsExactly(otherChain);
            store.detachConsumer(otherChain, "support_case_state");
            assertThat(store.miningChainIdsWithConsumer("support_case_state")).isEmpty();
            assertThat(store.consumerOffsets(otherChain)).extracting(ConsumerOffset::pipelineId).containsExactly(other);
        });
    }

    @Test
    void legacyAggregateProgressCannotStandForTwoDivergentSinkWriters() {
        withStore(store -> {
            store.create(CHAIN, null);
            // This is the complete shape released before writer-aware progress: the fast target reached
            // 100, the slow target reached 50, and only the aggregate 100 could be recorded.
            store.advanceSinkAcked(
                    CHAIN, "p1", "orders", new ChainPosition(new SourceOrder(1, 100), "t100"));

            assertThatThrownBy(() -> store.configureSinkWriters(
                    CHAIN, "p1", Map.of("orders", List.of("fast", "slow"))))
                    .isInstanceOf(TapstateException.class)
                    .satisfies(thrown -> {
                        TapstateException refusal = (TapstateException) thrown;
                        assertThat(refusal.code()).isEqualTo(IoError.SINK_WRITER_PROGRESS_AMBIGUOUS);
                        assertThat(refusal.args()).containsEntry("pipeline", "p1");
                    });

            // Refusal is atomic: it leaves the aggregate intact but does not promote it into writer
            // evidence. Clearing this consumer is the control-store half of the required full resync.
            assertThat(store.ringDoneThrough(CHAIN, "p1")).containsEntry("orders", 100L);
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("t100");
            store.detachConsumer(CHAIN, "p1");

            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("fast", "slow")));
            store.advanceSinkWriterAcked(
                    CHAIN, "p1", "fast", "orders", new ChainPosition(new SourceOrder(2, 100), "t2-100"));
            store.advanceSinkWriterAcked(
                    CHAIN, "p1", "slow", "orders", new ChainPosition(new SourceOrder(2, 50), "t2-50"));

            assertThat(store.ringDoneThrough(CHAIN, "p1")).containsEntry("orders", 50L);
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("t2-50");
        });
    }

    @Test
    void initialRingCursorSeedsEveryWriterWithoutClaimingSinkProgress() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.startRingAfter(CHAIN, "p1", "orders", 40);

            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("fast", "slow")));
            store.advanceSinkWriterAcked(
                    CHAIN, "p1", "fast", "orders", new ChainPosition(new SourceOrder(1, 41), "t41"));

            assertThat(store.ringDoneThrough(CHAIN, "p1"))
                    .as("the initial cursor belongs to both writers until the slower writer advances")
                    .containsEntry("orders", 40L);
            assertThat(onlyConsumer(store).sinkAcked()).isNull();
        });
    }

    @Test
    void aNewSinkWriterCannotInheritAnEstablishedWritersConfirmedEffects() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("old")));
            store.advanceSinkWriterAcked(CHAIN, "p1", "old", "orders",
                    new ChainPosition(new SourceOrder(1, 4), "old-writer-confirmed"));

            assertThatThrownBy(() -> store.configureSinkWriters(CHAIN, "p1",
                    Map.of("orders", List.of("old", "new"))))
                    .isInstanceOf(TapstateException.class)
                    .satisfies(thrown -> assertThat(((TapstateException) thrown).code())
                            .isEqualTo(IoError.SINK_WRITER_PROGRESS_AMBIGUOUS));
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("old-writer-confirmed");
            assertThatThrownBy(() -> store.advanceSinkWriterAcked(CHAIN, "p1", "new", "orders",
                    new ChainPosition(new SourceOrder(1, 5), "new-writer")))
                    .isInstanceOf(IllegalStateException.class);
            store.advanceSinkWriterAcked(CHAIN, "p1", "old", "orders",
                    new ChainPosition(new SourceOrder(1, 5), "old-writer-next"));
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("old-writer-next");
        });
    }

    @Test
    void wideningATableSelectionKeepsConfirmedTablesAndLeavesTheAddedTableUnconfirmed() {
        withCollection((store, collection) -> {
            String consumer = SrsConsumerId.of("pipeline", "source").value();
            store.create(CHAIN, null);
            store.configureSinkWriters(CHAIN, consumer,
                    Map.of("alpha", List.of("sink"), "beta", List.of("sink")), ConsumerProgressKind.SRS);
            ChainPosition snapshot = new ChainPosition(new SourceOrder(1, SourceOrder.SNAPSHOT_SEQ), "seam");
            for (String table : List.of("alpha", "beta")) {
                store.advanceSinkWriterAcked(CHAIN, consumer, "sink", table, snapshot);
                store.markSinkWriterSnapshotComplete(CHAIN, consumer, "sink", table);
            }
            store.startRingAfter(CHAIN, consumer, "gamma", 40);

            store.configureSinkWriters(CHAIN, consumer,
                    Map.of("alpha", List.of("sink"), "beta", List.of("sink"), "gamma", List.of("sink")),
                    ConsumerProgressKind.SRS);

            ConsumerOffset offset = store.read(CHAIN).orElseThrow().consumerOffset(consumer).orElseThrow();
            assertThat(offset.snapshotCompletedTables()).containsExactlyInAnyOrder("alpha", "beta");
            assertThat(offset.sinkAckedByTable()).containsExactlyInAnyOrderEntriesOf(
                    Map.of("alpha", snapshot, "beta", snapshot));
            assertThat(store.ringDoneThrough(CHAIN, consumer)).containsEntry("gamma", 40L);
            Document gamma = collection.find(new Document("miningChainId", CHAIN).append("pipelineId", consumer))
                    .first().get("sinkWriterProgress", Document.class)
                    .get("sink", Document.class).get("gamma", Document.class);
            assertThat(gamma).containsEntry("ringDone", 40L)
                    .doesNotContainKeys("sinkAckedEpoch", "sinkAckedSeq", "snapshotComplete");
            store.markSinkWriterSnapshotComplete(CHAIN, consumer, "sink", "gamma");
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables(consumer))
                    .containsExactlyInAnyOrder("alpha", "beta", "gamma");
        });
    }

    @Test
    void aWriterPlanDoesNotTreatLegacyReadingOrItsSeamAsConfirmedSourceNodeProgress() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceConsumerReadSeq(CHAIN, "pipeline", "orders", 7);
            store.setCdcStart(CHAIN, "pipeline", "old-seam", 1);
            ConsumerOffset legacy = store.read(CHAIN).orElseThrow().consumerOffset("pipeline").orElseThrow();
            String consumer = SrsConsumerId.of("pipeline", "source").value();

            store.configureSinkWriters(CHAIN, consumer, Map.of("orders", List.of("sink")), ConsumerProgressKind.SRS);

            assertThat(store.read(CHAIN).orElseThrow().consumerOffset("pipeline")).contains(legacy);
            ConsumerOffset configured = store.read(CHAIN).orElseThrow().consumerOffset(consumer).orElseThrow();
            assertThat(configured.perTableSeq()).isEmpty();
            assertThat(configured.sinkAcked()).isNull();
            assertThat(configured.sinkAckedByTable()).isEmpty();
            assertThat(configured.cdcStartPosition()).isNull();
            assertThat(configured.snapshotCompletedTables()).isEmpty();
            assertThat(store.ringDoneThrough(CHAIN, consumer)).isEmpty();
        });
    }

    @Test
    void partialWriterConfirmationCannotSeedANewWriterBeforeTheAggregateExists() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("fast", "slow")));
            store.advanceSinkWriterAcked(CHAIN, "p1", "fast", "orders",
                    new ChainPosition(new SourceOrder(1, 4), "fast-only-confirmed"));
            assertThat(onlyConsumer(store).sinkAcked()).isNull();

            assertThatThrownBy(() -> store.configureSinkWriters(CHAIN, "p1",
                    Map.of("orders", List.of("fast", "slow", "new"))))
                    .isInstanceOf(TapstateException.class)
                    .satisfies(thrown -> assertThat(((TapstateException) thrown).code())
                            .isEqualTo(IoError.SINK_WRITER_PROGRESS_AMBIGUOUS));
            store.advanceSinkWriterAcked(CHAIN, "p1", "slow", "orders",
                    new ChainPosition(new SourceOrder(1, 3), "slow-confirmed"));
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("slow-confirmed");
        });
    }

    @Test
    void anUnprocessedInitialRingBaselineCanBeSharedWithANewWriter() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.startRingAfter(CHAIN, "p1", "orders", 40);
            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("old")));
            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("old", "new")));
            store.advanceSinkWriterAcked(CHAIN, "p1", "old", "orders",
                    new ChainPosition(new SourceOrder(1, 41), "old-confirmed"));

            assertThat(onlyConsumer(store).sinkAcked()).isNull();
            assertThat(store.ringDoneThrough(CHAIN, "p1")).containsEntry("orders", 40L);
            store.advanceSinkWriterAcked(CHAIN, "p1", "new", "orders",
                    new ChainPosition(new SourceOrder(1, 41), "both-confirmed"));
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("old-confirmed");
        });
    }

    @Test
    void legacyAggregateProgressCanSeedItsOnlySinkWriter() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceSinkAcked(
                    CHAIN, "p1", "orders", new ChainPosition(new SourceOrder(1, 100), "t100"));

            store.configureSinkWriters(CHAIN, "p1", Map.of("orders", List.of("only")));
            store.advanceSinkWriterAcked(
                    CHAIN, "p1", "only", "orders", new ChainPosition(new SourceOrder(1, 101), "t101"));

            assertThat(store.ringDoneThrough(CHAIN, "p1")).containsEntry("orders", 101L);
            assertThat(onlyConsumer(store).sinkAckedSrcpos()).isEqualTo("t101");
        });
    }

    @Test
    void anArrivalIsMarkedOnceAndNeverMovesAPlaceThePipelineAlreadyHas() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceSinkAcked(CHAIN, "p1", "orders", new ChainPosition(new SourceOrder(1, 3), "t3"));

            // A run coming back arrives on a ring that has moved on; its place is where its target got to.
            store.startRingAfter(CHAIN, "p1", "orders", 20);
            // A table it never had a place in is marked where the ring stands.
            store.startRingAfter(CHAIN, "p1", "items", 20);
            store.startRingAfter(CHAIN, "p1", "items", 30);

            assertThat(store.ringDoneThrough(CHAIN, "p1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("orders", 3L, "items", 20L));
            assertThat(onlyConsumer(store).perTableSeq())
                    .as("the first arrival registers the table in the same consumer update")
                    .containsEntry("items", 20L);
        });
    }

    @Test
    void aRewrittenConsumerRecordCarriesNoRingPlaces() {
        withStore(store -> {
            store.create(CHAIN, null);
            store.advanceSinkAcked(CHAIN, "p1", "orders", new ChainPosition(new SourceOrder(1, 5), "t5"));

            // What a write-back that lets the acks go does: the record is rewritten without them, and the
            // next run starts where its read mode puts it rather than past a place nothing now stands behind.
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("p1", Map.of(), null));

            assertThat(store.ringDoneThrough(CHAIN, "p1")).isEmpty();
        });
    }

    @Test
    void setCdcStartPersistsEachPipelinesSeamPositionAndGenerationIndependently() {
        withStore(store -> {
            store.create(CHAIN, null);

            store.setCdcStart(CHAIN, "p1", "binlog.000042:1024", 3L);
            store.setCdcStart(CHAIN, "p2", "binlog.000099:2048", 7L);

            SrsMeta record = store.read(CHAIN).orElseThrow();
            ConsumerOffset p1 = record.consumerOffset("p1").orElseThrow();
            assertThat(p1.cdcStartPosition()).isEqualTo("binlog.000042:1024");
            assertThat(p1.snapshotEpoch()).isEqualTo(3L);
            ConsumerOffset p2 = record.consumerOffset("p2").orElseThrow();
            assertThat(p2.cdcStartPosition()).isEqualTo("binlog.000099:2048");
            assertThat(p2.snapshotEpoch()).isEqualTo(7L);
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
            store.setCdcStart(CHAIN, "p1", "binlog.000042:1024", running);

            store.openEpoch(CHAIN);

            // The restart that opens the next generation is exactly when a snapshot that had not drained
            // must keep the one it began in. Advancing both would hand a rerun's rows the newer generation
            // and let them overwrite changes the older one had already applied.
            SrsMeta record = store.read(CHAIN).orElseThrow();
            assertThat(record.epoch()).isEqualTo(2L);
            assertThat(record.consumerOffset("p1").orElseThrow().snapshotEpoch()).isEqualTo(1L);
        });
    }

    @Test
    void aRecordWrittenBeforeGenerationsExistedReadsBackWithNoneOpened() {
        withStore(store -> {
            store.create(CHAIN, null);

            // A freshly seeded document carries no chain generation and no pipeline snapshot state.
            SrsMeta record = store.read(CHAIN).orElseThrow();
            assertThat(record.epoch()).isZero();
            assertThat(record.consumerOffsets()).isEmpty();
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
                    List.of(new SchemaVersion(1, wideSchema(), 1)), null, 0L, null)));
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
     * <p>The history budget keeps ordinary chain records below the ceiling, but a record can still be
     * seeded with another field that leaves less room than one schema entry needs. What that refusal must
     * not do is send whoever reads it to check a store that is healthy: nothing is wrong with the store and
     * no retry can help. The endpoint reports this as an ordinary command failure, so it is only told apart
     * by being named.
     */
    @Test
    void aWriteRefusedForTheSizeOfItsRecordIsReportedAsTheSizeItIs() {
        withCollection((store, collection) -> {
            store.create(CHAIN, retentionFillingTheRecord());
            assertThat(bsonSize(collection.find(new Document("_id", CHAIN)).first()))
                    .as("the premise: the seeded record is under the ceiling, so what follows is refused "
                            + "for what it would add rather than for what is already there")
                    .isLessThan(DOCUMENT_CEILING);

            assertThatThrownBy(() -> store.appendSchemaVersion(
                    CHAIN, new SchemaVersion(1L, wideSchema(), 1L)))
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
            assertThatThrownBy(() -> store.setCdcStart("nope", "p", "x", 1L))
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
            store.appendSchemaVersion(CHAIN, new SchemaVersion(0, Map.of("id", "int"), 0));
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("departing", Map.of("orders", 100L),
                    new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100")));
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("staying", Map.of("orders", 900L),
                    new ChainPosition(new SourceOrder(1, 900), "gtid:aaa-1:900")));
            store.setCdcStart(CHAIN, "staying", "gtid:aaa-1:1", 1L);

            store.detachConsumer(CHAIN, "departing");

            SrsMeta after = store.read(CHAIN).orElseThrow();
            // The chain record outlives its consumers: it is keyed by the chain, so removing it would be
            // cross-pipeline data loss, and everything on it that is not the departing cursor is untouched.
            assertThat(after.consumerOffsets())
                    .containsExactly(new ConsumerOffset(
                            "staying",
                            Map.of("orders", 900L),
                            new ChainPosition(new SourceOrder(1, 900), "gtid:aaa-1:900"),
                            List.of(),
                            "gtid:aaa-1:1",
                            1L));
            assertThat(after.sourceReadOffset()).isEqualTo("gtid:aaa-1:500");
            assertThat(after.consumerOffset("staying").orElseThrow().cdcStartPosition())
                    .isEqualTo("gtid:aaa-1:1");
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
    void aStaleLegacyMigrationCannotRestoreAConsumerAfterDetachCompletes() throws Exception {
        CountDownLatch staleSnapshotRead = new CountDownLatch(1);
        CountDownLatch resumeStaleMigration = new CountDownLatch(1);
        AtomicBoolean paused = new AtomicBoolean();
        CommandListener pauseAfterFirstFind = new CommandListener() {
            @Override
            public void commandSucceeded(CommandSucceededEvent event) {
                if (!"find".equals(event.getCommandName()) || !paused.compareAndSet(false, true)) {
                    return;
                }
                staleSnapshotRead.countDown();
                try {
                    if (!resumeStaleMigration.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("stale migration was not resumed");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while holding the stale migration", e);
                }
            }
        };
        MongoClientSettings staleSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(pauseAfterFirstFind)
                .build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MongoClient staleClient = MongoClients.create(staleSettings);
                MongoClient currentClient = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> roots = currentClient.getDatabase("tapstate").getCollection("srs_meta");
            MongoCollection<Document> consumers =
                    currentClient.getDatabase("tapstate").getCollection("srs_consumer_offsets");
            roots.drop();
            consumers.drop();
            ConsumerOffset departing = new ConsumerOffset("departing", Map.of("orders", 10L), null);
            ConsumerOffset staying = new ConsumerOffset("staying", Map.of("orders", 20L), null);
            roots.insertOne(MongoSrsMetaStore.toDocument(
                    new SrsMeta(CHAIN, null, List.of(departing, staying), List.of(), null)));

            MongoSrsMetaStore staleStore = new MongoSrsMetaStore(staleClient,
                    staleClient.getDatabase("tapstate").getCollection("srs_meta"),
                    staleClient.getDatabase("tapstate").getCollection("srs_consumer_offsets"));
            MongoSrsMetaStore currentStore = new MongoSrsMetaStore(currentClient, roots, consumers);
            Future<?> staleWrite = executor.submit(
                    () -> staleStore.advanceConsumerReadSeq(CHAIN, "staying", "orders", 21L));

            assertThat(staleSnapshotRead.await(10, TimeUnit.SECONDS))
                    .as("the first writer is paused after reading the legacy cursors")
                    .isTrue();
            currentStore.detachConsumer(CHAIN, "departing");
            assertThat(currentStore.read(CHAIN).orElseThrow().consumerOffsets())
                    .extracting(ConsumerOffset::pipelineId)
                    .containsExactly("staying");
            assertThat(consumers.find(new Document("pipelineId", "departing")).first())
                    .as("detach leaves neither a cursor nor a cleanup marker for the departed pipeline")
                    .isNull();

            resumeStaleMigration.countDown();
            staleWrite.get(30, TimeUnit.SECONDS);

            assertThat(currentStore.read(CHAIN).orElseThrow().consumerOffsets())
                    .extracting(ConsumerOffset::pipelineId)
                    .containsExactly("staying");
        } finally {
            resumeStaleMigration.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void aConsumerWriteAfterDetachAttachesItAgain() {
        withCollection((store, collection) -> {
            store.create(CHAIN, null);
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("returning", Map.of("orders", 10L), null));
            store.detachConsumer(CHAIN, "returning");

            store.advanceConsumerReadSeq(CHAIN, "returning", "orders", 11L);

            assertThat(store.read(CHAIN).orElseThrow().consumerOffsets())
                    .containsExactly(new ConsumerOffset("returning", Map.of("orders", 11L), null));
            assertThat(collection.find(new Document("pipelineId", "returning")).first())
                    .doesNotContainKey("detached");
        });
    }

    @Test
    void aConsumerAdvanceAfterItsRootCheckCannotSurviveAConcurrentDrop() throws Exception {
        CountDownLatch rootChecked = new CountDownLatch(1);
        CountDownLatch resumeConsumerWrite = new CountDownLatch(1);
        AtomicBoolean paused = new AtomicBoolean();
        CommandListener pauseAfterRootCheck = new CommandListener() {
            @Override
            public void commandSucceeded(CommandSucceededEvent event) {
                if (!"find".equals(event.getCommandName()) || !paused.compareAndSet(false, true)) {
                    return;
                }
                rootChecked.countDown();
                try {
                    if (!resumeConsumerWrite.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("consumer write was not resumed");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while holding the consumer write", e);
                }
            }
        };
        MongoClientSettings writingSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(pauseAfterRootCheck)
                .build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (MongoClient writingClient = MongoClients.create(writingSettings);
                MongoClient currentClient = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> roots = currentClient.getDatabase("tapstate").getCollection("srs_meta");
            MongoCollection<Document> consumers =
                    currentClient.getDatabase("tapstate").getCollection("srs_consumer_offsets");
            roots.drop();
            consumers.drop();
            MongoSrsMetaStore writingStore = new MongoSrsMetaStore(writingClient,
                    writingClient.getDatabase("tapstate").getCollection("srs_meta"),
                    writingClient.getDatabase("tapstate").getCollection("srs_consumer_offsets"));
            MongoSrsMetaStore currentStore = new MongoSrsMetaStore(currentClient, roots, consumers);
            currentStore.create(CHAIN, null);

            Future<?> staleAdvance = executor.submit(
                    () -> writingStore.advanceConsumerReadSeq(CHAIN, "stale", "orders", 9L));
            assertThat(rootChecked.await(10, TimeUnit.SECONDS))
                    .as("the consumer writer is paused after confirming that the root exists")
                    .isTrue();

            currentStore.dropChain(CHAIN);
            assertThat(roots.find(new Document("_id", CHAIN)).first()).isNull();
            assertThat(consumers.countDocuments(new Document("miningChainId", CHAIN))).isZero();
            resumeConsumerWrite.countDown();

            assertThatThrownBy(() -> staleAdvance.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("srs meta mutate on an unseeded mining chain: " + CHAIN
                            + " (create must seed it first)");
            assertThat(consumers.countDocuments(new Document("miningChainId", CHAIN)))
                    .as("the refused writer leaves no orphan cursor")
                    .isZero();

            currentStore.create(CHAIN, null);
            assertThat(currentStore.read(CHAIN).orElseThrow().consumerOffsets())
                    .as("a later chain incarnation cannot adopt state from the refused writer")
                    .isEmpty();
        } finally {
            resumeConsumerWrite.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void droppingAChainRemovesItsSplitConsumersBeforeThatIdCanBeSeededAgain() {
        withCollection((store, collection) -> {
            store.create(CHAIN, null);
            store.upsertConsumerOffset(CHAIN, new ConsumerOffset("p1", Map.of("orders", 7L), null));

            store.dropChain(CHAIN);

            assertThat(collection.countDocuments()).isZero();
            store.create(CHAIN, null);
            assertThat(store.read(CHAIN).orElseThrow().consumerOffsets()).isEmpty();
        });
    }

    @Test
    void droppingAnOldChainCannotDeleteACursorFromTheRecreatedChain() throws Exception {
        CountDownLatch cursorCleanupPaused = new CountDownLatch(1);
        CountDownLatch resumeCursorCleanup = new CountDownLatch(1);
        CountDownLatch recreateStarted = new CountDownLatch(1);
        CountDownLatch dropCommitted = new CountDownLatch(1);
        AtomicBoolean paused = new AtomicBoolean();
        AtomicBoolean recreating = new AtomicBoolean();
        CommandListener pauseBeforeCursorCleanup = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (!"delete".equals(event.getCommandName())
                        || !"srs_consumer_offsets".equals(event.getCommand().getString("delete").getValue())
                        || !paused.compareAndSet(false, true)) {
                    return;
                }
                cursorCleanupPaused.countDown();
                try {
                    if (!resumeCursorCleanup.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("cursor cleanup was not resumed");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while holding cursor cleanup", e);
                }
            }
        };
        CommandListener observeRecreate = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if (recreating.get() && "insert".equals(event.getCommandName())
                        && "srs_meta".equals(event.getCommand().getString("insert").getValue())) {
                    recreateStarted.countDown();
                }
            }
        };
        MongoClientSettings droppingSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(pauseBeforeCursorCleanup)
                .build();
        MongoClientSettings currentSettings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(observeRecreate)
                .build();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (MongoClient droppingClient = MongoClients.create(droppingSettings);
                MongoClient currentClient = MongoClients.create(currentSettings)) {
            MongoCollection<Document> roots = currentClient.getDatabase("tapstate").getCollection("srs_meta");
            MongoCollection<Document> consumers =
                    currentClient.getDatabase("tapstate").getCollection("srs_consumer_offsets");
            roots.drop();
            consumers.drop();
            MongoSrsMetaStore droppingStore = new MongoSrsMetaStore(droppingClient,
                    droppingClient.getDatabase("tapstate").getCollection("srs_meta"),
                    droppingClient.getDatabase("tapstate").getCollection("srs_consumer_offsets"));
            MongoSrsMetaStore currentStore = new MongoSrsMetaStore(currentClient, roots, consumers);
            currentStore.create(CHAIN, "old");
            currentStore.upsertConsumerOffset(
                    CHAIN, new ConsumerOffset("old", Map.of("orders", 7L), null));

            Future<?> dropping = executor.submit(() -> droppingStore.dropChain(CHAIN));
            assertThat(cursorCleanupPaused.await(10, TimeUnit.SECONDS))
                    .as("the old chain's cursor cleanup is paused after its root delete ran")
                    .isTrue();

            ConsumerOffset replacement = new ConsumerOffset("new", Map.of("orders", 11L), null);
            Future<?> provision = executor.submit(() -> {
                recreating.set(true);
                try {
                    currentStore.create(CHAIN, "new");
                } catch (IllegalStateException stillPresent) {
                    try {
                        if (!dropCommitted.await(30, TimeUnit.SECONDS)) {
                            throw new AssertionError("chain drop did not commit");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("interrupted while waiting for the chain drop", e);
                    }
                    currentStore.create(CHAIN, "new");
                }
                currentStore.upsertConsumerOffset(CHAIN, replacement);
            });
            assertThat(recreateStarted.await(10, TimeUnit.SECONDS))
                    .as("the replacement provision attempts its insert while cursor cleanup is paused")
                    .isTrue();
            Document visibleRoot = roots.find(new Document("_id", CHAIN)).first();
            assertThat(visibleRoot)
                    .as("the old root remains visible until its cursor cleanup commits")
                    .isNotNull();
            assertThat(visibleRoot.getString("retention")).isEqualTo("old");

            resumeCursorCleanup.countDown();
            dropping.get(30, TimeUnit.SECONDS);
            dropCommitted.countDown();
            provision.get(30, TimeUnit.SECONDS);

            SrsMeta recreated = currentStore.read(CHAIN).orElseThrow();
            assertThat(recreated.retention()).isEqualTo("new");
            assertThat(recreated.consumerOffsets()).containsExactly(replacement);
        } finally {
            resumeCursorCleanup.countDown();
            dropCommitted.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
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
                        new SrsMeta(chain, null, List.of(), history, null, 0L, null))
                .getList("schemaHistory", Document.class);
    }

    /**
     * A retention string long enough to leave the seeded record just under the ceiling — measured off the
     * record the mapping writes without one, so it stays just under if the mapping ever gains a field.
     */
    private static String retentionFillingTheRecord() {
        long empty = bsonSize(MongoSrsMetaStore.toDocument(
                new SrsMeta(CHAIN, null, List.of(), List.of(), "", 0L, null)));
        return "r".repeat((int) (DOCUMENT_CEILING - empty - UNDER_THE_CEILING));
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

    /**
     * The position a source may be told to release up to is read with a majority read concern, and nothing but
     * that position: a write only the old primary had, rolled back in a failover, must never reach a source
     * that would then have let go of what the rolled-back record asks for again. Whether it is a write-through
     * checkpoint is read with it.
     */
    @Test
    void theDurablePositionIsReadWithAMajorityReadConcernAndSaysWhetherItWasWrittenThrough() {
        List<BsonDocument> finds = new CopyOnWriteArrayList<>();
        CommandListener recordFinds = new CommandListener() {
            @Override
            public void commandStarted(CommandStartedEvent event) {
                if ("find".equals(event.getCommandName())) {
                    finds.add(event.getCommand().clone());
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(recordFinds)
                .build();
        try (MongoClient client = MongoClients.create(settings)) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("srs_meta");
            collection.drop();
            MongoSrsMetaStore store = new MongoSrsMetaStore(client, collection, Clock.systemUTC());
            store.create(CHAIN, null);
            long epoch = store.openEpoch(CHAIN);
            assertThat(store.durableSourceRead(CHAIN)).as("no offset yet").isEmpty();

            ChainPosition bounded = new ChainPosition(new SourceOrder(epoch, 3), "t3");
            store.advanceSourceReadOffset(CHAIN, bounded);
            finds.clear();
            assertThat(store.durableSourceRead(CHAIN)).contains(new DurableSourceRead(bounded, false));
            assertThat(finds).hasSize(1);
            BsonDocument find = finds.getFirst();
            assertThat(find.getDocument("readConcern").getString("level").getValue()).isEqualTo("majority");
            assertThat(find.getDocument("projection").keySet()).containsExactlyInAnyOrder(
                    "sourceReadOffset", "sourceReadEpoch", "sourceReadSeq", "sourceReadDurable");

            store.requestCaptureTables(CHAIN, List.of("orders"));
            assertThat(store.publishCaptureTables(CHAIN, epoch, List.of("orders"))).isTrue();
            ChainPosition checkpoint = new ChainPosition(new SourceOrder(epoch, 4), "t4");
            store.advanceCaptureCheckpoint(CHAIN, checkpoint, List.of("orders"));
            assertThat(store.durableSourceRead(CHAIN)).contains(new DurableSourceRead(checkpoint, true));
        }
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
            test.run(new MongoSrsMetaStore(client, collection, clock), collection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
