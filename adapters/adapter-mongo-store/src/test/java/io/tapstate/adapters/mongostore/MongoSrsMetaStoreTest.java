package io.tapstate.adapters.mongostore;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The meta-document codec is the mapping core of the SRS meta store: a mining chain's record is stored
 * as a document keyed by the chain id — consumer cursors as a sub-document keyed by pipeline id, schema
 * history as an array — and reconstructed from it on read. These witness the mapping deterministically,
 * without a Mongo server. The real atomic operations — seed, advance, per-consumer upsert, append,
 * and the unseeded-chain ordering error — are exercised by {@code MongoSrsMetaStoreIT} (skipped where
 * Docker is absent).
 */
class MongoSrsMetaStoreTest {

    @Test
    void metaRoundTripsThroughTheDocumentMapping() {
        SrsMeta meta = new SrsMeta(
                "orders@mysql-1",
                "gtid:aaa-1:900",
                List.of(
                        new ConsumerOffset("p1", Map.of("orders", 42L), new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100")),
                        new ConsumerOffset("p2", new LinkedHashMap<>(Map.of("orders", 7L, "items", 3L)), null)),
                "binlog.000042:1024",
                List.of(
                        new SchemaVersion(0, Map.of("id", "int"), 0),
                        new SchemaVersion(1, new LinkedHashMap<>(Map.of("id", "int", "name", "string")), 12)),
                "7d",
                List.of("orders"));

        Document document = MongoSrsMetaStore.toDocument(meta);

        assertThat(document.getString("_id")).isEqualTo("orders@mysql-1");
        assertThat(document.getString("sourceReadOffset")).isEqualTo("gtid:aaa-1:900");
        assertThat(document.getString("cdcStartPosition")).isEqualTo("binlog.000042:1024");
        assertThat(document.getString("retention")).isEqualTo("7d");
        // consumer cursors keyed by pipeline id, so a per-consumer set targets one path
        assertThat(document.get("consumerOffsets", Document.class)).containsOnlyKeys("p1", "p2");
        // snapshot completion is per table: this chain has drained orders but not items
        assertThat(document.getList("snapshotCompletedTables", String.class)).containsExactly("orders");
        assertThat(MongoSrsMetaStore.toMeta(document)).isEqualTo(meta);
    }

    @Test
    void snapshotCompleteAddsToSetSoARepeatedMarkDoesNotDuplicateTheTable() {
        Document update = MongoSrsMetaStore.snapshotCompleteUpdate("orders");

        // $addToSet, not $push: a re-run of a table's snapshot (a restart mid-backfill, a replay) marks the
        // same table again, and the mark is a set membership question -- "has this table drained?" -- so a
        // second mark must be a no-op rather than a second entry.
        assertThat(update).isEqualTo(new Document("$addToSet", new Document("snapshotCompletedTables", "orders")));
    }

    @Test
    void toMetaOnADocumentWrittenBeforeSnapshotCompletionExistedReadsBackAsNoTableCompleted() {
        // The meta field set is append-only and a newer reader must stay compatible with a document written
        // before the field existed. Absence is not corruption here: it reads as "no table has been marked",
        // which is exactly what a chain snapshotted by an older build had recorded.
        Document old = new Document("_id", "chain")
                .append("consumerOffsets", new Document())
                .append("schemaHistory", List.of());

        SrsMeta meta = MongoSrsMetaStore.toMeta(old);

        assertThat(meta.snapshotCompletedTables()).isEmpty();
    }

    @Test
    void seedMetaRoundTripsWithNoOffsetsConsumersOrSchema() {
        SrsMeta seed = new SrsMeta("chain", null, List.of(), null, List.of(), null);

        Document document = MongoSrsMetaStore.toDocument(seed);

        // the nullable positions are simply absent, not stored as explicit nulls; the structural fields
        // are present-but-empty, so the seeded chain reads back as a seed rather than as corruption
        assertThat(document.containsKey("sourceReadOffset")).isFalse();
        assertThat(document.containsKey("cdcStartPosition")).isFalse();
        assertThat(document.containsKey("retention")).isFalse();
        assertThat(document.get("consumerOffsets", Document.class)).isEmpty();
        assertThat(document.getList("schemaHistory", Document.class)).isEmpty();
        assertThat(MongoSrsMetaStore.toMeta(document)).isEqualTo(seed);
    }

    @Test
    void toMetaOnADocumentMissingAStructuralFieldIsDocumentUnreadable() {
        // a stored meta missing a field this version requires (here: schemaHistory) is store corruption,
        // surfaced as a coded io diagnostic rather than a bare cast/unboxing crash while reconstructing.
        Document corrupt = new Document("_id", "chain").append("consumerOffsets", new Document());

        Throwable thrown = catchThrowable(() -> MongoSrsMetaStore.toMeta(corrupt));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(coded.args()).containsEntry("id", "chain");
    }

    @Test
    void createDuplicateKeyIsAnOrderingErrorAndOtherWriteFailuresAreCodedIo() {
        // a duplicate _id (re-seed) is a caller ordering error, surfaced bare; any other driver write
        // failure during the seed is a coded io diagnostic. Witnessed deterministically, without a
        // server, by classifying constructed driver write errors.
        MongoException duplicate = new MongoWriteException(
                new WriteError(11000, "E11000 duplicate key", new BsonDocument()), new ServerAddress(), Set.of());
        assertThat(MongoSrsMetaStore.classifyInsertFailure(duplicate, "chain"))
                .isInstanceOf(IllegalStateException.class);

        MongoException validation = new MongoWriteException(
                new WriteError(121, "document validation failure", new BsonDocument()), new ServerAddress(), Set.of());
        RuntimeException classified = MongoSrsMetaStore.classifyInsertFailure(validation, "chain");
        assertThat(classified).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) classified).code()).isEqualTo(IoError.STORE_UNAVAILABLE);
    }

    @Test
    void consumerReadSeqUpdateTargetsOnlyThatTablesCursorPathNotTheWholeConsumer() {
        // The reader's per-table cursor advance is a path-scoped $set: it touches only
        // consumerOffsets.<pipelineId>.perTableSeq.<table>, so a reader advancing its cursor never clobbers
        // the sink-acked position the sink writes to the same consumer document -- the two are independent
        // writers on one consumer record.
        Document update = MongoSrsMetaStore.consumerReadSeqUpdate("p1", "orders", 42L);

        assertThat(update.get("$set", Document.class))
                .containsExactly(Map.entry("consumerOffsets.p1.perTableSeq.orders", 42L));
    }

    @Test
    void sinkAckedUpdateTargetsOnlyThatConsumersAckPathNotThePerTableCursor() {
        // The sink's ack advance is a path-scoped $set: it touches only that consumer's acked position, so a
        // sink advancing it never clobbers the per-table read cursor the reader writes to the same consumer
        // document -- the two are independent writers on one consumer record. Both halves of the position
        // move in the one update: a stored token whose order was left behind can no longer be ranked, and an
        // order with no token is nothing a read resumes from.
        Document update = MongoSrsMetaStore.sinkAckedUpdate("p1", new ChainPosition(new SourceOrder(1, 99), "gtid:aaa-1:99"));

        assertThat(update.get("$set", Document.class)).containsOnly(
                Map.entry("consumerOffsets.p1.sinkAckedEpoch", 1L),
                Map.entry("consumerOffsets.p1.sinkAckedSeq", 99L),
                Map.entry("consumerOffsets.p1.sinkAckedSrcpos", "gtid:aaa-1:99"));
    }

    @Test
    void aConsumerWithOnlyASinkAckedPositionAndNoReadCursorReadsBackWithAnEmptyCursor() {
        // The sink and the reader are independent writers of one consumer; the sink may create the consumer
        // entry (a sink-ack-only $set) before the reader publishes any per-table cursor. Such a consumer
        // carries the acked position and no perTableSeq sub-document, and must read back as an empty cursor
        // -- not as corruption -- mirroring how an absent acked position reads back as null.
        Document doc = new Document("_id", "chain")
                .append("consumerOffsets", new Document("p1", new Document("sinkAckedSrcpos", "gtid:aaa-1:99")
                        .append("sinkAckedEpoch", 1L)
                        .append("sinkAckedSeq", 99L)))
                .append("schemaHistory", List.of());

        SrsMeta meta = MongoSrsMetaStore.toMeta(doc);

        assertThat(meta.consumerOffsets()).hasSize(1);
        ConsumerOffset p1 = meta.consumerOffsets().get(0);
        assertThat(p1.pipelineId()).isEqualTo("p1");
        assertThat(p1.perTableSeq()).isEmpty();
        assertThat(p1.sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(1, 99), "gtid:aaa-1:99"));
    }

    @Test
    void aStoredTokenWithNoOrderBesideItReadsBackAsNothingAcked() {
        // A token on its own cannot be ranked against the reader's position, and ranking is the whole use a
        // source-read advance has for it. Reading it as an ack would put an unrankable value into that
        // comparison; reading it as nothing acked pins the advance where it stands, which costs re-mining
        // changes already read and keeps every unacked one re-minable.
        Document doc = new Document("_id", "chain")
                .append("consumerOffsets", new Document("p1", new Document("sinkAckedSrcpos", "gtid:aaa-1:99")))
                .append("schemaHistory", List.of());

        SrsMeta meta = MongoSrsMetaStore.toMeta(doc);

        assertThat(meta.consumerOffsets().get(0).sinkAcked()).isNull();
    }
}
