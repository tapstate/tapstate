package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Witnesses the derived-schema side record against a real Mongo replica-set.
 *
 * <p>The document this store writes is hand-shaped -- one document per pipeline, steps as an array
 * rather than as sub-document keys, and a version history inside each -- so the encode / decode is code
 * of its own rather than a driver mapping. Everything else that exercises it runs against an in-memory
 * double that shares none of that code, which means the parts most able to be wrong (column order
 * surviving a round trip, one step's write not eating another's, a corrupt document reported as
 * corruption rather than as a class cast) had nothing looking at them until here.
 *
 * <p>Where Docker is absent this aborts on a developer machine and fails in CI, where a skip would be
 * a green build that ran nothing.
 */
@RequiresDocker
class MongoDerivedSchemaStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static Map<String, String> columns(String... namesAndTypes) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            columns.put(namesAndTypes[i], namesAndTypes[i + 1]);
        }
        return columns;
    }

    @Test
    void aRecordedDerivationReadsBackEqualThroughRealBson() {
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("order_id", "INT64 NOT NULL", "name", "STRING NULL"),
                    "sql-v1", "src-v1", "calcite-1.40.0");

            DerivedSchema read = store.latest("wide", "widen").orElseThrow();
            assertThat(read.version()).isZero();
            // Column order is part of the shape a target table is built from, and a map round-tripped
            // through bson is exactly where it would quietly stop being preserved.
            assertThat(read.schema().keySet()).containsExactly("order_id", "name");
            assertThat(read.schema()).containsEntry("name", "STRING NULL");
            assertThat(read.statement()).isEqualTo("sql-v1");
            assertThat(read.derivedFrom()).isEqualTo("src-v1");
            assertThat(read.derivedBy()).isEqualTo("calcite-1.40.0");
        });
    }

    @Test
    void aStepNothingHasRecordedReadsBackEmpty() {
        withStore((store, collection) -> assertThat(store.latest("wide", "never")).isEmpty());
    }

    @Test
    void changedColumnsAppendAVersionAndUnchangedOnesRefreshTheProvenanceInPlace() {
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-v1", "src-v1", "calcite");
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-v1", "src-v2", "calcite");

            assertThat(store.latest("wide", "widen").orElseThrow().version()).isZero();
            assertThat(store.latest("wide", "widen").orElseThrow().derivedFrom()).isEqualTo("src-v2");

            store.record("wide", "widen", columns("id", "DECIMAL NOT NULL"), "sql-v1", "src-v3", "calcite");

            assertThat(store.latest("wide", "widen").orElseThrow().version()).isEqualTo(1L);
            // One document per pipeline throughout: a version per document would put the drop this store
            // owes a removed pipeline beyond the reach of its own _id.
            assertThat(collection.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void oneStepsWriteDoesNotEatAnothersHistory() {
        // The two steps hold documents of their own, which is what the count pins: were they ever
        // merged back into one, writing either would be a read-modify-write over the other's history,
        // and losing the neighbour would look exactly like a step that was never recorded.
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");
            store.record("wide", "enrich", columns("name", "STRING NULL"), "sql-b", "src-v1", "calcite");
            store.record("wide", "widen", columns("id", "DECIMAL NOT NULL"), "sql-a", "src-v2", "calcite");

            assertThat(store.latest("wide", "enrich").orElseThrow().schema())
                    .containsExactly(Map.entry("name", "STRING NULL"));
            assertThat(store.latest("wide", "widen").orElseThrow().version()).isEqualTo(1L);
            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void deletingAPipelineRemovesItsRecordAndLeavesEveryOtherPipelineAlone() {
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");
            store.record("other", "widen", columns("id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");

            store.delete("wide");

            assertThat(store.latest("wide", "widen")).isEmpty();
            assertThat(store.latest("other", "widen")).isPresent();
        });
    }

    @Test
    void aVersionARunPinnedSurvivesEveryLaterRecordOnThatStep() {
        // The discriminating case, and the one the document layout is arranged for. The pin sits on the
        // step's own document beside its history, so a write that replaced the document wholesale would
        // take the pin with it - and the moment a pin is read is the moment somebody has just recorded a
        // shape, which is the same moment such a write happens.
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");
            store.pin("wide", "widen", 0L);

            store.record("wide", "widen", columns("id", "DECIMAL NOT NULL"), "sql-a", "src-v2", "calcite");

            assertThat(store.latest("wide", "widen").orElseThrow().version()).isEqualTo(1L);
            assertThat(store.pinned("wide", "widen").orElseThrow().schema())
                    .containsExactlyEntriesOf(columns("id", "INT64 NOT NULL"));
        });
    }

    @Test
    void aStepNothingHasPinnedReadsBackEmptyRatherThanAsItsNewestVersion() {
        // Never a fallback to the latest: a caller asking for the pin is asking what a run holds, and
        // answering with whatever is newest is precisely the confusion the pin exists to end.
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");

            assertThat(store.pinned("wide", "widen")).isEmpty();
        });
    }

    @Test
    void deletingAPipelineTakesItsPinsWithIt() {
        // A pin left behind would name a version of a history that is gone, and would be read as what a
        // run of whatever is applied under this id next is holding.
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");
            store.pin("wide", "widen", 0L);

            store.delete("wide");

            assertThat(store.pinned("wide", "widen")).isEmpty();
            assertThat(collection.countDocuments()).isZero();
        });
    }

    @Test
    void deletingAPipelineThatRecordedNothingIsNotAnError() {
        withStore((store, collection) ->
                assertThatCode(() -> store.delete("never-seen")).doesNotThrowAnyException());
    }

    @Test
    void aColumnNameHoldingADotSurvivesTheRoundTrip() {
        // The reason steps and columns are arrays rather than sub-document keys. A bson field name
        // cannot hold a dot, so keying by author-chosen text would work until the first author wrote
        // SELECT o.id AS "order.id" -- and would then fail inside the driver, where no message can name
        // the cause.
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("order.id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");

            assertThat(store.latest("wide", "widen").orElseThrow().schema())
                    .containsExactly(Map.entry("order.id", "INT64 NOT NULL"));
        });
    }

    @Test
    void aDocumentMissingAFieldThisVersionRequiresIsReportedAsCorruptionNotAsAClassCast() {
        // A stored document written by something else, or damaged, must not reach a caller as a bare
        // cast failure: that is a defect dressed as a runtime crash with no pointer to the document.
        withStore((store, collection) -> {
            collection.insertOne(new Document("_id", "wide.widen").append("versions",
                    List.of(new Document("version", 0)
                            .append("columns", List.of(new Document("name", "id")))
                            .append("statement", "sql-a")
                            .append("derivedFrom", "src-v1")
                            .append("derivedBy", "calcite"))));

            assertThatThrownBy(() -> store.latest("wide", "widen"))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code().code()).isEqualTo("io.document-unreadable");
                        assertThat(error.args()).containsEntry("id", "wide").containsEntry("field", "versions");
                    });
        });
    }

    /**
     * A pipeline of ordinary size cannot record its steps' schemas, because every step it has shares one
     * document and therefore one 16MB ceiling.
     *
     * <p>Five steps, each a 1000-column table recorded over forty shape changes, is roughly 3.7MB per
     * step - comfortably storable on its own - and roughly 18MB once the five share a document. Measured
     * 2026-09-08: the write fails partway through the fifth step, at the 170th record.
     *
     * <p><b>What fails is the write, so the failure is permanent.</b> The ceiling is not slowness: once
     * the document is at the cap no further shape change can be recorded for any step of that pipeline,
     * and a start that derives a new shape fails at this store every time it is tried. Nothing shrinks
     * the document back.
     *
     * <p>This is red until the record is split so that a step's history is stored per
     * {@code (pipelineId, stepId)} rather than per pipeline. It stays honest about what that split buys:
     * each step here is well under the cap, so the split is what this case needs and nothing more. A
     * single step whose own history passes 16MB is a different, still-open ceiling - the version history
     * is append-only with no bound - and no case here claims otherwise.
     */
    @Test
    void aPipelinesStepsDoNotShareOneDocumentCeiling() {
        withStore((store, collection) -> {
            for (int step = 0; step < 5; step++) {
                for (int version = 0; version < 40; version++) {
                    store.record("wide", "step_" + step, wideColumns(1000, version),
                            "sql-v" + version, "src-v" + version, "calcite-1.40.0");
                }
            }

            // The last step recorded is the one that proves the pipeline got all the way through; a
            // partial write leaves the earlier steps readable, so reading step 0 would pass regardless.
            assertThat(store.latest("wide", "step_4")).isPresent();
        });
    }

    /** A table wide enough to be worth storing, with the column names an author actually writes. */
    private static Map<String, String> wideColumns(int columns, int generation) {
        Map<String, String> schema = new LinkedHashMap<>();
        for (int i = 0; i < columns; i++) {
            schema.put("customer_shipping_address_line_detail_" + i + "_g" + generation,
                    "DECIMAL(18,4) NOT NULL");
        }
        return schema;
    }

    /** One stored version, in the shape a step's history holds. */
    private static Document version(int number, String column, String type, String statement) {
        return new Document("version", number)
                .append("columns", List.of(new Document("name", column).append("type", type)))
                .append("statement", statement)
                .append("derivedFrom", "src-v1")
                .append("derivedBy", "calcite");
    }

    /**
     * A pipeline still held in the superseded one-document-per-pipeline shape keeps every version it
     * had, both when it is read and when it is next written.
     *
     * <p>This history is the baseline a drift report is compared against, so it cannot be rebuilt by
     * re-deriving: a re-derivation produces today's shape and knows nothing of the ones before it.
     * Answering "never recorded" over such a pipeline would silently reset that baseline and report
     * the next genuine change as the first one - which is why the move happens rather than a discard.
     */
    @Test
    void aPipelineMigratedAtStartupKeepsItsHistoryWhenNextWritten() {
        withStore((store, collection) -> {
            collection.insertOne(new Document("_id", "legacy").append("steps", List.of(
                    new Document("step", "widen").append("versions", List.of(
                            version(0, "id", "INT64 NOT NULL", "sql-a"),
                            version(1, "id", "DECIMAL NOT NULL", "sql-b"))),
                    new Document("step", "enrich").append("versions", List.of(
                            version(0, "name", "STRING NULL", "sql-c"))))));

            try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
                SystemCollections.SYSTEM_META.on(client.getDatabase("tapstate")).drop();
                MigrationRunner.migrate(client.getDatabase("tapstate"));
            }
            // Startup has moved every step before the first store read or write.
            assertThat(store.latest("legacy", "widen").orElseThrow().version()).isEqualTo(1L);

            store.record("legacy", "widen", columns("id", "STRING NULL"), "sql-d", "src-v9", "calcite");

            // Version 2, not version 0: the new shape continues the history rather than restarting it.
            assertThat(store.latest("legacy", "widen").orElseThrow().version()).isEqualTo(2L);
            // The step that was not written came across as well - moving only the step being recorded
            // would strand the others the moment the old document was removed.
            assertThat(store.latest("legacy", "enrich").orElseThrow().schema())
                    .containsExactly(Map.entry("name", "STRING NULL"));
            assertThat(collection.find(new Document("_id", "legacy")).first()).isNull();
        });
    }

    @Test
    void deletingAPipelineAlsoRemovesWhatItHeldInTheSupersededDocument() {
        withStore((store, collection) -> {
            collection.insertOne(new Document("_id", "legacy").append("steps", List.of(
                    new Document("step", "widen").append("versions",
                            List.of(version(0, "id", "INT64 NOT NULL", "sql-a"))))));

            store.delete("legacy");

            // The superseded document keys on the pipeline id itself, which is outside the range the
            // split keys occupy; a drop that only swept that range would leave it behind for good.
            assertThat(collection.countDocuments()).isZero();
        });
    }

    private interface StoreTest {
        void run(MongoDerivedSchemaStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection =
                    client.getDatabase("tapstate").getCollection(MongoStorePort.DERIVED_SCHEMAS);
            collection.drop();
            test.run(new MongoDerivedSchemaStore(collection), collection);
        }
    }
}
