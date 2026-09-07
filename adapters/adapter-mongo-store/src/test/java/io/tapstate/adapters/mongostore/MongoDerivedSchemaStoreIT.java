package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
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
    void oneStepsWriteDoesNotEatAnothersInTheSameDocument() {
        // Both steps live in one document, so writing one is a read-modify-write over the other's
        // history. Losing the neighbour would look exactly like a step that was never recorded.
        withStore((store, collection) -> {
            store.record("wide", "widen", columns("id", "INT64 NOT NULL"), "sql-a", "src-v1", "calcite");
            store.record("wide", "enrich", columns("name", "STRING NULL"), "sql-b", "src-v1", "calcite");
            store.record("wide", "widen", columns("id", "DECIMAL NOT NULL"), "sql-a", "src-v2", "calcite");

            assertThat(store.latest("wide", "enrich").orElseThrow().schema())
                    .containsExactly(Map.entry("name", "STRING NULL"));
            assertThat(store.latest("wide", "widen").orElseThrow().version()).isEqualTo(1L);
            assertThat(collection.countDocuments()).isEqualTo(1);
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
            collection.insertOne(new Document("_id", "wide").append("steps",
                    List.of(new Document("step", "widen").append("versions",
                            List.of(new Document("version", 0)
                                    .append("columns", List.of(new Document("name", "id")))
                                    .append("statement", "sql-a")
                                    .append("derivedFrom", "src-v1")
                                    .append("derivedBy", "calcite"))))));

            assertThatThrownBy(() -> store.latest("wide", "widen"))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code().code()).isEqualTo("io.document-unreadable");
                        assertThat(error.args()).containsEntry("id", "wide");
                    });
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
