package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Witnesses the artifact truth layer against a real Mongo replica-set: a written artifact reads back
 * to the same canonical form, an absent id reads back empty, list returns every stored artifact, a
 * re-save of the same id replaces in place (last write wins) rather than accumulating documents, a
 * batch write commits atomically and rolls the whole batch back on a mid-batch write failure, and a
 * stored document that cannot be reconstructed is surfaced (not silently skipped) without leaking the
 * scan cursor. Where Docker is absent this aborts on a developer machine and fails in CI, where a skip
 * would be a green build that ran nothing.
 */
@RequiresDocker
class MongoArtifactStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static final String ORDERS = """
            version: tapstate/v1
            kind: source
            id: orders
            connector: mysql
            config:
              host: localhost
            """;

    private static final String ORDERS_SYNC = """
            version: tapstate/v1
            kind: pipeline
            id: orders_sync
            source: orders
            """;

    @Test
    void writtenArtifactReadsBackAsTheSameCanonicalForm() {
        withStore((store, collection) -> {
            Resource source = PARSER.parse(ORDERS);
            store.save(source);

            Optional<Resource> read = store.get("orders");
            assertThat(read).isPresent();
            assertThat(WRITER.write(read.get())).isEqualTo(WRITER.write(source));
        });
    }

    @Test
    void getReturnsEmptyForAnAbsentId() {
        withStore((store, collection) -> assertThat(store.get("nope")).isEmpty());
    }

    @Test
    void listReturnsEveryStoredArtifact() {
        withStore((store, collection) -> {
            store.save(PARSER.parse(ORDERS));
            store.save(PARSER.parse(ORDERS_SYNC));

            assertThat(store.list())
                    .extracting(Resource::id)
                    .containsExactlyInAnyOrder("orders", "orders_sync");
        });
    }

    @Test
    void reSaveOfTheSameIdReplacesInPlace() {
        withStore((store, collection) -> {
            store.save(PARSER.parse(ORDERS));
            // a changed resource under the same id: the config host differs
            String changed = """
                    version: tapstate/v1
                    kind: source
                    id: orders
                    connector: mysql
                    config:
                      host: replica
                    """;
            store.save(PARSER.parse(changed));

            assertThat(store.list()).extracting(Resource::id).containsExactly("orders");
            assertThat(WRITER.write(store.get("orders").orElseThrow()))
                    .isEqualTo(WRITER.write(PARSER.parse(changed)));
        });
    }

    @Test
    void versionedMutationsAreAtomicAndStaleWritesLeaveCanonicalBytesUnchanged() {
        withStore((store, collection) -> {
            Resource source = PARSER.parse(ORDERS);
            Resource changed = PARSER.parse("""
                    version: tapstate/v1
                    kind: source
                    id: orders
                    connector: mysql
                    config:
                      host: replica
                    """);
            Resource changedAgain = PARSER.parse("""
                    version: tapstate/v1
                    kind: source
                    id: orders
                    connector: mysql
                    config:
                      host: stale-writer
                    """);
            Resource differentId = PARSER.parse("""
                    version: tapstate/v1
                    kind: source
                    id: customers
                    connector: mysql
                    config:
                      host: replica
                    """);
            String oldHash = CanonicalHash.of(WRITER.write(source));
            String newHash = CanonicalHash.of(WRITER.write(changed));

            assertThat(store.create(source)).isEqualTo(ArtifactMutation.CREATED);
            assertThat(store.create(source)).isEqualTo(ArtifactMutation.ALREADY_EXISTS);
            assertThat(store.replace("orders", oldHash, changed)).isEqualTo(ArtifactMutation.REPLACED);

            String canonicalAfterReplace = collection.find(new Document("_id", "orders"))
                    .first().getString("canonical");
            assertThat(store.replace("orders", oldHash, changedAgain))
                    .isEqualTo(ArtifactMutation.VERSION_CONFLICT);
            assertThat(collection.find(new Document("_id", "orders")).first().getString("canonical"))
                    .isEqualTo(canonicalAfterReplace);

            assertThatThrownBy(() -> store.replace("orders", newHash, differentId))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(collection.find(new Document("_id", "orders")).first().getString("canonical"))
                    .isEqualTo(canonicalAfterReplace);

            assertThat(store.delete("orders", oldHash)).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
            assertThat(collection.find(new Document("_id", "orders")).first().getString("canonical"))
                    .isEqualTo(canonicalAfterReplace);

            assertThat(store.delete("orders", newHash)).isEqualTo(ArtifactMutation.DELETED);
            assertThat(store.delete("orders", newHash)).isEqualTo(ArtifactMutation.NOT_FOUND);
        });
    }

    @Test
    void conditionalBatchWritesKeepCreateAndReplaceConditionsAtomic() {
        withStore((store, collection) -> {
            Resource source = PARSER.parse(ORDERS);
            Resource changed = PARSER.parse(ORDERS.replace("localhost", "replica"));
            Resource stale = PARSER.parse(ORDERS.replace("localhost", "stale-writer"));
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);

            try (ExecutorService callers = Executors.newFixedThreadPool(2)) {
                Future<ArtifactBatchWrite> alpha = callers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return store.writeAll(List.of(ArtifactWrite.createOnly(source)));
                });
                Future<ArtifactBatchWrite> beta = callers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return store.writeAll(List.of(ArtifactWrite.createOnly(source)));
                });
                ready.await();
                start.countDown();

                List<ArtifactBatchWrite> outcomes = List.of(alpha.get(), beta.get());
                assertThat(outcomes).filteredOn(ArtifactBatchWrite::appliedSuccessfully).hasSize(1);
                assertThat(outcomes).filteredOn(outcome -> !outcome.appliedSuccessfully())
                        .singleElement().extracting(ArtifactBatchWrite::refusal)
                        .isEqualTo(ArtifactMutation.ALREADY_EXISTS);
            } catch (Exception error) {
                throw new AssertionError("concurrent create test failed", error);
            }

            String hash = collection.find(new Document("_id", "orders")).first().getString("contentHash");
            assertThat(store.writeAll(List.of(ArtifactWrite.replaceOnly(changed, hash))))
                    .isEqualTo(ArtifactBatchWrite.applied());
            String canonicalAfterReplace = collection.find(new Document("_id", "orders"))
                    .first().getString("canonical");

            ArtifactBatchWrite staleOutcome = store.writeAll(List.of(ArtifactWrite.replaceOnly(stale, hash)));
            assertThat(staleOutcome.refusedId()).isEqualTo("orders");
            assertThat(staleOutcome.refusal()).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
            assertThat(collection.find(new Document("_id", "orders")).first().getString("canonical"))
                    .isEqualTo(canonicalAfterReplace);
        });
    }

    @Test
    void concurrentReplacementWithTheSameVersionHasOneWinnerAndLeavesItsCanonicalBytesStored() {
        withStore((store, collection) -> {
            Resource original = PARSER.parse(ORDERS);
            Resource alphaReplacement = PARSER.parse(ORDERS.replace("localhost", "alpha"));
            Resource betaReplacement = PARSER.parse(ORDERS.replace("localhost", "beta"));
            assertThat(store.writeAll(List.of(ArtifactWrite.createOnly(original))))
                    .isEqualTo(ArtifactBatchWrite.applied());
            String declared = collection.find(new Document("_id", "orders")).first().getString("contentHash");

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            try (ExecutorService callers = Executors.newFixedThreadPool(2)) {
                Future<ArtifactBatchWrite> alpha = callers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return store.writeAll(List.of(ArtifactWrite.replaceOnly(alphaReplacement, declared)));
                });
                Future<ArtifactBatchWrite> beta = callers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return store.writeAll(List.of(ArtifactWrite.replaceOnly(betaReplacement, declared)));
                });
                ready.await();
                start.countDown();

                ArtifactBatchWrite alphaOutcome = alpha.get();
                ArtifactBatchWrite betaOutcome = beta.get();
                assertThat(List.of(alphaOutcome, betaOutcome))
                        .filteredOn(ArtifactBatchWrite::appliedSuccessfully)
                        .hasSize(1);
                assertThat(List.of(alphaOutcome, betaOutcome))
                        .filteredOn(outcome -> !outcome.appliedSuccessfully())
                        .singleElement()
                        .satisfies(outcome -> {
                            assertThat(outcome.refusedId()).isEqualTo("orders");
                            assertThat(outcome.refusal()).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
                        });

                String expectedCanonical = alphaOutcome.appliedSuccessfully()
                        ? WRITER.write(alphaReplacement)
                        : WRITER.write(betaReplacement);
                assertThat(collection.find(new Document("_id", "orders")).first().getString("canonical"))
                        .isEqualTo(expectedCanonical);
            } catch (Exception error) {
                throw new AssertionError("concurrent replace test failed", error);
            }
        });
    }

    @Test
    void saveAllCommitsTheWholeBatchAtomically() {
        withStore((store, collection) -> {
            store.saveAll(List.of(PARSER.parse(ORDERS), PARSER.parse(ORDERS_SYNC)));

            assertThat(store.list())
                    .extracting(Resource::id)
                    .containsExactlyInAnyOrder("orders", "orders_sync");
        });
    }

    @Test
    void aConditionalBatchWritesOnlyWhileEveryDeclaredVersionStillHolds() {
        withStore((store, collection) -> {
            Resource orders = PARSER.parse(ORDERS);
            store.save(orders);
            String declared = collection.find(new Document("_id", "orders")).first().getString("contentHash");
            Resource edited = PARSER.parse(ORDERS.replace("localhost", "replica"));

            assertThat(store.saveAll(List.of(edited), Map.of("orders", declared))).isEmpty();
            String afterEdit = collection.find(new Document("_id", "orders")).first().getString("canonical");

            // The version the caller declared has moved on. The batch must write none of itself — the
            // valid sibling included, since the comparison happens inside the same transaction as the
            // writes rather than before it.
            assertThat(store.saveAll(
                    List.of(PARSER.parse(ORDERS.replace("localhost", "stale-writer")), PARSER.parse(ORDERS_SYNC)),
                    Map.of("orders", declared)))
                    .contains("orders");
            assertThat(collection.find(new Document("_id", "orders")).first().getString("canonical"))
                    .isEqualTo(afterEdit);
            assertThat(collection.find(new Document("_id", "orders_sync")).first())
                    .as("the refused batch opened a transaction and committed none of it")
                    .isNull();
        });
    }

    @Test
    void aDeclaredVersionForAnIdThatIsGoneRefusesRatherThanRecreatingIt() {
        withStore((store, collection) -> {
            Resource orders = PARSER.parse(ORDERS);
            store.save(orders);
            String declared = collection.find(new Document("_id", "orders")).first().getString("contentHash");
            store.delete("orders", declared);

            // Nothing is stored, so nothing equals the declared version. Upserting here would silently
            // resurrect a resource whose author believes they are amending it.
            assertThat(store.saveAll(List.of(PARSER.parse(ORDERS)), Map.of("orders", declared)))
                    .contains("orders");
            assertThat(collection.find(new Document("_id", "orders")).first()).isNull();
        });
    }

    @Test
    void aConditionalBatchComparesOnlyTheHashAndNeverReconstructsTheBody() {
        withStore((store, collection) -> {
            store.save(PARSER.parse(ORDERS));
            // A sibling this version cannot parse. The comparison reads the hash field only, so it must
            // not be dragged into a batch that merely declares a version of something else — otherwise
            // one unreadable document would veto every conditional write in the store.
            collection.insertOne(new Document("_id", "corrupt").append("kind", "source")
                    .append("canonical", "not: [valid").append("contentHash", "0".repeat(64)));
            String declared = collection.find(new Document("_id", "orders")).first().getString("contentHash");

            assertThat(store.saveAll(
                    List.of(PARSER.parse(ORDERS.replace("localhost", "replica"))),
                    Map.of("orders", declared)))
                    .isEmpty();
            assertThat(collection.find(new Document("_id", "orders")).first().getString("canonical"))
                    .isEqualTo(WRITER.write(PARSER.parse(ORDERS.replace("localhost", "replica"))));
        });
    }

    @Test
    void saveAllRollsBackTheWholeBatchOnAWriteFailure() {
        withStore((store, collection) -> {
            // Force a genuine mid-transaction write error: a unique index on the (non-unique-in-reality)
            // kind field makes the second same-kind upsert in the batch collide, so the driver fails the
            // write inside the transaction. The whole batch must then roll back — not even the first,
            // earlier-ordered resource is left behind.
            collection.createIndex(new Document("kind", 1), new IndexOptions().unique(true));
            String orders2 = """
                    version: tapstate/v1
                    kind: source
                    id: orders2
                    connector: mysql
                    config:
                      host: localhost
                    """;

            Throwable thrown = catchThrowable(() ->
                    store.saveAll(List.of(PARSER.parse(ORDERS), PARSER.parse(orders2))));

            assertThat(thrown).isInstanceOf(TapstateException.class);
            assertThat(store.list()).as("the whole batch rolled back — nothing was stored").isEmpty();
        });
    }

    @Test
    void listSurfacesAnUnreadableStoredDocument() {
        withStore((store, collection) -> {
            store.save(PARSER.parse(ORDERS));
            // An out-of-band document whose body is not a parseable artifact (corruption, or a body
            // written by a newer grammar). The truth layer must surface it rather than silently skip
            // it, and the scan must not leak its server-side cursor on the failure path.
            collection.insertOne(new Document("_id", "corrupt").append("kind", "source").append("canonical", "not: [valid"));

            Throwable thrown = catchThrowable(store::list);
            assertThat(thrown).isInstanceOf(TapstateException.class);
            assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(((TapstateException) thrown).args()).containsEntry("id", "corrupt");
        });
    }

    private interface StoreTest {
        void run(MongoArtifactStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh artifact store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("artifacts");
            collection.drop();
            test.run(new MongoArtifactStore(client, collection), collection);
        }
    }
}
