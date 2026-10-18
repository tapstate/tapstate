package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Witnesses the state store's epoch-fencing compare-and-swap against a real Mongo replica-set: a seed
 * lands at epoch 0, a matching-epoch swap applies and bumps the epoch, a stale-epoch swap is fenced
 * and does not overwrite, the epoch advances monotonically, a re-seed is refused (insert-only, so the
 * fencing epoch is never reset), a swap on an unseeded pipeline is an ordering error, and two writers
 * racing the same transition yield exactly one winner and one fenced. Where Docker is absent this
 * aborts on a developer machine and fails in CI, where a skip would be a green build that ran nothing.
 */
@RequiresDocker
class MongoStateStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final Instant T0 = Instant.parse("2026-07-01T00:00:00Z");
    private static final String PIPELINE = "orders-sync";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void createSeedsEpochZeroAndReadReturnsIt() {
        withStore(store -> {
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);

            CheckpointDoc seeded = store.read(PIPELINE).orElseThrow();
            assertThat(seeded.pipelineId()).isEqualTo(PIPELINE);
            assertThat(seeded.epoch()).isZero();
            assertThat(seeded.stateJson()).isEqualTo("{\"state\":\"NEW\"}");
            assertThat(seeded.touchTime()).isEqualTo(T0);
        });
    }

    @Test
    void readReturnsEmptyForAnUnseededPipeline() {
        withStore(store -> assertThat(store.read("never-seeded")).isEmpty());
    }

    @Test
    void createIsInsertOnlyAndDoesNotResetAnExistingCheckpoint() {
        withStore(store -> {
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);
            store.compareAndSwap(PIPELINE, 0, "{\"state\":\"RUNNING\"}", T0.plusSeconds(1)); // epoch -> 1

            // a second seed must be refused: overwriting would reset the fencing epoch back to 0. The
            // collision is a caller ordering error, surfaced bare like the unseeded-swap ordering error.
            assertThatThrownBy(() -> store.create(PIPELINE, "{\"state\":\"RESET\"}", T0.plusSeconds(2)))
                    .isInstanceOf(IllegalStateException.class);

            CheckpointDoc stored = store.read(PIPELINE).orElseThrow();
            assertThat(stored.epoch()).isEqualTo(1L);
            assertThat(stored.stateJson()).isEqualTo("{\"state\":\"RUNNING\"}");
        });
    }

    @Test
    void compareAndSwapWithTheMatchingEpochAppliesAndBumpsTheEpoch() {
        withStore(store -> {
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);

            CasOutcome outcome = store.compareAndSwap(PIPELINE, 0, "{\"state\":\"RUNNING\"}", T0.plusSeconds(1));

            assertThat(outcome).isInstanceOf(CasOutcome.Applied.class);
            CheckpointDoc next = ((CasOutcome.Applied) outcome).next();
            assertThat(next.epoch()).isEqualTo(1L);
            assertThat(next.stateJson()).isEqualTo("{\"state\":\"RUNNING\"}");
            assertThat(next.touchTime()).isEqualTo(T0.plusSeconds(1));
            assertThat(store.read(PIPELINE)).contains(next);
        });
    }

    @Test
    void compareAndSwapWithAStaleEpochIsFencedAndDoesNotOverwrite() {
        withStore(store -> {
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);
            store.compareAndSwap(PIPELINE, 0, "{\"state\":\"RUNNING\"}", T0.plusSeconds(1)); // epoch -> 1

            // a writer that still believes the epoch is 0 is fenced by the stored epoch 1.
            CasOutcome stale = store.compareAndSwap(PIPELINE, 0, "{\"state\":\"STOPPED\"}", T0.plusSeconds(2));

            assertThat(stale).isInstanceOf(CasOutcome.Fenced.class);
            assertThat(((CasOutcome.Fenced) stale).currentEpoch()).isEqualTo(1L);
            CheckpointDoc stored = store.read(PIPELINE).orElseThrow();
            assertThat(stored.epoch()).isEqualTo(1L);
            assertThat(stored.stateJson()).isEqualTo("{\"state\":\"RUNNING\"}");
        });
    }

    @Test
    void theEpochAdvancesMonotonicallyAcrossASequenceOfSwaps() {
        withStore(store -> {
            store.create(PIPELINE, "{\"n\":0}", T0);

            long epoch = 0;
            for (int i = 1; i <= 5; i++) {
                CasOutcome outcome = store.compareAndSwap(PIPELINE, epoch, "{\"n\":" + i + "}", T0.plusSeconds(i));
                assertThat(outcome).as("swap %d applies", i).isInstanceOf(CasOutcome.Applied.class);
                long next = ((CasOutcome.Applied) outcome).next().epoch();
                assertThat(next).isEqualTo(epoch + 1);
                epoch = next;
            }
            assertThat(store.read(PIPELINE).orElseThrow().epoch()).isEqualTo(5L);
        });
    }

    @Test
    void theProductionSwapMatchesTheEpochCasContract() {
        // The store's real conditional update must equal the pure fencing contract, value for value, so
        // the two cannot drift: a matching swap applies to the same next doc, a stale swap fences on the
        // same stored epoch.
        withStore(store -> {
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);

            CheckpointDoc current = store.read(PIPELINE).orElseThrow();
            // a sub-second touch, so the real-Mongo Applied round-trip witnesses millisecond fidelity end to end
            Instant appliedTouch = T0.plusSeconds(1).plusMillis(123);
            CasOutcome pureApplied = EpochCas.swap(current, 0, "{\"state\":\"RUNNING\"}", appliedTouch);
            CasOutcome realApplied = store.compareAndSwap(PIPELINE, 0, "{\"state\":\"RUNNING\"}", appliedTouch);
            assertThat(realApplied).isEqualTo(pureApplied);

            CheckpointDoc afterApply = store.read(PIPELINE).orElseThrow();
            CasOutcome pureFenced = EpochCas.swap(afterApply, 0, "{\"state\":\"STOPPED\"}", T0.plusSeconds(2));
            CasOutcome realFenced = store.compareAndSwap(PIPELINE, 0, "{\"state\":\"STOPPED\"}", T0.plusSeconds(2));
            assertThat(realFenced).isEqualTo(pureFenced);
        });
    }

    @Test
    void compareAndSwapOnAnUnseededPipelineIsAnOrderingError() {
        // The CAS cannot bootstrap from an absent checkpoint; seeding is create's job. Swapping an
        // unseeded pipeline is a caller ordering error, not a fenced outcome.
        withStore(store -> assertThatThrownBy(() -> store.compareAndSwap("never-seeded", 0, "{}", T0))
                .isInstanceOf(IllegalStateException.class));
    }

    @Test
    void twoWritersRacingTheSameTransitionYieldOneWinnerAndOneFenced() {
        withStore(store -> {
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                CountDownLatch startLine = new CountDownLatch(1);
                Future<CasOutcome> writerA = pool.submit(racingSwap(store, startLine, "{\"state\":\"A\"}"));
                Future<CasOutcome> writerB = pool.submit(racingSwap(store, startLine, "{\"state\":\"B\"}"));
                startLine.countDown();

                List<CasOutcome> outcomes = List.of(
                        writerA.get(10, TimeUnit.SECONDS), writerB.get(10, TimeUnit.SECONDS));

                assertThat(outcomes).filteredOn(o -> o instanceof CasOutcome.Applied).hasSize(1);
                assertThat(outcomes).filteredOn(o -> o instanceof CasOutcome.Fenced).hasSize(1);

                CasOutcome.Applied applied = outcomes.stream()
                        .filter(o -> o instanceof CasOutcome.Applied).map(CasOutcome.Applied.class::cast)
                        .findFirst().orElseThrow();
                CasOutcome.Fenced fenced = outcomes.stream()
                        .filter(o -> o instanceof CasOutcome.Fenced).map(CasOutcome.Fenced.class::cast)
                        .findFirst().orElseThrow();
                assertThat(applied.next().epoch()).isEqualTo(1L);
                assertThat(fenced.currentEpoch()).isEqualTo(1L);

                CheckpointDoc stored = store.read(PIPELINE).orElseThrow();
                assertThat(stored.epoch()).isEqualTo(1L);
                assertThat(stored.stateJson()).isEqualTo(applied.next().stateJson());
            } finally {
                pool.shutdownNow();
            }
        });
    }

    @Test
    void deletingACheckpointTakesTheFencingEpochWithItSoTheIdStartsCleanIfReused() {
        withStore(store -> {
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);
            store.compareAndSwap(PIPELINE, 0, "{\"state\":\"RUNNING\"}", T0.plusSeconds(1));
            store.compareAndSwap(PIPELINE, 1, "{\"state\":\"STOPPED\"}", T0.plusSeconds(2));

            store.delete(PIPELINE);

            assertThat(store.read(PIPELINE)).isEmpty();
            // A later pipeline applied under the same id must not inherit an epoch — or a state — that a
            // different pipeline accumulated. Reseeding proves the history really left with the document.
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0.plusSeconds(3));
            CheckpointDoc reseeded = store.read(PIPELINE).orElseThrow();
            assertThat(reseeded.epoch()).isZero();
            assertThat(reseeded.stateJson()).isEqualTo("{\"state\":\"NEW\"}");
        });
    }

    @Test
    void deletingAnUnseededPipelineIsANoOp() {
        withStore(store -> {
            store.delete(PIPELINE);
            store.create(PIPELINE, "{\"state\":\"NEW\"}", T0);
            store.delete(PIPELINE);
            store.delete(PIPELINE);

            assertThat(store.read(PIPELINE)).isEmpty();
        });
    }

    private static Callable<CasOutcome> racingSwap(MongoStateStore store, CountDownLatch startLine, String next) {
        return () -> {
            startLine.await();
            return store.compareAndSwap(PIPELINE, 0, next, T0.plusSeconds(1));
        };
    }

    private interface StoreTest {
        void run(MongoStateStore store) throws Exception;
    }

    /** Runs a test body against a fresh state store over a clean checkpoints collection on the replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("checkpoints");
            collection.drop();
            test.run(new MongoStateStore(collection));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
