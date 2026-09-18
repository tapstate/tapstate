package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The control plane's overview reads cost the same over a full history as over an empty one.
 *
 * <p>The history is bounded by age, and the list, status and metrics reads must not depend on walking it:
 * at the default cadence and retention a pipeline's history is some twenty thousand samples, and an
 * overview that walked them - for a rate, a last-seen, anything - would slow as the history filled and
 * recover as it expired, a symptom nobody would trace back to the overview.
 *
 * <p>What is asserted is what the server did, not how long it took. The server counts every document and
 * every index key its query executor examined; the overview reads are made over an empty history and
 * again over a full one, and what the full round examined is required to stay below the size of the
 * history. An overview that scanned the history even once per round would examine all of it. The
 * wall-clock times of both rounds are printed beside the counts as the evidence a reader wants to see,
 * but a timing is not what decides - on a shared machine it can say anything.
 *
 * <p>The history is laid down through the adapter's own codec rather than as documents spelled here, so
 * what is scanned, or not, has the shape the product writes.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class OverviewQueriesDoNotScanTheHistoryIT {

    /** Fourteen days of samples at one a minute: a history one day short of the default retention. */
    private static final int FULL_HISTORY = 14 * 24 * 60;
    private static final Duration CADENCE = Duration.ofMinutes(1);
    private static final int READS_PER_ROUND = 20;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRoundOfOverviewReadsExaminesNoHistoryWorthOfDocuments(Tiers tier, @TempDir Path directory)
            throws Exception {
        String storeUri = storeUri("overview_history", tier);
        try (ServerHandle server = tier.launch(storeUri);
                MongoClient client = MongoClients.create(storeUri)) {
            RunningPipeline running = RunningPipeline.started(server, directory);
            ControlPlane control = running.control();
            String pipelineId = running.pipelineId();

            Round empty = round(control, pipelineId, client);
            layDownAFullHistory(client, storeUri, pipelineId);
            Round full = round(control, pipelineId, client);

            // The evidence line: printed, not asserted, and both rounds on purpose - a number alone does
            // not say whether it is large.
            System.out.printf(Locale.ROOT,
                    "%s overview reads x%d: empty history examined=%d in %d ms; %d samples examined=%d in %d ms%n",
                    tier, READS_PER_ROUND, empty.examined(), empty.millis(), FULL_HISTORY, full.examined(),
                    full.millis());

            assertThat(full.examined())
                    .as("documents and index keys the server examined for a round of overview reads over "
                            + "%d samples; one scan of the history per round would examine every one of them",
                            FULL_HISTORY)
                    .isLessThan(FULL_HISTORY);
        }
    }

    /** One round of the reads a console makes to draw its overview, and what the server examined for them. */
    private static Round round(ControlPlane control, String pipelineId, MongoClient client) {
        long examinedBefore = examined(client);
        long started = System.nanoTime();
        for (int i = 0; i < READS_PER_ROUND; i++) {
            control.pipelines();
            control.state(pipelineId);
            control.metrics(pipelineId);
        }
        long millis = (System.nanoTime() - started) / 1_000_000;
        return new Round(examined(client) - examinedBefore, millis);
    }

    /**
     * Documents plus index keys the server's query executor has examined since it started, over every
     * collection. Server-wide on purpose: a read that reached the history through another collection's
     * query would not show up in a count kept on the history alone.
     */
    private static long examined(MongoClient client) {
        Document status = client.getDatabase("admin").runCommand(new Document("serverStatus", 1));
        Document executor = status.get("metrics", Document.class).get("queryExecutor", Document.class);
        return executor.get("scannedObjects", Number.class).longValue()
                + executor.get("scanned", Number.class).longValue();
    }

    /** Fills the pipeline's history at the default cadence, newest sample a minute ago, through the product's codec. */
    private static void layDownAFullHistory(MongoClient client, String storeUri, String pipelineId) {
        MongoCollection<Document> history = client
                .getDatabase(new ConnectionString(storeUri).getDatabase())
                .getCollection(MongoStorePort.PIPELINE_RATE_HISTORY);
        Instant now = Instant.now();
        Instant countingSince = now.minus(CADENCE.multipliedBy(FULL_HISTORY));
        List<Document> batch = new ArrayList<>();
        for (int i = FULL_HISTORY; i >= 1; i--) {
            long taken = FULL_HISTORY - i;
            batch.add(MongoRateHistoryStore.toDocument(new RateSample(pipelineId, now.minus(CADENCE.multipliedBy(i)),
                    Map.of("records.out", taken, "records.in", taken), Map.of("orders", 0L), countingSince)));
            if (batch.size() == 2_000) {
                history.insertMany(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            history.insertMany(batch);
        }
        // At least: the running pipeline is sampling itself meanwhile, and its own samples count too.
        assertThat(history.countDocuments(new Document(MongoRateHistoryStore.PIPELINE_ID, pipelineId)))
                .as("the history laid down, before anything is read over it").isGreaterThanOrEqualTo(FULL_HISTORY);
    }

    private record Round(long examined, long millis) {
    }

    private static String storeUri(String name, Tiers tier) {
        return SharedMongo.replicaSetUrl(name + "_" + tier.name().toLowerCase(Locale.ROOT));
    }
}
