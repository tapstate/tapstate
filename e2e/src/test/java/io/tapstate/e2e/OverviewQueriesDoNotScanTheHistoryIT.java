package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = tier.launch(storage);
                MongoClient client = MongoClients.create(storage.cacheUri())) {
            RunningPipeline running = RunningPipeline.started(server, directory, storage);
            ControlPlane control = running.control();
            String pipelineId = running.pipelineId();

            Round empty = round(control, pipelineId, client);
            Instant newest = layDownAFullHistory(client, storage.cacheUri(), pipelineId);
            assertSeededHistoryReadable(server, control, pipelineId, newest);
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
    private static Instant layDownAFullHistory(MongoClient client, String storeUri, String pipelineId) {
        MongoCollection<Document> history = client
                .getDatabase(new ConnectionString(storeUri).getDatabase())
                .getCollection(SystemCollections.CACHE_PIPELINE_RATE_HISTORY.collectionName());
        Await.until("the running pipeline to publish an original scoped sample",
                () -> history.find(new Document(MongoRateHistoryStore.PIPELINE_ID, pipelineId)).first() != null,
                () -> "no real rate sample was published");
        Document scope = history.find(new Document(MongoRateHistoryStore.PIPELINE_ID, pipelineId))
                .first().get("scope", Document.class);
        assertThat(scope).isNotNull().containsEntry("pipelineId", pipelineId);
        Instant now = Instant.now();
        Instant countingSince = now.minus(CADENCE.multipliedBy(FULL_HISTORY));
        List<Document> batch = new ArrayList<>();
        for (int i = FULL_HISTORY; i >= 1; i--) {
            long taken = FULL_HISTORY - i;
            batch.add(MongoRateHistoryStore.toDocument(new RateSample(pipelineId, now.minus(CADENCE.multipliedBy(i)),
                    Map.of("records.out", taken, "records.in", taken), Map.of("orders", 0L), countingSince))
                    .append("scope", new Document(scope)));
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
        return now.minus(CADENCE);
    }

    private record Round(long examined, long millis) {
    }

    /** The native public reader must recognize the fixture before it can witness a missing scan. */
    private static void assertSeededHistoryReadable(ServerHandle server, ControlPlane control,
            String pipelineId, Instant newest) throws Exception {
        var request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/pipelines/" + pipelineId
                        + "/metrics/history?from=" + newest.minus(Duration.ofMinutes(4)) + "&to=" + newest
                        + "&resolution=raw&limit=20"))
                .timeout(Duration.ofSeconds(30)).header("Authorization", "Bearer " + control.credential()).GET().build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        var result = (Map<?, ?>) JsonReader.parse(response.body());
        assertThat(result.get("pipelineId")).isEqualTo(pipelineId);
        var segments = (List<?>) result.get("segments");
        assertThat(segments).isNotEmpty();
        assertThat(segments.stream().map(Map.class::cast).flatMap(segment -> ((List<?>) segment.get("points")).stream())
                .map(Map.class::cast).map(point -> (Map<?, ?>) point.get("recordsOut"))
                .filter(java.util.Objects::nonNull).map(rate -> ((Number) rate.get("delta")).longValue()).toList())
                .isNotEmpty().allMatch(delta -> delta == 1L);
    }
}
