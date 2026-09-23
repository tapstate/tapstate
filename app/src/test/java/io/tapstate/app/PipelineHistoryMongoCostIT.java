package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.HistoryResolution;
import io.tapstate.control.core.PipelineHistoryQuery;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.control.core.PipelineMetricsHistory;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Real-Mongo cost and latency baseline for the bounded query path. */
@RequiresDocker
class PipelineHistoryMongoCostIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final Instant NOW = Instant.parse("2026-09-21T12:00:00Z");
    private static final Instant COUNTING_SINCE = Instant.parse("2026-09-01T00:00:00Z");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void oneHourOneDayAndFifteenDaysStayBoundedAgainstRealMongo() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("history_query_cost_it");
            database.drop();
            MongoCollection<Document> collection = SystemCollections.PIPELINE_RATE_HISTORY.on(database);
            MongoRateHistoryStore mongo = new MongoRateHistoryStore(database, collection, Duration.ofDays(15));
            seedFifteenDays(collection);
            CountingHistory history = new CountingHistory(mongo);
            PipelineHistoryQueryService service = new PipelineHistoryQueryService(
                    artifacts(), history, Duration.ofMinutes(1), Clock.fixed(NOW, ZoneOffset.UTC),
                    new HistoryCursorCodec("history-cost-secret".getBytes(StandardCharsets.UTF_8),
                            Clock.fixed(NOW, ZoneOffset.UTC)));

            runAndReport(service, history, Duration.ofHours(1), 60);
            runAndReport(service, history, Duration.ofDays(1), 48);
            runAndReport(service, history, Duration.ofDays(15), 60);
        }
    }

    private static void runAndReport(PipelineHistoryQueryService service, CountingHistory history,
            Duration span, int expectedPoints) {
        history.resetCounts();
        long started = System.nanoTime();
        PipelineMetricsHistory response = service.query(new PipelineHistoryQuery(
                "orders", NOW.minus(span), NOW, HistoryResolution.AUTO,
                PipelineHistoryQuery.DEFAULT_LIMIT, List.of("orders"), null));
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis();
        int output = response.segments().stream().mapToInt(segment -> segment.points().size()).sum();

        assertThat(output).isEqualTo(expectedPoints);
        assertThat(history.scanned).isLessThanOrEqualTo(PipelineHistoryQueryService.DEFAULT_RAW_SCAN_BUDGET);
        assertThat(history.largestPage).isLessThanOrEqualTo(RateHistoryStore.MAX_PAGE_SIZE);
        assertThat(response.nextCursor()).isNull();
        System.out.printf("history-query-baseline os=%s/%s java=%s mongo=7.0 span=%s pages=%d scanned=%d"
                        + " largestPage=%d output=%d elapsedMs=%d%n",
                System.getProperty("os.name"), System.getProperty("os.arch"), System.getProperty("java.version"),
                span, history.pageReads, history.scanned, history.largestPage, output, elapsedMillis);
    }

    private static void seedFifteenDays(MongoCollection<Document> collection) {
        long minutes = Duration.ofDays(15).toMinutes();
        Instant first = NOW.minus(Duration.ofDays(15)).minus(Duration.ofMinutes(1));
        List<Document> documents = new ArrayList<>((int) minutes + 2);
        for (long minute = 0; minute <= minutes + 1; minute++) {
            Instant at = first.plus(Duration.ofMinutes(minute));
            long seconds = minute * 60L;
            documents.add(MongoRateHistoryStore.toDocument(new RateSample("orders", at,
                    Map.of("records.out", seconds, "bytes.out", seconds * 10),
                    Map.of("orders", minute % 11), COUNTING_SINCE)));
        }
        collection.insertMany(documents);
    }

    private static ArtifactQueryService artifacts() {
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                serve:
                  from: /.*/
                  sync:
                    - id: sink
                      source: target
                      write_mode: upsert
                      ddl: apply
                """);
        return new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<Resource> get(String id) {
                return pipeline.id().equals(id) ? Optional.of(pipeline) : Optional.empty();
            }

            @Override
            public List<Resource> list() {
                return List.of(pipeline);
            }
        });
    }

    private static final class CountingHistory implements RateHistoryStore {
        private final RateHistoryStore delegate;
        private int pageReads;
        private int scanned;
        private int largestPage;

        CountingHistory(RateHistoryStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public void append(RateSample sample) {
            delegate.append(sample);
        }

        @Override
        public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
            Page page = delegate.readPage(pipelineId, from, to, after, limit);
            pageReads++;
            scanned += page.entries().size() + (page.hasMore() ? 1 : 0);
            largestPage = Math.max(largestPage, page.entries().size());
            return page;
        }

        @Override
        public Optional<Entry> read(String pipelineId, Key key) {
            Optional<Entry> entry = delegate.read(pipelineId, key);
            entry.ifPresent(ignored -> scanned++);
            return entry;
        }

        @Override
        public Optional<Entry> predecessor(String pipelineId, Instant at) {
            Optional<Entry> entry = delegate.predecessor(pipelineId, at);
            entry.ifPresent(ignored -> scanned++);
            return entry;
        }

        @Override
        public Optional<Entry> successor(String pipelineId, Instant at) {
            Optional<Entry> entry = delegate.successor(pipelineId, at);
            entry.ifPresent(ignored -> scanned++);
            return entry;
        }

        @Override
        public void deleteAll(String pipelineId) {
            delegate.deleteAll(pipelineId);
        }

        @Override
        public Duration retention() {
            return delegate.retention();
        }

        void resetCounts() {
            pageReads = 0;
            scanned = 0;
            largestPage = 0;
        }
    }
}
