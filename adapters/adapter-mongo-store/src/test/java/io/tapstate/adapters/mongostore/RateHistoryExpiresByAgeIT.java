package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Samples leave by age, and the server is what makes them leave. The retention is shortened to seconds
 * so the case can wait it out; the server's expiry sweep, which runs once a minute by default, is asked
 * to run every second for the same reason. What is asserted is the shape rather than the moment: an old
 * sample goes, a young one stays, and a pipeline that wrote nothing further — which is what a stopped
 * pipeline is — has its old sample taken away all the same, since nothing on the writing side is what
 * removes it.
 *
 * <p>A single-node replica set expires on its primary, which is the only node there is; that this holds
 * across a cluster is the server's promise, not this case's.
 */
@RequiresDocker
class RateHistoryExpiresByAgeIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static RateSample sample(String pipelineId, Instant at) {
        return new RateSample(pipelineId, at, Map.of("records.out", 1L), Map.of(), at.minusSeconds(3600));
    }

    @Test
    void oldSamplesLeaveAndYoungOnesStayWithoutAnybodyWriting() throws Exception {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            client.getDatabase("admin").runCommand(new Document("setParameter", 1).append("ttlMonitorSleepSecs", 1));
            MongoDatabase database = client.getDatabase("history_expiry_it");
            MongoRateHistoryStore history = new MongoRateHistoryStore(database,
                    SystemCollections.PIPELINE_RATE_HISTORY.on(database), Duration.ofSeconds(5));
            Instant now = Instant.now();

            // One pipeline that keeps writing, and one that stops after its first sample: the second is the
            // case this store exists for, since a writer that trimmed on its way in would never reach it.
            history.append(sample("running", now.minusSeconds(120)));
            history.append(sample("running", now));
            history.append(sample("stopped", now.minusSeconds(120)));

            Instant deadline = Instant.now().plusSeconds(60);
            while (Instant.now().isBefore(deadline)
                    && !history.readBetween("stopped", now.minusSeconds(3600), now.plusSeconds(60)).isEmpty()) {
                Thread.sleep(500);
            }

            List<RateSample> running = history.readBetween("running", now.minusSeconds(3600), now.plusSeconds(60));
            assertThat(running).as("the old sample of the running pipeline has gone, the young one stays")
                    .extracting(RateSample::observedAt).containsExactly(now.minusSeconds(0).truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
            assertThat(history.readBetween("stopped", now.minusSeconds(3600), now.plusSeconds(60)))
                    .as("the pipeline that stopped writing had its old sample taken away all the same")
                    .isEmpty();
        }
    }
}
