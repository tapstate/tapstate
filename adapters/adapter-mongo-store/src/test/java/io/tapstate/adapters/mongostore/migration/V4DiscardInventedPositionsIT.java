package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The changeset that discards the positions a released build invented.
 *
 * <p>What it has to get right is which stored strings were never positions. Builds up to and including
 * the last release never obtained one from a connector: the wiring module handed the runtime a fixed
 * seam and a counter standing in for a watermark, and both were written down as though a source had
 * issued them. This build reads a stored position back through the connector's own codec, refuses the
 * ones it cannot decode -- correctly, since they decode to nothing -- and the pipeline then never
 * starts again.
 *
 * <p>So the two cases below disagree on the answer: one chain carries the invented strings and one
 * carries positions a connector really issued. A run that clears every position passes the first and
 * fails the second; a run that clears none does the reverse. Neither constant survives the pair.
 *
 * <p>Each position is erased with the order recorded beside it, never on its own. A token without its
 * order can no longer be ranked and an order without its token is nothing to resume from, so half an
 * erase would leave behind exactly the record the store's own writer refuses to produce.
 */
@RequiresDocker
class V4DiscardInventedPositionsIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    /** The fixed seam the released build wrote in place of one sampled at the source. */
    private static final String INVENTED_SEAM = "cdc-start-0";

    /** The counter it advanced in place of a watermark the connector issued. */
    private static final String INVENTED_WATERMARK = "w7";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static MongoClient client;

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void aChainTheReleasedBuildMinedLosesEveryPositionItInvented() {
        MongoDatabase database = freshDatabase("v4_invented");
        MongoCollection<Document> chains = SystemCollections.SRS_META.on(database);
        chains.insertOne(chainCarrying(INVENTED_SEAM, INVENTED_WATERMARK, "w3"));

        new V4DiscardInventedPositions().up(database, ChangeSet.Fence.HELD);

        Document stored = chains.find(new Document("_id", "chain")).first();
        assertThat(stored).isNotNull();
        assertThat(stored.get("cdcStartPosition"))
                .as("the seam was a constant, so nothing resumes from it")
                .isNull();
        assertThat(stored.get("snapshotEpoch"))
                .as("the generation the seam belonged to goes with the seam: one of the pair left "
                        + "behind is rows pinned to a generation whose seam is somewhere else")
                .isNull();
        assertThat(stored.get("sourceReadOffset")).isNull();
        assertThat(stored.get("sourceReadEpoch"))
                .as("an order with no token is nothing a read can resume from")
                .isNull();
        assertThat(stored.get("sourceReadSeq")).isNull();
        Document consumer = stored.get("consumerOffsets", Document.class).get("p1", Document.class);
        assertThat(consumer.get("sinkAckedSrcpos")).isNull();
        assertThat(consumer.get("sinkAckedEpoch")).isNull();
        assertThat(consumer.get("sinkAckedSeq")).isNull();
        assertThat(consumer.get("perTableSeq"))
                .as("what the reader had read is not a position and is not this changeset's to touch")
                .isNotNull();
    }

    @Test
    void aChainCarryingPositionsAConnectorIssuedIsLeftExactlyAsItIs() {
        MongoDatabase database = freshDatabase("v4_real");
        MongoCollection<Document> chains = SystemCollections.SRS_META.on(database);
        chains.insertOne(chainCarrying(tokenFor("seam"), tokenFor("read"), tokenFor("acked")));
        Document before = chains.find(new Document("_id", "chain")).first();

        new V4DiscardInventedPositions().up(database, ChangeSet.Fence.HELD);

        assertThat(chains.find(new Document("_id", "chain")).first())
                .as("these are places in a change stream that a source really named; discarding one "
                        + "re-mines every change since it, which is the loss this exists to avoid")
                .isEqualTo(before);
    }

    @Test
    void runningItTwiceIsTheSameAsRunningItOnce() {
        MongoDatabase database = freshDatabase("v4_twice");
        MongoCollection<Document> chains = SystemCollections.SRS_META.on(database);
        chains.insertOne(chainCarrying(INVENTED_SEAM, INVENTED_WATERMARK, "w3"));
        new V4DiscardInventedPositions().up(database, ChangeSet.Fence.HELD);
        Document afterFirst = chains.find(new Document("_id", "chain")).first();

        new V4DiscardInventedPositions().up(database, ChangeSet.Fence.HELD);

        assertThat(chains.find(new Document("_id", "chain")).first()).isEqualTo(afterFirst);
    }

    /** One chain record carrying all three positions, each with the order recorded beside it. */
    private static Document chainCarrying(String seam, String readOffset, String sinkAcked) {
        return new Document("_id", "chain")
                .append("retention", "P1D")
                .append("epoch", 4L)
                .append("cdcStartPosition", seam)
                .append("snapshotEpoch", 4L)
                .append("sourceReadOffset", readOffset)
                .append("sourceReadEpoch", 4L)
                .append("sourceReadSeq", 12L)
                .append("sourceReadAt", 1757000000000L)
                .append("consumerOffsets", new Document("p1", new Document()
                        .append("sinkAckedSrcpos", sinkAcked)
                        .append("sinkAckedEpoch", 4L)
                        .append("sinkAckedSeq", 9L)
                        .append("perTableSeq", new Document("orders", 9L))))
                .append("schemaHistory", java.util.List.of());
    }

    /** A token in the form the connector codec produces: base64 over the serialized offset. */
    private static String tokenFor(String offset) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(offset);
            }
            return Base64.getEncoder().encodeToString(bytes.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static MongoDatabase freshDatabase(String name) {
        if (client == null) {
            client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        }
        MongoDatabase database = client.getDatabase(name);
        database.drop();
        return database;
    }
}
