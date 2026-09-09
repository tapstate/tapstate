package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The changeset that records, on a stored pipeline, the srs switch it reads each source through.
 *
 * <p>What it has to get right is where the value comes from. The pipeline recorded nothing and the
 * author wrote nothing, so the only honest source is the source's own switch -- and taking a default
 * instead would leave a pipeline running the other way round from the one somebody configured, with
 * every face reporting it as healthy. So the switch each case seeds is the one it asserts, and the two
 * cases disagree on it: a run that ignored the source and wrote a constant passes one and fails the
 * other, whichever constant it picked.
 */
@RequiresDocker
class V3RecordedSrsSwitchesIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();

    private static final String SOURCE_WITH_SRS_OFF = """
            version: tapstate/v1
            kind: source
            id: src_off
            connector: mysql
            config: { host: localhost }
            mode: cdc
            tables: [ orders ]
            srs: { enabled: false }
            """;

    private static final String SOURCE_TAKING_THE_DEFAULT = """
            version: tapstate/v1
            kind: source
            id: src_default
            connector: mysql
            config: { host: localhost }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: tgt
            connector: mysql
            config: { host: localhost }
            """;

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
    void takesEachSwitchFromTheSourceTheReferenceNames() {
        MongoDatabase database = freshDatabase("v3_takes");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seed(artifacts, SOURCE_WITH_SRS_OFF);
        seed(artifacts, SOURCE_TAKING_THE_DEFAULT);
        seed(artifacts, TARGET);
        seed(artifacts, pipelineReading("p_off", "src_off"));
        seed(artifacts, pipelineReading("p_default", "src_default"));

        new V3RecordedSrsSwitches().up(database);

        assertThat(recordedSwitchOf(artifacts, "p_off"))
                .as("the source says the replay store is off, so the reference has to say so too")
                .isFalse();
        assertThat(recordedSwitchOf(artifacts, "p_default"))
                .as("a source that writes no srs block is on, and the reference records that reading "
                        + "rather than the absence")
                .isTrue();
    }

    @Test
    void retakesTheContentHashOverWhatItWrote() {
        MongoDatabase database = freshDatabase("v3_hash");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seed(artifacts, SOURCE_WITH_SRS_OFF);
        seed(artifacts, TARGET);
        seed(artifacts, pipelineReading("p_off", "src_off"));
        String before = artifacts.find(new Document("_id", "p_off")).first().getString("contentHash");

        new V3RecordedSrsSwitches().up(database);

        Document stored = artifacts.find(new Document("_id", "p_off")).first();
        assertThat(stored.getString("contentHash"))
                .as("the body moved, so the hash naming it has to move with it")
                .isNotEqualTo(before)
                .isEqualTo(CanonicalHash.of(PARSER.fromTree((Document) stored.get("body"))));
    }

    @Test
    void aPipelineThatAlreadyRecordsItsSwitchesIsLeftExactlyAsItIs() {
        MongoDatabase database = freshDatabase("v3_twice");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seed(artifacts, SOURCE_WITH_SRS_OFF);
        seed(artifacts, TARGET);
        seed(artifacts, pipelineReading("p_off", "src_off"));
        new V3RecordedSrsSwitches().up(database);
        Document afterFirst = artifacts.find(new Document("_id", "p_off")).first();

        new V3RecordedSrsSwitches().up(database);

        assertThat(artifacts.find(new Document("_id", "p_off")).first()).isEqualTo(afterFirst);
    }

    @Test
    void aReferenceNamingASourceTheStoreDoesNotHoldStopsTheRunAndNamesBothHalves() {
        MongoDatabase database = freshDatabase("v3_dangling");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seed(artifacts, SOURCE_WITH_SRS_OFF);
        seed(artifacts, TARGET);
        seed(artifacts, pipelineReading("p_off", "src_off"));
        seed(artifacts, pipelineReading("p_gone", "src_missing"));
        Document before = artifacts.find(new Document("_id", "p_off")).first();

        Throwable thrown = catchThrowable(() -> new V3RecordedSrsSwitches().up(database));

        assertThat(thrown).hasMessageContaining("p_gone").hasMessageContaining("src_missing");
        assertThat(artifacts.find(new Document("_id", "p_off")).first())
                .as("nothing is written when any reference cannot be answered: one version number "
                        + "covers the collection, so a half-done run reads as finished")
                .isEqualTo(before);
    }

    @Test
    void aBodyThisBuildCannotBindIsLeftWhereItIs() {
        MongoDatabase database = freshDatabase("v3_unbindable");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seed(artifacts, SOURCE_WITH_SRS_OFF);
        seed(artifacts, TARGET);
        seed(artifacts, pipelineReading("p_off", "src_off"));
        Document unbindable = new Document("_id", "p_alien").append("kind", "pipeline")
                .append("body", new Document("version", "tapstate/v9").append("kind", "pipeline"))
                .append("contentHash", "whatever-it-was");
        artifacts.insertOne(unbindable);

        new V3RecordedSrsSwitches().up(database);

        assertThat(recordedSwitchOf(artifacts, "p_off"))
                .as("the readable pipeline is still done")
                .isFalse();
        assertThat(artifacts.find(new Document("_id", "p_alien")).first())
                .as("a body this build does not understand is not this changeset's to repair")
                .isEqualTo(unbindable);
    }

    @Test
    void aStoreWithNothingLeftToRecordSaysSoWithoutTouchingIt() {
        MongoDatabase database = freshDatabase("v3_dryrun");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        seed(artifacts, SOURCE_WITH_SRS_OFF);
        seed(artifacts, TARGET);
        seed(artifacts, pipelineReading("p_off", "src_off"));

        assertThat(new V3RecordedSrsSwitches().dryRunSummary(database)).contains("1 pipeline");
        new V3RecordedSrsSwitches().up(database);
        assertThat(new V3RecordedSrsSwitches().dryRunSummary(database)).contains("already records");
    }

    /** A pipeline naming its source bare, which is every pipeline written before a reference carried more. */
    private static String pipelineReading(String id, String sourceId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                serve:
                  from: %s
                  sync:
                    - source: tgt
                """.formatted(id, sourceId, sourceId);
    }

    private static boolean recordedSwitchOf(MongoCollection<Document> artifacts, String id) {
        Document stored = artifacts.find(new Document("_id", id)).first();
        Resource bound = PARSER.fromTree((Document) stored.get("body"));
        SourceRef ref = ((PipelineResource) bound).sources().get(0);
        assertThat(ref)
                .as("the reference still names its source bare, so nothing was recorded on it")
                .isInstanceOf(SourceRef.Spec.class);
        return ((SourceRef.Spec) ref).srs();
    }

    /** One document in the shape a store holds once bodies are structured but references are still bare. */
    private static void seed(MongoCollection<Document> artifacts, String raw) {
        Resource resource = PARSER.parse(raw);
        artifacts.insertOne(new Document("_id", resource.id())
                .append("kind", resource.kind())
                .append("body", new Document(WRITER.tree(resource)))
                .append("contentHash", CanonicalHash.of(resource)));
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
