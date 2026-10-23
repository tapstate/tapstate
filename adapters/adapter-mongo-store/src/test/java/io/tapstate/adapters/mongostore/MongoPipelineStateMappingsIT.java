package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Stable physical state is created with configuration, never inferred from a new process or missing row. */
@RequiresDocker
class MongoPipelineStateMappingsIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void pipelineCreationPublishesItsInitialMappingAndOrdinaryReplacementPreservesIt() {
        withConfiguration(f -> {
            Resource pipeline = pipeline("flow");
            assertThat(f.domain.artifacts().create(pipeline)).isEqualTo(ArtifactMutation.CREATED);
            var original = f.domain.pipelineStates().read("flow").orElseThrow();
            assertThat(original).isEqualTo(new PipelineStateMapping("flow", 1, PipelineStateMapping.Phase.ACTIVE, null));
            assertThat(f.domain.artifacts().replace("flow", CanonicalHash.of(pipeline), pipeline)).isEqualTo(ArtifactMutation.REPLACED);
            assertThat(f.domain.pipelineStates().read("flow")).contains(original);
            assertThat(f.mapping().countDocuments()).isEqualTo(1);
            assertThat(f.revision()).isEqualTo(2);
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a")) {
                assertThat(reopened.pipelineStates().read("flow")).contains(original);
            }
            assertThat(f.domain.pipelineStates().read("unknown")).isEmpty();
            assertThat(f.mapping().countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void aSiblingRefusalRollsBackTheNewPipelineMappingAndRevisionTogether() {
        withConfiguration(f -> {
            f.domain.artifacts().create(source("occupied"));
            var result = f.domain.artifacts().writeAll(List.of(ArtifactWrite.createOnly(pipeline("fresh")),
                    ArtifactWrite.createOnly(source("occupied"))));
            assertThat(result.refusal()).isEqualTo(ArtifactMutation.ALREADY_EXISTS);
            assertThat(f.domain.artifacts().get("fresh")).isEmpty();
            assertThat(f.domain.pipelineStates().read("fresh")).isEmpty();
            assertThat(f.mapping().countDocuments()).isZero();
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    @Test
    void aMappingConstraintFaultDoesNotLeaveSiblingResourcesOrASecondPipeline() {
        withConfiguration(f -> {
            f.domain.artifacts().create(pipeline("existing"));
            f.mapping().createIndex(new Document("stateGeneration", 1), new IndexOptions().unique(true));
            assertThatThrownBy(() -> f.domain.artifacts().saveAll(List.of(source("sibling"), pipeline("fresh"))))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(f.domain.artifacts().get("sibling")).isEmpty();
            assertThat(f.domain.artifacts().get("fresh")).isEmpty();
            assertThat(f.mapping().countDocuments()).isEqualTo(1);
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    @Test
    void independentConfigurationClientsHaveOneCreationAndOneInitialGeneration() {
        withConfiguration(f -> {
            try (var other = MongoConfigurationDomain.open(f.settings, "cluster-a");
                    var executor = Executors.newFixedThreadPool(2)) {
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                var one = executor.submit(() -> create(f.domain, ready, start));
                var two = executor.submit(() -> create(other, ready, start));
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                assertThat(List.of(one.get(20, TimeUnit.SECONDS), two.get(20, TimeUnit.SECONDS)))
                        .containsExactlyInAnyOrder(ArtifactMutation.CREATED, ArtifactMutation.ALREADY_EXISTS);
                assertThat(f.mapping().countDocuments()).isEqualTo(1);
                assertThat(f.domain.pipelineStates().read("flow")).isEqualTo(other.pipelineStates().read("flow"));
                assertThat(f.revision()).isEqualTo(1);
            }
        });
    }

    @Test
    void anOrdinaryWriterCannotRemoveOrChangeTheKindOfAnUnretiredPipeline() {
        withConfiguration(f -> {
            Resource original = pipeline("flow");
            f.domain.artifacts().create(original);
            assertThatThrownBy(() -> f.domain.artifacts().delete("flow", CanonicalHash.of(original)))
                    .satisfies(MongoPipelineStateMappingsIT::retirementRequired);
            assertThatThrownBy(() -> f.domain.artifacts().replace("flow", CanonicalHash.of(original), source("flow")))
                    .satisfies(MongoPipelineStateMappingsIT::retirementRequired);
            assertThatThrownBy(() -> f.domain.artifacts().saveAll(List.of(source("sibling"), source("flow"))))
                    .satisfies(MongoPipelineStateMappingsIT::retirementRequired);
            assertThat(f.domain.artifacts().get("flow")).contains(original);
            assertThat(f.domain.artifacts().get("sibling")).isEmpty();
            assertThat(f.domain.pipelineStates().read("flow")).get().extracting(PipelineStateMapping::stateGeneration).isEqualTo(1L);
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    @Test
    void missingOrMalformedMappingsRefuseReadsAndReplacementWithoutRepair() {
        for (Object generation : List.of("missing", "1", 0L, -1L)) {
            withConfiguration(f -> {
                Resource original = pipeline("flow");
                f.domain.artifacts().create(original);
                if (generation.equals("missing")) f.mapping().deleteMany(new Document());
                else f.mapping().updateOne(new Document(), new Document("$set", new Document("stateGeneration", generation)));
                assertThatThrownBy(() -> f.domain.pipelineStates().read("flow")).satisfies(MongoPipelineStateMappingsIT::unreadable);
                assertThatThrownBy(() -> f.domain.artifacts().replace("flow", CanonicalHash.of(original), original))
                        .satisfies(MongoPipelineStateMappingsIT::unreadable);
                assertThat(f.revision()).isEqualTo(1);
                if (generation.equals("missing")) assertThat(f.mapping().countDocuments()).isZero();
                else assertThat(f.mapping().find().first()).containsEntry("stateGeneration", generation);
            });
        }
    }

    @Test
    void aRetainedIdentityCannotBeResetByRecreatingAMissingArtifact() {
        withConfiguration(f -> {
            f.domain.artifacts().create(pipeline("flow"));
            SystemCollections.PIPELINES.on(f.database).deleteMany(new Document());
            assertThatThrownBy(() -> f.domain.artifacts().create(pipeline("flow")))
                    .satisfies(MongoPipelineStateMappingsIT::retirementRequired);
            assertThatThrownBy(() -> f.domain.pipelineStates().read("flow")).satisfies(MongoPipelineStateMappingsIT::unreadable);
            assertThat(f.domain.artifacts().get("flow")).isEmpty();
            assertThat(f.mapping().find().first()).containsEntry("stateGeneration", 1L);
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    @Test
    void aPendingRetirementCannotBeOverwrittenByOrdinaryAuthoring() {
        withConfiguration(f -> {
            Resource original = pipeline("flow");
            f.domain.artifacts().create(original);
            f.mapping().updateOne(new Document(), new Document("$set", new Document("phase", "RETIRING")
                    .append("operationId", "delete-a")));
            assertThat(f.domain.pipelineStates().read("flow")).get().extracting(PipelineStateMapping::phase)
                    .isEqualTo(PipelineStateMapping.Phase.RETIRING);
            assertThatThrownBy(() -> f.domain.artifacts().replace("flow", CanonicalHash.of(original), original))
                    .satisfies(MongoPipelineStateMappingsIT::retirementRequired);
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    @Test
    void aSourceMayBecomeANewPipelineWithoutReusingAnyOldStateIdentity() {
        withConfiguration(f -> {
            Resource original = source("flow");
            f.domain.artifacts().create(original);
            assertThat(f.domain.artifacts().replace("flow", CanonicalHash.of(original), pipeline("flow")))
                    .isEqualTo(ArtifactMutation.REPLACED);
            assertThat(f.domain.pipelineStates().read("flow")).get().extracting(PipelineStateMapping::stateGeneration).isEqualTo(1L);
            assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isZero();
            assertThat(f.revision()).isEqualTo(2);
        });
    }

    @Test
    void anUnretiredPipelineIdIsNotAvailableForANewSourceEither() {
        withConfiguration(f -> {
            f.domain.artifacts().create(pipeline("flow"));
            SystemCollections.PIPELINES.on(f.database).deleteMany(new Document());
            assertThatThrownBy(() -> f.domain.artifacts().create(source("flow")))
                    .satisfies(MongoPipelineStateMappingsIT::retirementRequired);
            assertThat(f.domain.artifacts().get("flow")).isEmpty();
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    private static ArtifactMutation create(MongoConfigurationDomain domain, CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
        return domain.artifacts().create(pipeline("flow"));
    }
    private static Resource pipeline(String id) {
        return new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: " + id + "\nsource: []\n");
    }
    private static Resource source(String id) {
        return new DslParser().parse("version: tapstate/v1\nkind: source\nid: " + id + "\nconnector: mongodb\nconfig: {}\n");
    }
    private static void retirementRequired(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(StoreError.PIPELINE_RETIREMENT_REQUIRED));
    }
    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static void withConfiguration(Scenario scenario) {
        String name = "pipeline_mapping_" + UUID.randomUUID().toString().replace("-", "");
        try (MongoClient admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = admin.getDatabase(name);
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(name), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1)) {
                scenario.run(new Fixture(database, settings, domain));
            } finally { database.drop(); }
        } catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoDatabase database, MongoConnectionSettings settings, MongoConfigurationDomain domain) {
        com.mongodb.client.MongoCollection<Document> mapping() { return SystemCollections.OPERATOR_GENERATIONS.on(database); }
        long revision() { return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision"); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
