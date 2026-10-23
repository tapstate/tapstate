package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real configuration-only encrypted editing, publication and metadata-only wire projection witnesses. */
@RequiresDocker
class MongoConfigurationDraftStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Instant NOW = Instant.parse("2026-10-17T00:00:00Z");
    private static final DslParser PARSER = new DslParser();

    @Test
    void privateSummaryAndBodyAreEncryptedAndBsonValuesSurviveRestartWithoutCache() {
        withConfiguration((domain, database) -> {
            var settings = Map.<String, Object>of("password", "body-secret", "long", 5L, "decimal", new BigDecimal("12.3400"),
                    "binary", new byte[]{1, 2, 3}, "nested", Map.of("$numberLong", "7"));
            var graph = new PipelineDraft.Graph(List.of(new PipelineDraft.Node("node", "transform", null, null, settings,
                    Map.of("hint", "metadata-secret"))), List.of(), new PipelineDraft.Viewport(0, 0, 1));
            var draft = new PipelineDraft("orders", 1, 1, PipelineDraft.Mode.DAG, "summary-secret", "description-secret", graph,
                    null, null, null, null, NOW, NOW, "author");
            assertThat(domain.drafts().create(draft)).isEqualTo(PipelineDraftMutation.CREATED);
            var raw = SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).find().first();
            assertThat(raw.toJson()).doesNotContain("body-secret", "metadata-secret", "summary-secret", "description-secret", "$numberLong");
            assertThat(raw.keySet()).containsExactlyInAnyOrder("_id", "formatVersion", "revision", "deleted", "updatedAt", "summary", "body");
            assertThat(revision(database)).isOne();
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                var restored = reopened.drafts().get("orders").orElseThrow();
                var values = restored.graph().nodes().getFirst().config();
                assertThat(values).containsEntry("password", "body-secret").containsEntry("long", 5L)
                        .containsEntry("decimal", new BigDecimal("12.3400")).containsEntry("nested", Map.of("$numberLong", "7"));
                assertThat((byte[]) values.get("binary")).containsExactly((byte) 1, (byte) 2, (byte) 3);
                assertThat(reopened.drafts().listSummaries()).singleElement().satisfies(summary -> assertThat(summary.name()).isEqualTo("summary-secret"));
            }
        });
    }

    @Test
    void summariesReallyProjectOutTheEncryptedBodyOnTheMongoWire() {
        withConfiguration((domain, database) -> {
            domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null));
            List<BsonDocument> commands = new CopyOnWriteArrayList<>();
            CommandListener witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("find")) commands.add(event.getCommand().clone());
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(witness).build())) {
                var ciphers = SourceConfigCipherProvider.fixed(cipher(database));
                var target = observed.getDatabase(database.getName());
                var store = new MongoConfigurationDraftStore(observed, target, ciphers, new MongoConfigurationArtifactStore(observed, target, ciphers));
                assertThat(store.listSummaries(0, 1)).hasSize(1);
                assertThat(commands).hasSize(1);
                var projection = commands.getFirst().getDocument("projection");
                assertThat(projection.keySet()).containsExactlyInAnyOrder("_id", "formatVersion", "revision", "deleted", "updatedAt", "summary");
                assertThat(projection.getInt32("summary").getValue()).isOne();
                assertThat(projection.containsKey("body")).isFalse();
            }
        });
    }

    @Test
    void autosaveRebaseDeleteAndRecreationKeepDraftCasWithoutAdvancingPublishedConfiguration() {
        withConfiguration((domain, database) -> {
            var store = domain.drafts();
            assertThat(store.create(draft("orders", 1, PipelineDraft.Mode.WIZARD, "old", 1L, "published"))).isEqualTo(PipelineDraftMutation.CREATED);
            assertThat(store.create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null))).isEqualTo(PipelineDraftMutation.ALREADY_EXISTS);
            assertThat(store.replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG, "old", 1L, "published")))
                    .isEqualTo(PipelineDraftMutation.MODE_CONFLICT);
            var replacement = draft("orders", 2, PipelineDraft.Mode.WIZARD, "new", 1L, "published");
            assertThat(store.replace("orders", 1, replacement)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.rebase("orders", 1, "old", replacement)).isEqualTo(PipelineDraftMutation.REPLACED);
            assertThat(store.delete("orders", 1)).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.delete("orders", 2)).isEqualTo(PipelineDraftMutation.DELETED);
            assertThat(SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).find().first().keySet())
                    .containsExactlyInAnyOrder("_id", "formatVersion", "revision", "deleted", "updatedAt");
            assertThat(store.get("orders")).isEmpty();
            assertThat(store.create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null))).isEqualTo(PipelineDraftMutation.CREATED);
            assertThat(store.get("orders").orElseThrow().revision()).isEqualTo(3);
            assertThat(store.replace("orders", 2, draft("orders", 3, PipelineDraft.Mode.DAG, null, null, null)))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(store.publish(publication("orders", 2, null, Map.of()))).isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(revision(database)).isOne();
        });
    }

    @Test
    void publishingIncludesArtifactStableStateNotesAndPublicationMarkersInOneConfigurationTransaction() {
        withConfiguration((domain, database) -> {
            var source = source("src", "secret"); domain.artifacts().create(source);
            domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null));
            long before = revision(database);
            var publication = publication("orders", 1, null, Map.of("src", CanonicalHash.of(source)));
            assertThat(domain.drafts().publish(publication)).isEqualTo(PipelineDraftMutation.PUBLISHED);
            assertThat(domain.artifacts().get("orders")).contains(publication.artifact());
            var saved = domain.drafts().get("orders").orElseThrow();
            assertThat(saved.baseArtifactHash()).isEqualTo(publication.publishedArtifactHash());
            assertThat(saved.publishedDraftRevision()).isOne();
            assertThat(revision(database)).isEqualTo(before + 1);
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(database).find(new Document("_id", "orders")).first())
                    .containsEntry("stateGeneration", 1L);
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(database).find(new Document("_id.namespace", "pdk.state.orders.capture.src")).first()).isNotNull();
            assertThat(domain.drafts().replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG, null, null, null)))
                    .isEqualTo(PipelineDraftMutation.REVISION_CONFLICT);
            assertThat(domain.drafts().replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG,
                    saved.baseArtifactHash(), saved.publishedDraftRevision(), saved.publishedArtifactHash())))
                    .isEqualTo(PipelineDraftMutation.REPLACED);
            assertThat(revision(database)).isEqualTo(before + 1);
        });
    }

    @Test
    void failedFinalDraftWriteRollsBackArtifactScopeMappingVersionAndKeyFence() {
        withConfiguration((domain, database) -> {
            domain.artifacts().create(source("src", "secret"));
            domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null));
            var second = draft("another", 1, PipelineDraft.Mode.DAG, null, null, null);
            second = new PipelineDraft(second.pipelineId(), 1, 1, second.mode(), second.name(), second.description(), second.graph(), null,
                    null, null, null, NOW, NOW.plusSeconds(1), "author");
            domain.drafts().create(second);
            SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).createIndex(new Document("updatedAt", 1),
                    new IndexOptions().unique(true).name("forced_unique_updated_at"));
            long version = revision(database);
            var key = SystemCollections.CLUSTER_KEYS.on(database).find().first();
            assertThatThrownBy(() -> domain.drafts().publish(publication("orders", 1, null, Map.of())))
                    .isInstanceOf(TapstateException.class);
            assertThat(domain.artifacts().get("orders")).isEmpty();
            assertThat(SystemCollections.OPERATOR_GENERATIONS.on(database).find(new Document("_id", "orders")).first()).isNull();
            assertThat(SystemCollections.PDK_NOTE_SCOPES.on(database).find(new Document("_id.namespace", "pdk.state.orders.capture.src")).first()).isNull();
            assertThat(domain.drafts().get("orders").orElseThrow().publishedDraftRevision()).isNull();
            assertThat(revision(database)).isEqualTo(version);
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).find().first()).isEqualTo(key);
        });
    }

    @Test
    void staleDependenciesAndEveryOtherArtifactKindRefusePublicationWithoutSideEffects() {
        withConfiguration((domain, database) -> {
            var original = source("src", "old"); domain.artifacts().create(original);
            domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null));
            domain.artifacts().replace("src", CanonicalHash.of(original), source("src", "new"));
            long before = revision(database);
            assertThat(domain.drafts().publish(publication("orders", 1, null, Map.of("src", CanonicalHash.of(original)))))
                    .isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
            assertThat(revision(database)).isEqualTo(before);
            for (String kind : List.of("source", "transform", "view", "serve")) {
                String id = "taken_" + kind;
                var existing = kind.equals("source") ? source(id, "secret") : PARSER.parse("version: tapstate/v1\nkind: " + kind + "\nid: " + id + "\n"
                        + (kind.equals("transform") ? "type: filter\nexpr: 'true'\n" : kind.equals("view") ? "primary_key: id\n" : ""));
                domain.artifacts().create(existing);
                domain.drafts().create(draft(id, 1, PipelineDraft.Mode.DAG, null, null, null));
                long version = revision(database);
                assertThat(domain.drafts().publish(publication(id, 1, null, Map.of()))).isEqualTo(PipelineDraftMutation.ARTIFACT_CONFLICT);
                assertThat(domain.artifacts().get(id)).contains(existing);
                assertThat(revision(database)).isEqualTo(version);
            }
        });
    }

    @Test
    void twoClientsKeepOneDraftCreateAndOneExpectedRevisionWinner() {
        withConfiguration((domain, database) -> {
            try (var other = MongoConfigurationDomain.open(settings(database), "cluster-a"); var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); start.await(10, TimeUnit.SECONDS); return domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null)); });
                var b = executor.submit(() -> { ready.countDown(); start.await(10, TimeUnit.SECONDS); return other.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null)); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS)))
                        .containsExactlyInAnyOrder(PipelineDraftMutation.CREATED, PipelineDraftMutation.ALREADY_EXISTS);
                var x = executor.submit(() -> domain.drafts().replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG, null, null, null)));
                var y = executor.submit(() -> other.drafts().replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG, null, null, null)));
                assertThat(List.of(x.get(20, TimeUnit.SECONDS), y.get(20, TimeUnit.SECONDS)))
                        .containsExactlyInAnyOrder(PipelineDraftMutation.REPLACED, PipelineDraftMutation.REVISION_CONFLICT);
                assertThat(revision(database)).isOne();
            }
        });
    }

    @Test
    void bodySwapsAndPlaintextAreRefusedWithoutRepairWhileSummariesStayIndependent() {
        withConfiguration((domain, database) -> {
            domain.drafts().create(draft("a", 1, PipelineDraft.Mode.DAG, null, null, null));
            domain.drafts().create(draft("b", 1, PipelineDraft.Mode.DAG, null, null, null));
            var rows = SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database);
            String body = rows.find(new Document("_id", "a")).first().getString("body");
            rows.updateOne(new Document("_id", "b"), new Document("$set", new Document("body", body)));
            assertThatThrownBy(() -> domain.drafts().get("b")).satisfies(MongoConfigurationDraftStoreIT::unreadable);
            assertThat(domain.drafts().listSummaries()).hasSize(2);
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(database), "cluster-a"))
                    .satisfies(MongoConfigurationDraftStoreIT::unreadable);
            rows.replaceOne(new Document("_id", "b"), MongoPipelineDraftStore.toDocument(draft("b", 1, PipelineDraft.Mode.DAG, null, null, null)));
            assertThatThrownBy(() -> domain.drafts().get("b")).satisfies(MongoConfigurationDraftStoreIT::unreadable);
            assertThat(rows.find(new Document("_id", "b")).first()).containsKey("graph");
        });
    }

    @Test
    void authenticatedMissingGraphFieldsNeverReceiveLegacyEmptyDefaults() {
        withConfiguration((domain, database) -> {
            domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null));
            var raw = new org.bson.RawBsonDocument(new Document("graph", new Document("viewport", new Document("x", 0D).append("y", 0D).append("zoom", 1D))), new org.bson.codecs.DocumentCodec());
            byte[] bytes = Arrays.copyOfRange(raw.getBackingArray(), raw.getByteOffset(), raw.getByteOffset() + raw.getByteLength());
            String envelope = cipher(database).encryptBytes(JsonWriter.write(List.of("pipeline-draft", "orders", 1L, "body")), "pipeline-draft", bytes);
            SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).updateOne(new Document("_id", "orders"), new Document("$set", new Document("body", envelope)));
            assertThatThrownBy(() -> domain.drafts().get("orders")).satisfies(MongoConfigurationDraftStoreIT::unreadable);
            assertThat(domain.drafts().list()).isEmpty();
            assertThat(domain.drafts().listSummaries()).hasSize(1);
        });
    }

    @Test
    void wizardRelatedTransformsAndOutputKeepTheirTypedPrivateValues() {
        withConfiguration((domain, database) -> {
            var fields = Map.<String, Object>of("nested", Map.of("$date", "literal"), "amount", new BigDecimal("1.2300"));
            var transform = new PipelineDraft.Transform("t", "fields", fields);
            var wizard = new PipelineDraft.Wizard(new PipelineDraft.Root("root", "src", "orders", List.of("id"), List.of(transform)),
                    List.of(new PipelineDraft.Related("child", "root", "src", "lines",
                            new PipelineDraft.Relation(List.of(new PipelineDraft.FieldPair("order_id", "id")), PipelineDraft.Shape.ARRAY,
                                    "lines", List.of("id"), List.of("id")), List.of(transform))),
                    List.of(transform), new PipelineDraft.Output("target", fields));
            var draft = new PipelineDraft("wizard", 1, 1, PipelineDraft.Mode.WIZARD, "private-name", "", null, wizard,
                    null, null, null, NOW, NOW, "author");
            domain.drafts().create(draft);
            assertThat(domain.drafts().get("wizard").orElseThrow()).isEqualTo(draft);
            assertThat(SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).find().first().toJson())
                    .doesNotContain("private-name", "literal", "order_id");
        });
    }

    @Test
    void requestCaptureOwnsNestedMapsAndBinaryBeforeCallingTheKeyProvider() {
        withConfiguration((domain, database) -> {
            byte[] callerBytes = new byte[]{1, 2};
            var callerMap = new java.util.LinkedHashMap<String, Object>(); callerMap.put("value", "original-secret");
            var graph = new PipelineDraft.Graph(List.of(new PipelineDraft.Node("n", "transform", null, null,
                    Map.of("nested", callerMap, "bytes", callerBytes), Map.of())), List.of(), new PipelineDraft.Viewport(0, 0, 1));
            var draft = new PipelineDraft("snapshot", 1, 1, PipelineDraft.Mode.DAG, "", "", graph, null, null, null, null, NOW, NOW, "author");
            SourceConfigCipher original = cipher(database);
            SourceConfigCipherProvider provider = new SourceConfigCipherProvider() {
                @Override public SourceConfigCipher current() { return original; }
                @Override public SourceConfigCipher refresh() { callerBytes[0] = 9; callerMap.put("value", "changed-secret"); return original; }
            };
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var ownDatabase = client.getDatabase(database.getName());
                var store = new MongoConfigurationDraftStore(client, ownDatabase, provider,
                        new MongoConfigurationArtifactStore(client, ownDatabase, provider));
                store.create(draft);
                assertThat(callerBytes[0]).isEqualTo((byte) 9);
                var config = store.get("snapshot").orElseThrow().graph().nodes().getFirst().config();
                assertThat(config).containsEntry("nested", Map.of("value", "original-secret"));
                assertThat((byte[]) config.get("bytes")).containsExactly((byte) 1, (byte) 2);
            }
        });
    }

    @Test
    void unsupportedValuesAndMissingKeyNeverCommitOrRecreateNecessaryConfiguration() {
        withConfiguration((domain, database) -> {
            var graph = new PipelineDraft.Graph(List.of(new PipelineDraft.Node("n", "transform", null, null,
                    Map.of("secret", new BigDecimal("123456789012345678901234567890123456789")), Map.of())), List.of(), new PipelineDraft.Viewport(0, 0, 1));
            var invalid = new PipelineDraft("bad", 1, 1, PipelineDraft.Mode.DAG, "", "", graph, null, null, null, null, NOW, NOW, "author");
            assertThatThrownBy(() -> domain.drafts().create(invalid)).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(IoError.DOCUMENT_INVALID); assertThat(error.getCause()).isNull();
                assertThat(error.getMessage()).doesNotContain("123456789012345678901234567890123456789");
            });
            assertThat(domain.drafts().get("bad")).isEmpty();
            domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null));
            var before = SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).find().first();
            SystemCollections.CLUSTER_KEYS.on(database).deleteMany(new Document());
            assertThatThrownBy(() -> domain.drafts().replace("orders", 1, draft("orders", 2, PipelineDraft.Mode.DAG, null, null, null)))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
            assertThatThrownBy(() -> domain.drafts().delete("orders", 1)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).find().first()).isEqualTo(before);
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isZero();
            assertThat(revision(database)).isOne();
        });
    }

    @Test
    void ordinaryOpenNeverRecreatesADeletedDraftIndex() {
        withConfiguration((domain, database) -> {
            SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database).dropIndex("pipeline_drafts_revision");
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(database), "cluster-a"))
                    .satisfies(MongoConfigurationDraftStoreIT::unreadable);
            assertThat(IndexEnsure.existing(SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database),
                    SystemCollections.CONFIG_PIPELINE_DRAFTS.indexes().getFirst())).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"ttl", "extra-ttl", "partial", "sparse", "hidden"})
    void unsafeDraftIndexesAreRefusedWithoutRepair(String option) {
        withConfiguration((domain, database) -> {
            var collection = SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database);
            String name = option.equals("extra-ttl") ? "extra_unsafe_ttl" : "pipeline_drafts_updated_at";
            if (!option.equals("extra-ttl")) collection.dropIndex(name);
            var options = new IndexOptions().name(name);
            switch (option) {
                case "ttl", "extra-ttl" -> options.expireAfter(1_000_000_000L, TimeUnit.SECONDS);
                case "partial" -> options.partialFilterExpression(new Document("deleted", false));
                case "sparse" -> options.sparse(true);
                case "hidden" -> options.hidden(true);
                default -> throw new AssertionError(option);
            }
            collection.createIndex(new Document("updatedAt", option.equals("extra-ttl") ? -1 : 1), options);
            var before = collection.listIndexes().into(new java.util.ArrayList<>());
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(database), "cluster-a"))
                    .satisfies(MongoConfigurationDraftStoreIT::unreadable);
            assertThat(collection.listIndexes().into(new java.util.ArrayList<>())).isEqualTo(before);
        });
    }

    @Test
    void anIncompleteFullRowCannotBeMistakenForASummaryProjection() {
        withConfiguration((domain, database) -> {
            domain.drafts().create(draft("orders", 1, PipelineDraft.Mode.DAG, null, null, null));
            var rows = SystemCollections.CONFIG_PIPELINE_DRAFTS.on(database);
            rows.updateOne(new Document("_id", "orders"), new Document("$unset", new Document("body", "")));
            var before = rows.find().first();
            assertThat(domain.drafts().listSummaries()).hasSize(1);
            assertThatThrownBy(() -> domain.drafts().replace("orders", 1,
                    draft("orders", 2, PipelineDraft.Mode.DAG, null, null, null)))
                    .satisfies(MongoConfigurationDraftStoreIT::unreadable);
            assertThatThrownBy(() -> domain.drafts().delete("orders", 1)).satisfies(MongoConfigurationDraftStoreIT::unreadable);
            assertThat(rows.find().first()).isEqualTo(before);
            assertThat(revision(database)).isOne();
        });
    }

    private static PipelineDraft draft(String id, long revision, PipelineDraft.Mode mode, String base, Long publishedRevision, String publishedHash) {
        return new PipelineDraft(id, 1, revision, mode, id, "", mode == PipelineDraft.Mode.DAG ? new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1)) : null,
                mode == PipelineDraft.Mode.WIZARD ? new PipelineDraft.Wizard(new PipelineDraft.Root(id, "src", "orders", List.of(), List.of()), List.of(), List.of(), null) : null,
                base, publishedRevision, publishedHash, NOW, NOW, "author");
    }
    private static PipelineDraft.Publication publication(String id, long revision, String expected, Map<String, String> dependencies) {
        var pipeline = (PipelineResource) PARSER.parse("version: tapstate/v1\nkind: pipeline\nid: " + id + "\nsource: src\n");
        return new PipelineDraft.Publication(id, revision, expected, pipeline, CanonicalHash.of(pipeline), NOW.plusSeconds(1), "publisher", dependencies);
    }
    private static SourceResource source(String id, String secret) {
        return (SourceResource) PARSER.parse("version: tapstate/v1\nkind: source\nid: " + id + "\nconnector: mongodb\nconfig: {password: " + secret + "}\n");
    }
    private static SourceConfigCipher cipher(MongoDatabase database) {
        return new SourceConfigCipher(Base64.getUrlDecoder().decode(SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material")));
    }
    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.getCause()).isNull();
            assertThat(error.getMessage()).doesNotContain("body-secret", "summary-secret");
        });
    }
    private static long revision(MongoDatabase database) { return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision"); }
    private static MongoConnectionSettings settings(MongoDatabase database) {
        return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
    }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("config_drafts_" + UUID.randomUUID().toString().replace("-", ""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(database), "cluster-a", "installation-a", "setup-a", 1)) {
                domain.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP,
                        new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache.invalid/cache", "cache", null), source("views", "secret"));
                scenario.run(domain, database);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, MongoDatabase database) throws Exception; }
}
