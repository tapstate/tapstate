package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongoPipelineDraftStoreTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void boundsPersistentUnknownCommitResults(boolean changeDefinition) {
        var attempt = commitAttempt(changeDefinition);
        var unknown = unknownCommit();
        doThrow(unknown, unknown, unknown, new AssertionError("Unexpected fourth commit"))
                .when(attempt.session()).commitTransaction();

        assertThatThrownBy(() -> attempt.write().get())
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.PIPELINE_COMMIT_OUTCOME_UNKNOWN);
                    assertThat(failure.args()).containsEntry("pipeline", "orders");
                });
        verify(attempt.session(), times(3)).commitTransaction();
        verify(attempt.session(), never()).abortTransaction();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retriesAnUnconfirmedCommitWithinTheSameTransaction(boolean changeDefinition) {
        var attempt = commitAttempt(changeDefinition);
        doThrow(unknownCommit()).doNothing().when(attempt.session()).commitTransaction();

        assertThat(attempt.write().get()).isEqualTo(attempt.success());
        verify(attempt.session()).startTransaction();
        verify(attempt.session(), times(2)).commitTransaction();
        verify(attempt.session(), never()).abortTransaction();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void confirmedCommitFailureIsAbortedWithoutRetry(boolean changeDefinition) {
        var attempt = commitAttempt(changeDefinition);
        var failure = new MongoException("Commit failed");
        var cleanupFailure = new MongoException("Abort failed");
        doThrow(failure).when(attempt.session()).commitTransaction();
        doThrow(cleanupFailure).when(attempt.session()).abortTransaction();

        assertThatThrownBy(() -> attempt.write().get())
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
        assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
        verify(attempt.session()).commitTransaction();
        verify(attempt.session()).abortTransaction();
    }

    @Test
    @SuppressWarnings("unchecked")
    void legacyOccupancyChecksOnlyPrimaryIdHeadersAndRejectsUnreadableTombstones() {
        var client = mock(MongoClient.class);
        MongoCollection<Document> drafts = mock(MongoCollection.class, RETURNS_SELF);
        MongoCollection<Document> artifacts = mock(MongoCollection.class, RETURNS_SELF);
        FindIterable<Document> draftQuery = mock(FindIterable.class, RETURNS_SELF);
        FindIterable<Document> artifactQuery = mock(FindIterable.class, RETURNS_SELF);
        var id = new Document("_id", "orders");
        when(artifacts.find(id)).thenReturn(artifactQuery);
        when(drafts.find(id)).thenReturn(draftQuery);
        var store = new MongoPipelineDraftStore(client, drafts, artifacts);
        when(artifactQuery.first()).thenReturn(id, null);
        when(draftQuery.first()).thenReturn(null, id, new Document("deleted", true).append("revision", 1L),
                new Document("deleted", true));

        assertThat(store.idOccupied("orders")).contains(true);
        verify(drafts, never()).find(id);
        assertThat(store.idOccupied("orders")).contains(false);
        assertThat(store.idOccupied("orders")).contains(true);
        assertThat(store.idOccupied("orders")).contains(false);
        assertThatThrownBy(() -> store.idOccupied("orders"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        verify(artifacts, times(5)).withReadPreference(com.mongodb.ReadPreference.primary());
        verify(drafts, times(4)).withReadPreference(com.mongodb.ReadPreference.primary());
        verify(artifactQuery, times(5)).projection(new Document("_id", 1));
        verify(draftQuery, times(4)).projection(new Document("_id", 1).append("deleted", 1).append("revision", 1));
    }

    private record CommitAttempt(ClientSession session, Supplier<PipelineDraftMutation> write,
                                 PipelineDraftMutation success) {}

    private static MongoException unknownCommit() {
        var error = new MongoException("Commit result unavailable");
        error.addLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        return error;
    }

    @SuppressWarnings("unchecked")
    private static CommitAttempt commitAttempt(boolean changeDefinition) {
        MongoClient client = mock(MongoClient.class);
        ClientSession session = mock(ClientSession.class);
        MongoCollection<Document> drafts = mock(MongoCollection.class);
        MongoCollection<Document> artifacts = mock(MongoCollection.class);
        FindIterable<Document> draftQuery = mock(FindIterable.class);
        FindIterable<Document> artifactQuery = mock(FindIterable.class, RETURNS_SELF);
        when(client.startSession()).thenReturn(session);

        PipelineDraft draft = new PipelineDraft("orders", 1, 1, PipelineDraft.Mode.DAG,
                "Orders", "", new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1)),
                null, null, null, null, Instant.EPOCH, Instant.EPOCH, "author");
        when(drafts.find(eq(session), any(Document.class))).thenReturn(draftQuery);
        when(draftQuery.first()).thenReturn(MongoPipelineDraftStore.toDocument(draft));
        when(artifacts.find(eq(session), any(Document.class))).thenReturn(artifactQuery);
        when(artifactQuery.first()).thenReturn(new Document("kind", "pipeline").append("contentHash", "before"));
        when(artifacts.replaceOne(eq(session), any(Document.class), any(Document.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));
        when(drafts.replaceOne(eq(session), any(Document.class), any(Document.class)))
                .thenReturn(UpdateResult.acknowledged(1, 1L, null));

        var artifact = new PipelineResource("orders", null, List.of(SourceRef.bare("crm")), List.of(),
                null, null, null, Map.of());
        var store = new MongoPipelineDraftStore(client, drafts, artifacts);
        String hash = CanonicalHash.of(artifact);
        if (changeDefinition) {
            var change = new PipelineDraft.DefinitionChange("orders", "before", artifact, hash,
                    Map.of(), current -> current);
            return new CommitAttempt(session, () -> store.changeDefinition(change), PipelineDraftMutation.REPLACED);
        }
        var publication = new PipelineDraft.Publication("orders", 1, "before", artifact, hash,
                Instant.EPOCH, "publisher");
        return new CommitAttempt(session, () -> store.publish(publication), PipelineDraftMutation.PUBLISHED);
    }

    @Test
    void roundTripsTheVersionedDraftShapeWithoutCollapsingModePayloads() {
        PipelineDraft draft = new PipelineDraft("orders", 1, 3, PipelineDraft.Mode.DAG,
                "Orders", "draft", new PipelineDraft.Graph(
                        List.of(new PipelineDraft.Node("source", "source", "crm", "orders",
                                Map.of("selected", true), Map.of("label", "Orders"))),
                        List.of(), new PipelineDraft.Viewport(1, 2, 0.75), Map.of("crm", Map.of("preImage", false))), null,
                "hash-1", 2L, "hash-1", Instant.parse("2026-09-21T00:00:00Z"),
                Instant.parse("2026-09-21T01:00:00Z"), "author");

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft));

        assertThat(restored).isEqualTo(draft);
        assertThat(MongoPipelineDraftStore.toDocument(draft)).containsEntry("mode", "dag");
        assertThat(MongoPipelineDraftStore.toDocument(draft)).containsKey("graph").doesNotContainKey("wizard");
    }

    @Test
    void roundTripsDraftNodeConfigWithUnconfiguredNullValues() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("connectorId", null);
        PipelineDraft draft = new PipelineDraft("incomplete", 1, 1, PipelineDraft.Mode.DAG,
                "Incomplete", "", new PipelineDraft.Graph(
                        List.of(new PipelineDraft.Node("target", "target", null, null, config, Map.of())),
                        List.of(), new PipelineDraft.Viewport(0, 0, 1)), null,
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft));

        assertThat(restored.graph().nodes().getFirst().config()).containsKey("connectorId");
        assertThat(restored.graph().nodes().getFirst().config().get("connectorId")).isNull();
    }

    @Test
    void roundTripsWizardRelationsTransformsAndNullableOutput() {
        PipelineDraft draft = new PipelineDraft("orders", 1, 4, PipelineDraft.Mode.WIZARD,
                "Orders", "assembled", null,
                new PipelineDraft.Wizard(
                        new PipelineDraft.Root("orders", "mysql", "orders", List.of("id"),
                                List.of(new PipelineDraft.Transform("normalize-root", "map",
                                        Map.of("customer", "$customer_name", "discard", false, "active", true)))),
                        List.of(new PipelineDraft.Related("items", "orders", "mysql", "order_items",
                                new PipelineDraft.Relation(
                                        List.of(new PipelineDraft.FieldPair("order_id", "id"),
                                                new PipelineDraft.FieldPair("tenant_id", "tenant_id")),
                                        PipelineDraft.Shape.ARRAY, "items", List.of("item_id"), List.of("item_id")),
                                List.of(new PipelineDraft.Transform("keep-items", "filter",
                                        Map.of("expr", "active == true"))))),
                        List.of(new PipelineDraft.Transform("post-map", "map", Map.of("count", 1))), null,
                        Map.of("mysql", Map.of("preImage", false))),
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft));

        assertThat(restored).isEqualTo(draft);
        assertThat(restored.wizard().output()).isNull();
        assertThat(restored.wizard().related().getFirst().relation().on())
                .containsExactly(new PipelineDraft.FieldPair("order_id", "id"),
                        new PipelineDraft.FieldPair("tenant_id", "tenant_id"));
        assertThat(restored.wizard().root().preTransforms().getFirst().fields())
                .containsEntry("customer", "$customer_name").containsEntry("discard", false);
    }

    @Test
    void roundTripsRollupFieldsOnWizardRelations() {
        Map<String, Object> rollup = new LinkedHashMap<>();
        rollup.put("item_count", Map.of("count", true));
        rollup.put("latest", Map.of("last", Map.of("value", "$sku", "by", List.of("$created_at", "$id"))));
        PipelineDraft draft = new PipelineDraft("orders", 1, 4, PipelineDraft.Mode.WIZARD,
                "Orders", "", null,
                new PipelineDraft.Wizard(
                        new PipelineDraft.Root("orders", "mysql", "orders", List.of("id"), List.of()),
                        List.of(new PipelineDraft.Related("items", "orders", "mysql", "order_items",
                                new PipelineDraft.Relation(List.of(new PipelineDraft.FieldPair("order_id", "id")),
                                        PipelineDraft.Shape.ROLLUP, null, List.of(), List.of("item_id"), rollup, false),
                                List.of())),
                        List.of(), null),
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft));

        assertThat(restored).isEqualTo(draft);
        assertThat(restored.wizard().related().getFirst().relation().rollup()).containsOnlyKeys("item_count", "latest");
        assertThat(restored.wizard().related().getFirst().relation().trackKeyChanges()).isFalse();
    }

    @Test
    void roundTripsAnIncompleteWizardWithoutARoot() {
        PipelineDraft draft = new PipelineDraft("incomplete", 1, 1, PipelineDraft.Mode.WIZARD,
                "Incomplete", "", null, new PipelineDraft.Wizard(null, List.of(), List.of(), null),
                null, null, null, Instant.EPOCH, Instant.EPOCH, "author");

        assertThat(MongoPipelineDraftStore.fromDocument(MongoPipelineDraftStore.toDocument(draft)))
                .isEqualTo(draft);
    }

    @Test
    void readsDocumentMapsAndMissingLegacyFieldsWithoutLosingDefaults() {
        Document legacy = new Document("_id", "legacy-wizard")
                .append("mode", "wizard")
                .append("revision", 2L)
                .append("name", "Legacy")
                .append("updatedBy", "migration")
                .append("wizard", new Document("root", new Document("id", "root")
                        .append("sourceId", "mysql").append("table", "orders")
                        .append("key", "not-a-list")
                        .append("preTransforms", List.of(Map.of("id", "rename", "type", "map",
                                "fields", Map.of("customer", "$name")))))
                        .append("related", List.of(Map.of(
                                "id", "items", "parentId", "root", "sourceId", "mysql", "table", "order_items",
                                "relation", new Document("on", List.of(Map.of("childField", "order_id", "parentField", "id")))
                                        .append("shape", "array").append("path", "items")
                                        .append("key", "not-a-list").append("arrayKey", List.of("item_id")),
                                "preTransforms", "not-a-list")))
                        .append("transforms", "not-a-list")
                        .append("output", null));

        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(legacy);

        assertThat(restored.schemaVersion()).isEqualTo(PipelineDraft.CURRENT_SCHEMA_VERSION);
        assertThat(restored.createdAt()).isEqualTo(Instant.EPOCH);
        assertThat(restored.updatedAt()).isEqualTo(Instant.EPOCH);
        assertThat(restored.wizard().root().key()).isEmpty();
        assertThat(restored.wizard().root().preTransforms()).hasSize(1);
        assertThat(restored.wizard().related().getFirst().relation().key()).isEmpty();
        assertThat(restored.wizard().related().getFirst().preTransforms()).isEmpty();
        assertThat(restored.wizard().transforms()).isEmpty();
        assertThat(restored.wizard().output()).isNull();
    }

    @Test
    void migratesTheOriginalUnversionedEmptyDraftToSchemaOne() {
        Document legacy = new Document("_id", "orders")
                .append("revision", 1L)
                .append("name", "Orders")
                .append("description", "")
                .append("createdAt", java.util.Date.from(Instant.parse("2026-09-21T00:00:00Z")))
                .append("updatedAt", java.util.Date.from(Instant.parse("2026-09-21T00:00:00Z")))
                .append("updatedBy", "migration");

        Document migrated = PipelineDraftMigrations.migrate(legacy);
        PipelineDraft restored = MongoPipelineDraftStore.fromDocument(migrated);

        assertThat(migrated.getInteger("schemaVersion")).isEqualTo(1);
        assertThat(restored.mode()).isEqualTo(PipelineDraft.Mode.DAG);
        assertThat(restored.graph().nodes()).isEmpty();
        assertThat(restored.graph().viewport().zoom()).isEqualTo(1D);
    }
}
