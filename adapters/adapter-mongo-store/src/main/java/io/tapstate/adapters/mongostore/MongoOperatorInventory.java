package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.PipelineIntentScope;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Exact operator locations and first-shape facts retained separately from state and shape payloads. */
final class MongoOperatorInventory {
    private static final Set<String> FIELDS = Set.of("_id", "formatVersion", "preparationId", "revision", "shapes", "placements");
    private MongoOperatorInventory() {}

    /** Only the original checkpoint seed transaction may allocate a missing inventory root. */
    static void prepareInitial(ClientSession session, MongoDatabase database, PipelineIntentScope scope, String preparationId) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("operator inventory birth requires its checkpoint transaction");
        if (!UUID.fromString(preparationId).toString().equals(preparationId)) throw new IllegalArgumentException("invalid preparation identity");
        var table = SystemCollections.CACHE_OPERATOR_CLEANUP.on(database);
        if (table.find(session, new Document("_id", MongoPipelinePreparationFacts.scope(scope))).first() != null) throw MongoCachePipelinePreparations.rebuild(scope);
        table.insertOne(session, new Document("_id", MongoPipelinePreparationFacts.scope(scope)).append("formatVersion", 1)
                .append("preparationId", preparationId).append("revision", 0L).append("shapes", List.of()).append("placements", List.of()));
    }

    static void requireInitial(ClientSession session, MongoDatabase database, PipelineIntentScope scope, String preparationId) {
        Document root = require(session, database, scope);
        if (!preparationId.equals(root.get("preparationId")) || !Long.valueOf(0).equals(root.get("revision"))
                || !root.getList("shapes", Document.class).isEmpty() || !root.getList("placements", Document.class).isEmpty())
            throw MongoCachePipelinePreparations.rebuild(scope);
    }

    static Document require(ClientSession session, MongoDatabase database, PipelineIntentScope scope) {
        new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).verifyFacet(SystemCollections.CACHE_OPERATOR_CLEANUP);
        Document root = SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session, new Document("_id", MongoPipelinePreparationFacts.scope(scope))).first();
        return decode(session, database, scope, root, true);
    }

    private static Document decode(ClientSession session, MongoDatabase database, PipelineIntentScope scope, Document root, boolean current) {
        try {
            Set<String> expected = new HashSet<>(FIELDS);
            if (!current) expected.add("retirement");
            if (current && root != null && root.containsKey("retirement")) throw MongoCachePipelineAccess.claimFenced();
            if (root == null || !root.keySet().equals(expected) || !MongoPipelinePreparationFacts.scope(scope).equals(root.get("_id"))
                    || !Integer.valueOf(1).equals(root.get("formatVersion")) || !(root.get("revision") instanceof Long revision)
                    || revision < 0 || revision == Long.MAX_VALUE || !(root.get("preparationId") instanceof String preparationId)
                    || !UUID.fromString(preparationId).toString().equals(preparationId)) throw unreadable(scope);
            var shapes = documents(root, "shapes", scope); var placements = documents(root, "placements", scope);
            if (revision != (long) shapes.size() + placements.size()) throw unreadable(scope);
            if (current) {
                var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
                Document claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
                if (claim == null || !(claim.get("pipelinePreparation") instanceof Document preparation)
                        || !preparationId.equals(preparation.get("preparationId"))) throw unreadable(scope);
            }
            Set<Document> unique = new HashSet<>();
            for (Document shape : shapes) {
                if (!shape.keySet().equals(Set.of("database", "stepId")) || !(shape.get("database") instanceof String db) || db.isBlank()
                        || !(shape.get("stepId") instanceof String step) || OperatorStateText.decode(step).isBlank() || !unique.add(shape)) throw unreadable(scope);
            }
            unique.clear();
            for (Document location : placements) {
                OperatorPlacement decoded = placement(scope, location);
                if (!location.equals(location(decoded)) || !unique.add(location)) throw unreadable(scope);
                if (decoded.resource() == OperatorPlacement.Resource.NEST_STATE && !shapes.contains(shape(decoded.database(), decoded.stepId()))) throw unreadable(scope);
            }
            return root;
        } catch (IllegalArgumentException | ClassCastException | NullPointerException invalid) { throw unreadable(scope); }
    }

    /** Frozen old locations remain verifiable after the canonical claim points to its successor. */
    static void retire(ClientSession session, MongoDatabase database, io.tapstate.spi.store.PipelineRebuildPreparation original) {
        var scope = original.request().originalScope(); Document root = require(session, database, scope);
        if (!original.previousPreparationId().equals(root.get("preparationId"))) throw unreadable(scope);
        Document marker = retirement(original); marker.append("contentHash", frozenHash(root, marker));
        if (SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).updateOne(session,
                new Document("_id", root.get("_id")).append("preparationId", root.get("preparationId"))
                        .append("revision", root.get("revision")).append("retirement", new Document("$exists", false)),
                new Document("$set", new Document("retirement", marker))).getMatchedCount() != 1)
            throw new IllegalStateException("operator inventory changed inside its retirement transaction");
    }

    static Document requireRetired(ClientSession session, MongoDatabase database, io.tapstate.spi.store.PipelineRebuildPreparation original) {
        var scope = original.request().originalScope();
        new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).verifyFacet(SystemCollections.CACHE_OPERATOR_CLEANUP);
        Document root = SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session,
                new Document("_id", MongoPipelinePreparationFacts.scope(scope))).first();
        decode(session, database, scope, root, false);
        var expected = retirement(original); expected.append("contentHash", frozenHash(root, expected));
        if (!original.previousPreparationId().equals(root.get("preparationId")) || !expected.equals(root.get("retirement"))) throw unreadable(scope);
        return root;
    }

    private static Document retirement(io.tapstate.spi.store.PipelineRebuildPreparation original) {
        return new Document("phase", "RETIRED").append("operationId", original.request().operationId())
                .append("confirmationHash", original.confirmationHash())
                .append("targetScope", MongoPipelinePreparationFacts.scope(original.request().targetScope()))
                .append("targetPreparationId", original.preparationId());
    }

    private static String frozenHash(Document root, Document marker) {
        return io.tapstate.core.model.canonical.CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(
                new Document("scope", root.get("_id")).append("preparationId", root.get("preparationId"))
                        .append("revision", root.get("revision")).append("shapes", root.get("shapes"))
                        .append("placements", root.get("placements")).append("retirement", marker)));
    }

    static boolean knowsShape(Document root, String database, String step) { return root.getList("shapes", Document.class).contains(shape(database, step)); }

    static void rememberShape(ClientSession session, MongoDatabase database, PipelineIntentScope scope, Document root, String step) {
        var wanted = shape(database.getName(), step);
        if (root.getList("shapes", Document.class).contains(wanted)) throw unreadable(scope);
        var shapes = new ArrayList<>(root.getList("shapes", Document.class)); shapes.add(wanted);
        update(session, database, scope, root, "shapes", shapes);
    }

    static boolean knowsPlacement(Document root, OperatorPlacement placement) { return root.getList("placements", Document.class).contains(location(placement)); }

    static void rememberPlacement(ClientSession session, MongoDatabase database, Document root, OperatorPlacement placement) {
        Document wanted = location(placement);
        if (root.getList("placements", Document.class).contains(wanted)) return;
        var locations = new ArrayList<>(root.getList("placements", Document.class)); locations.add(wanted);
        update(session, database, placement.scope(), root, "placements", locations);
    }

    private static void update(ClientSession session, MongoDatabase database, PipelineIntentScope scope, Document root, String field, List<Document> values) {
        long revision = root.getLong("revision"); if (revision == Long.MAX_VALUE - 1) throw unreadable(scope);
        if (SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).updateOne(session,
                new Document("_id", MongoPipelinePreparationFacts.scope(scope)).append("preparationId", root.get("preparationId")).append("revision", revision),
                new Document("$set", new Document(field, values)).append("$inc", new Document("revision", 1L))).getMatchedCount() != 1)
            throw new IllegalStateException("operator inventory changed within its transaction snapshot");
    }

    static Document location(OperatorPlacement placement) {
        return new Document("database", placement.database()).append("resource", placement.resource().name())
                .append("stepId", OperatorStateText.encode(placement.stepId())).append("pathId", OperatorStateText.encode(placement.pathId()))
                .append("namespace", OperatorStateText.encode(placement.namespace()));
    }

    private static OperatorPlacement placement(PipelineIntentScope scope, Document location) {
        if (!location.keySet().equals(Set.of("database", "resource", "stepId", "pathId", "namespace"))) throw unreadable(scope);
        return new OperatorPlacement(scope, location.getString("database"), OperatorPlacement.Resource.valueOf(location.getString("resource")),
                OperatorStateText.decode(location.getString("stepId")), OperatorStateText.decode(location.getString("pathId")), OperatorStateText.decode(location.getString("namespace")));
    }
    private static Document shape(String database, String step) { return new Document("database", database).append("stepId", OperatorStateText.encode(step)); }
    private static List<Document> documents(Document root, String field, PipelineIntentScope scope) {
        if (!(root.get(field) instanceof List<?> values) || values.stream().anyMatch(value -> !(value instanceof Document))) throw unreadable(scope);
        return root.getList(field, Document.class);
    }
    private static TapstateException unreadable(PipelineIntentScope scope) { return new TapstateException(IoError.DOCUMENT_UNREADABLE,
            Map.of("id", scope.pipelineId(), "field", "operator-inventory"), null); }
}
