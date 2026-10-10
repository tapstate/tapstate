package io.tapstate.adapters.mongostore.migration;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.IndexEnsure;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import org.bson.Document;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Introduces profile-aware coordination and durable artifact incarnations after a cold upgrade. */
public final class V12ClusterExecutionProfiles implements ChangeSet {
    @Override
    public int version() {
        return 12;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        fence.requireStillHeld();
        MongoCollection<Document> claims = SystemCollections.WORKLOAD_CLAIMS.on(database)
                .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY);
        Document live = new Document("$expr", new Document("$or", List.of(
                new Document("$gt", List.of("$leaseUntil", "$$NOW")),
                new Document("$gt", List.of("$retiredAuthorizationUntil", "$$NOW")))));
        if (claims.find(live).first() != null) {
            throw new TapstateException(MigrationError.COLD_UPGRADE_REQUIRED, Map.of(), null);
        }
        HashSet<String> present = new HashSet<>(database.listCollectionNames().into(new java.util.ArrayList<>()));
        for (SystemCollections row : List.of(SystemCollections.CLUSTER_EXECUTION_PROFILES,
                SystemCollections.CLUSTER_NODE_REGISTRY, SystemCollections.CLUSTER_CAPACITY_OCCUPANCY,
                SystemCollections.CLUSTER_RECOVERY_QUEUE)) {
            fence.requireStillHeld();
            if (!present.contains(row.collectionName())) {
                database.createCollection(row.collectionName());
            }
            for (SystemCollections.IndexSpec index : row.indexes()) {
                fence.requireStillHeld();
                IndexEnsure.ensure(database, row.on(database), index);
            }
        }
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
        Document missing = new Document("incarnation", new Document("$exists", false));
        try (var rows = artifacts.find(missing).projection(new Document("_id", 1)).iterator()) {
            while (rows.hasNext()) {
                Document row = rows.next();
                fence.requireStillHeld();
                artifacts.updateOne(new Document("_id", row.get("_id")).append("incarnation",
                                new Document("$exists", false)),
                        new Document("$set", new Document("incarnation", UUID.randomUUID().toString())));
            }
        }
        try (var pipelines = artifacts.find(new Document("kind", "pipeline"))
                .projection(new Document("_id", 1).append("incarnation", 1)).iterator()) {
            while (pipelines.hasNext()) {
                Document artifact = pipelines.next();
                String incarnation = artifact.getString("incarnation");
                if (incarnation == null || incarnation.isBlank()) {
                    throw new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                            Map.of("id", String.valueOf(artifact.get("_id")), "field", "incarnation"), null);
                }
                fence.requireStillHeld();
                claims.withWriteConcern(WriteConcern.MAJORITY.withJournal(true)).updateMany(
                        new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", artifact.get("_id"))
                                .append("executionGeneration", new Document("$gt", 0L))
                                .append("executionProfileVersion", new Document("$exists", false))
                                .append("executionIncarnation", new Document("$exists", false)),
                        new Document("$set", new Document("executionIncarnation", incarnation)));
            }
        }
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        long missing = SystemCollections.ARTIFACTS.on(database)
                .countDocuments(new Document("incarnation", new Document("$exists", false)));
        return "requires all legacy workload leases to expire; creates profile/session registry; assigns "
                + missing + " missing artifact incarnations";
    }
}
