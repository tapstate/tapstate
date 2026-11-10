package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.ValidationClaims;
import org.bson.Document;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Bounded read snapshots for task status. The observations never grant storage or run authority. */
final class MongoValidationClaimObservation {
    record Configuration(MongoValidationPreparationFacts.Subject subject, long revision, Document binding,
                         Document setup, Document recovery, Document marker, String pipelineHash,
                         io.tapstate.spi.store.PipelineStateMapping pipelineState) {
        @Override public String toString() { return "ValidationClaimConfiguration[redacted]"; }
    }

    private static final Document BINDING_WITNESS = fields("bindingVersion", "candidateHash", "cache");
    private static final Document SETUP_WITNESS = fields("phase", "generation", "operationId", "bindingVersion", "candidateHash");
    private static final Document RECOVERY_WITNESS = fields("phase", "revision", "recoveryId", "bindingVersion", "cacheIncarnation");

    private final MongoDatabase config;
    private final MongoStorageBindingStore bindings;
    private final CacheBusinessScope scope;
    private final String pipeline;

    MongoValidationClaimObservation(MongoDatabase config, MongoStorageBindingStore bindings,
                                    CacheBusinessScope scope, String pipeline) {
        this.config = snapshot(config);
        this.bindings = bindings;
        this.scope = scope;
        this.pipeline = pipeline;
    }

    Configuration configuration() { return configuration(false); }

    private Configuration configuration(boolean recheck) {
        var row = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(config).aggregate(List.of(
                match("cluster"),
                lookup(SystemCollections.STORAGE_BINDING, "binding", "cluster", recheck ? BINDING_WITNESS : null),
                lookup(SystemCollections.STORAGE_SETUP, "setup", "cluster", recheck ? SETUP_WITNESS : null),
                lookup(SystemCollections.STORAGE_RECOVERY, "recovery", "cluster", recheck ? RECOVERY_WITNESS : null),
                lookup(SystemCollections.SCHEMA_MIGRATIONS, "schema", "config"),
                lookup(SystemCollections.OPERATOR_GENERATIONS, "mapping", pipeline),
                lookup(SystemCollections.PIPELINES, "pipeline", pipeline,
                        new Document("kind", 1).append("contentHash", 1)))).first();
        if (row == null || !"cluster".equals(row.get("_id")) || !scope.clusterId().equals(row.get("clusterId"))
                || !Long.valueOf(scope.bindingVersion()).equals(row.get("bindingVersion"))
                || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))) throw fenced();
        var marker = MongoValidationSchema.requireConfiguration(one(row, "schema", "config"), "READY");
        if (!scope.equals(MongoValidationSchema.scope(marker))
                || Integer.valueOf(2).equals(marker.get("formatVersion"))
                    && !"READY".equals(marker.get("scheduledAdmission", Document.class).get("phase"))) throw fenced();
        var artifact = one(row, "pipeline", pipeline);
        if (!"pipeline".equals(artifact.get("kind")) || !(artifact.get("contentHash") instanceof String hash)
                || !hash.matches("[0-9a-f]{64}")) throw fenced();
        var mapping = one(row, "mapping", pipeline);
        if (!(mapping.get("phase") instanceof String phase) || !Set.of("ACTIVE", "REBUILDING").contains(phase)
                || !(mapping.get("validationPreparation") instanceof Document preparation)) throw fenced();
        var state = MongoValidationPreparationFacts.decode(preparation, scope.clusterId(), pipeline);
        if (state.phase() != MongoValidationPreparationFacts.Phase.COMPLETE || state.closingOperationId() != null
                || !scope.equals(state.subject().scope())) throw fenced();
        return new Configuration(state.subject(), state.revision(), one(row, "binding", "cluster"),
                one(row, "setup", "cluster"), one(row, "recovery", "cluster"), marker, hash,
                MongoPipelineStateMappings.decode(pipeline, mapping));
    }

    boolean stillCurrent(Configuration original) {
        var current = configuration(true);
        return original.subject().equals(current.subject()) && original.revision() == current.revision()
                && original.pipelineState().equals(current.pipelineState()) && original.pipelineHash().equals(current.pipelineHash())
                && original.marker().equals(current.marker())
                && matches(original.binding(), current.binding(), BINDING_WITNESS)
                && matches(original.setup(), current.setup(), SETUP_WITNESS)
                && matches(original.recovery(), current.recovery(), RECOVERY_WITNESS);
    }

    private static boolean matches(Document original, Document current, Document projection) {
        return projection.keySet().stream().allMatch(field -> java.util.Objects.equals(original.get(field), current.get(field)));
    }

    private static Document fields(String... names) {
        var projection = new Document();
        for (String name : names) projection.put(name, 1);
        return projection;
    }

    StorageBinding target(Configuration observed) {
        return bindings.observationBinding(scope, observed.binding(), observed.setup(), observed.recovery());
    }

    Optional<WorkloadClaimReading> cache(MongoDatabase cache, MongoValidationPreparationFacts.Subject expected) {
        var claimKey = ValidationClaims.key(scope.clusterId(), pipeline);
        var claimId = MongoWorkloadClaimStore.id(claimKey);
        var controlId = MongoValidationControlStore.controlId(scope.clusterId(), pipeline);
        var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(snapshot(cache)).aggregate(List.of(
                match(claimId),
                MongoWorkloadClaimStore.leaseRemaining(true),
                lookup(SystemCollections.CACHE_IDENTITY, "observedIdentity", "cluster"),
                lookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "observedSchema", "business"),
                lookup(SystemCollections.VALIDATION_RUNS, "observedControl", controlId))).first();
        if (row == null) return Optional.empty();
        var schema = one(row, "observedSchema", "business");
        MongoCacheBusinessIdentity.requireObservedAdmission(one(row, "observedIdentity", "cluster"), schema, scope);
        if (!scope.equals(MongoValidationSchema.scope(MongoValidationSchema.requireVersionTwo(schema, "READY"))))
            throw fenced();
        var control = one(row, "observedControl", controlId);
        MongoValidationCachePreparations.validateRoot(control, scope, pipeline);
        if (!expected.equals(MongoValidationCachePreparations.binding(control))
                || !MongoValidationPreparationFacts.Outcome.PREPARED.name().equals(control.get("birthState"))
                || !Boolean.FALSE.equals(control.get("admissionClosed"))
                || !control.get("birth").equals(row.get("validationPreparation"))
                || !(row.get("fencedValidationWrites") instanceof Long writes) || writes < 0 || writes == Long.MAX_VALUE)
            throw fenced();

        var reading = MongoWorkloadClaimStore.observedReading(row);
        var claim = reading.claim();
        if (!claimKey.equals(claim.key())) throw fenced();
        if (control.get("owner") instanceof Document stored) {
            var owner = MongoValidationControlStore.owner(stored, pipeline);
            if (claim.claimGeneration() < owner.claimGeneration() || claim.executionGeneration() < owner.executionGeneration()
                    || (Long) control.get("countingGeneration") > claim.executionGeneration()
                    || claim.claimGeneration() == owner.claimGeneration()
                        && !claim.owner().equals(new WorkloadOwner(owner.nodeId(), owner.bootId()))) throw fenced();
        }
        return Optional.of(reading);
    }

    static boolean lacksSnapshotRead(com.mongodb.client.MongoClient client) {
        return client.getClusterDescription().getServerDescriptions().stream()
                .anyMatch(server -> server.getMaxWireVersion() > 0 && server.getMaxWireVersion() < 13);
    }

    private static MongoDatabase snapshot(MongoDatabase database) {
        return database.withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.SNAPSHOT);
    }

    private static Document match(Object id) {
        return new Document("$match", new Document("_id", id));
    }

    private static Document lookup(SystemCollections table, String field, Object id) {
        return lookup(table, field, id, null);
    }

    private static Document lookup(SystemCollections table, String field, Object id, Document projection) {
        var stages = new java.util.ArrayList<>(List.of(match(id), new Document("$limit", 2)));
        if (projection != null) stages.add(new Document("$project", projection));
        return new Document("$lookup", new Document("from", table.collectionName()).append("pipeline", stages).append("as", field));
    }

    private static Document one(Document row, String name, Object id) {
        if (!(row.get(name) instanceof List<?> matches) || matches.size() != 1
                || !(matches.getFirst() instanceof Document value) || !id.equals(value.get("_id"))) throw fenced();
        return value;
    }

    private static io.tapstate.core.common.TapstateException fenced() {
        return MongoValidationPreparations.fenced();
    }
}
