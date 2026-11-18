package io.tapstate.adapters.mongostore;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;

import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.*;
import static io.tapstate.adapters.mongostore.SystemCollections.*;

final class MongoPipelineObservationConfig {
    private static final List<SystemCollections> ARTIFACTS = List.of(
            PIPELINES, SOURCES, TRANSFORM_DEFINITIONS, VIEW_DEFINITIONS, SERVE_DEFINITIONS);
    static final List<SystemCollections> COLLECTIONS = java.util.stream.Stream.concat(ARTIFACTS.stream(), java.util.stream.Stream.of(
            CONFIG_CLUSTER_IDENTITY, CONFIGURATION_VERSION, STORAGE_SETUP, STORAGE_BINDING,
            STORAGE_RECOVERY, PDK_NOTE_SCOPES, OPERATOR_GENERATIONS, PIPELINE_INTENTS, CAPTURE_PREPARATIONS)).toList();

    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoStorageBindingStore bindings;
    private final MongoConfigurationArtifactStore artifacts;

    MongoPipelineObservationConfig(MongoClient client, MongoDatabase database, MongoStorageBindingStore bindings,
            MongoConfigurationArtifactStore artifacts) {
        this.client = client;
        this.database = database;
        this.bindings = bindings;
        this.artifacts = artifacts;
    }

    MongoConfigurationDomain.ObservationCheck read(PipelineIntentStore.ActuationReading before,
            String defaultSource, Map<String, String> sourceCaptures) {
        var scope = before.intent().scope();
        var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
        String pipeline = scope.pipelineId();
        var ids = new HashSet<>(sourceCaptures.keySet());
        ids.add(pipeline);
        ids.add(defaultSource);
        var query = new ArrayList<Document>();
        query.add(match(new Document("_id", ConfigurationVersion.ID)));
        query.add(one(CONFIG_CLUSTER_IDENTITY, "cluster", "cluster"));
        query.add(one(STORAGE_SETUP, "setup", "cluster"));
        query.add(one(STORAGE_BINDING, "binding", "cluster"));
        query.add(one(OPERATOR_GENERATIONS, "mapping", pipeline));
        query.add(one(PIPELINE_INTENTS, "intent", pipeline));
        query.add(one(PDK_NOTE_SCOPES, "global", new Document("bindingVersion", scope.bindingVersion()).append("namespace", "pdk.global-state")));
        query.add(lookup(STORAGE_RECOVERY, "recovery", List.of(new Document("$limit", 2), boundedRow())));
        var captureIds = sourceCaptures.values().stream().distinct().map(id -> MongoCapturePreparationFacts.id(business, id)).toList();
        query.add(lookup(CAPTURE_PREPARATIONS, "capture", bounded(match(in("_id", captureIds)), captureIds.size())));
        for (var kind : ARTIFACTS) query.add(lookup(kind, kind.collectionName(), bounded(match(in("_id", ids)), ids.size())));

        return StoreIo.call(() -> {
            try (var session = client.startSession(ClientSessionOptions.builder().snapshot(true).build())) {
                var root = CONFIGURATION_VERSION.on(database).aggregate(session, query).first();
                if (ConfigurationVersion.revision(root) != before.configurationRevision().orElseThrow())
                    return MongoConfigurationDomain.ObservationCheck.superseded();
                var mapping = single(root, "mapping");
                var intent = single(root, "intent");
                if (mapping == null || intent == null || MongoPipelineSharedRecoveryFacts.read(mapping).isPresent()) return MongoConfigurationDomain.ObservationCheck.fallback();
                var state = MongoPipelineStateMappings.decode(pipeline, mapping);
                var preparation = MongoPipelinePreparationFacts.decode(pipeline, mapping, scope.clusterId());
                if (state.phase() != PipelineStateMapping.Phase.ACTIVE || state.stateGeneration() != scope.stateGeneration()
                        || preparation.phase() != PipelinePreparation.Phase.COMPLETE || !scope.equals(preparation.scope())
                        || !before.intent().equals(new MongoPipelineIntentStore(client, database, scope.clusterId(), bindings).decode(intent))) {
                    return MongoConfigurationDomain.ObservationCheck.superseded();
                }

                Map<String, Document> definitions = new HashMap<>();
                for (var kind : ARTIFACTS) {
                    if (overflow(root, kind.collectionName(), ids.size())) return MongoConfigurationDomain.ObservationCheck.fallback();
                    for (var row : rows(root, kind.collectionName())) {
                        String id = row.getString("_id");
                        var expectedKind = pipeline.equals(id) ? PIPELINES : SOURCES;
                        if (!ids.contains(id) || kind != expectedKind || definitions.put(id, row) != null)
                            return MongoConfigurationDomain.ObservationCheck.fallback();
                    }
                }
                if (!definitions.keySet().equals(ids) || overflow(root, "capture", captureIds.size())
                        || List.of("cluster", "setup", "binding", "recovery", "global").stream()
                                .anyMatch(field -> single(root, field) == null)) return MongoConfigurationDomain.ObservationCheck.fallback();
                var stored = single(root, "binding");
                if (!defaultSource.equals(stored.get("defaultSourceId"))) return MongoConfigurationDomain.ObservationCheck.fallback();
                var audited = bindings.auditedObservationBinding(single(root, "cluster"), single(root, "setup"), stored,
                        single(root, "recovery"), definitions.get(defaultSource), single(root, "global"), false);
                if (!business.equals(audited.businessScope())) return MongoConfigurationDomain.ObservationCheck.fallback();

                if (!(artifacts.decode(definitions.get(pipeline)) instanceof PipelineResource definition)
                        || !Set.copyOf(definition.sourceIds()).equals(sourceCaptures.keySet())) return MongoConfigurationDomain.ObservationCheck.fallback();
                var sources = new HashMap<String, SourceResource>();
                for (var id : sourceCaptures.keySet()) {
                    if (!(artifacts.decode(definitions.get(id)) instanceof SourceResource source)) return MongoConfigurationDomain.ObservationCheck.fallback();
                    sources.put(id, source);
                }
                var captures = new HashMap<String, CapturePreparation>();
                for (var row : rows(root, "capture")) {
                    var capture = MongoCapturePreparationFacts.available(row);
                    if (!business.equals(capture.scope()) || captures.put(capture.captureId(), capture) != null) return MongoConfigurationDomain.ObservationCheck.fallback();
                }
                if (!captures.keySet().equals(Set.copyOf(sourceCaptures.values()))) return MongoConfigurationDomain.ObservationCheck.fallback();
                return new MongoConfigurationDomain.ObservationCheck(false,
                        Optional.of(new MongoConfigurationDomain.ObservationConfiguration(definition, sources, captures)));
            }
        });
    }
}
