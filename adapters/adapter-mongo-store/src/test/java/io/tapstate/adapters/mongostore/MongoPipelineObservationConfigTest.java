package io.tapstate.adapters.mongostore;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class MongoPipelineObservationConfigTest {
    @Test void unchangedSnapshotCarriesExactDefinitionsAndCaptureIdentity() {
        var f = new Fixture();
        var result = f.read();
        assertThat(result.changed()).isFalse();
        var configuration = result.configuration().orElseThrow();
        assertThat(configuration.pipeline()).isEqualTo(f.pipeline);
        assertThat(configuration.sources()).containsOnlyKeys("input");
        assertThat(configuration.captures()).containsOnlyKeys("capture");
        verify(f.session).close();
    }

    @Test void twoAliasedCapturesShareOnePhysicalSourceRead() {
        var f = new Fixture();
        var aliased = new PipelineResource("flow", null, List.of(
                SourceRef.of("input", true, Map.of(), "left"),
                SourceRef.of("input", true, Map.of(), "right")), null, null, null, null, null);
        when(f.artifacts.decode(f.pipelineRow)).thenReturn(aliased);
        var capture = new MongoConfigurationDomain.ObservationCapture("input", "capture");

        var result = f.read(Map.of("left", capture, "right", capture)).configuration().orElseThrow();

        assertThat(result.sources()).containsExactlyInAnyOrderEntriesOf(Map.of("left", f.source, "right", f.source));
        assertThat(result.captures()).containsOnlyKeys("capture");
        verify(f.artifacts).decode(f.sourceRow);
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-mapping", "missing-intent", "shared-recovery", "missing-source", "wrong-kind", "duplicate",
            "foreign-id", "wide-artifact", "missing-cluster", "missing-setup", "missing-binding",
            "missing-recovery", "missing-global", "different-default", "different-business", "not-pipeline",
            "different-sources", "not-source", "missing-capture", "duplicate-capture", "foreign-capture", "different-capture", "wide-capture"})
    void incompleteOrIncompatibleSnapshotRequiresFullRead(String damage) {
        var f = new Fixture();
        switch (damage) {
            case "missing-mapping" -> f.root.remove("mapping");
            case "missing-intent" -> f.root.remove("intent");
            case "shared-recovery" -> f.mapping.put("sharedRecovery", MongoPipelineSharedRecoveryFacts.encode(
                    new MongoPipelineSharedRecoveryFacts.Pending(Fixture.PREPARATION, f.scope)));
            case "missing-source" -> f.root.put("sources", List.of(f.outputRow));
            case "wrong-kind" -> f.root.put("transform_definitions", List.of(f.sourceRow));
            case "duplicate" -> f.root.put("sources", List.of(f.sourceRow, f.sourceRow, f.outputRow));
            case "foreign-id" -> f.root.put("sources", List.of(f.sourceRow, new Document("_id", "foreign")));
            case "wide-artifact" -> f.root.put("sources", List.of(new Document("tooLarge", true)));
            case "missing-cluster" -> f.root.remove("cluster");
            case "missing-setup" -> f.root.remove("setup");
            case "missing-binding" -> f.root.remove("binding");
            case "missing-recovery" -> f.root.remove("recovery");
            case "missing-global" -> f.root.remove("global");
            case "different-default" -> f.binding.put("defaultSourceId", "other");
            case "different-business" -> when(f.bindings.auditedObservationBinding(any(),any(),any(),any(),any(),any(),eq(false)))
                    .thenReturn(new CachePreparation("setup", new CacheAuthScope("cluster", 2, StorageBinding.Mode.OP, "cache", 1)));
            case "not-pipeline" -> when(f.artifacts.decode(f.pipelineRow)).thenReturn(f.source);
            case "different-sources" -> when(f.artifacts.decode(f.pipelineRow)).thenReturn(
                    new PipelineResource("flow", null, List.of(SourceRef.bare("other")), null, null, null, null, null));
            case "not-source" -> when(f.artifacts.decode(f.sourceRow)).thenReturn(f.pipeline);
            case "missing-capture" -> f.root.put("capture", List.of());
            case "duplicate-capture" -> f.root.put("capture", List.of(f.captureRow, f.captureRow));
            case "foreign-capture" -> f.root.put("capture", List.of(MongoCapturePreparationFacts.row(
                    new CapturePreparation(new CacheBusinessScope("other",1,"cache"), CapturePreparation.Mode.SHARED_SRS,
                            "chain", "capture", "mc-"+"a".repeat(64), Fixture.PREPARATION))));
            case "different-capture" -> f.root.put("capture", List.of(MongoCapturePreparationFacts.row(
                    new CapturePreparation(new CacheBusinessScope("cluster",1,"cache"), CapturePreparation.Mode.SHARED_SRS,
                            "other-chain", "other-capture", "mc-"+"a".repeat(64), Fixture.PREPARATION))));
            case "wide-capture" -> f.root.put("capture", List.of(new Document("tooLarge", true)));
            default -> throw new AssertionError(damage);
        }
        var result = f.read();
        assertThat(result.changed()).isFalse();
        assertThat(result.configuration()).isEmpty();
        verify(f.session).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"revision", "retired", "generation", "unprepared", "scope", "intent"})
    void supersededAuthorityCannotPublishTheOriginalObservation(String change) {
        var f = new Fixture();
        switch (change) {
            case "revision" -> f.root.put("revision", 12L);
            case "retired" -> { f.mapping.put("phase", "RETIRED"); f.mapping.put("operationId", Fixture.PREPARATION); }
            case "generation" -> {
                f.mapping.put("stateGeneration", 2L);
                var next = new PipelineIntentScope("cluster",1,"cache","flow",2);
                f.mapping.put("cachePreparation",MongoPipelinePreparationFacts.encode(
                        new PipelinePreparation("flow",2,PipelinePreparation.Phase.COMPLETE,next,Fixture.PREPARATION,f.node)));
            }
            case "unprepared" -> f.mapping.put("cachePreparation", new Document("phase", "UNASSIGNED"));
            case "scope" -> {
                var scope = new PipelineIntentScope("cluster",2,"cache","flow",1);
                f.mapping.put("cachePreparation", MongoPipelinePreparationFacts.encode(
                        new PipelinePreparation("flow",1,PipelinePreparation.Phase.COMPLETE,scope,Fixture.PREPARATION, f.node)));
            }
            case "intent" -> f.intent.put("intentRevision", 2L);
            default -> throw new AssertionError(change);
        }
        assertThat(f.read().changed()).isTrue();
        verifyNoInteractions(f.artifacts);
        verify(f.session).close();
    }

    private static final class Fixture {
        static final String PREPARATION = "11111111-1111-4111-8111-111111111111";
        final MongoClient client = mock(MongoClient.class);
        final MongoDatabase db = mock(MongoDatabase.class);
        final ClientSession session = mock(ClientSession.class);
        final MongoStorageBindingStore bindings = mock(MongoStorageBindingStore.class);
        final MongoConfigurationArtifactStore artifacts = mock(MongoConfigurationArtifactStore.class);
        final PipelineIntentScope scope = new PipelineIntentScope("cluster",1,"cache","flow",1);
        final WorkloadClaimFence node = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),
                new WorkloadOwner("node","boot"),1,0,1);
        final Document mapping = new Document("_id","flow").append("stateGeneration",1L).append("noteWriteFence",0L)
                .append("phase","ACTIVE").append("operationId",null).append("cachePreparation",
                        MongoPipelinePreparationFacts.encode(new PipelinePreparation("flow",1,PipelinePreparation.Phase.COMPLETE,scope,PREPARATION,node)));
        final Document intent = new Document("_id","flow").append("formatVersion",1).append("clusterId","cluster")
                .append("bindingVersion",1L).append("cacheIncarnation","cache").append("stateGeneration",1L)
                .append("intentRevision",1L).append("deleted",true);
        final Document binding = new Document("_id","cluster").append("defaultSourceId","views");
        final Document pipelineRow = new Document("_id","flow");
        final Document sourceRow = new Document("_id","input");
        final Document outputRow = new Document("_id","views");
        final Document captureRow = MongoCapturePreparationFacts.row(new CapturePreparation(new CacheBusinessScope("cluster",1,"cache"),
                CapturePreparation.Mode.SHARED_SRS,"chain","capture","mc-"+"a".repeat(64),PREPARATION));
        final PipelineResource pipeline = new PipelineResource("flow",null,List.of(SourceRef.bare("input")),null,null,null,null,null);
        final SourceResource source = new SourceResource("input",null,"mongodb",Map.of(),null,null,null,null);
        final Document root = new Document("_id",ConfigurationVersion.ID).append("revision",11L)
                .append("mapping",List.of(mapping)).append("intent",List.of(intent))
                .append("pipelines",List.of(pipelineRow)).append("sources",List.of(sourceRow,outputRow))
                .append("cluster",List.of(new Document("_id","cluster"))).append("setup",List.of(new Document("_id","cluster")))
                .append("binding",List.of(binding)).append("recovery",List.of(new Document("_id","cluster")))
                .append("global",List.of(new Document("_id","global"))).append("capture",List.of(captureRow));

        @SuppressWarnings("unchecked")
        Fixture() {
            MongoCollection<Document> collection = mock(MongoCollection.class);
            AggregateIterable<Document> query = mock(AggregateIterable.class);
            when(db.getName()).thenReturn("config");
            when(client.getDatabase("config")).thenReturn(db);
            when(bindings.belongsTo(client,"config")).thenReturn(true);
            when(collection.withReadConcern(any())).thenReturn(collection);
            when(collection.withWriteConcern(any())).thenReturn(collection);
            when(db.getCollection(anyString())).thenReturn(collection);
            when(collection.aggregate(eq(session),anyList())).thenReturn(query);
            when(query.first()).thenReturn(root);
            when(client.startSession(any(ClientSessionOptions.class))).thenReturn(session);
            when(bindings.auditedObservationBinding(any(),any(),any(),any(),any(),any(),eq(false)))
                    .thenReturn(new CachePreparation("setup",new CacheAuthScope("cluster",1,StorageBinding.Mode.OP,"cache",1)));
            when(artifacts.decode(pipelineRow)).thenReturn(pipeline);
            when(artifacts.decode(sourceRow)).thenReturn(source);
        }
        MongoConfigurationDomain.ObservationCheck read() {
            return read(Map.of("input", new MongoConfigurationDomain.ObservationCapture("input", "capture")));
        }
        MongoConfigurationDomain.ObservationCheck read(Map<String, MongoConfigurationDomain.ObservationCapture> inputs) {
            var original = new PipelineIntentStore.ActuationReading(new PipelineIntentSnapshot(scope,1,null),OptionalLong.of(11));
            return new MongoPipelineObservationConfig(client,db,bindings,artifacts).read(original,"views",inputs);
        }
    }
}
