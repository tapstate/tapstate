package io.tapstate.adapters.mongostore;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.stream.IntStream;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MongoPipelineObservationReadTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "flow", 1);
    private static final SrsConsumerRun RUN = run(SCOPE, "input", "boot");

    @ParameterizedTest @ValueSource(strings = {"empty", "many", "scope", "claim", "dotted", "tables"})
    void unsupportedSelectionsFallBackBeforeOpeningADriverSession(String changed) {
        var fixture = new Fixture();
        Map<SrsConsumerRun, MongoCacheDomain.ObservationSource> sources = switch (changed) {
            case "empty" -> Map.of();
            case "many" -> {
                var result = new LinkedHashMap<SrsConsumerRun, MongoCacheDomain.ObservationSource>();
                for (int n = 0; n < 5; n++) result.put(run(SCOPE, "input" + n, "boot"), source(Set.of("orders")));
                yield result;
            }
            case "scope" -> Map.of(run(new PipelineIntentScope("cluster", 1, "cache", "flow", 2), "input", "boot"), source(Set.of("orders")));
            case "claim" -> Map.of(run(SCOPE, "input", "replacement"), source(Set.of("orders")));
            case "dotted" -> Map.of(run(SCOPE, "input.nested", "boot"), source(Set.of("orders")));
            default -> Map.of(RUN, source(IntStream.range(0, 257).mapToObj(n -> "table" + n).collect(java.util.stream.Collectors.toSet())));
        };
        assertThat(fixture.reader.read(sources)).isEmpty();
        verifyNoInteractions(fixture.client);
    }

    @ParameterizedTest @ValueSource(strings = {"missing", "large", "schema", "state", "claim", "node", "counts",
            "schema-large", "state-large", "claim-large", "node-large", "counts-large"})
    void anIncompleteOrOversizedAggregateFallsBackWithoutInterpretingPartialFacts(String fault) {
        var fixture = new Fixture();
        var root = new Document();
        if (fault.equals("missing")) root = null;
        else if (fault.equals("large")) root.append("tooLarge", true);
        else {
            String name = fault.replace("-large", "");
            root.append(name, fault.endsWith("-large") ? List.of(new Document("tooLarge", true))
                    : Collections.nCopies(name.equals("counts") ? 257 : 2, new Document()));
        }
        when(fixture.aggregate.first()).thenReturn(root);
        assertThat(fixture.read()).isEmpty();
        verify(fixture.session).close();
    }

    @ParameterizedTest @ValueSource(strings = {"absent", "cluster", "binding", "incarnation", "layout", "phase"})
    void aCurrentCacheIdentityCannotBeSubstitutedByACompatibleLookingSnapshot(String fault) {
        var fixture = new Fixture();
        var root = root();
        if (fault.equals("absent")) root.remove("body");
        else {
            String field = switch (fault) {
                case "cluster" -> "clusterId";
                case "binding" -> "bindingVersion";
                case "incarnation" -> "cacheIncarnation";
                case "layout" -> "layoutId";
                default -> "businessPhase";
            };
            root.get("body", Document.class).put(field, fault.equals("binding") ? 2L : "other");
        }
        when(fixture.aggregate.first()).thenReturn(root);
        assertThatThrownBy(fixture::read).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
        verify(fixture.session).close();
    }

    @ParameterizedTest @ValueSource(strings = {"members", "offsets", "models", "contexts", "bounds", "events",
            "members-large", "offsets-large", "models-large", "contexts-large", "bounds-large", "events-large", "missing", "other"})
    void chainClosureMustFitInTheSameSnapshotBeforeItCanPublishAnObservation(String fault) {
        var fixture = new Fixture();
        var root = root();
        if (!fault.equals("missing")) {
            var chain = new Document("body", new Document("_id", fault.equals("other") ? "foreign" : "chain"));
            if (!fault.equals("other")) {
                String field = fault.replace("-large", "");
                int limit = Set.of("members", "offsets").contains(field) ? 128 : 256;
                chain.put(field, fault.endsWith("-large") ? List.of(new Document("tooLarge", true))
                        : Collections.nCopies(limit + 1, new Document()));
            }
            root.append("chain0", List.of(chain));
        }
        when(fixture.aggregate.first()).thenReturn(root);
        assertThat(fixture.read()).isEmpty();
        verify(fixture.session).close();
    }

    @ParameterizedTest @ValueSource(strings = {"current", "missing", "detached", "generation", "revision", "owner", "epoch", "tables", "selection"})
    void currentMembershipMustMatchTheOriginalExecutionBeforeReadingItsSourceSchema(String changed) {
        var fixture = new Fixture();
        var root = root();
        var cache = new CacheBusinessScope("cluster", 1, "cache");
        var capture = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "capture"),
                new WorkloadOwner("node", "boot"), 1, 1, 1);
        var chain = MongoSrsChainStore.initialRow(cache, "chain", "1h", capture);
        var context = changed.equals("generation") ? new SrsConsumerContext(RUN.context().key(), 2) : RUN.context();
        var fence = changed.equals("owner") ? run(SCOPE, "input", "new-boot").originalClaim() : RUN.originalClaim();
        var member = new SrsConsumerMembership(context, changed.equals("revision") ? 2 : 1,
                changed.equals("detached"), changed.equals("detached") ? Set.of() : Set.of("orders"), fence);
        var records = new MongoSrsConsumerRecords(cache);
        var raw = new Document("body", chain);
        if (!changed.equals("missing")) {
            raw.put("members", List.of(records.membership(member)));
            chain.put("consumerRevision", member.revision());
            if (!member.detached()) {
                chain.put("consumerCount", 1L);
                var cursor = new ConsumerOffset(context.key().consumerId(), Map.of("orders", -1L), null,
                        List.of(), null, 0, Map.of(), ConsumerProgressKind.SRS);
                raw.put("offsets", List.of(records.cursor(member, cursor, 0)));
            }
        }
        root.put("chain0", List.of(raw));
        String generation = "22222222-2222-4222-8222-222222222222";
        var schemaAccess = new MongoCacheMetadataAccess(fixture.client, fixture.database, cache,
                SystemCollections.CACHE_SOURCE_SCHEMAS, () -> {});
        var table = new SourceTable("orders", List.of(), List.of(), List.of());
        var discovered = new DiscoveredSourceModel("input", "mongodb", 1L, new SourceModel(List.of(table)));
        var envelope = schemaAccess.stamp(MongoSchemaStore.envelope(discovered, generation)
                .append("tableCount", 1).append("_writers", new Document()).append("_revision", 1L));
        var tableRow = schemaAccess.stamp(MongoSchemaStore.tableDocument(table)
                .append("_id", "input." + generation + ".0").append(MongoSchemaStore.GENERATION, generation).append("order", 0));
        root.put("sourceSchema0", List.of(new Document("body", envelope).append("tables", List.of(tableRow))));
        if (changed.equals("selection")) {
            var inputs = new PipelinePreparationInputs("a".repeat(64), Map.of("input", "b".repeat(64)),
                    Map.of("input", Set.of("other")), List.of());
            root.getList("claim", Document.class).getFirst().put("pipelineSources", MongoPipelineSourcePreparations.encode(inputs));
        }
        when(fixture.aggregate.first()).thenReturn(root);
        var tables = changed.equals("tables") ? Set.of("other") : Set.of("orders");
        var source = new MongoCacheDomain.ObservationSource(changed.equals("epoch") ? 1 : 0, tables);
        var result = fixture.reader.read(Map.of(RUN, source));
        if (changed.equals("current")) {
            assertThat(result).isPresent();
            assertThat(result.orElseThrow().schemas()).containsEntry("input", discovered);
            assertThat(result.orElseThrow().cursors()).containsKey("input");
        } else {
            assertThat(result).isEmpty();
        }
        verify(fixture.session).close();
    }

    private static SrsConsumerRun run(PipelineIntentScope scope, String source, String boot) {
        return new SrsConsumerRun(scope, "chain", source, 1, "node", boot, 1, 1, 1);
    }

    private static MongoCacheDomain.ObservationSource source(Set<String> tables) {
        return new MongoCacheDomain.ObservationSource(1, tables);
    }

    private static Document root() {
        var scope = MongoPipelinePreparationFacts.scope(SCOPE);
        var identity = new Document("clusterId", "cluster").append("bindingVersion", 1L).append("cacheIncarnation", "cache")
                .append("layoutId", MongoConfigurationDomain.LAYOUT).append("businessPhase", "READY")
                .append("businessPreparationId", "business").append("claimAdmissionFence", 0L);
        var state = new Document("_id", "flow").append("scope", scope).append("stateJson", "RUNNING").append("epoch", 1L)
                .append("runtimeWriteFence", 1L).append("touchMillis", 0L);
        var claim = new Document("_id", MongoWorkloadClaimStore.id(RUN.originalClaim().key()))
                .append("clusterId", "cluster").append("resourceType", "PIPELINE_ACTUATION").append("resourceId", "flow")
                .append("ownerNodeId", "node").append("ownerBootId", "boot").append("claimGeneration", 1L)
                .append("executionGeneration", 1L).append("topologyRevision", 1L).append("leaseUntil", new Date())
                .append("leaseRemainingMillis", 10000L).append("contextExecutionGeneration", 1L)
                .append("executionClaimGeneration", 1L).append("executionNodeIds", List.of("node"))
                .append("failureClaimGeneration", 0L).append("failureAfterMemberLoss", false)
                .append("pipelineScope", scope).append("pipelinePreparation", MongoCachePipelinePreparations.marker(
                        "11111111-1111-4111-8111-111111111111", PipelineCachePreparations.Phase.PREPARED))
                .append("pipelineSources", MongoPipelineSourcePreparations.encode(new PipelinePreparationInputs(
                        "a".repeat(64), Map.of("input", "b".repeat(64)), Map.of("input", Set.of("orders")), List.of())));
        return new Document("body", identity)
                .append("schema", List.of(new Document("installedVersion", 1).append("phase", "READY")
                        .append("operationId", "business").append("epoch", 1L)))
                .append("state", List.of(state)).append("claim", List.of(claim));
    }

    private static final class Fixture {
        final MongoClient client = mock(MongoClient.class);
        final MongoDatabase database = mock(MongoDatabase.class);
        final ClientSession session = mock(ClientSession.class);
        @SuppressWarnings("unchecked") final MongoCollection<Document> collection = mock(MongoCollection.class, RETURNS_SELF);
        @SuppressWarnings("unchecked") final AggregateIterable<Document> aggregate = mock(AggregateIterable.class);
        final MongoPipelineObservationRead reader;

        Fixture() {
            when(database.getName()).thenReturn("test");
            var collections = new HashMap<String, MongoCollection<Document>>();
            collections.put("cache_identity", collection);
            when(collection.getNamespace()).thenReturn(new com.mongodb.MongoNamespace("test", "cache_identity"));
            when(database.getCollection(anyString())).thenAnswer(call -> collections.computeIfAbsent(call.getArgument(0), name -> {
                @SuppressWarnings("unchecked") MongoCollection<Document> table = mock(MongoCollection.class, RETURNS_SELF);
                when(table.getNamespace()).thenReturn(new com.mongodb.MongoNamespace("test", name));
                return table;
            }));
            when(client.startSession(any(ClientSessionOptions.class))).thenReturn(session);
            when(collection.aggregate(eq(session), anyList())).thenReturn(aggregate);
            var access = new MongoCachePipelineAccess(client, database, SCOPE, RUN.originalClaim(), () -> {});
            reader = new MongoPipelineObservationRead(client, database, access);
        }

        Optional<MongoCacheDomain.ObservationFacts> read() {
            return reader.read(Map.of(RUN, source(Set.of("orders"))));
        }
    }
}
