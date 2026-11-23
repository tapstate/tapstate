package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsConsumerRun;
import io.tapstate.spi.store.SrsSnapshotPin;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SrsConsumerKey;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real transactions over an explicitly prepared business fixture; production capture is not exercised. */
@RequiresDocker
class MongoSrsConsumerStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final String CHAIN = "chain-a";
    private static final SrsConsumerContext A = context("pipeline-a", "source-a", 1);
    private static final SrsConsumerContext B = context("pipeline-b", "source-b", 1);
    @ParameterizedTest
    @CsvSource({"1,1", "2,8", "2,80"})
    void loadPredictionReadsOnlyOriginalConsumersWithoutPeerOrModelScans(int ownSources, int peers) {
        withBusiness(f -> {
            var expected = new java.util.ArrayList<io.tapstate.spi.store.SrsConsumerProgress>();
            for (int i = 0; i < ownSources; i++) {
                var context = i == 0 ? A : context(A.key().pipelineId(), "second-source", A.stateGeneration());
                f.consumers().attach(context, 0, Set.of("orders"), f.fenceA()).orElseThrow();
                expected.add(f.consumers().progress(context).orElseThrow());
            }
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            var records = new MongoSrsConsumerRecords(SCOPE);
            for (int i = 1; i < peers; i++) {
                var context = context("peer-" + i, "other-source", 1);
                var fence = new WorkloadClaimFence(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION,
                        context.key().pipelineId()), f.pipelineB().owner(), 1, 1, 1);
                var member = new io.tapstate.spi.store.SrsConsumerMembership(context, 1, false, Set.of("orders"), fence);
                SystemCollections.SRS_CONSUMERS.on(f.database()).insertOne(records.membership(member));
                SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).insertOne(records.cursor(member,
                        records.initial(context.key(), Set.of("orders")), 0));
            }
            if (peers > 1) SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id", CHAIN),
                    new Document("$inc", new Document("consumerCount", (long) peers - 1).append("consumerRevision", (long) peers - 1)));
            var sources = expected.stream().map(v -> v.context().key().sourceId()).collect(java.util.stream.Collectors.toSet());
            var commands = new java.util.ArrayList<String>();
            MongoCommandObservation.COLLECTIONS.set((name, collection) -> commands.add(name + ":" + collection));
            try {
                var admitted = new MongoSrsConsumerStore(f.client(), f.database(), SCOPE, () -> {}, () -> {});
                var actual = admitted.startLoadProgress(positionScope(A), CHAIN, sources).orElseThrow();
                assertThat(actual.consumers()).containsExactlyInAnyOrderElementsOf(expected);
                assertThat(commands).containsExactly("aggregate:cache_identity", "aggregate:srs_chains", "commitTransaction:");
                assertThat(commands).noneMatch(v -> v.contains("srs_models") || v.contains("srs_events") || v.startsWith("getMore:"));
                System.out.println("LIGHT_START_PROGRESS own=" + ownSources + " peers=" + peers + " commands=" + commands.size());
            } finally { MongoCommandObservation.COLLECTIONS.remove(); }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"peer-cursor", "revision-sum", "model"})
    void startupVerifiesIntegrityDeferredByTheLoadPrediction(String fault) {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            f.chains().requestTables(CHAIN, Set.of("orders"), f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 1, Set.of("orders"), f.captureFence())).isTrue();
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "original"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(refs.get("orders"), -1)), true);
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            f.consumers().pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "original", refs), f.fenceA());
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            switch (fault) {
                case "peer-cursor" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(B));
                case "revision-sum" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id", CHAIN),
                        new Document("$inc", new Document("consumerRevision", 1L)));
                case "model" -> SystemCollections.SRS_MODELS.on(f.database()).deleteMany(new Document());
                default -> throw new AssertionError(fault);
            }
            var read = f.consumers().startLoadProgress(positionScope(A), CHAIN, Set.of(A.key().sourceId())).orElseThrow();
            assertThat(read.consumers()).hasSize(1);
            assertThat(io.tapstate.spi.store.StartLoad.of(io.tapstate.core.model.ReadMode.SNAPSHOT_AND_CDC,
                    A.key().pipelineId(), List.of(new io.tapstate.spi.store.SrsMeta(CHAIN, null,
                    read.consumers().stream().map(io.tapstate.spi.store.SrsConsumerProgress::cursor).toList(),
                    List.of(), read.chain().retention(), read.chain().epoch(), null))))
                    .isEqualTo(io.tapstate.spi.store.StartLoad.RESUME);
            assertThatThrownBy(() -> f.consumers().startupSnapshot(A, Map.of(), true)).isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"absent", "detached-current", "detached-older"})
    void lightLoadPredictionKeepsANewConsumerDistinctFromAnExistingProgress(String absence) {
        withBusiness(f -> {
            var requested = A;
            if (!absence.equals("absent")) {
                var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
                f.consumers().detach(A, member.revision(), f.fenceA()).orElseThrow();
                if (absence.equals("detached-older")) {
                    requested = new SrsConsumerContext(A.key(), 2);
                    replacePipelineScope(f, requested);
                }
            }
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(B),
                    new Document("$set", new Document("contentHash", "damaged-peer-payload")));
            assertThat(f.consumers().startLoadProgress(positionScope(requested), CHAIN, Set.of(A.key().sourceId()))
                    .orElseThrow().consumers()).isEmpty();
        });
    }

    @Test
    void lightLoadPredictionDoesNotTreatReorderedOwnIdsAsMissingProgress() {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var membershipTable = SystemCollections.SRS_CONSUMERS.on(f.database());
            var cursorTable = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database());
            var membership = membershipTable.find(id(A)).first();
            var cursor = cursorTable.find(id(A)).first();
            var reordered = new Document();
            for (String key : new java.util.ArrayList<>(membership.get("_id", Document.class).keySet()).reversed())
                reordered.put(key, membership.get("_id", Document.class).get(key));
            membershipTable.deleteOne(id(A));
            membershipTable.insertOne(new Document(membership).append("_id", reordered));
            cursorTable.deleteOne(id(A));
            cursorTable.insertOne(new Document(cursor).append("_id", reordered));
            assertThatThrownBy(() -> f.consumers().startLoadProgress(positionScope(A), CHAIN, Set.of(A.key().sourceId())))
                    .isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().planningProgress(positionScope(A), CHAIN, Set.of(A.key().sourceId())))
                    .isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"cursor", "membership", "hash", "generation"})
    void lightLoadPredictionRetainsOriginalConsumerRefusals(String fault) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            switch (fault) {
                case "cursor" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(A));
                case "membership" -> SystemCollections.SRS_CONSUMERS.on(f.database()).deleteOne(id(A));
                case "hash" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),
                        new Document("$set", new Document("contentHash", "damaged")));
                case "generation" -> replacePipelineScope(f, new SrsConsumerContext(A.key(), 2));
                default -> throw new AssertionError(fault);
            }
            var original = fault.equals("generation") ? new SrsConsumerContext(A.key(), 2) : A;
            assertThatThrownBy(() -> f.consumers().startLoadProgress(positionScope(original), CHAIN, Set.of(A.key().sourceId())))
                    .isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest
    @CsvSource({"1,1", "1,8", "2,8", "2,80"})
    void startPredictionReadsOnlyItsOwnSourceCursorsInOneSnapshot(int ownSources, int peers) {
        withBusiness(f -> {
            var selected = new java.util.LinkedHashMap<SrsConsumerContext,io.tapstate.spi.store.SrsConsumerProgress>();
            for (int index = 0; index < ownSources; index++) {
                var context = index == 0 ? A : context(A.key().pipelineId(), "second-source", A.stateGeneration());
                f.consumers().attach(context,0,Set.of("orders"),f.fenceA()).orElseThrow();
                selected.put(context,f.consumers().progress(context).orElseThrow());
            }
            var records = new MongoSrsConsumerRecords(SCOPE);
            var addedMembers = new java.util.ArrayList<Document>();
            var addedCursors = new java.util.ArrayList<Document>();
            SrsConsumerContext damaged = null;
            for (int index = 0; index < peers; index++) {
                var context = index == 0 ? B : context("sibling-" + index,"other-source",1);
                if (index == 0) f.consumers().attach(context,0,Set.of("orders"),f.fenceB()).orElseThrow();
                else {
                    // Peer control facts do not grant an execution lease or require a running job.
                    var fence = new WorkloadClaimFence(new WorkloadClaimKey(SCOPE.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,
                            context.key().pipelineId()),f.pipelineB().owner(),1,1,1);
                    var member = new io.tapstate.spi.store.SrsConsumerMembership(context,1,false,Set.of("orders"),fence);
                    addedMembers.add(records.membership(member));
                    addedCursors.add(records.cursor(member,records.initial(context.key(),Set.of("orders")),0));
                }
                damaged = context;
            }
            if (!addedMembers.isEmpty()) {
                SystemCollections.SRS_CONSUMERS.on(f.database()).insertMany(addedMembers);
                SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).insertMany(addedCursors);
                SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id",CHAIN),
                        new Document("$inc",new Document("consumerCount",(long)addedMembers.size()).append("consumerRevision",(long)addedMembers.size())));
            }
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(damaged),
                    new Document("$set",new Document("contentHash","damaged-peer-cursor")));
            var commands = new java.util.ArrayList<String>();
            MongoCommandObservation.COLLECTIONS.set((command,collection) -> commands.add(command + ":" + collection));
            try {
                var result = f.consumers().planningProgress(positionScope(A),CHAIN,
                        selected.keySet().stream().map(context -> context.key().sourceId()).collect(java.util.stream.Collectors.toSet())).orElseThrow();
                assertThat(result.chain().miningChainId()).isEqualTo(CHAIN);
                assertThat(result.consumers()).containsExactlyInAnyOrderElementsOf(selected.values());
                assertThat(commands.stream().filter(("find:" + SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.collectionName())::equals))
                        .as("one cursor-header closure and one batch of this pipeline's payloads").hasSize(2);
                assertThat(commands.stream().filter(value -> value.startsWith("commitTransaction:"))).hasSize(1);
                assertThat(commands.stream().filter(value -> value.startsWith("getMore:"))).hasSize(peers >= 64 ? 2 : 0);
                assertThat(commands).as("metadata closure and own payload batches have a bounded command budget").hasSizeLessThanOrEqualTo(24);
                System.out.println("START_PLANNING_COMMAND_BUDGET own_sources=" + ownSources + " peers=" + peers + " commands=" + commands.size());
            } finally { MongoCommandObservation.COLLECTIONS.remove(); }
            assertThatThrownBy(() -> f.consumers().attached(CHAIN)).isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings={"peer-generation","peer-revision","peer-format","peer-progress-revision","own-payload","own-cursor","own-membership","peer-cursor","revision-sum"})
    void startPredictionKeepsControlAndOwnProgressRefusals(String damage) {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            switch (damage) {
                case "peer-generation" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(B),new Document("$set",new Document("stateGeneration",2L)));
                case "peer-revision" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(B),new Document("$set",new Document("membershipRevision",2L)));
                case "peer-format" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(B),new Document("$set",new Document("formatVersion",2)));
                case "peer-progress-revision" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(B),new Document("$set",new Document("progressRevision",-1L)));
                case "own-payload" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("contentHash","bad-own-hash")));
                case "own-cursor" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(A));
                case "own-membership" -> SystemCollections.SRS_CONSUMERS.on(f.database()).deleteOne(id(A));
                case "peer-cursor" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(B));
                case "revision-sum" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id",CHAIN),new Document("$inc",new Document("consumerRevision",1L)));
                default -> throw new AssertionError(damage);
            }
            var before=root(f); var own=cursor(f,A); var peer=cursor(f,B);
            assertThatThrownBy(() -> f.consumers().planningProgress(positionScope(A),CHAIN,Set.of(A.key().sourceId())))
                    .isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before); assertThat(cursor(f,A)).isEqualTo(own); assertThat(cursor(f,B)).isEqualTo(peer);
        });
    }

    @ParameterizedTest @ValueSource(strings={"absent","detached-current","detached-older"})
    void startPredictionProvesAbsenceWithoutParsingPeerProgress(String absence) {
        withBusiness(f -> {
            var original=A;
            if (!absence.equals("absent")) {
                var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
                f.consumers().detach(A,member.revision(),f.fenceA()).orElseThrow();
                if (absence.equals("detached-older")) {
                    original=new SrsConsumerContext(A.key(),2); replacePipelineScope(f,original);
                }
            }
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(B),new Document("$set",new Document("contentHash","bad-peer-hash")));
            assertThat(f.consumers().planningProgress(positionScope(original),CHAIN,Set.of(A.key().sourceId())).orElseThrow().consumers()).isEmpty();
            assertThatThrownBy(() -> f.consumers().attached(CHAIN)).isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void snapshotPinUsesAtMostNineReadsPerAttemptAndCommitsAfterALateConflict(boolean conflict) {
        withBusiness(f -> {
            var tables = Set.of("orders");
            var previous = publishSnapshotModels(f, tables).get("orders");
            f.chains().requestTables(CHAIN, tables, f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 1, tables, f.captureFence())).isTrue();
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "retained"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(previous, -1)), true);
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            f.chains().openEpoch(CHAIN, 1, f.captureFence()).orElseThrow();
            var next = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 2, 0);
            models(f).publishInitial(next, ring("orders"), new SourceTable("orders", List.of(new SourceField("id", "int")),
                    List.of(), List.of()), f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 2, tables, f.captureFence())).isTrue();
            var member = f.consumers().attach(A, 0, tables, f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(2, "new-snapshot", Map.of("orders", next));
            var competingRenewal = new java.util.concurrent.atomic.AtomicBoolean(conflict);
            var conflicts = new java.util.concurrent.atomic.AtomicInteger();
            var commands = new java.util.LinkedHashMap<Long, List<String>>();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    var body = event.getCommand();
                    if (!body.containsKey("autocommit")) return;
                    String command = event.getCommandName();
                    var target = body.get(command);
                    String collection = target != null && target.isString() ? target.asString().getValue() : "-";
                    commands.computeIfAbsent(body.getNumber("txnNumber").longValue(), ignored -> new ArrayList<>())
                            .add(command + ":" + collection);
                    if ("update".equals(command) && "workload_claims".equals(collection)
                            && competingRenewal.compareAndSet(true, false)) {
                        var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
                        var renewed = claims.renew(f.pipelineA(), Duration.ofSeconds(30)).orElseThrow();
                        assertThat(WorkloadClaimFence.from(renewed)).isEqualTo(f.fenceA());
                    }
                    if (conflict) java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(350).toNanos());
                }
                @Override public void commandFailed(com.mongodb.event.CommandFailedEvent event) {
                    if (event.getThrowable() instanceof com.mongodb.MongoCommandException failure && failure.getCode() == 112)
                        conflicts.incrementAndGet();
                }
            };
            try (var observed = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build())) {
                var store = runtimeConsumer(observed, observed.getDatabase(f.database().getName()));
                var start = System.nanoTime();
                assertThat(store.pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
                long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                var durable = f.consumers().progress(A).orElseThrow();
                assertThat(durable.snapshot()).isEqualTo(pin);
                assertThat(durable.revision()).isOne();
                assertThat(conflicts).hasValue(conflict ? 1 : 0);
                assertThat(commands).hasSize(conflict ? 2 : 1);
                var successful = commands.lastEntry().getValue();
                var writes = successful.stream().filter(c -> c.startsWith("update:") || c.startsWith("findAndModify:")).toList();
                assertThat(writes).containsExactly("findAndModify:srs_chains", "update:pipeline_state",
                        "update:srs_consumer_offsets", "update:workload_claims");
                assertThat(successful).endsWith("commitTransaction:-");
                var reads = commands.values().stream().map(rows -> rows.stream()
                        .filter(c -> c.startsWith("find:") || c.startsWith("aggregate:") || c.startsWith("getMore:")).count()).toList();
                System.out.printf("SNAPSHOT_PIN_READS late_conflict=%b reads=%s wall_ms=%d commands=%s%n",
                        conflict, reads, millis, commands.values());
                assertThat(reads).allSatisfy(readCount -> assertThat(readCount).isLessThanOrEqualTo(9));
                assertThat(millis).isLessThan(15_000);
            }
        });
    }

    @Test void startPredictionRefusesLostRootsAndCallerLossBeforeOwnPayloads() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var active=new java.util.concurrent.atomic.AtomicBoolean(true);
            var guarded=f.consumers().guardedBy(() -> {
                if (!active.get()) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            });
            var cursorReads=new java.util.concurrent.atomic.AtomicInteger();
            MongoCommandObservation.COLLECTIONS.set((command,collection) -> {
                if (command.equals("find") && collection.equals(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.collectionName())) {
                    cursorReads.incrementAndGet(); active.set(false);
                }
            });
            try {
                assertThatThrownBy(() -> guarded.planningProgress(positionScope(A),CHAIN,Set.of(A.key().sourceId())))
                        .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                assertThat(cursorReads).hasValue(1);
            } finally { MongoCommandObservation.COLLECTIONS.remove(); }
            SystemCollections.SRS_CHAINS.on(f.database()).deleteOne(new Document("_id",CHAIN));
            assertThatThrownBy(() -> f.consumers().planningProgress(positionScope(A),CHAIN,Set.of(A.key().sourceId())))
                    .isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.SRS_CHECKPOINT_UNPROVEN));
            assertThat(f.consumers().planningProgress(positionScope(A),"no-history",Set.of(A.key().sourceId()))).isEmpty();
        });
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void startPredictionNeverRetargetsAnotherActiveGeneration(boolean future) {
        withBusiness(f -> {
            var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var requested=new SrsConsumerContext(A.key(),2);
            if (future) {
                var records=new MongoSrsConsumerRecords(SCOPE);
                var next=new io.tapstate.spi.store.SrsConsumerMembership(requested,member.revision(),false,member.tables(),member.commandFence());
                var cursor=f.consumers().progress(A).orElseThrow();
                SystemCollections.SRS_CONSUMERS.on(f.database()).replaceOne(id(A),records.membership(next));
                SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).replaceOne(id(A),records.cursor(next,cursor.cursor(),cursor.revision()));
                requested=A;
            } else replacePipelineScope(f,requested);
            var original=requested;
            assertThatThrownBy(() -> f.consumers().planningProgress(positionScope(original),CHAIN,Set.of(A.key().sourceId())))
                    .isInstanceOf(TapstateException.class);
        });
    }

    @Test void startPredictionKeepsBusinessAndLiveIndexGuards() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var commands=new java.util.ArrayList<String>();
            MongoCommandObservation.CURRENT.set(commands::add);
            try {
                var other=new io.tapstate.spi.store.PipelineIntentScope("other-cluster",SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration());
                assertThatThrownBy(() -> f.consumers().planningProgress(other,CHAIN,Set.of(A.key().sourceId()))).isInstanceOf(TapstateException.class);
                assertThat(commands).isEmpty();
            } finally { MongoCommandObservation.CURRENT.remove(); }
            SystemCollections.SRS_CONSUMERS.on(f.database()).dropIndex(SystemCollections.SRS_CONSUMERS.indexes().getFirst().indexName());
            var before=root(f);
            assertThatThrownBy(() -> f.consumers().planningProgress(positionScope(A),CHAIN,Set.of(A.key().sourceId()))).isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before);
        });
    }

    @Test void startPredictionDoesNotAdoptACollationAliasForACursorIdentity() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var row=cursor(f,A);
            var table=SystemCollections.CACHE_SRS_CONSUMER_OFFSETS;
            table.on(f.database()).drop();
            f.database().createCollection(table.collectionName(),new com.mongodb.client.model.CreateCollectionOptions()
                    .collation(com.mongodb.client.model.Collation.builder().locale("en")
                            .collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build()));
            try (var session=f.client().startSession()) {
                for (var index:table.indexes()) IndexEnsure.createInTransaction(session,table.on(f.database()),index);
            }
            table.on(f.database()).insertOne(row);
            assertThat(f.consumers().planningProgress(positionScope(A),CHAIN,Set.of(A.key().sourceId())).orElseThrow().consumers()).hasSize(1);
            table.on(f.database()).deleteOne(id(A));
            var alias=new Document(row);
            alias.put("_id",new Document(row.get("_id",Document.class)).append("consumer",A.key().consumerId().toUpperCase(java.util.Locale.ROOT)));
            table.on(f.database()).insertOne(alias);
            assertThatThrownBy(() -> f.consumers().planningProgress(positionScope(A),CHAIN,Set.of(A.key().sourceId())))
                    .isInstanceOf(TapstateException.class);
            assertThat(table.on(f.database()).find().first()).isEqualTo(alias);
        });
    }
    @ParameterizedTest @ValueSource(strings={"revision","state","owner","detached","hash"})
    void validationMembershipRefusesChangedOriginalCoordinates(String changed){
        withBusiness(f->{
            var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var scope=new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration());
            var original=io.tapstate.spi.store.SrsConsumerRun.of(scope,member);var fence=member.commandFence();
            var next=new io.tapstate.spi.store.SrsConsumerMembership(changed.equals("state")?new SrsConsumerContext(A.key(),A.stateGeneration()+1):A,
                    changed.equals("revision")?member.revision()+1:member.revision(),changed.equals("detached"),changed.equals("detached")?Set.of():member.tables(),
                    changed.equals("owner")?new WorkloadClaimFence(fence.key(),new WorkloadOwner("other","other-boot"),fence.claimGeneration(),fence.executionGeneration(),fence.topologyRevision()):fence);
            var row=new MongoSrsConsumerRecords(SCOPE).membership(next);if(changed.equals("hash"))row.put("contentHash","a".repeat(64));
            SystemCollections.SRS_CONSUMERS.on(f.database()).replaceOne(id(A),row);
            if(changed.equals("hash"))assertThatThrownBy(()->f.consumers().attached(original,"orders")).isInstanceOf(TapstateException.class);
            else assertThat(f.consumers().attached(original,"orders")).isFalse();
            assertThat(membership(f,A)).isEqualTo(row);
        });
    }
    @Test void validationMembershipReadsOnlyItsOriginalRelationAndRejectsOversizedMetadata(){
        withBusiness(f->{
            var a=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var original=io.tapstate.spi.store.SrsConsumerRun.of(new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration()),a);
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(B),new Document("$set",new Document("progress",new Document("unrelated","x".repeat(512*1024)))));
            var before=membership(f,A);
            assertThat(f.consumers().attached(original,"orders")).isTrue();assertThat(f.consumers().attached(original,"missing")).isFalse();
            assertThatThrownBy(()->f.consumers().find(A.key())).isInstanceOf(TapstateException.class);assertThat(membership(f,A)).isEqualTo(before);
            var large=new TreeSet<String>();for(int index=0;index<1024;index++)large.add(index+"x".repeat(512));
            var oversized=new io.tapstate.spi.store.SrsConsumerMembership(a.context(),a.revision(),false,large,a.commandFence());
            SystemCollections.SRS_CONSUMERS.on(f.database()).replaceOne(id(A),new MongoSrsConsumerRecords(SCOPE).membership(oversized));
            assertThatThrownBy(()->f.consumers().attached(original,"orders")).isInstanceOf(TapstateException.class).hasMessageContaining("validation.controlMetadata");
        });
    }

    @Test void readLeaseFinishesOldEventsAndRevalidatesTheFirstEventOfTheNextCaptureEpoch() {
        withBusiness(f -> {
            String table = "orders";
            String ring = ring(table);
            var log = preparedTrimLog(f);
            f.chains().requestTables(CHAIN, Set.of(table), f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 1, Set.of(table), f.captureFence())).isTrue();

            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(), f.database().getName(), "find", "srs_models")) {
                var reader = new MongoSrsConsumerStore(witness.client, witness.database, SCOPE, () -> {}, null,
                        MongoSrsLease.selected());
                var member = reader.attach(A, 0, Set.of(table), f.fenceA()).orElseThrow();
                var run = SrsConsumerRun.of(positionScope(A), member);
                assertThat(reader.startReadingAfter(A, member.revision(), 0, table, -1, f.fenceA())).isTrue();
                var old = log.load(ring, 0).orElseThrow();
                var first = verify(reader, run, table, ring, 0, old);
                int firstProof = witness.confirmedCommands();
                assertThat(firstProof).isPositive();

                assertThat(f.chains().openEpoch(CHAIN, 1, f.captureFence())).hasValue(2);
                assertThat(f.chains().publishServingTables(CHAIN, 2, Set.of(table), f.captureFence())).isTrue();
                var nextKey = new SrsModelKey(CHAIN, table, SCOPE.cacheIncarnation(), 2, 0);
                models(f).publishInitial(nextKey, ring, first.table(), f.captureFence());
                var next = new SrsLogRecord("t5", Op.INSERT, 2, null, Map.of("id", 5), 0, f.captureFence(), 2);
                var latest = new SrsLogRecord("t6", Op.INSERT, 3, null, Map.of("id", 6), 0, f.captureFence(), 2);
                log.storeAll(ring, 5, List.of(next, latest));

                for (long sequence = 1; sequence < 5; sequence++) {
                    assertThat(verify(reader, run, table, ring, sequence, log.load(ring, sequence).orElseThrow())).isEqualTo(first);
                }
                assertThat(witness.confirmedCommands()).isEqualTo(firstProof);
                assertThat(verify(reader, run, table, ring, 5, log.load(ring, 5).orElseThrow()).key()).isEqualTo(nextKey);
                int nextProof = witness.confirmedCommands();
                assertThat(nextProof).isGreaterThan(firstProof);
                assertThat(verify(reader, run, table, ring, 6, log.load(ring, 6).orElseThrow()).key()).isEqualTo(nextKey);
                assertThat(verify(reader, run, table, ring, 3, log.load(ring, 3).orElseThrow())).isEqualTo(first);
                assertThat(witness.confirmedCommands()).isEqualTo(nextProof);

                assertThat(reader.detach(A, member.revision(), f.fenceA())).isPresent();
                assertThat(reader.verifyReadEvent(run, table, ring, 6, 2, 0, f.captureFence())).isEmpty();
            }
        });
    }

    private static SrsModel verify(MongoSrsConsumerStore reader, SrsConsumerRun run, String table,
            String ring, long sequence, SrsLogRecord event) {
        return reader.verifyReadEvent(run, table, ring, sequence, event.epoch(), event.schemaVer(),
                event.captureFence()).orElseThrow();
    }

    @Test void cursorLeaseSkipsRepeatedProofButFencesEachWriteAndKeepsTheRecoveryFloor() {
        withBusiness(f -> {
            String table = "orders";
            preparedTrimLog(f);
            f.chains().requestTables(CHAIN, Set.of(table), f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 1, Set.of(table), f.captureFence())).isTrue();
            var model = new SrsModelKey(CHAIN, table, SCOPE.cacheIncarnation(), 1, 0);
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 1), "t0"),
                    Map.of(table, new io.tapstate.spi.store.SrsTableCheckpoint(model, 0)));
            assertThat(f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence())).isPresent();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(), f.database().getName(), "find", "srs_models")) {
                var reader = new MongoSrsConsumerStore(witness.client, witness.database, SCOPE, () -> {}, null, MongoSrsLease.selected());
                var member = reader.attach(A, 0, Set.of(table), f.fenceA()).orElseThrow();
                assertThat(reader.startReadingAfter(A, member.revision(), 0, table, -1, f.fenceA())).isTrue();
                assertThat(reader.advanceReadSequence(A, member.revision(), table, 0, f.fenceA())).isTrue();
                int proofQueries = witness.confirmedCommands();
                assertThat(proofQueries).isPositive();
                assertThat(reader.advanceReadSequence(A, member.revision(), table, 1, f.fenceA())).isTrue();
                assertThat(witness.confirmedCommands()).isEqualTo(proofQueries);
                var held = f.consumers().progress(A).orElseThrow();
                assertThat(held.cursor().perTableSeq()).containsEntry(table, 1L);
                assertThat(held.ringDoneThrough()).containsEntry(table, -1L);
                assertThat(held.cursor().sinkAcked()).isNull();

                var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database());
                claims.updateOne(new Document("_id", MongoWorkloadClaimStore.id(f.pipelineA().key())),
                        new Document("$inc", new Document("claimGeneration", 1L)));
                assertThatThrownBy(() -> reader.advanceReadSequence(A, member.revision(), table, 2, f.fenceA()))
                        .satisfies(MongoSrsConsumerStoreIT::fenced);
                assertThat(f.consumers().progress(A)).contains(held);
            }
        });
    }

    @Test void sharedLossPreflightKeepsUnleasedConsumersAndReadsNoLostPayloadOrMutableAuthority() {
        withBusiness(f -> {
            preparedTrimLog(f);
            f.chains().requestTables(CHAIN,Set.of("orders"),f.captureFence());
            f.chains().publishServingTables(CHAIN,1,Set.of("orders"),f.captureFence());
            var model=new SrsModelKey(CHAIN,"orders",SCOPE.cacheIncarnation(),1,0);
            var checkpoint=new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,4),"t4"),
                    Map.of("orders",new io.tapstate.spi.store.SrsTableCheckpoint(model,4)));
            f.chains().checkpoint(CHAIN,0,checkpoint,f.captureFence()).orElseThrow();
            var a=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var b=f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var claims=new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            assertThat(claims.release(f.pipelineA())).isTrue();assertThat(claims.release(f.pipelineB())).isTrue();assertThat(claims.release(f.capture())).isTrue();
            var prepared=retainedCapturePreparation(f);var before=controlRows(f);
            SystemCollections.SRS_MODELS.on(f.database()).deleteMany(new Document());
            SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).deleteMany(new Document());
            SystemCollections.SRS_EVENTS.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> f.chains().load(CHAIN)).isInstanceOf(TapstateException.class);
            var snapshot=f.consumers().inspectSharedRecovery(prepared);
            assertThat(snapshot.previous()).isEqualTo(prepared);assertThat(snapshot.captureFence()).isEqualTo(f.captureFence());
            assertThat(snapshot.consumers()).containsExactly(a,b);assertThat(snapshot.pipelines().keySet()).containsExactly("pipeline-a","pipeline-b");
            assertThat(snapshot.pipelines().get("pipeline-a").scope()).isEqualTo(positionScope(A));
            assertThat(snapshot.pipelines().get("pipeline-b").claim()).isEqualTo(f.fenceB());
            assertThat(f.consumers().inspectSharedRecovery(prepared)).isEqualTo(snapshot);
            assertThat(controlRows(f)).isEqualTo(before);
            assertThat(SystemCollections.SRS_MODELS.on(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
        });
    }

    @Test void sharedControlPreflightUsesOneReadOnlyCacheSnapshot() {
        withBusiness(f -> {
            preparedTrimLog(f);f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var before=controlRows(f);var prepared=retainedCapturePreparation(f);
            var transactions=new java.util.HashSet<String>();var reads=new java.util.concurrent.atomic.AtomicInteger();
            var unbound=new java.util.concurrent.atomic.AtomicBoolean();var mutated=new java.util.concurrent.atomic.AtomicBoolean();
            var listener=new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if(!f.database().getName().equals(event.getDatabaseName()))return;
                    if(Set.of("update","insert","delete","findAndModify").contains(event.getCommandName()))mutated.set(true);
                    if(!Set.of("find","aggregate","getMore").contains(event.getCommandName()))return;
                    reads.incrementAndGet();
                    if(!event.getCommand().containsKey("txnNumber"))unbound.set(true);
                    else transactions.add(event.getCommand().get("txnNumber").toString());
                }
            };
            try(var observed=MongoClients.create(com.mongodb.MongoClientSettings.builder().applyConnectionString(
                    new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
                var consumers=new MongoSrsConsumerStore(observed,observed.getDatabase(f.database().getName()),SCOPE);
                assertThat(consumers.inspectSharedRecovery(prepared).pipelines()).hasSize(2);
            }
            assertThat(reads.get()).isPositive();assertThat(transactions).hasSize(1);assertThat(unbound).isFalse();assertThat(mutated).isFalse();
            assertThat(controlRows(f)).isEqualTo(before);
        });
    }

    @Test void sharedControlFingerprintObservesProgressAndAcceptsOnlyMonotonicRetainedClaims() {
        withBusiness(f -> {
            preparedTrimLog(f);var a=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var b=f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();var prepared=retainedCapturePreparation(f);
            assertThat(f.consumers().startReadingAfter(A,a.revision(),0,"orders",0,f.fenceA())).isTrue();
            var initial=f.consumers().inspectSharedRecovery(prepared);
            assertThat(f.consumers().advanceReadSequence(A,a.revision(),"orders",1,f.fenceA())).isTrue();
            var advanced=f.consumers().inspectSharedRecovery(prepared);
            assertThat(advanced.controlHash()).isNotEqualTo(initial.controlHash());assertThat(advanced.consumers()).containsExactly(a,b);
            var claims=new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            assertThat(claims.release(f.pipelineB())).isTrue();
            var next=claims.acquire(f.pipelineB().key(),new WorkloadOwner("next-node","next-boot"),2,Duration.ofSeconds(30)).claim();
            var before=controlRows(f);var retained=f.consumers().inspectSharedRecovery(prepared);
            assertThat(retained.consumers()).containsExactly(a,b);
            assertThat(retained.pipelines().get("pipeline-b").claim()).isEqualTo(WorkloadClaimFence.from(next));
            assertThat(retained.controlHash()).isNotEqualTo(advanced.controlHash());assertThat(controlRows(f)).isEqualTo(before);
        });
    }

    @ParameterizedTest @ValueSource(strings={"checkpoint","read","missing"})
    void retainedBoundsMustCoverKnownControlsWhileMissingBoundsNeverInventAHighWater(String state) {
        withBusiness(f -> {
            preparedTrimLog(f);f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            if(state.equals("checkpoint")) {
                f.chains().requestTables(CHAIN,Set.of("orders"),f.captureFence());f.chains().publishServingTables(CHAIN,1,Set.of("orders"),f.captureFence());
                f.chains().checkpoint(CHAIN,0,new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,4),"t4"),Map.of("orders",
                        new io.tapstate.spi.store.SrsTableCheckpoint(new SrsModelKey(CHAIN,"orders",SCOPE.cacheIncarnation(),1,0),4))),f.captureFence()).orElseThrow();
            } else {
                assertThat(f.consumers().startReadingAfter(A,1,0,"orders",2,f.fenceA())).isTrue();
                assertThat(f.consumers().advanceReadSequence(A,1,"orders",4,f.fenceA())).isTrue();
            }
            var previous=retainedCapturePreparation(f);var bounds=SystemCollections.SRS_LOG_BOUNDS.on(f.database());
            if(state.equals("missing"))bounds.deleteOne(new Document("_id",MongoSrsLogStore.boundsId(ring("orders"))));
            else bounds.updateOne(new Document("_id",MongoSrsLogStore.boundsId(ring("orders"))),new Document("$set",new Document("largestSequence",3L)));
            SystemCollections.SRS_MODELS.on(f.database()).deleteMany(new Document());SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).deleteMany(new Document());
            SystemCollections.SRS_EVENTS.on(f.database()).deleteMany(new Document());var before=controlRows(f);
            if(state.equals("missing"))assertThat(f.consumers().inspectSharedRecovery(previous).consumers()).hasSize(1);
            else assertThatThrownBy(() -> f.consumers().inspectSharedRecovery(previous)).isInstanceOf(TapstateException.class);
            assertThat(controlRows(f)).isEqualTo(before);
        });
    }

    private static io.tapstate.spi.store.CapturePreparation retainedCapturePreparation(Fixture f) {
        var row=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",MongoWorkloadClaimStore.id(f.captureFence().key()))).first();
        return MongoCaptureCachePreparations.requirePreparation(row,f.captureFence().key(),SCOPE);
    }
    @Test void sharedControlPreflightDistinguishesAProvenEmptyPoolFromMissingRelations() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().detach(A,1,f.fenceA()).orElseThrow();
            var before=controlRows(f);var snapshot=f.consumers().inspectSharedRecovery(retainedCapturePreparation(f));
            assertThat(snapshot.consumers()).isEmpty();assertThat(snapshot.pipelines()).isEmpty();
            assertThat(controlRows(f)).isEqualTo(before);
            SystemCollections.SRS_CONSUMERS.on(f.database()).deleteOne(id(A));
            assertThatThrownBy(() -> f.consumers().inspectSharedRecovery(retainedCapturePreparation(f))).isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest @ValueSource(strings={"member","cursor","root-type","capture-preparation","pipeline-scope","claim-regression",
            "member-hash","bounds-type","checkpoint-chain","checkpoint-scope","missing-head"})
    void sharedControlPreflightRefusesIncompleteOrCrossedOriginalControls(String fault) {
        withBusiness(f -> {
            preparedTrimLog(f);f.chains().requestTables(CHAIN,Set.of("orders"),f.captureFence());
            f.chains().publishServingTables(CHAIN,1,Set.of("orders"),f.captureFence());
            var model=new SrsModelKey(CHAIN,"orders",SCOPE.cacheIncarnation(),1,0);
            f.chains().checkpoint(CHAIN,0,new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,4),"t4"),
                    Map.of("orders",new io.tapstate.spi.store.SrsTableCheckpoint(model,4))),f.captureFence()).orElseThrow();
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var prepared=retainedCapturePreparation(f);var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database());
            switch(fault) {
                case "member" -> SystemCollections.SRS_CONSUMERS.on(f.database()).deleteOne(id(A));
                case "cursor" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(A));
                case "root-type" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id",CHAIN),new Document("$set",new Document("consumerRevision",2)));
                case "capture-preparation" -> claims.updateOne(new Document("_id",MongoWorkloadClaimStore.id(f.captureFence().key())),new Document("$unset",new Document("capturePreparation","")));
                case "pipeline-scope" -> claims.updateOne(new Document("_id",MongoWorkloadClaimStore.id(f.fenceA().key())),new Document("$set",new Document("pipelineScope.stateGeneration",2L)));
                case "claim-regression" -> claims.updateOne(new Document("_id",MongoWorkloadClaimStore.id(f.fenceA().key())),new Document("$set",new Document("executionGeneration",0L)));
                case "member-hash" -> SystemCollections.SRS_CONSUMERS.on(f.database()).updateOne(id(A),new Document("$set",new Document("contentHash","b".repeat(64))));
                case "bounds-type" -> SystemCollections.SRS_LOG_BOUNDS.on(f.database()).updateOne(new Document("_id",MongoSrsLogStore.boundsId(ring("orders"))),new Document("$set",new Document("largestSequence",4)));
                case "missing-head" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id",CHAIN),new Document("$set",new Document("modelContextHeads",List.of())));
                case "checkpoint-chain","checkpoint-scope" -> {
                    var root=root(f);var checkpoint=root.get("checkpoint",Document.class);
                    var key=checkpoint.getList("tables",Document.class).getFirst().get("model",Document.class);
                    if(fault.equals("checkpoint-chain"))key.put("chain","different-chain");
                    else key.get("captureContext",Document.class).put("cacheIncarnation","different-cache");
                    var content=new Document(checkpoint);content.remove("contentHash");
                    checkpoint.put("contentHash",io.tapstate.core.model.canonical.CanonicalHash.ofText(content.toJson()));
                    SystemCollections.SRS_CHAINS.on(f.database()).replaceOne(new Document("_id",CHAIN),root);
                }
                default -> throw new AssertionError(fault);
            }
            SystemCollections.SRS_MODELS.on(f.database()).deleteMany(new Document());SystemCollections.SRS_EVENTS.on(f.database()).deleteMany(new Document());
            var before=controlRows(f);
            assertThatThrownBy(() -> f.consumers().inspectSharedRecovery(prepared)).isInstanceOf(TapstateException.class);
            assertThat(controlRows(f)).isEqualTo(before);
        });
    }
    private static Map<String,List<Document>> controlRows(Fixture f) {
        var rows=new java.util.TreeMap<String,List<Document>>();
        for(var table:List.of(SystemCollections.CACHE_IDENTITY,SystemCollections.CACHE_SCHEMA_MIGRATIONS,SystemCollections.SRS_CHAINS,
                SystemCollections.CACHE_WORKLOAD_CLAIMS,SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS,SystemCollections.SRS_LOG_BOUNDS))
            rows.put(table.collectionName(),table.on(f.database()).find().into(new java.util.ArrayList<>()));
        return rows;
    }

    @Test void aWideSnapshotProgressReadProvesItsExactReferencesInOneBoundedTransaction() {
        withBusiness(f -> {
            var tables = java.util.stream.IntStream.range(0, 12).mapToObj(i -> "table-" + i)
                    .collect(java.util.stream.Collectors.toSet());
            var refs = publishSnapshotModels(f, tables);
            var member = f.consumers().attach(A, 0, tables, f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "original-seam", refs);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            var reads = new java.util.concurrent.atomic.AtomicInteger();
            var unbound = new java.util.concurrent.atomic.AtomicBoolean();
            var transactions = new java.util.HashSet<String>();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if (!event.getDatabaseName().equals(f.database().getName()) || !event.getCommandName().equals("find")) return;
                    reads.incrementAndGet();
                    if (!event.getCommand().containsKey("txnNumber")) unbound.set(true);
                    else transactions.add(event.getCommand().get("txnNumber").toString());
                }
            };
            try (var observed = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build())) {
                var consumers = new MongoSrsConsumerStore(observed, observed.getDatabase(f.database().getName()), SCOPE);
                assertThat(consumers.progress(A).orElseThrow().snapshot()).isEqualTo(pin);
                assertThat(unbound).isFalse(); assertThat(transactions).hasSize(1);
                assertThat(reads.get()).as("one progress read must not reopen the same snapshot proof for every table").isLessThanOrEqualTo(14);
            }
        });
    }

    @ParameterizedTest
    @CsvSource({"false,1", "true,1", "false,4", "true,4"})
    void retainedProgressSharesModelProofBetweenCheckpointAndSnapshot(boolean planning, int tables) {
        withBusiness(f -> {
            var selected = java.util.stream.IntStream.range(0, tables).mapToObj(i -> "table_" + i)
                    .collect(java.util.stream.Collectors.toSet());
            var refs = publishSnapshotModels(f, selected);
            f.chains().requestTables(CHAIN, selected, f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 1, selected, f.captureFence())).isTrue();
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "original"),
                    refs.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,
                            entry -> new io.tapstate.spi.store.SrsTableCheckpoint(entry.getValue(), -1))), true);
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            var member = f.consumers().attach(A, 0, selected, f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "original", refs);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            if (tables == 1) {
                f.chains().openEpoch(CHAIN, 1, f.captureFence()).orElseThrow();
                String table = selected.iterator().next();
                var next = new SrsModelKey(CHAIN, table, SCOPE.cacheIncarnation(), 2, 0);
                models(f).publishInitial(next, ring(table), new SourceTable(table,
                        List.of(new SourceField("id", "int")), List.of(), List.of()), f.captureFence());
            }
            try (var reads = new MongoCommandWitness(MONGO.getReplicaSetUrl(), f.database().getName(), "find", "srs_models")) {
                var consumer = new MongoSrsConsumerStore(reads.client, reads.database, SCOPE);
                var retained = planning ? consumer.planningProgress(positionScope(A), CHAIN, Set.of(A.key().sourceId()))
                        .orElseThrow().consumers().getFirst() : consumer.progress(A).orElseThrow();
                assertThat(retained.snapshot()).isEqualTo(pin);
                assertThat(reads.confirmedCommands()).as("both proofs share exact model bodies; history requires a distinct ring existence check")
                        .isEqualTo(tables == 1 ? 2 : 1);
            }
        });
    }

    @Test void anAttachedSnapshotKeepsItsClosedModelContextUntilItsOwnDetach() {
        withBusiness(f -> {
            var refs=publishSnapshotModels(f,Set.of("orders"));var first=refs.get("orders");
            var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().pinSnapshot(A,member.revision(),0,new SrsSnapshotPin(1,"seam",refs),f.fenceA());
            f.chains().openEpoch(CHAIN,1,f.captureFence()).orElseThrow();
            var next=new SrsModelKey(CHAIN,"orders",SCOPE.cacheIncarnation(),2,0);
            models(f).publishInitial(next,ring("orders"),new SourceTable("orders",List.of(new SourceField("id","int")),List.of(),List.of()),f.captureFence());
            assertThat(models(f).reclaim(CHAIN,f.captureFence())).isZero();assertThat(models(f).require(first).key()).isEqualTo(first);
            f.consumers().detach(A,member.revision(),f.fenceA()).orElseThrow();
            assertThat(models(f).reclaim(CHAIN,f.captureFence())).isOne();assertThat(models(f).require(next).key()).isEqualTo(next);
            assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void independentConsumersKeepSeparateRelationsAndCursorsAcrossReopenAndDetach() {
        withBusiness(f -> {
            var a = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var b = f.consumers().attach(B, 0, Set.of("orders", "quiet"), f.fenceB()).orElseThrow();
            assertThat(f.consumers().attached(CHAIN).revision()).isEqualTo(2);
            assertThat(f.consumers().cursor(A).orElseThrow().perTableSeq()).containsEntry("orders", -1L);
            Document bCursor = cursor(f, B); Document bMember = membership(f, B);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE);
                assertThat(reopened.find(A.key())).contains(a);
                assertThat(reopened.attached(CHAIN).consumers()).containsExactly(a, b);
            }
            var detached = f.consumers().detach(A, 1, f.fenceA()).orElseThrow();
            assertThat(detached.detached()).isTrue(); assertThat(detached.revision()).isEqualTo(2);
            assertThat(f.consumers().cursor(A)).isEmpty();
            assertThat(f.consumers().attached(CHAIN).consumers()).containsExactly(b);
            assertThat(cursor(f, B)).isEqualTo(bCursor); assertThat(membership(f, B)).isEqualTo(bMember);
            assertThat(root(f).getLong("consumerCount")).isOne();
            f.consumers().detach(B, 1, f.fenceB()).orElseThrow();
            assertThat(root(f).getLong("consumerCount")).isZero();
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).countDocuments()).isOne();
        });
    }

    @Test
    void detachedRevisionMarkersFenceLateAttachAndLateDetachEvenWhileTheOriginalClaimIsLive() {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var marker = f.consumers().detach(A, 1, f.fenceA()).orElseThrow();
            assertThat(f.consumers().detach(A, 1, f.fenceA())).contains(marker);
            assertThat(f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isEmpty();
            var next = f.consumers().attach(A, marker.revision(), Set.of("orders"), f.fenceA()).orElseThrow();
            assertThat(next.revision()).isEqualTo(3);
            assertThat(f.consumers().detach(A, 1, f.fenceA())).isEmpty();
            assertThat(f.consumers().cursor(A)).isPresent();
        });
    }

    @Test
    void detachingBeforeAnInitialAttachStillLeavesAnOriginalRevisionBarrier() {
        withBusiness(f -> {
            var marker = f.consumers().detach(A, 0, f.fenceA()).orElseThrow();
            assertThat(marker.detached()).isTrue(); assertThat(marker.revision()).isOne();
            assertThat(f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isEmpty();
            assertThat(f.consumers().attached(CHAIN).consumers()).isEmpty();
        });
    }

    @Test
    void losingTheOnlyDetachedMarkerCannotMakeAnOldInitialAttachNewAgain() {
        withBusiness(f -> {
            f.consumers().detach(A, 0, f.fenceA()).orElseThrow();
            SystemCollections.SRS_CONSUMERS.on(f.database()).deleteMany(new Document()); Document before = root(f);
            assertThatThrownBy(() -> f.consumers().attached(CHAIN)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(root(f)).isEqualTo(before); assertThat(cursor(f, A)).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"member", "cursor", "root"})
    void aLostRequiredFactIsNotAnAbsentConsumerOrAnEmptyChain(String lost) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var resource = switch (lost) { case "member" -> SystemCollections.SRS_CONSUMERS; case "cursor" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS; default -> SystemCollections.SRS_CHAINS; };
            resource.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> f.consumers().find(A.key())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThatThrownBy(() -> f.consumers().detach(A, 1, f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            if (lost.equals("root")) assertThatThrownBy(() -> f.chains().prepare(CHAIN, "retention", f.captureFence())).isInstanceOf(TapstateException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"srs_consumers", "srs_consumer_offsets"})
    void aLostEmptyCollectionCannotBeImplicitlyRecreatedByOrdinaryAttach(String collection) {
        withBusiness(f -> {
            if (collection.equals("srs_consumers")) SystemCollections.SRS_CONSUMERS.on(f.database()).drop();
            else SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).drop();
            Document before = root(f);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before);
            assertThat(f.database().listCollectionNames()).doesNotContain(collection);
        });
    }

    @Test
    void concurrentClientsCannotApplyDifferentSelectionsAtTheSameOriginalRevision() {
        withBusiness(f -> {
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).isPresent(); });
                var b = executor.submit(() -> { start.await(); return second.attach(A, 0, Set.of("quiet"), f.fenceA()).isPresent(); }); start.countDown();
                assertThat(List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            }
            assertThat(f.consumers().attached(CHAIN).consumers()).hasSize(1); assertThat(root(f).getLong("consumerCount")).isOne();
        });
    }

    @Test
    void aStateGenerationChangeRequiresExplicitDetachAndCannotReturnTheOldCursor() {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow(); var next = new SrsConsumerContext(A.key(), 2);
            assertThatThrownBy(() -> f.consumers().attach(next,1,Set.of("orders"),f.fenceA())).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED));
            f.consumers().detach(A,1,f.fenceA()).orElseThrow(); replacePipelineScope(f,next);
            f.consumers().attach(next, 2, Set.of("orders"), f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().cursor(A)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThatThrownBy(() -> f.consumers().detach(A,1,f.fenceA())).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED));
            assertThat(f.consumers().cursor(next)).isPresent();
        });
    }

    @Test void aStoppedSuccessorTreatsADetachedPredecessorAsAbsentButRefusesAnOrphanCursor() {
        withBusiness(f -> {
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var predecessorCursor = cursor(f, A);
            var next = new SrsConsumerContext(A.key(), A.stateGeneration() + 1);
            f.consumers().detach(A, member.revision(), f.fenceA()).orElseThrow();
            replacePipelineScope(f, next);
            assertThat(f.consumers().observationCursor(next)).isEmpty();
            var offsets = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database());
            offsets.insertOne(predecessorCursor);
            assertThatThrownBy(() -> f.consumers().observationCursor(next)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            offsets.deleteOne(id(A));
            f.consumers().attach(next, member.revision() + 1, Set.of("orders"), f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().observationCursor(A))
                    .satisfies(error -> code(error, IoError.WORKLOAD_CLAIM_FENCED));
        });
    }

    @Test
    void theOriginalPipelineClaimCannotBeReplacedByCaptureOrAnotherPipelineAuthorization() {
        withBusiness(f -> {
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.captureFence())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceB())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).release(f.pipelineA())).isTrue();
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThat(SystemCollections.SRS_CONSUMERS.on(f.database()).countDocuments()).isZero();
        });
    }

    @Test
    void anActuationClaimWithoutAnAllocatedExecutionCannotCreateMembership() {
        withBusiness(f -> {
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var unused = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, "unused"),
                    new WorkloadOwner("node-u", "boot-u"), 1, Duration.ofSeconds(30)).claim();
            assertThatThrownBy(() -> f.consumers().attach(context("unused", "source-u", 1), 0, Set.of("orders"), WorkloadClaimFence.from(unused)))
                    .satisfies(MongoSrsConsumerStoreIT::fenced);
        });
    }

    @Test
    void callbackGuardLossRollsBackBothResourcesTheChainAndTheClaimProof() {
        withBusiness(f -> {
            Document checkpoint = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first();
            Document root = root(f); Document claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())
                    .find(new Document("_id.resourceId", A.key().pipelineId())).first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_chains")) {
                var guarded = new MongoSrsConsumerStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> guarded.attach(A, 0, Set.of("orders"), f.fenceA())).satisfies(failure -> code(failure,IoError.CACHE_BUSINESS_FENCED));
                witness.requireCommand();
            }
            assertThat(root(f)).isEqualTo(root); assertThat(membership(f,A)).isNull(); assertThat(cursor(f,A)).isNull();
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first()).isEqualTo(checkpoint);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id.resourceId", A.key().pipelineId())).first()).isEqualTo(claim);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aFreshSameStateCommandStillFencesAnEarlierCapturedOppositeCommand(boolean attached) {
        withBusiness(f -> {
            if (attached) {
                f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
                long capturedDetachRevision = 1;
                f.consumers().attach(A, 1, Set.of("orders"), f.fenceA()).orElseThrow();
                assertThat(f.consumers().detach(A, capturedDetachRevision, f.fenceA())).isEmpty();
                assertThat(f.consumers().cursor(A)).isPresent();
            } else {
                f.consumers().detach(A, 0, f.fenceA()).orElseThrow();
                long capturedAttachRevision = 1;
                f.consumers().detach(A, 1, f.fenceA()).orElseThrow();
                assertThat(f.consumers().attach(A, capturedAttachRevision, Set.of("orders"), f.fenceA())).isEmpty();
                assertThat(f.consumers().cursor(A)).isEmpty();
            }
        });
    }

    @Test
    void reassertingMembershipPreservesRetainedReadAckAndSnapshotProgress() {
        withBusiness(f -> {
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var position = new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(1, 7), "saved-token");
            var progress = new io.tapstate.spi.store.ConsumerOffset(A.key().consumerId(), Map.of("orders", 7L), position,
                    List.of("orders"), "saved-seam", 1, Map.of("orders", position), io.tapstate.spi.store.ConsumerProgressKind.SRS);
            var codec = new MongoSrsConsumerRecords(SCOPE);
            var pins = publishSnapshotModels(f, Set.of("orders"));
            var pin = new SrsSnapshotPin(1, "saved-seam", pins);
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).replaceOne(id(A), codec.cursor(member, progress, 11, pin));
            var next = f.consumers().attach(A, 1, Set.of("orders", "quiet"), f.fenceA()).orElseThrow();
            var expected = new io.tapstate.spi.store.ConsumerOffset(A.key().consumerId(), Map.of("orders", 7L, "quiet", -1L), position,
                    List.of("orders"), "saved-seam", 1, Map.of("orders", position), io.tapstate.spi.store.ConsumerProgressKind.SRS);
            assertThat(f.consumers().cursor(A)).contains(expected);
            assertThat(cursor(f, A).getLong("progressRevision")).isEqualTo(11);
            assertThat(cursor(f, A).getLong("membershipRevision")).isEqualTo(next.revision());
        });
    }

    @Test
    void aCursorInsertFailureRollsBackTheRelationAndChainDependencyChange() {
        withBusiness(f -> {
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).createIndex(new Document("stateGeneration", 1),
                    new com.mongodb.client.model.IndexOptions().unique(true));
            Document before = root(f); Document retained = cursor(f, B);
            assertThatThrownBy(() -> f.consumers().attach(A, 0, Set.of("orders"), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(root(f)).isEqualTo(before); assertThat(cursor(f, B)).isEqualTo(retained);
            assertThat(membership(f, A)).isNull(); assertThat(cursor(f, A)).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"member-revision", "member-fence", "cursor-generation", "cursor-progress", "cursor-membership", "root-count", "root-revision"})
    void malformedOrInconsistentFactsAreDiagnosedWithoutRepair(String damage) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            switch (damage) {
                case "member-revision" -> SystemCollections.SRS_CONSUMERS.on(f.database()).updateOne(id(A), new Document("$set", new Document("revision", 1.5D)));
                case "member-fence" -> SystemCollections.SRS_CONSUMERS.on(f.database()).updateOne(id(A), new Document("$set", new Document("commandFence.claimGeneration", 1.5D)));
                case "cursor-generation" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$set", new Document("stateGeneration", 1.5D)));
                case "cursor-progress" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$unset", new Document("progress", "")));
                case "cursor-membership" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$set", new Document("membershipRevision", 2L)));
                case "root-count" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("consumerCount", 2L)));
                default -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("consumerRevision", 3L)));
            }
            Document before = root(f); Document member = membership(f, A); Document cursor = cursor(f, A);
            assertThatThrownBy(() -> f.consumers().find(A.key())).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.getCause()).isNull());
            assertThat(root(f)).isEqualTo(before); assertThat(membership(f, A)).isEqualTo(member); assertThat(cursor(f, A)).isEqualTo(cursor);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "ttl"})
    void ordinaryConsumerOperationsDoNotRepairOrAcceptInvalidIndexes(String damage) {
        withBusiness(f -> {
            if (damage.equals("missing")) SystemCollections.SRS_CONSUMERS.on(f.database()).dropIndex("srs_consumers_chain");
            else SystemCollections.SRS_CONSUMERS.on(f.database()).createIndex(new Document("expiresAt", 1),
                    new com.mongodb.client.model.IndexOptions().expireAfter(30L, TimeUnit.SECONDS));
            assertThatThrownBy(() -> f.consumers().find(A.key())).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(SystemCollections.SRS_CONSUMERS.on(f.database()).countDocuments()).isZero();
        });
    }

    private static SrsConsumerContext context(String pipeline, String source, long generation) { return new SrsConsumerContext(new SrsConsumerKey(CHAIN, pipeline, source), generation); }
    private static Document id(SrsConsumerContext context) { return new Document("_id", new MongoSrsConsumerRecords(SCOPE).id(context.key())); }
    private static Document membership(Fixture f, SrsConsumerContext context) { return SystemCollections.SRS_CONSUMERS.on(f.database()).find(id(context)).first(); }
    @Test
    void snapshotPinsAreIndependentAndReadableAcrossReopenWithoutAnyCaptureClaim() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var a = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var b = f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).release(f.capture())).isTrue();
            var before = root(f); var pinA = new SrsSnapshotPin(17, "opaque-seam-a", refs); var pinB = new SrsSnapshotPin(29, "opaque-seam-b", refs);
            assertThat(f.consumers().pinSnapshot(A, a.revision(), 0, pinA, f.fenceA())).isTrue();
            var savedA = cursor(f, A);
            assertThat(f.consumers().pinSnapshot(B, b.revision(), 0, pinB, f.fenceB())).isTrue();
            assertThat(cursor(f, A)).isEqualTo(savedA);
            assertThat(root(f).getLong("consumerRevision")).isEqualTo(before.getLong("consumerRevision"));
            assertThat(root(f).getLong("consumerCount")).isEqualTo(before.getLong("consumerCount"));
            assertThat(root(f).getLong("modelWriteFence")).isEqualTo(before.getLong("modelWriteFence") + 2);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var independent = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE);
                var held = independent.progress(A).orElseThrow();
                assertThat(held.snapshot()).isEqualTo(pinA); assertThat(held.revision()).isOne();
                assertThat(held.cursor().cdcStartPosition()).isEqualTo("opaque-seam-a"); assertThat(held.cursor().snapshotEpoch()).isEqualTo(17);
                assertThat(independent.pinSnapshot(A, a.revision(), 0, pinA, f.fenceA())).isTrue();
                assertThat(independent.progress(A)).contains(held);
                assertThat(independent.progress(B).orElseThrow().snapshot()).isEqualTo(pinB);
            }
        });
    }

    @Test
    void addingASelectedTablePreservesOldPinsAndOnlyExtendsTheSavedReferenceSet() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders", "quiet"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var first = new SrsSnapshotPin(91, "original-seam", Map.of("orders", refs.get("orders")));
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, first, f.fenceA())).isTrue();
            var expanded = f.consumers().attach(A, member.revision(), Set.of("orders", "quiet"), f.fenceA()).orElseThrow();
            assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(first);
            var full = new SrsSnapshotPin(91, "original-seam", refs);
            assertThat(f.consumers().pinSnapshot(A, expanded.revision(), 1, full, f.fenceA())).isTrue();
            assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(full);
            var reduced = f.consumers().attach(A, expanded.revision(), Set.of("quiet"), f.fenceA()).orElseThrow();
            var before = cursor(f, A);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, reduced.revision(), 2,
                    new SrsSnapshotPin(91, "original-seam", Map.of("quiet", refs.get("quiet"))), f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(cursor(f, A)).isEqualTo(before); assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(full);
        });
    }

    @Test
    void aSavedSnapshotNeverAdoptsANewerPublishedModelOrSnapshotOrder() {
        withBusiness(f -> {
            var tables = Set.of("orders", "quiet"); var refs = publishSnapshotModels(f, tables);
            var member = f.consumers().attach(A, 0, tables, f.fenceA()).orElseThrow();
            var first = new SrsSnapshotPin(17, "saved-seam", refs); assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, first, f.fenceA())).isTrue();
            var next = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 1, 1);
            models(f).publish(new SrsModel(next, ring("orders"), new SourceTable("orders", List.of(new SourceField("changed", "int")), List.of(), List.of()), 1), f.captureFence());
            var before = cursor(f, A);
            var changed = new java.util.HashMap<>(refs); changed.put("orders", next);
            for (var replacement : List.of(new SrsSnapshotPin(18, "saved-seam", refs), new SrsSnapshotPin(17, "new-seam", refs),
                    new SrsSnapshotPin(17, "saved-seam", changed))) {
                assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 1, replacement, f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            }
            assertThat(cursor(f, A)).isEqualTo(before);
            SystemCollections.SRS_MODELS.on(f.database()).updateOne(new Document("_id.modelVersion", 1L), new Document("$set", new Document("schema", new Document("bad", "secret-sentinel"))));
            assertThat(f.consumers().progress(A).orElseThrow().snapshot()).isEqualTo(first);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"body", "context", "bounds", "anchor", "malformed-bounds"})
    void losingARequiredSnapshotFactDoesNotUseLatestDiscoveryOrAnEmptyProgress(String missing) {
        withBusiness(f -> {
            var tables = Set.of("orders", "quiet"); var refs = publishSnapshotModels(f, tables);
            var member = f.consumers().attach(A, 0, tables, f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(17, "saved-seam", refs); assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            switch (missing) {
                case "body" -> SystemCollections.SRS_MODELS.on(f.database()).deleteOne(new Document("_id", MongoSrsModelStore.key(refs.get("orders"))));
                case "context" -> SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).deleteOne(new Document("_id", MongoSrsModelContexts.contextId(refs.get("orders"))));
                case "bounds" -> SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteOne(new Document("_id", MongoSrsLogStore.boundsId(ring("orders"))));
                case "anchor" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$pull", new Document("modelContextHeads", new Document("table", "orders"))));
                case "malformed-bounds" -> SystemCollections.SRS_LOG_BOUNDS.on(f.database()).updateOne(new Document("_id", MongoSrsLogStore.boundsId(ring("orders"))),
                        new Document("$set", new Document("trimmedThrough", 0L)));
                default -> throw new AssertionError(missing);
            }
            var before = cursor(f, A);
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f, A)).isEqualTo(before);
        });
    }

    @Test
    void snapshotCasAndTheOriginalMembershipCannotBeReplacedOnRetry() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs); var before = cursor(f, A);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 7, pin, f.fenceA())).isFalse(); assertThat(cursor(f, A)).isEqualTo(before);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            f.consumers().attach(A, member.revision(), Set.of("orders"), f.fenceA()).orElseThrow(); before = cursor(f, A);
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isFalse(); assertThat(cursor(f, A)).isEqualTo(before);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision() + 1, 1, pin, f.fenceB())).satisfies(MongoSrsConsumerStoreIT::fenced);
        });
    }

    @org.junit.jupiter.api.RepeatedTest(3)
    void pinWaitsForTheActualSameKeyRenewalBeforeOpeningItsSnapshot() {
        withBusiness(f -> {
            enablePipelineClaimAdmission(f);
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs);
            var active = new CountDownLatch(1);
            var resume = new CountDownLatch(1);
            var firstMongo = new CountDownLatch(1);
            var commands = new java.util.concurrent.atomic.AtomicInteger();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if (event.getCommand().containsKey("startTransaction")) firstMongo.countDown();
                    if ("commitTransaction".equals(event.getCommandName())) commands.incrementAndGet();
                }
            };
            var operations = new MongoClaimOperations(f.client());
            var authority = new MongoCacheWorkloadClaimStore(f.client(), f.database(), SCOPE, () -> {}, null, operations);
            try (var observed = pinClient(listener); var executor = Executors.newFixedThreadPool(2)) {
                var store = coordinatedPinStore(f, observed, operations);
                var held = executor.submit(() -> authority.withRenewalDeadline(f.pipelineA().key(),
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(5), () -> {}, () -> {
                            active.countDown();
                            awaitPinTestBarrier(resume);
                            return authority.renew(f.pipelineA(), Duration.ofSeconds(30));
                        }));
                try {
                    assertThat(active.await(5, TimeUnit.SECONDS)).isTrue();
                    var call = new CountDownLatch(1);
                    var pending = executor.submit(() -> {
                        call.countDown();
                        return store.pinSnapshot(A, member.revision(), 0, pin, f.fenceA());
                    });
                    assertThat(call.await(5, TimeUnit.SECONDS)).isTrue();
                    assertThat(firstMongo.await(250, TimeUnit.MILLISECONDS)).isFalse();
                    assertThat(renewPinTestLease(authority, f.pipelineB(), 30)).isPresent();
                    resume.countDown();
                    assertThat(held.get(5, TimeUnit.SECONDS)).isPresent();
                    assertThat(pending.get(5, TimeUnit.SECONDS)).isTrue();
                } finally {
                    resume.countDown();
                }
                assertThat(commands).hasValue(1);
                assertThat(f.consumers().progress(A)).hasValueSatisfying(value -> {
                    assertThat(value.snapshot()).isEqualTo(pin);
                    assertThat(value.revision()).isOne();
                });
            } finally {
                operations.closeIfIdle();
            }
        });
    }

    @ParameterizedTest
    @CsvSource({"true,false", "true,true", "false,false"})
    void pinProtectsOnlyTheNextAttemptAndLeavesRenewalAloneWhenLeaseIsShort(boolean enoughSlack, boolean unknownCommit) {
        withBusiness(f -> {
            enablePipelineClaimAdmission(f);
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs);
            var operations = new MongoClaimOperations(f.client());
            var authority = new MongoCacheWorkloadClaimStore(f.client(), f.database(), SCOPE, () -> {}, null, operations);
            var attempted = new java.util.concurrent.atomic.AtomicInteger();
            var commits = new java.util.concurrent.atomic.AtomicInteger();
            var conflicts = new java.util.concurrent.atomic.AtomicInteger();
            var deferred = new java.util.concurrent.atomic.AtomicInteger();
            var commitDeferred = new java.util.concurrent.atomic.AtomicInteger();
            var commitCommands = new java.util.ArrayList<org.bson.BsonDocument>();
            var otherRenewed = new java.util.concurrent.atomic.AtomicBoolean();
            var error = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var reserved = new java.util.HashSet<Integer>();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    var name = event.getCommandName();
                    if ("findAndModify".equals(name) && "srs_chains".equals(event.getCommand().getString(name).getValue())) {
                        reserved.add(event.getRequestId());
                    }
                    if ("commitTransaction".equals(name)) commitCommands.add(event.getCommand().clone());
                    if ("commitTransaction".equals(name) && enoughSlack) {
                        try {
                            assertNotStarted(() -> renewPinTestLease(authority, f.pipelineA(), 30));
                            commitDeferred.incrementAndGet();
                        } catch (AssertionError | RuntimeException unexpected) {
                            error.compareAndSet(null, unexpected);
                        }
                    }
                }

                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if ("commitTransaction".equals(event.getCommandName()) && !event.getResponse().containsKey("writeConcernError"))
                        commits.incrementAndGet();
                    if (!reserved.remove(event.getRequestId())) return;
                    try {
                        switch (attempted.incrementAndGet()) {
                            case 1 -> assertThat(renewPinTestLease(authority, f.pipelineA(), enoughSlack ? 30 : 10)).isPresent();
                            case 2 -> {
                                if (enoughSlack) {
                                    assertNotStarted(() -> renewPinTestLease(authority, f.pipelineA(), 30));
                                    deferred.incrementAndGet();
                                    assertThat(renewPinTestLease(authority, f.pipelineB(), 30)).isPresent();
                                    otherRenewed.set(true);
                                } else {
                                    assertThat(renewPinTestLease(authority, f.pipelineA(), 30)).isPresent();
                                }
                            }
                            default -> { }
                        }
                    } catch (AssertionError | RuntimeException unexpected) {
                        error.compareAndSet(null, unexpected);
                    }
                }

                @Override public void commandFailed(com.mongodb.event.CommandFailedEvent event) {
                    if (event.getThrowable() instanceof com.mongodb.MongoException failure && failure.getCode() == 112)
                        conflicts.incrementAndGet();
                }
            };
            try (var observed = pinClient(listener)) {
                var store = coordinatedPinStore(f, observed, operations);
                var admin = f.client().getDatabase("admin");
                if (unknownCommit) admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1))
                        .append("data", new Document("appName", "tapstate-pin-coordination")
                                .append("failCommands", List.of("commitTransaction"))
                                .append("writeConcernError", new Document("code", 64).append("errmsg", "test commit acknowledgement lost"))
                                .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
                try {
                    assertThat(store.pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
                } finally {
                    if (unknownCommit) admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
                assertThat(error.get()).isNull();
                assertThat(attempted).hasValue(enoughSlack ? 2 : 3);
                assertThat(conflicts).hasValue(enoughSlack ? 1 : 2);
                assertThat(commits).hasValue(1);
                assertThat(deferred).hasValue(enoughSlack ? 1 : 0);
                int commitAttempts = unknownCommit ? 2 : 1;
                assertThat(commitDeferred).hasValue(enoughSlack ? commitAttempts : 0);
                assertThat(commitCommands).hasSize(commitAttempts).allSatisfy(command -> {
                    assertThat(command.getDocument("lsid")).isEqualTo(commitCommands.getFirst().getDocument("lsid"));
                    assertThat(command.getNumber("txnNumber")).isEqualTo(commitCommands.getFirst().getNumber("txnNumber"));
                });
                assertThat(otherRenewed.get()).isEqualTo(enoughSlack);
                assertThat(renewPinTestLease(authority, f.pipelineA(), 30)).isPresent();
                assertThat(f.consumers().progress(A)).hasValueSatisfying(value -> {
                    assertThat(value.snapshot()).isEqualTo(pin);
                    assertThat(value.revision()).isOne();
                });
                System.out.printf("PIN_PRODUCTION_RETRY enough_slack=%s attempts=%d conflicts=%d pin_commits=%d deferred=%d%n",
                        enoughSlack, attempted.get(), conflicts.get(), commits.get(), deferred.get());
            } finally {
                operations.closeIfIdle();
            }
        });
    }

    @Test void pinWaitsForTheSameKeyMongoTransactionAfterTheRenewalWorkerExits() {
        withBusiness(f -> {
            enablePipelineClaimAdmission(f);
            var pin = new SrsSnapshotPin(1, "seam", publishSnapshotModels(f, Set.of("orders")));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var operations = new MongoClaimOperations(f.client());
            var authority = new MongoCacheWorkloadClaimStore(f.client(), f.database(), SCOPE, () -> {}, null, operations);
            var opened = new CountDownLatch(1);
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if (event.getCommand().containsKey("startTransaction")) opened.countDown();
                }
            };
            try (var observed = pinClient(listener); var executor = Executors.newSingleThreadExecutor()) {
                authority.withRenewalDeadline(f.pipelineA().key(), System.nanoTime() + TimeUnit.SECONDS.toNanos(5), () -> {},
                        () -> operations.pipelineRenewal(f.pipelineA().key(), () -> {
                            var session = f.client().startSession();
                            session.startTransaction();
                            operations.own(session);
                            // The application call exits while exact Mongo transaction ownership survives.
                            return new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()))
                                    .inStrictSession(session).renewBeforeReads(f.pipelineA(), Duration.ofSeconds(30)).orElseThrow();
                        }));
                var started = new CountDownLatch(1);
                var task = executor.submit(() -> {
                    started.countDown();
                    return coordinatedPinStore(f, observed, operations).pinSnapshot(A, member.revision(), 0, pin, f.fenceA());
                });
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                boolean openedBeforeRecovery = opened.await(250, TimeUnit.MILLISECONDS);
                assertThat(renewPinTestLease(authority, f.pipelineB(), 30)).isPresent();
                // The next same-key renewal uses the existing exact-session recovery.
                assertThat(renewPinTestLease(authority, f.pipelineA(), 30)).isPresent();
                assertThat(task.get(5, TimeUnit.SECONDS)).isTrue();
                assertThat(openedBeforeRecovery).as("pin opens only after Mongo renewal ownership is resolved").isFalse();
                assertThat(f.consumers().progress(A)).hasValueSatisfying(value -> assertThat(value.revision()).isOne());
            } finally {
                operations.closeIfIdle();
            }
        });
    }

    private static void enablePipelineClaimAdmission(Fixture f) {
        SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                new Document("$set", new Document("claimAdmissionFence", 0L)));
    }

    private static MongoClient pinClient(com.mongodb.event.CommandListener listener) {
        return MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                .applicationName("tapstate-pin-coordination").addCommandListener(listener).build());
    }

    private static MongoSrsConsumerStore coordinatedPinStore(Fixture f, MongoClient client, MongoClaimOperations operations) {
        return new MongoSrsConsumerStore(client, client.getDatabase(f.database().getName()), SCOPE,
                () -> {}, () -> {}, MongoSrsLease.perTransaction(), true, operations);
    }

    private static Optional<io.tapstate.spi.store.WorkloadClaim> renewPinTestLease(MongoCacheWorkloadClaimStore claims,
            io.tapstate.spi.store.WorkloadClaim original, int seconds) {
        return claims.withRenewalDeadline(original.key(), System.nanoTime() + TimeUnit.SECONDS.toNanos(5), () -> {},
                () -> claims.renew(original, Duration.ofSeconds(seconds)));
    }

    private static void assertNotStarted(Runnable renewal) {
        assertThatThrownBy(renewal::run).isInstanceOfSatisfying(io.tapstate.spi.store.WorkloadClaimRenewalException.class,
                failure -> assertThat(failure.reason()).isEqualTo(io.tapstate.spi.store.WorkloadClaimRenewalException.Reason.NOT_STARTED));
    }

    private static void awaitPinTestBarrier(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    @Test void cacheRetirementCannotCommitBetweenPinAuthorityProofAndPinCommit() {
        withBusiness(f -> {
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("claimAdmissionFence", 0L)));
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs);
            var attempted = new java.util.concurrent.atomic.AtomicBoolean();
            var retirementCode = new java.util.concurrent.atomic.AtomicInteger(-1);
            var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if (!"commitTransaction".equals(event.getCommandName()) || !attempted.compareAndSet(false, true)) return;
                    try (var session = f.client().startSession()) {
                        session.startTransaction();
                        new MongoCacheBusinessIdentity(f.database(), SCOPE).retire(session, "retire-before-pin-commit");
                        session.commitTransaction();
                        retirementCode.set(0);
                    } catch (com.mongodb.MongoException expected) {
                        retirementCode.set(expected.getCode());
                    } catch (Throwable unexpected) {
                        failure.set(unexpected);
                    }
                }
            };
            try (var observed = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build())) {
                assertThat(runtimeConsumer(observed, observed.getDatabase(f.database().getName()))
                        .pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
                assertThat(failure.get()).isNull();
                assertThat(attempted).isTrue();
                assertThat(retirementCode).hasValue(112);
            }
        });
    }

    @Test void renewalCanCommitDuringSnapshotPreparationAndThePinLandsOnce() {
        withBusiness(f -> {
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("claimAdmissionFence", 0L)));
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs);
            var before = f.consumers().progress(A).orElseThrow();
            long initialChainFence = root(f).getLong("modelWriteFence");
            var reserved = new CountDownLatch(1);
            var resume = new CountDownLatch(1);
            var claims = new MongoCacheWorkloadClaimStore(f.client(), f.database(), SCOPE, () -> {});
            var pauseOnce = new java.util.concurrent.atomic.AtomicBoolean(true);
            var commands = new java.util.concurrent.ConcurrentHashMap<Integer, String>();
            var commits = new java.util.concurrent.atomic.AtomicInteger();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    var command = event.getCommand().get(event.getCommandName());
                    if (command != null && command.isString())
                        commands.put(event.getRequestId(), command.asString().getValue());
                }

                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if ("commitTransaction".equals(event.getCommandName())) commits.incrementAndGet();
                    var collection = commands.remove(event.getRequestId());
                    if ("findAndModify".equals(event.getCommandName()) && "srs_chains".equals(collection)
                            && pauseOnce.compareAndSet(true, false)) {
                        reserved.countDown();
                        try { assertThat(resume.await(10, TimeUnit.SECONDS)).isTrue(); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
                    }
                }

                @Override public void commandFailed(com.mongodb.event.CommandFailedEvent event) {
                    commands.remove(event.getRequestId());
                }
            };
            try (var client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build());
                 var executor = Executors.newSingleThreadExecutor()) {
                var consumers = new MongoSrsConsumerStore(client, client.getDatabase(f.database().getName()), SCOPE,
                        () -> {}, () -> {}, MongoSrsLease.perTransaction(), true);
                var pending = executor.submit(() -> consumers.pinSnapshot(A, member.revision(), before.revision(), pin, f.fenceA()));
                try {
                    assertThat(reserved.await(5, TimeUnit.SECONDS)).isTrue();
                    var renewed = claims.withRenewalDeadline(System.nanoTime() + TimeUnit.SECONDS.toNanos(5),
                            () -> {}, () -> claims.renew(f.pipelineA(), Duration.ofSeconds(30))).orElseThrow();
                    assertThat(WorkloadClaimFence.from(renewed)).isEqualTo(f.fenceA());
                    assertThat(renewed.leaseUntil()).isAfter(f.pipelineA().leaseUntil());
                    assertThat(pending.isDone()).isFalse();
                    assertThat(f.consumers().progress(A)).contains(before);
                } finally {
                    resume.countDown();
                }
                assertThat(pending.get(10, TimeUnit.SECONDS)).isTrue();
                assertThat(commits.get()).isEqualTo(1);
                var after = f.consumers().progress(A).orElseThrow();
                assertThat(after.snapshot()).isEqualTo(pin);
                assertThat(after.revision()).isEqualTo(before.revision() + 1);
                assertThat(root(f).getLong("modelWriteFence")).isEqualTo(initialChainFence + 1);
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"expired-before", "expires-pending", "owner-pending", "retired-pending"})
    void latePinClaimProofStillRejectsLostAuthority(String scenario) {
        withBusiness(f -> {
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("claimAdmissionFence", 0L)));
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var snapshot = new SrsSnapshotPin(1, "seam", refs);
            var originalRoot = root(f);
            var originalCursor = cursor(f, A);
            var stateCollection = SystemCollections.CACHE_PIPELINE_STATE.on(f.database());
            var originalState = stateCollection.find(new Document("_id", A.key().pipelineId())).first();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var paused = new CountDownLatch(1);
            var resume = new CountDownLatch(1);
            var firstOffsetWrite = new java.util.concurrent.atomic.AtomicInteger(-1);
            var blockOnce = new java.util.concurrent.atomic.AtomicBoolean();
            var commits = new java.util.concurrent.atomic.AtomicInteger();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    var cmd = event.getCommand();
                    if ("update".equals(event.getCommandName())
                            && "srs_consumer_offsets".equals(cmd.getString("update").getValue())
                            && blockOnce.compareAndSet(false, true)) firstOffsetWrite.set(event.getRequestId());
                }
                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if ("commitTransaction".equals(event.getCommandName())) commits.incrementAndGet();
                    if (event.getRequestId() != firstOffsetWrite.get()) return;
                    paused.countDown();
                    try {
                        if (!resume.await(8, TimeUnit.SECONDS)) throw new AssertionError("pin barrier not released");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }
            };
            try (var observed = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build());
                 var executor = Executors.newSingleThreadExecutor()) {
                var pin = new MongoSrsConsumerStore(observed, observed.getDatabase(f.database().getName()), SCOPE,
                        () -> {}, () -> {}, MongoSrsLease.perTransaction(), true);
                if (scenario.equals("expired-before")) {
                    assertThat(claims.release(f.pipelineA())).isTrue();
                    assertThatThrownBy(() -> pin.pinSnapshot(A, member.revision(), 0, snapshot, f.fenceA()))
                            .isInstanceOfSatisfying(TapstateException.class,
                                    error -> assertThat(error.code()).isIn(IoError.WORKLOAD_CLAIM_FENCED, IoError.CACHE_BUSINESS_FENCED));
                } else {
                    var task = executor.submit(() -> pin.pinSnapshot(A, member.revision(), 0, snapshot, f.fenceA()));
                    try {
                        assertThat(paused.await(5, TimeUnit.SECONDS)).isTrue();
                        if (scenario.equals("retired-pending")) {
                            try (var session = f.client().startSession()) {
                                session.withTransaction(() -> {
                                    new MongoCacheBusinessIdentity(f.database(), SCOPE).retire(session, "pin-safety-retirement");
                                    return null;
                                });
                            }
                        } else {
                            assertThat(claims.release(f.pipelineA())).isTrue();
                            if (scenario.equals("owner-pending")) {
                                var replacement = claims.acquire(f.pipelineA().key(),
                                        new WorkloadOwner("replacement-node", "replacement-boot"), 1, Duration.ofSeconds(30));
                                assertThat(replacement.acquired()).isTrue();
                                assertThat(replacement.claim().owner()).isNotEqualTo(f.pipelineA().owner());
                            }
                        }
                    } finally {
                        resume.countDown();
                    }
                    assertThatThrownBy(() -> task.get(5, TimeUnit.SECONDS))
                            .isInstanceOfSatisfying(java.util.concurrent.ExecutionException.class,
                                    error -> code(error.getCause(), scenario.equals("retired-pending")
                                            ? IoError.CACHE_BUSINESS_FENCED : IoError.WORKLOAD_CLAIM_FENCED));
                }
                assertThat(commits).hasValue(0);
                assertThat(root(f)).isEqualTo(originalRoot);
                assertThat(cursor(f, A)).isEqualTo(originalCursor);
                assertThat(stateCollection.find(new Document("_id", A.key().pipelineId())).first()).isEqualTo(originalState);
            }
        });
    }

    @Test void pinReservesTheChainBeforeReadingProgressAndReplaysDoNotAdvanceFences() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs);
            var commands = new java.util.ArrayList<String>();
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    var body = event.getCommand();
                    if (!event.getDatabaseName().equals(f.database().getName()) || !body.containsKey("autocommit")) return;
                    String command = event.getCommandName();
                    var collection = body.get(command);
                    commands.add(command + "/" + (collection != null && collection.isString() ? collection.asString().getValue() : "-"));
                }
            };
            try (var observed = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build())) {
                var original = new MongoSrsConsumerStore(observed, observed.getDatabase(f.database().getName()), SCOPE);
                var before = root(f).getLong("modelWriteFence");
                assertThat(original.pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
                assertThat(commands).startsWith("findAndModify/srs_chains").endsWith("update/workload_claims")
                        .contains("update/srs_consumer_offsets").doesNotContain("update/srs_chains");
                assertThat(java.util.Collections.frequency(commands, "update/workload_claims")).isEqualTo(1);
                var root = root(f);
                var offset = cursor(f, A);
                var pipeline = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id", A.key().pipelineId())).first();
                var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(
                        new Document("_id", MongoWorkloadClaimStore.id(f.fenceA().key()))).first();
                assertThat(root.getLong("modelWriteFence")).isEqualTo(before + 1);
                assertThat(original.pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
                assertThat(original.pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "different", refs), f.fenceA())).isFalse();
                assertThat(root(f)).isEqualTo(root);
                assertThat(cursor(f, A)).isEqualTo(offset);
                assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id", A.key().pipelineId())).first())
                        .isEqualTo(pipeline);
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(
                        new Document("_id", MongoWorkloadClaimStore.id(f.fenceA().key()))).first()).isEqualTo(claim);
            }
        });
    }

    @Test
    void independentClientsCannotPinDifferentSnapshotSeamsAtOneOriginalRevision() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam-a", refs), f.fenceA()); });
                var b = executor.submit(() -> { start.await(); return second.pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam-b", refs), f.fenceA()); });
                start.countDown(); assertThat(List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
                assertThat(f.consumers().progress(A).orElseThrow().revision()).isOne();
            }
        });
    }

    @Test
    void guardLossRollsBackThePinOriginalClaimProofAndChainWriteFence() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var root = root(f); var cursor = cursor(f,A);
            var checkpoint = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first();
            var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id", WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_consumer_offsets")) {
                var guarded = new MongoSrsConsumerStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> guarded.pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam", refs), f.fenceA()))
                        .satisfies(failure -> code(failure,IoError.CACHE_BUSINESS_FENCED));
                witness.requireCommand();
            }
            assertThat(root(f)).isEqualTo(root); assertThat(cursor(f,A)).isEqualTo(cursor);
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first()).isEqualTo(checkpoint);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first()).isEqualTo(claim);
        });
    }

    @Test
    void aLatePinCannotReviveADetachedConsumerOrBorrowAnotherStateGeneration() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs); var detached = f.consumers().detach(A, member.revision(), f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            var next = new SrsConsumerContext(A.key(),2); replacePipelineScope(f,next);
            f.consumers().attach(next,detached.revision(),Set.of("orders"),f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A,detached.revision()+1,0,pin,f.fenceA())).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED));
            assertThat(f.consumers().progress(next).orElseThrow().snapshot()).isNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"snapshot", "snapshotEpoch", "modelEpoch", "seam"})
    void corruptSnapshotPayloadCannotLoseItsNecessaryReferences(String changed) {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var pin = new SrsSnapshotPin(1, "seam", refs); assertThat(f.consumers().pinSnapshot(A, member.revision(), 0, pin, f.fenceA())).isTrue();
            String path = switch (changed) {
                case "snapshot" -> "snapshot"; case "snapshotEpoch" -> "snapshot.snapshotEpoch";
                case "modelEpoch" -> "snapshot.models.0.model.captureContext.epoch"; case "seam" -> "progress.cdcStartPosition";
                default -> throw new AssertionError(changed);
            };
            Object value = changed.equals("snapshot") ? null : changed.equals("seam") ? "changed" : 1.0;
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A), new Document("$set", new Document(path, value)));
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
        });
    }

    @Test
    void counterExhaustionCannotLeaveAPinWithoutItsOriginalChainCompetition() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders")); var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("modelWriteFence", Long.MAX_VALUE - 1)));
            var before = cursor(f, A); var root = root(f);
            assertThatThrownBy(() -> f.consumers().pinSnapshot(A, member.revision(), 0, new SrsSnapshotPin(1, "seam", refs), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f, A)).isEqualTo(before); assertThat(root(f)).isEqualTo(root);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"SRS_MODELS", "SRS_MODEL_CONTEXTS"})
    void aRequiredModelUniqueIndexCannotBeDowngradedToANonUniqueOne(String resource) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var table = SystemCollections.valueOf(resource); var declared = table.indexes().getFirst();
            assertThat(declared.unique()).isTrue(); table.on(f.database()).dropIndex(declared.indexName());
            var keys = new Document(); declared.keys().forEach(key -> keys.put(key, 1));
            table.on(f.database()).createIndex(keys, new com.mongodb.client.model.IndexOptions().name(declared.indexName()).unique(false));
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
        });
    }

    @Test
    void writerAccountingSurvivesReopenAndOnlyTheSlowestWriterAcknowledgesItsOwnConsumer() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var roster = Map.of("orders",List.of("fast","slow"));
            assertThat(f.consumers().beginWriterRun(A,1,"run-a",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().advanceWriter(A,1,"run-a","orders","fast",point(1,10,"t10"),f.fenceA())).isTrue();
            assertThat(f.consumers().cursor(A).orElseThrow().sinkAcked()).isNull();
            var root = root(f);
            assertThat(f.consumers().advanceWriter(A,1,"run-a","orders","slow",point(1,6,"t6"),f.fenceA())).isTrue();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",20,f.fenceA())).isTrue();
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.cursor().sinkAcked()).isEqualTo(new ChainPosition(new SourceOrder(1,6),"t6"));
            assertThat(held.cursor().perTableSeq()).containsEntry("orders",20L);
            assertThat(held.writerRun().commandFence()).isEqualTo(f.fenceA());
            assertThat(held.writerRun().membershipRevision()).isOne();
            assertThat(f.consumers().beginWriterRun(A,1,"run-a",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().advanceWriter(A,1,"run-a","orders","fast",point(1,3,"late"),f.fenceA())).isTrue();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",2,f.fenceA())).isTrue();
            assertThat(f.consumers().progress(A)).contains(held); assertThat(root(f)).isEqualTo(root);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE);
                assertThat(reopened.progress(A)).contains(held);
                assertThat(reopened.advanceWriter(A,1,"run-a","orders","slow",point(1,7,"t7"),f.fenceA())).isTrue();
                assertThat(reopened.cursor(A).orElseThrow().sinkAcked().token()).isEqualTo("t7");
            }
        });
    }

    @Test
    void theOwningSnapshotSeamAndTableLocalCdcAcknowledgementsCommitTogether() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f,Set.of("orders","quiet"));
            f.consumers().attach(A,0,Set.of("orders","quiet"),f.fenceA()).orElseThrow();
            var roster = Map.of("orders",List.of("a"),"quiet",List.of("b"));
            assertThat(f.consumers().beginWriterRun(A,1,"run",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().pinSnapshot(A,1,1,new SrsSnapshotPin(7,"owner-seam",refs),f.fenceA())).isTrue();
            assertThat(f.consumers().progress(A).orElseThrow().writerRun().runId()).isEqualTo("run");
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","a",point(7,SourceOrder.SNAPSHOT_SEQ,"other-consumer"),f.fenceA())).isTrue();
            var partial = f.consumers().cursor(A).orElseThrow();
            assertThat(partial.snapshotCompletedTables()).containsExactly("orders"); assertThat(partial.sinkAcked()).isNull();
            assertThat(f.consumers().advanceWriter(A,1,"run","quiet","b",point(7,4,"quiet-cdc"),f.fenceA())).isTrue();
            var held = f.consumers().cursor(A).orElseThrow();
            assertThat(held.sinkAcked()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(7),"owner-seam"));
            assertThat(held.snapshotCompletedTables()).containsExactly("orders","quiet");
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","a",point(7,500,"orders-cdc"),f.fenceA())).isTrue();
            var advanced = f.consumers().cursor(A).orElseThrow();
            assertThat(advanced.sinkAcked()).isEqualTo(held.sinkAcked());
            assertThat(advanced.sinkAckedByTable().get("orders").token()).isEqualTo("orders-cdc");
            assertThat(advanced.sinkAckedByTable().get("quiet").token()).isEqualTo("quiet-cdc");
        });
    }

    @Test
    void aDurableEpochBoundIsNotDecodedAsAPhysicalResumePosition() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA())).isTrue();
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","writer",new WriterProgress(new SourceOrder(0,Long.MAX_VALUE),null),f.fenceA())).isTrue();
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.cursor().sinkAcked()).isNull();
            assertThat(held.cursor().sinkAckedByTable()).containsEntry("orders",new ChainPosition(new SourceOrder(0,Long.MAX_VALUE),null));
            assertThat(held.writerRun().progress().get("orders").get("writer").durableThrough()).isEqualTo(new SourceOrder(0,Long.MAX_VALUE));
        });
    }

    @Test
    void theSameExecutionCannotReplaceItsOriginalRunRosterOrBorrowANewerMembership() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            var roster = Map.of("orders",List.of("writer"));
            assertThat(f.consumers().beginWriterRun(A,1,"run",roster,f.fenceA())).isTrue();
            assertThat(f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("replacement")),f.fenceA())).isFalse();
            assertThat(f.consumers().beginWriterRun(A,1,"replacement",roster,f.fenceA())).isFalse();
            assertThat(f.consumers().advanceWriter(A,1,"replacement","orders","writer",point(1,1,"t1"),f.fenceA())).isFalse();
            assertThatThrownBy(() -> f.consumers().advanceWriter(A,1,"run","orders","unknown",point(1,1,"t1"),f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            f.consumers().attach(A,1,Set.of("orders"),f.fenceA()).orElseThrow(); var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.writerRun().membershipRevision()).isOne();
            assertThat(f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,1,"t1"),f.fenceA())).isFalse();
            assertThat(f.consumers().advanceWriter(A,2,"run","orders","writer",point(1,1,"t1"),f.fenceA())).isFalse();
            assertThat(f.consumers().beginWriterRun(A,2,"replacement",roster,f.fenceA())).isFalse();
            assertThat(f.consumers().progress(A)).contains(held);
        });
    }

    @Test
    void writerRegistrationCanRemainInFlightWhileTheOriginalPipelineRenews() {
        withBusiness(f -> {
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("claimAdmissionFence", 0L)));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var beforeWriterWrite = new CountDownLatch(1);
            var releaseWriter = new CountDownLatch(1);
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if (!"update".equals(event.getCommandName()) || beforeWriterWrite.getCount() == 0
                            || !"srs_consumer_offsets".equals(event.getCommand().getString("update").getValue())) return;
                    beforeWriterWrite.countDown();
                    try {
                        if (!releaseWriter.await(10, TimeUnit.SECONDS)) throw new AssertionError("writer was not released");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }
            };
            var settings = com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build();
            try (var other = MongoClients.create(settings); var workers = Executors.newSingleThreadExecutor()) {
                var store = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE);
                var registration = workers.submit(() -> store.beginWriterRun(A, member.revision(), "run",
                        Map.of("orders", List.of("writer")), f.fenceA()));
                try {
                    assertThat(beforeWriterWrite.await(5, TimeUnit.SECONDS)).isTrue();
                    var authority = new MongoCacheWorkloadClaimStore(f.client(), f.database(), SCOPE, () -> {});
                    var renewed = authority.withRenewalDeadline(System.nanoTime() + Duration.ofSeconds(2).toNanos(),
                            () -> {}, () -> authority.renew(f.pipelineA(), Duration.ofSeconds(60))).orElseThrow();
                    assertThat(WorkloadClaimFence.from(renewed)).isEqualTo(f.fenceA());
                } finally {
                    releaseWriter.countDown();
                }
                assertThat(registration.get(5, TimeUnit.SECONDS)).isTrue();
            }
        });
    }

    @Test
    void nodeBackedWriterUsesTheNodeLeaseAndRejectsBothOldExecutionsAndOldBoots() {
        withBusiness(f -> {
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("claimAdmissionFence", 0L)));
            var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var authority = new MongoCacheWorkloadClaimStore(f.client(), f.database(), SCOPE, () -> {});
            var nodeKey = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, SCOPE.clusterId());
            var ttl = Duration.ofSeconds(60);
            var node = raw.acquire(nodeKey, new WorkloadOwner(SCOPE.clusterId(), "first-boot"), 1, ttl).claim();
            assertThat(raw.release(f.pipelineA())).isTrue();
            var claim = authority.acquireNodeBackedPipeline(positionScope(A), WorkloadClaimFence.from(node), 1).claim();
            var running = authority.advanceExecution(claim, 1, Set.of(SCOPE.clusterId())).orElseThrow();
            var original = WorkloadClaimFence.from(running);
            var member = f.consumers().attach(A, 0, Set.of("orders"), original).orElseThrow();
            var roster = Map.of("orders", List.of("writer"));
            assertThat(f.consumers().beginWriterRun(A, member.revision(), "first", roster, original)).isTrue();
            assertThat(f.consumers().advanceWriter(A, member.revision(), "first", "orders", "writer",
                    point(1, 1, "t1"), original)).isTrue();

            var next = authority.advanceExecution(running, 1, Set.of(SCOPE.clusterId())).orElseThrow();
            assertThatThrownBy(() -> f.consumers().advanceWriter(A, member.revision(), "first", "orders", "writer",
                    point(1, 2, "late"), original)).satisfies(MongoSrsConsumerStoreIT::fenced);
            var renewedNode = raw.renew(node, Duration.ofSeconds(90)).orElseThrow();
            assertThat(authority.readPipeline(positionScope(A)).orElseThrow().claim().leaseUntil())
                    .isEqualTo(renewedNode.leaseUntil());
            assertThat(raw.acquire(nodeKey, new WorkloadOwner(SCOPE.clusterId(), "second-boot"), 1, ttl).acquired()).isFalse();

            assertThat(raw.release(renewedNode)).isTrue();
            assertThat(authority.readPipeline(positionScope(A)).orElseThrow().leased()).isFalse();
            assertThat(authority.advanceExecution(next, 1, Set.of(SCOPE.clusterId()))).isEmpty();
            assertThatThrownBy(() -> f.consumers().beginWriterRun(A, member.revision(), "second", roster,
                    WorkloadClaimFence.from(next))).satisfies(MongoSrsConsumerStoreIT::fenced);
            var replacement = raw.acquire(nodeKey, new WorkloadOwner(SCOPE.clusterId(), "second-boot"), 1, ttl).claim();
            var resumed = authority.acquireNodeBackedPipeline(positionScope(A), WorkloadClaimFence.from(replacement), 1).claim();
            assertThat(resumed.owner()).isEqualTo(replacement.owner());
            assertThat(resumed.claimGeneration()).isGreaterThan(running.claimGeneration());
            assertThatThrownBy(() -> f.consumers().advanceWriter(A, member.revision(), "first", "orders", "writer",
                    point(1, 3, "old-boot"), original)).satisfies(MongoSrsConsumerStoreIT::fenced);
        });
    }

    @Test
    void aSecondaryFenceDoesNotMaskTheFirstProgressFailure() {
        withBusiness(f -> {
            var first = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "controlled"), null);
            var secondary = new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            var checks = new java.util.concurrent.atomic.AtomicInteger();
            var guarded = f.consumers().guardedBy(() -> {
                if (checks.getAndIncrement() == 0) throw first;
                throw secondary;
            });
            assertThatThrownBy(() -> guarded.advanceWriter(A, 1, "run", "orders", "writer",
                    point(1, 9, "late"), f.fenceA())).isSameAs(first);
            assertThat(first.getSuppressed()).containsExactly(secondary);
        });
    }

    @Test
    void generationAdvanceFencesAProgressTransactionThatAlreadyReadTheOriginalClaim() {
        withBusiness(f -> {
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("claimAdmissionFence", 0L)));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            assertThat(f.consumers().beginWriterRun(A, member.revision(), "run",
                    Map.of("orders", List.of("writer")), f.fenceA())).isTrue();
            var before = cursor(f, A);
            var beforeFence = new CountDownLatch(1);
            var afterAdvance = new CountDownLatch(1);
            var listener = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if (!"update".equals(event.getCommandName()) || beforeFence.getCount() == 0
                            || !"pipeline_state".equals(event.getCommand().getString("update").getValue())) return;
                    beforeFence.countDown();
                    try {
                        if (!afterAdvance.await(5, TimeUnit.SECONDS)) throw new AssertionError("execution did not advance");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(interrupted);
                    }
                }
            };
            var settings = com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build();
            try (var other = MongoClients.create(settings); var workers = Executors.newSingleThreadExecutor()) {
                var store = new MongoSrsConsumerStore(other, other.getDatabase(f.database().getName()), SCOPE);
                var progress = workers.submit(() -> assertThatThrownBy(() -> store.advanceWriter(A, member.revision(),
                        "run", "orders", "writer", point(1, 9, "late"), f.fenceA())).satisfies(MongoSrsConsumerStoreIT::fenced));
                try {
                    assertThat(beforeFence.await(5, TimeUnit.SECONDS)).isTrue();
                    var authority = new MongoCacheWorkloadClaimStore(f.client(), f.database(), SCOPE, () -> {});
                    var advanced = authority.advanceExecution(f.pipelineA(), 1, Set.of(f.pipelineA().owner().nodeId()));
                    assertThat(advanced.orElseThrow().executionGeneration()).isEqualTo(f.fenceA().executionGeneration() + 1);
                } finally {
                    afterAdvance.countDown();
                }
                progress.get(5, TimeUnit.SECONDS);
                assertThat(cursor(f, A)).isEqualTo(before);
            }
        });
    }

    @Test
    void aNewExecutionReplacesOnlyWriterAccountingAndCannotAcceptLateOriginalReports() {
        withBusiness(f -> {
            var roster = Map.of("orders",List.of("writer"));
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().beginWriterRun(A,1,"old",roster,f.fenceA());
            f.consumers().advanceWriter(A,1,"old","orders","writer",point(1,7,"saved"),f.fenceA());
            f.consumers().advanceReadSequence(A,1,"orders",8,f.fenceA()); var before = f.consumers().progress(A).orElseThrow();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var next = WorkloadClaimFence.from(claims.advanceExecution(f.pipelineA(),1,Set.of(f.pipelineA().owner().nodeId())).orElseThrow());
            var current = f.consumers().attach(A,1,Set.of("orders"),next).orElseThrow();
            assertThat(f.consumers().beginWriterRun(A,current.revision(),"old",roster,next)).isFalse();
            assertThat(f.consumers().beginWriterRun(A,current.revision(),"new",roster,next)).isTrue();
            var after = f.consumers().progress(A).orElseThrow();
            assertThat(after.cursor()).isEqualTo(before.cursor()); assertThat(after.writerRun().progress()).isEmpty();
            assertThat(after.writerRun().commandFence()).isEqualTo(next);
            assertThatThrownBy(() -> f.consumers().advanceWriter(A,1,"old","orders","writer",point(1,99,"late"),f.fenceA())).satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThat(f.consumers().advanceWriter(A,current.revision(),"old","orders","writer",point(1,99,"late"),next)).isFalse();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",99,next)).isFalse();
            assertThat(f.consumers().progress(A)).contains(after);
        });
    }

    @Test
    void concurrentOriginalWritersFoldWithoutLosingEitherReportOrInventingAFasterMinimum() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("a","b")),f.fenceA());
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().advanceWriter(A,1,"run","orders","a",point(1,10,"t10"),f.fenceA()); });
                var b = executor.submit(() -> { start.await(); return second.advanceWriter(A,1,"run","orders","b",point(1,6,"t6"),f.fenceA()); });
                start.countDown(); assertThat(List.of(a.get(15,TimeUnit.SECONDS),b.get(15,TimeUnit.SECONDS))).containsOnly(true);
            }
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.writerRun().progress().get("orders")).containsKeys("a","b");
            assertThat(held.cursor().sinkAcked().token()).isEqualTo("t6"); assertThat(held.revision()).isEqualTo(3);
        });
    }

    @Test
    void anUnaffectedConsumerCanKeepReadingAndAcknowledgingWhenAnotherPrivateCursorIsLost() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            f.consumers().beginWriterRun(B,1,"b",Map.of("orders",List.of("writer")),f.fenceB());
            f.consumers().startReadingAfter(B,1,1,"orders",-1,f.fenceB());
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(A));
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().attached(CHAIN)).isInstanceOf(TapstateException.class);
            assertThat(f.consumers().advanceReadSequence(B,1,"orders",5,f.fenceB())).isTrue();
            assertThat(f.consumers().advanceWriter(B,1,"b","orders","writer",point(1,5,"b5"),f.fenceB())).isTrue();
            var held = f.consumers().progress(B).orElseThrow();
            assertThat(held.cursor().perTableSeq()).containsEntry("orders",5L); assertThat(held.cursor().sinkAcked().token()).isEqualTo("b5");
        });
    }

    @ParameterizedTest @ValueSource(strings={"missing","null-progress","wrong-revision","bad-fence","bad-bound"})
    void malformedWriterFactsCannotBeTreatedAsANewEmptyRun(String damage) {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,3,"t3"),f.fenceA());
            switch (damage) {
                case "missing" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$unset",new Document("writerRun","")));
                case "null-progress" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.progress",null)));
                case "wrong-revision" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.membershipRevision",2L)));
                case "bad-fence" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.commandFence.executionGeneration",1.5D)));
                default -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("writerRun.progress.0.writers.0.durableThrough.epoch",1.5D)));
            }
            var before = cursor(f,A);
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,4,"t4"),f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    @ParameterizedTest @ValueSource(strings={"begin","ack","read","arrival"})
    void losingTheOriginalGuardAfterARealProgressWriteRollsBackCursorClaimAndCheckpoint(String operation) {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            if (!operation.equals("begin")) f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
            if (operation.equals("read")) f.consumers().startReadingAfter(A,1,1,"orders",-1,f.fenceA());
            var before = cursor(f,A); var root = root(f);
            var checkpoint = SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first();
            var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_consumer_offsets")) {
                var store = operation.equals("ack") || operation.equals("read")
                        ? runtimeConsumer(witness.client, witness.database)
                        : new MongoSrsConsumerStore(witness.client, witness.database, SCOPE);
                var guarded = store.guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> {
                    switch (operation) {
                        case "begin" -> guarded.beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
                        case "ack" -> guarded.advanceWriter(A,1,"run","orders","writer",point(1,7,"t7"),f.fenceA());
                        case "arrival" -> guarded.startReadingAfter(A,1,1,"orders",5,f.fenceA());
                        default -> guarded.advanceReadSequence(A,1,"orders",7,f.fenceA());
                    }
                }).satisfies(error -> code(error,IoError.CACHE_BUSINESS_FENCED)); witness.requireCommand();
            }
            assertThat(cursor(f,A)).isEqualTo(before); assertThat(root(f)).isEqualTo(root);
            assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find(new Document("_id",A.key().pipelineId())).first()).isEqualTo(checkpoint);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",WorkloadClaimDocuments.live(f.fenceA()).get("_id"))).first()).isEqualTo(claim);
        });
    }

    @ParameterizedTest @ValueSource(strings={"begin","read"})
    void hotProgressCannotPassAMissingRequiredSnapshotModel(String operation) {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f,Set.of("orders"));
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().pinSnapshot(A,1,0,new SrsSnapshotPin(1,"seam",refs),f.fenceA());
            SystemCollections.SRS_MODELS.on(f.database()).deleteMany(new Document());
            var before = cursor(f,A);
            assertThatThrownBy(() -> {
                if (operation.equals("begin")) f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
                else f.consumers().advanceReadSequence(A,1,"orders",5,f.fenceA());
            }).isInstanceOf(TapstateException.class);
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    @Test void aFreshChainReadBatchesItsOwnRetainedCheckpointDependencies() {
        withBusiness(f -> {
            preparedTrimLog(f);
            f.chains().requestTables(CHAIN, Set.of("orders"), f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 1, Set.of("orders"), f.captureFence())).isTrue();
            var key = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 1, 0);
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1, 4), "t4"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(key, 4)));
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            var commands = new ArrayList<String>();
            io.tapstate.spi.store.SrsChainState loaded;
            MongoCommandObservation.COLLECTIONS.set((command, collection) -> commands.add(command + ":" + collection));
            try { loaded = f.chains().load(CHAIN).orElseThrow(); }
            finally { MongoCommandObservation.COLLECTIONS.remove(); }
            assertThat(loaded.checkpoint()).isEqualTo(checkpoint);
            System.out.println("FRESH_CHAIN_COMMANDS count=" + commands.size() + " commands=" + commands);
            assertThat(commands).hasSizeLessThanOrEqualTo(9);
        });
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({"false,0", "false,100", "true,0", "true,100"})
    void oneTableArrivalCountsRemainBoundedWithOtherConsumers(boolean checkpointed, int siblings) {
        withBusiness(f -> {
            var tables = Set.of("orders");
            Map<String, SrsModelKey> refs;
            if (checkpointed) {
                preparedTrimLog(f);
                var key = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 1, 0);
                refs = Map.of("orders", key);
                f.chains().requestTables(CHAIN, tables, f.captureFence());
                assertThat(f.chains().publishServingTables(CHAIN, 1, tables, f.captureFence())).isTrue();
                f.chains().checkpoint(CHAIN, 0, new io.tapstate.spi.store.SrsCaptureCheckpoint(
                        new ChainPosition(new SourceOrder(1, 4), "t4"),
                        Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(key, 4))), f.captureFence()).orElseThrow();
            } else refs = Map.of();
            var member = f.consumers().attach(A, 0, tables, f.fenceA()).orElseThrow();
            f.consumers().attach(B, 0, tables, f.fenceB()).orElseThrow();
            if (checkpointed) assertThat(f.consumers().pinSnapshot(A, member.revision(), 0,
                    new SrsSnapshotPin(1, "retained", refs), f.fenceA())).isTrue();
            var sibling = cursor(f, B);
            for (var table : List.of(SystemCollections.SRS_CHAINS, SystemCollections.SRS_CONSUMERS,
                    SystemCollections.CACHE_SRS_CONSUMER_OFFSETS, SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS)) {
                var template = table.on(f.database()).find().first();
                if (template == null) continue;
                var peers = new ArrayList<Document>();
                for (int i = 0; i < siblings; i++) {
                    var row = Document.parse(template.toJson());
                    Object key = row.get("_id");
                    if (key instanceof Document id) id.put("chain", "peer-" + i);
                    else row.put("_id", "peer-" + i);
                    row.put("cacheIncarnation", "peer-cache");
                    row.put("ringName", "peer-ring-" + i);
                    peers.add(row);
                }
                if (!peers.isEmpty()) table.on(f.database()).insertMany(peers);
            }
            var commands = new ArrayList<String>();
            MongoCommandObservation.COLLECTIONS.set((command, collection) -> commands.add(command + ":" + collection));
            io.tapstate.spi.store.SrsConsumerProgress progress;
            try {
                progress = runtimeConsumer(f.client(), f.database()).startReadingAfterAndGet(A, member.revision(),
                        checkpointed ? 1 : 0, Map.of("orders", checkpointed ? 4L : -1L), f.fenceA()).orElseThrow();
            } finally { MongoCommandObservation.COLLECTIONS.remove(); }
            System.out.println("ARRIVAL_COMMANDS checkpoint=" + checkpointed + " siblings=" + siblings + " count=" + commands.size() + " commands=" + commands);
            assertThat(progress.revision()).isEqualTo(checkpointed ? 2 : 1);
            assertThat(cursor(f, B)).isEqualTo(sibling);
            assertThat(commands).hasSize(checkpointed ? 14 : 8);
        });
    }

    @ParameterizedTest @ValueSource(strings = {"case-alias", "ordered-id", "large"})
    void arrivalPrefetchRetainsOriginalPointReadsForUncertainRows(String condition) {
        withBusiness(f -> {
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var table = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database());
            var expected = id(A);
            var row = table.find(expected).first();
            table.drop();
            var options = new com.mongodb.client.model.CreateCollectionOptions();
            if (condition.equals("case-alias")) options.collation(com.mongodb.client.model.Collation.builder().locale("en")
                    .collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build());
            f.database().createCollection(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.collectionName(), options);
            var storedId = row.get("_id", Document.class);
            if (condition.equals("case-alias")) storedId.put("consumer", storedId.getString("consumer").toUpperCase(java.util.Locale.ROOT));
            if (condition.equals("ordered-id")) {
                var ordered = new Document();
                var keys = new ArrayList<>(storedId.keySet());
                java.util.Collections.reverse(keys);
                keys.forEach(key -> ordered.put(key, storedId.get(key)));
                row.put("_id", ordered);
            }
            if (condition.equals("large")) row.put("extra", "x".repeat(70 * 1024));
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).insertOne(row);
            try (var session = f.client().startSession()) {
                session.startTransaction();
                var original = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).find(session, expected).first();
                var facts = new MongoSrsReadFacts(session, f.database(), SCOPE, true);
                facts.prefetchArrival(CHAIN, new MongoSrsConsumerRecords(SCOPE).id(A.key()));
                assertThat(MongoSrsReadFacts.find(facts, session, f.database(), SystemCollections.CACHE_SRS_CONSUMER_OFFSETS, expected))
                        .isEqualTo(original);
                session.commitTransaction();
            }
        });
    }

    @org.junit.jupiter.api.RepeatedTest(5)
    void startupArrivalPreparationLetsTheSamePipelineRenewBeforePublishingOnce() {
        withBusiness(f -> {
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var renewal = new java.util.concurrent.atomic.AtomicReference<io.tapstate.spi.store.WorkloadClaim>();
            var probe = new ArrivalPreparationProbe(() -> {
                try (var lease = f.client().startSession()) {
                    lease.startTransaction();
                    renewal.set(claims.inStrictSession(lease).renewBeforeReads(f.pipelineA(), Duration.ofSeconds(30))
                            .orElseThrow().claim());
                    lease.commitTransaction();
                }
            });

            try (var client = arrivalProbeClient(probe)) {
                var arrivals = runtimeConsumer(client, client.getDatabase(f.database().getName()));
                var cursor = arrivals.startReadingAfterAndGet(A, member.revision(), 0, Map.of("orders", -1L), f.fenceA()).orElseThrow();
                assertThat(probe.failure.get()).as("the same Pipeline can renew while the cursor is being read").isNull();
                assertThat(renewal.get()).isNotNull();
                assertThat(WorkloadClaimFence.from(renewal.get())).isEqualTo(f.fenceA());
                assertThat(probe.attempts).hasValue(2);
                assertThat(probe.conflicts).hasValue(1);
                assertThat(probe.commits).hasValue(1);
                assertThat(cursor.revision()).isOne();
                assertThat(cursor.ringDoneThrough()).containsEntry("orders", -1L);
                assertThat(f.consumers().progress(A)).contains(cursor);
                System.out.printf("ARRIVAL_RENEWAL renewal_committed=true attempts=%d conflicts=%d cursor_commits=%d%n",
                        probe.attempts.get(), probe.conflicts.get(), probe.commits.get());
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"expired", "new-owner", "new-execution", "retired"})
    void startupArrivalRejectsAuthorityLostDuringPreparation(String loss) {
        withBusiness(f -> {
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            var originalCursor = cursor(f, A);
            var states = SystemCollections.CACHE_PIPELINE_STATE.on(f.database());
            var stateKey = new Document("_id", A.key().pipelineId());
            var originalState = states.find(stateKey).first();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            if (loss.equals("expired")) assertThat(claims.renew(f.pipelineA(), Duration.ofSeconds(3))).isPresent();
            if (loss.equals("retired")) {
                SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document("_id", "cluster"),
                        new Document("$set", new Document("claimAdmissionFence", 0L)));
            }
            var probe = new ArrivalPreparationProbe(() -> {
                switch (loss) {
                    case "expired" -> {
                        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (claims.read(f.pipelineA().key()).orElseThrow().leased() && System.nanoTime() < limit) {
                            Thread.sleep(25);
                        }
                        assertThat(claims.read(f.pipelineA().key()).orElseThrow().leased()).isFalse();
                    }
                    case "new-owner" -> {
                        assertThat(claims.release(f.pipelineA())).isTrue();
                        assertThat(claims.acquire(f.pipelineA().key(), new WorkloadOwner("new-node", "new-boot"),
                                1, Duration.ofSeconds(30)).acquired()).isTrue();
                    }
                    case "new-execution" -> assertThat(claims.advanceExecution(f.pipelineA(),
                            f.pipelineA().topologyRevision(), Set.of(f.pipelineA().owner().nodeId()))).isPresent();
                    case "retired" -> {
                        try (var retirement = f.client().startSession()) {
                            retirement.startTransaction();
                            new MongoCacheBusinessIdentity(f.database(), SCOPE).retire(retirement, "retire-before-arrival");
                            retirement.commitTransaction();
                        }
                    }
                    default -> throw new AssertionError(loss);
                }
            });

            try (var client = arrivalProbeClient(probe)) {
                var arrivals = runtimeConsumer(client, client.getDatabase(f.database().getName()));
                assertThatThrownBy(() -> arrivals.startReadingAfterAndGet(A, member.revision(), 0, Map.of("orders", -1L), f.fenceA()))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(
                                loss.equals("retired") ? IoError.CACHE_BUSINESS_FENCED : IoError.WORKLOAD_CLAIM_FENCED));
                assertThat(probe.failure.get()).isNull();
                assertThat(probe.paused).isTrue();
                assertThat(probe.commits).hasValue(0);
                assertThat(cursor(f, A)).isEqualTo(originalCursor);
                assertThat(states.find(stateKey).first()).isEqualTo(originalState);
            }
        });
    }

    private static MongoClient arrivalProbeClient(ArrivalPreparationProbe probe) {
        return MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(probe).build());
    }

    private static final class ArrivalPreparationProbe implements com.mongodb.event.CommandListener {
        interface CompetingWrite { void run() throws Exception; }

        private final CompetingWrite competing;
        private final java.util.concurrent.atomic.AtomicInteger cursorRead = new java.util.concurrent.atomic.AtomicInteger(-1);
        private final java.util.concurrent.atomic.AtomicBoolean paused = new java.util.concurrent.atomic.AtomicBoolean();
        private final java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger conflicts = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger commits = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();

        ArrivalPreparationProbe(CompetingWrite competing) { this.competing = competing; }

        @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
            var body = event.getCommand();
            if (body.containsKey("startTransaction")) attempts.incrementAndGet();
            boolean cursor = "find".equals(event.getCommandName()) && "srs_consumer_offsets".equals(body.getString("find").getValue());
            if ("aggregate".equals(event.getCommandName()) && "srs_chains".equals(body.getString("aggregate").getValue())) {
                cursor = body.getArray("pipeline").stream().anyMatch(stage -> stage.isDocument()
                        && stage.asDocument().containsKey("$lookup")
                        && "srs_consumer_offsets".equals(stage.asDocument().getDocument("$lookup").getString("as").getValue()));
            }
            if (cursor) cursorRead.compareAndSet(-1, event.getRequestId());
        }

        @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
            if ("commitTransaction".equals(event.getCommandName())) commits.incrementAndGet();
            if (event.getRequestId() != cursorRead.get() || !paused.compareAndSet(false, true)) return;
            try {
                competing.run();
            } catch (Exception | AssertionError error) {
                failure.set(error);
            }
        }

        @Override public void commandFailed(com.mongodb.event.CommandFailedEvent event) {
            if (event.getThrowable() instanceof com.mongodb.MongoCommandException error && error.getCode() == 112) {
                conflicts.incrementAndGet();
            }
        }
    }

    @Test void aBatchInitializesSeparateSampledFloorsWithOneRevisionAndKeepsConfirmedPlaces() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders","items"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var other = cursor(f,B); var chain = root(f);
            var arrivals = Map.of("orders",10L,"items",20L);
            var startup = runtimeConsumer(f.client(), f.database());
            var attached = startup.attachedWithChain(CHAIN);
            assertThat(attached.topology()).isEqualTo(f.consumers().attached(CHAIN));
            assertThat(attached.chain()).isEqualTo(f.chains().load(CHAIN).orElseThrow());
            var initialized = startup.startReadingAfterAndGet(A, 1, 0, arrivals, f.fenceA()).orElseThrow();
            assertThat(f.consumers().progress(A)).contains(initialized);
            assertThat(initialized.revision()).isOne();
            assertThat(initialized.ringDoneThrough()).isEqualTo(arrivals);
            assertThat(initialized.cursor().perTableSeq()).isEqualTo(arrivals);
            assertThat(initialized.cursor().sinkAcked()).isNull();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",99,f.fenceA())).isTrue();
            var progressed = f.consumers().progress(A).orElseThrow();
            assertThat(f.consumers().startReadingAfter(A,1,0,arrivals,f.fenceA())).isTrue();
            assertThat(f.consumers().progress(A)).contains(progressed);
            assertThat(cursor(f,B)).isEqualTo(other); assertThat(root(f)).isEqualTo(chain);
        });
    }

    @Test void singleTableStartupReadsTheOriginalChainProgressModelAndBoundsInFewerCommands() {
        withBusiness(f -> {
            preparedTrimLog(f);
            var tables = Set.of("orders");
            var selection = Map.of("orders", ring("orders"));
            f.chains().requestTables(CHAIN, tables, f.captureFence());
            assertThat(f.chains().publishServingTables(CHAIN, 1, tables, f.captureFence())).isTrue();
            var key = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 1, 0);
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,4), "t4"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(key, 4)));
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            f.consumers().attach(A, 0, tables, f.fenceA()).orElseThrow();

            var separate = new ArrayList<String>();
            var combined = new ArrayList<String>();
            io.tapstate.spi.store.SrsChainState chain;
            io.tapstate.spi.store.SrsConsumerProgress progress;
            Optional<Map<String, SrsModel>> published;
            io.tapstate.spi.store.SrsConsumerStore.StartupSnapshot sample;
            var before = controlRows(f);
            try {
                MongoCommandObservation.CURRENT.set(separate::add);
                chain = f.chains().load(CHAIN).orElseThrow();
                progress = f.consumers().progress(A).orElseThrow();
                published = models(f).publishedForNewSnapshot(CHAIN, selection);
                MongoCommandObservation.CURRENT.set(combined::add);
                sample = f.consumers().startupSnapshot(A, selection).orElseThrow();
            } finally {
                MongoCommandObservation.CURRENT.remove();
            }

            assertThat(sample.chain()).isEqualTo(chain);
            assertThat(sample.progress()).isEqualTo(progress);
            assertThat(sample.published()).isEqualTo(published).isPresent();
            assertThat(sample.bounds()).containsOnlyKeys(ring("orders"));
            assertThat(sample.bounds().get(ring("orders")).largestSequence()).isEqualTo(4);
            assertThat(combined.stream().filter("commitTransaction"::equals)).hasSize(1);
            assertThat(combined).hasSizeLessThan(separate.size());
            assertThat(controlRows(f)).isEqualTo(before);
        });
    }

    @Test void aBatchRefusesStaleRevisionsWrongClaimsAndUnselectedTablesWithoutPartialProgress() {
        withBusiness(f -> {
            var tables = Set.of("orders", "items");
            var old = f.consumers().attach(A, 0, tables, f.fenceA()).orElseThrow();
            var current = f.consumers().attach(A, old.revision(), tables, f.fenceA()).orElseThrow();
            var startup = runtimeConsumer(f.client(), f.database());
            var held = startup.startReadingAfterAndGet(A, current.revision(), 0, Map.of("orders", 10L), f.fenceA()).orElseThrow();
            var before = cursor(f,A); var arrivals = Map.of("orders",10L,"items",20L);
            assertThat(startup.startReadingAfterAndGet(A, old.revision(), held.revision(), arrivals, f.fenceA())).isEmpty();
            assertThat(startup.startReadingAfterAndGet(A, current.revision(), 0, arrivals, f.fenceA())).isEmpty();
            assertThatThrownBy(() -> startup.startReadingAfterAndGet(A, current.revision(), held.revision(), arrivals, f.fenceB()))
                    .satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThatThrownBy(() -> startup.startReadingAfterAndGet(A, current.revision(), held.revision(), Map.of("orders",10L,"outside",20L),f.fenceA()))
                    .satisfies(MongoSrsConsumerStoreIT::unavailable);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(
                    new Document("_id", MongoWorkloadClaimStore.id(f.fenceA().key())),
                    new Document("$inc", new Document("executionGeneration", 1L)));
            assertThatThrownBy(() -> startup.startReadingAfterAndGet(A, current.revision(), held.revision(), arrivals, f.fenceA()))
                    .satisfies(MongoSrsConsumerStoreIT::fenced);
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    @Test void aBatchAddsOnlyMissingFloorsAndCannotRaiseOneRetainedFloor() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders","items"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA());
            f.consumers().advanceReadSequence(A,1,"orders",99,f.fenceA());
            var before = cursor(f,A);
            assertThat(f.consumers().startReadingAfter(A,1,2,Map.of("orders",100L,"items",20L),f.fenceA())).isFalse();
            assertThat(f.consumers().startReadingAfter(A,1,0,Map.of("orders",5L,"items",20L),f.fenceA())).isFalse();
            assertThat(cursor(f,A)).isEqualTo(before);
            assertThat(f.consumers().startReadingAfter(A,1,2,Map.of("orders",5L,"items",20L),f.fenceA())).isTrue();
            var progress = f.consumers().progress(A).orElseThrow();
            assertThat(progress.revision()).isEqualTo(3);
            assertThat(progress.ringDoneThrough()).containsExactlyInAnyOrderEntriesOf(Map.of("orders",10L,"items",20L));
            assertThat(progress.cursor().perTableSeq()).containsExactlyInAnyOrderEntriesOf(Map.of("orders",99L,"items",20L));
        });
    }

    @Test void oneUnexplainedReadAheadRefusesTheWholeArrivalBatch() {
        withBusiness(f -> {
            var member = f.consumers().attach(A,0,Set.of("orders","items"),f.fenceA()).orElseThrow();
            var progress = f.consumers().progress(A).orElseThrow(); var old = progress.cursor();
            var unexplained = new io.tapstate.spi.store.ConsumerOffset(old.pipelineId(),Map.of("orders",-1L,"items",7L),
                    old.sinkAcked(),old.snapshotCompletedTables(),old.cdcStartPosition(),old.snapshotEpoch(),old.sinkAckedByTable(),old.progressKind());
            var damaged = new MongoSrsConsumerRecords(SCOPE).cursor(member,unexplained,0,null,null,Map.of());
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).replaceOne(id(A),damaged);
            assertThatThrownBy(() -> f.consumers().startReadingAfter(A,1,0,Map.of("orders",10L,"items",20L),f.fenceA()))
                    .satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(cursor(f,A)).isEqualTo(damaged);
        });
    }

    @Test void anUnprovenCheckpointStillRefusesAllBatchedArrivals() {
        withBusiness(f -> {
            var tables = Set.of("orders","items"); var refs = publishSnapshotModels(f,tables);
            f.chains().requestTables(CHAIN,tables,f.captureFence()); f.chains().publishServingTables(CHAIN,1,tables,f.captureFence());
            var log = new MongoSrsLogStore(f.client(),SystemCollections.SRS_EVENTS.on(f.database()),
                    SystemCollections.SRS_LOG_BOUNDS.on(f.database()),SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()),models(f));
            log.storeAll(ring("orders"),0,List.of(new io.tapstate.spi.store.SrsLogRecord("t0",io.tapstate.core.event.Op.INSERT,
                    1,null,Map.of("id",1),0,f.captureFence(),1)));
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,0),"t0"),
                    Map.of("orders",new io.tapstate.spi.store.SrsTableCheckpoint(refs.get("orders"),0),
                            "items",new io.tapstate.spi.store.SrsTableCheckpoint(refs.get("items"),-1)));
            f.chains().checkpoint(CHAIN,0,checkpoint,f.captureFence()).orElseThrow();
            f.consumers().attach(A,0,tables,f.fenceA()).orElseThrow(); var before = cursor(f,A);
            SystemCollections.SRS_EVENTS.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> f.consumers().startReadingAfter(A,1,0,Map.of("orders",0L,"items",-1L),f.fenceA()))
                    .satisfies(failure -> code(failure,IoError.SRS_CHECKPOINT_UNPROVEN));
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    @Test void competingBatchesCannotCombineFloorsFromDifferentOriginalSamples() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders","items"),f.fenceA()).orElseThrow();
            var first = Map.of("orders",10L,"items",20L); var second = Map.of("orders",20L,"items",10L);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var peer = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE);
                var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().startReadingAfter(A,1,0,first,f.fenceA()); });
                var b = executor.submit(() -> { start.await(); return peer.startReadingAfter(A,1,0,second,f.fenceA()); });
                start.countDown();
                assertThat(List.of(a.get(10,TimeUnit.SECONDS),b.get(10,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
                var progress = f.consumers().progress(A).orElseThrow();
                assertThat(progress.ringDoneThrough()).isIn(first,second);
                assertThat(progress.cursor().perTableSeq()).isEqualTo(progress.ringDoneThrough());
                assertThat(progress.revision()).isOne();
            }
        });
    }

    @Test void losingTheGuardAfterABatchedWriteRollsBackEveryFloorAndItsClaimFence() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders","items"),f.fenceA()).orElseThrow();
            var before = cursor(f,A); var chain = root(f);
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database());
            var key = new Document("_id",WorkloadClaimDocuments.live(f.fenceA()).get("_id")); var claim = claims.find(key).first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_consumer_offsets")) {
                var guarded = new MongoSrsConsumerStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> guarded.startReadingAfter(A,1,0,Map.of("orders",10L,"items",20L),f.fenceA()))
                        .satisfies(failure -> code(failure,IoError.CACHE_BUSINESS_FENCED));
                witness.requireCommand();
            }
            assertThat(cursor(f,A)).isEqualTo(before); assertThat(root(f)).isEqualTo(chain); assertThat(claims.find(key).first()).isEqualTo(claim);
        });
    }

    @Test
    void initialArrivalAndReadCursorCommitTogetherButReadAheadNeverMovesTheRecoveryFloor() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThatThrownBy(() -> f.consumers().advanceReadSequence(A,1,"orders",9,f.fenceA())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(f.consumers().startReadingAfter(A,1,4,"orders",10,f.fenceA())).isFalse();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA())).isTrue();
            var arrival = f.consumers().progress(A).orElseThrow();
            assertThat(arrival.ringDoneThrough()).containsEntry("orders",10L); assertThat(arrival.cursor().perTableSeq()).containsEntry("orders",10L);
            assertThat(arrival.cursor().sinkAcked()).isNull();
            assertThat(f.consumers().advanceReadSequence(A,1,"orders",99,f.fenceA())).isTrue();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",100,f.fenceA())).isFalse();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA())).isTrue();
            var read = f.consumers().progress(A).orElseThrow();
            assertThat(read.ringDoneThrough()).isEqualTo(arrival.ringDoneThrough()); assertThat(read.cursor().perTableSeq()).containsEntry("orders",99L);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var reopened = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE);
                assertThat(reopened.progress(A)).contains(read);
            }
        });
    }

    @Test
    void actualRingConfirmationSurvivesAnEpochEndBoundAndWriterRunReplacement() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA());
            f.consumers().beginWriterRun(A,1,"old",Map.of("orders",List.of("writer")),f.fenceA());
            f.consumers().advanceWriter(A,1,"old","orders","writer",point(1,12,"t12"),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",12L);
            f.consumers().advanceWriter(A,1,"old","orders","writer",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            var ended = f.consumers().progress(A).orElseThrow();
            assertThat(ended.cursor().sinkAckedByTable().get("orders").order().seq()).isEqualTo(Long.MAX_VALUE);
            assertThat(ended.ringDoneThrough()).containsEntry("orders",12L);
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var next = WorkloadClaimFence.from(claims.advanceExecution(f.pipelineA(),1,Set.of(f.pipelineA().owner().nodeId())).orElseThrow());
            f.consumers().attach(A,1,Set.of("orders"),next).orElseThrow();
            assertThat(f.consumers().beginWriterRun(A,2,"new",Map.of("orders",List.of("writer")),next)).isTrue();
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).isEqualTo(ended.ringDoneThrough());
            assertThat(f.consumers().startReadingAfter(A,2,0,"orders",999,next)).isFalse();
        });
    }

    @ParameterizedTest @ValueSource(booleans={true,false})
    void aFiniteConfirmationArrivingAfterTheEpochEndStillRaisesThePhysicalRingFloor(boolean tokened) {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA());
            f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("writer")),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","writer",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","writer",point(1,12,tokened ? "t12" : null),f.fenceA());
            var held = f.consumers().progress(A).orElseThrow();
            assertThat(held.cursor().sinkAckedByTable().get("orders").order().seq()).isEqualTo(Long.MAX_VALUE);
            assertThat(held.ringDoneThrough()).containsEntry("orders",12L);
        });
    }

    @Test
    void aLateFiniteTokenStillWaitsForTheOtherWritersDurableBound() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow(); f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA());
            f.consumers().beginWriterRun(A,1,"run",Map.of("orders",List.of("fast","slow")),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","fast",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            f.consumers().advanceWriter(A,1,"run","orders","fast",point(1,12,"t12"),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",10L);
            f.consumers().advanceWriter(A,1,"run","orders","slow",point(1,11,null),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",11L);
            f.consumers().advanceWriter(A,1,"run","orders","slow",new WriterProgress(new SourceOrder(1,Long.MAX_VALUE),null),f.fenceA());
            assertThat(f.consumers().progress(A).orElseThrow().ringDoneThrough()).containsEntry("orders",12L);
        });
    }

    @Test
    void concurrentOriginalArrivalsNeverOverwriteTheFirstCommittedSafePlace() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var second = new MongoSrsConsumerStore(other,other.getDatabase(f.database().getName()),SCOPE); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { start.await(); return f.consumers().startReadingAfter(A,1,0,"orders",10,f.fenceA()); });
                var b = executor.submit(() -> { start.await(); return second.startReadingAfter(A,1,0,"orders",20,f.fenceA()); }); start.countDown();
                assertThat(a.get(15,TimeUnit.SECONDS)).isTrue(); boolean high = b.get(15,TimeUnit.SECONDS);
                var held = f.consumers().progress(A).orElseThrow(); long floor = held.ringDoneThrough().get("orders");
                assertThat(floor).isIn(10L,20L); assertThat(high).isEqualTo(floor == 20);
                assertThat(held.cursor().perTableSeq()).containsEntry("orders",floor); assertThat(held.revision()).isOne();
            }
        });
    }

    @ParameterizedTest @ValueSource(strings={"missing","fraction","unknown-table"})
    void aMalformedRecoveryFloorCannotFallBackToReadAheadOrADifferentArrival(String fault) {
        withBusiness(f -> {
            var member = f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",5,f.fenceA());
            if (fault.equals("missing")) SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$unset",new Document("ringDoneThrough","")));
            else SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("ringDoneThrough.0."+(fault.equals("fraction") ? "sequence" : "table"),fault.equals("fraction") ? 5.5D : "unknown")));
            var before = cursor(f,A);
            assertThatThrownBy(() -> f.consumers().progress(A)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().startReadingAfter(A,member.revision(),1,"orders",99,f.fenceA())).isInstanceOf(TapstateException.class);
            assertThat(cursor(f,A)).isEqualTo(before);
        });
    }

    private static WriterProgress point(long epoch,long sequence,String token) {
        var order = new SourceOrder(epoch,sequence); return new WriterProgress(order,token == null ? null : new ChainPosition(order,token));
    }

    private static Map<String, SrsModelKey> publishSnapshotModels(Fixture f, Set<String> tables) {
        assertThat(f.chains().openEpoch(CHAIN, 0, f.captureFence())).hasValue(1);
        var refs = new java.util.HashMap<String, SrsModelKey>();
        for (String table : tables) {
            var key = new SrsModelKey(CHAIN, table, SCOPE.cacheIncarnation(), 1, 0);
            models(f).publish(new SrsModel(key, ring(table), new SourceTable(table, List.of(new SourceField("id", "int")), List.of(), List.of()), 0), f.captureFence());
            refs.put(table, key);
        }
        return Map.copyOf(refs);
    }
    private static String ring(String table) { return "srs." + CHAIN + "." + table; }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void incrementalBatchKeepsSchemaBoundariesAndOriginalProgressFences(boolean leased) {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            var member = f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            assertThat(f.consumers().startReadingAfter(A, member.revision(), 0, "orders", -1, f.fenceA())).isTrue();
            assertThat(f.consumers().pinSnapshot(A, member.revision(), 1, new SrsSnapshotPin(1, "seam", refs), f.fenceA())).isTrue();
            assertThat(f.consumers().beginWriterRun(A, member.revision(), "run", Map.of("orders", List.of("sink")), f.fenceA())).isTrue();
            var scope = new io.tapstate.spi.store.PipelineIntentScope(
                    SCOPE.clusterId(), SCOPE.bindingVersion(), SCOPE.cacheIncarnation(), A.key().pipelineId(), A.stateGeneration());
            var run = io.tapstate.spi.store.SrsConsumerRun.of(scope, member);
            var ring = ring("orders");
            var next = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 1, 1), ring,
                    new SourceTable("orders", List.of(new SourceField("id", "long")), List.of(), List.of()), 100);
            models(f).publish(next, f.captureFence());
            var structure = new MongoCacheMetadataStructure(f.database(), Duration.ofHours(1));
            var store = leased
                    ? new MongoSrsConsumerStore(f.client(), f.database(), SCOPE, () -> {}, structure::verifySrs,
                            MongoSrsLease.selected(), true)
                    : runtimeConsumer(f.client(), f.database());
            var beforeDdl = new io.tapstate.spi.store.SrsConsumerStore.ReadEvent(99, 1, 0, f.captureFence());
            var afterDdl = new io.tapstate.spi.store.SrsConsumerStore.ReadEvent(100, 1, 1, f.captureFence());
            var wrongSchema = new io.tapstate.spi.store.SrsConsumerStore.ReadEvent(100, 1, 0, f.captureFence());
            var changed = f.captureFence();
            var wrongCapture = new WorkloadClaimFence(changed.key(), changed.owner(), changed.claimGeneration() + 1,
                    changed.executionGeneration(), changed.topologyRevision());
            var observedCommands = new java.util.ArrayList<String>();
            MongoCommandObservation.CURRENT.set(observedCommands::add);
            try {
                assertThat(store.verifyReadEvents(run, "orders", ring, List.of(beforeDdl, afterDdl))).isTrue();
                assertThat(observedCommands).contains("find");
                if (leased) {
                    observedCommands.clear();
                    assertThat(store.verifyReadEvents(run, "orders", ring, List.of(beforeDdl, afterDdl))).isTrue();
                    assertThat(observedCommands).isEmpty();
                }
            } finally {
                MongoCommandObservation.CURRENT.remove();
            }
            assertThatThrownBy(() -> store.verifyReadEvents(run, "orders", ring, List.of(beforeDdl, wrongSchema)))
                    .satisfies(error -> code(error, IoError.SRS_EVENT_MODEL_UNAVAILABLE));
            assertThatThrownBy(() -> store.verifyReadEvents(run, "orders", ring, List.of(afterDdl,
                    new io.tapstate.spi.store.SrsConsumerStore.ReadEvent(101, 1, 1, wrongCapture))))
                    .satisfies(error -> code(error, IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(store.advanceReadSequence(A, member.revision(), "orders", 100, f.fenceA())).isTrue();
            assertThat(store.advanceWriter(A, member.revision(), "run", "orders", "sink", point(1, 100, "t100"), f.fenceA())).isTrue();
            var before = cursor(f, A);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(
                    new Document("_id", MongoWorkloadClaimStore.id(f.fenceA().key())),
                    new Document("$inc", new Document("executionGeneration", 1L)));
            assertThatThrownBy(() -> store.advanceReadSequence(A, member.revision(), "orders", 101, f.fenceA()))
                    .satisfies(error -> code(error, IoError.WORKLOAD_CLAIM_FENCED));
            assertThatThrownBy(() -> store.advanceWriter(A, member.revision(), "run", "orders", "sink", point(1, 101, "t101"), f.fenceA()))
                    .satisfies(error -> code(error, IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(cursor(f, A)).isEqualTo(before);
        });
    }

    private static MongoSrsConsumerStore runtimeConsumer(MongoClient client, MongoDatabase database) {
        return MongoSrsConsumerStore.admittedRuntime(client, database, SCOPE, () -> {}, () -> {});
    }

    @Test void fusedReadKeepsTheHistoricalModelAndItsExactEffectiveRange() {
        withBusiness(f->{
            var refs=publishSnapshotModels(f,Set.of("orders"));
            var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().pinSnapshot(A,member.revision(),0,new SrsSnapshotPin(1,"seam",refs),f.fenceA())).isTrue();
            var scope=new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration());
            var run=io.tapstate.spi.store.SrsConsumerRun.of(scope,member);
            var next=new SrsModel(new SrsModelKey(CHAIN,"orders",SCOPE.cacheIncarnation(),1,1),ring("orders"),
                    new SourceTable("orders",List.of(new SourceField("id","long")),List.of(),List.of()),100);
            models(f).publish(next,f.captureFence());
            assertThat(f.consumers().supportsReadEventVerification()).isTrue();
            assertThat(f.consumers().verifyReadEvent(run,"orders",ring("orders"),99,1,0,f.captureFence()))
                    .contains(models(f).requireEvent(refs.get("orders"),ring("orders"),99,f.captureFence()));
            assertThat(f.consumers().verifyReadEvent(run,"orders",ring("orders"),100,1,1,f.captureFence())).contains(next);
            for(long[] coordinates:List.of(new long[]{100,0},new long[]{99,1},new long[]{1,2}))
                assertThatThrownBy(()->f.consumers().verifyReadEvent(run,"orders",ring("orders"),coordinates[0],1,coordinates[1],f.captureFence()))
                        .isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(IoError.SRS_EVENT_MODEL_UNAVAILABLE));
            assertThatThrownBy(()->f.consumers().verifyReadEvent(run,"orders","another-ring",99,1,0,f.captureFence()))
                    .isInstanceOf(TapstateException.class);
            var capture=f.captureFence();var changed=new WorkloadClaimFence(capture.key(),capture.owner(),capture.claimGeneration()+1,capture.executionGeneration(),capture.topologyRevision());
            assertThatThrownBy(()->f.consumers().verifyReadEvent(run,"orders",ring("orders"),99,1,0,changed))
                    .isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        });
    }

    @ParameterizedTest @ValueSource(strings={"revision","state","detached","table","snapshot","epoch","sequence","capture"})
    void fusedReadRetainsConsumerAndEventRejections(String changed) {
        withBusiness(f->{
            var refs=publishSnapshotModels(f,Set.of("orders"));
            var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().pinSnapshot(A,member.revision(),0,new SrsSnapshotPin(1,"seam",refs),f.fenceA())).isTrue();
            var scope=new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration()+(changed.equals("state")?1:0));
            var run=new io.tapstate.spi.store.SrsConsumerRun(scope,CHAIN,A.key().sourceId(),member.revision()+(changed.equals("revision")?1:0),
                    f.fenceA().owner().nodeId(),f.fenceA().owner().bootId(),f.fenceA().claimGeneration(),f.fenceA().executionGeneration(),f.fenceA().topologyRevision());
            if(changed.equals("detached"))f.consumers().detach(A,member.revision(),f.fenceA());
            if(changed.equals("snapshot"))SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).updateOne(id(A),new Document("$set",new Document("snapshot.snapshotEpoch",1.0)));
            if(changed.equals("state")){
                assertThatThrownBy(()->f.consumers().verifyReadEvent(run,"orders",ring("orders"),0,1,0,f.captureFence()))
                        .isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(IoError.SRS_CONSUMER_UNAVAILABLE));
            }else if(Set.of("revision","detached","table").contains(changed)){
                assertThat(f.consumers().verifyReadEvent(run,changed.equals("table")?"missing":"orders",ring("orders"),0,1,0,f.captureFence())).isEmpty();
            }else assertThatThrownBy(()->f.consumers().verifyReadEvent(run,"orders",ring("orders"),changed.equals("sequence")?-1:0,
                    changed.equals("epoch")?0:1,0,changed.equals("capture")?null:f.captureFence())).isInstanceOf(TapstateException.class);
        });
    }

    @Test void fusedRetriesAreReadOnlyAndConcurrentReadersKeepAllOriginalChecks() {
        withBusiness(f->{
            var refs=publishSnapshotModels(f,Set.of("orders"));var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().pinSnapshot(A,member.revision(),0,new SrsSnapshotPin(1,"seam",refs),f.fenceA())).isTrue();
            var scope=new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration());
            var run=io.tapstate.spi.store.SrsConsumerRun.of(scope,member);var before=cursor(f,A);var originalRoot=root(f);
            var originalClaims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find().sort(new Document("_id",1)).into(new java.util.ArrayList<>());
            var guards=new java.util.concurrent.atomic.AtomicInteger();
            var retrying=f.consumers().guardedBy(()->{
                if(guards.incrementAndGet()==3){var retry=new com.mongodb.MongoException(112,"transient read fixture");
                    retry.addLabel("TransientTransactionError");throw retry;}
            });
            assertThat(retrying.verifyReadEvent(run,"orders",ring("orders"),0,1,0,f.captureFence())).isPresent();
            assertThat(guards.get()).isGreaterThan(3);
            Runnable scalar=()->{
                var held=f.consumers().progress(A).orElseThrow();assertThat(held.membershipRevision()).isEqualTo(member.revision());
                assertThat(held.cursor().perTableSeq()).containsKey("orders");
                assertThat(models(f).requireEvent(refs.get("orders"),ring("orders"),0,f.captureFence()).key()).isEqualTo(refs.get("orders"));
            };
            Runnable fused=()->assertThat(f.consumers().verifyReadEvent(run,"orders",ring("orders"),0,1,0,f.captureFence()).orElseThrow().key()).isEqualTo(refs.get("orders"));
            scalar.run();fused.run();
            long scalarNanos=readWave(scalar),fusedNanos=readWave(fused);
            System.out.println("Concurrent original event verification: readers=4 events=32 scalarNanos="+scalarNanos+" fusedNanos="+fusedNanos);
            assertThat(cursor(f,A)).isEqualTo(before);assertThat(root(f)).isEqualTo(originalRoot);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find().sort(new Document("_id",1)).into(new java.util.ArrayList<>())).isEqualTo(originalClaims);
        });
    }

    private static long readWave(Runnable read) throws Exception {
        try(var executor=Executors.newFixedThreadPool(4)){
            var ready=new CountDownLatch(4);var release=new CountDownLatch(1);var futures=new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for(int worker=0;worker<4;worker++)futures.add(executor.submit(()->{
                ready.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("read wave was not released");}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                for(int event=0;event<8;event++)read.run();
            }));
            assertThat(ready.await(5,TimeUnit.SECONDS)).isTrue();long started=System.nanoTime();release.countDown();
            for(var future:futures)future.get(15,TimeUnit.SECONDS);
            return System.nanoTime()-started;
        }
    }

    @Test void concurrentFusedReadsCannotBorrowAReplacedMembership() {
        withBusiness(f->{
            var refs=publishSnapshotModels(f,Set.of("orders"));var member=f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().pinSnapshot(A,member.revision(),0,new SrsSnapshotPin(1,"seam",refs),f.fenceA())).isTrue();
            var scope=new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration());
            var run=io.tapstate.spi.store.SrsConsumerRun.of(scope,member);
            var entered=new CountDownLatch(4);var release=new CountDownLatch(1);
            var first=java.util.concurrent.ConcurrentHashMap.<Thread>newKeySet();
            var guarded=f.consumers().guardedBy(()->{
                if(first.add(Thread.currentThread())){entered.countDown();try{
                    if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("membership barrier was not released");
                }catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}}
            });
            try(var executor=Executors.newFixedThreadPool(4)){
                var readers=new java.util.ArrayList<java.util.concurrent.Future<?>>();
                for(int i=0;i<4;i++)readers.add(executor.submit(()->assertThat(guarded.verifyReadEvent(run,"orders",ring("orders"),0,1,0,f.captureFence())).isEmpty()));
                try{
                    assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();
                    assertThat(f.consumers().attach(A,member.revision(),Set.of("orders"),f.fenceA())).isPresent();
                    release.countDown();for(var reader:readers)reader.get(5,TimeUnit.SECONDS);
                }finally{release.countDown();}
            }
        });
    }
    private static MongoSrsModelStore models(Fixture f) { return new MongoSrsModelStore(f.client(), f.database(), SCOPE); }

    private static Document cursor(Fixture f, SrsConsumerContext context) { return SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).find(id(context)).first(); }
    private static Document root(Fixture f) { return SystemCollections.SRS_CHAINS.on(f.database()).find().first(); }
    private static void unavailable(Throwable failure) { code(failure, IoError.SRS_CONSUMER_UNAVAILABLE); }
    private static void fenced(Throwable failure) { code(failure, IoError.WORKLOAD_CLAIM_FENCED); }
    private static void code(Throwable failure, IoError expected) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> { assertThat(error.code()).isEqualTo(expected); assertThat(error.getCause()).isNull(); });
    }
    @Test void trimmingAnEarlierConfirmedCutCannotDeleteANewConsumersUnconfirmedRows() {
        withBusiness(f -> {
            publishSnapshotModels(f,Set.of("orders"));var log=new MongoSrsLogStore(f.client(),SystemCollections.SRS_EVENTS.on(f.database()),
                    SystemCollections.SRS_LOG_BOUNDS.on(f.database()),SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()),models(f));
            var events=new java.util.ArrayList<io.tapstate.spi.store.SrsLogRecord>();
            for(int i=0;i<5;i++)events.add(new io.tapstate.spi.store.SrsLogRecord("t"+i,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",i),0,f.captureFence(),1));
            log.storeAll(ring("orders"),0,events);
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            assertThat(f.consumers().startReadingAfter(A,1,0,"orders",4,f.fenceA())).isTrue();
            long oldCut=f.consumers().progress(A).orElseThrow().ringDoneThrough().get("orders");
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            log.trim(ring("orders"),oldCut,f.captureFence());
            assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(-1);
            assertThat(log.readBatch(ring("orders"),0,5).records()).hasSize(5);
            assertThat(f.consumers().startReadingAfter(B,1,0,"orders",1,f.fenceB())).isTrue();
            log.trim(ring("orders"),oldCut,f.captureFence());
            assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(1);
            assertThat(log.readBatch(ring("orders"),2,3).records().keySet()).containsExactly(2L,3L,4L);
            assertThat(f.consumers().advanceReadSequence(B,1,"orders",4,f.fenceB())).isTrue();
            log.trim(ring("orders"),oldCut,f.captureFence());assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(1);
            f.consumers().detach(B,1,f.fenceB()).orElseThrow();log.trim(ring("orders"),oldCut,f.captureFence());
            assertThat(log.bounds(ring("orders")).trimmedThrough()).isEqualTo(4);assertThat(log.readBatch(ring("orders"),0,5).records()).isEmpty();
        });
    }

    @Test void attachAfterTheTrimSnapshotForcesTheOriginalTransactionToRecheckItsCut() {
        withBusiness(f -> {
            var original=preparedTrimLog(f);f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",4,f.fenceA());
            var paused=new CountDownLatch(1);var proceed=new CountDownLatch(1);var once=new java.util.concurrent.atomic.AtomicBoolean();
            var pending=java.util.concurrent.ConcurrentHashMap.<Integer>newKeySet();
            var listener=new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if(event.getCommandName().equals("find")&&event.getCommand().getString("find").getValue().equals(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.collectionName()))pending.add(event.getRequestId());
                }
                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if(pending.remove(event.getRequestId())&&once.compareAndSet(false,true)) {
                        paused.countDown();try{if(!proceed.await(15,TimeUnit.SECONDS))throw new AssertionError("trim race barrier did not open");}
                        catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
                    }
                }
            };
            try(var observed=MongoClients.create(com.mongodb.MongoClientSettings.builder().applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build());
                var workers=Executors.newSingleThreadExecutor()) {
                var database=observed.getDatabase(f.database().getName());var models=new MongoSrsModelStore(observed,database,SCOPE);
                var log=new MongoSrsLogStore(observed,SystemCollections.SRS_EVENTS.on(database),SystemCollections.SRS_LOG_BOUNDS.on(database),SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database),models);
                var result=workers.submit(() -> log.trim(ring("orders"),4,f.captureFence()));
                try {
                    assertThat(paused.await(10,TimeUnit.SECONDS)).as("the trim read its original consumer snapshot").isTrue();
                    f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
                } finally {proceed.countDown();}
                result.get(20,TimeUnit.SECONDS);
            }
            assertThat(original.bounds(ring("orders")).trimmedThrough()).isEqualTo(-1);
            assertThat(original.readBatch(ring("orders"),0,5).records()).hasSize(5);
            assertThat(f.consumers().attached(CHAIN).consumers()).hasSize(2);
        });
    }

    @Test void trimCannotInterpretAMissingCursorAsAnUnreferencedLog() {
        withBusiness(f -> {
            var log=preparedTrimLog(f);f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().startReadingAfter(A,1,0,"orders",4,f.fenceA());
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).deleteOne(id(A));
            var bounds=SystemCollections.SRS_LOG_BOUNDS.on(f.database()).find().first();var claim=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",MongoWorkloadClaimStore.id(f.captureFence().key()))).first();
            assertThatThrownBy(() -> log.trim(ring("orders"),4,f.captureFence())).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).find().first()).isEqualTo(bounds);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isEqualTo(5);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find(new Document("_id",MongoWorkloadClaimStore.id(f.captureFence().key()))).first()).isEqualTo(claim);
        });
    }
    private static MongoSrsLogStore preparedTrimLog(Fixture f) {
        publishSnapshotModels(f,Set.of("orders"));var log=new MongoSrsLogStore(f.client(),SystemCollections.SRS_EVENTS.on(f.database()),
                SystemCollections.SRS_LOG_BOUNDS.on(f.database()),SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()),models(f));
        var events=new java.util.ArrayList<io.tapstate.spi.store.SrsLogRecord>();
        for(int i=0;i<5;i++)events.add(new io.tapstate.spi.store.SrsLogRecord("t"+i,io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",i),0,f.captureFence(),1));
        log.storeAll(ring("orders"),0,events);return log;
    }

    @Test void rebuildPreflightRequiresOnlyAConsumerDetachedAndKeepsBUnchanged() {
        withBusiness(f -> {
            f.consumers().attach(A,0,Set.of("orders"),f.fenceA()).orElseThrow();
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var bMember = membership(f,B); var bCursor = cursor(f,B);
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(failure ->
                    assertThat(((TapstateException)failure).code()).isEqualTo(IoError.PIPELINE_STATE_REBUILD_REQUIRED));
            f.consumers().detach(A,1,f.fenceA()).orElseThrow(); var chain = root(f);
            rebuildPreflight(f);
            assertThat(membership(f,B)).isEqualTo(bMember); assertThat(cursor(f,B)).isEqualTo(bCursor);
            assertThat(root(f)).isEqualTo(chain).containsEntry("consumerCount",1L);
        });
    }
    @Test void rebuildPreflightCannotIgnoreAnOrphanOffsetEvenWithoutAnAMembership() {
        withBusiness(f -> {
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            var orphan = new Document(cursor(f,B)); var oldId = orphan.get("_id",Document.class);
            orphan.put("_id",new Document(oldId).append("consumer","unknown-consumer"));
            SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).insertOne(orphan);
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.database()).countDocuments()).isEqualTo(2);
        });
    }
    @Test void rebuildPreflightCannotInterpretMissingCollectionOrUnclosedCountsAsNoAConsumer() {
        withBusiness(f -> {
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document("_id",CHAIN),new Document("$inc",new Document("consumerCount",1L).append("consumerRevision",1L)));
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            SystemCollections.SRS_CONSUMERS.on(f.database()).drop();
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(failure ->
                    assertThat(((TapstateException)failure).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(f.database().listCollectionNames().into(new java.util.ArrayList<>())).doesNotContain("srs_consumers");
        });
    }
    @Test void rebuildPreflightFindsMembershipsWhoseChainRootWasLost() {
        withBusiness(f -> {
            f.consumers().attach(B,0,Set.of("orders"),f.fenceB()).orElseThrow();
            SystemCollections.SRS_CHAINS.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> rebuildPreflight(f)).satisfies(MongoSrsConsumerStoreIT::unavailable);
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).countDocuments()).isZero();
        });
    }
    private static void rebuildPreflight(Fixture f) {
        var original = new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),A.key().pipelineId(),A.stateGeneration());
        try (var session = f.client().startSession()) {
            session.withTransaction(() -> { f.consumers().requireDetachedPipeline(session,original); return null; });
        }
    }

    @Test void theExactRestrictedCheckpointDoesNotRevokeReadersOrClearTheirProgress() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            f.chains().requestTables(CHAIN, Set.of("orders"), f.captureFence());
            f.chains().publishServingTables(CHAIN, 1, Set.of("orders"), f.captureFence());
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "original"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(refs.get("orders"), -1)), true);
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            f.consumers().pinSnapshot(A, 1, 0, new SrsSnapshotPin(1, "original", refs), f.fenceA());
            f.consumers().beginWriterRun(A, 1, "original-run", Map.of("orders", List.of("writer")), f.fenceA());
            f.consumers().advanceWriter(A, 1, "original-run", "orders", "writer", point(1, 3, "t3"), f.fenceA());
            var before = f.consumers().progress(A).orElseThrow();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var node = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, "node-edit"),
                    new WorkloadOwner("node-edit", "boot-edit"), 1, Duration.ofSeconds(30)).claim();
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("claimAdmissionFence", 0L)));
            var readers = Map.of(positionScope(A), f.fenceA(), positionScope(B), f.fenceB());
            var edit = f.consumers().positions(WorkloadClaimFence.from(node));
            assertThatThrownBy(() -> edit.rewrite(List.of(positionMove(f, "unknown", f.captureFence())), readers, java.time.Instant.now()))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.SRS_POSITION_MODEL_UNPROVEN));
            edit.rewrite(List.of(positionMove(f, "original", f.captureFence())), readers, java.time.Instant.now());
            assertThat(f.chains().load(CHAIN).orElseThrow().resumeOverride()).isNull();
            assertThat(f.consumers().progress(A)).contains(before);
            assertThat(claims.read(f.pipelineA().key()).orElseThrow().leased()).isTrue();
            assertThat(claims.read(f.pipelineB().key()).orElseThrow().leased()).isTrue();
        });
    }

    @Test void positionEditsPreserveTruthFenceOldWritersAndAllowANewEditAfterReacquisition() {
        withBusiness(f -> {
            var refs = publishSnapshotModels(f, Set.of("orders"));
            f.chains().requestTables(CHAIN, Set.of("orders"), f.captureFence());
            f.chains().publishServingTables(CHAIN, 1, Set.of("orders"), f.captureFence());
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "original"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(refs.get("orders"), -1)));
            f.chains().checkpoint(CHAIN, 0, checkpoint, f.captureFence()).orElseThrow();
            f.consumers().attach(A, 0, Set.of("orders"), f.fenceA()).orElseThrow();
            f.consumers().attach(B, 0, Set.of("orders"), f.fenceB()).orElseThrow();
            f.consumers().pinSnapshot(A, 1, 0, new SrsSnapshotPin(1, "original", refs), f.fenceA());
            f.consumers().beginWriterRun(A, 1, "old-run", Map.of("orders", List.of("writer")), f.fenceA());
            f.consumers().advanceWriter(A, 1, "old-run", "orders", "writer", point(1, 3, "t3"), f.fenceA());
            var before = f.consumers().progress(A).orElseThrow();
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            var node = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, "node-edit"),
                    new WorkloadOwner("node-edit", "boot-edit"), 1, Duration.ofSeconds(30)).claim();
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("claimAdmissionFence", 0L)));
            var readers = Map.of(positionScope(A), f.fenceA(), positionScope(B), f.fenceB());
            var edit = f.consumers().positions(WorkloadClaimFence.from(node));
            edit.rewrite(List.of(positionMove(f, "requested", f.captureFence())), readers, java.time.Instant.now());
            var changed = f.chains().load(CHAIN).orElseThrow();
            assertThat(changed.checkpoint()).isEqualTo(checkpoint); assertThat(changed.checkpointRevision()).isOne();
            assertThat(changed.resumeOverride().token()).isEqualTo("requested");
            var after = f.consumers().progress(A).orElseThrow();
            assertThat(after.cursor().sinkAcked()).isNull(); assertThat(after.cursor().sinkAckedByTable()).isEmpty(); assertThat(after.writerRun()).isNull();
            assertThat(after.snapshot()).isEqualTo(before.snapshot()); assertThat(after.ringDoneThrough()).isEqualTo(before.ringDoneThrough());
            assertThat(after.cursor().perTableSeq()).isEqualTo(before.cursor().perTableSeq());
            assertThat(claims.read(f.pipelineA().key()).orElseThrow().leased()).isFalse();
            assertThatThrownBy(() -> f.consumers().beginWriterRun(A, 1, "old-run", Map.of("orders", List.of("writer")), f.fenceA())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.consumers().advanceWriter(A, 1, "old-run", "orders", "writer", point(1, 4, "late"), f.fenceA())).isInstanceOf(TapstateException.class);
            var a = claims.acquire(f.pipelineA().key(), f.pipelineA().owner(), 1, Duration.ofSeconds(30)).claim();
            var b = claims.acquire(f.pipelineB().key(), f.pipelineB().owner(), 1, Duration.ofSeconds(30)).claim();
            assertThat(a.claimGeneration()).isEqualTo(f.pipelineA().claimGeneration()+1);
            edit.rewrite(List.of(positionMove(f, "second", f.captureFence())),
                    Map.of(positionScope(A), WorkloadClaimFence.from(a), positionScope(B), WorkloadClaimFence.from(b)), java.time.Instant.now());
            assertThat(f.chains().load(CHAIN).orElseThrow().resumeOverride().token()).isEqualTo("second");
            f.chains().openEpoch(CHAIN, 1, f.captureFence()).orElseThrow();
            assertThat(f.chains().load(CHAIN).orElseThrow().resumeOverride().token()).isEqualTo("second");
            assertThat(f.chains().load(CHAIN).orElseThrow().checkpoint()).isEqualTo(checkpoint);
            try (var reopened = MongoClients.create(MONGO.getReplicaSetUrl())) {
                assertThat(new MongoSrsChainStore(reopened, reopened.getDatabase(f.database().getName()), SCOPE).load(CHAIN).orElseThrow().resumeToken()).isEqualTo("second");
            }
            f.chains().publishServingTables(CHAIN, 2, Set.of("orders"), f.captureFence());
            var nextKey = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 2, 0);
            models(f).publishInitial(nextKey, ring("orders"), new SourceTable("orders", List.of(new SourceField("id", "int")), List.of(), List.of()), f.captureFence());
            var actual = new io.tapstate.spi.store.SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(2), "second"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(nextKey, -1)));
            var confirmed = f.chains().checkpoint(CHAIN, 1, actual, f.captureFence()).orElseThrow();
            assertThat(confirmed.resumeOverride()).isNull(); assertThat(confirmed.checkpoint()).isEqualTo(actual);
        });
    }
    private static io.tapstate.spi.store.PipelineIntentScope positionScope(SrsConsumerContext context) {
        return new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(), SCOPE.bindingVersion(), SCOPE.cacheIncarnation(), context.key().pipelineId(), context.stateGeneration());
    }
    private static io.tapstate.spi.store.SourcePositionEdits.Logged positionMove(Fixture f, String token, WorkloadClaimFence capture) {
        return new io.tapstate.spi.store.SourcePositionEdits.Logged(f.chains().load(CHAIN).orElseThrow(),
                Map.of(A, f.consumers().progress(A).orElseThrow(), B, f.consumers().progress(B).orElseThrow()), token, capture);
    }

    private static void withBusiness(Scenario scenario) {
        try (var client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(MongoCommandObservation.LISTENER).build())) {
            var database = client.getDatabase("srs_consumer_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                try (var session = client.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.SRS_CHAINS,
                            SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS,
                            SystemCollections.CACHE_WORKLOAD_CLAIMS,SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.SRS_CONSUMERS,SystemCollections.CACHE_SRS_CONSUMER_OFFSETS)) {
                        table.prepareCollectionOn(database, session); for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
                    }
                }
                SystemCollections.CACHE_IDENTITY.on(database).insertOne(new Document("_id", "cluster").append("clusterId", SCOPE.clusterId())
                        .append("layoutId", MongoConfigurationDomain.LAYOUT).append("bindingVersion", 1L).append("cacheIncarnation", SCOPE.cacheIncarnation())
                        .append("businessPhase", "READY").append("businessPreparationId", "business-a").append("authGeneration", 1L));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var capture = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-a"),
                        new WorkloadOwner("node-c", "boot-c"), 1, Duration.ofSeconds(30)).claim();
                WorkloadClaim a = pipeline(claims,"pipeline-a"); WorkloadClaim b = pipeline(claims,"pipeline-b");
                preparePipelineScope(database,"pipeline-a",1); preparePipelineScope(database,"pipeline-b",1);
                CaptureHistoryFixture.shared(database,SCOPE,WorkloadClaimFence.from(capture),CHAIN,"retention");
                var chains = new MongoSrsChainStore(client, database, SCOPE); chains.prepare(CHAIN, "retention", WorkloadClaimFence.from(capture));
                scenario.run(new Fixture(client, database, chains, new MongoSrsConsumerStore(client, database, SCOPE), capture, a, b));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    /** Explicit checkpoint fixtures support these transaction cases; native preparation is tested separately. */
    private static void preparePipelineScope(MongoDatabase database,String pipeline,long generation) {
        var physical = new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),pipeline,generation);
        var header = MongoPipelinePreparationFacts.scope(physical);
        SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id",pipeline).append("scope",header)
                .append("stateJson","NEW").append("epoch",0L).append("touchMillis",0L).append("runtimeWriteFence",1L));
        var key = new WorkloadClaimKey(SCOPE.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,pipeline);
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id",MongoWorkloadClaimStore.id(key)),
                new Document("$set",new Document("pipelineScope",header).append("pipelinePreparation",
                        MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(),io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
    }
    private static void replacePipelineScope(Fixture f,SrsConsumerContext context) {
        var physical = new io.tapstate.spi.store.PipelineIntentScope(SCOPE.clusterId(),SCOPE.bindingVersion(),SCOPE.cacheIncarnation(),context.key().pipelineId(),context.stateGeneration());
        var header = MongoPipelinePreparationFacts.scope(physical);
        SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id",context.key().pipelineId()),new Document("$set",new Document("scope",header)));
        var key = new WorkloadClaimKey(SCOPE.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,context.key().pipelineId());
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document("_id",MongoWorkloadClaimStore.id(key)),new Document("$set",new Document("pipelineScope",header)));
    }
    private static WorkloadClaim pipeline(MongoWorkloadClaimStore claims, String pipeline) {
        var acquired = claims.acquire(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline),
                new WorkloadOwner("node-" + pipeline, "boot-" + pipeline), 1, Duration.ofSeconds(30)).claim();
        return claims.advanceExecution(acquired, 1, Set.of(acquired.owner().nodeId())).orElseThrow();
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoSrsChainStore chains, MongoSrsConsumerStore consumers,
                           WorkloadClaim capture, WorkloadClaim pipelineA, WorkloadClaim pipelineB) {
        WorkloadClaimFence captureFence() { return WorkloadClaimFence.from(capture); }
        WorkloadClaimFence fenceA() { return WorkloadClaimFence.from(pipelineA); }
        WorkloadClaimFence fenceB() { return WorkloadClaimFence.from(pipelineB); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
