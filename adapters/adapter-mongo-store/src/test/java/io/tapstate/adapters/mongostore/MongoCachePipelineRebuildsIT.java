package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/** Real independent CONFIG and CACHE clients retain original history through one controlled replacement. */
@RequiresDocker
class MongoCachePipelineRebuildsIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofMinutes(2);
    private static final Instant AT = Instant.parse("2026-10-19T03:00:00Z");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static MongoDBContainer mongo() { return new MongoDBContainer(DockerImageName.parse("mongo:7.0")); }

    @Test void oneFreshFullTransactionPublishesBothModelsAndTheExactPinFloorsAndPendingRequest() {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f);
            var floors = new java.util.TreeMap<String, Long>();
            for (String table : java.util.Set.of("orders", "quiet")) floors.put(table,
                    SystemCollections.SRS_LOG_BOUNDS.on(f.cache).find(new Document("_id", MongoSrsLogStore.boundsId(shared.ring(table))))
                            .first().getLong("largestSequence"));
            var inspection = shared.consumers.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture);
            assertThat(inspection.originalChain().servingTables()).isEmpty();
            var actual = Map.of("orders", table("orders", true), "quiet", table("quiet", false));
            var receipt = inspection.publish(2, "fixed-new-cut", actual, AT);
            assertThat(receipt.chain().checkpoint()).isEqualTo(shared.checkpoint);
            assertThat(receipt.chain().checkpointRevision()).isOne();
            assertThat(receipt.chain().servingTables()).containsExactlyInAnyOrder("orders", "quiet");
            assertThat(receipt.chain().pendingFreshFull().origin()).isEqualTo(shared.full.origin("input"));
            assertThat(receipt.chain().resumeOverride().token()).isEqualTo("fixed-new-cut");
            assertThat(receipt.progress().snapshot().sourcePosition()).isEqualTo("fixed-new-cut");
            assertThat(receipt.progress().snapshot().models()).hasSize(2);
            assertThat(receipt.progress().ringDoneThrough()).isEqualTo(floors);
            assertThat(receipt.progress().cursor().perTableSeq()).isEqualTo(floors);
            assertThat(receipt.progress().cursor().sinkAcked()).isNull();
            assertThat(receipt.progress().cursor().snapshotCompletedTables()).isEmpty();
            assertThat(receipt.progress().writerRun()).isNull();
            for (var entry : receipt.progress().snapshot().models().entrySet()) {
                var model = shared.models.load(entry.getValue()).orElseThrow();
                assertThat(model.table()).isEqualTo(actual.get(entry.getKey()));
                assertThat(model.ddlSequence()).isEqualTo(floors.get(entry.getKey()) + 1);
                var prior = new SrsModelKey(shared.chain, entry.getKey(), f.business.cacheIncarnation(), 1, 0);
                assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.cache).find(new Document("_id", MongoSrsModelContexts.contextId(prior))).first()
                        .get("nextContext", Document.class)).isEqualTo(MongoSrsModelContexts.contextId(entry.getValue()));
            }
            assertThat(SystemCollections.SRS_CHAINS.on(f.cache).find(new Document("_id", shared.chain)).first()
                    .getList("modelContextHeads", Document.class)).hasSize(2);
        });
    }

    @Test void anUnissuedFullObjectIsRejectedBeforeAnyOfItsAccessorsOrAnyCacheWrite() {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f); var calls = new AtomicInteger(); var before = f.snapshot();
            CompletedPipelineFull fake = new CompletedPipelineFull() {
                public String operationId() { calls.incrementAndGet(); return shared.full.operationId(); }
                public PipelineIntentScope targetScope() { calls.incrementAndGet(); return shared.full.targetScope(); }
                public String preparationId() { calls.incrementAndGet(); return shared.full.preparationId(); }
                public String confirmationHash() { calls.incrementAndGet(); return shared.full.confirmationHash(); }
                public Map<String, String> sourceHashes() { calls.incrementAndGet(); return shared.full.sourceHashes(); }
                public void requireCurrent() { calls.incrementAndGet(); }
            };
            assertCode(() -> shared.consumers.inspectFreshFull(fake, shared.run, 0, f.node, shared.capture), IoError.PIPELINE_INTENT_FENCED);
            assertThat(calls).hasValue(0); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void matchingDirectChannelsCannotSubstituteForTheOriginalCompleteCacheInputs() {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f); var inputs = f.sharedInputs(shared.run.scope());
            var changed = new PipelinePreparationInputs(inputs.pipelineHash(), Map.of("input", "f".repeat(64)),
                    inputs.selectedTables(), inputs.directChannels());
            assertThat(changed.sameRecovery(inputs)).isTrue();
            f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(shared.run.originalClaim().key())),
                    new Document("$set", new Document("pipelineSources", MongoPipelineSourcePreparations.encode(changed))));
            var before = f.snapshot();
            assertCode(() -> shared.consumers.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void anActuallyCommittedPublicationWithALostReplyReturnsOnlyItsOriginalReceiptOnRetry() {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f); var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var lost = new AtomicInteger();
            MongoClient uncertain = (MongoClient) java.lang.reflect.Proxy.newProxyInstance(MongoClient.class.getClassLoader(),
                    new Class<?>[]{MongoClient.class}, (proxy, method, args) -> {
                        var result = invokeActual(method, f.cacheClient, args);
                        if (!method.getName().equals("startSession")) return result;
                        var session = (com.mongodb.client.ClientSession) result;
                        return java.lang.reflect.Proxy.newProxyInstance(com.mongodb.client.ClientSession.class.getClassLoader(),
                                new Class<?>[]{com.mongodb.client.ClientSession.class}, (wrapped, operation, parameters) -> {
                                    var value = invokeActual(operation, session, parameters);
                                    if (operation.getName().equals("withTransaction") && armed.compareAndSet(true, false)) {
                                        lost.incrementAndGet();
                                        // The real server commit is complete before the adapter loses its result.
                                        throw new com.mongodb.MongoException(91, "publication commit response unavailable");
                                    }
                                    return value;
                                });
                    });
            var store = new MongoSrsConsumerStore(uncertain, f.cache, f.business);
            var frozen = store.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture);
            var actual = Map.of("orders", table("orders", true), "quiet", table("quiet", false));
            armed.set(true);
            assertCode(() -> frozen.publish(2, "fixed-new-cut", actual, AT), IoError.STORE_UNAVAILABLE);
            assertThat(lost).hasValue(1);
            var pending = shared.chains.load(shared.chain).orElseThrow();
            var progress = shared.consumers.progress(shared.run.context()).orElseThrow();
            assertThat(pending.pendingFreshFull()).isNotNull(); assertThat(progress.snapshot()).isNotNull();
            var before = f.snapshot();
            var recovered = frozen.publish(2, "fixed-new-cut", actual, AT.plusSeconds(2));
            assertThat(recovered).isEqualTo(new SrsFreshFullReceipt(pending, progress));
            var expected = new java.util.TreeMap<>(before);
            expected.put(SystemCollections.CACHE_WORKLOAD_CLAIMS.name(), before.get(SystemCollections.CACHE_WORKLOAD_CLAIMS.name()).stream().map(row -> {
                var next = new Document(row);
                String counter = switch (row.getString("resourceType")) {
                    case "NODE_SESSION" -> "fencedPreparationWrites";
                    case "PIPELINE_ACTUATION" -> "fencedConsumerWrites";
                    case "CAPTURE" -> "fencedChainWrites";
                    default -> throw new AssertionError(row.getString("resourceType"));
                };
                next.put(counter, next.getLong(counter) + 1); return next;
            }).toList());
            expected.put(SystemCollections.CACHE_PIPELINE_STATE.name(), before.get(SystemCollections.CACHE_PIPELINE_STATE.name()).stream().map(row -> {
                var next = new Document(row); next.put("runtimeWriteFence", next.getLong("runtimeWriteFence") + 1); return next;
            }).toList());
            expected.put(SystemCollections.CACHE_IDENTITY.name(), before.get(SystemCollections.CACHE_IDENTITY.name()).stream().map(row -> {
                var next = new Document(row); next.put("claimAdmissionFence", next.getLong("claimAdmissionFence") + 1); return next;
            }).toList());
            // Revalidation writes only these five existing fence counters; every business fact stays exact.
            assertThat(f.snapshot()).isEqualTo(expected);
            assertCode(() -> frozen.publish(2, "another-cut", actual, AT), IoError.SRS_FULL_PREPARATION_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(expected);
        });
    }

    private static Object invokeActual(java.lang.reflect.Method method, Object target, Object[] arguments) throws Throwable {
        try { return method.invoke(target, arguments); }
        catch (java.lang.reflect.InvocationTargetException failed) { throw failed.getCause(); }
    }

    @ParameterizedTest @ValueSource(strings = {"model", "context", "bounds", "partial-new-epoch", "capture-identity"})
    void anIncompleteOrConflictingChainCannotBeReseededByTheFull(String damage) {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f);
            var old = new SrsModelKey(shared.chain, "quiet", f.business.cacheIncarnation(), 1, 0);
            switch (damage) {
                case "model" -> SystemCollections.SRS_MODELS.on(f.cache).deleteOne(new Document("_id", MongoSrsModelStore.key(old)));
                case "context" -> SystemCollections.SRS_MODEL_CONTEXTS.on(f.cache).deleteOne(new Document("_id", MongoSrsModelContexts.contextId(old)));
                case "bounds" -> SystemCollections.SRS_LOG_BOUNDS.on(f.cache).deleteOne(new Document("_id", MongoSrsLogStore.boundsId(shared.ring("quiet"))));
                case "partial-new-epoch" -> shared.models.publish(new SrsModel(new SrsModelKey(shared.chain, "orders", f.business.cacheIncarnation(), 2, 0),
                        shared.ring("orders"), table("orders", false), 1), shared.capture);
                case "capture-identity" -> f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(shared.capture.key())),
                        new Document("$set", new Document("capturePreparation.sourceIdentityHash", "mc-" + "f".repeat(64))));
                default -> throw new AssertionError(damage);
            }
            var before = f.snapshot();
            assertThatThrownBy(() -> shared.consumers.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aNewStoppedUnleasedConsumerInvalidatesTheFrozenPublicationAndStillCountsForInspection() {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f);
            var frozen = shared.consumers.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture);
            f.initialSharedSource("other"); var fence = f.advance("other");
            var other = new PipelineIntentScope(f.business.clusterId(), f.business.bindingVersion(), f.business.cacheIncarnation(), "other", 1);
            assertThat(f.configuration.capturePreparations().issueFirstCreate(new CapturePreparationRequest(other, fence, f.node, "input", f.sharedInputs(other))))
                    .isInstanceOf(CaptureCreationResult.AlreadyIssued.class);
            shared.consumers.attach(new SrsConsumerContext(new SrsConsumerKey(shared.chain, "other", "input"), 1),
                    0, java.util.Set.of("orders", "quiet"), fence).orElseThrow();
            var intent = f.configuration.intents().load("other").orElseThrow();
            assertThat(f.configuration.intents().compareAndSet(other, intent.intentRevision(),
                    new DesiredState("other", PipelineState.STOPPED, intent.desired().revision(), false))).isTrue();
            var claims = f.domain.workloadClaims(f.business);
            assertThat(claims.release(claims.read(fence.key()).orElseThrow().claim())).isTrue();
            var before = f.snapshot();
            assertCode(() -> frozen.publish(2, "fixed-new-cut", Map.of("orders", table("orders", true), "quiet", table("quiet", false)), AT),
                    IoError.SRS_FULL_PREPARATION_UNPROVEN);
            assertCode(() -> shared.consumers.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture), IoError.SRS_FULL_PREPARATION_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(shared.consumers.attached(shared.chain).consumers()).hasSize(2);
        });
    }

    @Test void aSecondTableBudgetFailureRollsBackTheFirstActualModelInsertAndBothHeads() {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f); var inserted = new AtomicInteger();
            var requests = java.util.concurrent.ConcurrentHashMap.<Integer>newKeySet();
            var witness = new com.mongodb.event.CommandListener() {
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if (event.getDatabaseName().equals(f.cache.getName()) && event.getCommandName().equals("insert")
                            && event.getCommand().getString("insert").getValue().equals(SystemCollections.SRS_MODELS.collectionName())) {
                        var row = event.getCommand().getArray("documents").get(0).asDocument();
                        if (row.getDocument("_id").getDocument("captureContext").getNumber("epoch").longValue() == 2) requests.add(event.getRequestId());
                    }
                }
                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if (requests.remove(event.getRequestId())) inserted.incrementAndGet();
                }
            };
            try (var observed = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(CACHE.getReplicaSetUrl())).addCommandListener(witness).build())) {
                var store = new MongoSrsConsumerStore(observed, observed.getDatabase(f.cache.getName()), f.business);
                var frozen = store.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture);
                var huge = new SourceTable("quiet", List.of(new SourceField("id", "integer"),
                        new SourceField("field_" + "x".repeat(1024 * 1024), "string")), List.of("id"), List.of());
                var before = f.snapshot();
                assertCode(() -> frozen.publish(2, "fixed-new-cut", Map.of("orders", table("orders", true), "quiet", huge), AT), IoError.SRS_MODEL_CAPACITY);
                assertThat(inserted).hasValue(1); assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }

    @Test void pendingPublicationRetriesKeepTheirValuesAndOnlyARealCompleteBatchConsumesTheRequest() {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f);
            var frozen = shared.consumers.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture);
            var actual = Map.of("orders", table("orders", true), "quiet", table("quiet", false));
            var published = frozen.publish(2, "fixed-new-cut", actual, AT);
            assertThat(frozen.publish(2, "fixed-new-cut", actual, AT.plusSeconds(1))).isEqualTo(published);
            var before = f.snapshot();
            assertCode(() -> frozen.publish(2, "different-cut", actual, AT), IoError.SRS_FULL_PREPARATION_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
            var initial = new java.util.TreeMap<String, SrsTableCheckpoint>();
            published.progress().snapshot().models().forEach((name, key) -> initial.put(name,
                    new SrsTableCheckpoint(key, published.progress().ringDoneThrough().get(name))));
            var onStart = new SrsCaptureCheckpoint(new io.tapstate.core.event.ChainPosition(
                    new io.tapstate.core.event.SourceOrder(2, io.tapstate.core.event.SourceOrder.SNAPSHOT_SEQ), "fixed-new-cut"), initial, true);
            assertCode(() -> shared.chains.checkpoint(shared.chain, 1, onStart, shared.capture), IoError.SRS_CHECKPOINT_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
            var log = new MongoSrsLogStore(f.cacheClient, SystemCollections.SRS_EVENTS.on(f.cache), SystemCollections.SRS_LOG_BOUNDS.on(f.cache),
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache), shared.models);
            log.store(shared.ring("orders"), 1, new SrsLogRecord("actual-batch", io.tapstate.core.event.Op.UPDATE, 2, null,
                    Map.of("id", 1, "note", "live"), 0, shared.capture, 2));
            var complete = new java.util.TreeMap<>(initial);
            complete.put("orders", new SrsTableCheckpoint(published.progress().snapshot().models().get("orders"), 1));
            var batch = new SrsCaptureCheckpoint(new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(2, 1), "actual-batch"), complete, true);
            var saved = shared.chains.checkpoint(shared.chain, 1, batch, shared.capture).orElseThrow();
            assertThat(saved.pendingFreshFull()).isNull(); assertThat(saved.resumeOverride()).isNull();
            assertThat(saved.checkpoint()).isEqualTo(batch); assertThat(saved.checkpoint().positionEditsRestricted()).isTrue();
            assertCode(() -> frozen.publish(2, "fixed-new-cut", actual, AT), IoError.SRS_FULL_PREPARATION_UNPROVEN);
            shared.chains.openEpoch(shared.chain, 2, shared.capture).orElseThrow();
            assertCode(() -> shared.consumers.inspectFreshFull(shared.full, shared.run,
                    published.progress().revision(), f.node, shared.capture), IoError.SRS_FULL_PREPARATION_UNPROVEN);
        });
    }

    @ParameterizedTest @ValueSource(strings = {"ordinary-pin", "pending-full", "consumed-full"})
    void aPinnedFullCannotDiscardItsOriginalBindingThroughOrdinaryDetach(String phase) {
        withFixture(true, f -> {
            var shared = preparedSharedFull(f);
            if (phase.equals("ordinary-pin")) {
                var refs = new java.util.TreeMap<String, SrsModelKey>();
                for (String name : List.of("orders", "quiet")) {
                    var key = new SrsModelKey(shared.chain, name, f.business.cacheIncarnation(), 2, 0);
                    shared.models.publish(new SrsModel(key, shared.ring(name), table(name, false), 1), shared.capture);
                    refs.put(name, key);
                }
                assertThat(shared.chains.publishServingTables(shared.chain, 2, refs.keySet(), shared.capture)).isTrue();
                assertThat(shared.consumers.pinSnapshot(shared.run.context(), shared.run.membershipRevision(), 0,
                        new SrsSnapshotPin(2, "ordinary-cut", refs), shared.run.originalClaim())).isTrue();
                assertThat(shared.chains.load(shared.chain).orElseThrow().checkpoint().positionEditsRestricted()).isFalse();
            } else {
                var frozen = shared.consumers.inspectFreshFull(shared.full, shared.run, 0, f.node, shared.capture);
                var published = frozen.publish(2, "fixed-new-cut", Map.of("orders", table("orders", true), "quiet", table("quiet", false)), AT);
                if (phase.equals("consumed-full")) {
                    var log = new MongoSrsLogStore(f.cacheClient, SystemCollections.SRS_EVENTS.on(f.cache), SystemCollections.SRS_LOG_BOUNDS.on(f.cache),
                            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache), shared.models);
                    log.store(shared.ring("orders"), 1, new SrsLogRecord("actual-batch", io.tapstate.core.event.Op.UPDATE, 2, null,
                            Map.of("id", 1, "note", "live"), 0, shared.capture, 2));
                    var frontiers = new java.util.TreeMap<String, SrsTableCheckpoint>();
                    published.progress().snapshot().models().forEach((name, key) -> frontiers.put(name,
                            new SrsTableCheckpoint(key, name.equals("orders") ? 1 : published.progress().ringDoneThrough().get(name))));
                    var checkpoint = new SrsCaptureCheckpoint(new io.tapstate.core.event.ChainPosition(
                            new io.tapstate.core.event.SourceOrder(2, 1), "actual-batch"), frontiers, true);
                    var current = shared.chains.checkpoint(shared.chain, 1, checkpoint, shared.capture).orElseThrow();
                    assertThat(current.pendingFreshFull()).isNull();
                    assertThat(current.checkpoint().positionEditsRestricted()).isTrue();
                }
            }
            var before = f.snapshot();
            assertCode(() -> shared.consumers.detach(shared.run.context(), shared.run.membershipRevision(), shared.run.originalClaim()),
                    IoError.SRS_FULL_PREPARATION_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    private static SourceTable table(String name, boolean changed) {
        var fields = new ArrayList<SourceField>(); fields.add(new SourceField("id", "integer"));
        if (changed) fields.add(new SourceField("note", "string"));
        return new SourceTable(name, fields, List.of("id"), List.of());
    }

    @Test void theActualPendingBsonBudgetRefusesBeforeAnyModelOrCursorPublication() {
        withFixture(true,f -> {
            var shared=preparedSharedFull(f);var frozen=shared.consumers.inspectFreshFull(shared.full,shared.run,0,f.node,shared.capture);
            var before=f.snapshot();var mapping=f.mapping();long version=f.version();
            assertCode(() -> frozen.publish(2,"x".repeat(SrsFreshFull.MAX_ENCODED_REQUEST_BYTES),
                    Map.of("orders",table("orders",true),"quiet",table("quiet",false)),AT),IoError.SRS_FULL_PREPARATION_CAPACITY);
            assertThat(f.snapshot()).isEqualTo(before);assertThat(f.mapping()).isEqualTo(mapping);assertThat(f.version()).isEqualTo(version);
        });
    }

    @Test void aPendingFullKeepsItsLogFloorWhenTheCurrentConsumerConfirmsNewerTokenlessRows() {
        withFixture(true,f -> {
            var shared=preparedSharedFull(f);var frozen=shared.consumers.inspectFreshFull(shared.full,shared.run,0,f.node,shared.capture);
            var published=frozen.publish(2,"fixed-new-cut",Map.of("orders",table("orders",true),"quiet",table("quiet",false)),AT);
            var log=new MongoSrsLogStore(f.cacheClient,SystemCollections.SRS_EVENTS.on(f.cache),SystemCollections.SRS_LOG_BOUNDS.on(f.cache),
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache),shared.models);
            for(long sequence=1;sequence<=2;sequence++)log.store(shared.ring("orders"),sequence,
                    new SrsLogRecord(null,io.tapstate.core.event.Op.INSERT,sequence,null,Map.of("id",sequence,"note","live"),0,shared.capture,2));
            assertThat(shared.consumers.advanceReadSequence(shared.run.context(),shared.run.membershipRevision(),"orders",2,shared.run.originalClaim())).isTrue();
            assertThat(shared.consumers.beginWriterRun(shared.run.context(),shared.run.membershipRevision(),"current-writers",
                    Map.of("orders",List.of("sink"),"quiet",List.of("sink")),shared.run.originalClaim())).isTrue();
            assertThat(shared.consumers.advanceWriter(shared.run.context(),shared.run.membershipRevision(),"current-writers","orders","sink",
                    new WriterProgress(new io.tapstate.core.event.SourceOrder(2,2),null),shared.run.originalClaim())).isTrue();
            assertThat(shared.consumers.progress(shared.run.context()).orElseThrow().ringDoneThrough()).containsEntry("orders",2L);
            assertThat(shared.chains.load(shared.chain).orElseThrow().checkpoint()).isEqualTo(shared.checkpoint);
            log.trim(shared.ring("orders"),2,shared.capture);
            assertThat(log.bounds(shared.ring("orders")).trimmedThrough()).isEqualTo(0);
            assertThat(log.load(shared.ring("orders"),1)).isPresent();assertThat(log.load(shared.ring("orders"),2)).isPresent();
            assertThat(shared.chains.load(shared.chain).orElseThrow().pendingFreshFull()).isEqualTo(published.chain().pendingFreshFull());
            var tables=new java.util.TreeMap<String,SrsTableCheckpoint>();
            published.progress().snapshot().models().forEach((name,key) -> tables.put(name,
                    new SrsTableCheckpoint(key,name.equals("orders")?2:published.progress().ringDoneThrough().get(name))));
            var complete=new SrsCaptureCheckpoint(new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(2,1),"actual-complete-batch"),tables,true);
            assertThat(shared.chains.checkpoint(shared.chain,1,complete,shared.capture).orElseThrow().pendingFreshFull()).isNull();
            log.trim(shared.ring("orders"),2,shared.capture);
            assertThat(log.bounds(shared.ring("orders")).trimmedThrough()).isEqualTo(2);
            assertThat(log.load(shared.ring("orders"),1)).isEmpty();assertThat(log.load(shared.ring("orders"),2)).isEmpty();
        });
    }

    @Test void anExplicitSuccessorFullCanRetireTheProtectedPinWithoutReauthorizingTheOldFull() {
        withFixture(true,f -> {
            var shared=preparedSharedFull(f);var frozen=shared.consumers.inspectFreshFull(shared.full,shared.run,0,f.node,shared.capture);
            var published=frozen.publish(2,"fixed-new-cut",Map.of("orders",table("orders",true),"quiet",table("quiet",false)),AT);
            var current=f.configuration.intents().load("flow").orElseThrow();
            assertThat(f.configuration.intents().compareAndSet(shared.run.scope(),current.intentRevision(),
                    new DesiredState("flow",PipelineState.STOPPED,current.desired().revision(),true))).isTrue();
            var next=new PipelineIntentScope(f.business.clusterId(),f.business.bindingVersion(),f.business.cacheIncarnation(),"flow",3);
            var request=new PipelineRebuildRequest("next-explicit-full",f.configuration.intents().load("flow").orElseThrow(),
                    shared.run.originalClaim(),f.state().getLong("epoch"),f.sharedInputs(next));
            var original=f.configuration.rebuilds().begin(request);
            assertCode(shared.full::requireCurrent,IoError.PIPELINE_INTENT_FENCED);
            var member=shared.consumers.find(shared.run.context().key()).orElseThrow();
            var detached=shared.consumers.detachForRebuild(original,member,f.node).orElseThrow();
            assertThat(detached.detached()).isTrue();assertThat(shared.consumers.progress(shared.run.context())).isEmpty();
            var prepared=f.configuration.rebuilds().complete(f.rebuilds().prepare(original,f.node,TTL,AT));
            assertThat(prepared.scope()).isEqualTo(next);
            assertThat(f.configuration.rebuilds().completedFull(next,request.inputs(),Map.of())).isPresent();
            assertThat(shared.chains.load(shared.chain).orElseThrow().pendingFreshFull()).isEqualTo(published.chain().pendingFreshFull());
        });
    }

    @Test void deletingTheOriginBeforeARealBatchKeepsPendingModelsThroughGcAndAnotherConsumersOwnPreparation() {
        withFixture(true,f -> {
            var shared=preparedSharedFull(f);var frozen=shared.consumers.inspectFreshFull(shared.full,shared.run,0,f.node,shared.capture);
            var actual=Map.of("orders",table("orders",true),"quiet",table("quiet",false));
            var published=frozen.publish(2,"fixed-new-cut",actual,AT);var pin=published.progress().snapshot();
            var originalCache=new PipelineRetirementRequest.CacheOriginal(shared.run.originalClaim(),f.state().getLong("epoch"));
            var definition=f.configuration.artifacts().get("flow").orElseThrow();
            var retirement=f.configuration.retirements().begin(new PipelineRetirementRequest("delete-pending-origin",shared.run.scope(),
                    CanonicalHash.of(definition),java.util.Optional.of(originalCache)));
            shared.consumers.detachForRetirement(retirement,shared.consumers.find(shared.run.context().key()).orElseThrow(),
                    f.node,java.util.Optional.of(originalCache)).orElseThrow();
            for(var notes:f.configuration.requiredNotes().retirements(shared.run.scope(),retirement.request().operationId())) {
                assertThat(notes.origin()).isEmpty();notes.completeAfterVerifiedRelease();
            }
            f.configuration.retirements().complete(f.domain.pipelineRetirements(f.business,ACTIVE).retire(retirement,f.node));
            assertThat(f.configuration.artifacts().get("flow")).isEmpty();assertThat(shared.consumers.progress(shared.run.context())).isEmpty();
            assertCode(shared.full::requireCurrent,IoError.PIPELINE_INTENT_FENCED);
            assertThat(shared.chains.openEpoch(shared.chain,2,shared.capture)).hasValue(3);
            for(String name:List.of("orders","quiet"))shared.models.publishInitial(
                    new SrsModelKey(shared.chain,name,f.business.cacheIncarnation(),3,0),shared.ring(name),actual.get(name),shared.capture);
            var log=new MongoSrsLogStore(f.cacheClient,SystemCollections.SRS_EVENTS.on(f.cache),SystemCollections.SRS_LOG_BOUNDS.on(f.cache),
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache),shared.models);
            for(String name:List.of("orders","quiet"))log.trim(shared.ring(name),0,shared.capture);
            assertThat(shared.models.reclaim(shared.chain,shared.capture)).isZero();
            pin.models().values().forEach(key -> assertThat(shared.models.load(key)).isPresent());
            var pending=shared.chains.load(shared.chain).orElseThrow();
            assertThat(pending.checkpoint()).isEqualTo(shared.checkpoint);assertThat(pending.pendingFreshFull()).isEqualTo(published.chain().pendingFreshFull());
            assertThat(pending.consumerCount()).isZero();
            f.initialSharedSource("other");var otherClaim=f.advance("other");
            var other=new PipelineIntentScope(f.business.clusterId(),f.business.bindingVersion(),f.business.cacheIncarnation(),"other",1);
            assertThat(f.configuration.capturePreparations().issueFirstCreate(new CapturePreparationRequest(other,otherClaim,f.node,"input",f.sharedInputs(other))))
                    .isInstanceOf(CaptureCreationResult.AlreadyIssued.class);
            var context=new SrsConsumerContext(new SrsConsumerKey(shared.chain,"other","input"),1);
            var member=shared.consumers.attach(context,0,pin.models().keySet(),otherClaim).orElseThrow();
            assertThat(shared.consumers.startReadingAfter(context,member.revision(),0,published.progress().ringDoneThrough(),otherClaim)).isTrue();
            var before=f.snapshot();
            assertCode(() -> shared.consumers.pinSnapshot(context,member.revision(),1,pin,shared.run.originalClaim()),IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(shared.consumers.pinSnapshot(context,member.revision(),1,pin,otherClaim)).isTrue();
            assertThat(shared.consumers.progress(context).orElseThrow().snapshot()).isEqualTo(pin);
            assertThat(shared.chains.load(shared.chain).orElseThrow().pendingFreshFull()).isEqualTo(pending.pendingFreshFull());
        });
    }

    /** Real CONFIG/CACHE grants and fences; source tokens are synthetic, without a connector invocation. */
    private static SharedFull preparedSharedFull(Fixture f) {
        var inputs = f.sharedInputs(f.scope);
        var originalClaim = f.advance("flow");
        var request = new CapturePreparationRequest(f.scope, originalClaim, f.node, "input", inputs);
        var creation = ((CaptureCreationResult.Issued) f.configuration.capturePreparations().issueFirstCreate(request)).creation();
        f.domain.capturePreparations(f.business, ACTIVE).createFirst(creation);
        String chain = creation.preparation().chainId();
        var claims = f.domain.workloadClaims(f.business);
        var capture = WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey(f.scope.clusterId(), WorkloadClaimType.CAPTURE,
                creation.preparation().captureId()), OWNER, 1, TTL).claim());
        var chains = new MongoSrsChainStore(f.cacheClient, f.cache, f.business);
        var models = new MongoSrsModelStore(f.cacheClient, f.cache, f.business);
        var consumers = new MongoSrsConsumerStore(f.cacheClient, f.cache, f.business);
        var log = new MongoSrsLogStore(f.cacheClient, SystemCollections.SRS_EVENTS.on(f.cache), SystemCollections.SRS_LOG_BOUNDS.on(f.cache),
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache), models);
        chains.requestTables(chain, java.util.Set.of("orders", "quiet"), capture);
        assertThat(chains.openEpoch(chain, 0, capture)).hasValue(1);
        var oldModels = new java.util.TreeMap<String, SrsModelKey>();
        var frontiers = new java.util.TreeMap<String, SrsTableCheckpoint>();
        for (String name : java.util.Set.of("orders", "quiet")) {
            var key = new SrsModelKey(chain, name, f.business.cacheIncarnation(), 1, 0); String ring = "srs." + chain + "." + name;
            models.publish(new SrsModel(key, ring, table(name, false), 0), capture);
            log.store(ring, 0, new SrsLogRecord("old-cut", io.tapstate.core.event.Op.INSERT, 1, null, Map.of("id", 1), 0, capture, 1));
            oldModels.put(name, key); frontiers.put(name, new SrsTableCheckpoint(key, 0));
        }
        assertThat(chains.publishServingTables(chain, 1, oldModels.keySet(), capture)).isTrue();
        var checkpoint = new SrsCaptureCheckpoint(new io.tapstate.core.event.ChainPosition(new io.tapstate.core.event.SourceOrder(1, 1), "old-cut"), frontiers);
        chains.checkpoint(chain, 0, checkpoint, capture).orElseThrow();
        var oldContext = new SrsConsumerContext(new SrsConsumerKey(chain, "flow", "input"), 1);
        var member = consumers.attach(oldContext, 0, oldModels.keySet(), originalClaim).orElseThrow();
        consumers.pinSnapshot(oldContext, member.revision(), 0, new SrsSnapshotPin(1, "old-cut", oldModels), originalClaim);
        var intent = f.configuration.intents().load("flow").orElseThrow();
        assertThat(f.configuration.intents().compareAndSet(f.scope, intent.intentRevision(),
                new DesiredState("flow", PipelineState.STOPPED, inputs.pipelineHash(), true))).isTrue();
        var target = new PipelineIntentScope(f.scope.clusterId(), f.scope.bindingVersion(), f.scope.cacheIncarnation(), "flow", 2);
        var targetInputs = f.sharedInputs(target);
        var fullRequest = new PipelineRebuildRequest("shared-explicit-full", f.configuration.intents().load("flow").orElseThrow(),
                originalClaim, 0, targetInputs);
        var pending = f.configuration.rebuilds().begin(fullRequest);
        var detached = consumers.detachForRebuild(pending, member, f.node).orElseThrow();
        var receipt = f.rebuilds().prepare(pending, f.node, TTL, AT);
        var prepared = f.configuration.rebuilds().complete(receipt);
        var stopped = f.configuration.intents().load("flow").orElseThrow();
        assertThat(f.configuration.intents().compareAndSet(target, stopped.intentRevision(),
                new DesiredState("flow", PipelineState.RUNNING, targetInputs.pipelineHash(), false))).isTrue();
        var owned = claims.acquirePipeline(target, OWNER, 1, TTL).claim();
        var running = WorkloadClaimFence.from(claims.advanceExecution(owned, 1, java.util.Set.of(OWNER.nodeId())).orElseThrow());
        var context = new SrsConsumerContext(new SrsConsumerKey(chain, "flow", "input"), 2);
        var current = consumers.attach(context, detached.revision(), oldModels.keySet(), running).orElseThrow();
        assertThat(chains.openEpoch(chain, 1, capture)).hasValue(2);
        var full = f.configuration.rebuilds().completedFull(prepared.scope(), targetInputs, Map.of()).orElseThrow();
        return new SharedFull(chain, capture, full, SrsConsumerRun.of(target, current), checkpoint, chains, models, consumers);
    }

    private record SharedFull(String chain, WorkloadClaimFence capture, CompletedPipelineFull full, SrsConsumerRun run,
                              SrsCaptureCheckpoint checkpoint, MongoSrsChainStore chains, MongoSrsModelStore models,
                              MongoSrsConsumerStore consumers) {
        String ring(String table) { return "srs." + chain + "." + table; }
    }

    @Test void aCompletedExplicitFullCanBeReadWithItsOriginalInputsAfterAControllerRestart() {
        withFixture(f -> {
            var request = modelFullRequest(f);
            var pending = f.configuration.rebuilds().begin(request);
            String digest = CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(
                    new Document("targetInputs", MongoPipelineSourcePreparations.encode(request.inputs()))
                            .append("dependencyHashes", new java.util.TreeMap<>(request.dependencyHashes()))));
            var beginning = f.mapping().get("stateRebuild", Document.class);
            assertThat(beginning).containsEntry("formatVersion", 2).containsEntry("inputClosureHash", digest);
            var receipt = f.rebuilds().prepare(pending, f.node, TTL, AT);
            var prepared = f.configuration.rebuilds().complete(receipt);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(f.config.getName()), null, Duration.ofSeconds(5));
            try (var reopened = MongoConfigurationDomain.open(settings, f.scope.clusterId())) {
                var proof = reopened.rebuilds().completedFull(prepared.scope(), request.inputs(), request.dependencyHashes());
                assertThat(proof).isPresent();
                assertThat(proof.orElseThrow().targetScope()).isEqualTo(prepared.scope());
                assertThat(proof.orElseThrow().operationId()).isEqualTo(request.operationId());
                assertThat(proof.orElseThrow().preparationId()).isEqualTo(pending.preparationId());
                assertThat(proof.orElseThrow().confirmationHash()).isEqualTo(pending.confirmationHash());
                assertThat(proof.orElseThrow().sourceHashes()).isEqualTo(request.inputs().sourceHashes());
                proof.orElseThrow().requireCurrent();
                var intent = reopened.intents().load("flow").orElseThrow();
                assertThat(reopened.intents().compareAndSet(prepared.scope(), intent.intentRevision(),
                        new DesiredState("flow", PipelineState.RUNNING, request.inputs().pipelineHash(), false))).isTrue();
                assertCode(proof.orElseThrow()::requireCurrent, IoError.PIPELINE_INTENT_FENCED);
                reopened.rebuilds().completedFull(prepared.scope(), request.inputs(), request.dependencyHashes()).orElseThrow().requireCurrent();
                assertThat(f.mapping().get("stateRebuild", Document.class).get("inputClosureHash")).isEqualTo(digest);
            }
            long version = f.version();
            f.domain.close();
            assertThat(f.configuration.rebuilds().complete(receipt)).isEqualTo(prepared);
            assertThat(f.version()).isEqualTo(version);
        });
    }

    @ParameterizedTest @ValueSource(strings = {"input", "target", "reusable_view", "reusable_serve"})
    void theOriginalFullCannotAuthorizeAnUpdatedCurrentDependencyClosure(String changed) {
        withFixture(f -> {
            var request = modelFullRequest(f);
            var pending = f.configuration.rebuilds().begin(request);
            var prepared = f.configuration.rebuilds().complete(f.rebuilds().prepare(pending, f.node, TTL, AT));
            var held = f.configuration.rebuilds().completedFull(prepared.scope(), request.inputs(), request.dependencyHashes()).orElseThrow();
            if (changed.equals("input") || changed.equals("target")) {
                var source = (SourceResource) f.configuration.artifacts().get(changed).orElseThrow();
                f.configuration.artifacts().save(new SourceResource(source.id(), source.metadata(), source.connector(),
                        Map.of("uri", "mongodb://changed.invalid/changed"), source.mode(), source.tables(), source.srs(), source.experimental()));
            } else if (changed.equals("reusable_view")) {
                f.configuration.artifacts().save(new io.tapstate.core.model.ViewResource(changed, null, "alternate_id", null, null));
            } else {
                f.configuration.artifacts().save(new io.tapstate.core.model.ServeResource(changed, null,
                        List.of(new io.tapstate.core.model.SyncElement("changed_sink", "target", null, null, null)), null, null, null));
            }
            var currentSource = (SourceResource) f.configuration.artifacts().get("input").orElseThrow();
            var pipeline = (io.tapstate.core.model.PipelineResource) f.configuration.artifacts().get("flow").orElseThrow();
            var inputs = PipelinePreparationInputs.of(prepared.scope(), pipeline, Map.of(currentSource.id(), currentSource),
                    request.inputs().selectedTables());
            var dependencies = new java.util.HashMap<String, String>();
            for (String id : request.dependencyHashes().keySet()) dependencies.put(id, CanonicalHash.of(f.configuration.artifacts().get(id).orElseThrow()));
            var before = f.mapping(); var cache = f.snapshot(); long version = f.version();
            assertCode(held::requireCurrent, IoError.PIPELINE_INTENT_FENCED);
            assertCode(() -> f.configuration.rebuilds().completedFull(prepared.scope(), inputs, dependencies), IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.mapping()).isEqualTo(before); assertThat(f.snapshot()).isEqualTo(cache); assertThat(f.version()).isEqualTo(version);
        });
    }

    @Test void aLegacyPendingFullRetainsItsOriginalShapeAcrossReopenCompletionAndLostReply() {
        withFixture(f -> {
            var request = modelFullRequest(f);
            f.configuration.rebuilds().begin(request);
            var legacy = new Document(f.mapping().get("stateRebuild", Document.class));
            legacy.put("formatVersion", 1); legacy.remove("inputClosureHash");
            SystemCollections.OPERATOR_GENERATIONS.on(f.config).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("stateRebuild", legacy)));
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(f.config.getName()), null, Duration.ofSeconds(5));
            PipelinePreparation prepared;
            PipelineRebuildReceipt completedReceipt;
            try (var reopened = MongoConfigurationDomain.open(settings, f.scope.clusterId())) {
                var restored = reopened.rebuilds().pending("flow", request.inputs(), request.dependencyHashes()).orElseThrow();
                var receipt = f.rebuilds().prepare(restored, f.node, TTL, AT);
                completedReceipt = receipt;
                prepared = reopened.rebuilds().complete(receipt);
                assertThat(f.mapping().get("stateRebuild", Document.class)).containsEntry("formatVersion", 1).doesNotContainKey("inputClosureHash");
                assertThat(reopened.rebuilds().completedFull(prepared.scope(), request.inputs(), request.dependencyHashes())).isEmpty();
            }
            var before = f.mapping(); long version = f.version(); f.domain.close();
            assertThat(f.configuration.rebuilds().completed(request)).contains(prepared);
            assertThat(f.configuration.rebuilds().completed(request)).contains(prepared);
            assertThat(f.mapping()).isEqualTo(before); assertThat(f.version()).isEqualTo(version);
            var intent = f.configuration.intents().load("flow").orElseThrow();
            assertThat(f.configuration.intents().compareAndSet(prepared.scope(), intent.intentRevision(),
                    new DesiredState("flow", PipelineState.STOPPED, request.inputs().pipelineHash(), true))).isTrue();
            var next = new PipelineRebuildRequest("new-explicit-full", f.configuration.intents().load("flow").orElseThrow(),
                    completedReceipt.claim(), completedReceipt.checkpointEpoch(),
                    request.inputs(), request.dependencyHashes());
            f.configuration.rebuilds().begin(next);
            assertThat(f.mapping().get("stateRebuild", Document.class)).containsEntry("formatVersion", 2).containsKey("inputClosureHash");
        });
    }

    @ParameterizedTest @ValueSource(strings = {"missing", "extra", "type", "hash", "version", "legacy-with-hash"})
    void aMalformedVersionTwoFullCannotBeDowngradedOrRepaired(String damage) {
        withFixture(f -> {
            var request = modelFullRequest(f);
            var pending = f.configuration.rebuilds().begin(request);
            var prepared = f.configuration.rebuilds().complete(f.rebuilds().prepare(pending, f.node, TTL, AT));
            var altered = new Document(f.mapping().get("stateRebuild", Document.class));
            switch (damage) {
                case "missing" -> altered.remove("inputClosureHash");
                case "extra" -> altered.put("unknown", true);
                case "type" -> altered.put("inputClosureHash", 7L);
                case "hash" -> altered.put("inputClosureHash", "f".repeat(64));
                case "version" -> altered.put("formatVersion", 3);
                case "legacy-with-hash" -> altered.put("formatVersion", 1);
                default -> throw new AssertionError(damage);
            }
            SystemCollections.OPERATOR_GENERATIONS.on(f.config).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("stateRebuild", altered)));
            var before = f.mapping(); var cache = f.snapshot(); long version = f.version();
            var expected = damage.equals("hash") ? IoError.PIPELINE_INTENT_FENCED : IoError.DOCUMENT_UNREADABLE;
            assertCode(() -> f.configuration.rebuilds().completedFull(prepared.scope(), request.inputs(), request.dependencyHashes()), expected);
            assertThat(f.mapping()).isEqualTo(before); assertThat(f.snapshot()).isEqualTo(cache); assertThat(f.version()).isEqualTo(version);
        });
    }

    private static PipelineRebuildRequest modelFullRequest(Fixture f) {
        var source = new SourceResource("input", null, "mongodb", Map.of("uri", "mongodb://input.invalid/db"),
                io.tapstate.core.model.SourceMode.CDC, List.of(io.tapstate.core.model.TableRef.literal("orders")), null, null);
        var target = new SourceResource("target", null, "mongodb", Map.of("uri", "mongodb://target.invalid/db"), null, null, null, null);
        var view = new io.tapstate.core.model.ViewResource("reusable_view", null, "id", null, null);
        var serve = new io.tapstate.core.model.ServeResource("reusable_serve", null,
                List.of(new io.tapstate.core.model.SyncElement("sink", target.id(), null, null, null)), null, null, null);
        var pipeline = new io.tapstate.core.model.PipelineResource("flow", null,
                List.of(io.tapstate.core.model.SourceRef.spec(source.id(), true)), null,
                new io.tapstate.core.model.ViewBlock.Use("materialized", view.id(), io.tapstate.core.model.FromRef.literal("orders")),
                new io.tapstate.core.model.ServeBlock.Use(null, serve.id(), io.tapstate.core.model.FromRef.literal("materialized")), null, null);
        f.configuration.artifacts().saveAll(List.of(source, target, view, serve, pipeline));
        var old = f.configuration.intents().load("flow").orElseThrow();
        assertThat(f.configuration.intents().compareAndSet(f.scope, old.intentRevision(),
                new DesiredState("flow", PipelineState.STOPPED, CanonicalHash.of(pipeline), true))).isTrue();
        var current = f.configuration.intents().load("flow").orElseThrow();
        var successor = new PipelineIntentScope(f.scope.clusterId(), f.scope.bindingVersion(), f.scope.cacheIncarnation(), "flow", 2);
        var inputs = PipelinePreparationInputs.of(successor, pipeline, Map.of(source.id(), source),
                Map.of(source.id(), java.util.Set.of("orders")));
        var claim = WorkloadClaimFence.from(f.domain.workloadClaims(f.business).readPipeline(f.scope).orElseThrow().claim());
        return new PipelineRebuildRequest("model-full-original", current, claim, 0, inputs,
                Map.of(target.id(), CanonicalHash.of(target), view.id(), CanonicalHash.of(view), serve.id(), CanonicalHash.of(serve)));
    }

    @Test void aPurgeWithPinnedModelsCanThenDeleteItsSuccessorWithoutTouchingAnotherPipeline() {
        withFixture(f -> {
            var originalClaim = f.advance("flow");
            var models = f.domain.pipelineDerivedSchemas(f.scope, originalClaim, ACTIVE);
            models.record("flow", "source.orders", Map.of("id", "bigint"), "statement", "sources", "derivation");
            models.pin("flow", "source.orders", 0);
            f.initial("other");
            var otherScope = new PipelineIntentScope(f.scope.clusterId(), 1, f.scope.cacheIncarnation(), "other", 1);
            f.domain.pipelineDerivedSchemas(otherScope, f.advance("other"), ACTIVE)
                    .record("other", "source.orders", Map.of("id", "bigint"), "statement", "sources", "derivation");
            var rows = SystemCollections.CACHE_DERIVED_SCHEMAS.on(f.cache);
            var other = rows.find(new Document("_id", "other.source.orders")).first();
            assertThat(other).isNotNull();
            var intent = f.configuration.intents().load("flow").orElseThrow();
            assertThat(f.configuration.intents().compareAndSet(f.scope, intent.intentRevision(),
                    new DesiredState("flow", PipelineState.STOPPED, intent.desired().revision(), true))).isTrue();
            var pending = f.pending(0);
            var receipt = f.rebuilds().prepare(pending, f.node, TTL, AT);
            var prepared = f.configuration.rebuilds().complete(receipt);
            var pipeline = f.configuration.artifacts().get("flow").orElseThrow();
            var deletion = f.configuration.retirements().begin(new PipelineRetirementRequest("delete-after-purge", prepared.scope(),
                    CanonicalHash.of(pipeline), java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(receipt.claim(), receipt.checkpointEpoch()))));

            var retired = f.domain.pipelineRetirements(f.business, ACTIVE).retire(deletion, f.node);
            f.configuration.retirements().complete(retired);

            assertThat(f.configuration.artifacts().get("flow")).isEmpty();
            assertThat(rows.find(new Document("_id", "flow.source.orders")).first()).isNull();
            assertThat(rows.find(new Document("_id", "other.source.orders")).first()).isEqualTo(other);
        });
    }

    @Test void anUnreadableOriginalModelPreventsPurgeFromPublishingASuccessor() {
        withFixture(f -> {
            var models = f.domain.pipelineDerivedSchemas(f.scope, f.advance("flow"), ACTIVE);
            models.record("flow", "source.orders", Map.of("id", "bigint"), "statement", "sources", "derivation");
            SystemCollections.CACHE_DERIVED_SCHEMAS.on(f.cache).updateOne(new Document("_id", "flow.source.orders"),
                    new Document("$set", new Document("pin", 4L)));
            var pending = f.pending(0);
            var before = f.snapshot();
            var mapping = f.mapping();

            assertCode(() -> f.rebuilds().prepare(pending, f.node, TTL, AT), IoError.DOCUMENT_UNREADABLE);

            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.mapping()).isEqualTo(mapping);
        });
    }

    @Test void aLateOldModelCannotBeAcceptedByTheOriginalRebuildRetry() {
        withFixture(f -> {
            var models = f.domain.pipelineDerivedSchemas(f.scope, f.advance("flow"), ACTIVE);
            models.record("flow", "source.orders", Map.of("id", "bigint"), "statement", "sources", "derivation");
            var rows = SystemCollections.CACHE_DERIVED_SCHEMAS.on(f.cache);
            var previous = rows.find(new Document("_id", "flow.source.orders")).first();
            var pending = f.pending(0);
            f.rebuilds().prepare(pending, f.node, TTL, AT);
            assertThat(rows.find(new Document("_id", "flow.source.orders")).first()).isNull();
            rows.insertOne(previous);
            var before = f.snapshot();

            assertCode(() -> f.rebuilds().prepare(pending, f.node, TTL, AT), IoError.DOCUMENT_UNREADABLE);

            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void completedDeletionReusesTheSameIdWithoutResettingClaimOrCheckpointHistory() {
        withFixture(f -> {
            var running=f.advance("flow");var sourceScope=RequiredNoteOwner.forNode(1,new io.tapstate.core.model.PipelineNode("flow","source"),io.tapstate.core.model.PipelineNode.Purpose.CAPTURE,1);
            var token=f.configuration.requiredNotes().initialize(sourceScope);
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();
            var request=new PipelineRetirementRequest("delete-original",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(running,0)));
            var pending=f.configuration.retirements().begin(request);assertThat(pending.privateScopesCount()).isOne();
            assertThat(f.configuration.artifacts().create(pipeline)).isEqualTo(ArtifactMutation.ALREADY_EXISTS);
            var receipt=f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node);
            assertCode(() -> f.configuration.retirements().complete(receipt),IoError.NOTE_SCOPE_UNAVAILABLE);
            var notes=f.configuration.requiredNotes().retirements(f.scope,request.operationId());assertThat(notes).hasSize(1);
            assertThat(notes.getFirst().origin()).isEmpty();notes.getFirst().completeAfterVerifiedRelease();
            var result=f.configuration.retirements().complete(receipt);assertThat(result.cache()).isPresent();
            assertThat(f.configuration.artifacts().get("flow")).isEmpty();assertThat(f.configuration.pipelineStates().read("flow").orElseThrow().phase()).isEqualTo(PipelineStateMapping.Phase.RETIRED);
            long retiredGeneration=f.claim().getLong("claimGeneration"),retiredEpoch=f.state().getLong("epoch");
            assertThat(retiredGeneration).isEqualTo(running.claimGeneration()+1);assertThat(retiredEpoch).isOne();
            f.configuration.artifacts().create(pipeline);var scope=new PipelineIntentScope(f.scope.clusterId(),1,f.scope.cacheIncarnation(),"flow",2);
            var tombstone=f.configuration.intents().load("flow").orElseThrow();assertThat(tombstone.scope()).isEqualTo(scope);assertThat(tombstone.deleted()).isTrue();
            var authority=((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(scope,f.node)).creation();
            var reservation=f.domain.pipelinePreparations(f.business,ACTIVE).reserveFirst(authority,TTL);
            assertThat(reservation.claim().claimGeneration()).isEqualTo(retiredGeneration+1);assertThat(reservation.claim().executionGeneration()).isEqualTo(running.executionGeneration());
            var prepared=f.domain.pipelinePreparations(f.business,ACTIVE).seed(reservation,f.configuration.preparations(),AT);f.configuration.preparations().complete(prepared);
            assertThat(f.state().getLong("epoch")).isEqualTo(retiredEpoch+1);assertThat(f.state().get("scope")).isEqualTo(MongoPipelinePreparationFacts.scope(scope));
            assertThat(f.configuration.retirements().complete(receipt)).isEqualTo(result);assertThat(f.configuration.artifacts().get("flow")).contains(pipeline);
            assertCode(() -> f.configuration.requiredNotes().read(token,"late"),IoError.NOTE_SCOPE_UNAVAILABLE);
        });
    }
    @Test void aNeverPreparedDefinitionHasAnExplicitEmptyHistoryRetirement() {
        withFixture(f -> {
            var pipeline=new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: unused\nsource: []\n");f.configuration.artifacts().create(pipeline);
            var scope=new PipelineIntentScope(f.business.clusterId(),1,f.business.cacheIncarnation(),"unused",1);
            var request=new PipelineRetirementRequest("delete-unused",scope,CanonicalHash.of(pipeline),java.util.Optional.empty());
            var pending=f.configuration.retirements().begin(request);var receipt=f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node);
            assertThat(receipt.retiredState()).isEmpty();f.configuration.retirements().complete(receipt);
            f.configuration.artifacts().create(pipeline);var next=new PipelineIntentScope(f.business.clusterId(),1,f.business.cacheIncarnation(),"unused",2);
            var authority=((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(next,f.node)).creation();
            var reserved=f.domain.pipelinePreparations(f.business,ACTIVE).reserveFirst(authority,TTL);
            f.configuration.preparations().complete(f.domain.pipelinePreparations(f.business,ACTIVE).seed(reserved,f.configuration.preparations(),AT));
            assertThat(reserved.claim().claimGeneration()).isOne();assertThat(reserved.claim().executionGeneration()).isZero();
        });
    }

    @Test void aCompletedRetirementCannotDropItsKnownCacheResultBeforeRecreation() {
        withFixture(f -> {
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();
            var request=new PipelineRetirementRequest("delete-damaged-result",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(f.advance("flow"),0)));
            var pending=f.configuration.retirements().begin(request);f.configuration.retirements().complete(f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node));
            SystemCollections.OPERATOR_GENERATIONS.on(f.config).updateOne(new Document("_id","flow"),new Document("$set",new Document("retirement.cacheResult",null)));
            var before=f.mapping();long version=f.version();
            assertCode(() -> f.configuration.artifacts().create(pipeline),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.mapping()).isEqualTo(before);assertThat(f.version()).isEqualTo(version);assertThat(f.configuration.artifacts().get("flow")).isEmpty();
        });
    }

    @Test void anUnpreparedDeletionPreservesUnexpectedCacheHistoryAndRefusesCompletion() {
        withFixture(f -> {
            var pipeline=new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: unused\nsource: []\n");
            f.configuration.artifacts().create(pipeline);
            var scope=new PipelineIntentScope(f.business.clusterId(),1,f.business.cacheIncarnation(),"unused",1);
            var request=new PipelineRetirementRequest("delete-unknown-history",scope,CanonicalHash.of(pipeline),java.util.Optional.empty());
            var pending=f.configuration.retirements().begin(request);
            f.states().insertOne(new Document("_id","unused").append("retainedEvidence","unknown-history"));
            var before=f.snapshot();

            assertCode(() -> f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node),IoError.PIPELINE_STATE_REBUILD_REQUIRED);

            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.configuration.artifacts().get("unused")).contains(pipeline);
            assertThat(f.configuration.retirements().completed("unused",request.operationId())).isEmpty();
            assertThat(f.configuration.pipelineStates().read("unused").orElseThrow().phase()).isEqualTo(PipelineStateMapping.Phase.RETIRING);
        });
    }

    @Test void retirementResumesAcrossIndependentClientsWithoutReissuingOrAdvancingItsResult() {
        withFixture(f -> {
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();
            var request=new PipelineRetirementRequest("delete-reopen",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(f.advance("flow"),0)));
            var pending=f.configuration.retirements().begin(request);
            var receipt=f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node);var beforeClaim=f.claim();var beforeState=f.state();
            try(var reopened=MongoConfigurationDomain.open(new MongoConnectionSettings(CONFIG.getReplicaSetUrl(f.config.getName()),null,Duration.ofSeconds(5)),"cluster-a");
                var other=MongoCacheDomain.connect(f.connection,Duration.ofSeconds(5),"deletion-resume")) {
                var retained=reopened.retirements().pending(f.scope,request.operationId()).orElseThrow();
                var again=other.pipelineRetirements(f.business,ACTIVE).retire(retained,f.node);
                assertThat(again.retiredState()).isEqualTo(receipt.retiredState());assertThat(f.claim()).isEqualTo(beforeClaim);assertThat(f.state()).isEqualTo(beforeState);
                var result=reopened.retirements().complete(again);long version=f.version();
                assertThat(f.configuration.retirements().complete(receipt)).isEqualTo(result);
                assertThat(reopened.retirements().completed("flow",request.operationId())).contains(result);assertThat(f.version()).isEqualTo(version);
            }
        });
    }
    @Test void aLostPrivateScopeCannotBeReinterpretedAsTheRemainingCompleteSet() {
        withFixture(f -> {
            var notes=f.configuration.requiredNotes();
            notes.initialize(RequiredNoteOwner.forNode(1,new io.tapstate.core.model.PipelineNode("flow","one"),io.tapstate.core.model.PipelineNode.Purpose.CAPTURE,1));
            notes.initialize(RequiredNoteOwner.forNode(1,new io.tapstate.core.model.PipelineNode("flow","two"),io.tapstate.core.model.PipelineNode.Purpose.SYNC,1));
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();
            var request=new PipelineRetirementRequest("delete-missing-note",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(f.advance("flow"),0)));
            var pending=f.configuration.retirements().begin(request);assertThat(pending.privateScopesCount()).isEqualTo(2);
            var receipt=f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node);
            notes.retirements(f.scope,request.operationId()).getFirst().completeAfterVerifiedRelease();
            var table=SystemCollections.PDK_NOTE_SCOPES.on(f.config);table.deleteOne(new Document("_id",table.find().first().get("_id")));
            assertCode(() -> f.configuration.retirements().pending(f.scope,request.operationId()),IoError.DOCUMENT_UNREADABLE);
            assertCode(() -> f.configuration.retirements().complete(receipt),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.configuration.artifacts().get("flow")).contains(pipeline);
        });
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void missingKnownRetiredHistoryNeverBecomesANewClaim(boolean loseCheckpoint) {
        withFixture(f -> {
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();
            var request=new PipelineRetirementRequest("delete-known",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(f.advance("flow"),0)));
            var pending=f.configuration.retirements().begin(request);f.configuration.retirements().complete(f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node));
            f.configuration.artifacts().create(pipeline);var scope=new PipelineIntentScope(f.scope.clusterId(),1,f.scope.cacheIncarnation(),"flow",2);
            var creation=((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(scope,f.node)).creation();
            if(loseCheckpoint)f.states().deleteOne(new Document("_id","flow"));else f.claims().deleteOne(new Document("_id.resourceId","flow"));
            var before=f.snapshot();assertThatThrownBy(() -> f.domain.pipelinePreparations(f.business,ACTIVE).reserveFirst(creation,TTL)).isInstanceOf(TapstateException.class);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void concurrentRecreationPublishesOneGenerationAndPreservesUnusedPredecessorHistory() {
        withFixture(f -> {
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();
            var request=new PipelineRetirementRequest("delete-first",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(f.advance("flow"),0)));
            var pending=f.configuration.retirements().begin(request);var first=f.configuration.retirements().complete(f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node));
            try(var reopened=MongoConfigurationDomain.open(new MongoConnectionSettings(CONFIG.getReplicaSetUrl(f.config.getName()),null,Duration.ofSeconds(5)),"cluster-a");var workers=Executors.newFixedThreadPool(2)) {
                var start=new CountDownLatch(1);
                var a=workers.submit(() -> {start.await();return f.configuration.artifacts().create(pipeline);});
                var b=workers.submit(() -> {start.await();return reopened.artifacts().create(pipeline);});start.countDown();
                assertThat(List.of(a.get(30,TimeUnit.SECONDS),b.get(30,TimeUnit.SECONDS))).containsExactlyInAnyOrder(ArtifactMutation.CREATED,ArtifactMutation.ALREADY_EXISTS);
            }
            var secondScope=new PipelineIntentScope(f.scope.clusterId(),1,f.scope.cacheIncarnation(),"flow",2);
            var secondRequest=new PipelineRetirementRequest("delete-unused-recreation",secondScope,CanonicalHash.of(pipeline),java.util.Optional.empty());
            var second=f.configuration.retirements().begin(secondRequest);var result=f.configuration.retirements().complete(f.domain.pipelineRetirements(f.business,ACTIVE).retire(second,f.node));
            assertThat(result.cache()).isEqualTo(first.cache());
            f.configuration.artifacts().create(pipeline);var thirdScope=new PipelineIntentScope(f.scope.clusterId(),1,f.scope.cacheIncarnation(),"flow",3);
            var authority=((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(thirdScope,f.node)).creation();
            var reserved=f.domain.pipelinePreparations(f.business,ACTIVE).reserveFirst(authority,TTL);
            assertThat(reserved.claim().claimGeneration()).isEqualTo(first.cache().orElseThrow().claim().claimGeneration()+1);
            f.configuration.preparations().complete(f.domain.pipelinePreparations(f.business,ACTIVE).seed(reserved,f.configuration.preparations(),AT));
            assertThat(f.state().getLong("epoch")).isEqualTo(first.cache().orElseThrow().epoch()+1);
        });
    }
    @Test void sameScopeExecutionChangeRequiresAFreshFixedIsolationCandidate() {
        withFixture(f -> {
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();var original=f.advance("flow");
            var request=new PipelineRetirementRequest("delete-inflight-start",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(original,0)));
            var pending=f.configuration.retirements().begin(request);
            var store=f.domain.workloadClaims(f.business);assertThat(store.release(store.readPipeline(f.scope).orElseThrow().claim())).isTrue();
            var candidate=WorkloadClaimFence.from(store.acquirePipeline(f.scope,OWNER,0,TTL).claim());
            assertThat(candidate.topologyRevision()).isZero();
            assertThat(candidate.claimGeneration()).isGreaterThan(original.claimGeneration());
            assertThatThrownBy(() -> f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node)).isInstanceOf(TapstateException.class);
            var isolated=java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(candidate,0));
            var receipt=f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node,isolated);
            assertThat(receipt.retiredState().orElseThrow().claim().claimGeneration()).isEqualTo(candidate.claimGeneration()+1);
            f.configuration.retirements().complete(receipt);
        });
    }

    @Test void reopenedCleanupUsesOnlyTheFrozenDeletedScopeAfterItsReplacementIsRunning() {
        withFixture(f -> {
            var running=f.advance("flow");var old=new OperatorPlacement(f.scope,f.cache.getName(),OperatorPlacement.Resource.JOIN_FACTS,"join","fact","facts");
            var writer=f.domain.operatorState(old,running,ACTIVE);writer.save(old.mapName(),"old",new byte[]{1});
            var pipeline=f.configuration.artifacts().get("flow").orElseThrow();
            var request=new PipelineRetirementRequest("delete-and-clean-later",f.scope,CanonicalHash.of(pipeline),java.util.Optional.of(new PipelineRetirementRequest.CacheOriginal(running,0)));
            var pending=f.configuration.retirements().begin(request);f.configuration.retirements().complete(f.domain.pipelineRetirements(f.business,ACTIVE).retire(pending,f.node));
            f.configuration.artifacts().create(pipeline);var scope=new PipelineIntentScope(f.scope.clusterId(),1,f.scope.cacheIncarnation(),"flow",2);
            var creation=((FirstPipelineCreationResult.Issued)f.configuration.preparations().issueFirstCreate(scope,f.node)).creation();
            var reservation=f.domain.pipelinePreparations(f.business,ACTIVE).reserveFirst(creation,TTL);
            f.configuration.preparations().complete(f.domain.pipelinePreparations(f.business,ACTIVE).seed(reservation,f.configuration.preparations(),AT));
            var store=f.domain.workloadClaims(f.business);var current=store.advanceExecution(store.acquirePipeline(scope,OWNER,1,TTL).claim(),1,java.util.Set.of(OWNER.nodeId())).orElseThrow();
            var replacement=new OperatorPlacement(scope,f.cache.getName(),OperatorPlacement.Resource.JOIN_FACTS,"join","fact","facts");
            f.domain.operatorState(replacement,WorkloadClaimFence.from(current),ACTIVE).save(replacement.mapName(),"keep",new byte[]{2});
            try(var reopened=MongoCacheDomain.connect(f.connection,Duration.ofSeconds(5),"old-payload-retry")) {
                var cleanup=reopened.retiredPipelinePayloads(f.business,ACTIVE);var original=cleanup.find(f.scope).orElseThrow();
                assertThat(cleanup.list("flow",0,128)).containsExactly(original);
                var fake=new RetiredPipelinePayloads.Original(original.scope(),original.operationId(),original.preparationId(),"0".repeat(64));
                assertThatThrownBy(() -> cleanup.reclaim(fake,f.node)).isInstanceOf(TapstateException.class);
                assertThat(cleanup.reclaim(original,f.node)).isGreaterThan(0);assertThat(cleanup.reclaim(original,f.node)).isZero();
                assertThat(cleanup.find(f.scope)).contains(original);assertThat(cleanup.list("flow",1,128)).isEmpty();
            }
            assertThat(f.domain.operatorState(replacement).load(replacement.mapName(),"keep")).contains(new byte[]{2});
            assertCode(() -> writer.save(old.mapName(),"late",new byte[]{3}),IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.claim().getLong("executionGeneration")).isEqualTo(2);
        });
    }

    @Test void originalPrivateRetirementTokensSurviveReopenAndCompletedOnesDoNotReleaseTwice() {
        withFixture(f -> {
            var notes=f.configuration.requiredNotes();
            var a=notes.initialize(RequiredNoteOwner.forNode(1,new io.tapstate.core.model.PipelineNode("flow","same"),io.tapstate.core.model.PipelineNode.Purpose.CAPTURE,1));
            var b=notes.initialize(RequiredNoteOwner.forNode(1,new io.tapstate.core.model.PipelineNode("flow","same"),io.tapstate.core.model.PipelineNode.Purpose.SYNC,1));
            var origin=new RequiredNoteOrigin("original","a".repeat(64),new byte[]{1});notes.prepareOrigin(a,origin);notes.prepareOrigin(b,origin);
            notes.compareAndSet(a,"slot",0,new byte[]{1},false);notes.compareAndSet(b,"slot",0,new byte[]{2},false);
            assertCode(() -> notes.retirements(f.scope,"operation"),IoError.NOTE_SCOPE_UNAVAILABLE);
            var pending=f.pending(0);var operation=pending.request().operationId();
            assertThat(notes.privateContexts(f.scope,operation)).hasSize(2).allSatisfy(context -> {assertThat(context.origin()).contains(origin);assertThat(context.completed()).isFalse();});
            var tokens=notes.retirements(f.scope,operation);assertThat(tokens).hasSize(2).allSatisfy(token -> assertThat(token.completed()).isFalse());
            var selected=tokens.stream().filter(token -> token.scope().equals(a)).findFirst().orElseThrow();selected.completeAfterVerifiedRelease();
            try(var reopened=MongoConfigurationDomain.open(new MongoConnectionSettings(CONFIG.getReplicaSetUrl(f.config.getName()),null,Duration.ofSeconds(5)),"cluster-a")) {
                assertThat(reopened.requiredNotes().privateContexts(f.scope,operation).stream().filter(RequiredNoteContext::completed)).hasSize(1);
                var retry=reopened.requiredNotes().retirements(f.scope,operation);assertThat(retry).hasSize(2);
                assertThat(retry.stream().filter(RequiredNoteRetirement::completed)).singleElement().satisfies(token -> assertThat(token.scope()).isEqualTo(a));
                var owed=retry.stream().filter(token -> !token.completed()).findFirst().orElseThrow();
                assertThat(owed.scope()).isEqualTo(b);assertThat(owed.origin()).contains(origin);
                assertThat(owed.notes().read(b,"slot").bytes()).containsExactly(2);owed.completeAfterVerifiedRelease();
                assertThat(reopened.requiredNotes().retirements(f.scope,operation)).allSatisfy(token -> assertThat(token.completed()).isTrue());
                assertCode(() -> reopened.requiredNotes().retirements(f.scope,"different-operation"),IoError.NOTE_SCOPE_UNAVAILABLE);
            }
        });
    }
    @Test void retirementRejectsCorruptPersistentNamespaceWithoutLeakingAnUncodedFailure() {
        withFixture(f -> {
            var pending=f.pending(0);var table=SystemCollections.PDK_NOTE_SCOPES.on(f.config);
            table.insertOne(new Document("_id",new Document("bindingVersion",1L).append("namespace","foreign-persistent-namespace"))
                    .append("ownerKind","PIPELINE").append("ownerId","flow").append("generation",1L).append("writeFence",0L)
                    .append("originGeneration",0L).append("outgoingCarries",0L).append("incomingCarries",0L));
            long before=table.countDocuments();assertCode(() -> f.configuration.requiredNotes().retirements(f.scope,pending.request().operationId()),IoError.NOTE_SCOPE_UNAVAILABLE);
            assertThat(table.countDocuments()).isEqualTo(before);
        });
    }
    @Test void anOriginalPendingRebuildDetachesOnlyItsConsumerAfterItsBusinessLeaseExpires() {
        withFixture(f -> {
            var original=f.advance("flow");f.initial("independent");var other=f.advance("independent");
            var raw=new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache));
            var capture=WorkloadClaimFence.from(raw.acquire(new WorkloadClaimKey("cluster-a",WorkloadClaimType.CAPTURE,"shared"),OWNER,1,TTL).claim());
            CaptureHistoryFixture.shared(f.cache,f.business,capture,"shared-chain",null);var stores=f.domain.srs(f.business,ACTIVE);stores.chains().prepare("shared-chain",null,capture);
            var a=stores.consumers().attach(new SrsConsumerContext(new SrsConsumerKey("shared-chain","flow","source"),1),0,java.util.Set.of("orders"),original).orElseThrow();
            var b=stores.consumers().attach(new SrsConsumerContext(new SrsConsumerKey("shared-chain","independent","source"),1),0,java.util.Set.of("orders"),other).orElseThrow();
            var progress=stores.consumers().progress(b.context()).orElseThrow();var pending=f.pending(0);
            f.claims().updateOne(new Document("_id.resourceId","flow"),new Document("$set",new Document("leaseUntil",new java.util.Date(0))));
            assertThat(stores.consumers().detachForRebuild(pending,a,f.node)).hasValueSatisfying(value -> assertThat(value.detached()).isTrue());
            assertThat(stores.consumers().progress(b.context())).contains(progress);assertThat(stores.consumers().attached("shared-chain").consumers()).containsExactly(b);
            f.configuration.rebuilds().complete(f.rebuilds().prepare(pending,f.node,TTL,AT));
        });
    }
    @Test void aReopenedControllerLoadsOnlyTheOriginalPendingOperationAndCleansWithItsOwnNode() {
        withFixture(f -> {
            var pending=f.pending(0);var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);
            f.claims().updateMany(new Document(),new Document("$set",new Document("leaseUntil",new java.util.Date(0))));
            var replacement=new WorkloadOwner("replacement","replacement-boot");
            var current=WorkloadClaimFence.from(f.domain.workloadClaims(f.business).acquire(new WorkloadClaimKey("cluster-a",WorkloadClaimType.NODE_SESSION,replacement.nodeId()),replacement,0,TTL).claim());
            try(var reopened=MongoConfigurationDomain.open(new MongoConnectionSettings(CONFIG.getReplicaSetUrl(f.config.getName()),null,Duration.ofSeconds(5)),"cluster-a")) {
                var retained=reopened.rebuilds().pending("flow",pending.request().inputs(),Map.of()).orElseThrow();
                assertThat(retained.request()).isEqualTo(pending.request());assertThat(retained.preparationId()).isEqualTo(pending.preparationId());
                var resumed=f.rebuilds().prepare(retained,current,TTL,AT);assertThat(resumed.claim()).isEqualTo(receipt.claim());
                while(f.rebuilds().reclaim(resumed,current)>0){}
                assertThat(reopened.rebuilds().complete(resumed).scope()).isEqualTo(pending.request().targetScope());
                assertThat(reopened.rebuilds().pending("flow",pending.request().inputs(),Map.of())).isEmpty();
            }
        });
    }
    @Test void retiredPayloadCleanupDoesNotRequireAStillRunningSuccessorBusinessLease() {
        withFixture(f -> {
            var fence=f.advance("flow");var location=new OperatorPlacement(f.scope,f.cache.getName(),OperatorPlacement.Resource.JOIN_FACTS,"join","fact","facts");
            f.domain.operatorState(location,fence,ACTIVE).save(location.mapName(),"old",new byte[]{1});
            var pending=f.pending(0);var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);
            f.claims().updateOne(new Document("_id.resourceId","flow"),new Document("$set",new Document("leaseUntil",new java.util.Date(0))));
            assertThat(f.rebuilds().reclaim(receipt)).isGreaterThan(0);
            assertThat(SystemCollections.CACHE_JOIN_FACTS.on(f.cache).countDocuments()).isZero();
            assertThat(f.configuration.rebuilds().complete(receipt).scope()).isEqualTo(pending.request().targetScope());
        });
    }
    @Test void retiredPayloadReclamationIsBoundedAndKeepsOtherScopesAndBirthHistory() {
        withFixture(f -> {
            var fence=f.advance("flow");
            var location=new OperatorPlacement(f.scope,f.cache.getName(),OperatorPlacement.Resource.JOIN_FACTS,"join","fact","facts");
            var writer=f.domain.operatorState(location,fence,ACTIVE);var values=new java.util.HashMap<String,byte[]>();
            for(int i=0;i<300;i++)values.put("key-"+i,new byte[]{1});values.forEach((key,value) -> writer.save(location.mapName(),key,value));
            f.initial("independent");var otherScope=new PipelineIntentScope(f.scope.clusterId(),f.scope.bindingVersion(),f.scope.cacheIncarnation(),"independent",1);
            var other=f.advance("independent");
            var neighbor=new OperatorPlacement(otherScope,f.cache.getName(),OperatorPlacement.Resource.JOIN_FACTS,"join","fact","facts");
            f.domain.operatorState(neighbor,other,ACTIVE).save(neighbor.mapName(),"keep",new byte[]{9});
            var pending=f.pending(0);var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);
            long first=f.rebuilds().reclaim(receipt);assertThat(first).isEqualTo(256);
            assertThat(SystemCollections.CACHE_JOIN_FACTS.on(f.cache).countDocuments()).isEqualTo(45);
            assertThat(f.rebuilds().reclaim(receipt)).isGreaterThanOrEqualTo(44).isLessThanOrEqualTo(256);
            assertThat(f.rebuilds().reclaim(receipt)).isZero();
            assertThat(SystemCollections.CACHE_JOIN_FACTS.on(f.cache).countDocuments()).isOne();
            assertThat(f.domain.operatorState(neighbor).load(neighbor.mapName(),"keep")).contains(new byte[]{9});
            try(var session=f.cacheClient.startSession()){session.withTransaction(() -> {MongoOperatorInventory.requireRetired(session,f.cache,pending);return null;});}
            assertCode(() -> writer.save(location.mapName(),"late",new byte[]{3}),IoError.CACHE_BUSINESS_FENCED);
            f.configuration.rebuilds().complete(receipt);assertThat(f.rebuilds().reclaim(receipt)).isZero();
            var claims=f.domain.workloadClaims(f.business);
            claims.advanceExecution(claims.readPipeline(pending.request().targetScope()).orElseThrow().claim(),1,java.util.Set.of(OWNER.nodeId())).orElseThrow();
            assertThat(f.rebuilds().reclaim(receipt)).isZero();
        });
    }

    @Test void successorHasItsOwnPreparedSnapshotOrderBeforeAValidNewGenerationCanStart() {
        withFixture(f -> {
            var pending=f.pending(0); f.rebuilds().prepare(pending,f.node,TTL,AT);
            var scope=pending.request().targetScope(); var encoded=MongoPipelinePreparationFacts.scope(scope);
            assertThat(SystemCollections.CACHE_SNAPSHOT_ORDERS.on(f.cache).find(new Document("_id",encoded)).first()).isNotNull();
            f.domain.pipelinePreparations(f.business,ACTIVE).verifySources(scope,pending.request().inputs());
            try(var session=f.cacheClient.startSession()) {
                session.withTransaction(() -> { MongoSnapshotStateStore.verifyInitial(session,f.cache,scope);return null; });
            }
            var beforeOrders=SystemCollections.CACHE_SNAPSHOT_ORDERS.on(f.cache).find().sort(new Document("_id",1)).into(new ArrayList<>());
            var beforeCounts=SystemCollections.CACHE_SNAPSHOT_COUNTS.on(f.cache).find().into(new ArrayList<>());
            var beforeState=f.state(); var beforeClaim=f.claim(); var again=f.rebuilds().prepare(pending,f.node,TTL,AT);
            assertThat(again.preparation().request().targetScope()).isEqualTo(scope);
            assertThat(SystemCollections.CACHE_SNAPSHOT_ORDERS.on(f.cache).find().sort(new Document("_id",1)).into(new ArrayList<>())).isEqualTo(beforeOrders);
            assertThat(SystemCollections.CACHE_SNAPSHOT_COUNTS.on(f.cache).find().into(new ArrayList<>())).isEqualTo(beforeCounts);
            assertThat(f.state()).isEqualTo(beforeState); assertThat(f.claim()).isEqualTo(beforeClaim);
        });
    }

    @Test void receiptReplayRefusesLostSnapshotOrderOrUnexpectedMeasurementsWithoutRepair() {
        withFixture(f -> {
            var pending=f.pending(0); f.rebuilds().prepare(pending,f.node,TTL,AT);
            var encoded=MongoPipelinePreparationFacts.scope(pending.request().targetScope());
            var orders=SystemCollections.CACHE_SNAPSHOT_ORDERS.on(f.cache); var initial=orders.find(new Document("_id",encoded)).first();
            orders.deleteOne(new Document("_id",encoded)); var before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
            orders.insertOne(initial);
            SystemCollections.CACHE_SNAPSHOT_COUNTS.on(f.cache).insertOne(new Document("_id",new Document("scope",encoded)).append("unknown",true)); before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void successorRetainsExecutionHistoryAndPipelineTopologyWithAZeroTopologyNode() {
        withFixture(f -> {
            var old = f.claim();
            f.claims().updateOne(new Document("_id",old.get("_id")), new Document("$set", new Document("executionGeneration",4L)
                    .append("topologyRevision",7L).append("contextExecutionGeneration",4L).append("executionClaimGeneration",1L)
                    .append("executionNodeIds",List.of("node-a","node-b")).append("failureClaimGeneration",1L).append("failureAfterMemberLoss",true)));
            f.states().updateOne(new Document("_id","flow"),new Document("$set",new Document("epoch",9L).append("runtimeWriteFence",11L).append("stateJson","RUNNING")));
            var original = f.pending(9);
            var placement = new OperatorPlacement(f.scope,f.cache.getName(),OperatorPlacement.Resource.JOIN_FACTS,"join","fact","facts");
            var writer = f.domain.operatorState(placement,original.request().originalClaim(),ACTIVE);
            writer.save(placement.mapName(),"original",new byte[]{1,2});
            var before = f.claim(); var oldState = f.state();
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.cache).insertOne(new Document("_id","flow").append("scope",MongoPipelinePreparationFacts.scope(f.scope)).append("diagnostic","old"));
            f.initial("independent"); var b = f.claims().find(new Document("_id.resourceId","independent")).first();
            var receipt = f.rebuilds().prepare(original,f.node,TTL,AT);
            assertThat(receipt.claim().claimGeneration()).isEqualTo(original.request().originalClaim().claimGeneration()+1);
            assertThat(receipt.claim().executionGeneration()).isEqualTo(4);
            assertThat(receipt.claim().topologyRevision()).isEqualTo(7);
            assertThat(receipt.issuerNode().topologyRevision()).isZero();
            for (String field : List.of("executionGeneration","contextExecutionGeneration","executionClaimGeneration","executionNodeIds","failureClaimGeneration","failureAfterMemberLoss","topologyRevision"))
                assertThat(f.claim().get(field)).as(field).isEqualTo(before.get(field));
            assertThat(f.state()).containsEntry("scope",MongoPipelinePreparationFacts.scope(original.request().targetScope()))
                    .containsEntry("epoch",10L).containsEntry("runtimeWriteFence",oldState.getLong("runtimeWriteFence")+1).containsEntry("stateJson","STOPPED");
            assertThat(f.claims().find(new Document("_id.resourceId","independent")).first()).isEqualTo(b);
            assertThat(SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.cache).find(new Document("_id","flow")).first()).isNull();
            assertThat(f.configuration.intents().loadForActuation("flow")).isEmpty();
            try (var session=f.cacheClient.startSession()) {
                session.withTransaction(() -> { assertThat(MongoOperatorInventory.requireRetired(session,f.cache,original).getList("placements",Document.class)).hasSize(1); return null; });
            }
            assertThat(SystemCollections.CACHE_JOIN_FACTS.on(f.cache).countDocuments()).isEqualTo(1);
            assertCode(() -> writer.save(placement.mapName(),"late",new byte[]{3}),IoError.CACHE_BUSINESS_FENCED);
            var again=f.rebuilds().prepare(original,f.node,TTL,AT.plusSeconds(1));
            assertThat(again.claim()).isEqualTo(receipt.claim()); assertThat(again.checkpointEpoch()).isEqualTo(receipt.checkpointEpoch());
        });
    }

    @Test void concurrentIndependentCacheClientsPublishOneSuccessorAndReplayItsOriginalReceipt() {
        withFixture(f -> {
            var pending=f.pending(0); var start=new CountDownLatch(1);
            try (var other=MongoCacheDomain.connect(f.connection,Duration.ofSeconds(5),"second-rebuild");var workers=Executors.newFixedThreadPool(2)) {
                var a=workers.submit(() -> { start.await(); return f.rebuilds().prepare(pending,f.node,TTL,AT); });
                var b=workers.submit(() -> { start.await(); return other.pipelineRebuilds(f.business,ACTIVE).prepare(pending,f.node,TTL,AT); });
                start.countDown(); var first=a.get(30,TimeUnit.SECONDS); var second=b.get(30,TimeUnit.SECONDS);
                assertThat(first.claim()).isEqualTo(second.claim()); assertThat(first.claim().claimGeneration()).isEqualTo(2);
                assertThat(first.checkpointEpoch()).isEqualTo(1); assertThat(second.checkpointEpoch()).isEqualTo(1);
                assertThat(SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.cache).countDocuments()).isEqualTo(2);
            }
        });
    }

    @Test void missingOriginalClaimOrCheckpointCannotBecomeFirstCreation() {
        withFixture(f -> {
            var pending=f.pending(0); var original=f.claim();
            f.claims().deleteOne(new Document("_id",original.get("_id"))); var before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.PIPELINE_STATE_REBUILD_REQUIRED); assertThat(f.snapshot()).isEqualTo(before);
            f.claims().insertOne(original); f.states().deleteOne(new Document("_id","flow")); before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.DOCUMENT_UNREADABLE); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void unknownSuccessorResourcesNeverGetAdoptedOrDeleted() {
        withFixture(f -> {
            var pending=f.pending(0); var scope=MongoPipelinePreparationFacts.scope(pending.request().targetScope());
            var fixtures=List.of(
                    Map.entry(SystemCollections.CACHE_NEST_STATE,new Document("placement",new Document("scope",scope))),
                    Map.entry(SystemCollections.CACHE_JOIN_FACTS,new Document("placement",new Document("scope",scope))),
                    Map.entry(SystemCollections.CACHE_JOIN_DIMENSIONS,new Document("placement",new Document("scope",scope))),
                    Map.entry(SystemCollections.CACHE_JOIN_INDEXES,new Document("placement",new Document("scope",scope))),
                    Map.entry(SystemCollections.CACHE_JOIN_WRITERS,new Document("placement",new Document("scope",scope))),
                    Map.entry(SystemCollections.CACHE_NEST_SHAPES,new Document("scope",scope)),Map.entry(SystemCollections.CACHE_OPERATOR_CLEANUP,scope),
                    Map.entry(SystemCollections.DIRECT_CHANNEL_OFFSETS,new Document("scope",scope)),Map.entry(SystemCollections.DIRECT_BATCHES,new Document("channel",new Document("scope",scope))),
                    Map.entry(SystemCollections.CACHE_SNAPSHOT_ORDERS,scope),Map.entry(SystemCollections.CACHE_SNAPSHOT_COUNTS,new Document("scope",scope)));
            for (var fixture:fixtures) {
                var table=fixture.getKey().on(f.cache); table.insertOne(new Document("_id",fixture.getValue()).append("unknown",true));var before=f.snapshot();
                assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.PIPELINE_STATE_REBUILD_REQUIRED); assertThat(f.snapshot()).as(fixture.getKey().name()).isEqualTo(before);
                table.deleteOne(new Document("_id",fixture.getValue()));
            }
            for (var table:List.of(SystemCollections.CACHE_DERIVED_SCHEMAS,SystemCollections.CACHE_PIPELINE_RATE_HISTORY)) {
                table.on(f.cache).insertOne(new Document("_id","unknown").append("scope",scope));var before=f.snapshot();
                assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.snapshot()).isEqualTo(before);
                table.on(f.cache).deleteOne(new Document("_id","unknown"));
            }
        });
    }

    @Test void originalClaimChangesAndForeignObservationPreventTheEntireTransition() {
        withFixture(f -> {
            var pending=f.pending(0); var row=f.claim();
            f.claims().updateOne(new Document("_id",row.get("_id")),new Document("$inc",new Document("claimGeneration",1L)));var before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.WORKLOAD_CLAIM_FENCED);assertThat(f.snapshot()).isEqualTo(before);
            f.claims().replaceOne(new Document("_id",row.get("_id")),row);
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.cache).insertOne(new Document("_id","flow").append("scope",MongoPipelinePreparationFacts.scope(pending.request().targetScope())));
            before=f.snapshot();assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void corruptOriginalInventoryAndDamagedReplayAreNeverRepaired() {
        withFixture(f -> {
            var pending=f.pending(0); var table=SystemCollections.CACHE_OPERATOR_CLEANUP.on(f.cache);var old=table.find().first();
            table.updateOne(new Document("_id",old.get("_id")),new Document("$set",new Document("revision",1L)));var before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.DOCUMENT_UNREADABLE);assertThat(f.snapshot()).isEqualTo(before);
            table.replaceOne(new Document("_id",old.get("_id")),old); f.rebuilds().prepare(pending,f.node,TTL,AT);
            table.updateOne(new Document("_id",old.get("_id")),new Document("$set",new Document("retirement.contentHash","0".repeat(64))));before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.DOCUMENT_UNREADABLE);assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void replayRejectsAnIssuerNodeWhoseCanonicalResourceDoesNotBelongToItsOwner() {
        withFixture(f -> {
            var pending=f.pending(0); f.rebuilds().prepare(pending,f.node,TTL,AT);
            f.claims().updateOne(new Document("_id.resourceId","flow"),new Document("$set",new Document("pipelineRebuild.issuerNode.resourceId","another-node")));
            var before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void completionPublishesOneSuccessorAndConsumesOnlyTheConfirmedOneShotFlags() {
        withFixture(f -> {
            for (Long stamp : java.util.Arrays.asList(null,0L)) {
                var current=f.configuration.intents().load("flow").orElseThrow();
                var desired=new DesiredState("flow",PipelineState.RUNNING,current.desired().revision(),true,"assembly",true,stamp);
                assertThat(f.configuration.intents().compareAndSet(current.scope(),current.intentRevision(),desired)).isTrue();
                var pending=f.pending(f.state().getLong("epoch")); var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);
                long version=f.version();var completed=f.configuration.rebuilds().complete(receipt);
                assertThat(completed.scope()).isEqualTo(pending.request().targetScope());assertThat(completed.phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
                var after=f.configuration.intents().loadForActuation("flow").orElseThrow();
                assertThat(after.scope()).isEqualTo(completed.scope());assertThat(after.intentRevision()).isEqualTo(pending.request().intent().intentRevision()+1);
                assertThat(after.desired()).isEqualTo(new DesiredState("flow",PipelineState.RUNNING,desired.revision(),false,"assembly",false,null));
                assertThat(f.version()).isEqualTo(version+1);
                assertThat(f.configuration.rebuilds().complete(receipt)).isEqualTo(completed);assertThat(f.version()).isEqualTo(version+1);
            }
        });
    }

    @Test void committedResultSurvivesLostResponseProcessReopenAndCacheProgressWithoutRepublishing() {
        withFixture(f -> {
            var pending=f.pending(0);var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);var complete=f.configuration.rebuilds().complete(receipt);long version=f.version();
            f.states().updateOne(new Document("_id","flow"),new Document("$inc",new Document("epoch",1L).append("runtimeWriteFence",1L)));
            try(var reopened=MongoConfigurationDomain.open(new MongoConnectionSettings(CONFIG.getReplicaSetUrl(f.config.getName()),null,Duration.ofSeconds(5)),"cluster-a")) {
                assertThat(reopened.rebuilds().completed(pending.request())).contains(complete);assertThat(f.version()).isEqualTo(version);
            }
            f.domain.close();assertThat(f.configuration.rebuilds().complete(receipt)).isEqualTo(complete);assertThat(f.version()).isEqualTo(version);
        });
    }

    @Test void staleCacheReceiptCannotPublishAndMissingDirectStructureIsNotRecreated() {
        withFixture(f -> {
            var pending=f.pending(0);var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);var before=f.mapping();long version=f.version();
            f.states().updateOne(new Document("_id","flow"),new Document("$inc",new Document("runtimeWriteFence",1L)));
            assertCode(() -> f.configuration.rebuilds().complete(receipt),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.mapping()).isEqualTo(before);assertThat(f.version()).isEqualTo(version);
            f.states().updateOne(new Document("_id","flow"),new Document("$inc",new Document("runtimeWriteFence",-1L)));
            SystemCollections.DIRECT_BATCHES.on(f.cache).drop();
            assertCode(() -> f.configuration.rebuilds().complete(receipt),IoError.DOCUMENT_UNREADABLE);assertThat(f.mapping()).isEqualTo(before);assertThat(f.version()).isEqualTo(version);
            assertThat(f.cache.listCollectionNames().into(new ArrayList<>())).doesNotContain(SystemCollections.DIRECT_BATCHES.collectionName());
        });
    }

    @Test void lossAfterTheActualConfigPublishWriteRollsBackMappingIntentAndVersion() {
        withFixture(f -> {
            var pending=f.pending(0);
            try(var witness=new MongoCommandWitness(CONFIG.getReplicaSetUrl(),f.config.getName(),"update","operator_generations")) {
                var material=SystemCollections.CLUSTER_KEYS.on(f.config).find().first().getString("material");
                var cipher=SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                var bindings=new MongoStorageBindingStore(witness.client,witness.database,cipher);
                var artifacts=new MongoConfigurationArtifactStore(witness.client,witness.database,cipher);
                var store=new MongoPipelineStateRebuilds(witness.client,witness.database,"cluster-a",bindings,artifacts,witness::refuseAfterCommand);
                var resumed=store.begin(pending.request());var receipt=f.rebuilds().prepare(resumed,f.node,TTL,AT);
                long version=f.version();var mapping=f.mapping();var intent=f.configuration.intents().load("flow");
                assertCode(() -> store.complete(receipt),IoError.CACHE_BUSINESS_FENCED);witness.requireCommand();
                assertThat(f.version()).isEqualTo(version);assertThat(f.mapping()).isEqualTo(mapping);assertThat(f.configuration.intents().load("flow")).isEqualTo(intent);
            }
        });
    }

    @Test void completionRejectsUnknownTargetBytesThatAppearedAfterPreparation() {
        withFixture(f -> {
            var pending=f.pending(0);var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);var before=f.mapping();long version=f.version();
            SystemCollections.CACHE_JOIN_FACTS.on(f.cache).insertOne(new Document("_id",new Document("placement",new Document("scope",MongoPipelinePreparationFacts.scope(pending.request().targetScope())))).append("untracked",true));
            assertCode(() -> f.configuration.rebuilds().complete(receipt),IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertThat(f.mapping()).isEqualTo(before);assertThat(f.version()).isEqualTo(version);
        });
    }

    @Test void saturationAtBeginReservesBothOriginalMappingWrites() {
        withFixture(f -> {
            SystemCollections.OPERATOR_GENERATIONS.on(f.config).updateOne(new Document("_id","flow"),new Document("$set",new Document("noteWriteFence",Long.MAX_VALUE-2)));
            var before=f.mapping();long version=f.version();var request=f.request(0);
            assertCode(() -> f.configuration.rebuilds().begin(request),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.mapping()).isEqualTo(before);assertThat(f.version()).isEqualTo(version);
        });
    }
    @Test void saturationAtBeginReservesTheCompletionIntentIncrement() {
        withFixture(f -> {
            SystemCollections.PIPELINE_INTENTS.on(f.config).updateOne(new Document("_id","flow"),new Document("$set",new Document("intentRevision",Long.MAX_VALUE-1)));
            var before=f.mapping();long version=f.version();var request=f.request(0);
            assertCode(() -> f.configuration.rebuilds().begin(request),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.mapping()).isEqualTo(before);assertThat(f.version()).isEqualTo(version);
        });
    }
    @Test void saturationAtBeginReservesTwoConfigurationVersionAdvances() {
        withFixture(f -> {
            SystemCollections.CONFIGURATION_VERSION.on(f.config).updateOne(new Document("_id","configuration"),new Document("$set",new Document("revision",Long.MAX_VALUE-2)));
            var before=f.mapping();long version=f.version();var request=f.request(0);
            assertCode(() -> f.configuration.rebuilds().begin(request),IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.mapping()).isEqualTo(before);assertThat(f.version()).isEqualTo(version);
        });
    }
    @Test void saturationNeverPublishesAnUnreadableConfigurationVersion() {
        withFixture(f -> {
            SystemCollections.CONFIGURATION_VERSION.on(f.config).updateOne(new Document("_id","configuration"),new Document("$set",new Document("revision",Long.MAX_VALUE-1)));
            var source=new SourceResource("unrelated",null,"mongodb",Map.of("uri","mongodb://other.invalid/db"),null,null,null,null);
            assertCode(() -> f.configuration.artifacts().create(source),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.configuration.artifacts().get(source.id())).isEmpty();assertThat(f.version()).isEqualTo(Long.MAX_VALUE-1);
        });
    }

    @Test void nonemptyDirectSourceInputsAreSeededOnlyInTheConfirmedSuccessor() {
        withFixture(f -> {
            var source=new SourceResource("input",null,"mongodb",Map.of("uri","mongodb://input.invalid/db","password","secret-sentinel"),io.tapstate.core.model.SourceMode.CDC,
                    List.of(io.tapstate.core.model.TableRef.literal("orders")),null,null);
            var pipeline=new io.tapstate.core.model.PipelineResource("flow",null,List.of(io.tapstate.core.model.SourceRef.spec(source.id(),false)),null,null,null,null,null);
            f.configuration.artifacts().saveAll(List.of(source,pipeline));var held=f.configuration.intents().load("flow").orElseThrow();
            f.configuration.intents().compareAndSet(f.scope,held.intentRevision(),new DesiredState("flow",PipelineState.RUNNING,CanonicalHash.of(pipeline),false));
            var current=f.configuration.intents().load("flow").orElseThrow();
            var target=new PipelineIntentScope(f.scope.clusterId(),f.scope.bindingVersion(),f.scope.cacheIncarnation(),"flow",2);
            var inputs=PipelinePreparationInputs.of(target,pipeline,Map.of(source.id(),source),Map.of(source.id(),java.util.Set.of("orders")));
            var claim=f.domain.workloadClaims(f.business).readPipeline(f.scope).orElseThrow().claim();
            var request=new PipelineRebuildRequest("direct-rebuild",current,WorkloadClaimFence.from(claim),0,inputs);
            var pending=f.configuration.rebuilds().begin(request);var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);
            assertThat(inputs.directChannels()).hasSize(1);
            assertThat(SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.cache).find().first())
                    .containsEntry("_id",MongoDirectChannelRecords.id(inputs.directChannels().getFirst().context()));
            assertThat(SystemCollections.DIRECT_CHANNEL_OFFSETS.on(f.cache).find().first().toJson()).doesNotContain("secret-sentinel","input.invalid");
            assertThat(f.configuration.rebuilds().complete(receipt).scope()).isEqualTo(request.targetScope());
            f.domain.pipelinePreparations(f.business,ACTIVE).verifySources(request.targetScope(),inputs);
        });
    }

    @Test void rebuildingADetachesOnlyAAndAllowsBToKeepAdvancingItsSharedConsumer() {
        withFixture(f -> {
            f.initial("independent");var aFence=f.advance("flow");var bFence=f.advance("independent");
            var initial=CaptureHistoryFixture.expired(f.cache,f.business,"capture","shared",OWNER,1,TTL);
            CaptureHistoryFixture.shared(f.cache,f.business,initial,"shared","retention");
            var capture=WorkloadClaimFence.from(f.domain.workloadClaims(f.business).acquire(new WorkloadClaimKey(f.scope.clusterId(),WorkloadClaimType.CAPTURE,"capture"),OWNER,1,TTL).claim());
            var srs=f.domain.srs(f.business,ACTIVE);srs.chains().prepare("shared","retention",capture);
            var a=new SrsConsumerContext(new SrsConsumerKey("shared","flow","input"),1);var b=new SrsConsumerContext(new SrsConsumerKey("shared","independent","input"),1);
            srs.consumers().attach(a,0,java.util.Set.of("orders"),aFence).orElseThrow();srs.consumers().attach(b,0,java.util.Set.of("orders"),bFence).orElseThrow();
            var pending=f.pending(0);var before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(pending,f.node,TTL,AT),IoError.PIPELINE_STATE_REBUILD_REQUIRED);assertThat(f.snapshot()).isEqualTo(before);
            srs.consumers().detach(a,1,aFence).orElseThrow();var receipt=f.rebuilds().prepare(pending,f.node,TTL,AT);
            assertThat(srs.consumers().startReadingAfter(b,1,0,"orders",0,bFence)).isTrue();
            assertThat(srs.consumers().advanceReadSequence(b,1,"orders",12,bFence)).isTrue();var bProgress=srs.consumers().progress(b);
            var chain=SystemCollections.SRS_CHAINS.on(f.cache).find(new Document("_id","shared")).first();
            f.configuration.rebuilds().complete(receipt);
            assertThat(srs.consumers().progress(b)).isEqualTo(bProgress);assertThat(SystemCollections.SRS_CHAINS.on(f.cache).find(new Document("_id","shared")).first()).isEqualTo(chain);
            assertThat(chain).containsEntry("consumerCount",1L);assertThat(srs.consumers().attached("shared").consumers()).hasSize(1);
        });
    }

    @Test void lossAfterTheActualCacheCheckpointWriteRollsBackTheWholeReplacement() {
        withFixture(f -> {
            var models = f.domain.pipelineDerivedSchemas(f.scope, f.advance("flow"), ACTIVE);
            models.record("flow", "source.orders", Map.of("id", "bigint"), "statement", "sources", "derivation");
            models.pin("flow", "source.orders", 0);
            var pending=f.pending(0);var before=f.snapshot();
            try(var witness=new MongoCommandWitness(CACHE.getReplicaSetUrl(),f.cache.getName(),"update","pipeline_state")) {
                var store=new MongoCachePipelineRebuilds(witness.client,witness.database,f.business,witness::refuseAfterCommand,MongoRateHistoryStore.DEFAULT_RETENTION);
                assertCode(() -> store.prepare(pending,f.node,TTL,AT),IoError.CACHE_BUSINESS_FENCED);witness.requireCommand();
            }
            assertThat(f.snapshot()).isEqualTo(before);f.configuration.rebuilds().requirePending(pending);
        });
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"pipelineRebuild","pipelineRebuild.issuerNode.resourceId","pipelineRebuild.unexpected","pipelineRebuild.checkpointEpoch","pipelineRebuild.runtimeWriteFence"})
    void anotherFullRejectsDamagedPreviousTransitionFacts(String path) {
        withFixture(f -> {
            var first=f.pending(0);f.configuration.rebuilds().complete(f.rebuilds().prepare(first,f.node,TTL,AT));
            var next=f.pending(1);
            f.claims().updateOne(new Document("_id.resourceId","flow"),new Document("$set",new Document(path,path.endsWith("checkpointEpoch")||path.endsWith("runtimeWriteFence") ? 100L : "damaged")));
            var before=f.snapshot();
            assertCode(() -> f.rebuilds().prepare(next,f.node,TTL,AT),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void anotherFullPreservesTheCurrentTopologyAfterALegalClaimAcquire() {
        withFixture(f -> {
            f.domain.workloadClaims(f.business).acquirePipeline(f.scope,OWNER,7,TTL);
            var first=f.pending(0);f.configuration.rebuilds().complete(f.rebuilds().prepare(first,f.node,TTL,AT));
            var acquired=f.domain.workloadClaims(f.business).acquirePipeline(first.request().targetScope(),OWNER,0,TTL);
            assertThat(acquired.acquired()).isTrue();assertThat(acquired.claim().topologyRevision()).isZero();
            var next=f.pending(1);var receipt=f.rebuilds().prepare(next,f.node,TTL,AT);
            assertThat(receipt.claim().topologyRevision()).isZero();assertThat(receipt.claim().claimGeneration()).isEqualTo(acquired.claim().claimGeneration()+1);
        });
    }

    @Test void fabricatedPreparationIsRefusedBeforeAnyAccessorRuns() {
        withFixture(f -> {
            var calls=new AtomicInteger(); PipelineRebuildPreparation fake=new PipelineRebuildPreparation() {
                public PipelineRebuildRequest request(){calls.incrementAndGet();return null;}
                public String preparationId(){calls.incrementAndGet();return null;}
                public String previousPreparationId(){calls.incrementAndGet();return null;}
                public String confirmationHash(){calls.incrementAndGet();return null;}
            };
            var before=f.snapshot();assertCode(() -> f.rebuilds().prepare(fake,f.node,TTL,AT),IoError.PIPELINE_INTENT_FENCED);
            assertThat(calls).hasValue(0);assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    private static void assertCode(Runnable action,io.tapstate.core.common.TapstateErrorCode expected) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(expected));
    }
    private static void withFixture(Scenario scenario) { withFixture(false, scenario); }
    private static void withFixture(boolean shared, Scenario scenario) {
        String suffix=UUID.randomUUID().toString().replace("-","");String configName="rebuild_cfg_"+suffix,cacheName="rebuild_cache_"+suffix;
        try(var configClient=MongoClients.create(CONFIG.getReplicaSetUrl());var cacheClient=MongoClients.create(CACHE.getReplicaSetUrl())) {
            var cfg=configClient.getDatabase(configName);var cache=cacheClient.getDatabase(cacheName);
            var settings=new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,CACHE.getReplicaSetUrl(cacheName),cacheName,null);
            try(var configuration=MongoConfigurationBootstrap.prepare(settings,"cluster-a","installation-a","setup-a",1);
                var domain=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"rebuild-test")) {
                var master=new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP),null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                configuration.bindings().commit(new StorageSetupScope("cluster-a","installation-a","setup-a",1),StorageBinding.Mode.OP,connection,master);
                var business=configuration.bindings().cachePreparation().orElseThrow().businessScope();
                domain.prepareAuth(new CacheAuthScope(business.clusterId(),business.bindingVersion(),StorageBinding.Mode.OP,business.cacheIncarnation(),1),"auth-a",ACTIVE);
                try(var session=cacheClient.startSession()) {
                    session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.LOCAL).build());
                    new MongoCacheMetadataStructure(cache,MongoRateHistoryStore.DEFAULT_RETENTION).prepareBusinessInTransaction(session,ACTIVE);session.commitTransaction();
                }
                SystemCollections.CACHE_IDENTITY.on(cache).updateOne(new Document("_id","cluster"),new Document("$set",new Document("businessPhase","READY").append("businessPreparationId","business-a").append("claimAdmissionFence",0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(cache).insertOne(new Document("_id","business").append("installedVersion",1).append("phase","READY").append("epoch",1L).append("operationId","business-a"));
                var node=WorkloadClaimFence.from(domain.workloadClaims(business).acquire(new WorkloadClaimKey("cluster-a",WorkloadClaimType.NODE_SESSION,OWNER.nodeId()),OWNER,0,TTL).claim());
                var scope=new PipelineIntentScope(business.clusterId(),business.bindingVersion(),business.cacheIncarnation(),"flow",1);
                var fixture=new Fixture(configuration,cfg,cacheClient,cache,domain,connection,business,scope,node);
                if (shared) fixture.initialSharedSource(); else fixture.initial("flow");
                scenario.run(fixture);
            } finally {cfg.drop();cache.drop();}
        } catch(RuntimeException|Error failure){throw failure;}catch(Exception failure){throw new AssertionError(failure);}
    }
    private record Fixture(MongoConfigurationDomain configuration,MongoDatabase config,MongoClient cacheClient,MongoDatabase cache,MongoCacheDomain domain,
            StorageBinding.Connection connection,CacheBusinessScope business,PipelineIntentScope scope,WorkloadClaimFence node) {
        void initialSharedSource() { initialSharedSource("flow"); }
        void initialSharedSource(String id) {
            var source = new SourceResource("input", null, "mongodb", Map.of("uri", "mongodb://input.invalid/db"),
                    io.tapstate.core.model.SourceMode.CDC, List.of(io.tapstate.core.model.TableRef.literal("orders"),
                    io.tapstate.core.model.TableRef.literal("quiet")), null, null);
            var pipeline = new io.tapstate.core.model.PipelineResource(id, null,
                    List.of(io.tapstate.core.model.SourceRef.spec("input", true)), null, null, null, null, null);
            configuration.artifacts().saveAll(List.of(source, pipeline));
            var initial = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), id, 1);
            configuration.intents().compareAndSet(initial, 0, new DesiredState(id, PipelineState.RUNNING, CanonicalHash.of(pipeline), false));
            var creation = ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(initial, node, sharedInputs(initial))).creation();
            var cache = domain.pipelinePreparations(business, ACTIVE);
            configuration.preparations().complete(cache.seed(cache.reserveFirst(creation, TTL), configuration.preparations(), AT));
        }
        PipelinePreparationInputs sharedInputs(PipelineIntentScope original) {
            var source = (SourceResource) configuration.artifacts().get("input").orElseThrow();
            var pipeline = (io.tapstate.core.model.PipelineResource) configuration.artifacts().get(original.pipelineId()).orElseThrow();
            return PipelinePreparationInputs.of(original, pipeline, Map.of(source.id(), source),
                    Map.of(source.id(), java.util.Set.of("orders", "quiet")));
        }
        void initial(String id) {
            var pipeline=new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: "+id+"\nsource: []\n");configuration.artifacts().create(pipeline);
            var initialScope=new PipelineIntentScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation(),id,1);
            configuration.intents().compareAndSet(initialScope,0,new DesiredState(id,PipelineState.RUNNING,CanonicalHash.of(pipeline),false));
            var creation=((FirstPipelineCreationResult.Issued)configuration.preparations().issueFirstCreate(initialScope,node)).creation();
            var reservation=domain.pipelinePreparations(business,ACTIVE).reserveFirst(creation,TTL);
            configuration.preparations().complete(domain.pipelinePreparations(business,ACTIVE).seed(reservation,configuration.preparations(),AT));
        }
        PipelineRebuildPreparation pending(long epoch) { return configuration.rebuilds().begin(request(epoch)); }
        PipelineRebuildRequest request(long epoch) {
            var pipeline=(io.tapstate.core.model.PipelineResource)configuration.artifacts().get("flow").orElseThrow();
            var current=configuration.intents().load("flow").orElseThrow();
            var target=new PipelineIntentScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation(),scope.pipelineId(),current.scope().stateGeneration()+1);
            var claim=domain.workloadClaims(business).read(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId())).orElseThrow().claim();
            return new PipelineRebuildRequest("original-rebuild-"+current.scope().stateGeneration(),current,WorkloadClaimFence.from(claim),epoch,
                    PipelinePreparationInputs.of(target,pipeline,Map.of(),Map.of()));
        }
        long version(){return SystemCollections.CONFIGURATION_VERSION.on(config).find().first().getLong("revision");}
        Document mapping(){return SystemCollections.OPERATOR_GENERATIONS.on(config).find(new Document("_id","flow")).first();}
        WorkloadClaimFence advance(String id){
            var scope=new PipelineIntentScope(business.clusterId(),business.bindingVersion(),business.cacheIncarnation(),id,1);
            var store=domain.workloadClaims(business);var original=store.acquirePipeline(scope,OWNER,1,TTL).claim();
            return WorkloadClaimFence.from(store.advanceExecution(original,1,java.util.Set.of(OWNER.nodeId())).orElseThrow());
        }
        PipelineCacheRebuilds rebuilds(){return domain.pipelineRebuilds(business,ACTIVE);}
        com.mongodb.client.MongoCollection<Document> claims(){return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache);}
        com.mongodb.client.MongoCollection<Document> states(){return SystemCollections.CACHE_PIPELINE_STATE.on(cache);}
        Document claim(){return claims().find(new Document("_id.resourceId","flow")).first();}
        Document state(){return states().find(new Document("_id","flow")).first();}
        Map<String,List<Document>> snapshot(){var result=new java.util.TreeMap<String,List<Document>>();for(var table:SystemCollections.values())if(table.database()==SystemCollections.Database.CACHE&&table!=SystemCollections.CACHE_CONNECTOR_ARTIFACTS)
            result.put(table.name(),table.on(cache).find().sort(new Document("_id",1)).into(new ArrayList<>()));return result;}
    }
    @FunctionalInterface private interface Scenario{void run(Fixture fixture)throws Exception;}
}
