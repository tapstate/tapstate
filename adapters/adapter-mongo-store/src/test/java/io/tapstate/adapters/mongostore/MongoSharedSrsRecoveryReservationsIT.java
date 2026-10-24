package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.StorageRecoveryCommands.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real CONFIG/CACHE control transactions only; no assertion here proves registered source or target recovery. */
@RequiresDocker
class MongoSharedSrsRecoveryReservationsIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Duration TTL = Duration.ofMinutes(2);
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster-a", "install-a", "setup-a", 1);

    @Test void reservationPublishesOneFixedConfigScopeWithoutMutatingCacheOrUnrelatedPipeline() {
        withFixture(f -> {
            var confirmed = f.confirmed();
            var cache = rows(f.cache); var notes = notes(f.config); var unrelated = f.mapping("c");
            var oldA = f.mapping("a"); var oldB = f.mapping("b"); var intentC = f.intentRow("c");
            long version = f.configuration.configurationRevision();
            var reservation = f.configuration.sharedRecoveryPreparations().reserve(confirmed);
            reservation.requireCurrent();
            assertThat(reservation.plan().phase()).isEqualTo(Phase.RUNNING);
            assertThat(reservation.plan().operationId()).isEqualTo(confirmed.operationId());
            assertThat(reservation.replacement().preparationId()).isEqualTo(confirmed.operationId());
            assertThat(reservation.replacement().chainId()).isEqualTo(MongoCapturePreparationFacts.replacementChain(f.previous, confirmed.operationId()));
            assertThat(f.configuration.configurationRevision()).isEqualTo(version + 1);
            for (String id : List.of("a", "b")) {
                var after = f.mapping(id); var marker = after.get("sharedRecovery", Document.class);
                assertThat(marker.getString("operationId")).isEqualTo(confirmed.operationId());
                var previous=id.equals("a")?oldA:oldB;
                assertThat(after.getLong("noteWriteFence")).isEqualTo(previous.getLong("noteWriteFence")+1);
                after.remove("sharedRecovery");after.put("noteWriteFence",previous.getLong("noteWriteFence"));assertThat(after).isEqualTo(previous);
                var intent = f.configuration.intents().loadForActuation(id).orElseThrow();
                assertThat(intent.scope()).isEqualTo(f.requests.get(id).pipeline());
                assertThat(intent.desired().targetState()).isEqualTo(PipelineState.STOPPED);
                assertThat(intent.desired().purgeState()).isFalse();
            }
            assertThat(f.mapping("c")).isEqualTo(unrelated); assertThat(f.intentRow("c")).isEqualTo(intentC);
            var recovery = SystemCollections.CAPTURE_PREPARATIONS.on(f.config).find().first().get("currentRecovery", Document.class);
            assertThat(recovery.getString("phase")).isEqualTo("RESERVED");
            var afterConfig = rows(f.config);
            var retried = f.configuration.sharedRecoveryPreparations().reserve(confirmed);
            assertThat(retried.plan()).isEqualTo(reservation.plan()); assertThat(retried.replacement()).isEqualTo(reservation.replacement());
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a")) {
                var pending = reopened.sharedRecoveryPreparations().pending(f.auth(), confirmed.operationId()).orElseThrow();
                pending.requireCurrent();
                assertThat(pending.plan()).isEqualTo(reservation.plan()); assertThat(pending.replacement()).isEqualTo(reservation.replacement());
            }
            assertThat(rows(f.config)).isEqualTo(afterConfig); assertThat(rows(f.cache)).isEqualTo(cache);
            assertThat(notes(f.config)).isEqualTo(notes);
        });
    }

    @Test void independentConfigClientsReserveTheSameConfirmedOperationOnce() {
        withFixture(f -> {
            var confirmed = f.confirmed(); long version = f.configuration.configurationRevision(); var cache = rows(f.cache);
            try (var other = MongoConfigurationDomain.open(f.settings, "cluster-a"); var threads = Executors.newFixedThreadPool(2)) {
                var start = new CountDownLatch(1);
                var one = threads.submit(() -> { start.await(); return f.configuration.sharedRecoveryPreparations().reserve(confirmed); });
                var two = threads.submit(() -> { start.await(); return other.sharedRecoveryPreparations().reserve(confirmed); });
                start.countDown();
                var first = one.get(20, TimeUnit.SECONDS); var second = two.get(20, TimeUnit.SECONDS);
                assertThat(second.plan()).isEqualTo(first.plan()); assertThat(second.replacement()).isEqualTo(first.replacement());
                first.requireCurrent(); second.requireCurrent();
            }
            assertThat(f.configuration.configurationRevision()).isEqualTo(version + 1);
            assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @ParameterizedTest @ValueSource(strings = {"unconfirmed", "version", "definition", "incomplete", "rebuilding","missing-origin"})
    void staleOrUnpreparedOriginalControlsRefuseWithoutPartialPublication(String damage) {
        withFixture(f -> {
            var planned = f.planned(); var original = damage.equals("unconfirmed") ? planned : f.configuration.recoveryCommands().confirm(planned);
            switch (damage) {
                case "version" -> f.configuration.artifacts().create(new SourceResource("unrelated", null, "mysql", Map.of("host", "other"), null, null, null, null));
                case "definition" -> f.changeSource();
                case "incomplete" -> SystemCollections.OPERATOR_GENERATIONS.on(f.config).updateOne(new Document("_id", "a"),
                        new Document("$set", new Document("cachePreparation.phase", "ISSUED")));
                case "rebuilding" -> SystemCollections.OPERATOR_GENERATIONS.on(f.config).updateOne(new Document("_id", "a"),
                        new Document("$set", new Document("phase", "REBUILDING")));
                case "missing-origin" -> SystemCollections.PDK_NOTE_ORIGINS.on(f.config).deleteMany(new Document());
                default -> { }
            }
            var config = rows(f.config); var cache = rows(f.cache); long version = f.configuration.configurationRevision();
            assertThatThrownBy(() -> f.configuration.sharedRecoveryPreparations().reserve(original)).isInstanceOf(TapstateException.class);
            assertThat(rows(f.config)).isEqualTo(config); assertThat(rows(f.cache)).isEqualTo(cache);
            assertThat(f.configuration.configurationRevision()).isEqualTo(version);
        });
    }

    @ParameterizedTest @ValueSource(strings={"metadata","identity"})
    void aRunningReservationKeepsItsOriginalScopeAfterAuthoringChangesAndCannotAuthorizeOrdinaryStart(String change) {
        withFixture(f -> {
            var confirmed = f.confirmed(); var reservation = f.configuration.sharedRecoveryPreparations().reserve(confirmed);
            if(change.equals("metadata"))f.changeSource();
            else {
                var source=(SourceResource)f.configuration.artifacts().get("input").orElseThrow();
                f.configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),Map.of("host","different-server"),
                        source.mode(),source.tables(),source.srs(),source.experimental()));
            }
            var config = rows(f.config); var cache = rows(f.cache);
            var retried = f.configuration.sharedRecoveryPreparations().reserve(confirmed);
            var pending = f.configuration.sharedRecoveryPreparations().pending(f.auth(), confirmed.operationId()).orElseThrow();
            retried.requireCurrent(); pending.requireCurrent();
            assertThat(retried.plan()).isEqualTo(reservation.plan()); assertThat(pending.replacement()).isEqualTo(reservation.replacement());
            assertThatThrownBy(() -> f.configuration.recoveryCommands().start(confirmed)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.configuration.recoveryCommands().complete(reservation.plan())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.configuration.capturePreparations().read(f.scope, f.previous.captureId())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.configuration.capturePreparations().issueFirstCreate(f.requests.get("a"))).isInstanceOf(TapstateException.class);
            var stopped = f.configuration.intents().loadForActuation("a").orElseThrow();
            assertThat(stopped.desired().targetState()).isEqualTo(PipelineState.STOPPED);
            assertThatThrownBy(() -> f.configuration.intents().compareAndSet(stopped.scope(), stopped.intentRevision(),
                    new DesiredState("a", PipelineState.RUNNING, stopped.desired().revision(), false))).isInstanceOf(TapstateException.class);
            assertThat(rows(f.config)).isEqualTo(config); assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @ParameterizedTest @ValueSource(strings={"before-commit","after-commit"})
    void authorizationLossRollsBackOrResumesTheExactCommittedReservation(String cut) {
        withFixture(f -> {
            var confirmed=f.confirmed();var config=rows(f.config);var cache=rows(f.cache);long version=f.configuration.configurationRevision();
            var calls=new java.util.concurrent.atomic.AtomicInteger();int failAt=cut.equals("before-commit")?2:3;
            var guarded=f.configuration.sharedRecoveryPreparations().guardedBy(() -> {
                if(calls.incrementAndGet()==failAt)throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            });
            assertThatThrownBy(() -> guarded.reserve(confirmed)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            if(cut.equals("before-commit")) {
                assertThat(rows(f.config)).isEqualTo(config);assertThat(f.configuration.configurationRevision()).isEqualTo(version);
                assertThat(f.configuration.sharedRecoveryPreparations().pending(f.auth(),confirmed.operationId())).isEmpty();
            } else {
                var pending=f.configuration.sharedRecoveryPreparations().pending(f.auth(),confirmed.operationId()).orElseThrow();
                pending.requireCurrent();var committed=rows(f.config);
                assertThat(pending.plan().operationId()).isEqualTo(confirmed.operationId());
                assertThat(pending.replacement().preparationId()).isEqualTo(confirmed.operationId());
                assertThat(f.configuration.configurationRevision()).isEqualTo(version+1);
                assertThat(f.configuration.sharedRecoveryPreparations().reserve(confirmed).replacement()).isEqualTo(pending.replacement());
                assertThat(rows(f.config)).isEqualTo(committed);
            }
            assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @Test void replacementRetiresOnlyTheOriginalPhysicalControlAndReplaysWithoutAnotherWrite() {
        withFixture(f -> {
            f.endAffectedTenures();
            var confirmed=f.confirmed();var reservation=f.configuration.sharedRecoveryPreparations().reserve(confirmed);
            var config=rows(f.config);var cache=rows(f.cache);var oldRoot=f.chain(f.previous.chainId());
            var oldCapture=f.claim(f.previous.captureId(),WorkloadClaimType.CAPTURE);
            var oldA=f.claim("a",WorkloadClaimType.PIPELINE_ACTUATION);var oldB=f.claim("b",WorkloadClaimType.PIPELINE_ACTUATION);
            var unrelated=f.claim("c",WorkloadClaimType.PIPELINE_ACTUATION);
            var oldMembers=SystemCollections.SRS_CONSUMERS.on(f.cache).find().into(new ArrayList<>());
            var receipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);
            assertThat(receipt.outcome()).isEqualTo(SharedSrsRecoveryReceipt.Outcome.RETIRED);
            assertThat(receipt.preparation().plan()).isEqualTo(reservation.plan());
            assertThat(receipt.preparation().replacement()).isEqualTo(reservation.replacement());receipt.requireCurrent();
            var capture=f.claim(f.previous.captureId(),WorkloadClaimType.CAPTURE);
            assertThat(capture.getLong("claimGeneration")).isEqualTo(oldCapture.getLong("claimGeneration")+1);
            assertThat(capture.getLong("executionGeneration")).isEqualTo(oldCapture.getLong("executionGeneration"));
            assertThat(capture.get("_id")).isEqualTo(oldCapture.get("_id"));
            var annex=capture.get("capturePreparation",Document.class);
            assertThat(annex.getInteger("formatVersion")).isEqualTo(2);
            assertThat(annex.get("original")).isEqualTo(oldCapture.get("capturePreparation"));
            assertThat(annex.getString("physicalChain")).isEqualTo(reservation.replacement().chainId());
            assertThat(annex.getString("preparationId")).isEqualTo(confirmed.operationId());
            var retired=f.chain(f.previous.chainId());var control=retired.get("recoveryControl",Document.class);
            assertThat(control.getString("phase")).isEqualTo("RETIRED");
            assertThat(control.getString("operationId")).isEqualTo(confirmed.operationId());
            assertThat(control.getString("planHash")).isEqualTo(confirmed.planHash());
            assertThat(control.getString("replacementChain")).isEqualTo(reservation.replacement().chainId());
            assertThat(control.getString("controlHash")).matches("[0-9a-f]{64}");
            for(String field:List.of("checkpoint","checkpointAt","checkpointRevision","modelContextHeads","epoch","resumeOverride"))
                assertThat(retired.get(field)).isEqualTo(oldRoot.get(field));
            assertThat(retired.getLong("consumerCount")).isZero();
            for(var before:oldMembers) {
                var after=SystemCollections.SRS_CONSUMERS.on(f.cache).find(new Document("_id",before.get("_id"))).first();
                assertThat(after.getBoolean("detached")).isTrue();assertThat(after.getLong("revision")).isEqualTo(before.getLong("revision")+1);
                assertThat(after.getLong("stateGeneration")).isEqualTo(before.getLong("stateGeneration"));
                assertThat(after.getList("tables",String.class)).isEmpty();
            }
            assertThat(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.cache).countDocuments()).isZero();
            var fresh=f.chain(reservation.replacement().chainId());
            assertThat(fresh.getInteger("formatVersion")).isEqualTo(1);assertThat(fresh.getLong("epoch")).isZero();
            assertThat(fresh.get("checkpoint")).isNull();assertThat(fresh.getLong("consumerCount")).isZero();
            assertThat(fresh.getString("captureId")).isEqualTo(f.previous.captureId());
            assertThat(fresh).doesNotContainKey("recoveryControl");
            assertPipelineClaimUnchangedExceptEnd(f.claim("a",WorkloadClaimType.PIPELINE_ACTUATION),oldA);
            assertPipelineClaimUnchangedExceptEnd(f.claim("b",WorkloadClaimType.PIPELINE_ACTUATION),oldB);
            assertThat(f.claim("c",WorkloadClaimType.PIPELINE_ACTUATION)).isEqualTo(unrelated);
            for(var table:List.of(SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,
                    SystemCollections.SRS_EVENTS,SystemCollections.SRS_LOG_BOUNDS))
                assertThat(table.on(f.cache).find().sort(new Document("_id",1)).into(new ArrayList<>())).isEqualTo(cache.get(table.collectionName()));
            assertThat(rows(f.config)).isEqualTo(config);
            assertThatThrownBy(() -> f.configuration.capturePreparations().issueFirstCreate(f.requests.get("a"))).isInstanceOf(TapstateException.class);
            var stopped=f.configuration.intents().loadForActuation("a").orElseThrow();
            assertThatThrownBy(() -> f.configuration.intents().compareAndSet(stopped.scope(),stopped.intentRevision(),
                    new DesiredState("a",PipelineState.RUNNING,stopped.desired().revision(),false))).isInstanceOf(TapstateException.class);
            var committed=rows(f.cache);var replay=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);
            assertThat(replay.outcome()).isEqualTo(receipt.outcome());assertThat(replay.preparation().replacement()).isEqualTo(reservation.replacement());
            replay.requireCurrent();assertThat(rows(f.cache)).isEqualTo(committed);assertThat(rows(f.config)).isEqualTo(config);
        });
    }

    @ParameterizedTest @ValueSource(strings={"pipeline","capture"})
    void replacementRefusesALiveOriginalTenureWithoutPartialCacheMutation(String live) {
        withFixture(f -> {
            if(live.equals("capture")) {
                f.endAffectedTenures();
                f.domain.workloadClaims(f.scope).acquire(new WorkloadClaimKey(f.scope.clusterId(),WorkloadClaimType.CAPTURE,f.previous.captureId()),OWNER,1,TTL);
            }
            var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            assertReplacementRefusesWithoutMutation(f,reservation);
        });
    }

    @Test void aForgedPublicPreparationIsRefusedBeforeItsAccessorsCanRun() {
        withFixture(f -> {
            var forged=new SharedSrsRecoveryPreparation() {
                public Plan plan(){throw new AssertionError("untrusted plan accessor invoked");}
                public CapturePreparation replacement(){throw new AssertionError("untrusted replacement accessor invoked");}
                public void requireCurrent(){throw new AssertionError("untrusted guard invoked");}
            };
            assertReplacementRefusesWithoutMutation(f,forged);
        });
    }

    @ParameterizedTest @ValueSource(strings={"root","member"})
    void changedActualControlAfterPreviewCannotApplyTheOriginalReplacement(String changed) {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var claims=f.domain.workloadClaims(f.scope);
            if(changed.equals("root")) {
                var acquired=claims.acquire(new WorkloadClaimKey(f.scope.clusterId(),WorkloadClaimType.CAPTURE,f.previous.captureId()),OWNER,1,TTL).claim();
                f.domain.srs(f.scope,ACTIVE).chains().requestTables(f.previous.chainId(),Set.of("orders"),WorkloadClaimFence.from(acquired));
                assertThat(claims.release(acquired)).isTrue();
            } else {
                var acquired=claims.acquirePipeline(f.requests.get("a").pipeline(),OWNER,1,TTL).claim();
                var context=new SrsConsumerContext(new SrsConsumerKey(f.previous.chainId(),"a","input"),1);
                f.domain.srs(f.scope,ACTIVE).consumers().attach(context,1,Set.of("orders"),WorkloadClaimFence.from(acquired)).orElseThrow();
                assertThat(claims.release(acquired)).isTrue();
            }
            assertReplacementRefusesWithoutMutation(f,reservation);
        });
    }

    @ParameterizedTest @ValueSource(strings={"root","model","context","member","offset","event","bounds"})
    void anOccupiedReplacementNamespaceIsRetainedAndNeverAdopted(String occupied) {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            // Deliberately corrupt only the proposed target; this supplies no original creation authority.
            String chain=reservation.replacement().chainId(),ring="srs."+chain+".orders";
            var table=switch(occupied) {
                case "root" -> SystemCollections.SRS_CHAINS;case "model" -> SystemCollections.SRS_MODELS;
                case "context" -> SystemCollections.SRS_MODEL_CONTEXTS;case "member" -> SystemCollections.SRS_CONSUMERS;
                case "offset" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS;case "event" -> SystemCollections.SRS_EVENTS;
                case "bounds" -> SystemCollections.SRS_LOG_BOUNDS;default -> throw new AssertionError(occupied);
            };
            Object id=switch(occupied) {
                case "root" -> chain;case "event" -> new Document("ring",ring).append("seq",0L);
                case "bounds" -> "srs-log-bounds:"+ring;default -> new Document("chain",chain);
            };
            table.on(f.cache).insertOne(new Document("_id",id).append("unknown",true));
            assertReplacementRefusesWithoutMutation(f,reservation);
        });
    }

    @Test void retainedCheckpointControlsCanReplaceLostBodiesWithoutInventingANewCheckpoint() {
        withFixture(f -> {
            // This is actual adapter history, without claiming a connector callback or a target write.
            var originalCapture=f.publishStoreHistory();f.endAffectedTenures();
            var originalRoot=f.chain(f.previous.chainId());
            for(var table:List.of(SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,SystemCollections.SRS_EVENTS))
                assertThat(table.on(f.cache).deleteMany(new Document()).getDeletedCount()).isPositive();
            var srs=f.domain.srs(f.scope,ACTIVE);
            assertThatThrownBy(() -> srs.chains().load(f.previous.chainId())).isInstanceOf(TapstateException.class);
            var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());var config=rows(f.config);
            var receipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);receipt.requireCurrent();
            assertThat(receipt.outcome()).isEqualTo(SharedSrsRecoveryReceipt.Outcome.RETIRED);
            var retired=f.chain(f.previous.chainId());
            for(String field:List.of("checkpoint","checkpointRevision","checkpointAt","modelContextHeads","epoch"))
                assertThat(retired.get(field)).isEqualTo(originalRoot.get(field));
            var fresh=f.chain(reservation.replacement().chainId());
            assertThat(fresh.get("checkpoint")).isNull();assertThat(fresh.getLong("checkpointRevision")).isZero();
            assertThat(fresh.getLong("epoch")).isZero();assertThat(fresh.getList("modelContextHeads",Document.class)).isEmpty();
            for(var table:List.of(SystemCollections.SRS_MODELS,SystemCollections.SRS_MODEL_CONTEXTS,SystemCollections.SRS_EVENTS))
                assertThat(table.on(f.cache).countDocuments()).isZero();
            String ring="srs."+f.previous.chainId()+".orders";
            assertThatThrownBy(() -> srs.chains().requestTables(f.previous.chainId(),Set.of("orders"),originalCapture)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> srs.log().store(ring,1,new SrsLogRecord("t1",io.tapstate.core.event.Op.INSERT,
                    1,null,Map.of("id",2),0,originalCapture,1))).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> srs.log().load(ring,0)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.domain.capturePreparations(f.scope,ACTIVE).load(f.previous)).isInstanceOf(TapstateException.class);
            var context=new SrsConsumerContext(new SrsConsumerKey(f.previous.chainId(),"a","input"),1);
            assertThatThrownBy(() -> srs.consumers().advanceReadSequence(context,1,"orders",0,f.requests.get("a").originalPipeline()))
                    .isInstanceOf(TapstateException.class);
            var intent=f.configuration.intents().loadForActuation("a").orElseThrow();
            assertThatThrownBy(() -> f.configuration.intents().compareAndSet(intent.scope(),intent.intentRevision(),
                    new DesiredState("a",PipelineState.STOPPED,intent.desired().revision(),true))).isInstanceOf(TapstateException.class);
            assertThat(rows(f.config)).isEqualTo(config);
        });
    }

    @ParameterizedTest @ValueSource(strings={"before-commit","after-commit"})
    void cacheAuthorizationLossRollsBackOrReplaysTheSameCommittedPhysicalReplacement(String cut) {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var config=rows(f.config);var cache=rows(f.cache);var calls=new java.util.concurrent.atomic.AtomicInteger();
            int failAt=cut.equals("before-commit")?3:4;
            var guarded=f.domain.sharedSrsRecoveries(f.scope,() -> {
                if(calls.incrementAndGet()==failAt)throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            });
            assertThatThrownBy(() -> guarded.replace(reservation,f.node)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            if(cut.equals("before-commit")) {
                assertThat(rows(f.cache)).isEqualTo(cache);assertThat(f.chain(reservation.replacement().chainId())).isNull();
            } else {
                assertThat(f.chain(f.previous.chainId()).get("recoveryControl",Document.class).getString("phase")).isEqualTo("RETIRED");
                var committed=rows(f.cache);
                var replay=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);replay.requireCurrent();
                assertThat(replay.outcome()).isEqualTo(SharedSrsRecoveryReceipt.Outcome.RETIRED);
                assertThat(replay.preparation().replacement()).isEqualTo(reservation.replacement());
                assertThat(rows(f.cache)).isEqualTo(committed);
            }
            assertThat(rows(f.config)).isEqualTo(config);
        });
    }

    @Test void reopenedConfigAndCacheClientsRecognizeTheSameReceiptBeforeConfigCompletion() {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node).requireCurrent();
            var config=rows(f.config);var cache=rows(f.cache);
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,CACHE.getReplicaSetUrl(f.cache.getName()),f.cache.getName(),null);
            try(var reopenedConfig=MongoConfigurationDomain.open(f.settings,"cluster-a");
                    var reopenedCache=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"shared-reservation-reopened-test")) {
                var pending=reopenedConfig.sharedRecoveryPreparations().pending(f.auth(),reservation.plan().operationId()).orElseThrow();
                var receipt=reopenedCache.sharedSrsRecoveries(f.scope,ACTIVE).replace(pending,f.node);receipt.requireCurrent();
                assertThat(receipt.outcome()).isEqualTo(SharedSrsRecoveryReceipt.Outcome.RETIRED);
                assertThat(receipt.preparation().plan()).isEqualTo(reservation.plan());
                assertThat(receipt.preparation().replacement()).isEqualTo(reservation.replacement());
                assertThat(pending.plan().phase()).isEqualTo(Phase.RUNNING);
            }
            assertThat(rows(f.config)).isEqualTo(config);assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @ParameterizedTest @ValueSource(strings={"new-root-fence-type","capture-write-counter","new-model","new-event","new-root-scope","old-offset"})
    void anAppliedReceiptCannotAdoptDamagedOrUnexpectedPoststageControls(String damage) {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var offset=SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.cache).find().first();
            var receipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);receipt.requireCurrent();
            String target=reservation.replacement().chainId();var roots=SystemCollections.SRS_CHAINS.on(f.cache);
            switch(damage) {
                case "new-root-fence-type" -> roots.updateOne(new Document("_id",target),new Document("$set",new Document("captureFence","unknown")));
                case "capture-write-counter" -> SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache).updateOne(
                        new Document("_id",MongoWorkloadClaimStore.id(reservation.plan().manifest().sharedSrs().captureFence().key())),
                        new Document("$set",new Document("fencedChainWrites","unknown")));
                case "new-model" -> SystemCollections.SRS_MODELS.on(f.cache).insertOne(new Document("_id",new Document("chain",target).append("unknown",true)));
                case "new-event" -> SystemCollections.SRS_EVENTS.on(f.cache).insertOne(new Document("_id",new Document("ring","srs."+target+".orders").append("seq",0L)).append("unknown",true));
                case "new-root-scope" -> roots.updateOne(new Document("_id",target),new Document("$set",new Document("cacheIncarnation","different")));
                case "old-offset" -> SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(f.cache).insertOne(offset);
                default -> throw new AssertionError(damage);
            }
            var cache=rows(f.cache);var config=rows(f.config);
            assertThatThrownBy(receipt::requireCurrent).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node)).isInstanceOf(TapstateException.class);
            assertThat(rows(f.cache)).isEqualTo(cache);assertThat(rows(f.config)).isEqualTo(config);
        });
    }

    @Test void completionPublishesReadyAndCompleteAtomicallyWithoutChangingCacheOrPipelineScopes() {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var receipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);
            var cache=rows(f.cache);var oldA=f.mapping("a");var oldB=f.mapping("b");var oldC=f.mapping("c");
            var intentA=f.intentRow("a");var intentB=f.intentRow("b");var notes=notes(f.config);
            long version=f.configuration.configurationRevision();
            var completed=f.configuration.sharedRecoveryPreparations().complete(receipt);
            assertThat(completed.phase()).isEqualTo(Phase.COMPLETE);
            assertThat(completed.operationId()).isEqualTo(reservation.plan().operationId());
            assertThat(completed.manifest()).isEqualTo(reservation.plan().manifest());
            assertThat(completed.planHash()).isEqualTo(reservation.plan().planHash());
            assertThat(f.configuration.configurationRevision()).isEqualTo(version+1);
            assertThat(f.configuration.recoveryCommands().find(f.auth(),completed.operationId())).contains(completed);
            assertThat(f.configuration.capturePreparations().read(f.scope,f.previous.captureId())).contains(reservation.replacement());
            var mapping=SystemCollections.CAPTURE_PREPARATIONS.on(f.config).find().first().get("currentRecovery",Document.class);
            assertThat(mapping.getString("phase")).isEqualTo("READY");
            assertThat(f.mapping("a")).isEqualTo(oldA);assertThat(f.mapping("b")).isEqualTo(oldB);assertThat(f.mapping("c")).isEqualTo(oldC);
            assertThat(f.intentRow("a")).isEqualTo(intentA);assertThat(f.intentRow("b")).isEqualTo(intentB);
            assertThat(notes(f.config)).isEqualTo(notes);assertThat(rows(f.cache)).isEqualTo(cache);
            var config=rows(f.config);
            assertThat(f.configuration.sharedRecoveryPreparations().complete(receipt)).isEqualTo(completed);
            assertThat(rows(f.config)).isEqualTo(config);assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @ParameterizedTest @ValueSource(strings={"metadata","identity"})
    void completionUsesTheFixedCommittedScopeAcrossAuthoringChangesAndReplaysWithoutWrites(String change) {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var receipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);
            if(change.equals("metadata"))f.changeSource();else f.changeSourceIdentity();
            var cache=rows(f.cache);long version=f.configuration.configurationRevision();
            var completed=f.configuration.sharedRecoveryPreparations().complete(receipt);
            assertThat(completed.phase()).isEqualTo(Phase.COMPLETE);assertThat(completed.manifest()).isEqualTo(reservation.plan().manifest());
            assertThat(f.configuration.configurationRevision()).isEqualTo(version+1);
            if(change.equals("metadata"))f.changeSourceIdentity();else f.changeSource();
            var config=rows(f.config);
            assertThat(f.configuration.sharedRecoveryPreparations().complete(receipt)).isEqualTo(completed);
            assertThat(f.configuration.capturePreparations().read(f.scope,f.previous.captureId())).contains(reservation.replacement());
            assertThat(rows(f.config)).isEqualTo(config);assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @ParameterizedTest @ValueSource(strings={"hostile","cancelled"})
    void aPublicReceiptCannotInvokeAccessorsOrPublishCompletion(String kind) {
        withFixture(f -> {
            var calls=new java.util.concurrent.atomic.AtomicInteger();
            var fake=new SharedSrsRecoveryReceipt() {
                public SharedSrsRecoveryPreparation preparation(){calls.incrementAndGet();throw new AssertionError("untrusted preparation accessed");}
                public Outcome outcome(){calls.incrementAndGet();if(kind.equals("cancelled"))return Outcome.CANCELLED;throw new AssertionError("untrusted outcome accessed");}
                public void requireCurrent(){calls.incrementAndGet();throw new AssertionError("untrusted receipt guard invoked");}
            };
            var config=rows(f.config);var cache=rows(f.cache);
            assertThatThrownBy(() -> f.configuration.sharedRecoveryPreparations().complete(fake)).isInstanceOf(TapstateException.class);
            assertThat(calls).hasValue(0);assertThat(rows(f.config)).isEqualTo(config);assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @ParameterizedTest @ValueSource(strings={"before-commit","after-commit"})
    void completionAuthorizationLossRollsBackOrRecognizesTheCommittedUnknownResult(String cut) {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var receipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);
            var config=rows(f.config);var cache=rows(f.cache);long version=f.configuration.configurationRevision();
            var calls=new java.util.concurrent.atomic.AtomicInteger();int failAt=cut.equals("before-commit")?4:5;
            var guarded=f.configuration.sharedRecoveryPreparations().guardedBy(() -> {
                if(calls.incrementAndGet()==failAt)throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            });
            assertThatThrownBy(() -> guarded.complete(receipt)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            if(cut.equals("before-commit")) {
                assertThat(rows(f.config)).isEqualTo(config);assertThat(f.configuration.configurationRevision()).isEqualTo(version);
                assertThat(f.configuration.recoveryCommands().find(f.auth(),reservation.plan().operationId()).orElseThrow().phase()).isEqualTo(Phase.RUNNING);
            } else {
                var committed=rows(f.config);
                var result=f.configuration.sharedRecoveryPreparations().complete(receipt);
                assertThat(result.phase()).isEqualTo(Phase.COMPLETE);assertThat(result.operationId()).isEqualTo(reservation.plan().operationId());
                assertThat(f.configuration.configurationRevision()).isEqualTo(version+1);assertThat(rows(f.config)).isEqualTo(committed);
            }
            assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @Test void reopenedClientsCompleteUsingTheirOwnFreshPendingHandleAndAppliedReceipt() {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var oldReceipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);
            var config=rows(f.config);var cache=rows(f.cache);long version=f.configuration.configurationRevision();
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,CACHE.getReplicaSetUrl(f.cache.getName()),f.cache.getName(),null);
            try(var reopenedConfig=MongoConfigurationDomain.open(f.settings,"cluster-a");
                    var reopenedCache=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"shared-completion-reopened-test")) {
                assertThatThrownBy(() -> reopenedConfig.sharedRecoveryPreparations().complete(oldReceipt)).isInstanceOf(TapstateException.class);
                assertThat(rows(f.config)).isEqualTo(config);assertThat(rows(f.cache)).isEqualTo(cache);
                var pending=reopenedConfig.sharedRecoveryPreparations().pending(f.auth(),reservation.plan().operationId()).orElseThrow();
                var freshReceipt=reopenedCache.sharedSrsRecoveries(f.scope,ACTIVE).replace(pending,f.node);
                var completed=reopenedConfig.sharedRecoveryPreparations().complete(freshReceipt);
                assertThat(completed.phase()).isEqualTo(Phase.COMPLETE);assertThat(completed.operationId()).isEqualTo(reservation.plan().operationId());
                assertThat(reopenedConfig.capturePreparations().read(f.scope,f.previous.captureId())).contains(reservation.replacement());
                var committed=rows(f.config);
                assertThat(reopenedConfig.sharedRecoveryPreparations().complete(freshReceipt)).isEqualTo(completed);
                assertThat(rows(f.config)).isEqualTo(committed);
            }
            assertThat(f.configuration.configurationRevision()).isEqualTo(version+1);assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    @Test void historicalCompletionReplayCannotRestoreAGateAfterAnExplicitStoreFullAdvancesThePipeline() {
        withFixture(f -> {
            f.endAffectedTenures();var reservation=f.configuration.sharedRecoveryPreparations().reserve(f.confirmed());
            var receipt=f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(reservation,f.node);
            var completed=f.configuration.sharedRecoveryPreparations().complete(receipt);
            // This source-only full proves existing store lifecycle integration, not a native snapshot or target result.
            var intent=f.configuration.intents().loadForActuation("a").orElseThrow();
            assertThat(f.configuration.intents().compareAndSet(intent.scope(),intent.intentRevision(),
                    new DesiredState("a",PipelineState.STOPPED,intent.desired().revision(),true))).isTrue();
            var clearing=f.configuration.intents().loadForActuation("a").orElseThrow();
            var target=new PipelineIntentScope(f.scope.clusterId(),f.scope.bindingVersion(),f.scope.cacheIncarnation(),"a",2);
            var pipeline=(PipelineResource)f.configuration.artifacts().get("a").orElseThrow();
            var source=(SourceResource)f.configuration.artifacts().get("input").orElseThrow();
            var inputs=PipelinePreparationInputs.of(target,pipeline,Map.of("input",source),Map.of("input",Set.of("orders")));
            var originalClaim=f.domain.workloadClaims(f.scope).readPipeline(clearing.scope()).orElseThrow().claim();
            var request=new PipelineRebuildRequest("explicit-full-after-shared-replacement",clearing,WorkloadClaimFence.from(originalClaim),0,inputs);
            var full=f.configuration.rebuilds().begin(request);
            var cacheFull=f.domain.pipelineRebuilds(f.scope,ACTIVE).prepare(full,f.node,TTL,Instant.parse("2026-10-19T16:00:00Z"));
            var rebuilt=f.configuration.rebuilds().complete(cacheFull);
            assertThat(rebuilt.scope()).isEqualTo(target);assertThat(f.mapping("a")).doesNotContainKey("sharedRecovery");
            assertThat(f.mapping("b")).containsKey("sharedRecovery");
            var config=rows(f.config);var cache=rows(f.cache);
            assertThat(f.configuration.sharedRecoveryPreparations().complete(receipt)).isEqualTo(completed);
            assertThat(rows(f.config)).isEqualTo(config);assertThat(rows(f.cache)).isEqualTo(cache);
        });
    }

    private static void assertReplacementRefusesWithoutMutation(Fixture f,SharedSrsRecoveryPreparation original) {
        var cache=rows(f.cache);var config=rows(f.config);
        assertThatThrownBy(() -> f.domain.sharedSrsRecoveries(f.scope,ACTIVE).replace(original,f.node)).isInstanceOf(TapstateException.class);
        assertThat(rows(f.cache)).isEqualTo(cache);assertThat(rows(f.config)).isEqualTo(config);
    }
    private static void assertPipelineClaimUnchangedExceptEnd(Document actual,Document before) {
        assertThat(actual.getLong("fencedConsumerWrites")).isEqualTo(before.getLong("fencedConsumerWrites")+1);
        assertThat(actual.getDate("leaseUntil").getTime()).isZero();
        var normalized=new Document(actual);normalized.put("fencedConsumerWrites",before.get("fencedConsumerWrites"));
        normalized.put("leaseUntil",before.get("leaseUntil"));assertThat(normalized).isEqualTo(before);
    }

    private static void withFixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configName = "shared_reservation_cfg_" + suffix, cacheName = "shared_reservation_cache_" + suffix;
        try (var configAdmin = MongoClients.create(CONFIG.getReplicaSetUrl()); var cacheAdmin = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var config = configAdmin.getDatabase(configName); var cache = cacheAdmin.getDatabase(cacheName);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
            var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheName), cacheName, null);
            try (var configuration = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "install-a", "setup-a", 1);
                    var domain = MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "shared-reservation-test")) {
                var master = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                var binding = configuration.bindings().commitWithFirstCache(SETUP, StorageBinding.Mode.OP, connection, master);
                domain.prepareFirstAuth(binding.firstCreation().orElseThrow()); var scope = domain.prepareInitialBusiness(binding.preparation(), ACTIVE);
                configuration.bindings().complete(SETUP);
                var claims = domain.workloadClaims(scope);
                var node = WorkloadClaimFence.from(claims.acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, OWNER.nodeId()), OWNER, 0, TTL).claim());
                configuration.artifacts().create(new SourceResource("input", null, "mysql", Map.of("host", "server-a", "password", "secret-sentinel"),
                        SourceMode.CDC, List.of(TableRef.literal("orders")), null, null));
                var requests = new TreeMap<String, CapturePreparationRequest>();
                var initial = new Fixture(configuration, config, cache, domain, settings, scope, node, requests, null);
                for (String id : List.of("a", "b", "c")) requests.put(id, initial.pipeline(id));
                var creation = ((CaptureCreationResult.Issued) configuration.capturePreparations().issueFirstCreate(requests.get("a"))).creation();
                var previous = creation.preparation(); domain.capturePreparations(scope, ACTIVE).createFirst(creation);
                var sharedNotes=configuration.requiredNotes().open(new RequiredNoteOwner(scope.bindingVersion(),"pdk.chain."+previous.canonicalChainId(),
                        RequiredNoteOwner.Kind.CHAIN,previous.canonicalChainId(),0));
                // An explicit store origin fixture is not a registered connector or native source proof.
                configuration.requiredNotes().prepareOrigin(sharedNotes,new RequiredNoteOrigin("mysql","a".repeat(64),new byte[]{1,2,3}));
                assertThat(configuration.capturePreparations().issueFirstCreate(requests.get("b"))).isInstanceOf(CaptureCreationResult.AlreadyIssued.class);
                var consumers = domain.srs(scope, ACTIVE).consumers();
                for (String id : List.of("a", "b")) consumers.attach(new SrsConsumerContext(new SrsConsumerKey(previous.chainId(), id, "input"), 1),
                        0, Set.of("orders"), requests.get(id).originalPipeline()).orElseThrow();
                scenario.run(new Fixture(configuration, config, cache, domain, settings, scope, node, requests, previous));
            } finally { config.drop(); cache.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; } catch (Exception failure) { throw new AssertionError(failure); }
    }

    private record Fixture(MongoConfigurationDomain configuration, MongoDatabase config, MongoDatabase cache, MongoCacheDomain domain,
            MongoConnectionSettings settings, CacheBusinessScope scope, WorkloadClaimFence node,
            Map<String, CapturePreparationRequest> requests, CapturePreparation previous) {
        WorkloadClaimFence publishStoreHistory() {
            var claims=domain.workloadClaims(scope);
            var acquired=claims.acquire(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,previous.captureId()),OWNER,1,TTL).claim();
            var capture=WorkloadClaimFence.from(claims.advanceExecution(acquired,1,Set.of(OWNER.nodeId())).orElseThrow());
            var srs=domain.srs(scope,ACTIVE);String chain=previous.chainId(),ring="srs."+chain+".orders";
            assertThat(srs.chains().openEpoch(chain,0,capture)).hasValue(1);
            var key=new SrsModelKey(chain,"orders",scope.cacheIncarnation(),1,0);
            srs.models().publish(new SrsModel(key,ring,new SourceTable("orders",List.of(new SourceField("id","int")),List.of(),List.of()),0),capture);
            srs.log().store(ring,0,new SrsLogRecord("t0",io.tapstate.core.event.Op.INSERT,1,null,Map.of("id",1),0,capture,1));
            srs.chains().requestTables(chain,Set.of("orders"),capture);
            assertThat(srs.chains().publishServingTables(chain,1,Set.of("orders"),capture)).isTrue();
            var checkpoint=new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(1,0),"t0"),Map.of("orders",new SrsTableCheckpoint(key,0)));
            srs.chains().checkpoint(chain,0,checkpoint,capture).orElseThrow();return capture;
        }
        void endAffectedTenures() {
            var claims=domain.workloadClaims(scope);
            for(String id:List.of("a","b")) {
                var current=claims.readPipeline(requests.get(id).pipeline()).orElseThrow();
                if(current.leased())assertThat(claims.release(current.claim())).isTrue();
            }
            var capture=claims.read(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,previous.captureId())).orElseThrow();
            if(capture.leased())assertThat(claims.release(capture.claim())).isTrue();
        }
        Document chain(String chain) {return SystemCollections.SRS_CHAINS.on(cache).find(new Document("_id",chain)).first();}
        Document claim(String resource,WorkloadClaimType type) {
            return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(new Document("_id",MongoWorkloadClaimStore.id(
                    new WorkloadClaimKey(scope.clusterId(),type,resource)))).first();
        }
        CacheAuthScope auth() { return configuration.bindings().cachePreparation().orElseThrow().authScope(); }
        CapturePreparationRequest pipeline(String id) {
            var pipeline = (PipelineResource) new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: " + id
                    + "\nsource: [{id: input, srs: true}]\nsettings: {read_mode: snapshot_and_cdc}\n");
            configuration.artifacts().create(pipeline);
            var original = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), id, 1);
            configuration.intents().compareAndSet(original, 0, new DesiredState(id, PipelineState.RUNNING, CanonicalHash.of(pipeline), false));
            var source = (SourceResource) configuration.artifacts().get("input").orElseThrow();
            var inputs = PipelinePreparationInputs.of(original, pipeline, Map.of("input", source), Map.of("input", Set.of("orders")));
            var creation = ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(original, node, inputs)).creation();
            var preparer = domain.pipelinePreparations(scope, ACTIVE);
            configuration.preparations().complete(preparer.seed(preparer.reserveFirst(creation, TTL), configuration.preparations(), Instant.parse("2026-10-19T16:00:00Z")));
            var claims = domain.workloadClaims(scope); var held = claims.acquirePipeline(original, OWNER, 1, TTL).claim();
            var running = claims.advanceExecution(held, 1, Set.of(OWNER.nodeId())).orElseThrow();
            return new CapturePreparationRequest(original, WorkloadClaimFence.from(running), node, "input", inputs);
        }
        Plan planned() {
            var observed = domain.srs(scope, ACTIVE).consumers().inspectSharedRecovery(previous);
            var hashes = new TreeMap<String, String>();
            for (String id : List.of("a", "b", "input", "views")) hashes.put(id, CanonicalHash.of(configuration.artifacts().get(id).orElseThrow()));
            var manifest = new Manifest(new Selection(Extent.SHARED_SRS, Set.of(), previous.canonicalChainId(), previous.chainId()),
                    observed.pipelines().keySet(), hashes, CanonicalHash.ofText(observed.controlHash()), configuration.configurationRevision(), observed);
            return configuration.recoveryCommands().plan(auth(), manifest, null, null);
        }
        Plan confirmed() { return configuration.recoveryCommands().confirm(planned()); }
        Document mapping(String id) { return SystemCollections.OPERATOR_GENERATIONS.on(config).find(new Document("_id", id)).first(); }
        Document intentRow(String id) { return SystemCollections.PIPELINE_INTENTS.on(config).find(new Document("_id", id)).first(); }
        void changeSourceIdentity() {
            var source=(SourceResource)configuration.artifacts().get("input").orElseThrow();
            configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),Map.of("host","different-server"),
                    source.mode(),source.tables(),source.srs(),source.experimental()));
        }
        void changeSource() {
            var source = (SourceResource) configuration.artifacts().get("input").orElseThrow();
            configuration.artifacts().save(new SourceResource(source.id(), new Metadata(Map.of(), "edited after reservation"), source.connector(),
                    source.config(), source.mode(), source.tables(), source.srs(), source.experimental()));
        }
    }
    private static List<Document> notes(MongoDatabase database) { return SystemCollections.PDK_NOTE_SCOPES.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()); }
    private static Map<String, List<Document>> rows(MongoDatabase database) {
        var result = new TreeMap<String, List<Document>>();
        for (String name : database.listCollectionNames()) result.put(name, database.getCollection(name).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
        return result;
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
