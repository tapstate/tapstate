package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
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

/** Actual CONFIG reservation only; these checks do not apply a CACHE replacement or prove business recovery. */
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
