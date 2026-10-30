package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.StorageRecoveryCommands.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** CONFIG intent and CAS evidence only; no assertion here claims actual cache or target recovery. */
@RequiresDocker
class MongoStorageRecoveryCommandsIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "install", "setup", 1);
    private static final SourceResource SOURCE = new SourceResource("source", null, "mysql", Map.of("host", "source.invalid", "password", "source-sentinel"),
            SourceMode.CDC, List.of(TableRef.literal("orders")), null, null);
    private static PipelineResource pipeline(String id) {
        return new PipelineResource(id, null, List.of(SourceRef.spec(SOURCE.id(), false)), null, null,
                new ServeBlock.Inline("serve", FromRef.literal("orders"), List.of(new SyncElement("sync", "views", null, null, null)), null, null), null, null);
    }
    @Test void plansAndConfirmationsRetainOneOriginalOperationWithoutChangingCacheOrPipelineGenerations() {
        withConfiguration(f -> {
            var originalCache = f.domain.bindings().cachePreparation().orElseThrow();
            Document root = SystemCollections.STORAGE_RECOVERY.on(f.database).find().first();
            long revision = f.domain.configurationRevision();
            var manifest = f.manifest(new Selection(Extent.PIPELINE, Set.of("a")), Set.of("a"), "a".repeat(64));
            var plan = f.domain.recoveryCommands().plan(f.scope(), manifest, null, null);
            assertThat(plan.phase()).isEqualTo(Phase.PLANNED);
            assertThat(f.domain.recoveryCommands().plan(f.scope(), manifest, null, null)).isEqualTo(plan);
            assertThat(f.domain.configurationRevision()).isEqualTo(revision+1);
            var confirmed = f.domain.recoveryCommands().confirm(plan);
            assertThat(confirmed.phase()).isEqualTo(Phase.CONFIRMED);
            assertThat(confirmed.controlRevision()).isEqualTo(plan.controlRevision());
            assertThat(f.domain.recoveryCommands().confirm(plan)).isEqualTo(confirmed);
            assertThat(f.domain.configurationRevision()).isEqualTo(revision+2);
            Document after = SystemCollections.STORAGE_RECOVERY.on(f.database).find().first();
            after.remove("pendingRecovery"); assertThat(after).isEqualTo(root);
            assertThat(f.domain.pipelineStates().read("a").orElseThrow().stateGeneration()).isEqualTo(1);
            assertThat(f.domain.pipelineStates().read("b").orElseThrow().stateGeneration()).isEqualTo(1);
            assertThat(f.domain.bindings().cachePreparation()).contains(originalCache);
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster")) {
                assertThat(reopened.recoveryCommands().find(f.scope(), plan.operationId())).contains(confirmed);
                assertThat(reopened.bindings().cachePreparation()).contains(originalCache);
            }
            assertThat(f.domain.recoveryCommands().find(f.scope(), UUID.randomUUID().toString())).isEmpty();
        });
    }
    @Test void aConfigurationEditOrChangedInfluenceRequiresANewConfirmation() {
        withConfiguration(f -> {
            var selected = new Selection(Extent.PIPELINE, Set.of("a"));
            var plan = f.domain.recoveryCommands().plan(f.scope(), f.manifest(selected, Set.of("a"), "a".repeat(64)), null, null);
            var edited = new SourceResource(SOURCE.id(), new Metadata(Map.of(), "changed"), SOURCE.connector(), SOURCE.config(), SOURCE.mode(), SOURCE.tables(), null, null);
            f.domain.artifacts().replace(SOURCE.id(), CanonicalHash.of(SOURCE), edited);
            long changed = f.domain.configurationRevision();
            assertThatThrownBy(() -> f.domain.recoveryCommands().confirm(plan)).isInstanceOf(TapstateException.class);
            assertThat(f.domain.configurationRevision()).isEqualTo(changed);
            var refreshed = f.domain.recoveryCommands().plan(f.scope(), f.manifest(selected, Set.of("a", "b"), "b".repeat(64)), plan.operationId(), null);
            assertThat(refreshed.operationId()).isEqualTo(plan.operationId());
            assertThat(refreshed.planHash()).isNotEqualTo(plan.planHash());
            assertThat(refreshed.manifest().affectedPipelineIds()).containsExactly("a", "b");
            assertThatThrownBy(() -> f.domain.recoveryCommands().confirm(plan)).isInstanceOf(TapstateException.class);
            assertThat(f.domain.recoveryCommands().confirm(refreshed).phase()).isEqualTo(Phase.CONFIRMED);
        });
    }
    @Test void aClusterRangeCannotOmitAnExistingPipelineAndAChangedDefinitionCannotBeConfirmed() {
        withConfiguration(f -> {
            var cluster = new Selection(Extent.CLUSTER, Set.of());
            long original = f.domain.configurationRevision();
            assertThatThrownBy(() -> f.domain.recoveryCommands().plan(f.scope(), f.manifest(cluster, Set.of("a"), "a".repeat(64)), null, null))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.domain.configurationRevision()).isEqualTo(original);
            assertThat(f.domain.recoveryCommands().current(f.scope())).isEmpty();
            var all = f.manifest(cluster, Set.of("a", "b"), "a".repeat(64));
            var plan = f.domain.recoveryCommands().plan(f.scope(), all, null, null);
            var hashes = new TreeMap<>(all.definitionHashes()); hashes.put("source", "f".repeat(64));
            var wrong = new Manifest(cluster, all.affectedPipelineIds(), hashes, all.dependencyHash(), f.domain.configurationRevision());
            assertThatThrownBy(() -> f.domain.recoveryCommands().plan(f.scope(), wrong, plan.operationId(), null)).isInstanceOf(TapstateException.class);
            assertThat(f.domain.recoveryCommands().find(f.scope(), plan.operationId())).contains(plan);
        });
    }
    @Test void independentClientsPublishAndConfirmOnlyOneOriginalOperation() {
        withConfiguration(f -> {
            var manifest = f.manifest(new Selection(Extent.CLUSTER, Set.of()), Set.of("a", "b"), "a".repeat(64));
            try (var other = MongoConfigurationDomain.open(f.settings, "cluster"); var threads = Executors.newFixedThreadPool(2)) {
                var start = new CountDownLatch(1);
                var first = threads.submit(() -> { start.await(); return f.domain.recoveryCommands().plan(f.scope(), manifest, null, null); });
                var second = threads.submit(() -> { start.await(); return other.recoveryCommands().plan(f.scope(), manifest, null, null); });
                start.countDown();
                Plan a = first.get(15, TimeUnit.SECONDS), b = second.get(15, TimeUnit.SECONDS);
                assertThat(a).isEqualTo(b);
                long revision = f.domain.configurationRevision();
                var confirmedA = threads.submit(() -> f.domain.recoveryCommands().confirm(a));
                var confirmedB = threads.submit(() -> other.recoveryCommands().confirm(b));
                assertThat(confirmedA.get(15, TimeUnit.SECONDS)).isEqualTo(confirmedB.get(15, TimeUnit.SECONDS));
                assertThat(f.domain.configurationRevision()).isEqualTo(revision+1);
            }
        });
    }
    @Test void anAuthorizationFailureAfterProtectedWritesRollsBackThePlanAndRevision() {
        withConfiguration(f -> {
            long revision = f.domain.configurationRevision(); var calls = new AtomicInteger();
            var guarded = f.domain.recoveryCommands().guardedBy(() -> {
                if (calls.incrementAndGet() == 2) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.plan(f.scope(), f.manifest(new Selection(Extent.PIPELINE, Set.of("a")), Set.of("a"), "a".repeat(64)), null, null))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.domain.configurationRevision()).isEqualTo(revision);
            assertThat(f.domain.recoveryCommands().current(f.scope())).isEmpty();
        });
    }
    @Test void malformedPendingFactsRefuseOrdinaryLoadWithoutRepair() {
        withConfiguration(f -> {
            var plan = f.domain.recoveryCommands().plan(f.scope(), f.manifest(new Selection(Extent.PIPELINE, Set.of("a")), Set.of("a"), "a".repeat(64)), null, null);
            var rows = SystemCollections.STORAGE_RECOVERY.on(f.database);
            Document root = rows.find().first(); Document pending = root.get("pendingRecovery", Document.class);
            var bad = new ArrayList<Document>();
            var missing = new Document(pending); missing.remove("definitionHashes"); bad.add(missing);
            bad.add(new Document(pending).append("planHash", "f".repeat(64)));
            bad.add(new Document(pending).append("controlRevision", plan.controlRevision()+1));
            bad.add(new Document(pending).append("phase", "UNKNOWN"));
            for (var fault : bad) {
                rows.updateOne(new Document("_id", "cluster"), new Document("$set", new Document("pendingRecovery", fault)));
                assertThatThrownBy(() -> f.domain.bindings().load()).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, "cluster")).isInstanceOf(TapstateException.class);
                assertThat(rows.find().first().get("pendingRecovery")).isEqualTo(fault);
            }
            rows.replaceOne(new Document("_id", "cluster"), root);
        });
    }
    @Test void aNewRecoveryMustNameItsCompletedPredecessorAndLateRequestsCannotCreateAnother() {
        withConfiguration(f -> {
            var selected = new Selection(Extent.PIPELINE, Set.of("a"));
            var initial = f.manifest(selected, Set.of("a"), "a".repeat(64));
            var plan = f.domain.recoveryCommands().plan(f.scope(), initial, null, null);
            var confirmed = f.domain.recoveryCommands().confirm(plan);
            var running = f.domain.recoveryCommands().start(confirmed);
            var completed = f.domain.recoveryCommands().complete(running);
            assertThat(f.domain.recoveryCommands().plan(f.scope(), initial, null, null)).isEqualTo(completed);
            long revision = f.domain.configurationRevision();
            var next = f.domain.recoveryCommands().plan(f.scope(), f.manifest(selected, Set.of("a"), "a".repeat(64)), null, plan.operationId());
            assertThat(next.operationId()).isNotEqualTo(plan.operationId());
            assertThat(next.previousOperationId()).isEqualTo(plan.operationId());
            assertThatThrownBy(() -> f.domain.recoveryCommands().confirm(plan)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.domain.recoveryCommands().plan(f.scope(), initial, null, null)).isInstanceOf(TapstateException.class);
            assertThat(f.domain.configurationRevision()).isEqualTo(revision+1);
            assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).countDocuments()).isEqualTo(1);
        });
    }
    @Test void aRetainedSharedPlanCannotUseTheGenericStartOrCompletionAsReplacementAuthority() {
        withConfiguration(f -> {
            var auth=f.scope();var business=new CacheBusinessScope(auth.clusterId(),auth.bindingVersion(),auth.cacheIncarnation());
            String chain=io.tapstate.core.model.canonical.SourceCaptureIdentity.resolve(SOURCE.connector(),SOURCE.config(),null);
            String captureId=io.tapstate.core.model.canonical.SourceCaptureIdentity.sharedCapture(chain);
            var preparation=new CapturePreparation(business,CapturePreparation.Mode.SHARED_SRS,chain,captureId,
                    io.tapstate.core.model.canonical.SourceCaptureIdentity.physical(SOURCE.connector(),SOURCE.config()),UUID.randomUUID().toString());
            var owner=new WorkloadOwner("node","boot");
            var capture=new WorkloadClaimFence(new WorkloadClaimKey(auth.clusterId(),WorkloadClaimType.CAPTURE,captureId),owner,1,1,0);
            var claim=new WorkloadClaimFence(new WorkloadClaimKey(auth.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,"a"),owner,1,1,0);
            var scope=new PipelineIntentScope(auth.clusterId(),auth.bindingVersion(),auth.cacheIncarnation(),"a",1);
            var member=new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey(chain,"a","source"),1),1,false,Set.of("orders"),claim);
            var observed=new SharedSrsRecoverySnapshot(preparation,capture,"a".repeat(64),List.of(member),
                    Map.of("a",new SharedSrsRecoverySnapshot.Pipeline(scope,claim)));
            var selection=new Selection(Extent.SHARED_SRS,Set.of(),chain,chain);
            var definitions=f.manifest(new Selection(Extent.PIPELINE,Set.of("a")),Set.of("a"),"a".repeat(64));
            var manifest=new Manifest(selection,Set.of("a"),definitions.definitionHashes(),definitions.dependencyHash(),f.domain.configurationRevision(),observed);
            var plan=new Plan(UUID.randomUUID().toString(),null,auth,manifest,MongoSharedSrsRecoveryFacts.hash(manifest),manifest.configurationRevision()+1,Phase.CONFIRMED);
            // A seeded retained plan proves decoding and refusal only; it supplies no actual CACHE control or handle.
            var table=SystemCollections.STORAGE_RECOVERY.on(f.database);
            table.updateOne(new Document("_id","cluster"),new Document("$set",new Document("pendingRecovery",MongoStorageRecoveryCommands.encode(plan))));
            var before=table.find().first();long revision=f.domain.configurationRevision();
            assertThat(f.domain.recoveryCommands().find(auth,plan.operationId())).contains(plan);
            try(var reopened=MongoConfigurationDomain.open(f.settings,"cluster")) {
                assertThat(reopened.bindings().cachePreparation().orElseThrow().authScope()).isEqualTo(auth);
                assertThat(reopened.recoveryCommands().find(auth,plan.operationId())).contains(plan);
            }
            assertThatThrownBy(() -> f.domain.recoveryCommands().start(plan)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
            var running=new Plan(plan.operationId(),plan.previousOperationId(),auth,manifest,plan.planHash(),plan.controlRevision(),Phase.RUNNING);
            assertThatThrownBy(() -> f.domain.recoveryCommands().complete(running)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
            assertThat(table.find().first()).isEqualTo(before);assertThat(f.domain.configurationRevision()).isEqualTo(revision);
        });
    }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var db = admin.getDatabase("recovery_commands_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(db.getName()), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, "cluster", "install", "setup", 1)) {
                var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache.invalid/cache", "cache", null);
                var target = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                domain.bindings().commit(SETUP, StorageBinding.Mode.OP, cache, target); domain.bindings().complete(SETUP);
                var writer = new io.tapstate.core.model.canonical.CanonicalWriter();
                var parsed = new io.tapstate.core.dsl.DslParser().fromTree(writer.tree(pipeline("a")));
                assertThat(writer.write(parsed)).isEqualTo(writer.write(pipeline("a")));
                domain.artifacts().saveAll(List.of(SOURCE, pipeline("a"), pipeline("b")));
                scenario.run(new Fixture(domain, db, settings));
            } finally { db.drop(); }
        } catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoConfigurationDomain domain, MongoDatabase database, MongoConnectionSettings settings) {
        CacheAuthScope scope() { return domain.bindings().cachePreparation().orElseThrow().authScope(); }
        Manifest manifest(Selection selection, Set<String> affected, String dependencyHash) {
            var hashes = new TreeMap<String,String>();
            for (String id : affected) hashes.put(id, CanonicalHash.of(domain.artifacts().get(id).orElseThrow()));
            for (String id : List.of("source", "views")) hashes.put(id, CanonicalHash.of(domain.artifacts().get(id).orElseThrow()));
            return new Manifest(selection, affected, hashes, dependencyHash, domain.configurationRevision());
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
