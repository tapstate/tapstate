package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import static org.assertj.core.api.Assertions.*;

/** Real CONFIG/CACHE domains and native leases; reservation does not execute business validation. */
@RequiresDocker
class NativeValidationOwnerReservationsIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test void nativeFullAndIncrementalReservationsReuseOneAdvancedOwnerWithoutLaunchTickets() {
        try (var f = new Fixture()) {
            var full = f.request("FULL", 1); var first = f.owners.reserve(full);
            assertThat(first.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var inc = f.owners.reserve(f.request("INCREMENTAL", 1));
            assertThat(inc.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED); assertThat(inc.owner()).isEqualTo(first.owner());
            assertThat(f.owners.reserve(full).owner()).isEqualTo(first.owner());
            var stored = f.owners.readOwner("flow").orElseThrow(); assertThat(ValidationClaims.owner("flow", stored.claim())).isEqualTo(first.owner());
            assertThat(stored.claim().executionNodeIds()).containsExactly("node");
            var task = f.tasks.read("flow", "task").orElseThrow(); assertThat(task.full().permission()).isNull(); assertThat(task.incremental().permission()).isNull();
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            assertThat(f.raw.getDatabase(f.cacheName).getCollection("pipeline_state").countDocuments()).isZero();
            assertThat(f.resources.snapshot().metadataFrames()).isEqualTo(2);
        }
    }

    @Test void capacityRefusalHappensBeforeNativeValidationClaimCreation() {
        try (var f = new Fixture()) {
            var held = new ArrayList<ValidationMemberResources.Run>();
            for (int index = 0; index < 2; index++) held.add(f.resources.admit(new ValidationRunIdentity(new ValidationOwner("cluster", "other", "node", "boot", 1, 1, 1),
                    "other", "held-" + index, ValidationMode.FULL, "spec", 1, "rules"), new ValidationLocalBudget.Amount(1024, 1, 0, 0), 1, () -> {}));
            assertThat(f.owners.reserve(f.request("FULL", 1)).outcome()).isEqualTo(ValidationOwnerRouter.Outcome.CAPACITY);
            assertThat(f.raw.getDatabase(f.cacheName).getCollection("workload_claims").countDocuments(new Document("_id.resourceType", "VALIDATION"))).isZero();
            held.forEach(value -> { value.close(); value.processorsExited(); });
            assertThat(f.owners.reserve(f.request("FULL", 1)).outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
        }
    }

    @Test void aReplacementContextEndsOldMetadataUnderANewTenureWithoutResumingItsRun() {
        try (var f = new Fixture()) {
            var request = f.request("FULL", 1); var accepted = f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec = new ValidationRunSpec(f.comparison, ValidationMode.FULL, ValidationMethod.COUNT);
            var identity = new ValidationRunIdentity(accepted.owner(), "task", request.run(), spec.mode(), spec.fingerprint(), 1, "rules");
            var start = new ValidationReportAdmission.Start(f.task, 1, 1, request.command().toString(), identity);
            var access = f.configuration.prepareValidationAccess("flow", f.scope, f.cache, WorkloadClaimFence.from(f.node), () -> {});
            assertThat(access.admissions().reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
            var old = new ValidationControlStore.Run(identity, 1, start, spec); assertThat(access.control().join(old)).isEqualTo(ValidationControlStore.Join.JOINED);
            access.admissions().open(new ValidationReportAdmission.Open(start, spec));
            f.owners.close(); f.resources.close();
            var resources = f.process.bind("cluster", "node", "boot", 4, () -> {});
            try (var replacement = f.owner(resources)) {
                var next = replacement.reserve(f.request("FULL", 2)); assertThat(next.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                assertThat(next.owner().claimGeneration()).isGreaterThan(accepted.owner().claimGeneration());
                assertThat(next.owner().executionGeneration()).isGreaterThan(accepted.owner().executionGeneration());
                var current = f.configuration.prepareValidationAccess("flow", f.scope, f.cache, WorkloadClaimFence.from(f.node), () -> {});
                var ended = current.control().read(next.owner(), old).orElseThrow(); assertThat(ended.retired()).isTrue(); assertThat(ended.state()).isEqualTo(ValidationRunState.FAILED);
                assertThat(f.tasks.read("flow", "task").orElseThrow().full().permission().phase()).isEqualTo(ValidationTaskStore.Phase.CLOSED);
                assertThat(current.control().snapshot(next.owner()).active()).isEmpty();
                assertThat(f.raw.getDatabase(f.configName).getCollection("validation_reports").find().first()).containsEntry("closed", true);
            }
        }
    }

    @Test void aProvenNewPipelineLifetimeUsesNewPreparationWithoutRestartingTheMemberContext() {
        try (var f = new Fixture()) {
            var oldRequest = f.request("FULL", 1); var old = f.owners.reserve(oldRequest);
            assertThat(old.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var definition = f.configuration.artifacts().get("flow").orElseThrow();
            var intent = new PipelineIntentScope("cluster", f.scope.bindingVersion(), f.scope.cacheIncarnation(), "flow", 1);
            var retirements = f.configuration.retirements();
            var pending = retirements.begin(new PipelineRetirementRequest(UUID.randomUUID().toString(), intent, CanonicalHash.of(definition), Optional.empty()));
            assertThat(f.configuration.retireValidation(pending, f.cache, WorkloadClaimFence.from(f.node), () -> {})).isTrue();
            var receipt = f.cache.pipelineRetirements(f.scope, () -> {}).retire(pending, WorkloadClaimFence.from(f.node));
            for (var notes : f.configuration.requiredNotes().retirements(intent, pending.request().operationId())) {
                assertThat(notes.origin()).isEmpty(); notes.completeAfterVerifiedRelease();
            }
            retirements.complete(receipt); f.configuration.artifacts().create(definition);
            var task = new ValidationReportAdmission.Task("cluster", "flow", "replacement-task", UUID.randomUUID());
            assertThat(f.tasks.create(new ValidationTaskStore.Create(task, f.comparison, ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT, 1000), UUID.randomUUID())))
                    .isEqualTo(ValidationTaskStore.Write.APPLIED);
            var request = new ValidationOwnerRouter.Request("flow", task.taskId(), task.generation(), 1, 1, "FULL", UUID.randomUUID(), UUID.randomUUID().toString());
            var accepted = f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            assertThat(accepted.owner().claimGeneration()).isGreaterThan(old.owner().claimGeneration());
            assertThat(f.owners.reserve(oldRequest).outcome()).isNotEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
        }
    }

    @Test void cancellationCompetesWithActivationWithoutLosingTheOriginalStopCallback() throws Exception {
        try (var f = new Fixture()) {
            var request = f.request("FULL", 1); var accepted = f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec = new ValidationRunSpec(f.comparison, ValidationMode.FULL, ValidationMethod.COUNT);
            var run = new ValidationRunIdentity(accepted.owner(), "task", request.run(), spec.mode(), spec.fingerprint(), 1, "rules");
            var reservation = f.owners.take(new ValidationReportAdmission.Start(f.task, 1, 1, request.command().toString(), run));
            var reservedResources = reservation.resources();
            var inGuard = new CountDownLatch(1); var leaveGuard = new CountDownLatch(1); var checks = new AtomicInteger(); var stops = new AtomicInteger();
            f.localGuard = () -> {
                if (Thread.currentThread().getName().equals("reservation-activate") && checks.incrementAndGet() == 2) {
                    inGuard.countDown(); try { assertThat(leaveGuard.await(5, TimeUnit.SECONDS)).isTrue(); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                }
            };
            var activation = new FutureTask<Void>(() -> { reservation.activate(stops::incrementAndGet); return null; });
            var activate = new Thread(activation, "reservation-activate"); var close = new Thread(reservation::close, "reservation-close");
            try {
                activate.start(); assertThat(inGuard.await(5, TimeUnit.SECONDS)).isTrue(); close.start();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (close.getState() != Thread.State.BLOCKED && close.isAlive() && System.nanoTime() - until < 0) Thread.onSpinWait();
                assertThat(close.getState()).isEqualTo(Thread.State.BLOCKED); leaveGuard.countDown();
                activation.get(5, TimeUnit.SECONDS); close.join(5000); assertThat(close.isAlive()).isFalse(); assertThat(stops).hasValue(1);
            } finally {
                leaveGuard.countDown(); activate.join(5000); close.join(5000); f.localGuard = () -> {};
                reservedResources.processorsExited(); reservation.releaseAfterKnownTerminal();
            }
        }
    }

    @Test void knownClosedAndPhysicallyReleasedCommandsDoNotConsumeCumulativeCapacity() {
        try (var f = new Fixture(64, 2)) {
            for (int sequence = 1; sequence <= 3; sequence++) {
                var request = f.request("FULL", sequence); var accepted = f.owners.reserve(request);
                assertThat(accepted.outcome()).as("start sequence %s", sequence).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
                var spec = new ValidationRunSpec(f.comparison, ValidationMode.FULL, ValidationMethod.COUNT);
                var identity = new ValidationRunIdentity(accepted.owner(), "task", request.run(), spec.mode(), spec.fingerprint(), 1, "rules");
                var start = new ValidationReportAdmission.Start(f.task, 1, sequence, request.command().toString(), identity);
                var reservation = f.owners.take(start); var resources = reservation.resources(); var access = reservation.access();
                var run = new ValidationControlStore.Run(identity, reservation.countingGeneration(), start, spec);
                assertThat(access.admissions().reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);
                assertThat(access.control().join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
                access.admissions().open(new ValidationReportAdmission.Open(start, spec)); reservation.activate(() -> {});
                access.control().transition(new ValidationControlStore.Transition(run, ValidationRunState.STARTING, ValidationRunState.STOPPING));
                access.admissions().close(start);
                access.control().transition(new ValidationControlStore.Transition(run, ValidationRunState.STOPPING, ValidationRunState.STOPPED));
                access.control().retire(new ValidationControlStore.Retirement(run, start.report(), ValidationRunState.STOPPED));
                resources.processorsExited(); reservation.releaseAfterKnownTerminal();
                assertThat(f.resources.snapshot().metadataFrames()).isZero(); assertThat(f.resources.snapshot().processors()).isZero();
                assertThat(f.owners.reserve(request).outcome()).isNotEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            }
        }
    }

    @Test void unusedPipelineMetadataDoesNotConsumeCumulativePipelineCapacity() {
        try (var f = new Fixture(2, 128)) {
            for (String pipeline : List.of("flow", "second", "third")) {
                if (!pipeline.equals("flow")) f.configuration.artifacts().create(new PipelineResource(pipeline, null, List.of(SourceRef.spec("input", false)), null, null,
                        new ServeBlock.Inline("serving", FromRef.literal("orders"), List.of(new SyncElement("out", "views", null, null, null)), null, null), null, null));
                assertThat(f.owners.readOwner(pipeline).orElseThrow().leased()).isFalse();
            }
        }
    }

    @Test void expiredAcceptedHandoffCannotResurrectItsOriginalWithoutAConfigTicket() {
        var now = new AtomicLong(System.nanoTime());
        try (var f = new Fixture(64, 2, now::get)) {
            var request = f.request("FULL", 1);
            assertThat(f.owners.reserve(request).outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            now.addAndGet(Duration.ofSeconds(31).toNanos()); f.owners.expire();
            assertThat(f.resources.snapshot().metadataFrames()).isZero(); assertThat(f.resources.snapshot().processors()).isZero();
            assertThat(f.owners.reserve(request).outcome()).isEqualTo(ValidationOwnerRouter.Outcome.REJECTED);
            assertThat(f.tasks.read("flow", "task").orElseThrow().full().permission()).isNull();
            assertThat(f.owners.reserve(f.request("FULL", 1)).outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            assertThat(f.owners.reserve(request).outcome()).isEqualTo(ValidationOwnerRouter.Outcome.REJECTED);
        }
    }

    static final class Fixture implements AutoCloseable {
        final String suffix = UUID.randomUUID().toString().replace("-", ""), configName = "owner_cfg_" + suffix, cacheName = "owner_cache_" + suffix;
        final com.mongodb.client.MongoClient raw = MongoClients.create(MONGO.getReplicaSetUrl());
        final MongoConnectionSettings settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
        final MongoConfigurationDomain configuration = MongoConfigurationBootstrap.prepare(settings, "cluster", "installation", "setup", 1);
        final StorageBinding.Connection endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(cacheName), cacheName, null);
        final MongoCacheDomain cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "native-owner-test");
        final ValidationMemberResources process = new ValidationMemberResources(); final ValidationOwnerTimers timers = new ValidationOwnerTimers();
        final ClusterMembershipGate gate; final HazelcastInstance member; final CacheBusinessScope scope; final WorkloadClaim node;
        final NodeSessionLease nodeLease;
        final ValidationTaskStore tasks; final ValidationReportAdmission.Task task = new ValidationReportAdmission.Task("cluster", "flow", "task", UUID.randomUUID());
        final ValidationTaskSpec comparison; final ValidationMemberResources.Member resources; final NativeValidationOwnerReservations owners;
        final int maxPipelines, maxOperations;
        final LongSupplier handoffClock;
        volatile Runnable localGuard = () -> {};
        volatile NativeValidationOwnerReservations.Startup startup=(saved,spec)->NativeValidationOwnerReservations.StartupGate.ready();
        Fixture() { this(64, 128); }
        Fixture(int maxPipelines, int maxOperations) { this(maxPipelines, maxOperations, System::nanoTime); }
        Fixture(int maxPipelines, int maxOperations, LongSupplier handoffClock) {
            this.maxPipelines = maxPipelines; this.maxOperations = maxOperations;
            this.handoffClock = handoffClock;
            var binding = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster", "installation", "setup", 1), StorageBinding.Mode.OP, endpoint,
                    new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://business.invalid/target"), null, null, null, null));
            scope = binding.preparation().businessScope(); cache.prepareFirstAuth(binding.firstCreation().orElseThrow()); cache.prepareInitialBusiness(binding.preparation(), () -> {});
            configuration.artifacts().create(new SourceResource("input", null, "mongodb", Map.of("uri", "mongodb://business.invalid/source"), SourceMode.CDC,
                    List.of(TableRef.literal("orders")), null, null));
            configuration.artifacts().create(new PipelineResource("flow", null, List.of(SourceRef.spec("input", false)), null, null,
                    new ServeBlock.Inline("serving", FromRef.literal("orders"), List.of(new SyncElement("out", "views", null, null, null)), null, null), null, null));
            try (var schema = MongoConfigurationDomain.prepareValidationSchema(settings, "cluster", UUID.randomUUID())) { schema.prepareCache(cache); while (!schema.advance(20).ready()) {} }
            long nodeAskedAt = System.nanoTime();
            node = cache.workloadClaims(scope).acquire(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"), new WorkloadOwner("node", "boot"), 1, Duration.ofMinutes(5)).claim();
            var config = new Config().setClusterName("native-owner-" + suffix); config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
            config.addExecutorConfig(new com.hazelcast.config.ExecutorConfig(ValidationOwnerRouter.EXECUTOR).setPoolSize(2).setQueueCapacity(16));
            config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE, "node").setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE, "boot");
            config.setProperty("hazelcast.logging.type", "none").setProperty("hazelcast.phone.home.enabled", "false").setProperty("hazelcast.shutdownhook.enabled", "false")
                    .setProperty("hazelcast.operation.thread.count", "2").setProperty("hazelcast.operation.generic.thread.count", "2")
                    .setProperty("hazelcast.io.input.thread.count", "1").setProperty("hazelcast.io.output.thread.count", "1").setProperty("hazelcast.partition.count", "17");
            config.getNetworkConfig().setPort(0).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface("127.0.0.1");
            var join = config.getNetworkConfig().getJoin(); join.getMulticastConfig().setEnabled(false); join.getTcpIpConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
            member = Hazelcast.newHazelcastInstance(config); var properties = new ClusterProperties(); properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
            nodeLease = new NodeSessionLease(cache.workloadClaims(scope), node, nodeAskedAt,
                    Duration.ofMinutes(5), Duration.ofMinutes(1), member::shutdown);
            gate = new ClusterMembershipGate(properties); gate.observeNodeSession(nodeLease::active);
            gate.install(new ClusterMembership("cluster", 1, Set.of("node"))); gate.apply(member.getCluster().getMembers());
            tasks = configuration.validationTasks(() -> {}); var hashes = new TreeMap<String, String>();
            for (String id : List.of("flow", "input", "views")) hashes.put(id, CanonicalHash.of(configuration.artifacts().get(id).orElseThrow()));
            var keys = List.of(new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number")));
            comparison = new ValidationTaskSpec("flow", hashes, 1, "rules", List.of(new ValidationTaskSpec.Mapping("mapping", ValidationTaskSpec.OutputKind.SYNC, "out",
                    new ValidationTaskSpec.Table("input", "input", "source", null, "orders", "model", "codec"),
                    new ValidationTaskSpec.Table("out", "views", "target", null, "orders", "model", "codec"), new ValidationTaskSpec.Keys("confirmed", keys, keys))));
            tasks.create(new ValidationTaskStore.Create(task, comparison, ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT, 1000), UUID.randomUUID()));
            resources = process.bind("cluster", "node", "boot", 4, () -> {}); owners = owner(resources);
        }
        ValidationOwnerRouter.Request request(String mode, long sequence) { return new ValidationOwnerRouter.Request("flow", "task", task.generation(), 1, sequence, mode, UUID.randomUUID(), UUID.randomUUID().toString()); }
        NativeValidationOwnerReservations owner(ValidationMemberResources.Member resources) {
            return new NativeValidationOwnerReservations(member, configuration, cache, scope, () -> WorkloadClaimFence.from(node), gate, resources, process, timers,
                    () -> localGuard.run(), Duration.ofSeconds(30), Duration.ofSeconds(5), maxPipelines, maxOperations, handoffClock,(saved,spec)->startup.inspect(saved,spec));
        }
        @Override public void close() {
            owners.close(); resources.close(); nodeLease.stopAndClose(member::shutdown); timers.close(); process.close(); configuration.close(); cache.close();
            raw.getDatabase(configName).drop(); raw.getDatabase(cacheName).drop(); raw.close();
        }
    }
}
