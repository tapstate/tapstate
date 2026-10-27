package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.ValidationTaskStore.Settings;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.spi.store.ValidationTaskStore.*;

/** Real CFG authoring and native control facts; these fixtures do not execute business checks. */
@RequiresDocker
class MongoValidationTaskStoreIT {
    @Container static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    static final Duration TTL = Duration.ofSeconds(30);

    @Test void configurationCreateAndEditRemainConfigOnlyWhenTheCacheIsAbsent() {
        try (var f = new Fixture()) {
            var create = f.create("flow"); long revision = f.configuration.configurationRevision(); f.cacheDb.drop();
            assertThat(f.tasks.create(create)).isEqualTo(Write.APPLIED);
            assertThat(f.configuration.configurationRevision()).isEqualTo(revision + 1);
            var original = f.read(create.task()); assertThat(original.revision()).isOne(); assertThat(original.schedulingRevision()).isOne();
            assertThat(original.full().nextStartSequence()).isOne(); assertThat(original.incremental().nextStartSequence()).isOne();
            assertThat(f.tasks.create(create)).isEqualTo(Write.DUPLICATE);
            var settings = new Settings(new Full(ValidationMethod.COUNT, 2, null, false, 20), new Incremental(2_000, 30));
            var update = new Update(create.task(), 1, create.comparison(), settings, UUID.randomUUID());
            assertThat(f.tasks.update(update)).isEqualTo(Write.APPLIED); long applied = f.configuration.configurationRevision();
            var current = f.read(create.task()); assertThat(current.revision()).isEqualTo(2); assertThat(current.schedulingRevision()).isEqualTo(2);
            assertThat(current.schedulingCommandId()).isEqualTo(update.commandId()); assertThat(current.settings()).isEqualTo(settings);
            assertThat(f.tasks.update(update)).isEqualTo(Write.DUPLICATE); assertThat(f.configuration.configurationRevision()).isEqualTo(applied);
            assertThat(f.tasks.create(create)).isEqualTo(Write.CONFLICT);
            assertThat(f.tasks.update(new Update(create.task(), 1, create.comparison(), create.settings(), UUID.randomUUID()))).isEqualTo(Write.CONFLICT);
            var replacement = new Create(new ValidationReportAdmission.Task("cluster", "flow", create.task().taskId(), UUID.randomUUID()), create.comparison(), settings, UUID.randomUUID());
            assertThat(f.tasks.create(replacement)).isEqualTo(Write.CONFLICT); assertThat(f.read(create.task())).isEqualTo(current);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            assertThat(f.cacheDb.listCollectionNames()).isEmpty();
        }
    }

    @Test void twoActualClientsAdvanceOneConfigurationAndSchedulingVersion() throws Exception {
        try (var f = new Fixture(); var other = MongoConfigurationDomain.open(f.settings, "cluster"); var workers = Executors.newFixedThreadPool(2)) {
            var create = f.create("flow"); f.tasks.create(create); long before = f.configuration.configurationRevision();
            var gate = new CountDownLatch(1); var a = new Update(create.task(), 1, create.comparison(), Settings.defaults(ValidationMethod.COUNT, 2_000), UUID.randomUUID());
            var b = new Update(create.task(), 1, create.comparison(), Settings.defaults(ValidationMethod.COUNT, 3_000), UUID.randomUUID());
            var one = workers.submit(() -> { gate.await(); return f.tasks.update(a); });
            var two = workers.submit(() -> { gate.await(); return other.validationTasks(() -> {}).update(b); }); gate.countDown();
            var first = one.get(15, TimeUnit.SECONDS); var second = two.get(15, TimeUnit.SECONDS);
            assertThat(List.of(first, second)).containsExactlyInAnyOrder(Write.APPLIED, Write.CONFLICT);
            var expected = first == Write.APPLIED ? a : b; var actual = f.read(create.task());
            assertThat(actual.revision()).isEqualTo(2); assertThat(actual.schedulingRevision()).isEqualTo(2);
            assertThat(actual.schedulingCommandId()).isEqualTo(expected.commandId()); assertThat(actual.settings()).isEqualTo(expected.settings());
            assertThat(f.configuration.configurationRevision()).isEqualTo(before + 1);
        }
    }

    @Test void definitionEditsPreserveTheExactOriginalStartAndDoNotApplyUnchangedDelayAgain() {
        try (var f = new Fixture()) {
            var create = f.create("flow"); f.tasks.create(create); var original = f.read(create.task()); var running = f.reserve(original, "original");
            assertThat(running.access().admissions().reserve(running.start())).isEqualTo(ValidationReportAdmission.Reservation.DUPLICATE);
            var pipeline = (PipelineResource) f.configuration.artifacts().get("flow").orElseThrow();
            var changed = new PipelineResource(pipeline.id(), new Metadata(Map.of(), "updated metadata"), pipeline.sources(), pipeline.transforms(), pipeline.view(), pipeline.serve(), pipeline.settings(), pipeline.experimental());
            assertThat(f.configuration.artifacts().replace("flow", CanonicalHash.of(pipeline), changed)).isEqualTo(ArtifactMutation.REPLACED);
            var comparison = f.comparison("flow"); var settings = new Settings(new Full(ValidationMethod.COUNT, 2, null, false, 10), original.settings().incremental());
            assertThat(f.tasks.update(new Update(create.task(), 1, comparison, settings, UUID.randomUUID()))).isEqualTo(Write.APPLIED);
            var updated = f.read(create.task()); assertThat(updated.full().permission().start()).isEqualTo(running.start());
            assertThat(updated.full().nextStartSequence()).isEqualTo(2); assertThat(updated.schedulingRevision()).isOne();
            assertThat(updated.schedulingCommandId()).isEqualTo(create.commandId()); assertThat(updated.comparison().fingerprint()).isNotEqualTo(original.comparison().fingerprint());
            var badIdentity = new ValidationRunIdentity(running.start().run().owner(), create.task().taskId(), "stale-spec", ValidationMode.FULL,
                    running.start().run().specHash(), 1, "rules-v1");
            var bad = new ValidationReportAdmission.Start(create.task(), 2, 2, UUID.randomUUID().toString(), badIdentity);
            running.access().admissions().close(running.start());
            assertThat(running.access().admissions().reserve(bad)).isEqualTo(ValidationReportAdmission.Reservation.STALE);
            assertThat(f.read(create.task()).full().nextStartSequence()).isEqualTo(2);
            assertThat(f.tasks.close(create.task())).isEqualTo(ValidationReportAdmission.Closure.CLOSED);
            assertThat(f.tasks.close(create.task())).isEqualTo(ValidationReportAdmission.Closure.ALREADY_CLOSED);
            assertThat(f.tasks.update(new Update(create.task(), 2, comparison, settings, UUID.randomUUID()))).isEqualTo(Write.CLOSED);
        }
    }

    @Test void staleDependenciesWrongResourceKindsAndOtherTaskLifetimesNeverWrite() {
        try (var f = new Fixture()) {
            var create = f.create("flow"); var source = (SourceResource) f.configuration.artifacts().get("input").orElseThrow();
            f.configuration.artifacts().replace("input", CanonicalHash.of(source), new SourceResource(source.id(), new Metadata(Map.of(), "changed"), source.connector(),
                    source.config(), source.mode(), source.tables(), source.srs(), source.execution(), source.experimental()));
            long revision = f.configuration.configurationRevision(); assertThat(f.tasks.create(create)).isEqualTo(Write.CONFLICT);
            assertThat(f.configuration.configurationRevision()).isEqualTo(revision); assertThat(f.tasks.read("flow", create.task().taskId())).isEmpty();
            var fresh = f.create("flow"); assertThat(f.tasks.create(fresh)).isEqualTo(Write.APPLIED);
            var otherLifetime = new ValidationReportAdmission.Task("cluster", "flow", fresh.task().taskId(), UUID.randomUUID());
            assertThat(f.tasks.close(otherLifetime)).isEqualTo(ValidationReportAdmission.Closure.STALE);
            assertThat(f.tasks.update(new Update(otherLifetime, 1, fresh.comparison(), fresh.settings(), UUID.randomUUID()))).isEqualTo(Write.CONFLICT);
            assertThat(f.read(fresh.task()).closed()).isFalse();
        }
    }

    @Test void localClosureAndMissingDefinitionFactsAreNeverSilentlyInitialized() {
        try (var f = new Fixture()) {
            var create = f.create("flow"); f.tasks.create(create); var live = new java.util.concurrent.atomic.AtomicBoolean(true);
            var guarded = f.configuration.validationTasks(() -> { if (!live.get()) throw new IllegalStateException("configuration context closed"); });
            live.set(false); var before = f.document(create.task());
            assertThatThrownBy(() -> guarded.update(new Update(create.task(), 1, create.comparison(), create.settings(), UUID.randomUUID()))).isInstanceOf(IllegalStateException.class);
            assertThat(f.document(create.task())).isEqualTo(before);
            f.collection().updateOne(new Document("_id", before.get("_id")), new Document("$unset", new Document("definition", "")));
            assertThatThrownBy(() -> f.tasks.read("flow", create.task().taskId())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.tasks.create(create)).isInstanceOf(TapstateException.class);
            assertThat(f.document(create.task())).doesNotContainKey("definition");
        }
    }

    @Test void aPipelineIdStartingWithDollarIsAnAggregationValueAndStillSupportsVersionedEdits() {
        try (var f = new Fixture()) {
            f.addPipeline("$flow"); var create = f.create("$flow"); assertThat(f.tasks.create(create)).isEqualTo(Write.APPLIED);
            var update = new Update(create.task(), 1, create.comparison(), Settings.defaults(ValidationMethod.COUNT, 2_000), UUID.randomUUID());
            assertThatCode(() -> assertThat(f.tasks.update(update)).isEqualTo(Write.APPLIED)).doesNotThrowAnyException();
            assertThat(f.read(create.task()).settings().incremental().firstCheckDelayMillis()).isEqualTo(2_000);
        }
    }

    @Test void mongoAndJavaChargeTheSameReservedBytesIncludingOversizedControlFacts() {
        try (var f = new Fixture()) {
            var create = f.create("flow"); f.tasks.create(create); var original = f.document(create.task());
            var malformed = new Document(original).append("_id", new Document("clusterId", "cluster").append("pipelineId", "flow").append("taskId", "oversized-control"))
                    .append("modes", new Document("unexpected", "x".repeat(32 * 1024)));
            f.collection().insertOne(malformed);
            for (var input : List.of(original, malformed)) {
                var output = f.collection().aggregate(List.of(new Document("$match", new Document("_id", input.get("_id"))),
                        new Document("$project", new Document("_id", 0).append("reserved", MongoValidationTaskCodec.reservedBytesExpression())))).first();
                assertThat(output.getLong("reserved")).isEqualTo(MongoValidationTaskCodec.reservedBytes(input));
                assertThat(output.getLong("reserved")).isGreaterThanOrEqualTo(MongoValidationTaskCodec.bsonBytes(input));
            }
        }
    }

    @Test void bornReportLossRollsBackTaskClosureAndTheConfigurationFence() {
        try (var f = new Fixture()) {
            var create = f.create("flow"); f.tasks.create(create); var reserved = f.reserve(f.read(create.task()), "born");
            assertThat(reserved.access().admissions().open(new ValidationReportAdmission.Open(reserved.start(),
                    new ValidationRunSpec(create.comparison(), ValidationMode.FULL, ValidationMethod.COUNT)))).isEqualTo(ValidationReportAdmission.Birth.CREATED);
            SystemCollections.VALIDATION_REPORTS.on(f.configDb).deleteOne(new Document("_id", MongoValidationAdmissionStore.reportId(reserved.start().run())));
            var before = f.document(create.task()); long revision = f.configuration.configurationRevision();
            assertThatThrownBy(() -> f.tasks.close(create.task())).isInstanceOf(TapstateException.class).hasMessageContaining("original-report");
            assertThat(f.document(create.task())).isEqualTo(before); assertThat(f.configuration.configurationRevision()).isEqualTo(revision);
            assertThat(f.read(create.task()).closed()).isFalse(); assertThat(f.read(create.task()).full().permission().phase()).isEqualTo(Phase.BORN);
        }
    }

    @Test void aPipelineDefinitionCannotBeUsedAsAPhysicalConnectionResource() {
        try (var f = new Fixture()) {
            var original = f.comparison("flow"); var mapping = original.mappings().getFirst();
            var wrong = new ValidationTaskSpec("flow", original.dependencyHashes(), 1, "rules-v1", List.of(new ValidationTaskSpec.Mapping(mapping.mappingId(), mapping.outputKind(), mapping.outputId(),
                    mapping.source(), new ValidationTaskSpec.Table("out", "flow", "master", null, "orders", null, null), null)));
            var create = new Create(new ValidationReportAdmission.Task("cluster", "flow", UUID.randomUUID().toString(), UUID.randomUUID()), wrong,
                    Settings.defaults(ValidationMethod.COUNT, 1_000), UUID.randomUUID());
            long before = f.configuration.configurationRevision(); assertThat(f.tasks.create(create)).isEqualTo(Write.CONFLICT);
            assertThat(f.configuration.configurationRevision()).isEqualTo(before); assertThat(f.tasks.read("flow", create.task().taskId())).isEmpty();
        }
    }

    @Test void authoringNeverExceedsTheRecoveryHeaderCountAndClosingStillWorksAtThatCapacity() {
        try (var f = new Fixture()) {
            var rows = new ArrayList<Document>();
            for (int i = 0; i < MongoValidationAdmissionStore.MAX_TASKS_PER_PIPELINE; i++) rows.add(new Document("_id",
                    new Document("clusterId", "cluster").append("pipelineId", "flow").append("taskId", "retained-" + i)).append("formatVersion", 1)
                    .append("taskGeneration", UUID.randomUUID().toString()).append("revision", 1L).append("closed", false)
                    .append("modes", new Document("full", new Document("nextStartSequence", 1L)).append("incremental", new Document("nextStartSequence", 1L))));
            f.collection().insertMany(rows); var create = f.create("flow"); long revision = f.configuration.configurationRevision();
            assertThatThrownBy(() -> f.tasks.create(create)).isInstanceOf(TapstateException.class).hasMessageContaining("validation.taskCount");
            assertThat(f.collection().countDocuments()).isEqualTo(MongoValidationAdmissionStore.MAX_TASKS_PER_PIPELINE);
            assertThat(f.configuration.configurationRevision()).isEqualTo(revision);
            var retained = rows.getFirst(); var id = retained.get("_id", Document.class);
            var task = new ValidationReportAdmission.Task("cluster", "flow", id.getString("taskId"), UUID.fromString(retained.getString("taskGeneration")));
            assertThat(f.tasks.close(task)).isEqualTo(ValidationReportAdmission.Closure.CLOSED);
            assertThat(f.collection().find(new Document("_id", id)).first()).containsEntry("closed", true);
            assertThatThrownBy(() -> f.tasks.create(create)).isInstanceOf(TapstateException.class).hasMessageContaining("validation.taskCount");
        }
    }

    @Test void bornReportsFreezeTheActualOriginalSpecAndMissingSnapshotsNeverAcknowledgeReadiness() {
        try (var f = new Fixture()) {
            var create = f.create("flow"); f.tasks.create(create); var reserved = f.reserve(f.read(create.task()), "frozen");
            var expected = new ValidationRunSpec(create.comparison(), ValidationMode.FULL, ValidationMethod.COUNT);
            var admissions = reserved.access().admissions();
            assertThatThrownBy(() -> admissions.open(reserved.start())).isInstanceOf(TapstateException.class).hasMessageContaining("report.specSnapshot");
            assertThat(admissions.open(new ValidationReportAdmission.Open(reserved.start(), expected))).isEqualTo(ValidationReportAdmission.Birth.CREATED);
            var pipeline = (PipelineResource) f.configuration.artifacts().get("flow").orElseThrow();
            f.configuration.artifacts().replace("flow", CanonicalHash.of(pipeline), new PipelineResource("flow", new Metadata(Map.of(), "changed after birth"),
                    pipeline.sources(), pipeline.transforms(), pipeline.view(), pipeline.serve(), pipeline.settings(), pipeline.experimental()));
            var current = f.comparison("flow");
            assertThat(f.tasks.update(new Update(create.task(), 1, current, Settings.defaults(ValidationMethod.COUNT, 2_000), UUID.randomUUID()))).isEqualTo(Write.APPLIED);
            var reports = SystemCollections.VALIDATION_REPORTS.on(f.configDb); var id = new Document("_id", MongoValidationAdmissionStore.reportId(reserved.start().run()));
            assertThat(MongoValidationTaskCodec.run(reports.find(id).first().get("specSnapshot", Document.class))).isEqualTo(expected);
            assertThat(admissions.open(reserved.start())).isEqualTo(ValidationReportAdmission.Birth.DUPLICATE);
            assertThatThrownBy(() -> new ValidationReportAdmission.Open(reserved.start(), new ValidationRunSpec(current, ValidationMode.FULL, ValidationMethod.COUNT)))
                    .isInstanceOf(TapstateException.class);
            var controls = reserved.access().control(); var run = new ValidationControlStore.Run(reserved.start().run(), 1, reserved.start());
            assertThat(controls.join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
            reports.updateOne(id, new Document("$unset", new Document("specSnapshot", "")));
            assertThatThrownBy(() -> controls.transition(new ValidationControlStore.Transition(run, ValidationRunState.STARTING, ValidationRunState.RUNNING)))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("original-spec-snapshot");
            assertThat(f.tasks.close(create.task())).isEqualTo(ValidationReportAdmission.Closure.CLOSED);
            assertThat(reports.find(id).first()).containsEntry("closed", true);
        }
    }

    @Test void taskCreationReservesFutureAdmissionBytesBeforeAcceptingANearLimitDefinition() {
        try (var f = new Fixture()) {
            Create selected = null;
            for (int low = 1, high = 500; low <= high;) {
                int length = (low + high) >>> 1; var candidate = f.create(f.denseComparison("flow", length));
                int bytes = MongoValidationTaskCodec.bsonBytes(MongoValidationTaskCodec.initial(candidate));
                if (bytes <= MongoValidationTaskCodec.MAX_TASK_BYTES) { selected = candidate; low = length + 1; } else high = length - 1;
            }
            assertThat(selected).isNotNull(); var candidate = selected; var row = MongoValidationTaskCodec.initial(candidate);
            assertThat(MongoValidationTaskCodec.bsonBytes(row)).isLessThanOrEqualTo(MongoValidationTaskCodec.MAX_TASK_BYTES);
            assertThat(MongoValidationTaskCodec.reservedBytes(row)).isGreaterThan(MongoValidationTaskCodec.MAX_TASK_BYTES);
            long before = f.configuration.configurationRevision();
            assertThatThrownBy(() -> f.tasks.create(candidate)).isInstanceOf(TapstateException.class).hasMessageContaining("validation.taskBytes");
            assertThat(f.configuration.configurationRevision()).isEqualTo(before); assertThat(f.tasks.read("flow", candidate.task().taskId())).isEmpty();
        }
    }

    @Test void twoClientsCannotOverbookTheLiteralDollarPipelineBudgetAndClosureDoesNotPretendToFreeBytes() throws Exception {
        try (var f = new Fixture(); var other = MongoConfigurationDomain.open(f.settings, "cluster"); var workers = Executors.newFixedThreadPool(2)) {
            f.addPipeline("$quota"); var spec = f.denseComparison("$quota", 350); var sample = f.create(spec);
            long charge = MongoValidationTaskCodec.reservedBytes(MongoValidationTaskCodec.initial(sample));
            int fit = (int) (MongoValidationTaskCodec.MAX_PIPELINE_TASK_BYTES / charge); assertThat(fit).isBetween(2, 200);
            var created = new ArrayList<Create>();
            for (int n = 0; n < fit - 1; n++) { var request = f.create(spec); assertThat(f.tasks.create(request)).isEqualTo(Write.APPLIED); created.add(request); }
            var a = f.create(spec); var b = f.create(spec); var gate = new CountDownLatch(1); long before = f.configuration.configurationRevision();
            var one = workers.submit(() -> { gate.await(); return capacityCreate(f.tasks, a, "validation.pipelineTaskBytes"); });
            var two = workers.submit(() -> { gate.await(); return capacityCreate(other.validationTasks(() -> {}), b, "validation.pipelineTaskBytes"); }); gate.countDown();
            assertThat(List.of(one.get(30, TimeUnit.SECONDS), two.get(30, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            assertThat(f.collection().countDocuments(new Document("_id.pipelineId", "$quota"))).isEqualTo(fit);
            assertThat(f.configuration.configurationRevision()).isEqualTo(before + 1);
            assertThat(f.tasks.close(created.getFirst().task())).isEqualTo(ValidationReportAdmission.Closure.CLOSED);
            assertThat(capacityCreate(f.tasks, f.create(spec), "validation.pipelineTaskBytes")).isFalse();
            assertThat(f.collection().countDocuments(new Document("_id.pipelineId", "$quota"))).isEqualTo(fit);
        }
    }

    @Test void differentPipelinesStillCompeteForOneClusterMetadataBudget() throws Exception {
        try (var f = new Fixture(); var other = MongoConfigurationDomain.open(f.settings, "cluster"); var workers = Executors.newFixedThreadPool(2)) {
            var specs = new ArrayList<ValidationTaskSpec>();
            for (String pipeline : List.of("quota-a", "quota-b", "quota-c", "quota-d", "quota-e")) { f.addPipeline(pipeline); specs.add(f.denseComparison(pipeline, 350)); }
            long charge = MongoValidationTaskCodec.reservedBytes(MongoValidationTaskCodec.initial(f.create(specs.getFirst())));
            int perPipeline = (int) (MongoValidationTaskCodec.MAX_PIPELINE_TASK_BYTES / charge);
            int perCluster = (int) (MongoValidationTaskCodec.MAX_CLUSTER_TASK_BYTES / charge); assertThat(perCluster).isBetween(8, 800);
            int count = 0;
            for (int pipeline = 0; pipeline < 4; pipeline++) {
                int limit = perPipeline - (pipeline == 3 ? 1 : 0);
                for (int n = 0; n < limit; n++) { assertThat(f.tasks.create(f.create(specs.get(pipeline)))).isEqualTo(Write.APPLIED); count++; }
            }
            var last = specs.get(4);
            while (count < perCluster - 1) { assertThat(f.tasks.create(f.create(last))).isEqualTo(Write.APPLIED); count++; }
            var a = f.create(last); var b = f.create(last); var gate = new CountDownLatch(1); long before = f.configuration.configurationRevision();
            var one = workers.submit(() -> { gate.await(); return capacityCreate(f.tasks, a, "validation.clusterTaskBytes"); });
            var two = workers.submit(() -> { gate.await(); return capacityCreate(other.validationTasks(() -> {}), b, "validation.clusterTaskBytes"); }); gate.countDown();
            assertThat(List.of(one.get(30, TimeUnit.SECONDS), two.get(30, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            assertThat(f.collection().countDocuments()).isEqualTo(perCluster); assertThat(f.configuration.configurationRevision()).isEqualTo(before + 1);
        }
    }

    @Test void originalSnapshotSurvivesAnEditBeforeBirthAndCompetesWithConfigurationClosure() throws Exception {
        try (var f = new Fixture(); var other = MongoConfigurationDomain.open(f.settings, "cluster"); var workers = Executors.newFixedThreadPool(2)) {
            var create = f.create("flow"); f.tasks.create(create); var reserved = f.reserve(f.read(create.task()), "before-edit");
            var snapshot = new ValidationRunSpec(create.comparison(), ValidationMode.FULL, ValidationMethod.COUNT);
            var pipeline = (PipelineResource) f.configuration.artifacts().get("flow").orElseThrow();
            f.configuration.artifacts().replace("flow", CanonicalHash.of(pipeline), new PipelineResource("flow", new Metadata(Map.of(), "edit before report birth"),
                    pipeline.sources(), pipeline.transforms(), pipeline.view(), pipeline.serve(), pipeline.settings(), pipeline.experimental()));
            var edited = f.comparison("flow"); assertThat(edited.fingerprint()).isNotEqualTo(create.comparison().fingerprint());
            assertThat(f.tasks.update(new Update(create.task(), 1, edited, create.settings(), UUID.randomUUID()))).isEqualTo(Write.APPLIED);
            var gate = new CountDownLatch(1);
            var birth = workers.submit(() -> { gate.await(); return reserved.access().admissions().open(new ValidationReportAdmission.Open(reserved.start(), snapshot)); });
            var close = workers.submit(() -> { gate.await(); return other.validationTasks(() -> {}).close(create.task()); }); gate.countDown();
            var opened = birth.get(15, TimeUnit.SECONDS); assertThat(opened).isIn(ValidationReportAdmission.Birth.CREATED, ValidationReportAdmission.Birth.CLOSED);
            assertThat(close.get(15, TimeUnit.SECONDS)).isEqualTo(ValidationReportAdmission.Closure.CLOSED);
            var current = f.read(create.task()); assertThat(current.closed()).isTrue(); assertThat(current.full().permission().phase()).isEqualTo(Phase.CLOSED);
            assertThat(current.full().permission().start()).isEqualTo(reserved.start());
            var report = SystemCollections.VALIDATION_REPORTS.on(f.configDb).find().first();
            if (report == null) assertThat(opened).isEqualTo(ValidationReportAdmission.Birth.CLOSED);
            else { assertThat(report).containsEntry("closed", true); assertThat(MongoValidationTaskCodec.run(report.get("specSnapshot", Document.class))).isEqualTo(snapshot); }
            assertThat(reserved.access().admissions().open(reserved.start())).isEqualTo(ValidationReportAdmission.Birth.CLOSED);
        }
    }

    private static boolean capacityCreate(ValidationTaskStore tasks, Create request, String limit) {
        try { assertThat(tasks.create(request)).isEqualTo(Write.APPLIED); return true; }
        catch (TapstateException refusal) {
            assertThat(refusal.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED); assertThat(refusal.getMessage()).contains(limit); return false;
        }
    }

    record Reserved(MongoConfigurationDomain.ValidationAccess access, ValidationReportAdmission.Start start) {}
    static final class Fixture implements AutoCloseable {
        final String suffix = UUID.randomUUID().toString().replace("-", "");
        final com.mongodb.client.MongoClient configClient = MongoClients.create(CONFIG.getReplicaSetUrl()), cacheClient = MongoClients.create(CACHE.getReplicaSetUrl());
        final com.mongodb.client.MongoDatabase configDb = configClient.getDatabase("task_config_" + suffix), cacheDb = cacheClient.getDatabase("task_cache_" + suffix);
        final MongoConnectionSettings settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configDb.getName()), null, Duration.ofSeconds(5));
        final MongoConfigurationDomain configuration = MongoConfigurationBootstrap.prepare(settings, "cluster", "installation", "setup", 1);
        final StorageBinding.Connection endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheDb.getName()), cacheDb.getName(), null);
        final MongoCacheDomain cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "validation-task-fixture");
        final CacheBusinessScope scope; final ValidationTaskStore tasks;
        Fixture() {
            var master = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://business.invalid/master"), null, null, null, null);
            var binding = configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster", "installation", "setup", 1), StorageBinding.Mode.OP, endpoint, master);
            scope = binding.preparation().businessScope(); cache.prepareFirstAuth(binding.firstCreation().orElseThrow()); cache.prepareInitialBusiness(binding.preparation(), () -> {});
            configuration.artifacts().create(new SourceResource("input", null, "mongodb", Map.of("uri", "mongodb://business.invalid/source"), SourceMode.CDC,
                    List.of(TableRef.literal("orders")), null, null));
            addPipeline("flow");
            try (var prepare = MongoConfigurationDomain.prepareValidationSchema(settings, "cluster", UUID.randomUUID())) { prepare.prepareCache(cache); while (!prepare.advance(20).ready()) {} }
            tasks = configuration.validationTasks(() -> {});
        }
        void addPipeline(String id) {
            configuration.artifacts().create(new PipelineResource(id, null, List.of(SourceRef.spec("input", false)), null, null,
                    new ServeBlock.Inline("serve", FromRef.literal("orders"), List.of(new SyncElement("out", "views", null, null, null)), null, null), null, null));
        }
        ValidationTaskSpec comparison(String pipeline) {
            var hashes = new TreeMap<String, String>(); for (var id : List.of(pipeline, "input", "views")) hashes.put(id, CanonicalHash.of(configuration.artifacts().get(id).orElseThrow()));
            return new ValidationTaskSpec(pipeline, hashes, 1, "rules-v1", List.of(new ValidationTaskSpec.Mapping("mapping", ValidationTaskSpec.OutputKind.SYNC, "out",
                    new ValidationTaskSpec.Table("input", "input", "source", null, "orders", null, null),
                    new ValidationTaskSpec.Table("out", "views", "master", null, "orders", null, null), null)));
        }
        Create create(String pipeline) { return create(comparison(pipeline)); }
        Create create(ValidationTaskSpec spec) { return new Create(new ValidationReportAdmission.Task("cluster", spec.pipelineId(), UUID.randomUUID().toString(), UUID.randomUUID()), spec, Settings.defaults(ValidationMethod.COUNT, 1_000), UUID.randomUUID()); }
        ValidationTaskSpec denseComparison(String pipeline, int textLength) {
            var base = comparison(pipeline); var types = new LinkedHashSet<String>(); for (int n = 0; n < 32; n++) types.add("type-" + n + "-" + "x".repeat(textLength));
            var fields = new ArrayList<ValidationKeyBinding.Component>(); for (int n = 0; n < 8; n++) fields.add(new ValidationKeyBinding.Component(List.of("field-" + n), ValidationKey.Kind.EXACT_NUMBER, types));
            var mapping = base.mappings().getFirst();
            // COUNT does not read this optional configured key metadata; its actual stored bytes still consume capacity.
            return new ValidationTaskSpec(pipeline, base.dependencyHashes(), 1, "rules-v1", List.of(new ValidationTaskSpec.Mapping("mapping", mapping.outputKind(), mapping.outputId(),
                    new ValidationTaskSpec.Table("input", "input", "source", null, "orders", "configured-model", "configured-codec"),
                    new ValidationTaskSpec.Table("out", "views", "master", null, "orders", "configured-model", "configured-codec"),
                    new ValidationTaskSpec.Keys("configured-confirmation", fields, fields))));
        }
        Snapshot read(ValidationReportAdmission.Task task) { return tasks.read(task.pipelineId(), task.taskId()).orElseThrow(); }
        Document document(ValidationReportAdmission.Task task) { return collection().find(new Document("_id", MongoValidationAdmissionStore.taskId(task))).first(); }
        com.mongodb.client.MongoCollection<Document> collection() { return SystemCollections.VALIDATION_TASKS.on(configDb); }
        Reserved reserve(Snapshot snapshot, String runId) {
            var node = cache.workloadClaims(scope).acquire(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"), new WorkloadOwner("node", "boot"), 1, TTL).claim();
            var access = configuration.prepareValidationAccess(snapshot.task().pipelineId(), scope, cache, WorkloadClaimFence.from(node), () -> {});
            var claims = access.claims(); var acquired = claims.acquire(ValidationClaims.key("cluster", snapshot.task().pipelineId()), node.owner(), 1, TTL).claim();
            var claim = claims.advanceExecution(acquired, 1, Set.of(node.owner().nodeId())).orElseThrow();
            var spec = new ValidationRunSpec(snapshot.comparison(), ValidationMode.FULL, snapshot.settings().full().method());
            var identity = new ValidationRunIdentity(ValidationClaims.owner(snapshot.task().pipelineId(), claim), snapshot.task().taskId(), runId, spec.mode(), spec.fingerprint(),
                    snapshot.comparison().mappingRevision(), snapshot.comparison().comparisonRulesVersion());
            var start = new ValidationReportAdmission.Start(snapshot.task(), snapshot.revision(), snapshot.full().nextStartSequence(), UUID.randomUUID().toString(), identity);
            assertThat(access.admissions().reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED); return new Reserved(access, start);
        }
        @Override public void close() { try { configDb.drop(); cacheDb.drop(); } finally { configuration.close(); cache.close(); configClient.close(); cacheClient.close(); } }
    }
}
