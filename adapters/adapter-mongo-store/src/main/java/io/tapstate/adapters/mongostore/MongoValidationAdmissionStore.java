package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ValidationReportAdmission;
import io.tapstate.spi.store.ValidationTaskStore;
import org.bson.Document;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Prepared CONFIG resources only. Neither missing tasks nor missing reports are recreated by retry. */
final class MongoValidationAdmissionStore implements ValidationReportAdmission {
    static final int MAX_TASKS_PER_PIPELINE = 1024;
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).readPreference(ReadPreference.primary()).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final Runnable requireActive;
    private final MongoCollection<Document> tasks;
    private final MongoCollection<Document> reports;
    private final MongoValidationPreparations.Authority validation;
    private final MongoCacheRecoveryStore.ValidationClosure recovery;

    MongoValidationAdmissionStore(MongoClient client, MongoDatabase database, String clusterId, Runnable requireActive) {
        this(client, database, clusterId, requireActive, null);
    }
    MongoValidationAdmissionStore(MongoClient client, MongoDatabase database, String clusterId, Runnable requireActive,
            MongoValidationPreparations.Authority validation) {
        this(client, database, clusterId, requireActive, validation, null);
    }
    private MongoValidationAdmissionStore(MongoClient client, MongoDatabase database, String clusterId, Runnable requireActive,
            MongoValidationPreparations.Authority validation, MongoCacheRecoveryStore.ValidationClosure recovery) {
        this.client = Objects.requireNonNull(client); this.database = Objects.requireNonNull(database);
        this.clusterId = Objects.requireNonNull(clusterId); this.requireActive = Objects.requireNonNull(requireActive);
        tasks = SystemCollections.VALIDATION_TASKS.on(database); reports = SystemCollections.VALIDATION_REPORTS.on(database);
        this.validation = validation;
        this.recovery = recovery;
    }
    static MongoValidationAdmissionStore forRecovery(MongoClient client, MongoDatabase database, String clusterId, Runnable requireActive,
            MongoCacheRecoveryStore.ValidationClosure original) {
        return new MongoValidationAdmissionStore(client, database, clusterId, requireActive, null, Objects.requireNonNull(original));
    }

    static ValidationTaskStore taskDefinitions(MongoClient client, MongoDatabase database, String cluster, Runnable active,
            MongoConfigurationArtifactStore artifacts) {
        if (!artifacts.belongsTo(client, database.getName())) throw new IllegalArgumentException("validation definitions need the original CONFIG client");
        return new Definitions(new MongoValidationAdmissionStore(client, database, cluster, active), artifacts);
    }

    private static final class Definitions implements ValidationTaskStore {
        private final MongoValidationAdmissionStore owner;
        private final MongoConfigurationArtifactStore artifacts;
        private Definitions(MongoValidationAdmissionStore owner, MongoConfigurationArtifactStore artifacts) { this.owner = owner; this.artifacts = artifacts; }

        @Override public java.util.Optional<Snapshot> read(String pipeline, String task) {
            identifier(pipeline); identifier(task);
            return call(session -> java.util.Optional.ofNullable(document(session, pipeline, task)).map(row -> MongoValidationTaskCodec.snapshot(row, owner.clusterId)));
        }

        @Override public Page list(String pipeline,int offset,int limit){
            identifier(pipeline);if(offset<0||offset>MAX_TASKS_PER_PIPELINE||limit<1||limit>20)throw taskLimit("validation.taskPage");
            return call(session->{
                var filter=new Document("_id.clusterId",owner.clusterId).append("_id.pipelineId",pipeline);var result=new java.util.ArrayList<Snapshot>();
                try(var cursor=owner.tasks.find(session,filter).sort(new Document("_id.taskId",1)).skip(offset).limit(limit).iterator()){
                    while(cursor.hasNext())result.add(MongoValidationTaskCodec.snapshot(cursor.next(),owner.clusterId));
                }
                long total=owner.tasks.countDocuments(session,filter);if(total>MAX_TASKS_PER_PIPELINE)throw taskLimit("validation.taskCount");
                return new Page(result,total,offset,limit);
            });
        }
        @Override public java.util.List<String> taskIds(String pipeline,String after,int limit){
            identifier(pipeline);if(after!=null)identifier(after);if(limit<1||limit>20)throw taskLimit("validation.taskPage");
            return call(session->{
                var filter=new Document("_id.clusterId",owner.clusterId).append("_id.pipelineId",pipeline);
                if(after!=null)filter.put("_id.taskId",new Document("$gt",after));var result=new java.util.ArrayList<String>();
                for(var row:owner.tasks.find(session,filter).projection(new Document("_id.taskId",1)).sort(new Document("_id.taskId",1)).limit(limit)){
                    var id=row.get("_id",Document.class).getString("taskId");identifier(id);result.add(id);
                }
                return java.util.List.copyOf(result);
            });
        }
        @Override public java.util.List<String> pipelines(String after,int limit){
            if(after!=null)identifier(after);if(limit<1||limit>64)throw taskLimit("validation.pipelinePage");
            return call(session->{
                var filter=new Document("_id.clusterId",owner.clusterId);if(after!=null)filter.put("_id.pipelineId",new Document("$gt",after));
                var stages=java.util.List.of(new Document("$match",filter),new Document("$group",new Document("_id","$_id.pipelineId")),
                        new Document("$sort",new Document("_id",1)),new Document("$limit",limit));
                var result=new java.util.ArrayList<String>();
                for(var row:owner.tasks.aggregate(session,stages)){var pipeline=row.getString("_id");identifier(pipeline);result.add(pipeline);}
                return java.util.List.copyOf(result);
            });
        }

        @Override public DeclarationInputs declarations(Selection selection){
            Objects.requireNonNull(selection);
            return call(session->{
                var parent=MongoValidationPreparations.taskParentGeneration(session,owner.database,owner.clusterId,selection.pipelineId());
                long current=ConfigurationVersion.read(session,SystemCollections.CONFIGURATION_VERSION.on(owner.database));
                return artifacts.validationDeclarationsInTransaction(session,selection,current,parent);
            });
        }

        @Override public java.util.Optional<Inputs> inputs(Task task, long revision) {
            Objects.requireNonNull(task); scope(task);
            if (revision < 1 || revision == Long.MAX_VALUE) throw unreadable("task-revision");
            return call(session -> {
                var row = document(session, task.pipelineId(), task.taskId()); if (row == null) return java.util.Optional.empty();
                var saved = MongoValidationTaskCodec.snapshot(row, owner.clusterId);
                if (!saved.task().equals(task) || saved.revision() != revision || saved.closed()) return java.util.Optional.empty();
                MongoValidationPreparations.requireTaskAuthoringParent(session, owner.database, owner.clusterId, task.pipelineId());
                long current = ConfigurationVersion.read(session, SystemCollections.CONFIGURATION_VERSION.on(owner.database));
                return artifacts.validationInputsInTransaction(session, saved.comparison().dependencyHashes()).map(resources -> new Inputs(saved, current, resources));
            });
        }

        @Override public Write create(Create command) {
            Objects.requireNonNull(command); scope(command.task());
            return call(session -> {
                var existing = document(session, command.task().pipelineId(), command.task().taskId());
                if (existing != null) {
                    var current = MongoValidationTaskCodec.snapshot(existing, owner.clusterId);
                    if (!current.task().equals(command.task())) return Write.CONFLICT;
                    if (current.closed()) return Write.CLOSED;
                    return current.revision() == 1 && current.commandId().equals(command.commandId()) && same(current, command.comparison(), command.settings())
                            ? Write.DUPLICATE : Write.CONFLICT;
                }
                MongoValidationPreparations.requireTaskAuthoringParent(session, owner.database, owner.clusterId, command.task().pipelineId());
                if (!currentResources(session, command.comparison())) return Write.CONFLICT;
                var row = MongoValidationTaskCodec.initial(command);
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(owner.database), owner.client);
                capacity(session, command.task().pipelineId(), null, row);
                owner.tasks.insertOne(session, row); return Write.APPLIED;
            });
        }

        @Override public Created createFresh(FreshCreate command){
            Objects.requireNonNull(command);var candidate=java.util.UUID.randomUUID();
            var task=new Task(owner.clusterId,command.pipelineId(),command.taskId(),candidate);
            return call(session->{
                var parent=MongoValidationPreparations.taskParentGeneration(session,owner.database,owner.clusterId,command.pipelineId());
                if(!parent.equals(command.parentGeneration()))return new Created(Write.CONFLICT,null);
                var existing=document(session,command.pipelineId(),command.taskId());
                if(existing!=null){
                    var current=MongoValidationTaskCodec.snapshot(existing,owner.clusterId);
                    if(current.closed())return new Created(Write.CLOSED,null);
                    return current.revision()==1&&current.commandId().equals(command.commandId())&&same(current,command.comparison(),command.settings())
                            ?new Created(Write.DUPLICATE,current.task().generation()):new Created(Write.CONFLICT,null);
                }
                if(!currentResources(session,command.comparison()))return new Created(Write.CONFLICT,null);
                if(!MongoValidationPreparations.taskParentGeneration(session,owner.database,owner.clusterId,command.pipelineId(),true).equals(parent))throw MongoValidationPreparations.fenced();
                var row=MongoValidationTaskCodec.initial(new Create(task,command.comparison(),command.settings(),command.commandId()));
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(owner.database), owner.client);capacity(session,task.pipelineId(),null,row);
                owner.tasks.insertOne(session,row);return new Created(Write.APPLIED,candidate);
            });
        }

        @Override public Write update(Update command) {
            Objects.requireNonNull(command); scope(command.task());
            return call(session -> {
                var previous = document(session, command.task().pipelineId(), command.task().taskId());
                if (previous == null) return Write.CONFLICT;
                var current = MongoValidationTaskCodec.snapshot(previous, owner.clusterId);
                if (!current.task().equals(command.task())) return Write.CONFLICT;
                if (current.closed()) return Write.CLOSED;
                if (current.commandId().equals(command.commandId())) return current.revision() == command.expectedRevision() + 1
                        && same(current, command.comparison(), command.settings()) ? Write.DUPLICATE : Write.CONFLICT;
                if (current.revision() != command.expectedRevision()) return Write.CONFLICT;
                boolean changedMapping = !current.comparison().mappings().equals(command.comparison().mappings());
                if (changedMapping ? command.comparison().mappingRevision() <= current.comparison().mappingRevision()
                        : command.comparison().mappingRevision() != current.comparison().mappingRevision()) throw unreadable("mapping-revision");
                MongoValidationPreparations.requireTaskAuthoringParent(session, owner.database, owner.clusterId, command.task().pipelineId());
                if (!currentResources(session, command.comparison())) return Write.CONFLICT;
                var next = MongoValidationTaskCodec.update(previous, current, command);
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(owner.database), owner.client);
                capacity(session, command.task().pipelineId(), previous, next);
                if (owner.tasks.replaceOne(session, new Document("_id", previous.get("_id")).append("taskGeneration", previous.get("taskGeneration"))
                        .append("revision", current.revision()), next).getMatchedCount() != 1) throw unreadable("task-definition-cas");
                return Write.APPLIED;
            });
        }

        @Override public Write disableSchedule(DisableSchedule command){
            Objects.requireNonNull(command);scope(command.task());
            return call(session->{
                var raw=owner.tasks.withDocumentClass(org.bson.RawBsonDocument.class).find(session,new Document("_id",taskId(command.task()))).batchSize(1).first();
                if(raw==null)return Write.CONFLICT;
                if(raw.getByteLength()>Inputs.MAX_STORED_BYTES)throw taskLimit("validation.taskDefinitionBytes");
                var previous=raw.decode(owner.tasks.getCodecRegistry().get(Document.class));var current=MongoValidationTaskCodec.snapshot(previous,owner.clusterId);
                if(!current.task().equals(command.task()))return Write.CONFLICT;if(current.closed())return Write.CLOSED;
                if(current.commandId().equals(command.commandId()))return current.revision()==command.expectedRevision()+1&&!current.settings().full().scheduleEnabled()?Write.DUPLICATE:Write.CONFLICT;
                if(current.revision()!=command.expectedRevision())return Write.CONFLICT;
                var full=current.settings().full();var settings=new Settings(new Full(full.method(),full.maxConcurrentTables(),full.schedule(),false,full.historyLimit()),current.settings().incremental());
                var next=MongoValidationTaskCodec.update(previous,current,new Update(current.task(),current.revision(),current.comparison(),settings,command.commandId()));
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(owner.database), owner.client);capacity(session,current.task().pipelineId(),previous,next);
                if(owner.tasks.replaceOne(session,new Document("_id",previous.get("_id")).append("taskGeneration",previous.get("taskGeneration")).append("revision",current.revision()),next).getMatchedCount()!=1)throw unreadable("task-schedule-cas");
                return Write.APPLIED;
            });
        }

        @Override public Closure close(Task task) {
            Objects.requireNonNull(task); scope(task);
            return call(session -> {
                var row = owner.tasks.find(session, new Document("_id", taskId(task))).projection(taskHeader()).first();
                if (row == null || validateTask(row, task, true) == null) return Closure.STALE;
                boolean wasClosed = Boolean.TRUE.equals(row.get("closed"));
                boolean pending = false;
                for (var selected : ValidationMode.values()) { var permission = permission(mode(row, selected)); if (permission != null && !phase(permission).equals("CLOSED")) pending = true; }
                if (wasClosed && !pending) return Closure.ALREADY_CLOSED;
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(owner.database), owner.client);
                if (owner.tasks.updateOne(session, new Document("_id", row.get("_id")).append("taskGeneration", row.get("taskGeneration"))
                        .append("revision", row.get("revision")), new Document("$set", new Document("closed", true))).getMatchedCount() != 1) throw unreadable("task-close");
                for (var selected : ValidationMode.values()) { var permission = permission(mode(row, selected)); if (permission != null) owner.closeInTransaction(session, decodeIdentity(permission.get("identity", Document.class))); }
                return Closure.CLOSED;
            });
        }

        private boolean currentResources(ClientSession session, io.tapstate.core.validation.ValidationTaskSpec comparison) {
            return matchingResources(artifacts.versionsInTransaction(session, comparison.dependencyHashes().keySet()), comparison);
        }

        private Document document(ClientSession session, String pipeline, String task) {
            return owner.definitionDocument(session, pipeline, task);
        }

        private void capacity(ClientSession session, String pipeline, Document previous, Document next) {
            long needed = MongoValidationTaskCodec.reservedBytes(next), prior = previous == null ? 0 : MongoValidationTaskCodec.reservedBytes(previous);
            if (needed > MongoValidationTaskCodec.MAX_TASK_BYTES) throw taskLimit("validation.taskBytes");
            var samePipeline = new Document("$eq", java.util.List.of("$_id.pipelineId", new Document("$literal", pipeline)));
            var size = MongoValidationTaskCodec.reservedBytesExpression();
            var sums = owner.tasks.aggregate(session, java.util.List.of(new Document("$match", new Document("_id.clusterId", owner.clusterId)),
                    new Document("$group", new Document("_id", null).append("clusterBytes", new Document("$sum", size))
                            .append("pipelineBytes", new Document("$sum", new Document("$cond", java.util.List.of(samePipeline, size, 0L))))
                            .append("pipelineCount", new Document("$sum", new Document("$cond", java.util.List.of(samePipeline, 1L, 0L))))))).first();
            long clusterBytes = sums == null ? 0 : aggregateNumber(sums, "clusterBytes"), pipelineBytes = sums == null ? 0 : aggregateNumber(sums, "pipelineBytes");
            long count = sums == null ? 0 : aggregateNumber(sums, "pipelineCount");
            if (clusterBytes < prior || pipelineBytes < prior) throw unreadable("task-accounting");
            if (previous == null && count >= MAX_TASKS_PER_PIPELINE) throw taskLimit("validation.taskCount");
            long increase = needed - prior;
            if (increase > 0 && pipelineBytes > MongoValidationTaskCodec.MAX_PIPELINE_TASK_BYTES - increase) throw taskLimit("validation.pipelineTaskBytes");
            if (increase > 0 && clusterBytes > MongoValidationTaskCodec.MAX_CLUSTER_TASK_BYTES - increase) throw taskLimit("validation.clusterTaskBytes");
        }

        private <T> T call(Function<ClientSession, T> action) {
            return owner.transaction(() -> MongoValidationSchema.requireReady(owner.database, true), session -> {
                if (!MongoValidationPreparations.validationEnabled(session, owner.database, owner.clusterId)) throw unreadable("schema");
                return action.apply(session);
            });
        }
        private void scope(Task task) { if (!owner.clusterId.equals(task.clusterId())) throw unreadable("task-cluster"); }
        private static boolean same(Snapshot snapshot, io.tapstate.core.validation.ValidationTaskSpec comparison, Settings settings) {
            return snapshot.comparison().equals(comparison) && snapshot.settings().equals(settings);
        }
        private static void identifier(String value) { if (value == null || value.isBlank()) throw unreadable("task-id"); ValidationError.fieldName(value, "task-id"); }
        private static long aggregateNumber(Document row, String field) {
            var value = row.get(field); long number;
            if (value instanceof Long numeric) number = numeric; else if (value instanceof Integer numeric) number = numeric; else throw unreadable("task-accounting");
            if (number < 0) throw unreadable("task-accounting"); return number;
        }
        private static TapstateException taskLimit(String limit) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", limit), null); }
    }

    private Document definitionDocument(ClientSession session, String pipeline, String task) {
        var id = new Document("clusterId", clusterId).append("pipelineId", pipeline).append("taskId", task);
        var length = tasks.aggregate(session, java.util.List.of(new Document("$match", new Document("_id", id)),
                new Document("$project", new Document("_id", 0).append("bytes", new Document("$bsonSize", "$$ROOT"))))).first();
        if (length == null) return null;
        if (Definitions.aggregateNumber(length, "bytes") > MongoValidationTaskCodec.MAX_TASK_BYTES) throw Definitions.taskLimit("validation.taskBytes");
        return tasks.find(session, new Document("_id", id)).first();
    }

    static boolean matchingResources(Map<String, MongoConfigurationArtifactStore.Version> versions, io.tapstate.core.validation.ValidationTaskSpec comparison) {
        if (!versions.containsKey(comparison.pipelineId()) || !"pipeline".equals(versions.get(comparison.pipelineId()).kind())) return false;
        for (var entry : comparison.dependencyHashes().entrySet())
            if (!versions.containsKey(entry.getKey()) || !entry.getValue().equals(versions.get(entry.getKey()).hash())) return false;
        for (var mapping : comparison.mappings())
            if (!"source".equals(versions.get(mapping.source().sourceId()).kind()) || !"source".equals(versions.get(mapping.target().sourceId()).kind())) return false;
        return true;
    }

    @Override public Reservation reserve(Start start) {
        requireStart(start);
        return transaction(true, () -> {}, session -> {
            var task = task(session, start); if (task == null) return Reservation.STALE;
            if (Boolean.TRUE.equals(task.get("closed"))) return Reservation.CLOSED;
            var mode = mode(task, start.run().mode()); var held = permission(mode);
            if (held != null && held.getLong("sequence") == start.sequence()) {
                if (!identity(start).equals(held.get("identity"))) return Reservation.STALE;
                return phase(held).equals("CLOSED") ? Reservation.CLOSED : Reservation.DUPLICATE;
            }
            if (mode.getLong("nextStartSequence") != start.sequence()
                    || task.getLong("revision") != start.configurationRevision()) return Reservation.STALE;
            if (held != null && !phase(held).equals("CLOSED")) return Reservation.MODE_BUSY;
            if (!matchingDefinition(task, start)) return Reservation.STALE;
            if (validation != null && task.containsKey("definition")) {
                var saved = MongoValidationTaskCodec.snapshot(definitionDocument(session, start.task().pipelineId(), start.task().taskId()), clusterId);
                if (!validation.currentResources(session, saved.comparison())) return Reservation.STALE;
                // Source/definition writers share this original CONFIG write. A concurrent edit
                // forces a retry of these exact pins instead of admitting a stale snapshot.
                ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), client);
            }
            var next = new Document("nextStartSequence", start.sequence() + 1)
                    .append("permission", new Document("sequence", start.sequence()).append("phase", "OPEN").append("identity", identity(start)));
            updateMode(session, task, start, next); return Reservation.RESERVED;
        });
    }

    @Override public Birth open(Start start) { return open(start, null); }
    @Override public Birth open(Open command) { Objects.requireNonNull(command); return open(command.start(), command.snapshot()); }
    private Birth open(Start start, io.tapstate.core.validation.ValidationRunSpec snapshot) {
        requireStart(start);
        return transaction(true, () -> {}, session -> {
            var task = task(session, start); if (task == null) return Birth.STALE;
            if (Boolean.TRUE.equals(task.get("closed"))) return Birth.CLOSED;
            var mode = mode(task, start.run().mode()); var held = matching(mode, start);
            if (held == null) return Birth.STALE;
            if (phase(held).equals("CLOSED")) return Birth.CLOSED;
            var existing = reports.find(session, new Document("_id", reportId(start.run()))).first();
            if (phase(held).equals("BORN")) {
                requireReport(existing, start);
                if (task.containsKey("definition")) requireSnapshot(existing, start);
                return existing.getBoolean("closed") ? Birth.CLOSED : Birth.DUPLICATE;
            }
            if (existing != null) throw unreadable("unexpected-report-before-birth");
            if (task.containsKey("definition") && snapshot == null) throw new TapstateException(ValidationError.INVALID_CONTRACT,
                    Map.of("field", "report.specSnapshot"), null);
            var report = new Document("_id", reportId(start.run())).append("formatVersion", 1)
                    .append("admission", identity(start)).append("openVersion", start.sequence()).append("closed", false)
                    .append("publicationRevision", 0L).append("rootCount", 0L).append("resultBytes", 0L);
            if (snapshot != null) report.append("specSnapshot", MongoValidationTaskCodec.run(snapshot));
            bounded(report, 512 * 1024);
            changePhase(session, task, start, mode, "BORN");
            reports.insertOne(session, report);
            return Birth.CREATED;
        });
    }

    @Override public Closure close(Start start) {
        requireStart(start);
        return transaction(session -> closeInTransaction(session, start));
    }

    Closure closeInTransaction(ClientSession session, Start start) {
            var task = task(session, start); if (task == null) return Closure.STALE;
            var mode = mode(task, start.run().mode()); var held = matching(mode, start);
            if (held == null) return Closure.STALE;
            if (phase(held).equals("CLOSED")) return Closure.ALREADY_CLOSED;
            var original = new Document("_id", reportId(start.run()));
            var report = reports.find(session, original).first();
            if (phase(held).equals("OPEN")) {
                if (report != null) throw unreadable("unexpected-report-before-birth");
            } else {
                requireReport(report, start);
                if (reports.updateOne(session, original.append("admission", identity(start)).append("openVersion", start.sequence()),
                        new Document("$set", new Document("closed", true))).getMatchedCount() != 1) throw unreadable("report-seal");
            }
            changePhase(session, task, start, mode, "CLOSED");
            return Closure.CLOSED;
    }

    boolean bornInTransaction(ClientSession session, Start start) {
        var task = task(session, start); if (task == null) return false;
        if (Boolean.TRUE.equals(task.get("closed"))) return false;
        var held = matching(mode(task, start.run().mode()), start);
        return held != null && phase(held).equals("BORN");
    }

    private Document task(ClientSession session, Start start) {
        requireStart(start);
        var task = tasks.find(session, new Document("_id", taskId(start.task()))).projection(new Document("_id", 1).append("formatVersion", 1)
                .append("taskGeneration", 1).append("revision", 1).append("modes", 1).append("closed", 1)
                .append("lastMutationId", 1).append("schedulingRevision", 1).append("schedulingCommandId", 1)
                .append("definition.formatVersion", 1).append("definition.comparison.fingerprint", 1).append("definition.comparison.mappingRevision", 1)
                .append("definition.comparison.comparisonRulesVersion", 1).append("definition.comparison.pipelineId", 1)
                .append("definition.keyInputsPresent", 1).append("definition.full.method", 1).append("definition.incremental.firstCheckDelayMillis", 1)).first();
        if (task == null) return null;
        return validateTask(task, start.task());
    }
    private Document validateTask(Document task, Task expected) {
        return validateTask(task, expected, validation != null || recovery != null);
    }
    private static Document validateTask(Document task, Task expected, boolean requireClosureFlag) {
        if (!Integer.valueOf(1).equals(task.get("formatVersion")) || !(task.get("revision") instanceof Long revision) || revision < 1
                || !(task.get("taskGeneration") instanceof String generation) || !(task.get("modes") instanceof Document modes)
                || !modes.keySet().equals(Set.of("full", "incremental")) || task.containsKey("closed") && !(task.get("closed") instanceof Boolean)
                || requireClosureFlag && !(task.get("closed") instanceof Boolean)) throw unreadable("task");
        try {
            if (!java.util.UUID.fromString(generation).toString().equals(generation)) throw unreadable("task-generation");
        } catch (IllegalArgumentException invalid) { throw unreadable("task-generation"); }
        if (!taskId(expected).equals(task.get("_id"))) throw unreadable("task-id");
        if (!generation.equals(expected.generation().toString())) return null;
        validateDefinitionHeader(task, revision);
        for (var selected : ValidationMode.values()) {
            var held = permission(mode(task, selected));
            if (held != null) {
                var original = decodeIdentity(held.get("identity", Document.class));
                if (!original.task().equals(expected) || original.run().mode() != selected
                        || original.configurationRevision() > revision) throw unreadable("permission-scope");
            }
        }
        bounded(admissionMetadata(task, modes), 16 * 1024);
        return task;
    }

    private static Document taskHeader() {
        return new Document("_id", 1).append("formatVersion", 1).append("taskGeneration", 1).append("revision", 1).append("modes", 1).append("closed", 1)
                .append("lastMutationId", 1).append("schedulingRevision", 1).append("schedulingCommandId", 1)
                .append("definition.formatVersion", 1).append("definition.comparison.fingerprint", 1).append("definition.comparison.mappingRevision", 1)
                .append("definition.comparison.comparisonRulesVersion", 1).append("definition.comparison.pipelineId", 1)
                .append("definition.keyInputsPresent", 1).append("definition.full.method", 1).append("definition.incremental.firstCheckDelayMillis", 1);
    }
    private static void validateDefinitionHeader(Document task, long revision) {
        boolean typed = task.containsKey("definition") || task.containsKey("lastMutationId") || task.containsKey("schedulingRevision") || task.containsKey("schedulingCommandId");
        if (!typed) return;
        try {
            var definition = task.get("definition", Document.class); var comparison = definition.get("comparison", Document.class);
            if (!Integer.valueOf(1).equals(definition.get("formatVersion")) || !(definition.get("keyInputsPresent") instanceof Boolean) || !(task.get("closed") instanceof Boolean)
                    || !task.get("_id", Document.class).get("pipelineId").equals(comparison.get("pipelineId"))
                    || !(task.get("schedulingRevision") instanceof Long scheduling) || scheduling < 1 || scheduling > revision
                    || !(comparison.get("mappingRevision") instanceof Long mapping) || mapping < 1 || mapping == Long.MAX_VALUE
                    || !(comparison.get("fingerprint") instanceof String fingerprint) || !fingerprint.matches("[a-f0-9]{64}")
                    || !(comparison.get("comparisonRulesVersion") instanceof String rules) || rules.isBlank()) throw unreadable("task-definition");
            ValidationError.fieldName(rules, "task.rules");
            MongoValidationTaskCodec.uuid(task, "lastMutationId"); MongoValidationTaskCodec.uuid(task, "schedulingCommandId");
            io.tapstate.core.validation.ValidationMethod.valueOf(definition.get("full", Document.class).getString("method"));
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("task-definition"); }
    }
    private static boolean matchingDefinition(Document task, Start start) {
        if (!task.containsKey("definition")) return true;
        var definition = task.get("definition", Document.class); var comparison = definition.get("comparison", Document.class);
        var method = start.run().mode() == ValidationMode.FULL
                ? io.tapstate.core.validation.ValidationMethod.valueOf(definition.get("full", Document.class).getString("method"))
                : io.tapstate.core.validation.ValidationMethod.FULL_COLUMN;
        if (method != io.tapstate.core.validation.ValidationMethod.COUNT && !definition.getBoolean("keyInputsPresent")) return false;
        return start.run().keyMappingRevision() == comparison.getLong("mappingRevision")
                && start.run().comparisonRulesVersion().equals(comparison.get("comparisonRulesVersion"))
                && start.run().specHash().equals(io.tapstate.core.validation.ValidationRunSpec.fingerprint(comparison.getString("fingerprint"), start.run().mode(), method));
    }

    boolean closePipelineInTransaction(ClientSession session, String pipeline, int maximum) {
        if (maximum < 1 || maximum > 20 || !(validation != null && validation.closing() && validation.pipelineId().equals(pipeline)
                || recovery != null && recovery.pipeline().equals(pipeline))) throw unreadable("pipeline-closure");
        var filter = recovery == null ? MongoValidationPreparations.unfinishedReports(clusterId, pipeline)
                : MongoValidationPreparations.openReportPermissions(clusterId, pipeline);
        try (var cursor = tasks.find(session, filter).projection(taskHeader()).limit(maximum).iterator()) {
            while (cursor.hasNext()) {
                var row = cursor.next(); Task original;
                try {
                    var id = row.get("_id", Document.class);
                    original = new Task(clusterId, pipeline, id.getString("taskId"), java.util.UUID.fromString(row.getString("taskGeneration")));
                } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("task"); }
                validateTask(row, original);
                if (recovery == null && tasks.updateOne(session, new Document("_id", row.get("_id")).append("taskGeneration", row.get("taskGeneration"))
                        .append("revision", row.get("revision")).append("modes", row.get("modes")),
                        new Document("$set", new Document("closed", true))).getMatchedCount() != 1) throw unreadable("task-close");
                for (var selected : ValidationMode.values()) {
                    var held = permission(mode(row, selected));
                    if (held != null) closeInTransaction(session, decodeIdentity(held.get("identity", Document.class)));
                }
            }
        }
        boolean complete = tasks.find(session, filter).projection(new Document("_id", 0).append("remaining", new Document("$literal", true))).limit(1).first() == null;
        if (complete && recovery != null) requireClosedPipelinePermissions(session, database, clusterId, pipeline);
        return complete;
    }

    /** Recovery seals run rights, while the durable user configuration and its counters remain available. */
    static void requireClosedPipelinePermissions(ClientSession session, MongoDatabase database, String cluster, String pipeline) {
        requireKnownPipelinePermissions(session, database, cluster, pipeline, null);
    }
    private static void requireKnownPipelinePermissions(ClientSession session, MongoDatabase database, String cluster, String pipeline, Long currentOwner) {
        var filter = new Document("_id.clusterId", cluster).append("_id.pipelineId", pipeline);
        int count = 0;
        try (var cursor = SystemCollections.VALIDATION_TASKS.on(database).find(session, filter).projection(taskHeader())
                .batchSize(20).limit(MAX_TASKS_PER_PIPELINE + 1).iterator()) {
            while (cursor.hasNext()) {
                if (++count > MAX_TASKS_PER_PIPELINE) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                        Map.of("limit", "validation.taskControlHeaders"), null);
                var row = cursor.next(); Task task;
                try { task = new Task(cluster, pipeline, row.get("_id", Document.class).getString("taskId"), java.util.UUID.fromString(row.getString("taskGeneration"))); }
                catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("task"); }
                validateTask(row, task, true);
                for (var selected : ValidationMode.values()) {
                    var held = permission(mode(row, selected));
                    if (held != null && !phase(held).equals("CLOSED") && (currentOwner == null
                            || decodeIdentity(held.get("identity", Document.class)).run().owner().claimGeneration() < currentOwner))
                        throw unreadable("original-permission-closure");
                }
            }
        }
    }

    boolean closeObsoletePermissionsInTransaction(ClientSession session, io.tapstate.core.validation.ValidationOwner current, int maximum) {
        if (validation == null || validation.closing() || !validation.pipelineId().equals(current.pipelineId())
                || !clusterId.equals(current.clusterId()) || maximum < 1 || maximum > 20) throw unreadable("obsolete-permission-closure");
        var filter = new Document("_id.clusterId", clusterId).append("_id.pipelineId", current.pipelineId()).append("$or", java.util.List.of(
                new Document("modes.full.permission.phase", new Document("$in", java.util.List.of("OPEN", "BORN")))
                        .append("modes.full.permission.identity.owner.claimGeneration", new Document("$lt", current.claimGeneration())),
                new Document("modes.incremental.permission.phase", new Document("$in", java.util.List.of("OPEN", "BORN")))
                        .append("modes.incremental.permission.identity.owner.claimGeneration", new Document("$lt", current.claimGeneration()))));
        try (var cursor = tasks.find(session, filter).projection(taskHeader()).limit(maximum).iterator()) {
            while (cursor.hasNext()) {
                var row = cursor.next(); Task task;
                try { task = new Task(clusterId, current.pipelineId(), row.get("_id", Document.class).getString("taskId"), java.util.UUID.fromString(row.getString("taskGeneration"))); }
                catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("task"); }
                validateTask(row, task, true);
                for (var mode : ValidationMode.values()) {
                    var held = permission(mode(row, mode));
                    if (held != null && !phase(held).equals("CLOSED")) {
                        var start = decodeIdentity(held.get("identity", Document.class));
                        if (start.run().owner().claimGeneration() < current.claimGeneration()) closeInTransaction(session, start);
                    }
                }
            }
        }
        boolean complete = tasks.find(session, filter).projection(new Document("_id", 1)).limit(1).first() == null;
        if (complete) requireKnownPipelinePermissions(session, database, clusterId, current.pipelineId(), current.claimGeneration());
        return complete;
    }
    void requireOriginalInTransaction(ClientSession session, Start start, boolean closed) {
        var task = task(session, start); if (task == null) throw unreadable("original-task");
        var selected = mode(task, start.run().mode()); var held = permission(selected);
        if (closed) {
            if (held == null || held.getLong("sequence") < start.sequence()
                    || held.getLong("sequence") == start.sequence() && (!identity(start).equals(held.get("identity")) || !phase(held).equals("CLOSED")))
                throw unreadable("original-permission-closure");
        } else if (Boolean.TRUE.equals(task.get("closed")) || held == null || !identity(start).equals(held.get("identity"))
                || !Set.of("OPEN", "BORN").contains(phase(held))) throw unreadable("original-permission");
    }
    void requireBornInTransaction(ClientSession session, Start start) {
        requireOriginalInTransaction(session, start, false);
        var task = task(session, start); var held = matching(mode(task, start.run().mode()), start);
        if (held == null || !phase(held).equals("BORN")) throw unreadable("original-report-birth");
        var report = reports.find(session, new Document("_id", reportId(start.run()))).first();
        requireReport(report, start);
        if (task.containsKey("definition")) requireSnapshot(report, start);
        if (report.getBoolean("closed")) throw unreadable("original-report-closed");
    }
    void requireRunSnapshotInTransaction(ClientSession session, Start start, io.tapstate.core.validation.ValidationRunSpec snapshot) {
        requireOriginalInTransaction(session, start, false);
        if (task(session, start).containsKey("definition") && snapshot == null)
            throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "control.specSnapshot"), null);
        if (snapshot != null) new Open(start, snapshot);
    }
    boolean savedDelayInTransaction(ClientSession session, Start start, long revision, java.util.UUID command, long delayMillis) {
        var task = task(session, start); if (task == null || Boolean.TRUE.equals(task.get("closed"))) return false;
        var held = matching(mode(task, start.run().mode()), start);
        if (held == null || !Set.of("OPEN", "BORN").contains(phase(held))) return false;
        try {
            var incremental = task.get("definition", Document.class).get("incremental", Document.class);
            if (!(incremental.get("firstCheckDelayMillis") instanceof Long saved) || saved < 0 || saved > 86_400_000L) throw unreadable("delay-settings");
            return revision == task.getLong("schedulingRevision") && command.equals(MongoValidationTaskCodec.uuid(task, "schedulingCommandId")) && delayMillis == saved;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("delay-settings"); }
    }
    private static void requireSnapshot(Document report, Start start) {
        try {
            var snapshot = MongoValidationTaskCodec.run(report.get("specSnapshot", Document.class));
            new Open(start, snapshot);
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) { throw unreadable("original-spec-snapshot"); }
    }
    private static Document mode(Document task, ValidationMode mode) {
        var modes = task.get("modes", Document.class);
        var value = modes.get(modeName(mode));
        if (!(value instanceof Document row) || !(row.get("nextStartSequence") instanceof Long next) || next < 1
                || !(row.keySet().equals(Set.of("nextStartSequence")) || row.keySet().equals(Set.of("nextStartSequence", "permission"))))
            throw unreadable("mode");
        var held = permission(row);
        if (held != null && held.getLong("sequence") + 1 != next || held == null && next != 1) throw unreadable("mode-sequence");
        return row;
    }
    private static Document permission(Document mode) {
        if (!mode.containsKey("permission")) return null;
        if (!(mode.get("permission") instanceof Document held) || !held.keySet().equals(Set.of("sequence", "phase", "identity"))
                || !(held.get("sequence") instanceof Long sequence) || sequence < 1 || sequence == Long.MAX_VALUE
                || !(held.get("identity") instanceof Document identity) || !Long.valueOf(sequence).equals(identity.get("sequence")))
            throw unreadable("permission");
        phase(held); return held;
    }
    static Start decodeIdentity(Document value) {
        try {
            if (!value.keySet().equals(Set.of("taskGeneration", "configurationRevision", "sequence", "commandId", "run", "mode", "specHash",
                    "keyMappingRevision", "comparisonRulesVersion", "owner"))) throw unreadable("permission-identity");
            var run = value.get("run", Document.class); var owner = value.get("owner", Document.class);
            if (run == null || !run.keySet().equals(Set.of("clusterId", "pipelineId", "taskId", "runId")) || owner == null
                    || !owner.keySet().equals(Set.of("nodeId", "bootId", "claimGeneration", "executionGeneration", "topologyRevision")))
                throw unreadable("permission-identity");
            var task = new Task(run.getString("clusterId"), run.getString("pipelineId"), run.getString("taskId"),
                    java.util.UUID.fromString(value.getString("taskGeneration")));
            var tenure = new io.tapstate.core.validation.ValidationOwner(task.clusterId(), task.pipelineId(), owner.getString("nodeId"),
                    owner.getString("bootId"), owner.getLong("claimGeneration"), owner.getLong("executionGeneration"), owner.getLong("topologyRevision"));
            var identity = new ValidationRunIdentity(tenure, task.taskId(), run.getString("runId"), ValidationMode.valueOf(value.getString("mode")),
                    value.getString("specHash"), value.getLong("keyMappingRevision"), value.getString("comparisonRulesVersion"));
            var original = new Start(task, value.getLong("configurationRevision"), value.getLong("sequence"), value.getString("commandId"), identity);
            if (!identity(original).equals(value)) throw unreadable("permission-identity");
            return original;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) {
            throw unreadable("permission-identity");
        }
    }

    private static String phase(Document held) {
        if (!(held.get("phase") instanceof String phase) || !Set.of("OPEN", "BORN", "CLOSED").contains(phase)) throw unreadable("permission-phase");
        return phase;
    }
    private static Document matching(Document mode, Start start) {
        var held = permission(mode);
        return held != null && held.getLong("sequence") == start.sequence() && identity(start).equals(held.get("identity")) ? held : null;
    }
    private void changePhase(ClientSession session, Document task, Start start, Document mode, String phase) {
        var changed = new Document(mode); var permission = new Document(mode.get("permission", Document.class));
        permission.put("phase", phase); changed.put("permission", permission); updateMode(session, task, start, changed);
    }
    private void updateMode(ClientSession session, Document task, Start start, Document next) {
        var modes = new Document(task.get("modes", Document.class)); modes.put(modeName(start.run().mode()), next);
        bounded(admissionMetadata(task, modes), 16 * 1024);
        var filter = new Document("_id", task.get("_id")).append("taskGeneration", task.get("taskGeneration"))
                .append("revision", task.get("revision")).append("modes", task.get("modes"));
        if (tasks.updateOne(session, filter, new Document("$set", new Document("modes", modes))).getMatchedCount() != 1)
            throw unreadable("permission-cas");
    }
    private static void requireReport(Document report, Start start) {
        if (report == null || !reportId(start.run()).equals(report.get("_id")) || !Integer.valueOf(1).equals(report.get("formatVersion"))
                || !identity(start).equals(report.get("admission")) || !Long.valueOf(start.sequence()).equals(report.get("openVersion"))
                || !(report.get("closed") instanceof Boolean) || !(report.get("publicationRevision") instanceof Long revision) || revision < 0
                || !(report.get("rootCount") instanceof Long roots) || roots < 0
                || !(report.get("resultBytes") instanceof Long bytes) || bytes < 0)
            throw unreadable("original-report");
    }
    private void requireStart(Start start) {
        Objects.requireNonNull(start);
        if (!clusterId.equals(start.task().clusterId())) throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "start.clusterId"), null);
        if (validation != null && !validation.pipelineId().equals(start.task().pipelineId())) throw unreadable("start-pipeline");
        bounded(identity(start), 8 * 1024);
    }
    void checkActive() { requireActive.run(); }
    io.tapstate.spi.store.ValidationTaskStore.Snapshot retentionTask(ClientSession session,Task expected){
        if(!clusterId.equals(expected.clusterId()))throw unreadable("retention-cluster");requirePipeline(expected.pipelineId());
        var row=tasks.find(session,new Document("_id",taskId(expected))).first();if(row==null)return null;
        var saved=MongoValidationTaskCodec.snapshot(row,clusterId);if(!saved.task().equals(expected))return null;
        // Birth and authoring use this same CONFIG write fence. No new lease or acknowledgement is introduced.
        ConfigurationVersion.advance(session, SystemCollections.CONFIGURATION_VERSION.on(database), client);
        return saved;
    }
    boolean closing() { return validation != null && validation.closing() || recovery != null; }
    void requirePipeline(String pipeline) {
        if (validation != null && !validation.pipelineId().equals(pipeline) || recovery != null && !recovery.pipeline().equals(pipeline)) throw unreadable("pipeline");
    }
    <T> T transaction(Function<ClientSession, T> work) { return transaction(() -> {}, work); }
    <T> T transaction(Runnable verifyAdditionalStructures, Function<ClientSession, T> work) {
        return transaction(false, verifyAdditionalStructures, work);
    }
    private <T> T transaction(boolean admission, Runnable verifyAdditionalStructures, Function<ClientSession, T> work) {
        return StoreIo.call(() -> {
            requireActive.run(); verifyStructures(); verifyAdditionalStructures.run();
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    requireActive.run(); if (validation != null) validation.enter(client, database, session, admission);
                    if (recovery != null) {
                        if (admission) throw unreadable("recovery-admission");
                        recovery.require(client, database, session);
                    }
                    var result = work.apply(session); requireActive.run(); return result;
                }, TRANSACTION);
            }
        });
    }
    private void verifyStructures() {
        for (var table : java.util.List.of(SystemCollections.VALIDATION_TASKS, SystemCollections.VALIDATION_REPORTS)) {
            var row = database.listCollections().filter(new Document("name", table.collectionName())).first();
            if (row == null || !"collection".equals(row.get("type"))) throw unreadable("validation-collection");
        }
    }
    static Document taskId(Task task) {
        return new Document("clusterId", task.clusterId()).append("pipelineId", task.pipelineId()).append("taskId", task.taskId());
    }
    static Document reportId(ValidationRunIdentity run) {
        return new Document("clusterId", run.owner().clusterId()).append("pipelineId", run.owner().pipelineId())
                .append("taskId", run.taskId()).append("runId", run.runId());
    }
    static Document identity(Start start) {
        var run = start.run(); var owner = run.owner();
        return new Document("taskGeneration", start.task().generation().toString()).append("configurationRevision", start.configurationRevision())
                .append("sequence", start.sequence()).append("commandId", start.commandId()).append("run", reportId(run))
                .append("mode", run.mode().name()).append("specHash", run.specHash()).append("keyMappingRevision", run.keyMappingRevision())
                .append("comparisonRulesVersion", run.comparisonRulesVersion()).append("owner", new Document("nodeId", owner.nodeId())
                        .append("bootId", owner.bootId()).append("claimGeneration", owner.claimGeneration())
                        .append("executionGeneration", owner.executionGeneration()).append("topologyRevision", owner.topologyRevision()));
    }
    private static String modeName(ValidationMode mode) { return mode == ValidationMode.FULL ? "full" : "incremental"; }
    private static Document admissionMetadata(Document task, Document modes) {
        var maximumModes = new Document();
        for (var entry : modes.entrySet()) {
            var mode = new Document((Document) entry.getValue());
            if (mode.get("permission") instanceof Document held) {
                // Admission reserves the largest remaining phase so every accepted permission can close.
                mode.put("permission", new Document(held).append("phase", "CLOSED"));
            }
            maximumModes.put(entry.getKey(), mode);
        }
        var metadata = new Document("taskGeneration", task.get("taskGeneration")).append("modes", maximumModes);
        if (task.containsKey("closed")) metadata.append("closed", task.get("closed"));
        for (var field : java.util.List.of("lastMutationId", "schedulingRevision", "schedulingCommandId"))
            if (task.containsKey(field)) metadata.append(field, task.get(field));
        return metadata;
    }
    private static void bounded(Document value, int maximum) {
        // Every input component is bounded before BSON materialization; this measures the actual encoded form.
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build());
            if (output.getSize() > maximum) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "admission-bytes"), null);
        }
    }
    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "validation", "field", field), null);
    }
}
