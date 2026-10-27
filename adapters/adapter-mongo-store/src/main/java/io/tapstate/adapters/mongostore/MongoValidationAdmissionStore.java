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
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).readPreference(ReadPreference.primary()).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final String clusterId;
    private final Runnable requireActive;
    private final MongoCollection<Document> tasks;
    private final MongoCollection<Document> reports;

    MongoValidationAdmissionStore(MongoClient client, MongoDatabase database, String clusterId, Runnable requireActive) {
        this.client = Objects.requireNonNull(client); this.database = Objects.requireNonNull(database);
        this.clusterId = Objects.requireNonNull(clusterId); this.requireActive = Objects.requireNonNull(requireActive);
        tasks = SystemCollections.VALIDATION_TASKS.on(database); reports = SystemCollections.VALIDATION_REPORTS.on(database);
    }

    @Override public Reservation reserve(Start start) {
        requireStart(start);
        return transaction(session -> {
            var task = task(session, start); if (task == null) return Reservation.STALE;
            var mode = mode(task, start.run().mode()); var held = permission(mode);
            if (held != null && held.getLong("sequence") == start.sequence()) {
                if (!identity(start).equals(held.get("identity"))) return Reservation.STALE;
                return phase(held).equals("CLOSED") ? Reservation.CLOSED : Reservation.DUPLICATE;
            }
            if (mode.getLong("nextStartSequence") != start.sequence()
                    || task.getLong("revision") != start.configurationRevision()) return Reservation.STALE;
            if (held != null && !phase(held).equals("CLOSED")) return Reservation.MODE_BUSY;
            var next = new Document("nextStartSequence", start.sequence() + 1)
                    .append("permission", new Document("sequence", start.sequence()).append("phase", "OPEN").append("identity", identity(start)));
            updateMode(session, task, start, next); return Reservation.RESERVED;
        });
    }

    @Override public Birth open(Start start) {
        requireStart(start);
        return transaction(session -> {
            var task = task(session, start); if (task == null) return Birth.STALE;
            var mode = mode(task, start.run().mode()); var held = matching(mode, start);
            if (held == null) return Birth.STALE;
            if (phase(held).equals("CLOSED")) return Birth.CLOSED;
            var existing = reports.find(session, new Document("_id", reportId(start.run()))).first();
            if (phase(held).equals("BORN")) { requireReport(existing, start); return existing.getBoolean("closed") ? Birth.CLOSED : Birth.DUPLICATE; }
            if (existing != null) throw unreadable("unexpected-report-before-birth");
            var report = new Document("_id", reportId(start.run())).append("formatVersion", 1)
                    .append("admission", identity(start)).append("openVersion", start.sequence()).append("closed", false)
                    .append("publicationRevision", 0L).append("rootCount", 0L).append("resultBytes", 0L);
            bounded(report, 8 * 1024);
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
        var held = matching(mode(task, start.run().mode()), start);
        return held != null && phase(held).equals("BORN");
    }

    private Document task(ClientSession session, Start start) {
        var task = tasks.find(session, new Document("_id", taskId(start.task()))).first();
        if (task == null) return null;
        if (!Integer.valueOf(1).equals(task.get("formatVersion")) || !(task.get("revision") instanceof Long revision) || revision < 1
                || !(task.get("taskGeneration") instanceof String generation) || !(task.get("modes") instanceof Document modes)
                || !modes.keySet().equals(Set.of("full", "incremental"))) throw unreadable("task");
        try {
            if (!java.util.UUID.fromString(generation).toString().equals(generation)) throw unreadable("task-generation");
        } catch (IllegalArgumentException invalid) { throw unreadable("task-generation"); }
        if (!generation.equals(start.task().generation().toString())) return null;
        for (var selected : ValidationMode.values()) {
            var held = permission(mode(task, selected));
            if (held != null) {
                var original = decodeIdentity(held.get("identity", Document.class));
                if (!original.task().equals(start.task()) || original.run().mode() != selected
                        || original.configurationRevision() > revision) throw unreadable("permission-scope");
            }
        }
        bounded(new Document("taskGeneration", generation).append("modes", modes), 16 * 1024);
        return task;
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
        bounded(new Document("taskGeneration", task.get("taskGeneration")).append("modes", modes), 16 * 1024);
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
        bounded(identity(start), 8 * 1024);
    }
    void checkActive() { requireActive.run(); }
    <T> T transaction(Function<ClientSession, T> work) { return transaction(() -> {}, work); }
    <T> T transaction(Runnable verifyAdditionalStructures, Function<ClientSession, T> work) {
        return StoreIo.call(() -> {
            requireActive.run(); verifyStructures(); verifyAdditionalStructures.run();
            try (var session = client.startSession()) {
                return session.withTransaction(() -> { requireActive.run(); var result = work.apply(session); requireActive.run(); return result; }, TRANSACTION);
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
