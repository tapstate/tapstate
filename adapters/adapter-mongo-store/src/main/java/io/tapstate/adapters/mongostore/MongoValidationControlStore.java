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
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import org.bson.Document;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

/** CACHE-only validation control. Normal operations require an original jointly prepared claim/control. */
final class MongoValidationControlStore implements ValidationControlStore {
    static final int MAX_ACTIVE_RUNS = 128;
    static final long MAX_RETAINED_RUNS = 4096;
    private static final int MAX_CONTROL_BYTES = 1024 * 1024;
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).readPreference(ReadPreference.primary()).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final MongoCollection<Document> rows;

    MongoValidationControlStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this.client = Objects.requireNonNull(client); this.database = Objects.requireNonNull(database);
        this.scope = Objects.requireNonNull(scope); this.requireActive = Objects.requireNonNull(requireActive);
        rows = SystemCollections.VALIDATION_RUNS.on(database);
    }

    @Override public Join join(Run run) {
        Objects.requireNonNull(run);
        return transaction(run.identity().owner(), true, session -> {
            var control = control(session, run.identity().owner().pipelineId());
            var existing = row(session, run);
            if (existing != null) {
                if (existing.getBoolean("retired") || state(existing).terminal() || control.getBoolean("closed")) return Join.GENERATION_CLOSED;
                return current(control, run) && slot(control, run) ? Join.ALREADY_JOINED : Join.GENERATION_CLOSED;
            }
            var slots = slots(control); String key = slotKey(run.identity());
            if (slots.containsKey(key)) {
                var occupied = rows.find(session, new Document("_id", slots.get(key, Document.class))).first();
                if (occupied == null) throw unreadable("missing-slot-run");
                return Join.MODE_BUSY;
            }
            if (slots.isEmpty() && control.getBoolean("closed")) {
                long previous = control.getLong("countingGeneration");
                if (previous == Long.MAX_VALUE || run.countingGeneration() != previous + 1) return Join.GENERATION_CLOSED;
                var prior = control.get("owner", Document.class);
                if (prior != null) {
                    var old = owner(prior, run.identity().owner().pipelineId());
                    if (run.identity().owner().claimGeneration() < old.claimGeneration()
                            || run.identity().owner().executionGeneration() <= old.executionGeneration()) return Join.GENERATION_CLOSED;
                }
                control.put("owner", WorkloadClaimDocuments.stored(run.fence()));
                control.put("countingGeneration", run.countingGeneration()); control.put("closed", false); control.put("problemLimit", false);
            }
            if (!current(control, run) || control.getBoolean("closed")) return Join.GENERATION_CLOSED;
            if (slots.size() == MAX_ACTIVE_RUNS || control.getLong("retainedRuns") == MAX_RETAINED_RUNS) throw limit();
            slots.put(key, runId(run)); control.put("retainedRuns", control.getLong("retainedRuns") + 1);
            writeControl(session, control);
            rows.insertOne(session, new Document("_id", runId(run)).append("formatVersion", 1).append("scope", scope(scope))
                    .append("identity", identity(run)).append("state", ValidationRunState.STARTING.name()).append("retired", false));
            return Join.JOINED;
        });
    }

    @Override public Mutation transition(Transition command) {
        Objects.requireNonNull(command); var run = command.run();
        return transaction(run.identity().owner(), false, session -> {
            var control = control(session, run.identity().owner().pipelineId()); var row = row(session, run);
            if (row == null || row.getBoolean("retired") || !current(control, run) || !slot(control, run)) return Mutation.STALE;
            var current = state(row);
            if (current == command.next()) return Mutation.ALREADY_APPLIED;
            if (current != command.expected()) return Mutation.STALE;
            writeControl(session, control);
            updateRun(session, run, row, new Document("state", command.next().name()));
            return Mutation.APPLIED;
        });
    }

    @Override public Mutation latchProblemLimit(Run run) {
        Objects.requireNonNull(run);
        return transaction(run.identity().owner(), false, session -> {
            var control = control(session, run.identity().owner().pipelineId()); var target = row(session, run);
            if (target == null || target.getBoolean("retired") || !current(control, run) || !slot(control, run)) return Mutation.STALE;
            if (control.getBoolean("problemLimit")) return Mutation.ALREADY_APPLIED;
            if (state(target).terminal()) return Mutation.STALE;
            for (var id : slots(control).values()) {
                var held = rows.find(session, new Document("_id", id)).first();
                if (held == null) throw unreadable("missing-slot-run");
                var member = decodeRun(held); requireRow(held, member);
                if (!current(control, member) || held.getBoolean("retired")) throw unreadable("slot-owner");
                var state = state(held);
                if (state == ValidationRunState.STARTING || state == ValidationRunState.RUNNING)
                    updateRun(session, member, held, new Document("state", ValidationRunState.STOPPING.name()));
            }
            control.put("problemLimit", true); control.put("closed", true); writeControl(session, control);
            return Mutation.APPLIED;
        });
    }

    @Override public Mutation retire(Retirement command) {
        Objects.requireNonNull(command); var run = command.run();
        return transaction(run.identity().owner(), false, session -> {
            var control = control(session, run.identity().owner().pipelineId()); var row = row(session, run);
            if (row == null || !current(control, run) || state(row) != command.terminal()) return Mutation.STALE;
            if (row.getBoolean("retired")) return Mutation.ALREADY_APPLIED;
            if (!slot(control, run)) throw unreadable("missing-retirement-slot");
            updateRun(session, run, row, new Document("retired", true)); removeSlot(control, run); writeControl(session, control);
            return Mutation.APPLIED;
        });
    }

    @Override public Mutation terminateLostOwner(LostOwner command) {
        Objects.requireNonNull(command); var obsolete = command.obsolete();
        return transaction(command.authority(), false, session -> {
            var control = control(session, command.authority().pipelineId()); var row = row(session, obsolete);
            if (row == null) return Mutation.STALE;
            if (row.getBoolean("retired")) return Mutation.ALREADY_APPLIED;
            if (!slot(control, obsolete) || !current(control, obsolete)) throw unreadable("obsolete-slot");
            var values = new Document("retired", true);
            if (!state(row).terminal()) values.put("state", ValidationRunState.FAILED.name());
            updateRun(session, obsolete, row, values); removeSlot(control, obsolete); writeControl(session, control);
            return Mutation.APPLIED;
        });
    }

    private <T> T transaction(ValidationOwner owner, boolean admission, Function<ClientSession, T> work) {
        if (!scope.clusterId().equals(owner.clusterId())) throw fenced();
        return StoreIo.call(() -> {
            requireActive.run(); requireCollection(database);
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    requireActive.run();
                    // Prove the latest live tenure before any read pins a snapshot. Every later domain
                    // and preparation check is in this transaction and rolls the witness back on failure.
                    var filter = WorkloadClaimDocuments.live(ValidationClaims.fence(owner))
                            .append("validationPreparation.scope", scope(scope)).append("validationPreparation.pipelineId", owner.pipelineId())
                            .append("contextExecutionGeneration", owner.executionGeneration()).append("executionClaimGeneration", owner.claimGeneration())
                            .append("executionNodeIds", List.of(owner.nodeId()))
                            .append("fencedValidationWrites", new Document("$gte", 0L).append("$lt", Long.MAX_VALUE));
                    if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session, filter,
                            new Document("$inc", new Document("fencedValidationWrites", 1L))).getMatchedCount() != 1) throw fenced();
                    var business = new MongoCacheBusinessIdentity(database, scope);
                    if (admission) business.admitMetadataWrite(session); else business.requireAdmissionState(session);
                    var key = ValidationClaims.key(scope.clusterId(), owner.pipelineId());
                    var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
                    requirePreparedClaim(session, database, claim, key, scope);
                    var decoded = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).inStrictSession(session).decodeStored(claim);
                    if (!ValidationClaims.owner(owner.pipelineId(), decoded).equals(owner)) throw fenced();
                    var result = work.apply(session); requireActive.run(); return result;
                }, TRANSACTION);
            }
        });
    }

    /** Only an existing controlled preparation may call this, in the same transaction as its original Pipeline facts. */
    static void prepareInTransaction(ClientSession session, MongoDatabase database, PipelineIntentScope pipeline,
            String preparationId, boolean mayCreate) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("validation preparation needs the original CACHE transaction");
        var scope = new CacheBusinessScope(pipeline.clusterId(), pipeline.bindingVersion(), pipeline.cacheIncarnation());
        var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); var rows = SystemCollections.VALIDATION_RUNS.on(database);
        var originalKey = new WorkloadClaimKey(pipeline.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline.pipelineId());
        var originalId = new Document("_id", MongoWorkloadClaimStore.id(originalKey));
        var original = claims.find(session, originalId).first();
        if (original == null || !MongoPipelinePreparationFacts.scope(pipeline).equals(original.get("pipelineScope"))
                || !(original.get("pipelinePreparation") instanceof Document prepared)
                || !preparationId.equals(prepared.get("preparationId")) || !"PREPARED".equals(prepared.get("phase"))) throw fenced();
        var issuer = new MongoWorkloadClaimStore(claims).inStrictSession(session).decodeStored(original).owner();
        var state = SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session, new Document("_id", pipeline.pipelineId())).first();
        if (state == null || !MongoPipelinePreparationFacts.scope(pipeline).equals(state.get("scope"))) throw unreadable("original-pipeline-state");
        var key = ValidationClaims.key(pipeline.clusterId(), pipeline.pipelineId());
        var existing = claims.find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        var control = rows.find(session, new Document("_id", controlId(pipeline.clusterId(), pipeline.pipelineId()))).first();
        if (existing != null || control != null) { requirePreparedClaim(session, database, existing, key, scope); return; }
        if (!mayCreate || original.containsKey("validationControlBirth")) throw unreadable("missing-original-control");
        if (rows.find(session, new Document("_id.clusterId", pipeline.clusterId()).append("_id.pipelineId", pipeline.pipelineId())).limit(1).first() != null)
            throw unreadable("orphan-runs");
        var birth = new Document("formatVersion", 1).append("scope", scope(scope)).append("pipelineScope", MongoPipelinePreparationFacts.scope(pipeline))
                .append("pipelineId", pipeline.pipelineId()).append("preparationId", preparationId);
        bounded(birth, 8 * 1024);
        if (claims.updateOne(session, originalId.append("pipelineScope", original.get("pipelineScope"))
                        .append("pipelinePreparation", prepared).append("validationControlBirth", new Document("$exists", false)),
                new Document("$set", new Document("validationControlBirth", birth))).getMatchedCount() != 1) throw fenced();
        claims.insertOne(session, new Document("_id", MongoWorkloadClaimStore.id(key)).append("clusterId", key.clusterId())
                .append("resourceType", key.type().name()).append("resourceId", key.resourceId())
                .append("ownerNodeId", issuer.nodeId()).append("ownerBootId", issuer.bootId())
                .append("claimGeneration", 1L).append("executionGeneration", 0L).append("topologyRevision", 0L).append("leaseUntil", new Date(0))
                .append("validationPreparation", birth).append("fencedValidationWrites", 0L));
        rows.insertOne(session, new Document("_id", controlId(pipeline.clusterId(), pipeline.pipelineId())).append("formatVersion", 1)
                .append("scope", scope(scope)).append("birth", birth).append("countingGeneration", 0L).append("owner", null)
                .append("closed", true).append("problemLimit", false).append("revision", 0L).append("retainedRuns", 0L).append("slots", new Document()));
    }

    static void requirePreparedClaim(ClientSession session, MongoDatabase database, Document claim, WorkloadClaimKey key, CacheBusinessScope scope) {
        requireCollection(database);
        if (claim == null || key.type() != WorkloadClaimType.VALIDATION || !(claim.get("validationPreparation") instanceof Document birth)
                || !birth.keySet().equals(Set.of("formatVersion", "scope", "pipelineScope", "pipelineId", "preparationId"))
                || !Integer.valueOf(1).equals(birth.get("formatVersion")) || !scope(scope).equals(birth.get("scope"))
                || !(birth.get("pipelineId") instanceof String pipeline) || !(birth.get("preparationId") instanceof String operation)
                || operation.isBlank() || !key.equals(ValidationClaims.key(scope.clusterId(), pipeline))
                || !(claim.get("fencedValidationWrites") instanceof Long writes) || writes < 0 || writes == Long.MAX_VALUE) throw fenced();
        var control = SystemCollections.VALIDATION_RUNS.on(database).find(session, new Document("_id", controlId(scope.clusterId(), pipeline))).first();
        validateControl(control, scope, pipeline);
        if (!birth.equals(control.get("birth"))) throw unreadable("control-birth");
        var original = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(
                new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline)))).first();
        if (original == null || !birth.equals(original.get("validationControlBirth"))) throw unreadable("original-control-birth");
        var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).inStrictSession(session);
        raw.decodeStored(claim); raw.decodeStored(original);
        var originalKey = new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline);
        var current = MongoCacheWorkloadClaimStore.preparedScope(original, originalKey, scope);
        if (!(birth.get("pipelineScope") instanceof Document bornDocument)) throw unreadable("control-birth-scope");
        final PipelineIntentScope born;
        try { born = MongoPipelinePreparationFacts.scope(bornDocument); }
        catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("control-birth-scope"); }
        bounded(birth, 8 * 1024);
        if (!MongoPipelinePreparationFacts.scope(born).equals(bornDocument) || !pipeline.equals(born.pipelineId())
                || !scope.equals(new CacheBusinessScope(born.clusterId(), born.bindingVersion(), born.cacheIncarnation()))
                || current.stateGeneration() < born.stateGeneration()
                || current.stateGeneration() == born.stateGeneration() && !operation.equals(original.get("pipelinePreparation", Document.class).get("preparationId")))
            throw unreadable("original-preparation");
        var state = SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session, new Document("_id", pipeline)).first();
        if (state == null || !MongoPipelinePreparationFacts.scope(current).equals(state.get("scope"))
                || !(state.get("epoch") instanceof Long epoch) || epoch < 0 || epoch == Long.MAX_VALUE
                || !(state.get("runtimeWriteFence") instanceof Long revision) || revision < 1 || revision == Long.MAX_VALUE
                || !(state.get("stateJson") instanceof String) || !(state.get("touchMillis") instanceof Long)) throw unreadable("original-pipeline-state");
    }

    private Document control(ClientSession session, String pipeline) {
        var row = rows.find(session, new Document("_id", controlId(scope.clusterId(), pipeline))).first(); validateControl(row, scope, pipeline); return row;
    }
    private static void validateControl(Document row, CacheBusinessScope scope, String pipeline) {
        if (row == null || !Integer.valueOf(1).equals(row.get("formatVersion")) || !scope(scope).equals(row.get("scope"))
                || !(row.get("birth") instanceof Document birth) || !scope(scope).equals(birth.get("scope")) || !pipeline.equals(birth.get("pipelineId"))) throw unreadable("control");
        validateLedger(row, scope, pipeline);
    }
    /** Both birth formats retain the same bounded owner, generation and physical mode-slot ledger. */
    static void validateLedger(Document row, CacheBusinessScope scope, String pipeline) {
        if (!(row.get("countingGeneration") instanceof Long generation) || generation < 0 || !(row.get("revision") instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE || !(row.get("retainedRuns") instanceof Long count) || count < 0 || count > MAX_RETAINED_RUNS
                || !row.containsKey("owner") || !(row.get("closed") instanceof Boolean closed) || !(row.get("problemLimit") instanceof Boolean)) throw unreadable("control");
        var slots = slots(row); if (slots.size() > MAX_ACTIVE_RUNS || count < slots.size() || !closed && (slots.isEmpty() || row.getBoolean("problemLimit"))) throw unreadable("control-counts");
        if (generation == 0) { if (row.get("owner") != null || !closed || !slots.isEmpty() || count != 0 || row.getBoolean("problemLimit")) throw unreadable("control-first-use"); }
        else if (!(row.get("owner") instanceof Document current) || !scope.clusterId().equals(owner(current, pipeline).clusterId())) throw unreadable("control-owner");
        for (var entry : slots.entrySet()) {
            if (!(entry.getValue() instanceof Document id) || !id.keySet().equals(Set.of("kind", "clusterId", "pipelineId", "taskId", "mode", "runId"))
                    || !"run".equals(id.get("kind")) || !scope.clusterId().equals(id.get("clusterId")) || !pipeline.equals(id.get("pipelineId"))
                    || !(id.get("taskId") instanceof String task) || !(id.get("mode") instanceof String mode) || !(id.get("runId") instanceof String run)
                    || task.isBlank() || run.isBlank() || !Set.of("FULL", "INCREMENTAL").contains(mode) || !entry.getKey().equals(slotKey(task, mode))) throw unreadable("slot");
        }
        bounded(row, MAX_CONTROL_BYTES);
    }
    static void validateIndependentSlots(ClientSession session, MongoDatabase database, Document control, CacheBusinessScope scope, Document subject) {
        for (var id : slots(control).values()) {
            var row = SystemCollections.VALIDATION_RUNS.on(database).find(session, new Document("_id", id)).first();
            if (row == null) throw unreadable("missing-slot-run");
            var run = decodeRun(row);
            if (!runId(run).equals(row.get("_id")) || !Integer.valueOf(2).equals(row.get("formatVersion"))
                    || !scope(scope).equals(row.get("scope")) || !subject.equals(row.get("validationSubject"))
                    || !Boolean.FALSE.equals(row.get("retired")) || !current(control, run)) throw unreadable("slot-run");
            state(row);
        }
    }
    private Document row(ClientSession session, Run run) {
        var row = rows.find(session, new Document("_id", runId(run))).first(); if (row != null) requireRow(row, run); return row;
    }
    private void requireRow(Document row, Run run) {
        if (!runId(run).equals(row.get("_id")) || !Integer.valueOf(1).equals(row.get("formatVersion")) || !scope(scope).equals(row.get("scope"))
                || !identity(run).equals(row.get("identity")) || !(row.get("retired") instanceof Boolean)) throw unreadable("run");
        state(row);
    }
    private static Run decodeRun(Document row) {
        try {
            var id = row.get("identity", Document.class); var who = owner(id.get("owner", Document.class), id.getString("pipelineId"));
            var run = new Run(new ValidationRunIdentity(who, id.getString("taskId"), id.getString("runId"), ValidationMode.valueOf(id.getString("mode")),
                    id.getString("specHash"), id.getLong("keyMappingRevision"), id.getString("comparisonRulesVersion")), id.getLong("countingGeneration"));
            if (!identity(run).equals(id)) throw unreadable("run-identity"); return run;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) { throw unreadable("run-identity"); }
    }
    static ValidationOwner owner(Document value, String pipeline) {
        try {
            var who = new ValidationOwner(value.getString("clusterId"), pipeline, value.getString("ownerNodeId"), value.getString("ownerBootId"),
                    value.getLong("claimGeneration"), value.getLong("executionGeneration"), value.getLong("topologyRevision"));
            if (!WorkloadClaimDocuments.stored(ValidationClaims.fence(who)).equals(value)) throw unreadable("owner"); return who;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException | TapstateException invalid) { throw unreadable("owner"); }
    }
    private static ValidationRunState state(Document row) {
        try { return ValidationRunState.valueOf(row.getString("state")); }
        catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("state"); }
    }
    private void updateRun(ClientSession session, Run run, Document previous, Document values) {
        if (rows.updateOne(session, new Document("_id", runId(run)).append("identity", identity(run)).append("state", previous.get("state"))
                .append("retired", previous.get("retired")), new Document("$set", values)).getMatchedCount() != 1) throw unreadable("run-cas");
    }
    private void writeControl(ClientSession session, Document control) {
        long revision = control.getLong("revision"); if (revision == Long.MAX_VALUE - 1) throw limit();
        var next = new Document(control); next.put("revision", revision + 1); bounded(next, MAX_CONTROL_BYTES);
        if (rows.replaceOne(session, new Document("_id", control.get("_id")).append("scope", scope(scope)).append("revision", revision), next).getMatchedCount() != 1)
            throw unreadable("control-cas");
    }
    private static boolean current(Document control, Run run) {
        return Long.valueOf(run.countingGeneration()).equals(control.get("countingGeneration")) && WorkloadClaimDocuments.stored(run.fence()).equals(control.get("owner"));
    }
    private static boolean slot(Document control, Run run) { return runId(run).equals(slots(control).get(slotKey(run.identity()))); }
    private static void removeSlot(Document control, Run run) { slots(control).remove(slotKey(run.identity())); if (slots(control).isEmpty()) control.put("closed", true); }
    private static Document slots(Document control) { if (!(control.get("slots") instanceof Document slots)) throw unreadable("slots"); return slots; }
    static Document controlId(String cluster, String pipeline) { return new Document("kind", "pipeline").append("clusterId", cluster).append("pipelineId", pipeline); }
    static Document runId(Run run) { var id = run.identity(); return new Document("kind", "run").append("clusterId", id.owner().clusterId()).append("pipelineId", id.owner().pipelineId()).append("taskId", id.taskId()).append("mode", id.mode().name()).append("runId", id.runId()); }
    private static String slotKey(ValidationRunIdentity run) { return slotKey(run.taskId(), run.mode().name()); }
    private static String slotKey(String task, String mode) { return Base64.getUrlEncoder().withoutPadding().encodeToString(task.getBytes(StandardCharsets.UTF_8)) + ":" + mode; }
    static Document identity(Run run) { var id = run.identity(); return new Document("pipelineId", id.owner().pipelineId()).append("owner", WorkloadClaimDocuments.stored(run.fence()))
            .append("taskId", id.taskId()).append("runId", id.runId()).append("mode", id.mode().name()).append("specHash", id.specHash())
            .append("keyMappingRevision", id.keyMappingRevision()).append("comparisonRulesVersion", id.comparisonRulesVersion()).append("countingGeneration", run.countingGeneration()); }
    static Document scope(CacheBusinessScope scope) { return new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation()); }
    static void requireCollection(MongoDatabase database) {
        var found = database.listCollections().filter(new Document("name", SystemCollections.VALIDATION_RUNS.collectionName())).first();
        if (found == null || !"collection".equals(found.get("type"))) throw unreadable("validation-runs");
    }
    private static void bounded(Document value, int maximum) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build()); if (output.getSize() > maximum) throw limit();
        }
    }
    private static TapstateException limit() { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "validation-control"), null); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
    private static TapstateException unreadable(String field) { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "validation", "field", field), null); }
}
