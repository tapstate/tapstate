package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationRunState;
import io.tapstate.spi.store.*;
import org.bson.Document;
import java.util.*;
import java.util.function.Function;
import static io.tapstate.adapters.mongostore.MongoValidationPreparationFacts.*;

/** Independent CACHE first birth and cancellation share one original Pipeline control key. */
final class MongoValidationCachePreparations {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).readPreference(ReadPreference.primary()).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final String targetFingerprint;
    private final Runnable active;

    MongoValidationCachePreparations(MongoClient client, MongoDatabase database, CacheBusinessScope scope, String targetFingerprint, Runnable active) {
        this.client = Objects.requireNonNull(client); this.database = Objects.requireNonNull(database); this.scope = Objects.requireNonNull(scope);
        this.targetFingerprint = Objects.requireNonNull(targetFingerprint); this.active = Objects.requireNonNull(active);
    }

    /** Native proof retains its actual CACHE reader; a matching DTO cannot impersonate a committed result. */
    static final class Proof {
        private final MongoValidationCachePreparations owner;
        private final Subject subject;
        private Proof(MongoValidationCachePreparations owner, Subject subject) { this.owner = owner; this.subject = subject; }
        Receipt requirePrepared(Subject expected) {
            if (!subject.equals(expected)) throw fenced(); var receipt = owner.observe(subject);
            if (receipt.outcome() != Outcome.PREPARED) throw fenced(); return receipt;
        }
        Receipt requireClosed(Subject expected) {
            if (!subject.equals(expected)) throw fenced(); var receipt = owner.observe(subject);
            if (receipt.outcome() == Outcome.PREPARED) throw fenced(); return receipt;
        }
        @Override public String toString() { return "ValidationCacheProof[original]"; }
    }

    Proof prepare(MongoValidationPreparations.Permission permission, WorkloadClaimFence node) {
        Objects.requireNonNull(permission); var before = permission.requireCurrent(); var subject = permission.subject();
        requireTarget(permission.binding(), subject); boolean first = permission.consumeFirstAttempt();
        mutate(node, session -> {
            var current = control(session, subject.pipelineId());
            if (current != null && subject.equals(binding(current))) {
                var receipt = receipt(session, current); if (receipt.outcome() != Outcome.PREPARED) throw fenced(); return null;
            }
            if (!first) throw unreadable();
            var predecessor = requirePredecessor(session, current, before.previous(), subject);
            var key = ValidationClaims.key(scope.clusterId(), subject.pipelineId());
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
            var retained = claims.find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
            Subject origin;
            if (predecessor == null || predecessor.origin() == null) {
                if (retained != null) throw unreadable(); origin = subject;
            } else {
                if (retained == null) throw unreadable(); origin = predecessor.origin();
            }
            var birth = birth(subject, origin); var replacement = root(subject, origin, Outcome.PREPARED, current);
            if (retained == null) {
                claims.insertOne(session, new Document("_id", MongoWorkloadClaimStore.id(key)).append("clusterId", key.clusterId())
                        .append("resourceType", key.type().name()).append("resourceId", key.resourceId())
                        .append("ownerNodeId", node.owner().nodeId()).append("ownerBootId", node.owner().bootId())
                        .append("claimGeneration", 1L).append("executionGeneration", 0L).append("topologyRevision", 0L)
                        .append("leaseUntil", new Date(0)).append("validationPreparation", birth).append("fencedValidationWrites", 0L));
            } else {
                if (claims.updateOne(session, new Document("_id", retained.get("_id")).append("validationPreparation", retained.get("validationPreparation"))
                        .append("claimGeneration", retained.get("claimGeneration")).append("executionGeneration", retained.get("executionGeneration")),
                        new Document("$set", new Document("validationPreparation", birth))).getMatchedCount() != 1) throw fenced();
            }
            replaceControl(session, current, replacement); return null;
        });
        permission.requireCurrent(); return new Proof(this, subject);
    }

    /** Returns PREPARED while original active runs still require known report closure and retirement. */
    Proof cancel(MongoValidationPreparations.Closing closing, WorkloadClaimFence node) {
        Objects.requireNonNull(closing); var state = closing.requireCurrent(); var subject = state.subject();
        if (subject == null) throw new IllegalArgumentException("an unissued CONFIG scope needs no CACHE cancellation");
        requireTarget(closing.binding(), subject);
        mutate(node, session -> {
            var current = control(session, subject.pipelineId());
            if (current == null || !subject.equals(binding(current))) {
                var previous = requirePredecessor(session, current, state.previous(), subject);
                var origin = previous == null ? null : previous.origin();
                var replacement = root(subject, origin, Outcome.CANCELLED, current); replacement.put("closingOperationId", closing.operation().toString());
                if (origin != null) rebindRetainedClaim(session, current, subject, origin);
                replaceControl(session, current, replacement); return null;
            }
            var known = receipt(session, current);
            if (known.outcome() != Outcome.PREPARED) return null;
            var already = current.getString("closingOperationId"); if (already != null && !already.equals(closing.operation().toString())) throw fenced();
            var next = new Document(current); next.put("admissionClosed", true); next.put("closingOperationId", closing.operation().toString());
            var slots = next.get("slots", Document.class);
            for (var raw : slots.values()) {
                if (!(raw instanceof Document id)) throw unreadable();
                var row = SystemCollections.VALIDATION_RUNS.on(database).find(session, new Document("_id", id)).first();
                if (row == null || !encode(subject).equals(row.get("validationSubject"))) throw unreadable();
                var status = runState(row);
                if (status == ValidationRunState.STARTING || status == ValidationRunState.RUNNING)
                    SystemCollections.VALIDATION_RUNS.on(database).updateOne(session, new Document("_id", id).append("state", status.name()),
                            new Document("$set", new Document("state", ValidationRunState.STOPPING.name())));
            }
            if (slots.isEmpty()) {
                var key = ValidationClaims.key(scope.clusterId(), subject.pipelineId());
                if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,
                        new Document("_id", MongoWorkloadClaimStore.id(key)).append("validationPreparation", current.get("birth")),
                        List.of(new Document("$set", new Document("leaseUntil", "$$NOW")))).getMatchedCount() != 1) throw fenced();
                next.put("birthState", Outcome.RETIRED.name());
            }
            bump(next); replaceControl(session, current, next); return null;
        });
        closing.requireCurrent(); return new Proof(this, subject);
    }

    Receipt observe(Subject subject) {
        requireScope(subject);
        return read(session -> {
            var current = control(session, subject.pipelineId());
            if (current == null || !subject.equals(binding(current))) throw unreadable(); return receipt(session, current);
        });
    }

    private Receipt requirePredecessor(ClientSession session, Document current, Receipt expected, Subject next) {
        if (current == null) {
            if (expected != null && expected.subject().scope().equals(scope)) throw unreadable();
            var key = ValidationClaims.key(scope.clusterId(), next.pipelineId());
            if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first() != null) throw unreadable();
            if (SystemCollections.VALIDATION_RUNS.on(database).find(session,
                    new Document("_id.clusterId", scope.clusterId()).append("_id.pipelineId", next.pipelineId())).limit(1).first() != null) throw unreadable();
            return null;
        }
        if (expected == null || !receipt(session, current).equals(expected) || expected.outcome() == Outcome.PREPARED
                || !current.get("slots", Document.class).isEmpty()) throw unreadable();
        return expected;
    }
    private void rebindRetainedClaim(ClientSession session, Document current, Subject next, Subject origin) {
        var key = ValidationClaims.key(scope.clusterId(), next.pipelineId());
        if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,
                new Document("_id", MongoWorkloadClaimStore.id(key)).append("validationPreparation", current.get("birth")),
                new Document("$set", new Document("validationPreparation", birth(next, origin)))).getMatchedCount() != 1) throw fenced();
    }
    private Receipt receipt(ClientSession session, Document root) {
        var subject = binding(root); var origin = origin(root); var state = outcome(root);
        long generation = 0, execution = 0;
        var key = ValidationClaims.key(scope.clusterId(), subject.pipelineId());
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var claim = table.find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        if (origin == null) { if (claim != null || state != Outcome.CANCELLED) throw unreadable(); }
        else {
            if (claim == null || !root.get("birth").equals(claim.get("validationPreparation"))) throw unreadable();
            var decoded = new MongoWorkloadClaimStore(table).inStrictSession(session).decodeStored(claim);
            generation = decoded.claimGeneration(); execution = decoded.executionGeneration();
            if (state != Outcome.PREPARED && new MongoWorkloadClaimStore(table).inStrictSession(session).read(key).orElseThrow().leased()) throw unreadable();
        }
        return new Receipt(subject, origin, state, longValue(root, "revision"), generation, execution);
    }
    private Document control(ClientSession session, String pipeline) {
        var row = SystemCollections.VALIDATION_RUNS.on(database).find(session, new Document("_id", MongoValidationControlStore.controlId(scope.clusterId(), pipeline))).first();
        if (row == null) return null;
        validateRoot(row, scope, pipeline); return row;
    }
    static Subject binding(Document root) { return subject(root.get("birth", Document.class).get("subject", Document.class)); }
    static Subject origin(Document root) { var raw = root.get("birth", Document.class).get("origin"); return raw == null ? null : subject((Document) raw); }
    static Outcome outcome(Document root) {
        try { return Outcome.valueOf(root.getString("birthState")); }
        catch (NullPointerException | ClassCastException | IllegalArgumentException bad) { throw unreadable(); }
    }
    static Document birth(Subject subject, Subject origin) {
        var value = new Document("formatVersion", 2).append("subject", encode(subject)).append("origin", origin == null ? null : encode(origin)); bounded(value); return value;
    }
    static void validateRoot(Document row, CacheBusinessScope scope, String pipeline) {
        try {
            if (!Integer.valueOf(2).equals(row.get("formatVersion")) || !(row.get("birth") instanceof Document birth)
                    || !Integer.valueOf(2).equals(birth.get("formatVersion")) || !birth.keySet().equals(Set.of("formatVersion", "subject", "origin"))) throw unreadable();
            var subject = binding(row); var origin = origin(row); var outcome = outcome(row);
            if (!scope.equals(subject.scope()) || !pipeline.equals(subject.pipelineId()) || !MongoValidationControlStore.scope(scope).equals(row.get("scope"))
                    || !MongoValidationControlStore.controlId(scope.clusterId(), pipeline).equals(row.get("_id"))
                    || origin != null && (!origin.scope().equals(scope) || !origin.pipelineId().equals(pipeline))
                    || origin == null && outcome != Outcome.CANCELLED || !(row.get("slots") instanceof Document slots)
                    || !(row.get("admissionClosed") instanceof Boolean closed) || outcome != Outcome.PREPARED && (!closed || !slots.isEmpty())
                    || !(row.get("revision") instanceof Long revision) || revision < 0 || revision == Long.MAX_VALUE) throw unreadable();
            bounded(birth);
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable(); }
    }
    private static Document root(Subject subject, Subject origin, Outcome outcome, Document previous) {
        var row = previous == null ? new Document("_id", MongoValidationControlStore.controlId(subject.scope().clusterId(), subject.pipelineId()))
                .append("formatVersion", 2).append("scope", MongoValidationControlStore.scope(subject.scope())).append("countingGeneration", 0L)
                .append("owner", null).append("closed", true).append("problemLimit", false).append("revision", 0L)
                .append("retainedRuns", 0L).append("slots", new Document()) : new Document(previous);
        if (previous != null) bump(row);
        row.put("birth", birth(subject, origin)); row.put("birthState", outcome.name()); row.put("admissionClosed", outcome != Outcome.PREPARED);
        row.remove("closingOperationId"); return row;
    }
    private static void bump(Document row) { long revision = longValue(row, "revision"); if (revision >= Long.MAX_VALUE - 1) throw unreadable(); row.put("revision", revision + 1); }
    private void replaceControl(ClientSession session, Document previous, Document next) {
        var rows = SystemCollections.VALIDATION_RUNS.on(database);
        if (previous == null) rows.insertOne(session, next);
        else if (rows.replaceOne(session, new Document("_id", previous.get("_id")).append("revision", previous.get("revision"))
                .append("birth", previous.get("birth")), next).getMatchedCount() != 1) throw fenced();
    }
    private <T> T mutate(WorkloadClaimFence node, Function<ClientSession, T> work) {
        return StoreIo.call(() -> {
            active.run(); MongoValidationControlStore.requireCollection(database);
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    active.run(); MongoCachePipelinePreparations.proveNode(session, database, scope, node);
                    new MongoCacheBusinessIdentity(database, scope).admitMetadataWrite(session);
                    var result = work.apply(session); active.run(); return result;
                }, TRANSACTION);
            }
        });
    }
    private <T> T read(Function<ClientSession, T> work) {
        return StoreIo.call(() -> {
            active.run(); MongoValidationControlStore.requireCollection(database);
            try (var session = client.startSession()) { return session.withTransaction(() -> {
                active.run(); new MongoCacheBusinessIdentity(database, scope).requireAdmissionState(session);
                var result = work.apply(session); active.run(); return result;
            }, TRANSACTION); }
        });
    }
    private void requireTarget(StorageBinding binding, Subject subject) {
        requireScope(subject); if (!targetFingerprint.equals(MongoStorageBindingStore.targetFingerprint(binding.cache()))) throw fenced();
    }
    private void requireScope(Subject subject) { if (!scope.equals(subject.scope())) throw fenced(); }
    private static ValidationRunState runState(Document row) {
        try { return ValidationRunState.valueOf(row.getString("state")); }
        catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable(); }
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
