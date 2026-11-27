package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.bson.Document;

/** The display projection has one key per cluster and Pipeline, regardless of execution count. */
public final class MongoCachePipelineRunProgressStore {
    public enum Execution { KEEP, BIND, REQUIRE_BOUND }
    public record Snapshot(long startRevision, PipelineRunProgress progress) {}
    private static final SystemCollections TABLE = SystemCollections.CACHE_PIPELINE_RUN_PROGRESS;
    private final MongoCachePipelineAccess access;
    private final WorkloadClaimFence claim;

    MongoCachePipelineRunProgressStore(MongoCachePipelineAccess access, WorkloadClaimFence claim) {
        this.access = access;
        this.claim = claim;
    }

    public Optional<Snapshot> latest() {
        return access.pointRead(() -> {
            var row = rows().withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.MAJORITY)
                    .find(filter(access.scope)).first();
            return Optional.ofNullable(row).map(saved -> new Snapshot(saved.getLong("startRevision"), decode(saved)));
        });
    }

    /** The original claim touch and the projection change commit together. A no-op does not touch the claim. */
    public boolean update(long startRevision, PipelineRunProgress seed, Execution execution,
            UnaryOperator<PipelineRunProgress> change) {
        var writer = java.util.Objects.requireNonNull(claim, "writer claim");
        var authority = new Document("generation", writer.executionGeneration())
                .append("claim", writer.claimGeneration()).append("topology", writer.topologyRevision())
                .append("node", writer.owner().nodeId()).append("boot", writer.owner().bootId());
        return access.transaction(session -> {
            var id = key(access.scope);
            var preparation = MongoPipelinePreparationFacts.scope(access.scope);
            var saved = rows().find(session, new Document("_id", id)).first();
            if (saved != null && preparation.equals(saved.get("scope"))
                    && startRevision < saved.getLong("startRevision")) return false;
            boolean same = saved != null && preparation.equals(saved.get("scope"))
                    && Long.valueOf(startRevision).equals(saved.get("startRevision"));
            var bound = same ? saved.get("execution", Document.class) : null;
            if (execution == Execution.REQUIRE_BOUND && !authority.equals(bound)) return false;
            if (execution == Execution.BIND && writer.executionGeneration() == 0) throw MongoCachePipelineAccess.claimFenced();
            var nextBinding = execution == Execution.BIND ? authority : bound;
            var previous = same ? decode(saved) : seed;
            if (!previous.runId().equals(seed.runId())) throw access.unreadable("runId");
            var next = change.apply(previous);
            if (same && next.equals(previous) && java.util.Objects.equals(nextBinding, bound)) return true;
            access.state(session, true);
            access.proveClaim(session, true);
            var replacement = encode(next).append("_id", id).append("scope", preparation)
                    .append("startRevision", startRevision).append("execution", nextBinding);
            if (saved == null) rows().insertOne(session, replacement);
            else if (rows().replaceOne(session, new Document("_id", id), replacement).getMatchedCount() != 1)
                throw MongoCachePipelineAccess.claimFenced();
            return true;
        });
    }

    static long deleteOriginal(MongoDatabase database, ClientSession session, PipelineIntentScope scope) {
        return TABLE.on(database).deleteOne(session, filter(scope)).getDeletedCount();
    }

    private MongoCollection<Document> rows() { return TABLE.on(access.database); }

    private static Document key(PipelineIntentScope scope) {
        return new Document("clusterId", scope.clusterId()).append("pipelineId", scope.pipelineId());
    }

    private static Document filter(PipelineIntentScope scope) {
        return new Document("_id", key(scope)).append("scope", MongoPipelinePreparationFacts.scope(scope));
    }

    private static Document encode(PipelineRunProgress progress) {
        var load = progress.initialLoad();
        return new Document("runId", progress.runId()).append("version", progress.version())
                .append("active", progress.active().stream().map(a -> item(a.stage(), a.since())).toList())
                .append("milestones", progress.milestones().stream().map(m -> item(m.stage(), m.reachedAt())).toList())
                .append("initialLoad", load == null ? null : new Document("landed", load.landed()).append("total", load.total()))
                .append("failedStage", progress.failedStage() == null ? null : progress.failedStage().name())
                .append("readMode", progress.readMode() == null ? null : progress.readMode().name())
                .append("runFailed", progress.runFailed());
    }

    private PipelineRunProgress decode(Document row) {
        try {
            var active = row.getList("active", Document.class);
            var milestones = row.getList("milestones", Document.class);
            validateStages(active);
            validateStages(milestones);
            var load = row.get("initialLoad", Document.class);
            String failed = row.getString("failedStage");
            String mode = row.getString("readMode");
            return new PipelineRunProgress(row.getString("runId"), row.getLong("version"),
                    active.stream().map(a -> new PipelineRunProgress.Activity(stage(a), at(a))).toList(),
                    milestones.stream().map(m -> new PipelineRunProgress.Milestone(stage(m), at(m))).toList(),
                    load == null ? null : new PipelineRunProgress.LoadProgress(load.getLong("landed"), load.getLong("total")),
                    failed == null ? null : PipelineRunStage.valueOf(failed),
                    mode == null ? null : PipelineRunProgress.Mode.valueOf(mode), Boolean.TRUE.equals(row.getBoolean("runFailed")));
        } catch (IllegalArgumentException | ClassCastException | NullPointerException | java.time.DateTimeException unreadable) {
            throw access.unreadable("runProgress");
        }
    }

    private static void validateStages(java.util.List<Document> entries) {
        var stages = EnumSet.noneOf(PipelineRunStage.class);
        if (entries.size() > PipelineRunStage.values().length || entries.stream().anyMatch(e -> !stages.add(stage(e))))
            throw new IllegalArgumentException("unexpected run stages");
    }

    private static Document item(PipelineRunStage stage, Instant at) {
        return new Document("stage", stage.name()).append("at", at == null ? null : at.toString());
    }

    private static PipelineRunStage stage(Document entry) { return PipelineRunStage.valueOf(entry.getString("stage")); }
    private static Instant at(Document entry) {
        var time = entry.getString("at");
        return time == null ? null : Instant.parse(time);
    }
}
