package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import org.bson.Document;

import java.util.List;
import java.util.Objects;

/**
 * The commit fence rows of a pipeline's operator state: one row per vertex instance, kept in the cache
 * database's {@code nest_state} under one reserved placement per pipeline scope.
 *
 * <p>A group commit compare-and-sets its own instance's row, conditioned on the tenure its group was proved
 * under, and reads the claim inside the same snapshot; it never writes the claim. That is what keeps the
 * instances of one pipeline from serializing on one shared document.
 *
 * <p>The safety of that rests on one invariant, which every caller here keeps: <b>any transaction that changes
 * which tenure may write a pipeline's operator state also rewrites every fence row of that pipeline carrying
 * another tenure.</b> A proof under a new tenure rewrites them to its own tenure before the tenure reads
 * anything; a release, a stop of the cache domain and a retirement rewrite them to a tenure no group holds,
 * inside the transaction that already ends the tenure. An old-tenure commit in flight then writes the same row
 * as that transaction, so one of the two aborts, and an old commit starting later matches nothing.
 *
 * <p>Rows are created only by a group's proof; a commit never upserts one, so a commit whose row is missing
 * matches nothing and is refused.
 */
final class MongoCommitFence {
    /**
     * The reserved coordinates. No step can be named with a NUL character, so no authored placement can
     * collide with them, and a store asked for this placement refuses it.
     */
    static final String STEP = "\u0000commit-fence";
    static final String PATH = "\u0000";
    static final String NAMESPACE = "fence";
    private static final String WRITES = "writes";
    /** The claim generation of a tenure no group holds; claim generations start at one. */
    private static final long ENDED_GENERATION = 0L;

    private MongoCommitFence() {}

    /** The fence placement of {@code scope}, in the cache database where its claim lives. */
    static OperatorPlacement placement(PipelineIntentScope scope, MongoDatabase cache) {
        return new OperatorPlacement(Objects.requireNonNull(scope, "scope"), cache.getName(),
                OperatorPlacement.Resource.NEST_STATE, STEP, PATH, NAMESPACE);
    }

    static boolean reserved(OperatorPlacement placement) {
        return placement.resource() == OperatorPlacement.Resource.NEST_STATE && STEP.equals(placement.stepId());
    }

    /** The tenure a group was proved under, as its fence row records it. */
    static Document tenure(WorkloadClaimFence claim, PipelineIntentScope scope) {
        return new Document("ownerBootId", claim.owner().bootId()).append("claimGeneration", claim.claimGeneration())
                .append("executionGeneration", claim.executionGeneration()).append("topologyRevision", claim.topologyRevision())
                .append("stateGeneration", scope.stateGeneration());
    }

    static Document rowId(PipelineIntentScope scope, MongoDatabase cache, String vertexInstance) {
        return new Document("placement", address(scope, cache)).append("key", OperatorStateText.encode(Objects.requireNonNull(vertexInstance, "vertexInstance")));
    }

    /**
     * The proof half: rewrites every fence row of the pipeline whose tenure differs to {@code tenure}, then
     * creates this instance's own row when it has none. A proof by the tenure the rows already carry writes
     * nothing, so the routine re-proofs of each instance never collide with its siblings' commits.
     */
    static void adopt(ClientSession session, MongoDatabase cache, PipelineIntentScope scope, String vertexInstance, Document tenure) {
        var rows = SystemCollections.CACHE_NEST_STATE.on(cache);
        rows.updateMany(session, new Document("_id.placement", address(scope, cache)).append("$nor", List.of(tenure)),
                new Document("$set", tenure).append("$inc", new Document(WRITES, 1L)));
        rows.updateOne(session, new Document("_id", rowId(scope, cache, vertexInstance)),
                new Document("$setOnInsert", new Document(tenure).append(WRITES, 0L)), new UpdateOptions().upsert(true));
    }

    /**
     * The commit half: the compare-and-set on this instance's own row. False when the row is missing or carries
     * another tenure, which the caller turns into a refusal of the whole group.
     */
    static boolean fence(ClientSession session, MongoDatabase cache, PipelineIntentScope scope, String vertexInstance, Document tenure) {
        var filter = new Document("_id", rowId(scope, cache, vertexInstance));
        filter.putAll(tenure);
        return SystemCollections.CACHE_NEST_STATE.on(cache).updateOne(session, filter,
                new Document("$inc", new Document(WRITES, 1L))).getMatchedCount() == 1;
    }

    /**
     * The tenure-end half, for a transaction that ends a tenure with no proof to follow (release, stop of the
     * cache domain, retirement): every fence row of the pipeline is set to a tenure no group holds, so a commit
     * in flight collides with this transaction instead of landing after it.
     */
    static void end(ClientSession session, MongoDatabase cache, PipelineIntentScope scope) {
        SystemCollections.CACHE_NEST_STATE.on(cache).updateMany(session,
                new Document("_id.placement", address(scope, cache)).append("claimGeneration", new Document("$ne", ENDED_GENERATION)),
                new Document("$set", new Document("claimGeneration", ENDED_GENERATION)).append("$inc", new Document(WRITES, 1L)));
    }

    private static Document address(PipelineIntentScope scope, MongoDatabase cache) {
        return MongoCacheOperatorStateStore.address(placement(scope, cache));
    }
}
