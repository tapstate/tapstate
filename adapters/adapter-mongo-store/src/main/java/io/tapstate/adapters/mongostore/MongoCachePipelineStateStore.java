package io.tapstate.adapters.mongostore;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.spi.store.StateStore;
import org.bson.Document;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** A load-only checkpoint view; normal convergence cannot create or retire physical state. */
final class MongoCachePipelineStateStore implements StateStore {
    private final MongoCachePipelineAccess access;
    MongoCachePipelineStateStore(MongoCachePipelineAccess access) { this.access = access; }

    @Override public Optional<CheckpointDoc> read(String pipelineId) {
        access.requirePipeline(pipelineId);
        return access.transaction(session -> Optional.of(MongoStateStore.toCheckpoint(access.state(session))));
    }

    @Override public void create(String pipelineId, String stateJson, Instant touchTime) {
        access.requirePipeline(pipelineId);
        throw MongoCachePipelineAccess.fenced();
    }

    @Override public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
        access.requirePipeline(pipelineId); Objects.requireNonNull(nextStateJson, "stateJson"); Objects.requireNonNull(touchTime, "touchTime");
        if (expectedEpoch < 0) throw new IllegalArgumentException("expected epoch must be nonnegative");
        return access.transaction(session -> {
            Document current = access.state(session);
            access.proveClaim(session);
            long epoch = current.getLong("epoch");
            if (epoch != expectedEpoch) return new CasOutcome.Fenced(epoch);
            access.updateState(session, current, new Document("stateJson", nextStateJson).append("touchMillis", touchTime.toEpochMilli()), true);
            return new CasOutcome.Applied(new CheckpointDoc(pipelineId, nextStateJson, epoch + 1, Instant.ofEpochMilli(touchTime.toEpochMilli())));
        });
    }

    @Override public void delete(String pipelineId) {
        access.requirePipeline(pipelineId);
        throw MongoCachePipelineAccess.fenced();
    }
}
