package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;

/** Gates readers of the optional stop marker without rewriting any existing checkpoint. */
public final class V15StopReservationShape implements ChangeSet {

    @Override
    public int version() {
        return 15;
    }

    @Override
    public void up(MongoDatabase database, Fence fence) {
        fence.requireStillHeld();
    }

    @Override
    public String dryRunSummary(MongoDatabase database) {
        return "recognizes an optional stop reservation; existing checkpoints remain unchanged";
    }
}
