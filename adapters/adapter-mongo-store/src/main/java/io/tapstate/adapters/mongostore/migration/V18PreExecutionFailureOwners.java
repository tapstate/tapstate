package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;

/** Installs the interpretation gate for private execution-free diagnostic ownership. */
public final class V18PreExecutionFailureOwners implements ChangeSet {
    @Override public int version() { return 18; }
    @Override public void up(MongoDatabase database, Fence fence) { fence.requireStillHeld(); }
    @Override public String dryRunSummary(MongoDatabase database) {
        return "recognizes pre-execution refusal owners in the existing latest manifest; existing documents remain unchanged";
    }
}
