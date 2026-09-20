package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet.Fence;

import java.util.ArrayList;
import java.util.List;

/** A versioned migration that installs one collection's declared indexes. */
public abstract class DeclaredCollectionIndexes implements ChangeSet {

    private final int version;
    private final SystemCollections collection;

    protected DeclaredCollectionIndexes(int version, SystemCollections collection) {
        this.version = version;
        this.collection = collection;
    }

    @Override
    public final int version() {
        return version;
    }

    @Override
    public final void up(MongoDatabase database, Fence fence) {
        for (SystemCollections.IndexSpec index : collection.indexes()) {
            fence.requireStillHeld();
            IndexEnsure.ensure(database, collection.indexTargetOn(database), index);
        }
    }

    @Override
    public final String dryRunSummary(MongoDatabase database) {
        List<String> planned = new ArrayList<>();
        for (SystemCollections.IndexSpec index : collection.indexes()) {
            planned.add(collection.indexTarget() + "." + index.indexName()
                    + (IndexEnsure.existing(collection.indexTargetOn(database), index) != null
                            ? " (already built)" : " (to build)"));
        }
        return "builds " + planned.size() + " index(es): " + String.join(", ", planned);
    }
}
