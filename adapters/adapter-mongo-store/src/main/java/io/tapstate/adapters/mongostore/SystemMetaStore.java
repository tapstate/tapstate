package io.tapstate.adapters.mongostore;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * The one document recording how far the system data has been brought, and the lock the member doing
 * the bringing holds while it works.
 *
 * <p>The lock is a conditional update on this document rather than anything the cluster offers,
 * because of when it is needed: migration happens before the members have found each other, so asking
 * a cluster for a lock would be asking something that does not exist yet. The store, by contrast, has
 * already answered — it is the only thing that has.
 *
 * <p>Every write a holder makes carries the epoch it acquired. A holder that stalled long enough to be
 * taken over therefore cannot write anything afterwards: its updates match no document and it is told
 * so, instead of both members writing and the later one winning by arriving second.
 */
public final class SystemMetaStore {

    /** The id of the single document. There is one, and its name is part of the read every build does. */
    static final String SCHEMA_DOC_ID = "schema";
    /** The field carrying the version the store has been brought to. */
    static final String INSTALLED_VERSION = "installedVersion";

    private static final String LOCK = "lock";
    private static final String OWNER = "lock.owner";
    private static final String EPOCH = "lock.epoch";
    private static final String SINCE = "lock.since";
    private static final String HEARTBEAT = "lock.heartbeat";

    /** The server's error code for a unique-index collision, which is what losing the race looks like. */
    private static final int DUPLICATE_KEY = 11000;

    private final MongoCollection<Document> systemMeta;

    public SystemMetaStore(MongoDatabase database) {
        this.systemMeta = SystemCollections.SYSTEM_META.on(Objects.requireNonNull(database, "database"));
    }

    /**
     * The version the store says it is at, exactly as it is stored. Returned raw rather than as a
     * number: only this class ever writes the field, so a value that is not a number was written by
     * something else, and the caller has to be able to tell that apart from a version it simply does
     * not know. Empty when nothing has ever migrated this store.
     */
    public Optional<Object> installedVersion() {
        Document schema = systemMeta.find(new Document("_id", SCHEMA_DOC_ID)).first();
        return Optional.ofNullable(schema == null ? null : schema.get(INSTALLED_VERSION));
    }

    /** Who holds the lock, if anybody does. Read for the diagnostic when the wait runs out. */
    public Optional<Lock> lock() {
        Document schema = systemMeta.find(new Document("_id", SCHEMA_DOC_ID)).first();
        Document held = schema == null ? null : schema.get(LOCK, Document.class);
        if (held == null || held.getString("owner") == null) {
            return Optional.empty();
        }
        return Optional.of(new Lock(held.getString("owner"),
                held.get("epoch", Number.class).longValue(),
                toInstant(held.getDate("since"))));
    }

    /**
     * Takes the lock if it is free or the member holding it has stopped saying it is alive, and returns
     * the epoch taken. Empty means somebody else holds it and is still working.
     *
     * <p>Taking over from a stalled holder bumps the epoch, which is what stops that holder writing
     * anything afterwards. Bumping rather than setting is why the same update serves both cases: on a
     * store where the document does not exist yet the increment creates it at one.
     */
    public OptionalLong tryAcquire(String owner, Duration lockTtl, Instant now) {
        Document expiredOrFree = new Document("_id", SCHEMA_DOC_ID).append("$or", List.of(
                new Document(HEARTBEAT, null),
                new Document(HEARTBEAT, new Document("$lt", Date.from(now.minus(lockTtl))))));
        Document take = new Document("$set", new Document(OWNER, owner)
                        .append(SINCE, Date.from(now))
                        .append(HEARTBEAT, Date.from(now)))
                .append("$inc", new Document(EPOCH, 1L));
        try {
            Document taken = systemMeta.findOneAndUpdate(expiredOrFree, take,
                    new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
            return OptionalLong.of(taken.get(LOCK, Document.class).get("epoch", Number.class).longValue());
        } catch (MongoCommandException e) {
            // Not matching is how losing looks: the filter selected nothing because somebody holds the
            // lock and is still alive, so the upsert tried to insert a document whose id is already
            // there. Losing is a normal outcome of contending for a lock, not a failure to report.
            //
            // The exception is a command failure rather than a write failure, because it comes back
            // from findAndModify rather than from an insert. Catching the write kind instead compiles,
            // reads correctly, and lets a raw driver stack out of this module on the one path that
            // exists for two members starting together.
            if (e.getErrorCode() == DUPLICATE_KEY) {
                return OptionalLong.empty();
            }
            throw e;
        }
    }

    /**
     * Says the holder is still alive. False once the lock has been taken over, which is the holder's
     * signal to stop rather than carry on writing into a store somebody else is now changing.
     */
    public boolean heartbeat(long epoch, Instant now) {
        return systemMeta.updateOne(ownedAt(epoch),
                new Document("$set", new Document(HEARTBEAT, Date.from(now)))).getMatchedCount() == 1;
    }

    /**
     * Records that the store is now at {@code version}. Written the moment a changeset succeeds rather
     * than once they all have: a run interrupted half way is then resumed from the step after the last
     * one that finished, instead of from the beginning.
     */
    public boolean recordVersion(long epoch, int version) {
        return systemMeta.updateOne(ownedAt(epoch),
                new Document("$set", new Document(INSTALLED_VERSION, version))).getMatchedCount() == 1;
    }

    /**
     * Gives the lock up. A holder that never reaches this is taken over once its heartbeat goes stale.
     *
     * <p>The epoch is deliberately left behind. Removing the whole lock would restart the count at one
     * for the next holder, and the count is the entire basis of the fencing: two different holders
     * writing under the same epoch is exactly the thing it exists to make impossible. What is removed
     * is who held it and when, which is what leaves it free for the next.
     */
    public void release(long epoch) {
        systemMeta.updateOne(ownedAt(epoch), new Document("$unset",
                new Document(OWNER, "").append(SINCE, "").append(HEARTBEAT, "")));
    }

    /** Matches the document only while this epoch is still the one holding it. */
    private static Document ownedAt(long epoch) {
        return new Document("_id", SCHEMA_DOC_ID).append(EPOCH, epoch);
    }

    private static Instant toInstant(Date date) {
        return date == null ? Instant.EPOCH : date.toInstant();
    }

    /** Who holds the migration lock, under which epoch, and since when. */
    public record Lock(String owner, long epoch, Instant since) {
    }
}
