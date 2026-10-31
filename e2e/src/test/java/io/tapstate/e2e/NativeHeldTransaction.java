package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import io.tapstate.adapters.mongostore.SystemCollections;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.bson.BsonValue;
import org.bson.Document;

/** An uncommitted lock on the original native append fence, attributed to one exact departed boot. */
final class NativeHeldTransaction implements AutoCloseable {
    private final MongoClient client;
    private final ClientSession session;
    private final BsonValue sessionId;
    private final String applicationName;

    NativeHeldTransaction(PreparedNativeStore storage, Document captureFence, String applicationName) {
        this(storage, captureFence, applicationName, null);
    }

    NativeHeldTransaction(PreparedNativeStore storage, Document captureFence, String applicationName,
            CommandListener listener) {
        this.applicationName = Objects.requireNonNull(applicationName);
        var settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(storage.cacheUri())).applicationName(applicationName);
        if (listener != null) settings.addCommandListener(listener);
        client = MongoClients.create(settings.build());
        ClientSession acquired = null;
        try {
            session = acquired = client.startSession();
            sessionId = session.getServerSession().getIdentifier().get("id");
            if (!"CAPTURE".equals(captureFence.getString("resourceType")))
                throw new AssertionError("the held transaction must name the source capture claim");
            Document id = new Document("clusterId", captureFence.getString("clusterId"))
                    .append("resourceType", "CAPTURE").append("resourceId", captureFence.getString("resourceId"));
            Document original = new Document("_id", id);
            for (String field : List.of("ownerNodeId", "ownerBootId", "claimGeneration", "executionGeneration", "topologyRevision"))
                original.append(field, captureFence.get(field));
            original.append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
            long acquisitionDeadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (true) {
                session.startTransaction();
                try {
                    long matched = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(client.getDatabase(storage.cacheDatabase()))
                            .updateOne(session, original, new Document("$inc", new Document("fencedAppends", 1L))).getMatchedCount();
                    if (matched != 1) throw new AssertionError("the original live capture fence is unavailable to hold");
                    break;
                } catch (MongoException contention) {
                    // Live claim renewal can win acquisition; no member has been killed yet.
                    if (!contention.hasErrorLabel("TransientTransactionError")
                            || System.nanoTime() - acquisitionDeadline >= 0) throw contention;
                    abort(session);
                    try { Thread.sleep(25); }
                    catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("interrupted while acquiring the original transaction hold", interrupted);
                    }
                }
            }
        } catch (RuntimeException | Error failure) {
            try { close(acquired, client); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Server-side transaction evidence, not the client session's locally retained transaction flag. */
    boolean stillHeld() {
        return client.getDatabase("admin").aggregate(List.of(
                new Document("$currentOp", new Document("allUsers", false).append("idleSessions", true)),
                new Document("$match", new Document("appName", applicationName)
                        .append("lsid.id", sessionId).append("transaction", new Document("$exists", true)))))
                .first() != null;
    }

    @Override public void close() {
        close(session, client);
    }

    private static void close(ClientSession session, MongoClient client) {
        try {
            abort(session);
        } finally {
            try { if (session != null) session.close(); } finally { client.close(); }
        }
    }

    private static void abort(ClientSession session) {
        try {
            if (session != null && session.hasActiveTransaction()) session.abortTransaction();
        } catch (MongoException ended) {
            if (ended.getCode() != 251) throw ended;
        }
    }
}
