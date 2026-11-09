package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * A kill-test member that stops inside a write: it opens a transaction, writes the pipeline's claim document
 * the way every fenced state write proves its claim, says so, and waits with the transaction open until told
 * to commit. Frozen by the test while it waits, it is an owner stopped inside a group past its lease.
 *
 * <p>Arguments: the store URL, the database, the process id it connects as. It prints {@code HOLDING} once
 * the write is in, and on the command {@code commit} prints {@code COMMITTED} or {@code REFUSED <code>}.
 */
public final class FrozenClaimHolder {

    private FrozenClaimHolder() {
    }

    public static void main(String[] arguments) throws Exception {
        String url = arguments[0];
        String database = arguments[1];
        String processId = arguments[2];
        try (var client = CachePipelineRig.clientOf(url, processId); var session = client.startSession()) {
            session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                    .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build());
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(client.getDatabase(database)).updateOne(session,
                    new Document("_id", MongoWorkloadClaimStore.id(CachePipelineRig.KEY)),
                    new Document("$inc", new Document("heldWrites", 1L)));
            System.out.println("HOLDING");
            System.out.flush();
            var commands = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            for (String command = commands.readLine(); command != null; command = commands.readLine()) {
                if (!command.equals("commit")) continue;
                try {
                    session.commitTransaction();
                    System.out.println("COMMITTED");
                } catch (MongoException refused) {
                    System.out.println("REFUSED " + refused.getCode());
                }
                System.out.flush();
                return;
            }
        }
    }
}
