package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.CommandFailedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;

/** A server-confirmed target command, independent of how often an authorization guard is called. */
final class MongoCommandWitness implements CommandListener, AutoCloseable {
    final MongoClient client;
    final MongoDatabase database;
    private final String databaseName, command, collection;
    private final Set<Integer> pending = ConcurrentHashMap.newKeySet();
    private final AtomicInteger completed = new AtomicInteger();
    MongoCommandWitness(String uri,String databaseName,String command,String collection) {
        this.databaseName = databaseName; this.command = command; this.collection = collection;
        this.client = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri)).addCommandListener(this).build());
        this.database = client.getDatabase(databaseName);
    }
    @Override public void commandStarted(CommandStartedEvent event) {
        if (databaseName.equals(event.getDatabaseName()) && command.equals(event.getCommandName())
                && event.getCommand().getString(command).getValue().equals(collection)) pending.add(event.getRequestId());
    }
    @Override public void commandSucceeded(CommandSucceededEvent event) {
        if (!pending.remove(event.getRequestId())) return;
        var response = event.getResponse();
        if (response.containsKey("writeErrors") && !response.getArray("writeErrors").isEmpty() || response.containsKey("writeConcernError")) return;
        boolean changed = command.equals("find")
                ? !response.getDocument("cursor").getArray("firstBatch").isEmpty()
                : response.containsKey("n") && response.getNumber("n").longValue() > 0;
        if (changed) completed.incrementAndGet();
    }
    @Override public void commandFailed(CommandFailedEvent event) { pending.remove(event.getRequestId()); }
    void refuseAfterCommand() {
        if (completed.get() > 0) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null);
    }
    void requireCommand() { assertThat(completed.get()).as("the target command reached the server before guard loss").isPositive(); }
    @Override public void close() { client.close(); }
}
