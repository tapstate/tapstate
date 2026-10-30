package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.StorageBinding;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;

/** Separate native CONFIG writer; its parent owns the transaction barrier and process lifetime. */
public final class ConfigurationImportTransactionProcess {
    private ConfigurationImportTransactionProcess() {}
    public static void main(String[] args) throws Exception {
        var settings = new MongoConnectionSettings(args[0],null,Duration.ofSeconds(5),args[2]);
        try (var domain = MongoConfigurationDomain.openExisting(settings); var observer = MongoClients.create(args[0])) {
            var binding = domain.bindings().load().orElseThrow();
            var scope = new ConfigurationImportStore.Scope(domain.clusterId(),binding.bindingVersion(),StorageBinding.Mode.OP,null);
            var ticket = domain.imports().find(scope,args[1]).orElseThrow().ticket();
            var gate = new AtomicBoolean(Boolean.parseBoolean(args[3]));
            var imports = domain.imports().guardedBy(() -> {
                if (!gate.get()) return;
                var row = SystemCollections.CONFIGURATION_IMPORTS.on(observer.getDatabase(new com.mongodb.ConnectionString(args[0]).getDatabase()))
                        .find(new Document("_id",ticket.id())).first();
                if (row != null && "COMMITTING".equals(row.getString("phase")) && gate.compareAndSet(true,false)) {
                    System.out.println("IMPORT_FINAL_TRANSACTION_READY"); System.out.flush();
                    try { if (System.in.read() != '\n') throw new IllegalStateException("parent did not release the final transaction"); }
                    catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
                }
            });
            var receipt = imports.commit(ticket,MongoConfigurationImportStoreIT.complete());
            System.out.println("IMPORT_COMMITTED "+JsonWriter.write(ConfigurationImportPayload.receiptTree(receipt)));
        }
    }
}
