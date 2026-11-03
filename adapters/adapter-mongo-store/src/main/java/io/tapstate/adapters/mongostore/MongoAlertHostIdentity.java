package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import org.bson.Document;

/** A storage binding retains its creation capability; later setup credential rotation does not rebind it. */
final class MongoAlertHostIdentity {
    private MongoAlertHostIdentity() { }

    static boolean current(Document row, StorageSetupScope original, StorageBinding binding) {
        if (row == null || !original.clusterId().equals(row.get("clusterId"))
                || !original.installationId().equals(row.get("installationId"))
                || !original.operationId().equals(row.get("operationId"))
                || !(row.get("generation") instanceof Long current)) return false;
        if (binding == null) return current == original.generation();
        return original.equals(binding.setup()) && current >= original.generation();
    }
}
