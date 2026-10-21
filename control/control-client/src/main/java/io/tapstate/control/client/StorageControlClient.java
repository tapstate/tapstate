package io.tapstate.control.client;

import java.net.URI;
import java.util.Map;
import java.util.Set;

/** Storage operation routes shared by connected clients; secrets occur only in POST bodies. */
public final class StorageControlClient {
    private StorageControlClient() {}
    public static final Set<String> OPERATIONS = Set.of("storage.status", "storage.test", "storage.setup.commit", "storage.setup.status",
            "storage.recovery.plan", "storage.recovery.confirm", "storage.recovery.status", "configuration.export", "configuration.import.preflight",
            "configuration.import.commit", "configuration.import.status");
    public static String path(String operation) {
        if (!OPERATIONS.contains(operation)) throw new IllegalArgumentException("not a storage operation");
        return "/api/" + operation.replace('.', '/');
    }
    /** Never retries a write whose result was lost; callers query the original operation explicitly. */
    public static ControlResponse execute(HttpControlClient client, URI server, String bearer, String operation, Map<String,Object> input) {
        return client.post(server, bearer, path(operation), input,
                operation.equals("storage.test") || operation.equals("storage.setup.commit") ? RequestBudget.CONNECTION : RequestBudget.HEAVY);
    }
}
