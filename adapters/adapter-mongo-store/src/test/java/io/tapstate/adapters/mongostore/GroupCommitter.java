package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.GroupCommitStateStore;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.StateGroup;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A kill-test member that commits operator-state groups until it is killed: one vertex instance's group over
 * three namespaces, {@link #KEYS} keys a namespace, each key named after its group.
 *
 * <p>Arguments: the store URL, the database, the process id it connects as, and optionally {@code hold}.
 * Between a group's payload and its commit it prints {@code PAYLOAD <n>} and waits a moment, so a kill aimed
 * after that line lands inside the group's transaction; after the commit returns it prints
 * {@code COMMITTED <n>}. With {@code hold} it waits inside group 2 until it reads {@code commit}, and then
 * prints {@code COMMITTED 2} or {@code REFUSED <code>} and exits.
 */
public final class GroupCommitter {

    static final List<String> NAMESPACES = List.of("lookup", "references", "state");
    static final int KEYS = 10;

    private GroupCommitter() {
    }

    public static void main(String[] arguments) {
        String url = arguments[0];
        String database = arguments[1];
        String processId = arguments[2];
        try (var rig = CachePipelineRig.attach(url, database, processId);
             var client = CachePipelineRig.clientOf(CachePipelineRig.withDatabase(url, database), processId)) {
            var claim = rig.claims().readPipeline(CachePipelineRig.SCOPE).orElseThrow().claim();
            var named = namespaces(client, database, claim);
            StateGroup group = named.get(0).store().openGroup("assembler/0", named);
            boolean hold = arguments.length > 3 && arguments[3].equals("hold");
            var commands = new java.io.BufferedReader(new java.io.InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8));
            int[] current = {0};
            MongoStateGroup.afterPayload = () -> {
                System.out.println("PAYLOAD " + current[0]);
                System.out.flush();
                if (hold && current[0] == 2) {
                    try {
                        for (String command = commands.readLine(); command != null && !command.equals("commit"); command = commands.readLine()) { }
                    } catch (java.io.IOException unreadable) {
                        throw new java.io.UncheckedIOException(unreadable);
                    }
                } else {
                    sleep(20 + ThreadLocalRandom.current().nextInt(30));
                }
            };
            for (int n = 0; ; n++) {
                current[0] = n;
                Map<String, StateGroup.Changes> changes = new LinkedHashMap<>();
                for (var namespace : named) {
                    Map<String, byte[]> writes = new LinkedHashMap<>();
                    for (int k = 0; k < KEYS; k++) writes.put("g" + n + "-" + k, new byte[64]);
                    changes.put(namespace.namespace(), new StateGroup.Changes(writes, Map.of(), Set.of()));
                }
                try {
                    group.commit(changes);
                } catch (io.tapstate.core.common.TapstateException refused) {
                    System.out.println("REFUSED " + refused.code().code());
                    System.out.flush();
                    return;
                }
                System.out.println("COMMITTED " + n);
                System.out.flush();
                if (hold && n == 2) return;
            }
        }
    }

    /** One vertex instance's stores over the three namespaces, under {@code claim}, as a member opens them. */
    static List<GroupCommitStateStore.GroupNamespace> namespaces(com.mongodb.client.MongoClient client, String database,
            io.tapstate.spi.store.WorkloadClaim claim) {
        var named = new ArrayList<GroupCommitStateStore.GroupNamespace>();
        for (String namespace : NAMESPACES) {
            var placement = new OperatorPlacement(CachePipelineRig.SCOPE, database, OperatorPlacement.Resource.NEST_STATE,
                    CachePipelineRig.STEP, CachePipelineRig.PATH, namespace + "/assembler/0");
            var access = new MongoCachePipelineAccess(client, client.getDatabase(database), CachePipelineRig.SCOPE,
                    WorkloadClaimFence.from(claim), () -> { });
            named.add(new GroupCommitStateStore.GroupNamespace(new MongoCacheOperatorStateStore(access, placement), placement.mapName()));
        }
        return named;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
