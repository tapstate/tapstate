package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.MemberProcess;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/**
 * A member killed with SIGKILL inside a group's transaction leaves no part of that group in the store: every
 * group the store holds is whole, across every namespace it wrote.
 *
 * <p>A process, not a thread, so nothing in the member runs after the kill: no finally block, no abort, no
 * socket closed. Each trial kills after the member has written a group's payload and before it commits it, at
 * a random point in that window, and then reads what the store kept. A group split into several
 * transactions tears under this; one transaction per group cannot.
 */
@RequiresDocker
class GroupCommitKillIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-killed");
    private static final int TRIALS = 6;

    @Test
    void aGroupKilledInsideItsTransactionIsNeverPartlyInTheStore() {
        String url = MONGO.getReplicaSetUrl();
        int insideTransaction = 0;
        for (int trial = 0; trial < TRIALS; trial++) {
            var prepared = CachePipelineRig.prepare(url, OWNER, Duration.ofMinutes(5), Set.of("node-a"));
            try (var rig = prepared.rig();
                 var member = MemberProcess.start("committer-" + trial, GroupCommitter.class, Map.of(),
                         url, rig.database(), OWNER.bootId())) {
                int target = 3 + ThreadLocalRandom.current().nextInt(5);
                member.awaitLine("PAYLOAD " + target, Duration.ofSeconds(60));
                sleep(ThreadLocalRandom.current().nextInt(15));
                member.kill();
                boolean reported = member.output().contains("COMMITTED " + target);

                Map<String, Integer> byGroup = keysByGroup(rig);
                assertThat(byGroup.values())
                        .describedAs("trial %d: every group the store holds is whole, %d keys over %d namespaces; held %s",
                                trial, GroupCommitter.KEYS * 3, 3, byGroup)
                        .allMatch(keys -> keys == GroupCommitter.KEYS * GroupCommitter.NAMESPACES.size());
                for (int n = 0; n < target; n++) {
                    assertThat(byGroup).as("trial %d: group %d was reported committed", trial, n).containsKey("g" + n);
                }
                if (!reported && !byGroup.containsKey("g" + target)) insideTransaction++;
            }
        }
        assertThat(insideTransaction)
                .describedAs("kills that landed with a group's payload written and not committed; with none, this "
                        + "case never observed the window it is about")
                .isPositive();
    }

    /** Keys the store holds per group, across every namespace, the fence rows left out. */
    private static Map<String, Integer> keysByGroup(CachePipelineRig rig) {
        Map<String, Integer> byGroup = new TreeMap<>();
        try (var client = CachePipelineRig.clientOf(rig.url(), "reader")) {
            for (Document row : SystemCollections.CACHE_NEST_STATE.on(client.getDatabase(rig.database())).find()) {
                Document id = row.get("_id", Document.class);
                if (OperatorStateText.encode(MongoCommitFence.STEP).equals(id.get("placement", Document.class).get("stepId"))) continue;
                String key = OperatorStateText.decode(id.getString("key"));
                byGroup.merge(key.substring(0, key.indexOf('-')), 1, Integer::sum);
            }
        }
        return byGroup;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
