package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;

import java.time.Duration;

/** Readiness shared by tests that need a guarded object before inducing a later refusal. */
final class SplitBrainProtectionTestSupport {

    private SplitBrainProtectionTestSupport() {
    }

    static void awaitMinimumSize(String protectionName, HazelcastInstance... members) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!hasMinimumSize(protectionName, members)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(
                        "timed out waiting for split brain protection " + protectionName + " on every member");
            }
            Thread.sleep(50);
        }
    }

    private static boolean hasMinimumSize(String protectionName, HazelcastInstance... members) {
        for (HazelcastInstance member : members) {
            if (!member.getSplitBrainProtectionService()
                    .getSplitBrainProtection(protectionName)
                    .hasMinimumSize()) {
                return false;
            }
        }
        return true;
    }
}
