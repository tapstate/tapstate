package io.tapstate.runtime.engine.join;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** A new real member for one case or carrier sample, with its preparation recorded separately. */
final class StandaloneJoinMember {

    private static final Set<UUID> STARTED_MEMBERS = ConcurrentHashMap.newKeySet();

    private StandaloneJoinMember() {
    }

    static HazelcastInstance start(Config config, String fixture) {
        requireStandalone(config);
        long began = System.nanoTime();
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        long ended = System.nanoTime();
        try {
            recordStarted(member, fixture, began, ended);
            return member;
        } catch (RuntimeException | Error failure) {
            try {
                member.shutdown();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    static void requireStandalone(Config config) {
        var join = config.getNetworkConfig().getJoin();
        if (config.getJetConfig().isEnabled() || join.getMulticastConfig().isEnabled()
                || join.getTcpIpConfig().isEnabled() || join.getAutoDetectionConfig().isEnabled()) {
            throw new AssertionError("a standalone join fixture must disable Jet and discovery");
        }
    }

    static void recordStarted(HazelcastInstance member, String fixture, long began, long ended) {
        requireStandalone(member.getConfig());
        if (!member.getLifecycleService().isRunning() || member.getCluster().getMembers().size() != 1) {
            throw new AssertionError("a join fixture needs one running real member");
        }
        UUID identity = member.getCluster().getLocalMember().getUuid();
        if (!STARTED_MEMBERS.add(identity)) {
            throw new AssertionError("a join fixture reused a member across cases or carrier samples");
        }
        if (!fixture.matches("[A-Za-z0-9/_-]+") || ended < began) {
            throw new AssertionError("invalid join fixture receipt");
        }
        System.out.printf(Locale.ROOT,
                "join-member {\"schema\":1,\"fixture\":\"%s\",\"member_uuid\":\"%s\","
                        + "\"started_ns\":%d,\"completed_ns\":%d,\"startup_ns\":%d,"
                        + "\"members\":1,\"running\":true,\"auto_detection\":false,"
                        + "\"multicast\":false,\"tcp_ip\":false,\"jet\":false}%n",
                fixture, identity, began, ended, ended - began);
    }
}
