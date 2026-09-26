package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.lifecycle.ExecutionPlan;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What the read faces take from the engine - a run's plan, and the members a run would take part on - is left out
 * of their answers while the engine cannot be asked, rather than failing the answer.
 *
 * <p>A member shuts its engine down when it can no longer prove its node session, and does not start it again by
 * itself; its store can come back before its engine does. A status or an explanation asked of it then can still say
 * what the store holds - the pipeline's state and why it last failed - and the plan and the waiting members, which
 * only a running engine holds, are the part it cannot say. Left to throw, the whole answer is an uncoded page, which
 * a caller cannot tell from the member having fallen over. Absent, they read as what they are: not known here now.
 *
 * <p>Only reading is let off. A run's plan written while the engine is down fails the start that writes it, as it
 * should: that start has no engine to submit to either.
 */
class WhatOnlyTheEngineKnowsIsLeftOutWhileItCannotBeAskedTest {

    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aStoppedEngineLeavesTheRunsPlanOutOfAReadInsteadOfFailingIt() {
        member = Hazelcast.newHazelcastInstance(config());
        HazelcastExecutionPlans plans = new HazelcastExecutionPlans(member);
        plans.record(plan("p"));
        assertThat(plans.current(List.of("p"))).as("readable while the engine runs").containsKey("p");

        member.shutdown();

        assertThat(plans.current(List.of("p"))).as("not known here while the engine is down").isEmpty();
    }

    @Test
    void aStoppedEngineLeavesTheMembersARunWouldTakePartOnOutOfAReadInsteadOfFailingIt() {
        member = Hazelcast.newHazelcastInstance(config());
        assertThat(ClusterMembershipGate.dataMembersIfReadable(member)).as("readable while the engine runs").hasSize(1);

        member.shutdown();

        assertThat(ClusterMembershipGate.dataMembersIfReadable(member)).as("not known here while it is down").isEmpty();
    }

    private static ExecutionPlan plan(String pipelineId) {
        return new ExecutionPlan(pipelineId, 1L, 1L, 1L, List.of("m1"),
                List.of(new ExecutionPlan.Node("serve.s", 4, "node-default", "native", 1, 4, 4, List.of(), 1024, 0L,
                        List.of("serve.s"))),
                Instant.parse("2026-09-26T10:00:00Z"));
    }

    private static Config config() {
        Config config = new Config();
        config.setClusterName("engine-not-asked-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        return config;
    }
}
