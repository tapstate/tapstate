package io.tapstate.runtime.engine.join;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StandaloneJoinMemberTest {

    @Test
    void theImapFixtureCannotAttemptAutomaticDiscovery() {
        var config = ImapJoinStoresTest.memberConfig();

        assertThat(config.getNetworkConfig().getJoin().getAutoDetectionConfig().isEnabled()).isFalse();
        StandaloneJoinMember.requireStandalone(config);
    }

    @Test
    void everyCarrierTierDisablesAutomaticDiscovery() {
        for (String tier : new String[]{"nostore", "hot", "mixed", "cold"}) {
            var config = JoinBenchRun.memberConfig("F1", tier);

            assertThat(config.getNetworkConfig().getJoin().getAutoDetectionConfig().isEnabled())
                    .as("tier %s", tier).isFalse();
            StandaloneJoinMember.requireStandalone(config);
        }
    }

    @Test
    void enablingAutomaticDiscoveryIsRejectedBeforeAStartupCanBeTimed() {
        var config = ImapJoinStoresTest.memberConfig();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(true);

        assertThatThrownBy(() -> StandaloneJoinMember.start(config, "discovery-control"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("disable Jet and discovery");
    }

    @Test
    void oneRunningMemberCannotStandForTwoCases() {
        var member = StandaloneJoinMember.start(ImapJoinStoresTest.memberConfig(), "identity-control");
        try {
            assertThatThrownBy(() -> StandaloneJoinMember.recordStarted(member, "another-case", 0, 0))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("reused a member");
            assertThat(member.getLifecycleService().isRunning()).isTrue();
        } finally {
            member.shutdown();
        }
    }
}
