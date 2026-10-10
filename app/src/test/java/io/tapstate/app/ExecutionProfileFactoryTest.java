package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ParallelismBudget;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionProfileFactoryTest {
    private static final String ABI = "runtime-abi=tapstate-runtime-1\nprofile-protocol=1\npdk-api=2.0.8-SNAPSHOT\n"
            + "pdk-entity=2.0.9-SNAPSHOT\npdk-errorcode=2.8-SNAPSHOT\npdk-runner=2.8-SNAPSHOT\npdk-common=2.8-SNAPSHOT\n";

    @Test
    void constructingTheFactoryDoesNotLoadMetadataForDefaultNone() {
        AtomicInteger reads = new AtomicInteger();
        new ExecutionProfileFactory(new HazelcastProperties(), new ClusterProperties(),
                new ExecutionProfileProperties(), ParallelismBudget.DEFAULTS, new ClusterCapacityProperties().limits(),
                () -> 1_073_741_824L, () -> { reads.incrementAndGet(); return null; },
                () -> { reads.incrementAndGet(); return null; });
        assertThat(reads.get()).isZero();
    }

    @Test
    void actualHeapThreadsDeploymentBudgetsAndAbiChangeCompatibility() {
        HazelcastProperties hz = new HazelcastProperties();
        hz.getJet().setCooperativeThreadCount(4);
        var first = factory(hz, 1_073_741_824L, ABI).create();
        assertThat(first.attributes()).containsEntry("maxHeapBytes", "1073741824")
                .containsEntry("cooperativeThreads", "4").containsEntry("pdk-api", "2.0.8-SNAPSHOT")
                .doesNotContainKeys("connectorRegistry", "compiledDemand", "activeConnections");
        hz.getJet().setCooperativeThreadCount(8);
        assertThat(factory(hz, 1_073_741_824L, ABI).create().hash()).isNotEqualTo(first.hash());
        hz.getJet().setCooperativeThreadCount(4);
        assertThat(factory(hz, 2_147_483_648L, ABI).create().hash()).isNotEqualTo(first.hash());
        assertThat(factory(hz, 1_073_741_824L, ABI.replace("2.0.8-SNAPSHOT", "2.0.9-SNAPSHOT"))
                .create().hash()).isNotEqualTo(first.hash());
    }

    @Test
    void missingMetadataAndHeapBelowItsTierAreCodedBeforeMemberCreation() {
        assertThatThrownBy(() -> factory(new HazelcastProperties(), 1_073_741_824L,
                ABI.replace("pdk-api=2.0.8-SNAPSHOT", "pdk-api=${pdk-api.version}")).create())
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(BootError.EXECUTION_PROFILE_INVALID));
        assertThatThrownBy(() -> factory(new HazelcastProperties(), 1024, ABI).create())
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(BootError.EXECUTION_PROFILE_INVALID));
    }

    @Test
    void packagedMetadataUsesActualFilteredMavenCoordinates() {
        var profile = new ExecutionProfileFactory(new HazelcastProperties(), new ClusterProperties(),
                new ExecutionProfileProperties(), ParallelismBudget.DEFAULTS, new ClusterCapacityProperties().limits()).create();
        assertThat(profile.attributes()).containsKeys("productVersion", "runtime-abi", "pdk-api", "systemDataVersion");
        assertThat(profile.attributes().values()).allSatisfy(value -> assertThat(value).doesNotContain("${"));
    }

    @Test
    void actualRuntimeBuffersParticipateInCompatibilityAndMissingHandoffIsExplicit() {
        var configuration = new com.hazelcast.config.Config();
        configuration.getJetConfig().setCooperativeThreadCount(3);
        configuration.getJetConfig().getDefaultEdgeConfig().setQueueSize(256);
        configuration.addRingBufferConfig(new com.hazelcast.config.RingbufferConfig("srs.*").setCapacity(2048));
        var factory = factory(new HazelcastProperties(), 1_073_741_824L, ABI);
        var first = factory.create(configuration, 1000);
        assertThat(first.attributes()).containsEntry("cooperativeThreads", "3")
                .containsEntry("defaultEdgeQueueCapacity", "256").containsEntry("changeRingCapacity", "2048")
                .containsEntry("snapshotHandoffCapacity", "1000");
        configuration.getJetConfig().getDefaultEdgeConfig().setQueueSize(512);
        assertThat(factory.create(configuration, 1000).hash()).isNotEqualTo(first.hash());
        configuration.getJetConfig().getDefaultEdgeConfig().setQueueSize(256);
        assertThat(factory.create(configuration, 2000).hash()).isNotEqualTo(first.hash());
        assertThat(factory.create(configuration, null).attributes()).containsEntry("snapshotHandoffCapacity", "absent");
    }

    private static ExecutionProfileFactory factory(HazelcastProperties hz, long heap, String abi) {
        return new ExecutionProfileFactory(hz, new ClusterProperties(), new ExecutionProfileProperties(),
                ParallelismBudget.DEFAULTS, new ClusterCapacityProperties().limits(), () -> heap,
                () -> bytes("version=0.6.0\n"), () -> bytes(abi));
    }

    private static ByteArrayInputStream bytes(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }
}
