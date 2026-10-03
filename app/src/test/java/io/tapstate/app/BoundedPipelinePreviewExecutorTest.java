package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.probe.PipelinePreviewEvent;
import io.tapstate.runtime.probe.PipelinePreviewRequest;
import io.tapstate.spi.store.StorePort;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedPipelinePreviewExecutorTest {

    @Test
    void invalidCandidateEmitsSafeFailureAndReleasesItsPipelineLease() throws Exception {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("preview-executor-test-" + UUID.randomUUID());
        properties.setMemberPort(0);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        BoundedPipelinePreviewExecutor executor = new BoundedPipelinePreviewExecutor(
                unusedStore(), (ConnectorProvisioner) connectorId -> null, member, NestSettings.defaults(),
                java.time.Clock.systemUTC());
        try {
            var stream = executor.preview(new PipelinePreviewRequest(
                    "run-invalid", "author", "pipeline", "output", 3, "sample", "candidate-hash",
                    List.of("not: a tapstate resource"), Instant.now().plusSeconds(10)));
            PipelinePreviewEvent failed = stream.next();
            assertThat(failed.kind()).isEqualTo("run.failed");
            assertThat(failed.payload()).containsKeys("code", "reason");
            assertThat(failed.payload().get("code")).isEqualTo("dsl.unsupported-version");
            assertThat(member.<String, String>getMap(BoundedPipelinePreviewExecutor.LEASE_MAP).size()).isZero();
            assertThat(stream.next()).isNull();
        } finally {
            executor.close();
            member.shutdown();
        }
    }

    private static StorePort unusedStore() {
        return (StorePort) Proxy.newProxyInstance(StorePort.class.getClassLoader(), new Class<?>[] {StorePort.class},
                (proxy, method, args) -> {
                    throw new AssertionError("invalid candidate must fail before accessing store port");
                });
    }
}
