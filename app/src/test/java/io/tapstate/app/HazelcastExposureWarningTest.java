package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.hazelcast.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HazelcastExposureWarningTest {

    private final ListAppender<ILoggingEvent> written = new ListAppender<>();
    private Logger logger;

    @BeforeEach
    void captureConfigurationWarnings() {
        logger = (Logger) LoggerFactory.getLogger(HazelcastConfiguration.class);
        written.start();
        logger.addAppender(written);
    }

    @AfterEach
    void stopCapturing() {
        logger.detachAppender(written);
        written.stop();
    }

    @Test
    void aRoutableBindAddressReplacesLoopbackAndWarnsAboutTheUnauthenticatedProtocol() {
        HazelcastProperties properties = bind(Map.of(
                "tapstate.hz.cluster-name", "cluster-red",
                "tapstate.hz.discovery.mode", "tcp-ip",
                "tapstate.hz.discovery.tcp-ip.seeds[0]", "10.20.0.12:5701",
                "tapstate.hz.bind-address", "10.20.0.11"));

        Config config = HazelcastConfiguration.memberConfig(properties);

        assertThat(config.getNetworkConfig().getInterfaces().getInterfaces())
                .containsExactly("10.20.0.11");
        assertThat(written.list).singleElement().satisfies(event ->
                assertThat(event.getFormattedMessage())
                        .contains("unauthenticated")
                        .contains("private network or NetworkPolicy"));
    }

    @Test
    void theDefaultLoopbackMemberEmitsNoExposureWarning() {
        HazelcastConfiguration.memberConfig(new HazelcastProperties());

        assertThat(written.list).isEmpty();
    }

    @Test
    void processFailureOnlyWarnsThatItIsNotNetworkPartitionSafe() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);

        HazelcastConfiguration.warnAboutClusterProfile(properties);

        assertThat(written.list).singleElement().satisfies(event ->
                assertThat(event.getFormattedMessage())
                        .contains("process-failure-only")
                        .contains("does not provide network-partition safety")
                        .contains("production-ha"));
    }

    private static HazelcastProperties bind(Map<String, String> values) {
        return new Binder(new MapConfigurationPropertySource(values))
                .bind("tapstate.hz", Bindable.of(HazelcastProperties.class))
                .orElseGet(HazelcastProperties::new);
    }
}
