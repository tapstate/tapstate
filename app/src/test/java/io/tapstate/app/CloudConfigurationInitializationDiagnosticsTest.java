package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class CloudConfigurationInitializationDiagnosticsTest {
    private static final String OPERATION = "a9c7faf0-c2a4-437a-a698-767c515e37ec";

    @Test
    void theFirstPreparationFailureRemainsDiagnosableWhenTheNextEntryCanOnlyResume() {
        var settings = new MongoConnectionSettings("mongodb://fixture/config", null, Duration.ofSeconds(1));
        var properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example"); properties.setToken("token-secret-sentinel");
        properties.setClusterId("cluster"); properties.setAtlasUri(settings.uri());
        var cloud = CloudRuntimeSettings.resolve(properties);
        var bridge = mock(CloudSdkBridge.class);
        when(bridge.claimConfiguration(anyString(), anyString())).thenAnswer(call ->
                new CloudSdkBridge.ConfigurationInitialization("cluster", OPERATION, "CLAIMED", call.getArgument(1), true));
        var first = new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "MongoCommandException code=24", "secret", "parameter-secret-sentinel"),
                new IllegalArgumentException("cause-secret-sentinel"));
        var later = new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
        try (var logs = new Logs(); var domains = mockStatic(MongoConfigurationDomain.class);
                var bootstrap = mockStatic(MongoConfigurationBootstrap.class);
                var initialization = new CloudConfigurationInitialization(settings, cloud, bridge, BootId.fresh())) {
            domains.when(() -> MongoConfigurationDomain.open(settings, "cluster")).thenThrow(later);
            bootstrap.when(() -> MongoConfigurationBootstrap.prepare(settings, "cluster", "cloud-cluster", OPERATION, 1)).thenThrow(first);
            bootstrap.when(() -> MongoConfigurationBootstrap.resume(settings, "cluster", "cloud-cluster", OPERATION, 1)).thenThrow(later);

            assertThat(catchThrowable(initialization::open)).isSameAs(first);
            assertThat(catchThrowable(initialization::open)).isSameAs(later);
            assertThat(logs.events.list).hasSize(2);
            assertThat(logs.events.list.get(0).getFormattedMessage()).contains("stage=prepare", "io.store-unavailable",
                    "MongoCommandException code=24", "creationGranted=true", "creationInvoked=true");
            assertThat(logs.events.list.get(1).getFormattedMessage()).contains("stage=resume", later.code().code());
            assertSafe(logs);
            verify(bridge, times(1)).claimConfiguration(anyString(), anyString());
            bootstrap.verify(() -> MongoConfigurationBootstrap.prepare(settings, "cluster", "cloud-cluster", OPERATION, 1), times(1));
            bootstrap.verify(() -> MongoConfigurationBootstrap.resume(settings, "cluster", "cloud-cluster", OPERATION, 1), times(1));
        }
    }

    @Test
    void anUntrustedDriverDetailCannotTurnTheDiagnosticIntoASecretOrLogInjection() {
        var settings = new MongoConnectionSettings("mongodb://fixture/config", null, Duration.ofSeconds(1));
        var properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example"); properties.setToken("token-secret-sentinel");
        properties.setClusterId("cluster"); properties.setAtlasUri(settings.uri());
        var bridge = mock(CloudSdkBridge.class);
        var refusal = new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "MongoCommandException code=24\ncredential-secret-sentinel"),
                new IllegalArgumentException("cause-secret-sentinel"));
        doThrow(refusal).when(bridge).claimConfiguration(anyString(), anyString());
        try (var logs = new Logs(); var domains = mockStatic(MongoConfigurationDomain.class);
                var initialization = new CloudConfigurationInitialization(settings, CloudRuntimeSettings.resolve(properties), bridge, BootId.fresh())) {
            domains.when(() -> MongoConfigurationDomain.open(settings, "cluster")).thenThrow(refusal);
            assertThat(catchThrowable(initialization::open)).isSameAs(refusal);
            assertThat(logs.events.list).singleElement().satisfies(event ->
                    assertThat(event.getFormattedMessage()).contains("stage=claim", "io.store-unavailable", "detail=none", "creationGranted=unknown"));
            assertSafe(logs);
        }
    }

    private static void assertSafe(Logs logs) {
        for (var event : logs.events.list) {
            assertThat(event.getFormattedMessage()).doesNotContain("secret-sentinel", "mongodb://", "https://", "\n");
            assertThat(event.getThrowableProxy()).isNull();
        }
    }

    private static final class Logs implements AutoCloseable {
        final Logger logger = (Logger) LoggerFactory.getLogger(CloudConfigurationInitialization.class);
        final Level previous = logger.getLevel();
        final ListAppender<ILoggingEvent> events = new ListAppender<>();
        Logs() {
            events.setContext(logger.getLoggerContext()); events.start();
            logger.setLevel(Level.WARN); logger.addAppender(events);
        }
        @Override public void close() { logger.detachAppender(events); events.stop(); logger.setLevel(previous); }
    }
}
