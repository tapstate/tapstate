package io.tapstate.app;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.StorePort;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** One server build can compare raw and cached reads without changing public query resolution. */
class HistoryRollupReadSelectionTest {

    @Configuration
    @EnableConfigurationProperties(MetricsHistoryProperties.class)
    static class Binding {
    }

    @Test
    void aStartupPropertySelectsRawReadsWithoutChangingTheSampleInterval() {
        new ApplicationContextRunner().withUserConfiguration(Binding.class)
                .withPropertyValues("tapstate.metrics.history.rollup-read-enabled=false")
                .run(context -> {
                    MetricsHistoryProperties settings = context.getBean(MetricsHistoryProperties.class);
                    assertThat(settings.isRollupReadEnabled()).isFalse();
                    assertThat(settings.getSampleInterval()).isEqualTo(Duration.ofMinutes(1));
                });
    }

    @Test
    void disablingCacheReadsSelectsRawWhileTheDefaultStillUsesThePersistedCache() throws Exception {
        StorePort store = mock(StorePort.class);
        RateHistoryStore raw = mock(RateHistoryStore.class);
        HistoryRollupStore rollups = mock(HistoryRollupStore.class);
        when(store.rateHistory()).thenReturn(raw);
        when(store.historyRollups()).thenReturn(rollups);
        @SuppressWarnings("unchecked")
        ObjectProvider<HistoryRollupWorker> workers = mock(ObjectProvider.class);
        Clock clock = Clock.systemUTC();
        HistoryCursorCodec cursors = new HistoryCursorCodec("query-mode-test-secret".getBytes(), clock);
        ControlPlaneConfiguration assembly = new ControlPlaneConfiguration();
        ArtifactQueryService artifacts = mock(ArtifactQueryService.class);
        HistoryRollupQueryHealth health = new HistoryRollupQueryHealth();
        MetricsHistoryProperties settings = new MetricsHistoryProperties();
        settings.setSampleInterval(Duration.ofMinutes(1));

        assertThat(rollupsOf(assembly.pipelineHistoryQueryService(
                artifacts, store, settings, clock, cursors, workers, health))).isSameAs(rollups);

        settings.setRollupReadEnabled(false);
        assertThat(rollupsOf(assembly.pipelineHistoryQueryService(
                artifacts, store, settings, clock, cursors, workers, health))).isNull();
    }

    private static Object rollupsOf(PipelineHistoryQueryService query) throws Exception {
        Field field = PipelineHistoryQueryService.class.getDeclaredField("rollups");
        field.setAccessible(true);
        return field.get(query);
    }
}
