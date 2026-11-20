package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineStateMappings;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class NativePipelineLogWindowTest {
    @Test
    void currentLifetimeLogsRemainReadableWhenCacheReadinessIsUnavailable() {
        var fixture = new Fixture();
        when(fixture.mappings.logWindow("orders")).thenReturn(
                Optional.of(new PipelineStateMappings.LogWindow(4, 7)));
        doThrow(ThreeLayerStorePort.refused()).when(fixture.original).requireBusiness();
        fixture.logs.append("orders", 3, new LogLine(0, "WARN", "retired lifetime"));
        fixture.logs.append("orders", 4, new LogLine(1, "WARN", "current lifetime"));
        fixture.logs.append("orders", 7, new LogLine(2, "WARN", "current rebuilt generation"));

        assertThat(fixture.service.logs("orders").lines()).extracting(LogLine::message)
                .containsExactly("current lifetime", "current rebuilt generation");
        verify(fixture.original).requireActive();
        verify(fixture.mappings).logWindow("orders");
    }

    @Test
    void anAbsentOrRetiredLifetimeCannotExposeBufferedLines() {
        var fixture = new Fixture();
        when(fixture.mappings.logWindow("orders")).thenReturn(Optional.empty());
        fixture.logs.append("orders", 4, new LogLine(0, "WARN", "retired lifetime"));

        assertThat(fixture.service.logs("orders").lines()).isEmpty();
        verify(fixture.original).requireActive();
        verify(fixture.mappings).logWindow("orders");
    }

    @Test
    void anInactiveOriginalBoundStorageCannotReadASuccessorsWindow() {
        var fixture = new Fixture();
        doThrow(ThreeLayerStorePort.refused()).when(fixture.original).requireActive();

        assertThatThrownBy(() -> fixture.service.logs("orders"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        verifyNoInteractions(fixture.mappings);
    }

    private static final class Fixture {
        final StorageRuntime.Bound original = mock(StorageRuntime.Bound.class);
        final PipelineStateMappings mappings = mock(PipelineStateMappings.class);
        final RingBufferLogSink logs = new RingBufferLogSink(2, 8);
        final PipelineLogQueryService service;

        Fixture() {
            var root = mock(StorageRoot.class);
            var runtime = mock(StorageRuntime.class);
            var configuration = mock(MongoConfigurationDomain.class);
            when(root.runtime()).thenReturn(runtime);
            when(runtime.requireConfiguration()).thenReturn(configuration);
            when(configuration.pipelineStates()).thenReturn(mappings);
            var context = new NativeBusinessContext(root, original, mock(ConfigurableApplicationContext.class));
            service = PipelineLogQueryService.scoped(logs, context::pipelineLogWindow);
        }
    }
}
