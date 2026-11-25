package io.tapstate.app;

import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScopedSinkStateProviderTest {
    @Test void absentMonitoredSourceIsReusedDuringWriterStartup() {
        var startup = new ScopedSinkStateProvider.WriterStartup();
        assertThat(startup.monitoredSource(Optional::empty)).isEmpty();
        assertThat(startup.monitoredSource(() -> {
            throw new AssertionError("the same startup must reuse the absent source");
        })).isEmpty();
    }

    @Test void failedMonitoredSourceReadCanBeRetried() {
        var startup = new ScopedSinkStateProvider.WriterStartup();
        var unavailable = new IllegalStateException("source unavailable");
        assertThatThrownBy(() -> startup.monitoredSource(() -> {
            throw unavailable;
        })).isSameAs(unavailable);
        assertThat(startup.monitoredSource(Optional::empty)).isEmpty();
    }
}
