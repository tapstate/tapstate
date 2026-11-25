package io.tapstate.app;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorePort;
import java.time.Clock;
import java.util.Optional;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class StagedCloudRuntimeConfigurationTest {
    @Test void aCloseRequestedDuringTheFinalStoreReadCannotPublishCloudStatus() {
        var root = mock(StorageRoot.class, CALLS_REAL_METHODS);
        var runtime = mock(StorageRuntime.class);
        var business = mock(NativeBusinessContext.class);
        var store = mock(StorePort.class, RETURNS_DEEP_STUBS);
        ReflectionTestUtils.setField(root, "runtime", runtime);
        ReflectionTestUtils.setField(root, "installationActive", (Runnable) () -> {});
        when(runtime.businessForLiveness()).thenReturn(Optional.of(business));
        when(business.available()).thenReturn(true);
        when(business.statusStore()).thenReturn(store);
        when(store.desired().pipelineIds()).thenAnswer(ignored -> {
            ReflectionTestUtils.setField(root, "closeRequested", true);
            return List.of();
        });
        var properties = new CloudProperties();
        properties.setBaseUrl("https://cloud.example"); properties.setClusterId("cluster");
        properties.setAtlasUri("mongodb://config/config"); properties.setToken("fixture-token");
        var status = new StagedCloudRuntimeConfiguration().cloudRuntimeStatusProvider(
                CloudRuntimeSettings.resolve(properties), root, Clock.systemUTC());
        assertThatThrownBy(status::snapshot).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
    }
}
