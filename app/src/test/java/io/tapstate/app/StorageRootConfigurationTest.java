package io.tapstate.app;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.HandlerExecutionChain;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class StorageRootConfigurationTest {
    @Test void aPublishedBusinessHandlerRetainsItsOriginalRequest() throws Exception {
        var root = root();
        var business = mock(NativeBusinessContext.class);
        when(root.runtime().businessForLiveness()).thenReturn(Optional.of(business));
        var request = new MockHttpServletRequest("GET", "/");
        var selected = new HandlerExecutionChain(new Object());
        when(business.handlerFor(request)).thenReturn(selected);

        assertThat(new StorageRootConfiguration().runtimeHttpHandlers(root).handlerFor(request)).isSameAs(selected);
        verify(business).handlerFor(request);
    }

    @Test void aCloseRequestedDuringSelectionCannotPublishABusinessHandler() throws Exception {
        var root = root();
        var business = mock(NativeBusinessContext.class);
        when(root.runtime().businessForLiveness()).thenReturn(Optional.of(business));
        var request = new MockHttpServletRequest("GET", "/");
        when(business.handlerFor(request)).thenAnswer(ignored -> {
            ReflectionTestUtils.setField(root, "closeRequested", true);
            return new HandlerExecutionChain(new Object());
        });

        assertThatThrownBy(() -> new StorageRootConfiguration().runtimeHttpHandlers(root).handlerFor(request))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
    }

    private static StorageRoot root() {
        var root = mock(StorageRoot.class, CALLS_REAL_METHODS);
        ReflectionTestUtils.setField(root, "runtime", mock(StorageRuntime.class));
        ReflectionTestUtils.setField(root, "installationActive", (Runnable) () -> {});
        return root;
    }
}
