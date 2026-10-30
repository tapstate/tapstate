package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AiProviderNativeContextTest {
    private static final String OWNER = JsonWriter.write(List.of("cloud", "organization", "cluster"));
    @Test void anEpochChangeInsideTheReadCannotPublishSettingsOrAdoptAnotherOwner() {
        var service = mock(AiProviderService.class); var auth = mock(CloudAuthenticationService.class);
        var next = mock(CloudAuthenticationService.class); var legacy = mock(CloudAuthenticationService.class);
        var live = new AtomicBoolean(true); var captures = new AtomicInteger();
        var context = new AtomicReference<>(new ManagedAuthenticationContext(auth, () -> active(live)));
        when(auth.clusterContextView("cookie")).thenReturn(Optional.of(view()));
        when(service.get(OWNER)).thenAnswer(call -> {
            live.set(false); context.set(new ManagedAuthenticationContext(next, () -> {}));
            return Optional.of(new AiProviderSettings("openai-compatible", "https://api.example.com/v1", "model", "private-key"));
        });
        var controller = controller(service, legacy, mock(AiProviderClient.class), () -> { captures.incrementAndGet(); return context.get(); });
        assertThatThrownBy(() -> controller.get(request())).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        assertThat(captures).hasValue(1); verify(service).get(OWNER); verifyNoInteractions(next, legacy);
    }
    @Test void failedContextChecksNeverLeakProviderCheckCapacityOrPublishSuccess() {
        var service = mock(AiProviderService.class); var auth = mock(CloudAuthenticationService.class);
        var legacy = mock(CloudAuthenticationService.class); var client = mock(AiProviderClient.class);
        var live = new AtomicBoolean(true);
        when(auth.clusterContextView("cookie")).thenReturn(Optional.of(view()));
        var settings = new AiProviderSettings("openai-compatible", "https://api.example.com/v1", "model", "private-key");
        when(service.resolve(OWNER, settings)).thenReturn(settings);
        doAnswer(call -> { live.set(false); return null; }).when(client).check(settings);
        var controller = controller(service, legacy, client, () -> new ManagedAuthenticationContext(auth, () -> active(live)));
        for (int attempt = 0; attempt < 3; attempt++) {
            live.set(true);
            assertThatThrownBy(() -> controller.check(request(), settings)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        }
        live.set(true); doNothing().when(client).check(settings);
        assertThat(controller.check(request(), settings).getBody()).containsEntry("ok", true);
        verifyNoInteractions(legacy);
    }
    private static AiProviderController controller(AiProviderService service, CloudAuthenticationService legacy,
            AiProviderClient client, ManagedAuthenticationContext.Provider context) {
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("service", service); beans.registerSingleton("legacy", legacy);
        beans.registerSingleton("mode", AuthenticationMode.CLOUD); beans.registerSingleton("context", context);
        return new AiProviderController(beans.getBeanProvider(AiProviderService.class), beans.getBeanProvider(CloudAuthenticationService.class),
                beans.getBeanProvider(AuthenticationMode.class), client, beans.getBeanProvider(ManagedAuthenticationContext.Provider.class));
    }
    private static MockHttpServletRequest request() { var request = new MockHttpServletRequest(); request.setCookies(new Cookie(CloudSessionCookies.NAME, "cookie")); return request; }
    private static CloudClusterContextView view() { return new CloudClusterContextView("organization", "cluster", "Org", "Cluster", "region", null, "UNKNOWN"); }
    private static void active(AtomicBoolean live) {
        if (!live.get()) throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
    }
}
