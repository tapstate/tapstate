package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AuthenticationContextSelectionTest {
    private static Runnable guard(AtomicBoolean active) {
        return () -> { if (!active.get()) throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null); };
    }

    @Test void anExchangeCannotPublishOrSwitchToAnotherIssuerWhenItsSelectedContextCloses() {
        var beans = new DefaultListableBeanFactory();
        var active = new AtomicBoolean(true);
        var sessions = mock(SessionService.class); var login = mock(LoginService.class); var identity = mock(ClusterIdentityService.class);
        var nextSessions = mock(SessionService.class); var nextIdentity = mock(ClusterIdentityService.class);
        when(identity.identityView()).thenReturn(new ClusterIdentityView("old-issuer", "cluster"));
        var original = new LocalAuthenticationContext(login, sessions, identity, guard(active));
        var next = new LocalAuthenticationContext(mock(LoginService.class), nextSessions, nextIdentity, () -> {});
        var selected = new AtomicReference<>(original);
        beans.registerSingleton("contexts", (LocalAuthenticationContext.Provider)selected::get);
        when(sessions.exchange("tss_id.secret", "old-issuer")).thenAnswer(invocation -> {
            selected.set(next); active.set(false);
            return Optional.of(new AccessTokenGrant("old-token", Instant.now().plusSeconds(60), "admin", Scope.ADMIN));
        });
        var controller = new AuthController(beans.getBeanProvider(LocalAuthenticationContext.Provider.class),
                beans.getBeanProvider(LoginService.class), beans.getBeanProvider(SessionService.class),
                beans.getBeanProvider(ClusterIdentityService.class), beans.getBeanProvider(BootstrapService.class),
                beans.getBeanProvider(AuthenticationMode.class), beans.getBeanProvider(CloudAuthenticationService.class),
                beans.getBeanProvider(ManagedAuthenticationContext.Provider.class));
        var request = new MockHttpServletRequest(); request.addHeader("Authorization", AuthWire.sessionAuthorization("tss_id.secret"));
        assertThatThrownBy(() -> controller.session(request)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        verify(sessions).exchange("tss_id.secret", "old-issuer"); verifyNoInteractions(nextSessions, nextIdentity);
    }

    @Test void aLateCloudExchangeCannotPublishItsCookieAfterItsSelectedContextCloses() {
        var beans = new DefaultListableBeanFactory(); beans.registerSingleton("mode", AuthenticationMode.CLOUD);
        var active = new AtomicBoolean(true); var service = mock(CloudAuthenticationService.class); var next = mock(CloudAuthenticationService.class);
        var selected = new AtomicReference<>(new ManagedAuthenticationContext(service, guard(active)));
        beans.registerSingleton("contexts", (ManagedAuthenticationContext.Provider)selected::get);
        when(service.exchangeCode("code", "cluster.example")).thenAnswer(invocation -> {
            selected.set(new ManagedAuthenticationContext(next, () -> {})); active.set(false);
            return new CreatedCloudSession("cookie", Instant.now().plusSeconds(60));
        });
        var controller = new CloudAuthController(beans.getBeanProvider(AuthenticationMode.class),
                beans.getBeanProvider(CloudAuthenticationService.class), beans.getBeanProvider(CloudSessionCallbackVerifier.class),
                beans.getBeanProvider(ManagedAuthenticationContext.Provider.class));
        var request = new MockHttpServletRequest(); request.addHeader("Host", "cluster.example");
        assertThatThrownBy(() -> controller.exchange(request, "code")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        verify(service).exchangeCode("code", "cluster.example"); verifyNoInteractions(next);
    }
}
