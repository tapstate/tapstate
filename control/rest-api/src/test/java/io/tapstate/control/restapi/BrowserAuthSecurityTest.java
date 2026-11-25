package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

class BrowserAuthSecurityTest {
    private final BrowserAuthController controller = new BrowserAuthController(null, null, null,
            new DefaultListableBeanFactory().getBeanProvider(AuthenticationMode.class));

    {
        controller.publicOrigins("https://console.example");
    }

    @Test
    void untrustedTransportOriginsHeadersAndCookiesAreRefusedBeforeAuthorityAccess() {
        for (String origin : new String[] {"null", "https://other.example", "https://console.example/path",
                "https://user@console.example", "https://console.example#fragment"}) {
            MockHttpServletRequest request = request();
            request.removeHeader("Origin");
            request.addHeader("Origin", origin);
            refused(request);
        }
        MockHttpServletRequest missing = request();
        missing.removeHeader("Origin");
        refused(missing);
        MockHttpServletRequest custom = request();
        custom.removeHeader("X-Tapstate-Browser-Request");
        refused(custom);
        MockHttpServletRequest bearer = request();
        bearer.addHeader("Authorization", "Bearer ignored");
        refused(bearer);
        MockHttpServletRequest duplicates = request();
        duplicates.setCookies(new Cookie("__Host-tapstate-op-session", "tss_id.secret"),
                new Cookie("__Host-tapstate-op-session", "tss_other.secret"));
        refused(duplicates);
        MockHttpServletRequest wrongCookie = request();
        wrongCookie.setCookies(new Cookie("tapstate-op-session", "tss_id.secret"));
        refused(wrongCookie);
        MockHttpServletRequest plainRemote = request();
        plainRemote.setSecure(false);
        plainRemote.setScheme("http");
        plainRemote.setServerPort(80);
        plainRemote.removeHeader("Origin");
        plainRemote.addHeader("Origin", "http://console.example");
        refused(plainRemote);
    }

    @Test
    void remotePeerCannotUseALoopbackHostToReachBrowserAuthenticationOverHttp() {
        MockHttpServletRequest spoofedLoopback = new MockHttpServletRequest("POST", AuthWire.BROWSER_SESSION_PATH);
        spoofedLoopback.setSecure(false);
        spoofedLoopback.setScheme("http");
        spoofedLoopback.setServerName("localhost");
        spoofedLoopback.setServerPort(8080);
        spoofedLoopback.setRemoteAddr("203.0.113.9");
        spoofedLoopback.addHeader("Origin", "http://localhost:8080");
        spoofedLoopback.addHeader("X-Tapstate-Browser-Request", "1");
        spoofedLoopback.setCookies(new Cookie("tapstate-op-session", "tss_id.secret"));

        assertThatThrownBy(() -> controller.session(spoofedLoopback))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.UNAUTHENTICATED));
    }

    @Test
    void missingPeerAddressFailsClosedWithoutAnInternalError() {
        MockHttpServletRequest request = request();
        request.setRemoteAddr(null);
        assertThatThrownBy(() -> controller.session(request))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.UNAUTHENTICATED));
    }

    @Test
    void untrustedPeerCannotAssertAForwardedHttpsOrigin() {
        MockHttpServletRequest request = request();
        request.setSecure(false);
        request.setScheme("http");
        request.setServerPort(8080);
        request.setRemoteAddr("203.0.113.9");
        request.addHeader("X-Forwarded-Proto", "https");
        request.addHeader("X-Forwarded-Host", "console.example");
        request.addHeader("X-Forwarded-Port", "443");
        request.setCookies(new Cookie("__Host-tapstate-op-session", "tss_id.secret"));
        assertThatThrownBy(() -> controller.session(request))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.UNAUTHENTICATED));
    }

    @Test
    void plaintextLoopbackIsDisabledUnlessExplicitlyEnabled() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", AuthWire.BROWSER_SESSION_PATH);
        request.setSecure(false);
        request.setScheme("http");
        request.setServerName("127.0.0.1");
        request.setServerPort(8080);
        request.setRemoteAddr("127.0.0.1");
        request.addHeader("Origin", "http://127.0.0.1:8080");
        request.addHeader("X-Tapstate-Browser-Request", "1");
        request.setCookies(new Cookie("tapstate-op-session", "tss_id.secret"));
        assertThatThrownBy(() -> controller.session(request))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.UNAUTHENTICATED));
    }

    @Test
    void invalidPublicOriginConfigurationIsCodedAndRejected() {
        assertThatThrownBy(() -> controller.publicOrigins("http://console.example/path"))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.BROWSER_AUTH_CONFIG_INVALID));
    }

    private MockHttpServletRequest request() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", AuthWire.BROWSER_SESSION_PATH);
        request.setSecure(true);
        request.setScheme("https");
        request.setServerName("console.example");
        request.setServerPort(443);
        request.addHeader("Origin", "https://console.example");
        request.addHeader("X-Tapstate-Browser-Request", "1");
        return request;
    }

    private void refused(MockHttpServletRequest request) {
        assertThatThrownBy(() -> controller.session(request)).isInstanceOf(TapstateException.class);
    }
}
