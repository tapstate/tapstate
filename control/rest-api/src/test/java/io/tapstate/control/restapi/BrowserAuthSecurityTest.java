package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrowserAuthSecurityTest {
    private final BrowserAuthController controller = new BrowserAuthController(null, null, null,
            new DefaultListableBeanFactory().getBeanProvider(AuthenticationMode.class));

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

    private MockHttpServletRequest request() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/auth/browser/session");
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
