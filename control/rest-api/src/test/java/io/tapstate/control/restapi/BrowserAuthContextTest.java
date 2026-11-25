package io.tapstate.control.restapi;

import io.tapstate.control.core.AccessTokenService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.LocalAuthenticationContext;
import io.tapstate.control.core.LoginService;
import io.tapstate.control.core.PasswordHasher;
import io.tapstate.control.core.SessionService;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.SessionRecord;
import io.tapstate.spi.store.SessionStore;
import io.tapstate.spi.store.UserStore;
import jakarta.servlet.http.Cookie;
import java.time.Instant;
import java.time.Clock;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BrowserAuthContextTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void theNativeContextIsCapturedOnceAndCannotPublishAfterLosingItsAuthority(boolean loseDuringExchange) {
        var active = new AtomicBoolean(true);
        var guards = new AtomicInteger();
        var captures = new AtomicInteger();
        var lost = new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        var fixedIdentity = new ClusterIdentity("original-cluster");
        var identity = new ClusterIdentityService(port(ClusterIdentityStore.class, (method, args) -> {
            assertThat(method).isEqualTo("find");
            return Optional.of(fixedIdentity);
        }));
        var tokens = new AccessTokenService(port(TokenSigner.class, (method, args) -> {
            assertThat(method).isEqualTo("issue");
            return "access";
        }), Clock.systemUTC());
        var now = Instant.now();
        var session = new SessionRecord("id", "secret-hash", "admin", "ADMIN", fixedIdentity.issuer(), false,
                now, now, now.plusSeconds(60), now.plusSeconds(120));
        var sessions = new SessionService(port(SessionStore.class, (method, args) -> {
            assertThat(method).isEqualTo("exchange");
            assertThat(args[0]).isEqualTo("id");
            assertThat(args[1]).isEqualTo("secret-hash");
            assertThat(args[2]).isEqualTo(fixedIdentity.issuer());
            if (loseDuringExchange) active.set(false);
            return Optional.of(session);
        }), port(TokenSecrets.class, (method, args) -> {
            assertThat(method).isEqualTo("hash");
            assertThat(args[0]).isEqualTo("secret");
            return "secret-hash";
        }), tokens, Clock.systemUTC());
        var login = new LoginService(port(UserStore.class, BrowserAuthContextTest::unexpected),
                port(PasswordHasher.class, BrowserAuthContextTest::unexpected), tokens);
        var original = new LocalAuthenticationContext(login, sessions, identity, () -> {
            guards.incrementAndGet();
            if (!active.get()) throw lost;
        });
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean(LocalAuthenticationContext.Provider.class, () -> () -> {
                assertThat(captures.incrementAndGet()).as("one request retains its first authority").isEqualTo(1);
                return original;
            });
            context.registerBean(BrowserAuthController.class);
            context.refresh();
            var controller = context.getBean(BrowserAuthController.class);
            controller.publicOrigins("https://console.example");
            var request = new MockHttpServletRequest("POST", AuthWire.BROWSER_SESSION_PATH);
            request.setSecure(true);
            request.setScheme("https");
            request.setServerName("console.example");
            request.setServerPort(443);
            request.setRemoteAddr("203.0.113.10");
            request.addHeader("Origin", "https://console.example");
            request.addHeader("X-Tapstate-Browser-Request", "1");
            request.setCookies(new Cookie("__Host-tapstate-op-session", "tss_id.secret"));
            if (loseDuringExchange) {
                assertThatThrownBy(() -> controller.session(request)).isSameAs(lost);
            } else {
                var response = controller.session(request);
                assertThat(response.getBody().issuer()).isEqualTo(fixedIdentity.issuer());
                assertThat(response.getBody().token()).isEqualTo("access");
            }
            assertThat(captures).hasValue(1);
            assertThat(guards).hasValue(2);
        }
    }

    private static Object unexpected(String method, Object[] args) {
        throw new AssertionError("unexpected authentication port call: " + method);
    }

    @SuppressWarnings("unchecked")
    private static <T> T port(Class<T> type, java.util.function.BiFunction<String, Object[], Object> calls) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> calls.apply(method.getName(), args));
    }
}
