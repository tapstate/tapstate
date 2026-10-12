package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudSessionCallbackVerifier;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Real MVC/controller contract over controlled SDK ports, not a witness of the actual Cloud SDK. */
class CloudAuthApiTest {

    private static final Instant NOW = Instant.parse("2026-09-28T10:00:00Z");
    private static final String AUDIENCE = "cluster.example";
    private static final CloudSessionIdentity IDENTITY =
            new CloudSessionIdentity("https://cloud.example", "org-a", "cluster-a");
    private final DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
    private final MemoryStore store = new MemoryStore();
    private final MutableClock clock = new MutableClock();
    private final AtomicInteger exchanges = new AtomicInteger();
    private final AtomicInteger validations = new AtomicInteger();
    private final AtomicReference<String> jwtAudience = new AtomicReference<>(AUDIENCE);
    private final AtomicReference<String> requestAudience = new AtomicReference<>();
    private final CloudSessionService sessions = new CloudSessionService(store, IDENTITY, new TokenSecrets() {
        @Override public GeneratedSecret generate() { return new GeneratedSecret("unused", "local-secret", "hash-local-secret"); }
        @Override public String hash(String raw) { return "hash-" + raw; }
        @Override public boolean matches(String raw, String hash) { return hash(raw).equals(hash); }
    }, clock);
    private final CloudAuthenticationService authentication = new CloudAuthenticationService((code, cluster) -> {
        exchanges.incrementAndGet();
        return "raw-cloud-jwt-sentinel";
    }, (jwt, deployment, audience) -> {
        validations.incrementAndGet();
        requestAudience.set(audience);
        if (!jwtAudience.get().equals(audience)) return Optional.empty();
        return Optional.of(new CloudLoginIdentity(IDENTITY, "stable-user", "jti-a", Scope.WRITE,
                NOW.plusSeconds(900)));
    }, sessions);

    @ParameterizedTest
    @MethodSource("invalidHosts")
    void malformedRequestHostIsRejectedBeforeConsumingTheCode(String[] hosts) throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, false);
        var request = get(CloudAuthController.EXCHANGE_PATH).param("code", "one-use-code-secret-sentinel");
        if (hosts.length > 0) request.header(HttpHeaders.HOST, (Object[]) hosts);
        var response = mvc.perform(request).andExpect(status().isBadRequest()).andReturn().getResponse();
        assertThat(response.getContentAsString()).contains("control.malformed-request")
                .doesNotContain("one-use-code-secret-sentinel", "untrusted-host-secret-sentinel");
        assertThat(exchanges.get()).isZero();
        assertThat(validations.get()).isZero();
        assertThat(store.records).isEmpty();
    }

    private static Stream<Arguments> invalidHosts() {
        return Stream.of(
                new String[] {},
                new String[] {""},
                new String[] {" "},
                new String[] {"cluster.example", "other.example"},
                new String[] {"cluster.example,other.example"},
                new String[] {"https://cluster.example"},
                new String[] {"untrusted-host-secret-sentinel@cluster.example"},
                new String[] {"cluster.example/path"},
                new String[] {"cluster.example?query"},
                new String[] {"cluster.example#fragment"},
                new String[] {"cluster.example\\other.example"},
                new String[] {"cluster.example\nforged"},
                new String[] {"cluster.example:"},
                new String[] {"cluster.example:0"},
                new String[] {"cluster.example:65536"},
                new String[] {"cluster.example:-1"},
                new String[] {"cluster.example:abc"},
                new String[] {"bad_label.example"},
                new String[] {"-bad.example"},
                new String[] {"bad..example"},
                new String[] {"."},
                new String[] {"[fe80::1%25en0]"})
                .map(hosts -> Arguments.of((Object) hosts));
    }

    @ParameterizedTest
    @MethodSource("validRequestHosts")
    void handoffUsesOnlyTheNormalizedHostAndIgnoresForwardedAuthorities(String host, String audience) throws Exception {
        jwtAudience.set(audience);
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, false);
        mvc.perform(handoff("short-code", host)
                        .header("Forwarded", "for=127.0.0.1;host=forged.example;proto=https")
                        .header("X-Forwarded-Host", "forged.example")
                        .header("X-Forwarded-Port", "1234"))
                .andExpect(status().isFound());
        assertThat(requestAudience.get()).isEqualTo(audience);
        assertThat(exchanges.get()).isEqualTo(1);
        assertThat(validations.get()).isEqualTo(1);
        assertThat(store.records).hasSize(1);
    }

    private static Stream<Arguments> validRequestHosts() {
        return Stream.of(
                Arguments.of("Cluster-A.Dev.Cloud.Tapstate.Com:443", "cluster-a.dev.cloud.tapstate.com"),
                Arguments.of("cluster-a.cloud.tapstate.com", "cluster-a.cloud.tapstate.com"),
                Arguments.of("Data.Customer.Example:18443", "data.customer.example"),
                Arguments.of("Cluster.Example.", "cluster.example"),
                Arguments.of("localhost:8080", "localhost"),
                Arguments.of("127.0.0.1:65535", "127.0.0.1"),
                Arguments.of("[2001:DB8::1]:443", "[2001:db8::1]"));
    }

    @Test
    void aDifferentHostCannotBeReplacedByAForgedForwardedHost() throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, false);
        var response = mvc.perform(handoff("short-code", "different.example")
                        .header("X-Forwarded-Host", AUDIENCE)
                        .header("Forwarded", "host=" + AUDIENCE))
                .andExpect(status().isUnauthorized()).andReturn().getResponse();
        assertThat(response.getContentAsString()).contains("control.unauthenticated");
        assertThat(requestAudience.get()).isEqualTo("different.example");
        assertThat(exchanges.get()).isEqualTo(1);
        assertThat(validations.get()).isEqualTo(1);
        assertThat(store.records).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = " \t ")
    void aMissingOrBlankCodeWithAValidHostStillCannotBeExchanged(String code) throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, false);
        var request = get(CloudAuthController.EXCHANGE_PATH).header(HttpHeaders.HOST, AUDIENCE);
        if (code != null) request.param("code", code);
        var response = mvc.perform(request).andExpect(status().isBadRequest()).andReturn().getResponse();
        Object body = JsonReader.parse(response.getContentAsString());
        assertThat(body).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) body).get("code")).isEqualTo(ControlError.MALFORMED_REQUEST.code());
        assertThat(exchanges.get()).isZero();
        assertThat(validations.get()).isZero();
        assertThat(store.records).isEmpty();
        assertThat(store.revoked).isEmpty();
    }

    @Test
    void handoffSetsOnlyAHostOnlyHttpOnlyCookieAndRedirectsWithoutJwtOrCode() throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, false);
        var response = mvc.perform(handoff("short-code"))
                .andExpect(status().isFound()).andReturn().getResponse();
        assertThat(response.getHeader(HttpHeaders.LOCATION)).isEqualTo("/");
        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
        assertThat(response.getHeader("Referrer-Policy")).isEqualTo("no-referrer");
        String cookie = response.getHeader(HttpHeaders.SET_COOKIE);
        assertThat(cookie).contains(CloudSessionCookies.NAME + "=tcs_", "Path=/", "Secure", "HttpOnly", "SameSite=Lax")
                .doesNotContain("Domain=", "Max-Age=", "raw-cloud-jwt-sentinel", "short-code");
        assertThat(response.getContentAsString()).isEmpty();
        clock.now = NOW.plusSeconds(20 * 60);
        assertThat(authentication.authenticate(cookieValue(cookie))).isPresent();
        assertThat(exchanges.get()).isEqualTo(1);
        assertThat(validations.get()).isEqualTo(1);
    }

    @Test
    void onPremCannotUseHandoffOrBackChannelAndCloudWithoutSdkFailsClosed() throws Exception {
        MockMvc op = mvc(AuthenticationMode.ON_PREM, false, false);
        op.perform(handoff("code")).andExpect(status().isForbidden());
        op.perform(callback("controlled")).andExpect(status().isForbidden());
        assertThat(store.records).isEmpty();
        DefaultListableBeanFactory other = new DefaultListableBeanFactory();
        other.registerSingleton("mode", AuthenticationMode.CLOUD);
        MockMvc missing = controllerMvc(other);
        missing.perform(handoff("code"))
                .andExpect(status().isServiceUnavailable());
        missing.perform(callback("controlled")).andExpect(status().isServiceUnavailable());
    }

    @Test
    void jtiAndUserCredentialsAloneCannotAuthorizeTheRevocationCallback() throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, true);
        String cookie = authentication.exchangeCode("code", AUDIENCE).token();
        mvc.perform(callback("invalid-proof")).andExpect(status().isUnauthorized());
        mvc.perform(callback("invalid-proof")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer raw-user-jwt")
                        .cookie(new Cookie(CloudSessionCookies.NAME, cookie)))
                .andExpect(status().isUnauthorized());
        assertThat(authentication.authenticate(cookie)).isPresent();
        assertThat(store.revoked).isEmpty();
    }

    @Test
    void authenticatedCallbackInvalidatesAnActiveLocalSessionEvenAfterJwtExpiryAndIsIdempotent() throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, true);
        String cookie = authentication.exchangeCode("code", AUDIENCE).token();
        clock.now = NOW.plusSeconds(20 * 60);
        assertThat(authentication.authenticate(cookie)).isPresent();
        for (int request = 0; request < 2; request++) {
            mvc.perform(callback("controlled")).andExpect(status().isNoContent());
        }
        assertThat(authentication.authenticate(cookie)).isEmpty();
        assertThat(validations.get()).isEqualTo(1);
    }

    @Test
    void anEarlyAuthenticatedCallbackPreventsALateHandoffFromResurrectingTheSession() throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, true);
        mvc.perform(callback("controlled")).andExpect(status().isNoContent());
        mvc.perform(handoff("code")).andExpect(status().isUnauthorized());
        assertThat(store.records).isEmpty();
    }

    @Test
    void missingCallbackVerifierCannotBeReplacedByTheOutboundToken() throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, false);
        mvc.perform(callback("outbound-cloud-token")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer outbound-cloud-token"))
                .andExpect(status().isServiceUnavailable());
        assertThat(store.revoked).isEmpty();
    }

    @Test
    void malformedCallbackParametersNeverEchoTheirValuesOrWriteARevocation() throws Exception {
        MockMvc mvc = mvc(AuthenticationMode.CLOUD, true, true);
        var invalid = mvc.perform(post(CloudAuthController.INVALIDATE_PATH)
                        .param("jti", "query-secret-sentinel")
                        .param("ts", "")
                        .param("nonce", "controlled-nonce")
                        .param("sign", "controlled"))
                .andExpect(status().isBadRequest()).andReturn().getResponse();
        assertThat(invalid.getContentAsString()).doesNotContain("query-secret-sentinel");
        assertThat(store.revoked).isEmpty();
        mvc.perform(post(CloudAuthController.INVALIDATE_PATH)
                        .param("jti", "").param("ts", "1780000000000")
                        .param("nonce", "controlled-nonce").param("sign", "controlled"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void localLogoutClearsTheCookieWithoutCloudCallsAndRejectsCrossOriginWrites() throws Exception {
        mvc(AuthenticationMode.CLOUD, true, false);
        AuthController controller = new AuthController(null, null, null, null,
                beans.getBeanProvider(AuthenticationMode.class), beans.getBeanProvider(CloudAuthenticationService.class));
        String cookie = authentication.exchangeCode("code", AUDIENCE).token();
        MockHttpServletRequest request = new MockHttpServletRequest("POST", AuthWire.LOGOUT_PATH);
        request.setScheme("https"); request.setServerName("cluster.example"); request.setServerPort(443);
        request.setCookies(new Cookie(CloudSessionCookies.NAME, cookie));
        request.addHeader("Origin", "https://other.example");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> controller.logout(request))
                .isInstanceOf(TapstateException.class);
        assertThat(authentication.authenticate(cookie)).isPresent();
        request.removeHeader("Origin"); request.addHeader("Origin", "https://cluster.example");
        var response = controller.logout(request);
        assertThat(response.getStatusCode().value()).isEqualTo(204);
        assertThat(response.getHeaders().getFirst(HttpHeaders.SET_COOKIE)).contains("Max-Age=0");
        assertThat(authentication.authenticate(cookie)).isEmpty();
        assertThat(exchanges.get()).isEqualTo(1); assertThat(validations.get()).isEqualTo(1);
    }

    @Test
    void cloudConverterRejectsBearersAmbiguousCookiesAndCrossSiteRequests() {
        CloudCookieAuthenticationConverter converter = new CloudCookieAuthenticationConverter();
        String cookie = authentication.exchangeCode("code", AUDIENCE).token();
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/artifacts:apply");
        request.setCookies(new Cookie(CloudSessionCookies.NAME, cookie));
        assertThat(((TapstateCredentialAuthenticationToken) converter.convert(request)).credential()).isEqualTo(cookie);
        request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer jwt");
        assertThat(((TapstateCredentialAuthenticationToken) converter.convert(request)).credential()).isEmpty();
        request.removeHeader(HttpHeaders.AUTHORIZATION);
        request.setCookies(new Cookie(CloudSessionCookies.NAME, cookie), new Cookie(CloudSessionCookies.NAME, cookie));
        assertThat(converter.convert(request)).isNull();
        request.setCookies(new Cookie(CloudSessionCookies.NAME, cookie)); request.addHeader("Sec-Fetch-Site", "same-site");
        assertThat(((TapstateCredentialAuthenticationToken) converter.convert(request)).credential()).isEmpty();
    }

    private MockMvc mvc(AuthenticationMode mode, boolean withSdkPorts, boolean withCallback) {
        beans.registerSingleton("mode", mode);
        if (withSdkPorts) beans.registerSingleton("cloudAuthentication", authentication);
        if (withCallback) beans.registerSingleton("callbacks", (CloudSessionCallbackVerifier)
                (issuer, org, cluster, method, timestamp, nonce, data, signature) ->
                issuer.equals(IDENTITY.issuer()) && org.equals(IDENTITY.organizationId()) && cluster.equals(IDENTITY.clusterId())
                        && method.equals("POST") && timestamp.equals("1780000000000")
                        && nonce.equals("controlled-nonce") && data.equals("jti-a")
                        && signature.equals("controlled"));
        return controllerMvc(beans);
    }

    private static MockMvc controllerMvc(DefaultListableBeanFactory beans) {
        CloudAuthController controller = new CloudAuthController(beans.getBeanProvider(AuthenticationMode.class),
                beans.getBeanProvider(CloudAuthenticationService.class),
                beans.getBeanProvider(CloudSessionCallbackVerifier.class));
        return MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new ApiExceptionHandler(MessageCatalog.bundled())).build();
    }

    private static MockHttpServletRequestBuilder callback(String signature) {
        return post(CloudAuthController.INVALIDATE_PATH)
                .param("jti", "jti-a")
                .param("ts", "1780000000000")
                .param("nonce", "controlled-nonce")
                .param("sign", signature);
    }

    private static MockHttpServletRequestBuilder handoff(String code) {
        return handoff(code, AUDIENCE);
    }

    private static MockHttpServletRequestBuilder handoff(String code, String host) {
        return get(CloudAuthController.EXCHANGE_PATH).param("code", code).header(HttpHeaders.HOST, host);
    }

    private static String cookieValue(String header) { return header.substring(header.indexOf('=') + 1, header.indexOf(';')); }
    private static final class MutableClock extends Clock {
        Instant now = NOW;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
    private record Key(CloudSessionIdentity identity, String jti) { }
    private static final class MemoryStore implements CloudSessionStore {
        final Map<Key, CloudSessionRecord> records = new HashMap<>();
        final Set<Key> revoked = new HashSet<>();
        @Override public boolean create(CloudSessionRecord record) {
            Key key = new Key(record.identity(), record.jwtId());
            return !revoked.contains(key) && records.putIfAbsent(key, record) == null;
        }
        @Override public Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jti) {
            return Optional.ofNullable(records.get(new Key(identity, jti)));
        }
        @Override public Optional<CloudSessionRecord> authenticate(CloudSessionIdentity identity, String jti, String hash, Instant now, Instant expiry) {
            Key key = new Key(identity, jti); CloudSessionRecord old = records.get(key);
            if (old == null || revoked.contains(key) || !old.secretHash().equals(hash) || !old.idleExpiresAt().isAfter(now)) return Optional.empty();
            CloudSessionRecord touched = new CloudSessionRecord(identity, jti, hash, old.userId(), old.scope(), false, old.createdAt(), now, expiry);
            records.put(key, touched); return Optional.of(touched);
        }
        @Override public boolean logout(CloudSessionIdentity identity, String jti, String hash, Instant now) {
            Key key = new Key(identity, jti); CloudSessionRecord old = records.get(key);
            if (old == null || !old.secretHash().equals(hash)) return false;
            revoked.add(key); return true;
        }
        @Override public void invalidate(CloudSessionIdentity identity, String jti, Instant now) { revoked.add(new Key(identity, jti)); }
    }
}
