package io.tapstate.app;

import static org.assertj.core.api.Assertions.*;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.core.common.JsonReader;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import java.util.Map;
import org.junit.jupiter.api.*;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class CloudHttpPerformanceDiagnosticsTest {
    private final CloudHttpDiagnosticsFilter filter = new CloudHttpDiagnosticsFilter();
    private final Logger logger = (Logger) LoggerFactory.getLogger("io.tapstate.diagnostics.http");
    private final ListAppender<ILoggingEvent> events = new ListAppender<>();
    private Level previous;
    @BeforeEach void capture() { previous = logger.getLevel(); events.start(); logger.addAppender(events); }
    @AfterEach void restore() { logger.detachAppender(events); events.stop(); logger.setLevel(previous); }

    @Test void disabledByLoggerLevelKeepsSuccessfulApiQuiet() throws Exception {
        logger.setLevel(Level.INFO);
        filter.doFilter(new MockHttpServletRequest("GET", "/api/sources"), new MockHttpServletResponse(), (req, res) -> { });
        assertThat(events.list).isEmpty();
    }

    @Test void enabledSuccessfulApiHasOneCorrelatedSummaryAndNoSensitiveInputs() throws Exception {
        logger.setLevel(Level.DEBUG);
        var request = new MockHttpServletRequest("GET", "/api/alerts/center");
        request.setQueryString("token=query-secret"); request.addHeader("Authorization", "Bearer header-secret");
        request.setCookies(new Cookie("__Host-tapstate-cloud-session", "cookie-secret"));
        request.setContent("body-secret".getBytes());
        var response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> {
            try (var stage = MongoHttpDiagnosticsScope.phase(MongoHttpDiagnosticsScope.Phase.AUTH_VERIFY)) { }
            response.getWriter().write("unchanged-response");
        });
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getContentAsString()).isEqualTo("unchanged-response");
        assertThat(events.list).singleElement().satisfies(event -> {
            String message = event.getFormattedMessage();
            assertThat(message).contains("http_diag", response.getHeader("X-Request-ID"), "/api/alerts/center", "auth_verify")
                    .doesNotContain("query-secret", "header-secret", "cookie-secret", "body-secret", "unchanged-response");
            @SuppressWarnings("unchecked") var value = (Map<String, Object>) JsonReader.parse(message);
            assertThat(((Number) value.get("status")).intValue()).isEqualTo(200);
            assertThat(value).containsEntry("complete", true)
                    .containsEntry("timing_semantics", "phases_are_inclusive_not_additive;mongo_is_driver_elapsed_not_server_cpu");
        });
    }

    @Test void sdkBoundaryTimingContainsNoProviderPayload() throws Exception {
        logger.setLevel(Level.DEBUG);
        var settings = new CloudProperties(); settings.setBaseUrl("https://cloud.example");
        settings.setToken("provider-token-sentinel"); settings.setClusterId("cluster");
        settings.setAtlasUri("mongodb://localhost/config");
        var sdk = org.mockito.Mockito.mock(CloudSdkBridge.SdkRuntime.class);
        org.mockito.Mockito.when(sdk.currentEgressAddress()).thenReturn(
                new io.tapstate.cloud.sdk.client.ClusterEgressAddress("org", "cluster", "provider-ip-sentinel", "PROVISIONING"));
        var bridge = new CloudSdkBridge(CloudRuntimeSettings.resolve(settings), sdk);
        filter.doFilter(new MockHttpServletRequest("GET", "/api/alerts/center"), new MockHttpServletResponse(),
                (req, res) -> bridge.currentAddress("org", "cluster"));
        assertThat(events.list).singleElement().satisfies(event -> assertThat(event.getFormattedMessage())
                .contains("sdk_egress", "http_diag").doesNotContain("provider-token-sentinel", "provider-ip-sentinel"));
    }

    @Test void exceptionsRemainUnchangedAndHaveAnEffectiveFailureStatus() {
        logger.setLevel(Level.DEBUG);
        var failure = new ServletException("exception-secret");
        var response = new MockHttpServletResponse();
        assertThatThrownBy(() -> filter.doFilter(new MockHttpServletRequest("GET", "/api/views"), response,
                (req, res) -> { throw failure; })).isSameAs(failure);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(events.list).singleElement().satisfies(event -> {
            String message = event.getFormattedMessage();
            assertThat(message).contains("\"status\":500", "\"response_status\":200", "\"unhandled\":true")
                    .doesNotContain("exception-secret");
        });
    }
}
