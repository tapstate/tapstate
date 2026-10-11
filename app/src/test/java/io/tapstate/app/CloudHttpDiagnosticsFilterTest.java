package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.BufferedReader;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudHttpDiagnosticsFilterTest {

    private final CloudHttpDiagnosticsFilter filter = new CloudHttpDiagnosticsFilter();
    private final ListAppender<ILoggingEvent> written = new ListAppender<>() {
        @Override
        protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing();
            super.append(event);
        }
    };
    private Logger logger;
    private Level previousLevel;
    private Map<String, String> previousMdc;

    @BeforeEach
    void captureDiagnostics() {
        previousMdc = MDC.getCopyOfContextMap();
        MDC.clear();
        logger = (Logger) LoggerFactory.getLogger(CloudHttpDiagnosticsFilter.class);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        written.start();
        logger.addAppender(written);
    }

    @AfterEach
    void restoreLogging() {
        logger.detachAppender(written);
        logger.setLevel(previousLevel);
        written.stop();
        if (previousMdc == null) MDC.clear();
        else MDC.setContextMap(previousMdc);
    }

    @Test
    void apiUnauthorizedLogsPathAndServerIdButNotRequestOrResponseCredentials() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/sources");
        request.setQueryString("token=query-token-sentinel&code=exchange-code-sentinel");
        request.addHeader("Authorization", "Bearer jwt-header-sentinel");
        request.addHeader("X-Request-ID", "caller-chosen-id-sentinel");
        request.setCookies(new Cookie("__Host-tapstate-cloud-session", "session-cookie-sentinel"),
                new Cookie("other-cookie-sentinel", "other-cookie-value-sentinel"));
        request.setContent("{\"password\":\"body-password-sentinel\"}".getBytes());
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            assertThat(suppliedRequest).isSameAs(request);
            assertThat(suppliedResponse).isSameAs(response);
            response.setStatus(401);
            response.setHeader("Content-Type", "application/json");
            response.getWriter().write("{\"code\":\"control.unauthenticated\",\"jwt\":\"response-jwt-sentinel\"}");
        });

        String requestId = response.getHeader("X-Request-ID");
        assertThat(UUID.fromString(requestId).toString()).isEqualTo(requestId);
        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getContentType()).isEqualTo("application/json");
        assertThat(response.getContentAsString())
                .isEqualTo("{\"code\":\"control.unauthenticated\",\"jwt\":\"response-jwt-sentinel\"}");
        assertThat(written.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains(
                    "request_id=" + requestId, "method=GET", "path=/api/sources", "status=401",
                    "elapsed_ms=", "stage=session-authentication", "reason=http-dispatch",
                    "cloudSessionCookiePresent=true");
            assertThat(event.getFormattedMessage()).doesNotContain(
                    "query-token-sentinel", "exchange-code-sentinel", "jwt-header-sentinel",
                    "caller-chosen-id-sentinel", "session-cookie-sentinel", "other-cookie-sentinel",
                    "other-cookie-value-sentinel", "body-password-sentinel", "response-jwt-sentinel");
            assertThat(event.getMDCPropertyMap()).containsEntry(CloudHttpDiagnosticsFilter.REQUEST_ID_MDC, requestId);
            assertThat(event.getThrowableProxy()).isNull();
        });
        assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
    }

    @Test
    void successfulExchangeLogsStartAndCompletedSessionStageWithoutChangingTheRedirect() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/auth/exchange");
        request.setQueryString("code=successful-code-sentinel");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            MDC.put(CloudHttpDiagnosticsFilter.STAGE_MDC, "session-created");
            MDC.put(CloudHttpDiagnosticsFilter.REASON_MDC, "authenticated");
            response.setHeader("Set-Cookie", "__Host-tapstate-cloud-session=new-cookie-sentinel; Secure; HttpOnly; Path=/");
            response.sendRedirect("/");
        });

        String requestId = response.getHeader("X-Request-ID");
        assertThat(response.getStatus()).isEqualTo(302);
        assertThat(response.getRedirectedUrl()).isEqualTo("/");
        assertThat(response.getHeader("Set-Cookie")).contains("new-cookie-sentinel");
        assertThat(written.list).hasSize(2).allSatisfy(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.INFO);
            assertThat(event.getFormattedMessage()).contains("request_id=" + requestId, "path=/auth/exchange")
                    .doesNotContain("successful-code-sentinel", "new-cookie-sentinel");
        });
        assertThat(written.list.getFirst().getFormattedMessage())
                .contains("request started", "stage=auth-route", "reason=http-dispatch");
        assertThat(written.list.getLast().getFormattedMessage())
                .contains("request finished", "status=302", "stage=session-created", "reason=authenticated");
        assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
    }

    @Test
    void exchangeFailureUsesTheFinalAuthenticationClassification() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/auth/exchange");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            MDC.put(CloudHttpDiagnosticsFilter.STAGE_MDC, "jwt-verification");
            MDC.put(CloudHttpDiagnosticsFilter.REASON_MDC, "bad-issuer");
            MDC.put(CloudHttpDiagnosticsFilter.ERROR_CODE_MDC, "control.unauthenticated");
            response.setStatus(401);
        });

        assertThat(written.list).hasSize(2);
        assertThat(written.list.getLast().getLevel()).isEqualTo(Level.WARN);
        assertThat(written.list.getLast().getFormattedMessage()).contains(
                "status=401", "stage=jwt-verification", "reason=bad-issuer",
                "error_code=control.unauthenticated", "cloudSessionCookiePresent=false");
        assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
    }

    @Test
    void cloudDiagnosticSlotsAreIsolatedWhileOtherAttributionIsPreservedAndRestored() throws Exception {
        Map<String, String> outer = Map.of(
                CloudHttpDiagnosticsFilter.REQUEST_ID_MDC, "outer-request",
                CloudHttpDiagnosticsFilter.STAGE_MDC, "outer-stage",
                CloudHttpDiagnosticsFilter.REASON_MDC, "outer-reason",
                CloudHttpDiagnosticsFilter.ERROR_CODE_MDC, "outer-error",
                "pipeline_id", "outer-pipeline");
        MDC.setContextMap(outer);
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/pipelines");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            assertThat(MDC.get(CloudHttpDiagnosticsFilter.REQUEST_ID_MDC)).isNotEqualTo("outer-request");
            assertThat(MDC.get(CloudHttpDiagnosticsFilter.STAGE_MDC)).isEqualTo("session-authentication");
            assertThat(MDC.get(CloudHttpDiagnosticsFilter.REASON_MDC)).isEqualTo("http-dispatch");
            assertThat(MDC.get(CloudHttpDiagnosticsFilter.ERROR_CODE_MDC)).isNull();
            assertThat(MDC.get("pipeline_id")).isEqualTo("outer-pipeline");
            MDC.put("inner-only", "not-for-the-next-request");
            response.setStatus(204);
        });

        assertThat(MDC.getCopyOfContextMap()).isEqualTo(outer);
        assertThat(response.getStatus()).isEqualTo(204);
        assertThat(written.list).isEmpty();
    }

    @Test
    void anUnexpectedExceptionKeepsItsIdentityAndRestoresMdcWithoutPretendingSuccess() {
        Map<String, String> outer = Map.of("component", "outer-component");
        MDC.setContextMap(outer);
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/sources");
        MockHttpServletResponse response = new MockHttpServletResponse();
        ServletException failure = new ServletException("exception-message-secret-sentinel");

        assertThatThrownBy(() -> filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            MDC.put(CloudHttpDiagnosticsFilter.STAGE_MDC, "http-dispatch");
            throw failure;
        })).isSameAs(failure);

        assertThat(MDC.getCopyOfContextMap()).isEqualTo(outer);
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getContentAsByteArray()).isEmpty();
        assertThat(response.isCommitted()).isFalse();
        assertThat(written.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains(
                    "status=500", "response_status=200", "unhandled=true", "exception_type=ServletException")
                    .doesNotContain("exception-message-secret-sentinel", "outer-component");
            assertThat(event.getThrowableProxy()).isNull();
        });
    }

    @Test
    void unsupportedMethodsAreDiagnosedWithoutReadingBodiesOrBufferingTheResponse() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("PUT", "/auth/exchange") {
            @Override
            public ServletInputStream getInputStream() {
                throw new AssertionError("The diagnostic filter must not read the request body.");
            }

            @Override
            public BufferedReader getReader() {
                throw new AssertionError("The diagnostic filter must not read the request body.");
            }
        };
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            assertThat(suppliedRequest).isSameAs(request);
            assertThat(suppliedResponse).isSameAs(response);
            response.setStatus(405);
            response.setHeader("Allow", "GET");
            response.getWriter().write("unchanged-method-not-allowed");
        });

        assertThat(response.getStatus()).isEqualTo(405);
        assertThat(response.getHeader("Allow")).isEqualTo("GET");
        assertThat(response.getContentAsString()).isEqualTo("unchanged-method-not-allowed");
        assertThat(written.list.getLast().getLevel()).isEqualTo(Level.WARN);
        assertThat(written.list.getLast().getFormattedMessage()).contains(
                "method=PUT", "path=/auth/exchange", "status=405");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/wp-login.php", "/wordpress/wp-admin", "/.env", "/favicon.ico", "/api", "/auth"})
    void unrelatedScannerAndStaticPathsBypassDiagnostics(String path) throws Exception {
        MDC.put("component", "unchanged-outer");
        MockHttpServletRequest request = new MockHttpServletRequest("GET", path);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicBoolean reached = new AtomicBoolean();

        filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            reached.set(true);
            response.setStatus(404);
        });

        assertThat(reached).isTrue();
        assertThat(response.getStatus()).isEqualTo(404);
        assertThat(response.getHeader("X-Request-ID")).isNull();
        assertThat(MDC.getCopyOfContextMap()).containsExactlyEntriesOf(Map.of("component", "unchanged-outer"));
        assertThat(written.list).isEmpty();
    }

    @Test
    void pathAndClassificationFieldsAreBoundedAndCannotInjectLogLines() throws Exception {
        String uri = "/api/\n\r\t\u2028\u2029\u202e" + "x".repeat(600);
        MockHttpServletRequest request = new MockHttpServletRequest("GET", uri);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, (suppliedRequest, suppliedResponse) -> {
            MDC.put(CloudHttpDiagnosticsFilter.REASON_MDC, "failure\nforged-line" + "r".repeat(300));
            response.setStatus(401);
        });

        assertThat(written.list).singleElement().satisfies(event -> {
            String message = event.getFormattedMessage();
            assertThat(message).contains("path=/api/??????", "reason=failure?forged-line")
                    .doesNotContain("\n", "\r", "\t", "\u2028", "\u2029", "\u202e", "x".repeat(257), "r".repeat(97));
            String pathField = message.substring(message.indexOf("path=") + 5, message.indexOf(", status="));
            assertThat(pathField).hasSize(256);
        });
    }
}
