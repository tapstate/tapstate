package io.tapstate.app;

import io.tapstate.control.core.CloudAuthenticationObserver;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Correlates the initial synchronous Cloud request without reading credentials or changing responses.
 * Async and error redispatches retain the standard once-per-request filter behavior.
 */
final class CloudHttpDiagnosticsFilter extends OncePerRequestFilter {

    static final String REQUEST_ID_MDC = CloudAuthenticationObserver.REQUEST_ID_CONTEXT_KEY;
    static final String STAGE_MDC = "cloud_auth_stage";
    static final String REASON_MDC = "cloud_auth_reason";
    static final String ERROR_CODE_MDC = CloudAuthenticationObserver.ERROR_CODE_CONTEXT_KEY;

    private static final Logger LOG = LoggerFactory.getLogger(CloudHttpDiagnosticsFilter.class);
    private static final String SESSION_COOKIE_NAME = "__Host-tapstate-cloud-session";
    private static final int PATH_LIMIT = 256;
    private static final int FIELD_LIMIT = 96;

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path == null || !(path.startsWith("/auth/") || path.startsWith("/api/"));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        String requestId = UUID.randomUUID().toString();
        String method = safeField(request.getMethod(), 16);
        String path = safeField(request.getRequestURI(), PATH_LIMIT);
        boolean exchange = "/auth/exchange".equals(request.getRequestURI());
        boolean cookiePresent = cloudSessionCookiePresent(request);
        long started = System.nanoTime();
        Throwable thrown = null;

        try {
            MDC.remove(REQUEST_ID_MDC);
            MDC.remove(STAGE_MDC);
            MDC.remove(REASON_MDC);
            MDC.remove(ERROR_CODE_MDC);
            MDC.put(REQUEST_ID_MDC, requestId);
            request.setAttribute(REQUEST_ID_MDC, requestId);
            MDC.put(STAGE_MDC, request.getRequestURI().startsWith("/api/")
                    ? "session-authentication" : "auth-route");
            MDC.put(REASON_MDC, "http-dispatch");
            response.setHeader("X-Request-ID", requestId);
            if (exchange) {
                LOG.info("Cloud HTTP request started [request_id={}, method={}, path={}, stage={}, reason={}]",
                        requestId, method, path, diagnostic(STAGE_MDC), diagnostic(REASON_MDC));
            }
            chain.doFilter(request, response);
        } catch (ServletException | IOException | RuntimeException | Error failure) {
            thrown = failure;
            throw failure;
        } finally {
            try {
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
                Object coded = request.getAttribute(ERROR_CODE_MDC);
                if (coded instanceof String errorCode) MDC.put(ERROR_CODE_MDC, errorCode);
                int responseStatus = response.getStatus();
                if (thrown != null || responseStatus >= 400) {
                    int failureStatus = thrown != null && responseStatus < 400 ? 500 : responseStatus;
                    LOG.warn("Cloud HTTP request failed [request_id={}, method={}, path={}, status={}, "
                                    + "response_status={}, elapsed_ms={}, stage={}, reason={}, error_code={}, "
                                    + "cloudSessionCookiePresent={}, unhandled={}, exception_type={}]",
                            requestId, method, path, failureStatus, responseStatus, elapsedMs,
                            diagnostic(STAGE_MDC), diagnostic(REASON_MDC), diagnostic(ERROR_CODE_MDC),
                            cookiePresent, thrown != null,
                            thrown == null ? "none" : safeField(thrown.getClass().getSimpleName(), FIELD_LIMIT));
                } else if (exchange) {
                    LOG.info("Cloud HTTP request finished [request_id={}, method={}, path={}, status={}, "
                                    + "elapsed_ms={}, stage={}, reason={}, error_code={}]",
                            requestId, method, path, responseStatus, elapsedMs,
                            diagnostic(STAGE_MDC), diagnostic(REASON_MDC), diagnostic(ERROR_CODE_MDC));
                }
            } finally {
                if (previous == null) {
                    MDC.clear();
                } else {
                    MDC.setContextMap(previous);
                }
            }
        }
    }

    private static String diagnostic(String key) {
        return safeField(MDC.get(key), FIELD_LIMIT);
    }

    private static boolean cloudSessionCookiePresent(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (SESSION_COOKIE_NAME.equals(cookie.getName())) return true;
            }
        }
        return false;
    }

    private static String safeField(String value, int limit) {
        if (value == null || value.isEmpty()) return "none";
        StringBuilder result = new StringBuilder(Math.min(value.length(), limit));
        int length = Math.min(value.length(), limit);
        for (int index = 0; index < length; index++) {
            char character = value.charAt(index);
            result.append(Character.isISOControl(character)
                    || Character.getType(character) == Character.FORMAT
                    || character == '\u2028' || character == '\u2029' ? '?' : character);
        }
        return result.toString();
    }
}
