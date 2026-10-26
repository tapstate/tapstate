package io.tapstate.control.restapi;

import io.tapstate.control.core.AccessTokenGrant;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.LoginService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SessionService;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Same-origin browser adapters over the existing revocable on-prem session authority. */
@Controller
final class BrowserAuthController {
    private static final String HTTPS_COOKIE = "__Host-tapstate-op-session";
    private static final String LOOPBACK_COOKIE = "tapstate-op-session";
    private final LoginService login;
    private final SessionService sessions;
    private final ClusterIdentityService identity;
    private final AuthenticationMode mode;

    BrowserAuthController(LoginService login, SessionService sessions, ClusterIdentityService identity,
            ObjectProvider<AuthenticationMode> modes) {
        this.login = login;
        this.sessions = sessions;
        this.identity = identity;
        this.mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
    }

    @PostMapping(value = "/auth/browser/login", consumes = "application/json")
    ResponseEntity<BrowserGrant> login(HttpServletRequest request, @RequestBody LoginRequest credentials) {
        guard(request);
        MalformedRequest.requireText(credentials.username(), "a `username` is required");
        MalformedRequest.requireText(credentials.password(), "a `password` is required");
        AccessTokenGrant grant = login.loginGrant(credentials.username(), credentials.password());
        String issuer = identity.identityView().issuer();
        var session = sessions.create(grant.principal(), grant.scope(), issuer);
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-store")
                .header(HttpHeaders.SET_COOKIE, cookie(request, session.token(), false))
                .body(grant(grant, issuer, session.token()));
    }

    @PostMapping("/auth/browser/session")
    ResponseEntity<BrowserGrant> session(HttpServletRequest request) {
        guard(request);
        String token = readCookie(request);
        String issuer = identity.identityView().issuer();
        AccessTokenGrant grant = sessions.exchange(token, issuer).orElseThrow(BrowserAuthController::refused);
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(grant(grant, issuer, token));
    }

    @PostMapping("/auth/browser/logout")
    ResponseEntity<Void> logout(HttpServletRequest request) {
        guard(request);
        String token = readCookie(request);
        // A syntactically valid, already expired/revoked session is also safe to forget locally.
        sessions.logout(token, identity.identityView().issuer());
        return ResponseEntity.noContent().header(HttpHeaders.CACHE_CONTROL, "no-store")
                .header(HttpHeaders.SET_COOKIE, cookie(request, "", true)).build();
    }

    private void guard(HttpServletRequest request) {
        if (mode != AuthenticationMode.ON_PREM) {
            throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "cloud"), null);
        }
        if (request.getHeaders(HttpHeaders.AUTHORIZATION).hasMoreElements()
                || !Collections.list(request.getHeaders("X-Tapstate-Browser-Request")).equals(List.of("1"))) {
            throw refused();
        }
        boolean secure = request.isSecure();
        if (!secure && !isLoopback(request.getServerName())) throw refused();
        List<String> origins = Collections.list(request.getHeaders(HttpHeaders.ORIGIN));
        if (origins.size() != 1) throw refused();
        try {
            URI origin = URI.create(origins.getFirst());
            int port = origin.getPort() < 0 ? ("https".equals(origin.getScheme()) ? 443 : 80) : origin.getPort();
            if (origin.getUserInfo() != null || origin.getHost() == null || origin.getRawQuery() != null
                    || origin.getRawFragment() != null || !origin.getRawPath().isEmpty()
                    || !request.getScheme().equals(origin.getScheme())
                    || !request.getServerName().equalsIgnoreCase(origin.getHost())
                    || request.getServerPort() != port) throw refused();
        } catch (IllegalArgumentException malformed) {
            throw refused();
        }
    }

    private static boolean isLoopback(String host) {
        return "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host)
                || "[::1]".equals(host) || "::1".equals(host);
    }

    private static String readCookie(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        String found = null;
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (HTTPS_COOKIE.equals(cookie.getName()) || LOOPBACK_COOKIE.equals(cookie.getName())) {
                    if (found != null || !cookieName(request).equals(cookie.getName())) throw refused();
                    found = cookie.getValue();
                }
            }
        }
        if (found == null || !found.matches("tss_[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+")) throw refused();
        return found;
    }

    private static String cookieName(HttpServletRequest request) {
        return request.isSecure() ? HTTPS_COOKIE : LOOPBACK_COOKIE;
    }

    private static String cookie(HttpServletRequest request, String token, boolean clear) {
        ResponseCookie.ResponseCookieBuilder builder = ResponseCookie.from(cookieName(request), token)
                .httpOnly(true).secure(request.isSecure()).sameSite("Lax").path("/");
        if (clear) builder.maxAge(0);
        return builder.build().toString();
    }

    private static BrowserGrant grant(AccessTokenGrant grant, String issuer, String session) {
        return new BrowserGrant(grant.token(), grant.expiresAt().toString(), issuer, grant.principal(),
                Arrays.stream(Scope.values()).filter(grant.scope()::permits)
                        .map(scope -> scope.name().toLowerCase(java.util.Locale.ROOT)).toList(),
                session.substring(4, session.indexOf('.')));
    }

    private static TapstateException refused() {
        return new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
    }

    record BrowserGrant(String token, String accessExpiresAt, String issuer, String principal,
            List<String> scopes, String sessionId) { }
}
