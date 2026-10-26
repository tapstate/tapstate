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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Same-origin browser adapters over the existing revocable on-prem session authority. */
@Controller
final class BrowserAuthController {
    private static final String HTTPS_COOKIE = "__Host-tapstate-op-session";
    private static final String LOOPBACK_COOKIE = "tapstate-op-session";
    private final LoginService login;
    private final SessionService sessions;
    private final ClusterIdentityService identity;
    private final AuthenticationMode mode;
    private Set<String> trustedProxyAddresses = Set.of();

    BrowserAuthController(LoginService login, SessionService sessions, ClusterIdentityService identity,
            ObjectProvider<AuthenticationMode> modes) {
        this.login = login;
        this.sessions = sessions;
        this.identity = identity;
        this.mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
    }

    @Value("${tapstate.browser-auth.trusted-proxies:}")
    void trustedProxyAddresses(String addresses) {
        trustedProxyAddresses = Arrays.stream(addresses.split(","))
                .map(String::trim).filter(address -> !address.isEmpty())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    @PostMapping(value = "/auth/browser/login", consumes = "application/json")
    ResponseEntity<BrowserGrant> login(HttpServletRequest request, @RequestBody LoginRequest credentials) {
        BrowserRequest browser = guard(request);
        MalformedRequest.requireText(credentials.username(), "a `username` is required");
        MalformedRequest.requireText(credentials.password(), "a `password` is required");
        AccessTokenGrant grant = login.loginGrant(credentials.username(), credentials.password());
        String issuer = identity.identityView().issuer();
        var session = sessions.create(grant.principal(), grant.scope(), issuer);
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-store")
                .header(HttpHeaders.SET_COOKIE, loginCookies(browser.secure(), session.token()))
                .body(grant(grant, issuer, session.token()));
    }

    @PostMapping("/auth/browser/session")
    ResponseEntity<BrowserGrant> session(HttpServletRequest request) {
        BrowserRequest browser = guard(request);
        String token = readCookie(request, browser.secure());
        String issuer = identity.identityView().issuer();
        AccessTokenGrant grant = sessions.exchange(token, issuer).orElseThrow(BrowserAuthController::refused);
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(grant(grant, issuer, token));
    }

    @PostMapping("/auth/browser/logout")
    ResponseEntity<Void> logout(HttpServletRequest request) {
        BrowserRequest browser = guard(request);
        String token = readCookie(request, browser.secure());
        SessionService.LogoutResult result = sessions.logoutForBrowser(token, identity.identityView().issuer());
        if (result == SessionService.LogoutResult.INVALID) throw refused();
        return ResponseEntity.noContent().header(HttpHeaders.CACHE_CONTROL, "no-store")
                .header(HttpHeaders.SET_COOKIE, cookie(browser.secure(), "", true)).build();
    }

    private BrowserRequest guard(HttpServletRequest request) {
        if (mode != AuthenticationMode.ON_PREM) {
            throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "cloud"), null);
        }
        if (request.getHeaders(HttpHeaders.AUTHORIZATION).hasMoreElements()
                || !Collections.list(request.getHeaders("X-Tapstate-Browser-Request")).equals(List.of("1"))) {
            throw refused();
        }
        BrowserRequest browser = browserRequest(request);
        if (!browser.secure() && !(isLoopback(browser.host()) && isLoopbackAddress(request.getRemoteAddr()))) {
            throw refused();
        }
        List<String> origins = Collections.list(request.getHeaders(HttpHeaders.ORIGIN));
        if (origins.size() != 1) throw refused();
        try {
            URI origin = URI.create(origins.getFirst());
            int port = origin.getPort() < 0 ? ("https".equalsIgnoreCase(origin.getScheme()) ? 443 : 80) : origin.getPort();
            if (origin.getUserInfo() != null || origin.getHost() == null || origin.getRawQuery() != null
                    || origin.getRawFragment() != null || !origin.getRawPath().isEmpty()
                    || !browser.scheme().equalsIgnoreCase(origin.getScheme())
                    || !browser.host().equalsIgnoreCase(origin.getHost())
                    || browser.port() != port) throw refused();
        } catch (IllegalArgumentException malformed) {
            throw refused();
        }
        return browser;
    }

    private static boolean isLoopback(String host) {
        return "localhost".equalsIgnoreCase(host) || isLoopbackAddress(host);
    }

    private BrowserRequest browserRequest(HttpServletRequest request) {
        String scheme = request.getScheme();
        String host = request.getServerName();
        int port = request.getServerPort();
        boolean secure = request.isSecure() && "https".equalsIgnoreCase(scheme);
        if (trustedProxyAddresses.contains(request.getRemoteAddr())) {
            String forwardedProto = singleForwardedHeader(request, "X-Forwarded-Proto");
            String forwardedHost = singleForwardedHeader(request, "X-Forwarded-Host");
            String forwardedPort = singleForwardedHeader(request, "X-Forwarded-Port");
            int hostPort = -1;
            if (forwardedProto != null) {
                if (!"https".equalsIgnoreCase(forwardedProto) && !"http".equalsIgnoreCase(forwardedProto)) {
                    throw refused();
                }
                scheme = forwardedProto.toLowerCase(Locale.ROOT);
                secure = "https".equals(scheme);
            }
            if (forwardedHost != null) {
                HostAndPort authority = parseAuthority(scheme, forwardedHost);
                host = authority.host();
                hostPort = authority.port();
                if (hostPort > 0) port = hostPort;
            }
            if (forwardedPort != null) {
                int parsedPort = parsePort(forwardedPort);
                if (hostPort > 0 && hostPort != parsedPort) throw refused();
                port = parsedPort;
            } else if (forwardedProto != null && hostPort < 0) {
                port = defaultPort(scheme);
            }
        }
        return new BrowserRequest(scheme, host, port, secure);
    }

    private static String singleForwardedHeader(HttpServletRequest request, String name) {
        List<String> values = Collections.list(request.getHeaders(name));
        if (values.isEmpty()) return null;
        if (values.size() != 1 || values.getFirst().isBlank() || values.getFirst().contains(",")) throw refused();
        return values.getFirst().trim();
    }

    private static HostAndPort parseAuthority(String scheme, String authority) {
        try {
            URI parsed = URI.create(scheme + "://" + authority);
            if (parsed.getHost() == null || parsed.getRawUserInfo() != null || !parsed.getRawPath().isEmpty()
                    || parsed.getRawQuery() != null || parsed.getRawFragment() != null) throw refused();
            return new HostAndPort(parsed.getHost(), parsed.getPort());
        } catch (IllegalArgumentException malformed) {
            throw refused();
        }
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) throw refused();
            return port;
        } catch (NumberFormatException malformed) {
            throw refused();
        }
    }

    private static int defaultPort(String scheme) {
        return "https".equalsIgnoreCase(scheme) ? 443 : 80;
    }

    private static boolean isLoopbackAddress(String value) {
        if (value == null) return false;
        String address = value;
        if (address.startsWith("[") && address.endsWith("]")) address = address.substring(1, address.length() - 1);
        try {
            if (!address.contains(":")) {
                String[] parts = address.split("\\.", -1);
                if (parts.length != 4) return false;
                byte[] bytes = new byte[4];
                for (int i = 0; i < parts.length; i++) {
                    if (parts[i].isEmpty() || !parts[i].matches("[0-9]{1,3}")) return false;
                    int octet = Integer.parseInt(parts[i]);
                    if (octet > 255) return false;
                    bytes[i] = (byte) octet;
                }
                return InetAddress.getByAddress(bytes).isLoopbackAddress();
            }
            if (!address.matches("[0-9A-Fa-f:.]+")) return false;
            return InetAddress.getByName(address).isLoopbackAddress();
        } catch (UnknownHostException | NumberFormatException malformed) {
            return false;
        }
    }

    private static String readCookie(HttpServletRequest request, boolean secure) {
        Cookie[] cookies = request.getCookies();
        String found = null;
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (HTTPS_COOKIE.equals(cookie.getName()) || LOOPBACK_COOKIE.equals(cookie.getName())) {
                    if (found != null || !cookieName(secure).equals(cookie.getName())) throw refused();
                    found = cookie.getValue();
                }
            }
        }
        if (found == null || !found.matches("tss_[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+")) throw refused();
        return found;
    }

    private static String[] loginCookies(boolean secure, String token) {
        String active = cookie(secure, token, false);
        if (!secure) return new String[] { active };
        String retired = ResponseCookie.from(LOOPBACK_COOKIE, "").httpOnly(true).secure(true)
                .sameSite("Lax").path("/").maxAge(0).build().toString();
        return new String[] { active, retired };
    }

    private static String cookieName(boolean secure) {
        return secure ? HTTPS_COOKIE : LOOPBACK_COOKIE;
    }

    private static String cookie(boolean secure, String token, boolean clear) {
        ResponseCookie.ResponseCookieBuilder builder = ResponseCookie.from(cookieName(secure), token)
                .httpOnly(true).secure(secure).sameSite("Lax").path("/");
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

    private record BrowserRequest(String scheme, String host, int port, boolean secure) { }

    private record HostAndPort(String host, int port) { }

    record BrowserGrant(String token, String accessExpiresAt, String issuer, String principal,
            List<String> scopes, String sessionId) { }
}
