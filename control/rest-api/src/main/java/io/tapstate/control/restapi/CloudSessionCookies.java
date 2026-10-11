package io.tapstate.control.restapi;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseCookie;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;

/** Host-only, server-managed Cloud cookies and same-origin protection for cookie-authenticated writes. */
final class CloudSessionCookies {

    static final String NAME = "__Host-tapstate-cloud-session";

    private CloudSessionCookies() { }

    static String set(String token) {
        // A browser-session cookie: the server, not a fixed client Max-Age, owns the sliding idle window.
        return ResponseCookie.from(NAME, token).httpOnly(true).secure(true).sameSite("Lax").path("/")
                .build().toString();
    }

    static String clear() {
        return ResponseCookie.from(NAME, "").httpOnly(true).secure(true).sameSite("Lax").path("/")
                .maxAge(0).build().toString();
    }

    static Optional<String> read(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null) {
            return Optional.empty();
        }
        String token = null;
        for (Cookie cookie : cookies) {
            if (NAME.equals(cookie.getName())) {
                if (token != null || cookie.getValue() == null || cookie.getValue().isBlank()) {
                    return Optional.empty();
                }
                token = cookie.getValue();
            }
        }
        return Optional.ofNullable(token);
    }

    static boolean permitsCookieRequest(HttpServletRequest request) {
        if ("GET".equals(request.getMethod()) || "HEAD".equals(request.getMethod())
                || "OPTIONS".equals(request.getMethod())) {
            return true;
        }
        var origins = Collections.list(request.getHeaders("Origin"));
        if (origins.size() > 1) {
            return false;
        }
        if (origins.size() == 1) {
            try {
                URI origin = URI.create(origins.getFirst());
                int originPort = origin.getPort() < 0
                        ? ("https".equalsIgnoreCase(origin.getScheme()) ? 443 : 80) : origin.getPort();
                return origin.getUserInfo() == null && origin.getHost() != null && origin.getQuery() == null
                        && origin.getFragment() == null && (origin.getPath().isEmpty() || "/".equals(origin.getPath()))
                        && request.getScheme().equalsIgnoreCase(origin.getScheme())
                        && request.getServerName().equalsIgnoreCase(origin.getHost())
                        && request.getServerPort() == originPort;
            } catch (IllegalArgumentException malformed) {
                return false;
            }
        }
        String site = request.getHeader("Sec-Fetch-Site");
        return site == null || "same-origin".equalsIgnoreCase(site) || "none".equalsIgnoreCase(site);
    }
}
