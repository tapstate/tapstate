package io.tapstate.control.restapi;

import java.net.URI;

/** Compares an Origin header to the effective request authority. */
final class SameOrigin {
    private SameOrigin() { }

    static boolean matches(String value, String scheme, String host, int port, boolean allowRootPath) {
        try {
            URI origin = URI.create(value);
            String path = allowRootPath ? origin.getPath() : origin.getRawPath();
            int originPort = port(origin);
            return scheme != null && host != null && origin.getUserInfo() == null && origin.getHost() != null && origin.getRawQuery() == null
                    && origin.getRawFragment() == null && (path.isEmpty() || (allowRootPath && "/".equals(path)))
                    && scheme.equalsIgnoreCase(origin.getScheme()) && host.equalsIgnoreCase(origin.getHost())
                    && port == originPort;
        } catch (IllegalArgumentException malformed) {
            return false;
        }
    }

    private static int port(URI origin) {
        if (origin.getPort() >= 0) return origin.getPort();
        return "https".equalsIgnoreCase(origin.getScheme()) ? 443 : 80;
    }
}
