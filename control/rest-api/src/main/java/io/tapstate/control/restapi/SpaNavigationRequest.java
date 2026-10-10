package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.regex.Pattern;

/** Identifies browser document navigations without granting access to server-owned paths. */
public final class SpaNavigationRequest {

    public static final String CLIENT_ROOT_PATTERN =
            "(?!(?:api|auth|assets|connector-icons|healthz|version|error)$)[A-Za-z0-9_-]+";

    private static final Pattern CLIENT_ROOT = Pattern.compile(CLIENT_ROOT_PATTERN);

    private SpaNavigationRequest() {
    }

    public static boolean matches(HttpServletRequest request) {
        if (!("GET".equals(request.getMethod()) || "HEAD".equals(request.getMethod()))) {
            return false;
        }
        String path = request.getServletPath();
        if (path == null || !path.startsWith("/")) {
            return false;
        }
        if (path.length() > 1) {
            String first = path.substring(1).split("/", 2)[0];
            if (!CLIENT_ROOT.matcher(first).matches()) {
                return false;
            }
        }
        String accept = request.getHeader(HttpHeaders.ACCEPT);
        if (accept == null) {
            return false;
        }
        try {
            return MediaType.parseMediaTypes(accept).stream()
                    .anyMatch(type -> type.getQualityValue() > 0
                            && "text".equalsIgnoreCase(type.getType())
                            && "html".equalsIgnoreCase(type.getSubtype()));
        } catch (IllegalArgumentException invalidAccept) {
            return false;
        }
    }
}
