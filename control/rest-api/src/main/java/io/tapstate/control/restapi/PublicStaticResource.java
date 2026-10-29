package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.io.ClassPathResource;

import java.util.regex.Pattern;

/** Allows reads of packaged root-level image resources without exposing application endpoints. */
final class PublicStaticResource {
    private static final Pattern ROOT_IMAGE =
            Pattern.compile("/[A-Za-z0-9._-]+\\.(?i:svg|png|ico|jpg|jpeg|webp|gif|avif|bmp)");

    private PublicStaticResource() { }

    static boolean matches(HttpServletRequest request) {
        String method = request.getMethod();
        if (!("GET".equals(method) || "HEAD".equals(method))) {
            return false;
        }
        String path = request.getServletPath();
        return path != null && ROOT_IMAGE.matcher(path).matches()
                && new ClassPathResource("static" + path).isReadable();
    }
}
