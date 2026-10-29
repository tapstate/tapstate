package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.io.ClassPathResource;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/** Allows reads of packaged root-level image resources without exposing application endpoints. */
final class PublicStaticResource {
    private static final Pattern ROOT_IMAGE =
            Pattern.compile("/[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)*\\.(?i:svg|png|ico|jpg|jpeg|webp|gif|avif|bmp)");
    private static final Set<String> READABLE_IMAGES = ConcurrentHashMap.newKeySet();

    private PublicStaticResource() { }

    static boolean matches(HttpServletRequest request) {
        String method = request.getMethod();
        if (!("GET".equals(method) || "HEAD".equals(method))) {
            return false;
        }
        return matchesPath(request.getServletPath());
    }

    static boolean matchesPath(String path) {
        if (!hasPublicRootImageName(path)) {
            return false;
        }
        if (READABLE_IMAGES.contains(path)) {
            return true;
        }
        if (!new ClassPathResource("static" + path).isReadable()) {
            return false;
        }
        READABLE_IMAGES.add(path);
        return true;
    }

    static boolean hasPublicRootImageName(String path) {
        return path != null && ROOT_IMAGE.matcher(path).matches();
    }
}
