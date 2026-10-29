package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

/** Allows reads of packaged root-level image resources without exposing application endpoints. */
final class PublicStaticResource {
    private static final Pattern ROOT_IMAGE =
            Pattern.compile("/(?![^/]*\\.\\.)[A-Za-z0-9_-][A-Za-z0-9._-]*\\.(?i:svg|png|ico|jpg|jpeg|webp|gif|avif|bmp)");
    private static final Set<String> PUBLIC_IMAGES = loadPublicImages(
            new PathMatchingResourcePatternResolver(PublicStaticResource.class.getClassLoader()));

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
        return PUBLIC_IMAGES.contains(path);
    }

    static boolean hasPublicRootImageName(String path) {
        return path != null && ROOT_IMAGE.matcher(path).matches();
    }

    static Set<String> loadPublicImages(PathMatchingResourcePatternResolver resolver) {
        try {
            return Arrays.stream(resolver.getResources("classpath*:/static/*"))
                    .map(Resource::getFilename)
                    .filter(Objects::nonNull)
                    .map(filename -> "/" + filename)
                    .filter(PublicStaticResource::hasPublicRootImageName)
                    .collect(Collectors.toUnmodifiableSet());
        } catch (IOException failure) {
            throw new IllegalStateException("Unable to enumerate packaged public images from classpath*:/static/*", failure);
        }
    }
}
