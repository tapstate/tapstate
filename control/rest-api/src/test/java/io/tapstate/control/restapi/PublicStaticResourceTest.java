package io.tapstate.control.restapi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublicStaticResourceTest {
    @Test
    void packagedImagesArePublicForGetAndHead() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/tapstate-lockup-dark.svg");
        request.setServletPath("/tapstate-lockup-dark.svg");
        assertThat(PublicStaticResource.matches(request)).isTrue();
        assertThat(PublicStaticResource.hasPublicRootImageName("/brand.png")).isTrue();
        assertThat(PublicStaticResource.hasPublicRootImageName("/brand.webp")).isTrue();
        assertThat(PublicStaticResource.hasPublicRootImageName("/brand.PNG")).isTrue();

        request.setMethod("HEAD");
        assertThat(PublicStaticResource.matches(request)).isTrue();
    }

    @Test
    void nonImageMethodsPathsAndUnpackagedImagesAreNotPublic() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/tapstate-lockup-dark.svg");
        assertThat(PublicStaticResource.matches(request)).isFalse();

        request.setMethod("GET");
        request.setRequestURI("/api/tapstate-lockup-dark.svg");
        request.setServletPath("/api/tapstate-lockup-dark.svg");
        assertThat(PublicStaticResource.matches(request)).isFalse();

        request.setRequestURI("/not-packaged.svg");
        request.setServletPath("/not-packaged.svg");
        assertThat(PublicStaticResource.matches(request)).isFalse();

        request.setRequestURI("/readme.txt");
        request.setServletPath("/readme.txt");
        assertThat(PublicStaticResource.matches(request)).isFalse();

        assertThat(PublicStaticResource.hasPublicRootImageName("/..image.svg")).isFalse();
        assertThat(PublicStaticResource.hasPublicRootImageName("/nested/image.svg")).isFalse();
    }

    @Test
    void classpathScanFailureRetainsItsCause() {
        IOException cause = new IOException("resource scan failed");
        var resolver = new PathMatchingResourcePatternResolver(getClass().getClassLoader()) {
            @Override
            public Resource[] getResources(String pattern) throws IOException {
                throw cause;
            }
        };

        assertThatThrownBy(() -> PublicStaticResource.loadPublicImages(resolver))
                .isInstanceOf(IllegalStateException.class)
                .hasCause(cause);
    }

    @Test
    void discoversRootPngAndSvgFromAWebJar(@TempDir Path directory) throws IOException {
        Path webJar = directory.resolve("web.jar");
        try (var jar = new JarOutputStream(Files.newOutputStream(webJar))) {
            addEntry(jar, "static/", "");
            addEntry(jar, "static/brand.png", "png fixture");
            addEntry(jar, "static/brand.svg", "svg fixture");
            addEntry(jar, "static/index.html", "html fixture");
            addEntry(jar, "static/nested/", "");
            addEntry(jar, "static/nested/hidden.png", "nested fixture");
        }

        try (var loader = new URLClassLoader(new URL[] {webJar.toUri().toURL()}, null)) {
            var resolver = new PathMatchingResourcePatternResolver(loader);
            assertThat(PublicStaticResource.loadPublicImages(resolver))
                    .containsExactlyInAnyOrder("/brand.png", "/brand.svg");
        }
    }

    private static void addEntry(JarOutputStream jar, String name, String contents) throws IOException {
        jar.putNextEntry(new JarEntry(name));
        jar.write(contents.getBytes(StandardCharsets.UTF_8));
        jar.closeEntry();
    }
}
