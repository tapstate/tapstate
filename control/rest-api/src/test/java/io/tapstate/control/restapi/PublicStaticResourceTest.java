package io.tapstate.control.restapi;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;

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
}
