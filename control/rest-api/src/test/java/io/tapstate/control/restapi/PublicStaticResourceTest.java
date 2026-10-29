package io.tapstate.control.restapi;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class PublicStaticResourceTest {
    @Test
    void packagedImagesArePublicForGetAndHead() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/tapstate-lockup-dark.svg");
        request.setServletPath("/tapstate-lockup-dark.svg");
        assertThat(PublicStaticResource.matches(request)).isTrue();

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
    }
}
