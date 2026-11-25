package io.tapstate.control.restapi;

import io.tapstate.core.common.OperationErrorDiagnostics;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import static org.assertj.core.api.Assertions.assertThat;

class OperationErrorDiagnosticsFilterTest {
    @Test void onlyAuthenticatedBusinessDispatchOwnsAProbeScope() throws Exception {
        var context = SecurityContextHolder.getContext();
        var previous = context.getAuthentication();
        try {
            var request = new MockHttpServletRequest("GET", "/api/pipelines");
            var response = new MockHttpServletResponse();
            context.setAuthentication(null);
            new OperationErrorDiagnosticsFilter().doFilter(request, response,
                    (incoming, outgoing) -> assertThat(OperationErrorDiagnostics.active()).isFalse());
            context.setAuthentication(new AnonymousAuthenticationToken("key", "anonymous", List.of(new SimpleGrantedAuthority("ANONYMOUS"))));
            new OperationErrorDiagnosticsFilter().doFilter(request, response,
                    (incoming, outgoing) -> assertThat(OperationErrorDiagnostics.active()).isFalse());
            context.setAuthentication(new TestingAuthenticationToken("operator", null, "ADMIN"));
            new OperationErrorDiagnosticsFilter().doFilter(request, response,
                    (incoming, outgoing) -> assertThat(OperationErrorDiagnostics.active()).isTrue());
            assertThat(OperationErrorDiagnostics.active()).isFalse();
            new OperationErrorDiagnosticsFilter().doFilter(new MockHttpServletRequest("POST", "/auth/bootstrap"), response,
                    (incoming, outgoing) -> assertThat(OperationErrorDiagnostics.active()).isFalse());
        } finally { context.setAuthentication(previous); }
    }
}
