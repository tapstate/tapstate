package io.tapstate.control.restapi;

import io.tapstate.core.common.OperationErrorDiagnostics;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/** Diagnoses only authenticated business requests after authorization has allowed them through. */
final class OperationErrorDiagnosticsFilter extends OncePerRequestFilter {
    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var authentication = org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication();
        String path = request.getRequestURI();
        if (path == null || !path.startsWith("/api/") || authentication == null || !authentication.isAuthenticated()
                || authentication instanceof org.springframework.security.authentication.AnonymousAuthenticationToken) {
            chain.doFilter(request, response);
            return;
        }
        try (var diagnostics = OperationErrorDiagnostics.open()) { chain.doFilter(request, response); }
    }
}
