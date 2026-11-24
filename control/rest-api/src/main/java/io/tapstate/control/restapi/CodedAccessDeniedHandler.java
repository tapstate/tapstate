package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;

import java.io.IOException;
import java.util.Objects;

/** Emits a coded forbidden error while retaining the registry-derived operation metadata when available. */
final class CodedAccessDeniedHandler implements AccessDeniedHandler {

    private final ApiSecurityErrorWriter errors;

    CodedAccessDeniedHandler(ApiSecurityErrorWriter errors) {
        this.errors = Objects.requireNonNull(errors, "errors");
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response,
            AccessDeniedException accessDeniedException) throws IOException {
        Object resolved = request.getAttribute(OperationAuthorizationManager.REQUIREMENT_ATTRIBUTE);
        if (resolved instanceof OperationAuthorizationManager.Requirement requirement) {
            errors.forbidden(request, response, requirement.operationId(), requirement.scope().name());
            return;
        }
        errors.forbidden(request, response, "unknown", "unknown");
    }
}
