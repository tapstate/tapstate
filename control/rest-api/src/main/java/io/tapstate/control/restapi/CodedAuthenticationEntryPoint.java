package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import java.io.IOException;
import java.util.Objects;

/** Emits Tapstate's canonical unauthenticated error instead of a redirect, Basic challenge, or HTML page. */
final class CodedAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private final ApiSecurityErrorWriter errors;

    CodedAuthenticationEntryPoint(ApiSecurityErrorWriter errors) {
        this.errors = Objects.requireNonNull(errors, "errors");
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response,
            AuthenticationException authenticationException) throws IOException {
        errors.unauthenticated(request, response);
    }
}
