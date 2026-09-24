package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.http.HttpHeaders;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Locale;

/** Writes the same coded JSON error contract from Spring Security's filter layer as MVC advice writes. */
final class ApiSecurityErrorWriter {

    private final MessageCatalog catalog;
    private final ObjectMapper json;

    ApiSecurityErrorWriter(MessageCatalog catalog, ObjectMapper json) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.json = Objects.requireNonNull(json, "json");
    }

    void write(HttpServletResponse response, TapstateErrorCode code, Map<String, Object> args) throws IOException {
        MessageCatalog.Rendered rendered = catalog.render(code, args);
        response.setStatus(ApiExceptionHandler.statusFor(code).value());
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-store");
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        json.writeValue(response.getOutputStream(), new ApiError(code.code(), new TreeMap<>(args), rendered.message()));
    }

    void unauthenticated(HttpServletResponse response) throws IOException {
        write(response, ControlError.UNAUTHENTICATED, Map.of());
    }

    void forbidden(HttpServletResponse response, String operation, String required) throws IOException {
        write(response, ControlError.FORBIDDEN,
                Map.of("op", operation, "required", required.toLowerCase(Locale.ROOT)));
    }
}
