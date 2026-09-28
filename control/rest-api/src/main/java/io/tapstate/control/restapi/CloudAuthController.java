package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudSessionCallbackVerifier;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.core.JacksonException;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Managed login and the separately authenticated back-channel invalidation surface. */
@Controller
class CloudAuthController {

    static final String EXCHANGE_PATH = "/auth/exchange";
    static final String INVALIDATE_PATH = "/auth/invalidate-session";
    private static final int MAX_CALLBACK_BYTES = 4096;

    private final AuthenticationMode mode;
    private final ObjectProvider<CloudAuthenticationService> authentication;
    private final ObjectProvider<CloudSessionCallbackVerifier> callbacks;
    private final ObjectMapper json;

    CloudAuthController(ObjectProvider<AuthenticationMode> modes,
            ObjectProvider<CloudAuthenticationService> authentication,
            ObjectProvider<CloudSessionCallbackVerifier> callbacks, ObjectMapper json) {
        mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.authentication = authentication;
        this.callbacks = callbacks;
        this.json = json;
    }

    @GetMapping(EXCHANGE_PATH)
    ResponseEntity<Void> exchange(@RequestParam(name = "code", required = false) String code) {
        CloudAuthenticationService service = requireCloud();
        var session = service.exchangeCode(code);
        return ResponseEntity.status(302).location(URI.create("/"))
                .header(HttpHeaders.SET_COOKIE, CloudSessionCookies.set(session.token()))
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .header("Referrer-Policy", "no-referrer").build();
    }

    @PostMapping(INVALIDATE_PATH)
    ResponseEntity<Void> invalidate(HttpServletRequest request) {
        CloudAuthenticationService service = requireCloud();
        CloudSessionCallbackVerifier verifier = callbacks.getIfAvailable();
        if (verifier == null) {
            throw CloudAuthenticationService.unavailable();
        }
        byte[] body;
        try {
            body = request.getInputStream().readNBytes(MAX_CALLBACK_BYTES + 1);
        } catch (IOException unreadable) {
            throw malformed();
        }
        if (body.length > MAX_CALLBACK_BYTES) {
            throw malformed();
        }
        Map<String, List<String>> headers = new LinkedHashMap<>();
        request.getHeaderNames().asIterator().forEachRemaining(name -> {
            List<String> values = new ArrayList<>();
            request.getHeaders(name).asIterator().forEachRemaining(values::add);
            headers.put(name.toLowerCase(Locale.ROOT), List.copyOf(values));
        });
        if (!service.authorizeCallback(verifier, request.getMethod(), INVALIDATE_PATH,
                Map.copyOf(headers), body.clone())) {
            throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
        }
        Invalidation parsed;
        try {
            parsed = json.readValue(body, Invalidation.class);
        } catch (JacksonException malformed) {
            // JSON parser messages can quote raw callback content. Do not attach them to diagnostics.
            throw malformed();
        }
        if (parsed == null || parsed.jti() == null || parsed.jti().isBlank()) {
            throw malformed();
        }
        service.invalidateJwt(parsed.jti());
        return ResponseEntity.noContent().header(HttpHeaders.CACHE_CONTROL, "no-store").build();
    }

    private CloudAuthenticationService requireCloud() {
        if (mode != AuthenticationMode.CLOUD) {
            throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "on-prem"), null);
        }
        CloudAuthenticationService service = authentication.getIfAvailable();
        if (service == null) {
            throw CloudAuthenticationService.unavailable();
        }
        return service;
    }

    private static TapstateException malformed() {
        return new TapstateException(ControlError.MALFORMED_REQUEST,
                Map.of("reason", "a valid jti callback body is required"), null);
    }

    private record Invalidation(String jti) { }
}
