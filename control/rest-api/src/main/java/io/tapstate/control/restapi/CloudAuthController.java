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

import java.net.URI;
import java.util.Map;

/** Managed login and the separately authenticated back-channel invalidation surface. */
@Controller
class CloudAuthController {

    static final String EXCHANGE_PATH = "/auth/exchange";
    static final String INVALIDATE_PATH = "/auth/invalidate-session";
    private static final int MAX_JTI_LENGTH = 512;
    private static final int MAX_TIMESTAMP_LENGTH = 32;
    private static final int MAX_NONCE_LENGTH = 256;
    private static final int MAX_SIGNATURE_LENGTH = 2048;

    private final AuthenticationMode mode;
    private final ObjectProvider<CloudAuthenticationService> authentication;
    private final ObjectProvider<CloudSessionCallbackVerifier> callbacks;

    CloudAuthController(ObjectProvider<AuthenticationMode> modes,
            ObjectProvider<CloudAuthenticationService> authentication,
            ObjectProvider<CloudSessionCallbackVerifier> callbacks) {
        mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.authentication = authentication;
        this.callbacks = callbacks;
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
    ResponseEntity<Void> invalidate(HttpServletRequest request,
            @RequestParam(name = "jti", required = false) String jti,
            @RequestParam(name = "ts", required = false) String timestamp,
            @RequestParam(name = "nonce", required = false) String nonce,
            @RequestParam(name = "sign", required = false) String signature) {
        CloudAuthenticationService service = requireCloud();
        CloudSessionCallbackVerifier verifier = callbacks.getIfAvailable();
        if (verifier == null) {
            throw CloudAuthenticationService.unavailable();
        }
        if (!bounded(jti, MAX_JTI_LENGTH)
                || !bounded(timestamp, MAX_TIMESTAMP_LENGTH)
                || !bounded(nonce, MAX_NONCE_LENGTH)
                || !bounded(signature, MAX_SIGNATURE_LENGTH)) {
            throw malformed();
        }
        // The provider signs exactly POST|ts|nonce|jti and sends an empty body. The SDK adapter owns
        // signature/JWKS details; this controller only preserves the signed fields without inventing
        // a second callback format or accepting a user credential as proof.
        if (!service.authorizeCallback(verifier, request.getMethod(), timestamp, nonce, jti, signature)) {
            throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
        }
        service.invalidateJwt(jti);
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
                Map.of("reason", "valid jti, ts, nonce, and sign callback parameters are required"), null);
    }

    private static boolean bounded(String value, int maximumLength) {
        return value != null && !value.isBlank() && value.length() <= maximumLength;
    }
}
