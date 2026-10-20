package io.tapstate.control.restapi;

import io.tapstate.control.core.BootstrapService;
import io.tapstate.control.core.CallerOrigin;
import io.tapstate.control.core.AccessTokenGrant;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.ClusterIdentityView;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.CreatedSession;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.LoginService;
import io.tapstate.control.core.LocalAuthenticationContext;
import io.tapstate.control.core.ManagedAuthenticationContext;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SessionService;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The pre-authentication entry points, served at the root outside the {@code /api} verb surface — a caller
 * cannot be required to present a credential to obtain one. Like the liveness probe they are a plain
 * {@code @Controller}, so the {@code /api} path prefix does not sweep them up and the {@code /api}
 * authentication interceptor never guards them; each self-guards instead.
 *
 * <ul>
 *   <li>{@code POST /auth/login} — verify a username / password and mint a short-lived session token. A bad
 *       credential is refused with {@code control.auth-failed} (401), revealing nothing about which half was
 *       wrong.
 *   <li>{@code POST /auth/bootstrap} — the zero-user exception: on a brand-new server create the first admin,
 *       accepted only from the loopback interface and only while no user exists. The remote address is
 *       classified here into a {@link CallerOrigin} and passed to the service, which owns the guards.
 * </ul>
 */
@Controller
class AuthController {

    private final java.util.function.Supplier<LocalAuthenticationContext> local;
    private final java.util.function.Supplier<BootstrapService> bootstrap;
    private final AuthenticationMode authenticationMode;
    private final ObjectProvider<CloudAuthenticationService> cloudAuthentication;
    private final ObjectProvider<ManagedAuthenticationContext.Provider> managedContexts;

    AuthController(
            LoginService loginService,
            SessionService sessionService,
            ClusterIdentityService clusterIdentityService,
            BootstrapService bootstrapService,
            ObjectProvider<AuthenticationMode> authenticationModes) {
        this(loginService, sessionService, clusterIdentityService, bootstrapService, authenticationModes, null);
    }

    AuthController(
            LoginService loginService, SessionService sessionService, ClusterIdentityService clusterIdentityService,
            BootstrapService bootstrapService, ObjectProvider<AuthenticationMode> authenticationModes,
            ObjectProvider<CloudAuthenticationService> cloudAuthentication) {
        this.local = () -> new LocalAuthenticationContext(loginService, sessionService, clusterIdentityService, () -> {});
        this.bootstrap = () -> bootstrapService;
        this.authenticationMode = authenticationModes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.cloudAuthentication = cloudAuthentication;
        this.managedContexts = null;
    }

    @Autowired
    AuthController(ObjectProvider<LocalAuthenticationContext.Provider> contexts,
            ObjectProvider<LoginService> logins, ObjectProvider<SessionService> sessions,
            ObjectProvider<ClusterIdentityService> identities, ObjectProvider<BootstrapService> bootstraps,
            ObjectProvider<AuthenticationMode> modes, ObjectProvider<CloudAuthenticationService> managed,
            ObjectProvider<ManagedAuthenticationContext.Provider> managedContexts) {
        this.local = () -> {
            var selected = contexts.getIfAvailable();
            return selected != null ? selected.current()
                    : new LocalAuthenticationContext(logins.getObject(), sessions.getObject(), identities.getObject(), () -> {});
        };
        this.bootstrap = () -> {
            if (contexts.getIfAvailable() != null)
                throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null);
            return bootstraps.getObject();
        };
        this.authenticationMode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.cloudAuthentication = managed;
        this.managedContexts = managedContexts;
    }

    @PostMapping(AuthWire.LOGIN_PATH)
    ResponseEntity<LoginResponse> login(@RequestBody LoginRequest request) {
        requireOnPremAuthentication();
        // A missing / blank credential field is a malformed request (a 400), distinct from a present-but-wrong
        // credential (the auth-failed 401 the service raises); refuse it here before the service's bare guard.
        MalformedRequest.requireText(request.username(), "a `username` is required");
        MalformedRequest.requireText(request.password(), "a `password` is required");
        var original = local.get(); original.requireActive().run();
        AccessTokenGrant grant = original.login().loginGrant(request.username(), request.password());
        ClusterIdentityView identity = original.identity().identityView();
        CreatedSession session = request.createSession()
                ? original.sessions().create(grant.principal(), grant.scope(), identity.issuer())
                : null;
        original.requireActive().run();
        return ResponseEntity.ok(new LoginResponse(
                grant.token(),
                grant.expiresAt().toString(),
                identity.issuer(),
                grant.principal(),
                scopesFor(grant.scope()),
                session == null ? null : session.token(),
                session == null ? null : session.idleExpiresAt().toString(),
                session == null ? null : session.absoluteExpiresAt().toString()));
    }

    @PostMapping(AuthWire.SESSION_PATH)
    ResponseEntity<SessionExchangeResponse> session(HttpServletRequest request) {
        requireOnPremAuthentication();
        String token = requiredSessionCredential(request);
        var original = local.get(); original.requireActive().run();
        ClusterIdentityView identity = original.identity().identityView();
        AccessTokenGrant grant = original.sessions().exchange(token, identity.issuer())
                .orElseThrow(AuthController::unauthenticated);
        original.requireActive().run();
        return ResponseEntity.ok(new SessionExchangeResponse(
                grant.token(), grant.expiresAt().toString(), identity.issuer(), grant.principal(),
                scopesFor(grant.scope())));
    }

    @PostMapping(AuthWire.LOGOUT_PATH)
    ResponseEntity<Void> logout(HttpServletRequest request) {
        if (authenticationMode == AuthenticationMode.CLOUD) {
            var provider = managedContexts == null ? null : managedContexts.getIfAvailable();
            var original = provider == null ? null : provider.current();
            if (original != null) original.requireActive().run();
            CloudAuthenticationService cloud = original != null ? original.service()
                    : cloudAuthentication == null ? null : cloudAuthentication.getIfAvailable();
            if (cloud == null) {
                throw CloudAuthenticationService.unavailable();
            }
            if (request.getHeaders(HttpHeaders.AUTHORIZATION).hasMoreElements()
                    || !CloudSessionCookies.permitsCookieRequest(request)) {
                throw unauthenticated();
            }
            String cookie = CloudSessionCookies.read(request).orElseThrow(AuthController::unauthenticated);
            if (!cloud.logout(cookie)) {
                throw unauthenticated();
            }
            if (original != null) original.requireActive().run();
            return ResponseEntity.noContent().header(HttpHeaders.SET_COOKIE, CloudSessionCookies.clear())
                    .header(HttpHeaders.CACHE_CONTROL, "no-store").build();
        }
        requireOnPremAuthentication();
        String token = requiredSessionCredential(request);
        var original = local.get(); original.requireActive().run();
        String issuer = original.identity().identityView().issuer();
        if (!original.sessions().logout(token, issuer)) {
            throw unauthenticated();
        }
        original.requireActive().run();
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/auth/bootstrap")
    ResponseEntity<Void> bootstrap(@RequestBody BootstrapRequest request, HttpServletRequest http) {
        requireOnPremAuthentication();
        // A missing / blank credential is refused as a coded 400 before the service builds the User — a blank
        // password would otherwise be hashed into a non-blank hash and silently create an empty-password admin.
        MalformedRequest.requireText(request.username(), "a `username` is required");
        MalformedRequest.requireText(request.password(), "a `password` is required");
        CallerOrigin origin = CallerOrigins.classify(http.getRemoteAddr());
        bootstrap.get().createFirstAdmin(origin, request.username(), request.password());
        return ResponseEntity.noContent().build();
    }

    private static String requiredSessionCredential(HttpServletRequest request) {
        List<String> values = new ArrayList<>();
        request.getHeaders(HttpHeaders.AUTHORIZATION).asIterator().forEachRemaining(values::add);
        if (values.size() != 1) {
            throw unauthenticated();
        }
        return AuthWire.sessionCredential(values.get(0)).orElseThrow(AuthController::unauthenticated);
    }

    private static TapstateException unauthenticated() {
        return new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
    }

    private void requireOnPremAuthentication() {
        if (authenticationMode != AuthenticationMode.ON_PREM) {
            throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE,
                    Map.of("mode", authenticationMode.name().toLowerCase(java.util.Locale.ROOT)), null);
        }
    }

    private static List<String> scopesFor(Scope grade) {
        List<String> scopes = new ArrayList<>();
        for (Scope scope : Scope.values()) {
            if (grade.permits(scope)) {
                scopes.add(scope.name().toLowerCase(java.util.Locale.ROOT));
            }
        }
        return List.copyOf(scopes);
    }
}
