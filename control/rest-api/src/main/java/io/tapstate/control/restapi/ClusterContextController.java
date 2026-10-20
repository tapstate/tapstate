package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ManagedAuthenticationContext;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Reads only the ClusterContext saved on the authenticated Cloud session. */
@RestController
class ClusterContextController {

    private final AuthenticationMode mode;
    private final ObjectProvider<CloudAuthenticationService> authentication;
    private final ObjectProvider<ManagedAuthenticationContext.Provider> contexts;

    ClusterContextController(ObjectProvider<AuthenticationMode> modes,
            ObjectProvider<CloudAuthenticationService> authentication) {
        this(modes, authentication, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    ClusterContextController(ObjectProvider<AuthenticationMode> modes,
            ObjectProvider<CloudAuthenticationService> authentication,
            ObjectProvider<ManagedAuthenticationContext.Provider> contexts) {
        mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.authentication = authentication;
        this.contexts = contexts;
    }

    @Verb("cluster.context")
    @GetMapping("/cluster/context")
    ResponseEntity<ClusterContextResponse> current(HttpServletRequest request) {
        if (mode != AuthenticationMode.CLOUD) {
            throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "on-prem"), null);
        }
        var provider = contexts == null ? null : contexts.getIfAvailable();
        var original = provider == null ? null : provider.current();
        if (original != null) original.requireActive().run();
        CloudAuthenticationService service = original == null ? authentication.getIfAvailable() : original.service();
        if (service == null) {
            throw CloudAuthenticationService.unavailable();
        }
        String cookie = CloudSessionCookies.read(request)
                .orElseThrow(() -> new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null));
        ClusterContextResponse context = service.clusterContextView(cookie)
                .map(ClusterContextResponse::from)
                .orElseThrow(() -> new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null));
        if (original != null) original.requireActive().run();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(context);
    }
}
