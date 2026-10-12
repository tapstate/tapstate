package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.ControlError;
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

    ClusterContextController(ObjectProvider<AuthenticationMode> modes,
            ObjectProvider<CloudAuthenticationService> authentication) {
        mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.authentication = authentication;
    }

    @Verb("cluster.context")
    @GetMapping("/cluster/context")
    ResponseEntity<ClusterContextResponse> current(HttpServletRequest request) {
        if (mode != AuthenticationMode.CLOUD) {
            throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "on-prem"), null);
        }
        CloudAuthenticationService service = authentication.getIfAvailable();
        if (service == null) {
            throw CloudAuthenticationService.unavailable();
        }
        String cookie = CloudSessionCookies.read(request)
                .orElseThrow(() -> new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null));
        ClusterContextResponse context = service.clusterContextView(cookie)
                .map(ClusterContextResponse::from)
                .orElseThrow(() -> new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null));
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(context);
    }
}
