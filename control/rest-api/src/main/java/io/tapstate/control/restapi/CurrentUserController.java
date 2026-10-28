package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CredentialType;
import io.tapstate.control.core.CurrentUserQueryService;
import io.tapstate.control.core.CurrentUserView;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Projects only the identity authenticated by the existing workload security chain. */
@RestController
class CurrentUserController {
    private final CurrentUserQueryService users;
    private final ObjectProvider<CloudAuthenticationService> cloud;

    CurrentUserController(CurrentUserQueryService users, ObjectProvider<CloudAuthenticationService> cloud) {
        this.users = users;
        this.cloud = cloud;
    }

    @Verb("auth.current-user")
    @GetMapping("/auth/me")
    ResponseEntity<CurrentUserResponse> current(HttpServletRequest request) {
        var principal = AuthenticatedCaller.principal();
        String email = null;
        if (principal.credentialType() == CredentialType.CLOUD_SESSION) {
            CloudAuthenticationService service = cloud.getIfAvailable();
            if (service != null) {
                email = CloudSessionCookies.read(request)
                        .flatMap(service::clusterContext)
                        .map(context -> context.userEmail()).orElse(null);
            }
        }
        CurrentUserView user = users.get(principal, email);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(new CurrentUserResponse(user.mode(), user.principal(), user.scopes(), user.username(), user.email()));
    }

    record CurrentUserResponse(String mode, String principal, List<String> scopes,
            @JsonInclude(JsonInclude.Include.ALWAYS) String username,
            @JsonInclude(JsonInclude.Include.ALWAYS) String email) { }
}
