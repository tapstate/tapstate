package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CurrentUserQueryService;
import io.tapstate.control.core.CurrentUserView;
import io.tapstate.control.core.ManagedAuthenticationContext;
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
    private final ObjectProvider<ManagedAuthenticationContext.Provider> contexts;

    CurrentUserController(CurrentUserQueryService users, ObjectProvider<CloudAuthenticationService> cloud) {
        this(users,cloud,null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    CurrentUserController(CurrentUserQueryService users, ObjectProvider<CloudAuthenticationService> cloud,
            ObjectProvider<ManagedAuthenticationContext.Provider> contexts) {
        this.users = users;
        this.cloud = cloud;
        this.contexts = contexts;
    }

    @Verb("auth.current-user")
    @GetMapping("/auth/me")
    ResponseEntity<CurrentUserResponse> current(HttpServletRequest request) {
        String email = null;
        Runnable active=()->{};
        if (AuthenticatedCaller.isCloudSession()) {
            var provider=contexts==null?null:contexts.getIfAvailable();
            var original=provider==null?null:provider.current();
            if(provider!=null&&original==null)throw CloudAuthenticationService.unavailable();
            if(original!=null)active=original.requireActive();
            active.run();
            CloudAuthenticationService service = original==null?cloud.getIfAvailable():original.service();
            if (service != null) {
                email = CloudSessionCookies.read(request)
                        .flatMap(service::userEmail).orElse(null);
            }
        }
        CurrentUserView user = users.get(AuthenticatedCaller.principal(), email);
        active.run();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(new CurrentUserResponse(user.mode(), user.principal(), user.scopes(), user.username(), user.email()));
    }

    record CurrentUserResponse(String mode, String principal, List<String> scopes,
            @JsonInclude(JsonInclude.Include.ALWAYS) String username,
            @JsonInclude(JsonInclude.Include.ALWAYS) String email) { }
}
