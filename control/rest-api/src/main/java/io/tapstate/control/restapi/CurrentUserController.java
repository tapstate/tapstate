package io.tapstate.control.restapi;

import io.tapstate.control.core.CurrentUserQueryService;
import io.tapstate.control.core.CurrentUserView;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Projects only the identity authenticated by the existing workload security chain. */
@RestController
class CurrentUserController {
    private final CurrentUserQueryService users;

    CurrentUserController(CurrentUserQueryService users) {
        this.users = users;
    }

    @Verb("auth.current-user")
    @GetMapping("/auth/me")
    ResponseEntity<CurrentUserView> current() {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(users.get(AuthenticatedCaller.principal()));
    }
}
