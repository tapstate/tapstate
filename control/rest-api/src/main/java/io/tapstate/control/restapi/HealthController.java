package io.tapstate.control.restapi;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.LivenessState;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * The anonymous liveness probe for load balancers and readiness checks. It is pure HTTP, not the
 * projection of a control operation: it carries no {@code @Verb}, is not registered, and needs no
 * authentication — a probe must answer before anyone can log in. It is a plain {@code @Controller}
 * (not {@code @RestController}) precisely so it stays at the root, outside the {@code /api} prefix
 * that sweeps up every verb endpoint.
 *
 * <p>It answers 200 while this process serves HTTP and has not been declared broken, and 503 once it
 * has. Broken is the application's own liveness: the process is still up and cannot recover by itself,
 * so whatever watches this probe is expected to restart it. A server whose engine has been lost is the
 * case this exists for: it goes on serving HTTP over nothing that can run a pipeline. The probe states
 * nothing else — no version, no topology, no dependency health, and not why the process is broken.
 * Anything more informative than liveness is a registry operation behind authentication.
 *
 * <p>Only a liveness that has been declared broken fails the probe. A context that keeps no liveness at
 * all, or has not been told one yet, answers as it always has, because an unknown liveness is not a
 * reason to have the process restarted.
 */
@Controller
class HealthController {

    private final ObjectProvider<ApplicationAvailability> availability;

    HealthController(ObjectProvider<ApplicationAvailability> availability) {
        this.availability = availability;
    }

    @GetMapping("/healthz")
    ResponseEntity<String> healthz() {
        ApplicationAvailability kept = availability.getIfAvailable();
        if (kept != null && kept.getState(LivenessState.class) == LivenessState.BROKEN) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("broken");
        }
        return ResponseEntity.ok("ok");
    }
}
