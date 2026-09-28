package io.tapstate.app;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Sends the browser's client-side navigation routes to the packaged SPA entry point.
 *
 * <p>This is deliberately an allowlist, not a catch-all. The control API, authentication, liveness,
 * connector icon, and every other server-owned path keep their normal dispatcher behavior, including
 * their 404 responses. Static assets are served by Spring Boot's classpath resource handler and never
 * reach this controller.
 */
@Controller
class SpaRouteController {

    @GetMapping({
            "/",
            "/login",
            "/pipelines/{*path}",
            "/sources/{*path}",
            "/explorations/{*path}"
    })
    String serveSpaEntryPoint() {
        return "forward:/index.html";
    }
}
