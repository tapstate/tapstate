package io.tapstate.app;

import io.tapstate.control.restapi.SpaNavigationRequest;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.server.ResponseStatusException;

/**
 * Sends the browser's client-side navigation routes to the packaged SPA entry point.
 *
 * <p>Only browser document navigations under client-owned paths receive the SPA shell. Server-owned
 * paths and static assets retain their normal dispatch and error behavior.
 */
@Controller
class SpaRouteController {

    @GetMapping({
            "/",
            "/{first:" + SpaNavigationRequest.CLIENT_ROOT_PATTERN + "}",
            "/{first:" + SpaNavigationRequest.CLIENT_ROOT_PATTERN + "}/{*path}"
    })
    String serveSpaEntryPoint(HttpServletRequest request) {
        if (!SpaNavigationRequest.matches(request)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return "forward:/index.html";
    }
}
