package io.tapstate.control.restapi;

import io.tapstate.control.core.AlertControlDispatcher;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** Bounded HTTP face; credentials and scope never come from the JSON request. */
@RestController
class AlertController {
    private final ObjectProvider<AlertControlDispatcher> dispatchers;
    AlertController(ObjectProvider<AlertControlDispatcher> dispatchers) { this.dispatchers = dispatchers; }
    @Verb("alert.catalog") @PostMapping("/alerts/catalog")
    ResponseEntity<Map<String,Object>> catalog(HttpServletRequest r) throws IOException { return invoke("alert.catalog", r); }
    @Verb("alert.resources") @PostMapping("/alerts/resources")
    ResponseEntity<Map<String,Object>> resources(HttpServletRequest r) throws IOException { return invoke("alert.resources", r); }
    @Verb("alert.center") @PostMapping("/alerts/center")
    ResponseEntity<Map<String,Object>> center(HttpServletRequest r) throws IOException { return invoke("alert.center", r); }
    @Verb("alert.detail") @PostMapping("/alerts/detail")
    ResponseEntity<Map<String,Object>> detail(HttpServletRequest r) throws IOException { return invoke("alert.detail", r); }
    @Verb("alert.deliveries") @PostMapping("/alerts/deliveries")
    ResponseEntity<Map<String,Object>> deliveries(HttpServletRequest r) throws IOException { return invoke("alert.deliveries", r); }
    @Verb("alert.health") @PostMapping("/alerts/health")
    ResponseEntity<Map<String,Object>> health(HttpServletRequest r) throws IOException { return invoke("alert.health", r); }
    @Verb("alert.configuration.list") @PostMapping("/alerts/configuration/list")
    ResponseEntity<Map<String,Object>> list(HttpServletRequest r) throws IOException { return invoke("alert.configuration.list", r); }
    @Verb("alert.configuration.save") @PostMapping("/alerts/configuration/save")
    ResponseEntity<Map<String,Object>> save(HttpServletRequest r) throws IOException { return invoke("alert.configuration.save", r); }
    @Verb("alert.configuration.retire") @PostMapping("/alerts/configuration/retire")
    ResponseEntity<Map<String,Object>> retire(HttpServletRequest r) throws IOException { return invoke("alert.configuration.retire", r); }
    @Verb("alert.rule-preview") @PostMapping("/alerts/rule-preview")
    ResponseEntity<Map<String,Object>> rulePreview(HttpServletRequest r) throws IOException { return invoke("alert.rule-preview", r); }
    @Verb("alert.route-preview") @PostMapping("/alerts/route-preview")
    ResponseEntity<Map<String,Object>> routePreview(HttpServletRequest r) throws IOException { return invoke("alert.route-preview", r); }
    @Verb("alert.preview") @PostMapping("/alerts/preview")
    ResponseEntity<Map<String,Object>> preview(HttpServletRequest r) throws IOException { return invoke("alert.preview", r); }
    @Verb("alert.test") @PostMapping("/alerts/test")
    ResponseEntity<Map<String,Object>> test(HttpServletRequest r) throws IOException { return invoke("alert.test", r); }
    @Verb("alert.test-result") @PostMapping("/alerts/test-result")
    ResponseEntity<Map<String,Object>> result(HttpServletRequest r) throws IOException { return invoke("alert.test-result", r); }
    @Verb("alert.retry") @PostMapping("/alerts/retry")
    ResponseEntity<Map<String,Object>> retry(HttpServletRequest r) throws IOException { return invoke("alert.retry", r); }
    private ResponseEntity<Map<String,Object>> invoke(String op, HttpServletRequest request) throws IOException {
        var dispatcher = dispatchers.getIfAvailable();
        if (dispatcher == null) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        var authentication = SecurityContextHolder.getContext().getAuthentication();
        if (!(authentication instanceof TapstateAuthentication verified) || verified.original() == null)
            throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
        byte[] body = request.getInputStream().readNBytes(64 * 1024 + 1);
        if (body.length > 64 * 1024) throw new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "alert request exceeds limit"), null);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(dispatcher.dispatch(op, verified.original(), StorageControlController.decode(body)));
    }
}
