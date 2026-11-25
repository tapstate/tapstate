package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.SourceView;
import io.tapstate.control.core.StateStoreSetupService;
import java.util.Map;
import java.util.Objects;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Connects the user's MongoDB Atlas store for materialized Cloud views. */
@RestController
class StateStoreController {
    private final StateStoreSetupService setup;

    private final AuditGate audit;

    StateStoreController(StateStoreSetupService setup, AuditGate audit) {
        this.audit = Objects.requireNonNull(audit, "audit");
        this.setup = Objects.requireNonNull(setup, "setup");
    }

    @Verb("state-store.connection-uri")
    @PostMapping("/state-store/{id}/connection-uri")
    ResponseEntity<Map<String, String>> connectionUri(@PathVariable("id") String id) {
        String uri = setup.connectionUri(AuthenticatedCaller.principal(), id, AuthenticatedCaller.requestGuard(), audit);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Map.of("uri", uri));
    }

    @Verb("state-store.connect")
    @PostMapping("/state-store")
    SourceView connect(@RequestBody Map<String, Object> config) {
        return setup.connect(AuthenticatedCaller.subject(), config);
    }
}
