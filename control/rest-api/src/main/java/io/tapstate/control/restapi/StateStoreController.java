package io.tapstate.control.restapi;

import io.tapstate.control.core.SourceView;
import io.tapstate.control.core.StateStoreSetupService;
import java.util.Map;
import java.util.Objects;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Connects the user's MongoDB Atlas store for materialized Cloud views. */
@RestController
class StateStoreController {
    private final StateStoreSetupService setup;

    StateStoreController(StateStoreSetupService setup) {
        this.setup = Objects.requireNonNull(setup, "setup");
    }

    @Verb("state-store.connect")
    @PostMapping("/state-store")
    SourceView connect(@RequestBody Map<String, Object> config) {
        return setup.connect(AuthenticatedCaller.subject(), config);
    }
}
