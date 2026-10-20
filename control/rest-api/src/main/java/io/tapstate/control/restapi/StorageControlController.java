package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StorageControlCaller;
import io.tapstate.control.core.StorageControlDispatcher;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** Authenticated projections of the shared storage dispatcher, with bounded request-only secrets. */
@RestController
class StorageControlController {
    private static final int MAX_REQUEST_BYTES = 9 * 1024 * 1024;
    private final ObjectProvider<StorageControlDispatcher> dispatchers;
    private final ObjectProvider<StorageControlCaller> callers;
    StorageControlController(ObjectProvider<StorageControlDispatcher> dispatchers, ObjectProvider<StorageControlCaller> callers) {
        this.dispatchers = dispatchers; this.callers = callers;
    }
    @Verb("storage.status")
    @PostMapping("/storage/status")
    ResponseEntity<Map<String,Object>> storage_status(HttpServletRequest request) throws IOException { return invoke("storage.status", request); }
    @Verb("storage.test")
    @PostMapping("/storage/test")
    ResponseEntity<Map<String,Object>> storage_test(HttpServletRequest request) throws IOException { return invoke("storage.test", request); }
    @Verb("storage.setup.commit")
    @PostMapping("/storage/setup/commit")
    ResponseEntity<Map<String,Object>> storage_setup_commit(HttpServletRequest request) throws IOException { return invoke("storage.setup.commit", request); }
    @Verb("storage.setup.status")
    @PostMapping("/storage/setup/status")
    ResponseEntity<Map<String,Object>> storage_setup_status(HttpServletRequest request) throws IOException { return invoke("storage.setup.status", request); }
    @Verb("storage.recovery.plan")
    @PostMapping("/storage/recovery/plan")
    ResponseEntity<Map<String,Object>> storage_recovery_plan(HttpServletRequest request) throws IOException { return invoke("storage.recovery.plan", request); }
    @Verb("storage.recovery.confirm")
    @PostMapping("/storage/recovery/confirm")
    ResponseEntity<Map<String,Object>> storage_recovery_confirm(HttpServletRequest request) throws IOException { return invoke("storage.recovery.confirm", request); }
    @Verb("storage.recovery.status")
    @PostMapping("/storage/recovery/status")
    ResponseEntity<Map<String,Object>> storage_recovery_status(HttpServletRequest request) throws IOException { return invoke("storage.recovery.status", request); }
    @Verb("configuration.import.preflight")
    @PostMapping("/configuration/import/preflight")
    ResponseEntity<Map<String,Object>> configuration_import_preflight(HttpServletRequest request) throws IOException { return invoke("configuration.import.preflight", request); }
    @Verb("configuration.import.commit")
    @PostMapping("/configuration/import/commit")
    ResponseEntity<Map<String,Object>> configuration_import_commit(HttpServletRequest request) throws IOException { return invoke("configuration.import.commit", request); }
    @Verb("configuration.import.status")
    @PostMapping("/configuration/import/status")
    ResponseEntity<Map<String,Object>> configuration_import_status(HttpServletRequest request) throws IOException { return invoke("configuration.import.status", request); }
    private ResponseEntity<Map<String,Object>> invoke(String operation, HttpServletRequest request) throws IOException {
        var dispatcher = dispatchers.getIfAvailable(); var caller = callers.getIfAvailable();
        if (dispatcher == null || caller == null) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        var verified = caller.current();
        byte[] body = request.getInputStream().readNBytes(MAX_REQUEST_BYTES + 1);
        if (body.length > MAX_REQUEST_BYTES) throw malformed();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(dispatcher.dispatch(operation, verified, decode(body)));
    }
    static Map<String,Object> decode(byte[] body) {
        Object decoded;
        try { decoded = JsonReader.parse(new String(body, java.nio.charset.StandardCharsets.UTF_8)); }
        catch (IllegalArgumentException invalid) { throw malformed(); }
        if (!(decoded instanceof Map<?,?> map) || map.keySet().stream().anyMatch(key -> !(key instanceof String))) throw malformed();
        @SuppressWarnings("unchecked") var input = (Map<String,Object>) map; return input;
    }
    private static TapstateException malformed() {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "storage operation input does not match its schema"), null);
    }
}
