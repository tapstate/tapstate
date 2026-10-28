package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/** Anonymous navigation hints only; the authenticated snapshot remains behind the dispatcher. */
@Controller
class StorageEntryController {
    private final ObjectProvider<StorageEntryStatus> states;
    private final ObjectProvider<InstallationHttpAccess> installations;
    StorageEntryController(ObjectProvider<StorageEntryStatus> states, ObjectProvider<InstallationHttpAccess> installations) {
        this.states = states; this.installations = installations;
    }
    @GetMapping("/auth/storage-entry") @ResponseBody
    ResponseEntity<Map<String,Object>> entry(HttpServletRequest request) {
        var installation = installations.getIfAvailable();
        boolean management = installation != null && installation.isManagementRequest(request);
        if (management) installation.requireManagementRequest(request);
        var progress = management ? installation.initializationProgress() : Map.<String,Object>of();
        var state = "RUNNING".equals(progress.get("state")) ? null : states.getIfAvailable();
        var summary = "RUNNING".equals(progress.get("state"))
                ? new StorageEntryStatus.Summary("OP", StorageEntryStatus.Entry.UNAVAILABLE)
                : state == null ? new StorageEntryStatus.Summary("OP", StorageEntryStatus.Entry.UNAVAILABLE) : state.read();
        var body = new java.util.LinkedHashMap<String,Object>();
        body.put("mode", summary.mode()); body.put("entry", summary.entry().name()); body.put("management", management);
        if (management && installation.automaticAuthorization()) body.put("automaticAuthorization", true);
        if (!progress.isEmpty()) body.put("initializationProgress", progress);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(body);
    }
}
