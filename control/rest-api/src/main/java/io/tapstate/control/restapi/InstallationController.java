package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/** First-administrator creation is available only on the installation connector and its own purpose. */
@Controller
class InstallationController {
    private final ObjectProvider<InstallationHttpAccess> installations;
    InstallationController(ObjectProvider<InstallationHttpAccess> installations) { this.installations = installations; }

    @PostMapping("/installation/first-admin") @ResponseBody
    ResponseEntity<Map<String,Object>> create(HttpServletRequest request) throws IOException {
        var installation = installations.getIfAvailable();
        if (installation == null) throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null);
        installation.requireManagementRequest(request);
        byte[] body = request.getInputStream().readNBytes(16 * 1024 + 1);
        if (body.length > 16 * 1024) throw malformed();
        var values = StorageControlController.decode(body);
        if (!values.keySet().equals(java.util.Set.of("username", "password")) || !(values.get("username") instanceof String username)
                || !(values.get("password") instanceof String password)) throw malformed();
        installation.createAdministrator(request, username, password);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Map.of("phase", "COMPLETE", "next", "login"));
    }
    private static TapstateException malformed() {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "administrator credentials are required"), null);
    }
}
