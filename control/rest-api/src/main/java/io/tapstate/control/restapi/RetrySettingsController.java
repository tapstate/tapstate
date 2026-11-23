package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.RetrySettingsService;
import io.tapstate.control.core.RetrySettingsService.SettingsBody;
import io.tapstate.core.common.TapstateException;
import java.util.Map;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
class RetrySettingsController {
    private final ObjectProvider<RetrySettingsService> services;

    RetrySettingsController(ObjectProvider<RetrySettingsService> services) {
        this.services = services;
    }

    @Verb("retry-settings.get")
    @GetMapping("/retry-settings")
    ResponseEntity<SettingsBody> get() {
        return response(service().get());
    }

    @Verb("retry-settings.replace")
    @PutMapping("/retry-settings")
    ResponseEntity<SettingsBody> put(@RequestBody SettingsBody body) {
        return response(service().replace(AuthenticatedCaller.subject(), body));
    }

    private RetrySettingsService service() {
        var service = services.getIfAvailable();
        if (service == null) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        return service;
    }

    private static ResponseEntity<SettingsBody> response(SettingsBody body) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(body);
    }
}
