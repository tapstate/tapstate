package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.AiProviderSettings;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/** Builder credential setup; all reads expose metadata only. */
@RestController
class AiProviderController {
    private static final Semaphore CHECK_CAPACITY = new Semaphore(2);
    private final ObjectProvider<AiProviderService> services;
    private final ObjectProvider<CloudAuthenticationService> cloud;
    private final AuthenticationMode mode;
    private final AiProviderClient client;
    AiProviderController(ObjectProvider<AiProviderService> services, ObjectProvider<CloudAuthenticationService> cloud,
            ObjectProvider<AuthenticationMode> modes, AiProviderClient client) {
        this.services = services; this.cloud = cloud;
        this.mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.client = client;
    }
    @Verb("ai-provider.get") @GetMapping("/ai-provider-settings")
    ResponseEntity<Map<String, Object>> get(HttpServletRequest request) {
        AiProviderSettings settings = service().get(owner(request)).orElse(null);
        Map<String, Object> result = new LinkedHashMap<>(); result.put("configured", settings != null);
        result.put("providerId", settings == null ? null : settings.providerId());
        result.put("baseUrl", settings == null ? null : settings.baseUrl());
        result.put("model", settings == null ? null : settings.model());
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(result);
    }
    @Verb("ai-provider.replace") @PutMapping("/ai-provider-settings")
    ResponseEntity<Void> put(HttpServletRequest request, @RequestBody AiProviderSettings settings) {
        service().put(owner(request), AuthenticatedCaller.subject(), settings);
        return ResponseEntity.noContent().cacheControl(CacheControl.noStore()).build();
    }
    @Verb("ai-provider.delete") @DeleteMapping("/ai-provider-settings")
    ResponseEntity<Void> delete(HttpServletRequest request) {
        service().delete(owner(request), AuthenticatedCaller.subject());
        return ResponseEntity.noContent().cacheControl(CacheControl.noStore()).build();
    }
    @Verb("ai-provider.check") @PostMapping("/ai-provider-settings:check")
    ResponseEntity<Map<String, Object>> check(HttpServletRequest request, @RequestBody AiProviderSettings settings) {
        String scope = owner(request);
        if (!CHECK_CAPACITY.tryAcquire()) throw AiProviderService.error(ControlError.AI_OVERLOADED);
        try {
            client.check(service().resolve(scope, settings));
            return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Map.of("ok", true));
        } finally {
            CHECK_CAPACITY.release();
        }
    }
    String owner(HttpServletRequest request) {
        if (mode != AuthenticationMode.CLOUD) throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "on-prem"), null);
        CloudAuthenticationService authentication = cloud.getIfAvailable();
        if (authentication == null) throw CloudAuthenticationService.unavailable();
        String cookie = CloudSessionCookies.read(request).orElseThrow(() -> AiProviderService.error(ControlError.UNAUTHENTICATED));
        var context = authentication.clusterContextView(cookie).orElseThrow(() -> AiProviderService.error(ControlError.UNAUTHENTICATED));
        return JsonWriter.write(java.util.List.of("cloud", context.organizationId(), context.clusterId()));
    }
    AiProviderService service() {
        AiProviderService service = services.getIfAvailable();
        if (service == null) throw AiProviderService.error(ControlError.AI_NOT_CONFIGURED);
        return service;
    }
}
