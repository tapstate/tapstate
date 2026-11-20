package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.ManagedAuthenticationContext;
import io.tapstate.control.core.ManagedAiProvider;
import io.tapstate.control.core.Scope;
import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.control.core.AiProviderSettings;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import java.util.concurrent.Semaphore;

/** Builder credential setup; all reads expose metadata only. */
@RestController
class AiProviderController {
    private static final Semaphore CHECK_CAPACITY = new Semaphore(2);
    private final ObjectProvider<AiProviderService> services;
    private final ObjectProvider<CloudAuthenticationService> cloud;
    private final AuthenticationMode mode;
    private final AiProviderClient client;
    private final ManagedAiProvider managed;
    private final Set<String> providerManagers;
    private final ObjectProvider<ManagedAuthenticationContext.Provider> contexts;
    AiProviderController(ObjectProvider<AiProviderService> services, ObjectProvider<CloudAuthenticationService> cloud,
            ObjectProvider<AuthenticationMode> modes, AiProviderClient client) {
        this(services, cloud, modes, client, null, null, "");
    }
    AiProviderController(ObjectProvider<AiProviderService> services, ObjectProvider<CloudAuthenticationService> cloud,
            ObjectProvider<AuthenticationMode> modes, AiProviderClient client,
            ObjectProvider<ManagedAuthenticationContext.Provider> contexts) {
        this(services, cloud, modes, client, contexts, null, "");
    }

    @org.springframework.beans.factory.annotation.Autowired
    AiProviderController(ObjectProvider<AiProviderService> services, ObjectProvider<CloudAuthenticationService> cloud,
            ObjectProvider<AuthenticationMode> modes, AiProviderClient client,
            ObjectProvider<ManagedAuthenticationContext.Provider> contexts,
            ObjectProvider<ManagedAiProvider> managed,
            @Value("${tapstate.ai.bedrock.provider-managers:}") String providerManagers) {
        this.contexts = contexts;
        this.services = services; this.cloud = cloud;
        this.mode = modes.getIfAvailable(() -> AuthenticationMode.ON_PREM);
        this.client = client;
        this.managed = mode == AuthenticationMode.CLOUD && managed != null ? managed.getIfAvailable() : null;
        this.providerManagers = java.util.Arrays.stream(providerManagers.split(",")).map(String::strip)
                .filter(value -> !value.isEmpty()).collect(Collectors.toUnmodifiableSet());
    }

    @Verb("ai-provider.get") @GetMapping("/ai-provider-settings")
    ResponseEntity<Map<String, Object>> get(HttpServletRequest request) {
        var workspace = workspace(request);
        var saved = service().saved(workspace.owner()).orElse(null);
        AiProviderSettings settings = saved == null ? null : saved.settings();
        boolean useManaged = usesManaged(saved);
        workspace.requireActive().run();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("configured", useManaged ? managed != null : settings != null);
        result.put("providerId", settings == null ? null : settings.providerId());
        result.put("baseUrl", settings == null ? null : settings.baseUrl());
        result.put("model", settings == null ? null : settings.model());
        if (mode == AuthenticationMode.CLOUD && (managed != null || useManaged || !providerManagers.isEmpty())) {
            result.put("mode", useManaged ? "managed" : settings != null ? "byok" : null);
            result.put("byokConfigured", settings != null);
            result.put("managedAvailable", managed != null);
            result.put("managedModel", managed == null ? null : managed.settings().model());
            result.put("canManage", canManage());
        }
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(result);
    }
    @Verb("ai-provider.replace") @PutMapping("/ai-provider-settings")
    ResponseEntity<Void> put(HttpServletRequest request, @RequestBody AiProviderSettings settings) {
        var workspace = workspace(request);
        if (requiresManager(workspace.owner())) requireManager("ai-provider.replace");
        service().put(workspace.owner(), AuthenticatedCaller.subject(), settings);
        workspace.requireActive().run();
        return ResponseEntity.noContent().cacheControl(CacheControl.noStore()).build();
    }
    @Verb("ai-provider.delete") @DeleteMapping("/ai-provider-settings")
    ResponseEntity<Void> delete(HttpServletRequest request) {
        var workspace = workspace(request);
        if (requiresManager(workspace.owner())) requireManager("ai-provider.delete");
        service().delete(workspace.owner(), AuthenticatedCaller.subject());
        workspace.requireActive().run();
        return ResponseEntity.noContent().cacheControl(CacheControl.noStore()).build();
    }
    record Selection(String mode) { }

    @Verb("ai-provider.select") @PostMapping("/ai-provider-settings:select")
    ResponseEntity<Void> select(HttpServletRequest request, @RequestBody Selection selection) {
        var workspace = workspace(request);
        if (mode != AuthenticationMode.CLOUD || selection == null
                || !("managed".equals(selection.mode()) || "byok".equals(selection.mode()))) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "invalid AI provider selection"), null);
        }
        requireManager("ai-provider.select");
        boolean useManaged = "managed".equals(selection.mode());
        if (useManaged && managed == null) throw AiProviderService.error(ControlError.AI_NOT_CONFIGURED);
        service().select(workspace.owner(), AuthenticatedCaller.subject(), useManaged);
        workspace.requireActive().run();
        return ResponseEntity.noContent().cacheControl(CacheControl.noStore()).build();
    }

    private boolean requiresManager(String owner) {
        return mode == AuthenticationMode.CLOUD && (managed != null || !providerManagers.isEmpty()
                || service().saved(owner).map(AiProviderService.SavedProvider::managedSelected).orElse(false));
    }

    private boolean canManage() {
        var principal = AuthenticatedCaller.principal();
        return AuthenticatedCaller.isCloudSession() && principal.permits(Scope.WRITE)
                && providerManagers.contains(principal.subject());
    }

    private void requireManager(String operation) {
        if (!canManage()) {
            throw new TapstateException(ControlError.FORBIDDEN,
                    Map.of("op", operation, "required", "workspace AI manager"), null);
        }
    }

    record Route(AiProviderSettings settings, AiProviderClient client) { }

    Route assistantRoute(String owner, AiProviderClient byok) {
        if (mode != AuthenticationMode.CLOUD) return new Route(service().require(owner), byok);
        var saved = service().saved(owner).orElse(null);
        if (usesManaged(saved)) {
            if (managed == null) throw AiProviderService.error(ControlError.AI_NOT_CONFIGURED);
            return new Route(managed.settings(), managed.client());
        }
        if (saved == null) throw AiProviderService.error(ControlError.AI_NOT_CONFIGURED);
        return new Route(AiProviderService.validated(saved.settings()), byok);
    }

    private boolean usesManaged(AiProviderService.SavedProvider saved) {
        return mode == AuthenticationMode.CLOUD && (saved == null ? managed != null : saved.managedSelected());
    }

    @Verb("ai-provider.check") @PostMapping("/ai-provider-settings:check")
    ResponseEntity<Map<String, Object>> check(HttpServletRequest request, @RequestBody AiProviderSettings settings) {
        var workspace = workspace(request);
        String scope = workspace.owner();
        if (!CHECK_CAPACITY.tryAcquire()) throw AiProviderService.error(ControlError.AI_OVERLOADED);
        try {
            client.check(service().resolve(scope, settings));
            workspace.requireActive().run();
            return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Map.of("ok", true));
        } finally {
            CHECK_CAPACITY.release();
        }
    }
    @Verb("ai-provider.models") @PostMapping("/ai-provider-settings:models")
    ResponseEntity<Map<String, Object>> models(HttpServletRequest request, @RequestBody AiProviderSettings settings) {
        var workspace = workspace(request);
        if (!CHECK_CAPACITY.tryAcquire()) throw AiProviderService.error(ControlError.AI_OVERLOADED);
        try {
            var models = client.models(service().resolveModels(workspace.owner(), settings));
            workspace.requireActive().run();
            return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Map.of("models", models));
        } finally {
            CHECK_CAPACITY.release();
        }
    }
    record Workspace(String owner, Runnable requireActive) { }
    String owner(HttpServletRequest request) { return workspace(request).owner(); }
    Workspace workspace(HttpServletRequest request) {
        if (mode == AuthenticationMode.ON_PREM) {
            Runnable active = AuthenticatedCaller.requestGuard();
            active.run();
            return new Workspace(AiProviderSettingsStore.ON_PREM_OWNER, active);
        }
        var provider = contexts == null ? null : contexts.getIfAvailable();
        var selected = provider == null ? null : provider.current();
        CloudAuthenticationService authentication = selected == null ? cloud.getIfAvailable() : selected.service();
        if (authentication == null || provider != null && selected == null) throw CloudAuthenticationService.unavailable();
        Runnable active = selected == null ? () -> {} : selected.requireActive();
        active.run();
        String cookie = CloudSessionCookies.read(request).orElseThrow(() -> AiProviderService.error(ControlError.UNAUTHENTICATED));
        var context = authentication.clusterContextView(cookie).orElseThrow(() -> AiProviderService.error(ControlError.UNAUTHENTICATED));
        active.run();
        return new Workspace(JsonWriter.write(java.util.List.of("cloud", context.organizationId(), context.clusterId())), active);
    }
    AiProviderService service() {
        AiProviderService service = services.getIfAvailable();
        if (service == null) throw AiProviderService.error(ControlError.AI_NOT_CONFIGURED);
        return service;
    }
}
