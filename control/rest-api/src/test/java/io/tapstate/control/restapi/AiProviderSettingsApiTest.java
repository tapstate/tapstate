package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ManagedAiProvider;
import io.tapstate.control.core.CredentialAuthentication;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.control.core.AiProviderSettings;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.CloudSessionContext;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.core.context.SecurityContextHolder;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Provider settings are scoped through the verified Cloud session and expose credential-free metadata. */
class AiProviderSettingsApiTest {
    private static final Instant NOW = Instant.parse("2026-10-17T00:00:00Z");
    private static final CloudSessionIdentity DEPLOYMENT =
            new CloudSessionIdentity("https://cloud.example", "org-a", "cluster-a");

    @Test void settingsAreWorkspaceScopedWriteOnlyAndRotateWithoutForwardingTheOldKey() throws Exception {
        var values = new HashMap<String, AiProviderSettingsStore.Settings>();
        AiProviderSettingsStore settings = new AiProviderSettingsStore() {
            @Override public Optional<AiProviderSettingsStore.Settings> get(String owner) { return Optional.ofNullable(values.get(owner)); }
            @Override public void put(String owner, AiProviderSettingsStore.Settings value) { values.put(owner, value); }
            @Override public void delete(String owner) { values.remove(owner); }
        };
        List<AuditRecord> auditRecords = new ArrayList<>();
        AuditStore audit = new AuditStore() {
            @Override public void record(AuditRecord record) { auditRecords.add(record); }
        };
        AiProviderService service = new AiProviderService(settings, new AuditGate(audit, Clock.fixed(NOW, ZoneOffset.UTC)));
        AtomicReference<AiProviderSettings> checked = new AtomicReference<>();
        AiProviderClient fake = new AiProviderClient() {
            @Override public void check(AiProviderSettings value) { checked.set(value); }
            @Override public List<String> models(AiProviderSettings value) { checked.set(value); return List.of("model-a", "model-b"); }
            @Override public Optional<Map<String, Object>> propose(AiProviderSettings ignored, List<Map<String, String>> messages,
                    Map<String, Object> context, java.util.function.Consumer<String> text) {
                throw new AssertionError("provider settings test must not request a proposal");
            }
        };
        MemoryCloudSessions sessionStore = new MemoryCloudSessions();
        TokenSecrets secrets = new TokenSecrets() {
            @Override public GeneratedSecret generate() { return new GeneratedSecret("unused", "unused", "unused"); }
            @Override public String hash(String raw) { return "hash-" + raw; }
            @Override public boolean matches(String raw, String hash) { return hash(raw).equals(hash); }
        };
        CloudSessionService sessions = new CloudSessionService(sessionStore, DEPLOYMENT, secrets, Clock.fixed(NOW, ZoneOffset.UTC));
        CloudAuthenticationService authentication = new CloudAuthenticationService((code, cluster) -> "unused",
                (jwt, identity, audience) -> Optional.empty(), sessions);
        sessionStore.add("jti-a", "secret-a", "user-a", "org-a", "cluster-a");
        sessionStore.add("jti-b", "secret-b", "user-b", "org-b", "cluster-b");

        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("mode", AuthenticationMode.CLOUD);
        beans.registerSingleton("service", service);
        beans.registerSingleton("authentication", authentication);
        var controller = new AiProviderController(beans.getBeanProvider(AiProviderService.class),
                beans.getBeanProvider(CloudAuthenticationService.class), beans.getBeanProvider(AuthenticationMode.class), fake);
        MockHttpServletRequest workspaceA = request("jti-a", "secret-a");
        MockHttpServletRequest workspaceB = request("jti-b", "secret-b");
        SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(
                TapstatePrincipal.cloudSession(new VerifiedToken("user-a", Scope.WRITE))));

        try {
            assertThatThrownBy(() -> controller.get(new MockHttpServletRequest("GET", "/ai-provider-settings")))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
            controller.put(workspaceA, new AiProviderSettings("openai-compatible", null, "model-a", "sk-old-key"));
            var metadata = controller.get(workspaceA).getBody();
            assertThat(metadata).containsEntry("configured", true).containsEntry("model", "model-a")
                    .doesNotContainKey("apiKey");
            assertThat(controller.get(workspaceB).getBody()).containsEntry("configured", false);

            controller.put(workspaceA, new AiProviderSettings("openai-compatible", null, "model-b", "sk-new-key"));
            var capacityField = AiProviderController.class.getDeclaredField("CHECK_CAPACITY");
            capacityField.setAccessible(true);
            java.util.concurrent.Semaphore capacity = (java.util.concurrent.Semaphore) capacityField.get(null);
            assertThat(capacity.tryAcquire(2)).isTrue();
            try {
                assertThatThrownBy(() -> controller.check(workspaceA,
                        new AiProviderSettings("openai-compatible", null, "model-b", null)))
                        .isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_OVERLOADED));
                assertThat(checked).hasValue(null);
            } finally {
                capacity.release(2);
            }
            controller.check(workspaceA, new AiProviderSettings("openai-compatible", null, "model-b", null));
            assertThat(checked.get().apiKey()).isEqualTo("sk-new-key");
            var discovered = controller.models(workspaceA, new AiProviderSettings("openai-compatible", null, null, null));
            assertThat(discovered.getBody()).containsEntry("models", List.of("model-a", "model-b")).doesNotContainKey("apiKey");
            assertThat(discovered.getHeaders().getCacheControl()).isEqualTo("no-store");
            assertThat(checked.get().apiKey()).isEqualTo("sk-new-key");
            assertThatThrownBy(() -> controller.models(workspaceA,
                    new AiProviderSettings("openai-compatible", "https://another.example/v1", null, null)))
                    .isInstanceOf(TapstateException.class);
            assertThat(service.require(owner("org-a", "cluster-a")).model()).isEqualTo("model-b");
            controller.put(workspaceA, new AiProviderSettings("openai-responses", null, "model-b", null));
            assertThat(controller.get(workspaceA).getBody()).containsEntry("providerId", "openai-responses").doesNotContainKey("apiKey");
            controller.models(workspaceA, new AiProviderSettings("openai-responses", null, null, null));
            assertThat(checked.get().providerId()).isEqualTo("openai-responses");
            assertThat(checked.get().apiKey()).isEqualTo("sk-new-key");


            assertThat(service.require(owner("org-a", "cluster-a")).apiKey()).isEqualTo("sk-new-key");

            assertThatThrownBy(() -> controller.check(workspaceA,
                    new AiProviderSettings("openai-compatible", "https://api.deepseek.com/v1", "model-b", null)))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> {
                                assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                                assertThat(failure.getMessage()).doesNotContain("sk-new-key", "sk-old-key");
                            });
            assertThat(checked.get().baseUrl()).isEqualTo("https://api.openai.com/v1");
            assertThatThrownBy(() -> controller.put(workspaceA,
                    new AiProviderSettings("openai-compatible", "http://127.0.0.1/v1", "model-c", "sk-bad-key")))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
            assertThat(auditRecords.toString()).doesNotContain("sk-old-key", "sk-new-key", "sk-bad-key");
            assertThat(metadata.toString()).doesNotContain("sk-old-key", "sk-new-key");
            controller.delete(workspaceA);
            assertThat(service.get(owner("org-a", "cluster-a"))).isEmpty();
            assertThat(controller.get(workspaceA).getBody()).containsEntry("configured", false)
                    .doesNotContainKey("apiKey");
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"", " , , "})
    void rejectsManagedCloudStartupWithoutProviderManagers(String managers) {
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("mode", AuthenticationMode.CLOUD);
        var client = org.mockito.Mockito.mock(AiProviderClient.class);
        beans.registerSingleton("managed", new ManagedAiProvider(new AiProviderSettings("bedrock-managed",
                "https://bedrock-runtime.ap-southeast-1.amazonaws.com/openai/v1", "global.moonshotai.kimi-k3", null),
                client));
        assertThatThrownBy(() -> new AiProviderController(null, null,
                beans.getBeanProvider(AuthenticationMode.class), client, null,
                beans.getBeanProvider(ManagedAiProvider.class), managers))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("TAPSTATE_AI_BEDROCK_PROVIDER_MANAGERS");
    }

    @Test void managedDefaultAndExplicitSwitchKeepTheWorkspaceKeyAndRejectUnauthorizedChanges() {
        var values = new HashMap<String, AiProviderSettingsStore.Settings>();
        AiProviderSettingsStore store = new AiProviderSettingsStore() {
            public Optional<Settings> get(String owner) { return Optional.ofNullable(values.get(owner)); }
            public void put(String owner, Settings settings) { values.put(owner, settings); }
            public void delete(String owner) { values.remove(owner); }
        };
        var service = new AiProviderService(store, new AuditGate(record -> { }, Clock.systemUTC()));
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("mode", AuthenticationMode.CLOUD);
        beans.registerSingleton("service", service);
        var byok = org.mockito.Mockito.mock(AiProviderClient.class);
        var managed = org.mockito.Mockito.mock(AiProviderClient.class);
        var route = new ManagedAiProvider(new AiProviderSettings("bedrock-managed",
                "https://bedrock-runtime.ap-southeast-1.amazonaws.com/openai/v1", "global.moonshotai.kimi-k3", null), managed);
        beans.registerSingleton("managed", route);
        var requestedOwner = new AtomicReference<>("workspace-a");
        class Controller extends AiProviderController {
            Controller(boolean enabled) {
                super(beans.getBeanProvider(AiProviderService.class), null,
                        beans.getBeanProvider(AuthenticationMode.class), byok, null,
                        enabled ? beans.getBeanProvider(ManagedAiProvider.class) : null, "manager");
            }
            @Override Workspace workspace(jakarta.servlet.http.HttpServletRequest request) {
                return new Workspace(requestedOwner.get(), () -> { });
            }
        }
        var controller = new Controller(true);
        var request = new MockHttpServletRequest();
        var managedSelection = new AiProviderController.Selection("managed");
        var savedSelection = new AiProviderController.Selection("byok");
        try {
            SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(
                    TapstatePrincipal.cloudSession(new VerifiedToken("member", Scope.WRITE))));
            assertThat(controller.get(request).getBody()).containsEntry("mode", "managed")
                    .containsEntry("configured", true).containsEntry("canManage", false);
            assertThat(controller.assistantRoute("workspace-a", byok).client()).isSameAs(managed);
            assertThatThrownBy(() -> controller.select(request, savedSelection))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.FORBIDDEN));

            SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(
                    TapstatePrincipal.cloudSession(new VerifiedToken("manager", Scope.WRITE))));
            controller.put(request, new AiProviderSettings("openai-compatible", null, "saved-model", "sk-workspace-a"));
            assertThat(controller.assistantRoute("workspace-a", byok).settings().apiKey()).isEqualTo("sk-workspace-a");
            controller.select(request, managedSelection);
            assertThat(controller.assistantRoute("workspace-a", byok).settings().apiKey()).isNull();
            var probe = new AiProviderSettings("openai-compatible", null, "saved-model", null);
            SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(
                    TapstatePrincipal.cloudSession(new VerifiedToken("member", Scope.WRITE))));
            org.assertj.core.api.SoftAssertions.assertSoftly(softly -> {
                softly.assertThatThrownBy(() -> controller.check(request, probe))
                        .isInstanceOfSatisfying(TapstateException.class, error ->
                                assertThat(error.code()).isEqualTo(ControlError.FORBIDDEN));
                softly.assertThatThrownBy(() -> controller.models(request, probe))
                        .isInstanceOfSatisfying(TapstateException.class, error ->
                                assertThat(error.code()).isEqualTo(ControlError.FORBIDDEN));
            });
            assertThat(controller.check(request,
                    new AiProviderSettings("openai-compatible", null, "probe-model", "ephemeral-test-key")))
                    .isNotNull();
            SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(
                    TapstatePrincipal.cloudSession(new VerifiedToken("manager", Scope.WRITE))));
            assertThat(controller.check(request, probe)).isNotNull();
            assertThat(controller.models(request, probe)).isNotNull();
            assertThat(controller.get(request).getBody()).containsEntry("byokConfigured", true)
                    .containsEntry("mode", "managed").doesNotContainKey("apiKey");
            assertThat(controller.get(request).getBody().toString()).doesNotContain("sk-workspace-a");
            assertThatThrownBy(() -> new Controller(false).assistantRoute("workspace-a", byok))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_NOT_CONFIGURED));
            requestedOwner.set("workspace-b");
            assertThatThrownBy(() -> controller.select(request, savedSelection))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_NOT_CONFIGURED));
            requestedOwner.set("workspace-a");
            new Controller(false).select(request, savedSelection);
            assertThat(controller.assistantRoute("workspace-a", byok).client()).isSameAs(byok);
            assertThat(controller.assistantRoute("workspace-a", byok).settings().apiKey()).isEqualTo("sk-workspace-a");
            assertThat(controller.assistantRoute("workspace-b", byok).client()).isSameAs(managed);
        } finally { SecurityContextHolder.clearContext(); }
    }

    @Test void onPremUsesAuthenticatedSharedProviderSettings() {
        var values = new HashMap<String, AiProviderSettingsStore.Settings>();
        AiProviderSettingsStore store = new AiProviderSettingsStore() {
            public Optional<Settings> get(String owner) { return Optional.ofNullable(values.get(owner)); }
            public void put(String owner, Settings settings) { values.put(owner, settings); }
            public void delete(String owner) { values.remove(owner); }
        };
        var service = new AiProviderService(store, new AuditGate(record -> {}, Clock.fixed(NOW, ZoneOffset.UTC)));
        var beans = new DefaultListableBeanFactory();
        beans.registerSingleton("mode", AuthenticationMode.ON_PREM);
        beans.registerSingleton("service", service);
        var controller = new AiProviderController(beans.getBeanProvider(AiProviderService.class),
                beans.getBeanProvider(CloudAuthenticationService.class), beans.getBeanProvider(AuthenticationMode.class), null);
        var request = new MockHttpServletRequest();
        assertThatThrownBy(() -> controller.get(request)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        var token = new VerifiedToken("local-user", Scope.WRITE);
        SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(
                TapstatePrincipal.humanJwt(token), new CredentialAuthentication(token, Optional.empty(), Optional.empty(), () -> {})));
        try {
            controller.put(request, new AiProviderSettings("openai-compatible", null, "model", "private-key"));
            assertThat(controller.get(request).getBody()).containsEntry("configured", true).doesNotContainKey("apiKey");
            assertThat(values).containsKey("[\"on-prem\"]");
            controller.delete(request);
            assertThat(controller.get(request).getBody()).containsEntry("configured", false);
        } finally { SecurityContextHolder.clearContext(); }
    }

    private static MockHttpServletRequest request(String jwtId, String secret) {
        String encodedId = Base64.getUrlEncoder().withoutPadding().encodeToString(jwtId.getBytes(StandardCharsets.UTF_8));
        var request = new MockHttpServletRequest("GET", "/ai-provider-settings");
        request.setCookies(new Cookie(CloudSessionCookies.NAME, "tcs_" + encodedId + "." + secret));
        return request;
    }

    private static String owner(String organization, String cluster) {
        return io.tapstate.core.common.JsonWriter.write(List.of("cloud", organization, cluster));
    }

    private static final class MemoryCloudSessions implements CloudSessionStore {
        private final Map<String, CloudSessionRecord> records = new HashMap<>();

        void add(String jwtId, String secret, String user, String organization, String cluster) {
            var context = new CloudSessionContext(organization, cluster, organization, cluster, "test");
            records.put(jwtId, new CloudSessionRecord(DEPLOYMENT, jwtId, "hash-" + secret, user, "WRITE", false,
                    NOW, NOW, NOW.plusSeconds(600), context));
        }

        @Override public boolean create(CloudSessionRecord record) {
            return records.putIfAbsent(record.jwtId(), record) == null;
        }
        @Override public Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jwtId) {
            return Optional.ofNullable(records.get(jwtId));
        }
        @Override public Optional<CloudSessionRecord> authenticate(CloudSessionIdentity identity, String jwtId,
                String hash, Instant now, Instant idleExpiresAt) {
            CloudSessionRecord record = records.get(jwtId);
            if (record == null || !record.secretHash().equals(hash) || !record.idleExpiresAt().isAfter(now)) return Optional.empty();
            var touched = new CloudSessionRecord(record.identity(), record.jwtId(), record.secretHash(), record.userId(),
                    record.scope(), record.revoked(), record.createdAt(), now, idleExpiresAt, record.clusterContext());
            records.put(jwtId, touched);
            return Optional.of(touched);
        }
        @Override public boolean logout(CloudSessionIdentity identity, String jwtId, String hash, Instant now) { return false; }
        @Override public void invalidate(CloudSessionIdentity identity, String jwtId, Instant now) { records.remove(jwtId); }
    }
}
