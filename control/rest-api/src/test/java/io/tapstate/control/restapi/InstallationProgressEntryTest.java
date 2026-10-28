package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import static org.assertj.core.api.Assertions.assertThat;

class InstallationProgressEntryTest {
    @Test void localProgressDoesNotWaitForStorageStartupOrGrantCommands() {
        var reads = new AtomicInteger(); var guards = new AtomicInteger();
        StorageEntryStatus state = () -> { reads.incrementAndGet(); throw new AssertionError("startup is not ready"); };
        var factory = new StaticListableBeanFactory(Map.of("state", state, "installation", access(true, guards)));
        var body = new StorageEntryController(factory.getBeanProvider(StorageEntryStatus.class),
                factory.getBeanProvider(InstallationHttpAccess.class)).entry(new MockHttpServletRequest()).getBody();
        assertThat(reads.get()).isZero(); assertThat(guards.get()).isEqualTo(1);
        assertThat(body).containsEntry("management", true).containsEntry("entry", "UNAVAILABLE")
                .containsEntry("initializationProgress", Map.of("state", "RUNNING"));
        assertThat(body).doesNotContainKeys("allowedActions", "capability");
    }
    @Test void thePublicListenerNeverDisclosesInstallationProgress() {
        StorageEntryStatus state = () -> new StorageEntryStatus.Summary("OP", StorageEntryStatus.Entry.LOGIN);
        var factory = new StaticListableBeanFactory(Map.of("state", state, "installation", access(false, new AtomicInteger())));
        var body = new StorageEntryController(factory.getBeanProvider(StorageEntryStatus.class),
                factory.getBeanProvider(InstallationHttpAccess.class)).entry(new MockHttpServletRequest()).getBody();
        assertThat(body).containsEntry("management", false).doesNotContainKey("initializationProgress");
    }
    private InstallationHttpAccess access(boolean management, AtomicInteger guards) {
        return new InstallationHttpAccess() {
            public boolean isManagementRequest(HttpServletRequest request) { return management; }
            public void requireManagementRequest(HttpServletRequest request) { guards.incrementAndGet(); }
            public Map<String,Object> initializationProgress() {
                if (!management) throw new AssertionError("public listener must not read progress");
                return Map.of("state", "RUNNING");
            }
            public io.tapstate.control.core.StorageControlAccess caller(HttpServletRequest request) { throw new AssertionError("no command authority"); }
            public void createAdministrator(HttpServletRequest request, String username, String password) { throw new AssertionError("no administrator writes"); }
        };
    }
}
