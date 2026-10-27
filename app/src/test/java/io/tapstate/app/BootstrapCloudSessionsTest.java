package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BootstrapCloudSessionsTest {
    private static final CloudSessionIdentity ID = new CloudSessionIdentity("https://cloud.example", "deployment", "cluster");
    private static final Instant NOW = Instant.parse("2026-10-20T12:00:00Z");
    private static CloudSessionRecord record(String jwt) {
        return new CloudSessionRecord(ID, jwt, "hash", "user", "WRITE", false, NOW, NOW, NOW.plusSeconds(60));
    }
    @Test void revocationBeforeCreationRemainsEffectiveAndCannotBeReplacedByAnotherSession() {
        try (var sessions = new BootstrapCloudSessions(ID, () -> {})) {
            sessions.invalidate(ID, "revoked", NOW);
            assertThat(sessions.create(record("revoked"))).isFalse();
            assertThat(sessions.create(record("live"))).isTrue();
            assertThat(sessions.create(record("live"))).isFalse();
            assertThat(sessions.authenticate(ID, "live", "hash", NOW, NOW.plusSeconds(120))).isPresent();
            sessions.invalidate(ID, "live", NOW);
            assertThat(sessions.authenticate(ID, "live", "hash", NOW, NOW.plusSeconds(180))).isEmpty();
            assertThat(sessions.create(record("live"))).isFalse();
        }
    }
    @Test void closingSetupRejectsLateCreatesAuthenticationsAndCallbacksWithoutReopening() {
        var sessions = new BootstrapCloudSessions(ID, () -> {});
        assertThat(sessions.create(record("live"))).isTrue(); sessions.close();
        assertThatThrownBy(() -> sessions.create(record("late"))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> sessions.authenticate(ID, "live", "hash", NOW, NOW.plusSeconds(120))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> sessions.invalidate(ID, "late", NOW)).isInstanceOf(TapstateException.class);
        sessions.close();
    }
}
