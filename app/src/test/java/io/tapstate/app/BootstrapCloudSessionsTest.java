package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

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

    @Test void handoffPreservesTheOriginalRecordAndAllDenialsWithoutExtendingExpiry() {
        var destination = mock(CloudSessionStore.class);
        var live = record("live");
        try (var sessions = new BootstrapCloudSessions(ID, () -> {})) {
            sessions.create(live);
            sessions.create(record("logged-out"));
            sessions.logout(ID, "logged-out", "hash", NOW);
            sessions.invalidate(ID, "before-login", NOW);
            sessions.create(new CloudSessionRecord(ID, "expired", "hash", "user", "WRITE", false,
                    NOW.minusSeconds(120), NOW.minusSeconds(120), NOW));
            sessions.handoffTo(destination, NOW);
            verify(destination).invalidate(ID, "logged-out", NOW);
            verify(destination).invalidate(ID, "before-login", NOW);
            verify(destination).create(live);
            verifyNoMoreInteractions(destination);
            assertThatThrownBy(() -> sessions.find(ID, "live")).isInstanceOf(TapstateException.class);
            sessions.handoffTo(destination, NOW.plusSeconds(1));
            verifyNoMoreInteractions(destination);
        }
    }

    @Test void handoffNeverReplacesAnExistingSession() {
        var destination = mock(CloudSessionStore.class);
        when(destination.create(any())).thenReturn(false);
        try (var sessions = new BootstrapCloudSessions(ID, () -> {})) {
            sessions.create(record("existing"));
            sessions.handoffTo(destination, NOW);
            verify(destination).create(record("existing"));
            verifyNoMoreInteractions(destination);
        }
    }

    @Test void aFailedHandoffRetainsItsOriginalFactsForTheNextAttempt() {
        var destination = mock(CloudSessionStore.class);
        when(destination.create(any())).thenThrow(new IllegalStateException("controlled write failure")).thenReturn(true);
        try (var sessions = new BootstrapCloudSessions(ID, () -> {})) {
            sessions.create(record("live"));
            sessions.invalidate(ID, "before-login", NOW);
            assertThatThrownBy(() -> sessions.handoffTo(destination, NOW)).isInstanceOf(IllegalStateException.class);
            assertThat(sessions.find(ID, "live")).contains(record("live"));
            sessions.handoffTo(destination, NOW);
            verify(destination, times(2)).invalidate(ID, "before-login", NOW);
            verify(destination, times(2)).create(record("live"));
            assertThatThrownBy(() -> sessions.find(ID, "live")).isInstanceOf(TapstateException.class);
        }
    }
}
