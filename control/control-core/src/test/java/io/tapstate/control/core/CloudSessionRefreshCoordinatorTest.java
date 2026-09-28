package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class CloudSessionRefreshCoordinatorTest {

    private static final Instant NOW = Instant.parse("2026-09-28T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void aCredentialOutsideTheRefreshWindowMakesNoCloudCall() {
        List<String> calls = new ArrayList<>();
        CloudUserToken token = token("jwt-a", NOW.plusSeconds(600));
        CloudSessionRefreshCoordinator coordinator = new CloudSessionRefreshCoordinator(
                current -> {
                    calls.add("refresh");
                    return Optional.empty();
                }, calls::add, CLOCK, Duration.ofMinutes(1));

        assertThat(coordinator.current("session-a", token)).containsSame(token);
        assertThat(calls).isEmpty();
    }

    @Test
    void aCredentialInsideTheWindowIsReplacedOnlyByTheSameIdentity() {
        List<String> invalidated = new ArrayList<>();
        CloudUserToken current = token("jwt-a", NOW.plusSeconds(30));
        CloudUserToken refreshed = token("jwt-b", NOW.plusSeconds(900));
        CloudSessionRefreshCoordinator coordinator = new CloudSessionRefreshCoordinator(
                token -> Optional.of(refreshed), invalidated::add, CLOCK, Duration.ofMinutes(1));

        assertThat(coordinator.current("session-a", current)).containsSame(refreshed);
        assertThat(invalidated).isEmpty();
    }

    @Test
    void aRefreshRefusalInvalidatesTheClusterSession() {
        List<String> invalidated = new ArrayList<>();
        CloudSessionRefreshCoordinator coordinator = new CloudSessionRefreshCoordinator(
                current -> Optional.empty(), invalidated::add, CLOCK, Duration.ofMinutes(1));

        assertThat(coordinator.current("session-a", token("jwt-a", NOW.minusSeconds(1)))).isEmpty();
        assertThat(invalidated).containsExactly("session-a");
    }

    @Test
    void aRefreshFailureInvalidatesTheClusterSession() {
        List<String> invalidated = new ArrayList<>();
        CloudSessionRefreshCoordinator coordinator = new CloudSessionRefreshCoordinator(
                current -> {
                    throw new IllegalStateException("provider unavailable");
                }, invalidated::add, CLOCK, Duration.ofMinutes(1));

        assertThat(coordinator.current("session-a", token("jwt-a", NOW.plusSeconds(30)))).isEmpty();
        assertThat(invalidated).containsExactly("session-a");
    }

    @Test
    void aRefreshThatMovesTheSessionToAnotherUserIsRefused() {
        List<String> invalidated = new ArrayList<>();
        CloudUserToken other = new CloudUserToken(
                "jwt-b", "other-user", "org-a", "cluster-a", NOW.plusSeconds(900));
        CloudSessionRefreshCoordinator coordinator = new CloudSessionRefreshCoordinator(
                current -> Optional.of(other), invalidated::add, CLOCK, Duration.ofMinutes(1));

        assertThat(coordinator.current("session-a", token("jwt-a", NOW.plusSeconds(30)))).isEmpty();
        assertThat(invalidated).containsExactly("session-a");
    }

    @Test
    void tokenDiagnosticsNeverRenderBearerBytes() {
        assertThat(token("secret-jwt", NOW.plusSeconds(30)).toString())
                .doesNotContain("secret-jwt")
                .contains("bearer=<redacted>");
    }

    private static CloudUserToken token(String bearer, Instant expiresAt) {
        return new CloudUserToken(bearer, "user-a", "org-a", "cluster-a", expiresAt);
    }
}
