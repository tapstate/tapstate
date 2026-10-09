package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TelemetryMongoIdentityWitnessTest {
    @Test
    void theFirstResourceHasNoFormerEventIdentityToExclude() {
        TelemetryMongoIdentityWitness.requireNoFormerEventIds(List.of("current-start"), Set.of());
    }

    @Test
    void currentEventsRemainReadableWhileDifferentFormerEventsAreRetained() {
        TelemetryMongoIdentityWitness.requireNoFormerEventIds(
                List.of("current-start", "current-recovery"), Set.of("former-start", "former-stop"));
    }

    @Test
    void oneFormerEventAmongCurrentEventsIsStillRefused() {
        assertThatThrownBy(() -> TelemetryMongoIdentityWitness.requireNoFormerEventIds(
                List.of("current-start", "former-stop", "current-recovery"),
                Set.of("former-start", "former-stop")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("new-resource events exclude all retained former-resource event identities");
    }
}
