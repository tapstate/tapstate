package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

/** A killed diagnostic process cannot turn an interrupted legacy migration into native initialization. */
@RequiresDocker
class MigrationSurvivesAKillIT {
    @Test
    void anInterruptedLegacyStoreRemainsUnchangedAfterAKilledProcessAndItsReplacement() throws Exception {
        try (var store = new LegacyStoreRefusal(2_000, true)) {
            try (var first = store.launch()) {
                store.requireRefused(first);
                first.kill();
                assertThat(first.isAlive()).isFalse();
                store.requireUnchanged();
            }
            try (var replacement = store.launch()) {
                store.requireRefused(replacement);
            }
            store.requireUnchanged();
        }
    }
}
