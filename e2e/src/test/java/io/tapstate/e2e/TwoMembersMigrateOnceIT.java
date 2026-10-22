package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

/** Concurrent native starts both refuse a legacy store without migrating or initializing it. */
@RequiresDocker
class TwoMembersMigrateOnceIT {
    @Test
    void concurrentMembersCannotMigrateOrInitializeTheSameLegacyStore() throws Exception {
        try (var store = new LegacyStoreRefusal(12, false)) {
            // Both processes exist before either startup is awaited; partial startup failure still closes both.
            try (var first = store.launch(); var second = store.launch()) {
                store.requireRefused(first);
                store.requireRefused(second);
            }
            store.requireUnchanged();
        }
    }
}
