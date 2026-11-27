package io.tapstate.app;

import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ManagedStorageInspectionTest {
    @Test void unavailableDomainsDoNotReportHealthyAndOnlyTheSelectedMemberScans() {
        var selected = new AtomicBoolean(false);
        var recorded = new ArrayList<ManagedStorageInspection.Observation>();
        var now = Instant.parse("2026-11-18T09:00:00Z");
        try (var worker = new ManagedStorageInspection(Clock.fixed(now, ZoneOffset.UTC), selected::get,
                List.of(new ManagedStorageInspection.Domain(SystemCollections.Database.CONFIG, budget -> {
                            throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "inspection unavailable"), null);
                        }),
                        new ManagedStorageInspection.Domain(SystemCollections.Database.CACHE, budget -> List.of())),
                recorded::add)) {
            worker.runOnce();
            assertThat(recorded).isEmpty();
            selected.set(true);
            worker.runOnce();
            assertThat(recorded).containsExactly(
                    new ManagedStorageInspection.Observation(SystemCollections.Database.CONFIG, now, List.of(), false),
                    new ManagedStorageInspection.Observation(SystemCollections.Database.CACHE, now, List.of(), true));
        }
    }
}
