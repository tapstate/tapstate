package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.control.core.RetrySettingsService.PolicyBody;
import io.tapstate.control.core.RetrySettingsService.SettingsBody;
import io.tapstate.core.common.RetryPolicy;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.RetrySettingsStore;
import io.tapstate.spi.store.RetrySettingsStore.OperationOverride;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RetrySettingsServiceTest {
    private static final PolicyBody GLOBAL = new PolicyBody(true, 60_000L, 28_800_000L);
    private final List<AuditRecord> audits = new ArrayList<>();
    private final MemoryStore store = new MemoryStore();
    private final RetrySettingsService service = new RetrySettingsService(store, new AuditGate(audits::add, Clock.systemUTC()));

    private final class MemoryStore implements RetrySettingsStore {
        private Settings current = Settings.unconfigured();
        private int writes;

        @Override
        public Settings read() {
            return current;
        }

        @Override
        public Optional<Settings> replace(long revision, RetryPolicy policy, Map<String, OperationOverride> operations) {
            assertThat(audits).hasSize(++writes);
            if (revision != current.revision()) return Optional.empty();
            current = new Settings(revision + 1, policy, operations);
            return Optional.of(current);
        }
    }

    @Test
    void replacingSettingsAuditsTheOriginalActorAndReturnsOverridesWithInheritance() {
        assertThat(service.get()).isEqualTo(new SettingsBody(0L, GLOBAL, Map.of()));
        var policy = new RetryPolicy(true, Duration.ofSeconds(60), Duration.ofHours(8));
        var overrides = Map.of("snapshot-read", new OperationOverride(false, null, Duration.ofHours(4)),
                "stream-read", new OperationOverride(null, Duration.ofSeconds(10), null));
        var expected = Map.of("snapshot-read", new PolicyBody(false, null, 14_400_000L),
                "stream-read", new PolicyBody(null, 10_000L, null));
        assertThat(service.replace("admin", new SettingsBody(0L, GLOBAL, expected)))
                .isEqualTo(new SettingsBody(1L, GLOBAL, expected));
        assertThat(audits).singleElement().satisfies(record -> {
            assertThat(record.principal()).isEqualTo("admin");
            assertThat(record.operationId()).isEqualTo("retry-settings.replace");
        });
        assertThat(store.current).isEqualTo(new RetrySettingsStore.Settings(1, policy, overrides));
    }

    @Test
    void malformedSettingsNeverReachTheAuditOrStore() {
        var invalid = new SettingsBody[]{
            null,
            new SettingsBody(null, GLOBAL, Map.of()),
            new SettingsBody(-1L, GLOBAL, Map.of()),
            new SettingsBody(Long.MAX_VALUE, GLOBAL, Map.of()),
            new SettingsBody(0L, null, Map.of()),
            new SettingsBody(0L, new PolicyBody(null, 100L, 100L), Map.of()),
            new SettingsBody(0L, new PolicyBody(true, null, 100L), Map.of()),
            new SettingsBody(0L, new PolicyBody(true, 100L, null), Map.of()),
            new SettingsBody(0L, new PolicyBody(true, 0L, 100L), Map.of()),
            new SettingsBody(0L, GLOBAL, java.util.Collections.singletonMap("snapshot-read", null)),
            new SettingsBody(0L, GLOBAL, Map.of("unknown", new PolicyBody(true, null, null))),
        };
        for (var request : invalid) {
            assertThatThrownBy(() -> service.replace("admin", request)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        }
        assertThat(audits).isEmpty();
        assertThat(store.writes).isZero();
    }

    @Test
    void rejectedRevisionRemainsAnAuditedControlConflict() {
        store.current = new RetrySettingsStore.Settings(1, RetrySettingsStore.DEFAULT, Map.of());
        assertThatThrownBy(() -> service.replace("admin", new SettingsBody(0L, GLOBAL, null)))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.RETRY_SETTINGS_REVISION_CONFLICT));
        assertThat(audits).hasSize(1);
    }
}
