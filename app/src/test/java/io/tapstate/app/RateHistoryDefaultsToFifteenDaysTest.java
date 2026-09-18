package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the history keeps when nothing is configured. The most common way a configuration property fails
 * is that it can be set and its default is not wired; the cases that shorten the retention to witness
 * expiry are green whatever the default is, so this one asks for the default alone.
 */
class RateHistoryDefaultsToFifteenDaysTest {

    @Test
    @DisplayName("with nothing configured a sample is kept fifteen days and taken once a minute")
    void theDefaultsAreFifteenDaysAndAMinute() {
        MetricsHistoryProperties history = new MetricsHistoryProperties();

        assertThat(history.getRetention()).isEqualTo(Duration.ofDays(15));
        assertThat(history.getSampleInterval()).isEqualTo(Duration.ofSeconds(60));
        // The store's own default, used where the port is built without these properties, is the same
        // number: two defaults that could drift would be two retentions nobody configured.
        assertThat(MongoRateHistoryStore.DEFAULT_RETENTION).isEqualTo(history.getRetention());
    }
}
