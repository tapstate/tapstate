package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.NestOrigin;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.NestStateWindow;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class NestStateReadingJsonTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();

    @Test
    void anOlderReadingKeepsItsLegacyValuesWithoutInventingAKeyShare() {
        NestStateReading reading = JSON.readValue("""
                {"entries":8,"accesses":100,"backfills":400,"backfillMillis":20,
                 "pendingHighWater":0,"stored":800,"origin":null}
                """, NestStateReading.class);
        assertThat(reading.accesses()).isEqualTo(100);
        assertThat(reading.backfills()).isEqualTo(400);
        assertThat(reading.stored()).hasValue(800);
        assertThat(reading.keyAccesses()).isEmpty();
        assertThat(reading.coldKeys()).isEmpty();
        assertThat(NestStateWindow.fromStart(reading).servedFromCold()).isEmpty();
    }

    @Test
    void keyCountersAndDefinitionOriginSurviveTheJsonRoundTrip() {
        NestStateReading reading = new NestStateReading(8, 100, 400, 20, OptionalLong.of(800))
                .withKeyCounts(800, 400).withOrigin(new NestOrigin("doc", "[]", "state"));
        assertThat(JSON.readValue(JSON.writeValueAsString(reading), NestStateReading.class))
                .isEqualTo(reading);
    }
}
