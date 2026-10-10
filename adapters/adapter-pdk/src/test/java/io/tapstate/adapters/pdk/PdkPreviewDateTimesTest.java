package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapdata.entity.schema.value.DateTime;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PdkPreviewDateTimesTest {

    @Test
    void identifiesAndRoundTripsPdkDateTimeValuesAsIsoInstants() {
        Instant instant = Instant.parse("2026-10-04T00:00:00.123Z");
        DateTime value = new DateTime(instant);

        assertThat(PdkPreviewDateTimes.isDateTime(value)).isTrue();
        assertThat(PdkPreviewDateTimes.isDateTime(instant)).isFalse();
        assertThat(PdkPreviewDateTimes.toIsoString(value)).isEqualTo(instant.toString());
        assertThat(PdkPreviewDateTimes.fromIsoString(instant.toString())).isEqualTo(value);
    }
}
