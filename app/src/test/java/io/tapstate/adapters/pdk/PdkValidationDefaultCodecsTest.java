package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Exercises the real default codec registry on the assembly root's existing PDK runtime classpath. */
class PdkValidationDefaultCodecsTest {
    private record DriverValue(String value) {}

    @Test void actualDefaultRegistryConvertsDateButDoesNotMakeUnknownRawValuesComparable() {
        var selector = new PdkValidationCodecs("connector", "binding-v1", new TapCodecsRegistry());
        Instant instant = Instant.parse("2026-10-21T04:00:00.123Z");
        var selected = selector.select("stamp", Date.from(instant), null);
        assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.DEFAULT);
        assertThat(((DateTime) ((TapValue<?, ?>) selected.value()).getValue()).toInstant()).isEqualTo(instant);
        assertThatThrownBy(() -> selector.select("unknown", new DriverValue("value"), null))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
    }
}
