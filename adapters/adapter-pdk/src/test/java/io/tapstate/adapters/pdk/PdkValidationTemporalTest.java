package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.adapters.pdk.PdkValidationTemporal.Kind.*;
import static io.tapstate.adapters.pdk.PdkValidationTemporal.LegacyEncoding.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkValidationTemporalTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(4096, 50, 8);
    private record DriverTime(String source) {}

    @Test void declaredEpochSecondsRetainNanosecondsAndDoNotCallASecondCodec() {
        var registry = new TapCodecsRegistry();
        var calls = new AtomicInteger();
        Instant expected = Instant.parse("2026-10-21T01:02:03.123456789Z");
        registry.registerToTapValue(DriverTime.class, (value, model) -> {
            calls.incrementAndGet(); return new TapDateTimeValue(new DateTime(expected));
        });
        var decoder = decoder(registry);
        Object actual = decode(decoder, new DriverTime("driver"), INSTANT, EPOCH_SECONDS);
        assertThat(actual).isEqualTo(expected);
        assertThat(calls).hasValue(1);
        assertThat(ValidationValueComparison.compare(row(actual), row(Instant.ofEpochMilli(expected.toEpochMilli())))).isPresent();
    }

    @Test void selfDescribingOffsetValuesCompareByInstantWithoutUsingTheDefaultZone() {
        var decoder = decoder(new TapCodecsRegistry());
        var first = OffsetDateTime.parse("2026-10-21T09:02:03.123456789+08:00");
        var second = first.withOffsetSameInstant(ZoneOffset.ofHours(-4));
        assertThat(decode(decoder, first, INSTANT, null)).isEqualTo(first.toInstant());
        assertThat(decode(decoder, second.toZonedDateTime(), INSTANT, null)).isEqualTo(first.toInstant());
        assertThat(ValidationValueComparison.compare(row(decode(decoder, first, INSTANT, null)),
                row(decode(decoder, second, INSTANT, null)))).isEmpty();
    }

    @Test void explicitWallEncodingKeepsAClockGapAsWallFieldsAcrossJvmDefaultZones() {
        var decoder = decoder(new TapCodecsRegistry());
        var wall = LocalDateTime.parse("2026-03-08T02:30:00.123456789");
        var legacy = new TapDateTimeValue(new DateTime(wall));
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : List.of("UTC", "America/New_York", "Asia/Shanghai")) {
                TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of(zone)));
                assertThat(decode(decoder, legacy, LOCAL_DATE_TIME, UTC_WALL_FIELDS)).isEqualTo(wall);
            }
        } finally { TimeZone.setDefault(original); }
        assertUnavailable(() -> decode(decoder, legacy, INSTANT, UTC_WALL_FIELDS));
        assertUnavailable(() -> decode(decoder, legacy, LOCAL_DATE_TIME, EPOCH_SECONDS));
        assertUnavailable(() -> decode(decoder, legacy, LOCAL_DATE_TIME, null));
    }

    @Test void dateTimeAndInstantDomainsAreNeverInferredFromEachOtherOrFromText() {
        var decoder = decoder(new TapCodecsRegistry());
        var date = LocalDate.of(2026, 10, 21);
        var time = LocalTime.of(1, 2, 3, 987654321);
        assertThat(decode(decoder, date, DATE, null)).isSameAs(date);
        assertThat(decode(decoder, time, TIME, null)).isSameAs(time);
        assertUnavailable(() -> decode(decoder, date, INSTANT, null));
        assertUnavailable(() -> decode(decoder, date.atStartOfDay(), DATE, null));
        assertUnavailable(() -> decode(decoder, "2026-10-21T00:00:00Z", INSTANT, EPOCH_SECONDS));
        assertUnavailable(() -> decode(decoder, new TapStringValue("2026-10-21"), DATE, UTC_WALL_FIELDS));
        assertUnavailable(() -> decode(decoder, new TapDateTimeValue(new DateTime(date.atStartOfDay())), DATE, UTC_WALL_FIELDS));
    }

    @Test void dateAndTimeWrappersCannotSilentlyDiscardExtraComponents() {
        var decoder = decoder(new TapCodecsRegistry());
        var date = LocalDate.of(2026, 10, 21);
        assertThat(decode(decoder, new TapDateValue(new DateTime(date.atStartOfDay())), DATE, UTC_WALL_FIELDS)).isEqualTo(date);
        var time = LocalTime.of(1, 2, 3, 123456789);
        assertThat(decode(decoder, new TapTimeValue(new DateTime(LocalDate.ofEpochDay(0).atTime(time))), TIME, UTC_WALL_FIELDS))
                .isEqualTo(time);
        assertUnavailable(() -> decode(decoder, new TapDateValue(new DateTime(date.atTime(12, 0))), DATE, UTC_WALL_FIELDS));
        assertUnavailable(() -> decode(decoder, new TapTimeValue(new DateTime(LocalDate.ofEpochDay(1).atTime(time))), TIME, UTC_WALL_FIELDS));
    }

    @Test void nullAndAlreadyPortableValuesKeepBindingAndLogicalDomain() {
        var decoder = decoder(new TapCodecsRegistry());
        assertThat(decode(decoder, null, INSTANT, null)).isNull();
        assertThat(decode(decoder, new TapDateTimeValue(null), INSTANT, null)).isNull();
        Instant value = Instant.parse("2026-10-21T00:00:00Z");
        assertThat(decode(decoder, new ValidationValues.Typed("instant", "timestamp", "binding", value), INSTANT, null))
                .isSameAs(value);
        assertUnavailable(() -> decode(decoder, new ValidationValues.Typed("date", "timestamp", "binding", value), INSTANT, null));
        assertUnavailable(() -> decode(decoder, new ValidationValues.Typed("instant", "timestamp", "stale", value), INSTANT, null));
    }

    @Test void malformedLegacyFieldsCannotNormalizeIntoAnotherValidInstant() {
        var decoder = decoder(new TapCodecsRegistry());
        for (Integer nanos : new Integer[] { null, -1, 1_000_000_000 }) {
            var value = new DateTime(Instant.EPOCH); value.setNano(nanos);
            assertUnavailable(() -> decode(decoder, new TapDateTimeValue(value), INSTANT, EPOCH_SECONDS));
        }
        var missing = new DateTime(Instant.EPOCH); missing.setSeconds(null);
        assertUnavailable(() -> decode(decoder, new TapDateTimeValue(missing), INSTANT, EPOCH_SECONDS));
        var outside = new DateTime(Instant.EPOCH); outside.setSeconds(Long.MAX_VALUE);
        assertUnavailable(() -> decode(decoder, new TapDateTimeValue(outside), INSTANT, EPOCH_SECONDS));
    }

    @Test void getterFailuresPreserveTheirCauseAndFatalErrorsStayBare() {
        var decoder = decoder(new TapCodecsRegistry());
        var failure = new IllegalStateException("temporal payload failed");
        var broken = new DateTime(Instant.EPOCH) { @Override public Long getSeconds() { throw failure; } };
        assertThatThrownBy(() -> decode(decoder, new TapDateTimeValue(broken), INSTANT, EPOCH_SECONDS))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ValidationError.CODEC_FAILED);
                    assertThat(error.getCause()).isSameAs(failure);
                });
        var fatal = new AssertionError("fatal temporal payload");
        var fatalValue = new DateTime(Instant.EPOCH) { @Override public Integer getNano() { throw fatal; } };
        assertThatThrownBy(() -> decode(decoder, new TapDateTimeValue(fatalValue), INSTANT, EPOCH_SECONDS)).isSameAs(fatal);
    }

    private static PdkValidationCodecs decoder(TapCodecsRegistry registry) { return new PdkValidationCodecs("fixture", "binding", registry); }
    private static Object decode(PdkValidationCodecs decoder, Object value, PdkValidationTemporal.Kind kind,
            PdkValidationTemporal.LegacyEncoding encoding) {
        return decoder.temporal("time", decoder.select("time", value, new TapDateTime()), kind, encoding);
    }
    private static void assertUnavailable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
    }
    private static ValidationValues.Row row(Object value) { return ValidationValues.ownRow(Map.of("time", value), LIMITS); }
}
