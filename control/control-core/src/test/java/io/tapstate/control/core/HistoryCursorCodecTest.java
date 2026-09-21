package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.RateHistoryStore.Key;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class HistoryCursorCodecTest {

    private static final byte[] SECRET = "history-test-secret".getBytes(StandardCharsets.UTF_8);
    private static final Instant T0 = Instant.parse("2026-09-21T10:00:00.123456789Z");
    private static final HistoryCursorCodec.QueryBinding BINDING = new HistoryCursorCodec.QueryBinding(
            "orders", T0.minus(Duration.ofHours(1)), T0.plus(Duration.ofHours(1)),
            HistoryResolution.PT5M, 240, List.of("items", "orders"));
    private static final Key KEY = new Key(T0.minusSeconds(30), "opaque-key");

    private static HistoryCursorCodec codecAt(Instant now) {
        return new HistoryCursorCodec(SECRET, Duration.ofMinutes(10), Clock.fixed(now, ZoneOffset.UTC));
    }

    @Test
    void signedClaimsRoundTripEveryFrozenBoundaryAndOpaqueKey() {
        HistoryCursorCodec codec = codecAt(T0);
        String token = codec.issue(BINDING, T0.minusSeconds(3600), T0,
                T0.minus(Duration.ofDays(15)), KEY, T0.minusSeconds(15));

        HistoryCursorCodec.State state = codec.read(token, BINDING);

        assertThat(state.binding()).isEqualTo(BINDING);
        assertThat(state.afterKey()).isEqualTo(KEY);
        assertThat(state.effectiveFrom()).isEqualTo(T0.minusSeconds(3600));
        assertThat(state.effectiveTo()).isEqualTo(T0);
        assertThat(state.retentionCutoff()).isEqualTo(T0.minus(Duration.ofDays(15)));
        assertThat(state.resumeAt()).isEqualTo(T0.minusSeconds(15));
        assertThat(state.expiresAt()).isEqualTo(T0.plus(Duration.ofMinutes(10)));
    }

    @Test
    void changingTheSignatureIsTampering() {
        HistoryCursorCodec codec = codecAt(T0);
        String token = codec.issue(BINDING, T0.minusSeconds(3600), T0,
                T0.minus(Duration.ofDays(15)), KEY, T0.minusSeconds(15));
        char replacement = token.endsWith("A") ? 'B' : 'A';
        String tampered = token.substring(0, token.length() - 1) + replacement;

        TapstateException refusal = catchThrowableOfType(() -> codec.read(tampered, BINDING),
                TapstateException.class);

        assertThat(refusal.code()).isEqualTo(MonitorError.INVALID_CURSOR);
        assertThat(refusal.args()).containsEntry("reason", "TAMPERED");
    }

    @Test
    void aCursorCannotBeReusedWithDifferentSelectors() {
        HistoryCursorCodec codec = codecAt(T0);
        String token = codec.issue(BINDING, T0.minusSeconds(3600), T0,
                T0.minus(Duration.ofDays(15)), KEY, T0.minusSeconds(15));
        HistoryCursorCodec.QueryBinding changed = new HistoryCursorCodec.QueryBinding(
                BINDING.pipelineId(), BINDING.from(), BINDING.to(), BINDING.resolution(), BINDING.limit(),
                List.of("orders"));

        TapstateException refusal = catchThrowableOfType(() -> codec.read(token, changed),
                TapstateException.class);

        assertThat(refusal.code()).isEqualTo(MonitorError.INVALID_CURSOR);
        assertThat(refusal.args()).containsEntry("reason", "QUERY_MISMATCH");
    }

    @Test
    void tenMinutesIsExpiredRatherThanAQueryMismatch() {
        String token = codecAt(T0).issue(BINDING, T0.minusSeconds(3600), T0,
                T0.minus(Duration.ofDays(15)), KEY, T0.minusSeconds(15));

        TapstateException refusal = catchThrowableOfType(
                () -> codecAt(T0.plus(Duration.ofMinutes(10))).read(token, BINDING), TapstateException.class);

        assertThat(refusal.code()).isEqualTo(MonitorError.CURSOR_EXPIRED);
    }
}
