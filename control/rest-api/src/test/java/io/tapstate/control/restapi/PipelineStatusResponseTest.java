package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.messages.MessageCatalog;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How the status face answers "how current is this?". The projection reads now from the server's own clock,
 * because the caller's is its own: a client minutes out of step would otherwise call a stalled pipeline fresh.
 * When the projection carries no time, the face says so by omitting both fields rather than by substituting
 * anything — "nobody can say how old this is" is an answer, and the only honest one available.
 */
class PipelineStatusResponseTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-07-01T12:34:56.789Z");

    private final MessageCatalog catalog = MessageCatalog.bundled();

    @Test
    void carriesWhenTheObservationWasTakenAndHowLongAgoThatWas() {
        PipelineStatus status = new PipelineStatus("orders", PipelineState.RUNNING, null, OBSERVED_AT);

        PipelineStatusResponse response = PipelineStatusResponse.of(status, catalog,
                Clock.fixed(OBSERVED_AT.plusSeconds(90), ZoneOffset.UTC));

        assertThat(response.observedAt()).isEqualTo(OBSERVED_AT);
        // The age is asserted as a value, not as "present": an age measured from the wrong end, or from the
        // caller's clock, is present too - and reads as a healthy pipeline exactly when it is wrong.
        assertThat(response.observedAgeMillis()).isEqualTo(90_000L);
    }

    @Test
    void theAgeGrowsWhileNothingElseAboutThePipelineChanges() {
        PipelineStatus unchanged = new PipelineStatus("orders", PipelineState.RUNNING, null, OBSERVED_AT);

        PipelineStatusResponse earlier = PipelineStatusResponse.of(unchanged, catalog,
                Clock.fixed(OBSERVED_AT.plusSeconds(10), ZoneOffset.UTC));
        PipelineStatusResponse later = PipelineStatusResponse.of(unchanged, catalog,
                Clock.fixed(OBSERVED_AT.plusSeconds(310), ZoneOffset.UTC));

        // The same RUNNING state, read twice five minutes apart: this is what a stopped publisher looks like,
        // and the age is the only part of the answer that moves.
        assertThat(earlier.state()).isEqualTo(later.state());
        assertThat(later.observedAgeMillis() - earlier.observedAgeMillis()).isEqualTo(300_000L);
    }

    @Test
    void saysNothingAboutAgeWhenTheObservationTimeIsNotKnown() {
        PipelineStatus status = new PipelineStatus("orders", PipelineState.RUNNING);

        PipelineStatusResponse response = PipelineStatusResponse.of(status, catalog,
                Clock.fixed(OBSERVED_AT, ZoneOffset.UTC));

        // Both omitted together. An age computed against a missing observation time would be an age measured
        // from the epoch, which serializes as a number and reads as a real answer.
        assertThat(response.observedAt()).isNull();
        assertThat(response.observedAgeMillis()).isNull();
    }
}
