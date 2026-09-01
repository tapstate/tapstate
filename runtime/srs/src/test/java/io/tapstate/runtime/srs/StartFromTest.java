package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parsing a pipeline's {@code start_from} setting — where this pipeline starts consuming the incremental
 * tail — into a typed entry point. The three forms are the two keywords {@code earliest} / {@code latest}
 * and an ISO-8601 instant; the value is a free string at the authoring layer (the schema does not constrain
 * its format), so an unrecognized value is a runtime, user-facing error rather than a validate-layer one.
 */
class StartFromTest {

    @Test
    void parsesEarliest() {
        assertThat(StartFrom.parse("earliest")).isEqualTo(StartFrom.earliest());
    }

    @Test
    void parsesLatest() {
        assertThat(StartFrom.parse("latest")).isEqualTo(StartFrom.latest());
    }

    @Test
    void parsesAnIso8601Instant() {
        assertThat(StartFrom.parse("2026-07-11T00:00:00Z"))
                .isEqualTo(StartFrom.at(Instant.parse("2026-07-11T00:00:00Z")));
    }

    @Test
    void rejectsAValueThatIsNeitherKeywordNorInstantWithACode() {
        // start_from is a free string at authoring time, so a value like this reaches the runtime; it is a
        // user-facing, diagnosable error carrying the offending value, not a bare crash.
        assertThatThrownBy(() -> StartFrom.parse("yesterday"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException ce = (TapstateException) e;
                    assertThat(ce.code().code()).isEqualTo("capture.start-from-unparsable");
                    assertThat(ce.args()).containsEntry("value", "yesterday");
                });
    }

    /**
     * An instant this build cannot address is refused here, where the value the author wrote is still in
     * hand.
     *
     * <p>The year range {@link Instant} parses is far wider than the epoch milliseconds every consumer of
     * a start addresses it by, and the gap is reachable from an ordinary setting: the value below parses
     * cleanly and then overflows when the ring reader converts it. That arrives as a bare arithmetic
     * failure on whichever member ran the read — no code, no setting named, nothing connecting it to the
     * line of yaml that caused it. Refusing at the parse keeps the diagnosis attached to the input.
     */
    @Test
    void rejectsAnInstantTooFarOutToAddressWithACode() {
        for (String unusable : new String[] {"+999999999-01-01T00:00:00Z", "-1000000000-01-01T00:00:00Z"}) {
            assertThatThrownBy(() -> StartFrom.parse(unusable))
                    .as("start_from %s", unusable)
                    .isInstanceOf(TapstateException.class)
                    .satisfies(e -> {
                        TapstateException ce = (TapstateException) e;
                        assertThat(ce.code().code()).isEqualTo("capture.start-from-unparsable");
                        assertThat(ce.args()).containsEntry("value", unusable);
                    });
        }
    }

    /**
     * The control: an instant at the edge of what can be addressed is still accepted, so the refusal above
     * is a boundary rather than a blanket rejection of anything unusual.
     */
    @Test
    void anInstantAtTheEdgeOfTheAddressableRangeIsStillAccepted() {
        assertThat(StartFrom.parse("+292278994-08-17T07:12:55Z"))
                .isEqualTo(StartFrom.at(Instant.parse("+292278994-08-17T07:12:55Z")));
    }

    @Test
    void rejectsNullAsAProgrammerErrorNotACode() {
        // A null start_from is an invariant violation (the setting defaults to a value); it stays a bare NPE
        // rather than being laundered into a user-facing code.
        assertThatThrownBy(() -> StartFrom.parse(null)).isInstanceOf(NullPointerException.class);
    }
}
