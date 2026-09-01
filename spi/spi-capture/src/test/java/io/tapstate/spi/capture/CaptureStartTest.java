package io.tapstate.spi.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Where a change stream begins, as the port is told it. The type exists to keep apart statements that a
 * single nullable position collapses into one: "resume at this recorded position", "begin at this
 * instant", "begin at the oldest you still hold" and "begin at the source's present moment". A caller
 * holding no recorded position has to choose one of them before it reaches the port, so a first run and
 * a deliberate start-from-now stay distinguishable — a stream that began at the present because nothing
 * was passed misses every change made before it started, and looks from the outside exactly like a first
 * run that had nothing to miss.
 *
 * <p>Which cases exist is pinned by the contract golden in arch-tests, not here; what is pinned here is
 * what each of them means and that none of them means "you decide".
 */
class CaptureStartTest {

    @Test
    void resumeCarriesThePositionTheStreamPicksUpFrom() {
        CaptureStart start = CaptureStart.resume(new SourcePosition("binlog.000042:1234"));

        assertThat(start).isInstanceOf(CaptureStart.Resume.class);
        assertThat(((CaptureStart.Resume) start).position().token()).isEqualTo("binlog.000042:1234");
    }

    @Test
    void noTwoStartsAreTheSameStatement() {
        assertThat(CaptureStart.present())
                .isInstanceOf(CaptureStart.Present.class)
                .isNotEqualTo(CaptureStart.resume(new SourcePosition("now")))
                .isNotEqualTo(CaptureStart.earliest())
                .isNotEqualTo(CaptureStart.at(Instant.parse("2026-09-01T10:00:00Z")));
        // The oldest a source still holds is not a fixed point, so it is not the same ask as any instant
        // that happens to be old. Reading them as one is how "as far back as you go" turns into a literal
        // 1970 nobody wrote, and the difference only shows when the source cannot reach that far.
        assertThat(CaptureStart.earliest()).isNotEqualTo(CaptureStart.at(Instant.EPOCH));
    }

    @Test
    void anInstantStartCarriesTheMomentItNames() {
        CaptureStart start = CaptureStart.at(Instant.parse("2026-09-01T10:00:00Z"));

        assertThat(start).isInstanceOf(CaptureStart.At.class);
        assertThat(((CaptureStart.At) start).instant()).isEqualTo(Instant.parse("2026-09-01T10:00:00Z"));
    }

    /**
     * An instant is a point on the timeline, not a reading of a clock somewhere: the same moment written
     * with two different offsets is one start, so a pipeline does not begin at two different points
     * depending on which zone its author wrote in.
     */
    @Test
    void theSameMomentWrittenInTwoZonesIsOneStart() {
        assertThat(CaptureStart.at(Instant.parse("2026-09-01T18:00:00+08:00")))
                .isEqualTo(CaptureStart.at(Instant.parse("2026-09-01T10:00:00Z")));
    }

    @Test
    void anInstantStartWithoutAnInstantIsRejected() {
        assertThatNullPointerException().isThrownBy(() -> CaptureStart.at(null));
    }

    @Test
    void twoStartsSayingTheSameThingAreEqual() {
        assertThat(CaptureStart.resume(new SourcePosition("gtid:9-17")))
                .isEqualTo(CaptureStart.resume(new SourcePosition("gtid:9-17")));
        assertThat(CaptureStart.present()).isEqualTo(CaptureStart.present());
        assertThat(CaptureStart.earliest()).isEqualTo(CaptureStart.earliest());
    }

    @Test
    void aResumeWithoutAPositionIsRejected() {
        assertThatNullPointerException().isThrownBy(() -> CaptureStart.resume(null));
    }

    @Test
    void aPortCanOnlyBeHandedAStartTheContractAlreadyNames() {
        // Sealed: there is no case outside this file, and in particular none that means "I have no
        // position, you pick" - which is the state the port used to be handed as a null and answer by
        // starting wherever it liked.
        assertThat(CaptureStart.class.isSealed()).isTrue();
    }
}
