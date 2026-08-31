package io.tapstate.spi.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

/**
 * Where a change stream begins, as the port is told it. The type exists to keep two statements apart
 * that a single nullable position collapses into one: "resume at this recorded position" and "begin at
 * the source's present moment". A caller holding no recorded position has to choose one of them before
 * it reaches the port, so a first run and a deliberate start-from-now stay distinguishable — a stream
 * that began at the present because nothing was passed misses every change made before it started, and
 * looks from the outside exactly like a first run that had nothing to miss.
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
    void theTwoStartsAreNeverTheSameStatement() {
        assertThat(CaptureStart.present())
                .isInstanceOf(CaptureStart.Present.class)
                .isNotEqualTo(CaptureStart.resume(new SourcePosition("now")));
    }

    @Test
    void twoStartsSayingTheSameThingAreEqual() {
        assertThat(CaptureStart.resume(new SourcePosition("gtid:9-17")))
                .isEqualTo(CaptureStart.resume(new SourcePosition("gtid:9-17")));
        assertThat(CaptureStart.present()).isEqualTo(CaptureStart.present());
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
