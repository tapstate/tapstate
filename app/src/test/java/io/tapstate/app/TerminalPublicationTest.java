package io.tapstate.app;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class TerminalPublicationTest {
    @Test void aSealCannotPassAReservedProducerAndDoesNotAdmitLateWork() {
        var gate = new TerminalPublication.Admission();
        assertThat(gate.enter()).isTrue();assertThat(gate.seal()).isFalse();
        gate.leave();assertThat(gate.seal()).isTrue();assertThat(gate.enter()).isFalse();
        assertThat(gate.idle()).isTrue();
        gate.reopen();assertThat(gate.enter()).isTrue();gate.leave();
        gate.failed=true;assertThat(gate.seal()).isFalse();
    }
    @Test void unknownReceiptsNeverSeal() {
        assertThat(TerminalPublication.failed().seal()).isFalse();
        assertThat(TerminalPublication.complete().seal()).isTrue();
    }
}
