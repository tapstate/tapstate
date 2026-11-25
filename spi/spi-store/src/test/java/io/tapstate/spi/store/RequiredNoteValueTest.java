package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequiredNoteValueTest {
    @Test
    void bytesAreDetachedOnInputAndOutput() {
        byte[] supplied = {1, 2};
        var value = new RequiredNoteValue(1, supplied);
        supplied[0] = 9;
        byte[] returned = value.bytes();
        returned[1] = 9;
        assertThat(value.bytes()).containsExactly((byte) 1, (byte) 2);
    }

    @Test
    void absentAndDeletedValuesRetainDifferentRevisions() {
        assertThat(new RequiredNoteValue(0, null).revision()).isZero();
        assertThat(new RequiredNoteValue(2, null).revision()).isEqualTo(2);
        assertThatThrownBy(() -> new RequiredNoteValue(0, new byte[]{1})).isInstanceOf(IllegalArgumentException.class);
    }
}
