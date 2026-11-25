package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequiredNoteOriginTest {
    @Test
    void settingsAreDetachedAndComparedByValueWithoutBeingPrinted() {
        byte[] input = {1, 2};
        var origin = new RequiredNoteOrigin("mongo", "a".repeat(64), input);
        input[0] = 9;
        origin.settings()[1] = 9;
        assertThat(origin).isEqualTo(new RequiredNoteOrigin("mongo", "a".repeat(64), new byte[]{1, 2}));
        assertThat(origin.settings()).containsExactly((byte) 1, (byte) 2);
        assertThat(origin.toString()).isEqualTo("RequiredNoteOrigin[redacted]");
    }

    @Test
    void aMissingOrNonContentAddressedArtifactCannotBecomeARetainedOrigin() {
        assertThatThrownBy(() -> new RequiredNoteOrigin("mongo", "../../artifact", new byte[]{1}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RequiredNoteOrigin("mongo", "a".repeat(64), new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
