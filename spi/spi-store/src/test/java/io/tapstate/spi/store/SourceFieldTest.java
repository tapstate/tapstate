package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A discovered column, and the one thing it may not do: arrive without a type and without saying which
 * of the several unknowns it is. The several are the same value and different problems - a connector
 * that declared nothing, a spelling no mapping names, a record written before types were kept - and
 * folded together they read as the one that needs no action.
 */
class SourceFieldTest {

    @Test
    @DisplayName("a resolved type carries no reason, because there is nothing to attribute")
    void aResolvedTypeCarriesNoReason() {
        assertThat(new SourceField("qty", "bigint", TapstateType.INT64).unknownBecause()).isNull();
    }

    @Test
    @DisplayName("an unknown that does not say which unknown it is, is refused")
    void anUnknownWithNoReasonIsRefused() {
        assertThatThrownBy(() -> new SourceField("shape", "geometry", TapstateType.UNKNOWN, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shape");
        assertThatThrownBy(() -> new SourceField("shape", "geometry", TapstateType.UNKNOWN, "  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the short form is for a type that resolved, so an unknown does not fit through it")
    void theShortFormRefusesAnUnknown() {
        // The form that takes a type and no reason is the resolved case. A caller holding an unknown
        // has something to say and the full form is where it is said; letting this one through would
        // put every unattributed unknown back in one bucket by the cheapest possible route.
        assertThatThrownBy(() -> new SourceField("shape", "geometry", TapstateType.UNKNOWN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a null type is unknown and still has to say which unknown")
    void aNullTypeIsUnknownAndStillNeedsAReason() {
        assertThat(new SourceField("shape", "geometry", null, "nobody asked").type())
                .isEqualTo(TapstateType.UNKNOWN);
        assertThatThrownBy(() -> new SourceField("shape", "geometry", null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("the type-less form says so itself rather than leaving the field silent")
    void theTypelessFormSaysSoItself() {
        SourceField field = new SourceField("shape", "geometry");

        assertThat(field.type()).isEqualTo(TapstateType.UNKNOWN);
        assertThat(field.unknownBecause()).isNotBlank();
    }

    @Test
    @DisplayName("a resolved type may not also carry a reason for being unknown")
    void aResolvedTypeMayNotCarryAReason() {
        // Not tidiness: a field is read by asking whether the reason is there, so a known type
        // carrying one answers "unknown" to every reader that asks the cheap way.
        assertThatThrownBy(() -> new SourceField("qty", "bigint", TapstateType.INT64, "left over"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("INT64");
    }
}
