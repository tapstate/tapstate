package io.tapstate.spi.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.NumericType;
import io.tapstate.core.common.TapstateType;
import java.util.List;
import org.junit.jupiter.api.Test;

class TargetFieldTest {

    @Test
    void carriesNameTypeAndPrimaryKeyFlag() {
        TargetField field = new TargetField("id", "int", true);

        assertThat(field.name()).isEqualTo("id");
        assertThat(field.type()).isEqualTo("int");
        assertThat(field.primaryKey()).isTrue();
    }

    @Test
    void typeMayBeNullWhenUnresolved() {
        assertThat(new TargetField("id", null, false).type()).isNull();
    }

    @Test
    void onlyAnUntypedNonKeyCandidateWithoutAnInferredTypeCanDeferDecimal() {
        var decimal = new NumericType(null, true, null, null, null, null, 10, 2);
        assertThat(TargetField.deferredDecimal("amount", decimal).isDeferredDecimal()).isTrue();
        assertThat(List.of(
                new TargetField("id", null, true, TapstateType.UNKNOWN, decimal),
                new TargetField("explicit", "decimal(10,2)", false, TapstateType.DECIMAL, decimal),
                new TargetField("inferred", null, false, TapstateType.DECIMAL, decimal),
                new TargetField("unknown", null, false, TapstateType.UNKNOWN)))
                .noneMatch(TargetField::isDeferredDecimal);
    }

    @Test
    void requiresANonBlankName() {
        assertThatThrownBy(() -> new TargetField(" ", "int", false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TargetField(null, "int", false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
