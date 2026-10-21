package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationKey.Part.*;
import static org.assertj.core.api.Assertions.*;

class ValidationKeyTest {
    private static ValidationKey key(ValidationKey.Part... parts) {
        return new ValidationKey(List.of(parts));
    }

    @Test void exactNumbersShareEqualityHashAndOrderWithoutLosingLargeValues() {
        var one = key(number(1));
        var scaled = key(number(new BigDecimal("1.0000")));
        assertThat(one).isEqualTo(scaled).hasSameHashCodeAs(scaled);
        assertThat(one.compareTo(scaled)).isZero();
        assertThat(new HashSet<>(List.of(one, scaled))).hasSize(1);
        var low = key(number(new BigInteger("9007199254740992")));
        var high = key(number(new BigInteger("9007199254740993")));
        assertThat(low).isNotEqualTo(high);
        assertThat(low.compareTo(high)).isNegative();
        assertThat(key(number(new BigDecimal("1.5"))).compareTo(one)).isPositive();
    }

    @Test void zeroAndNegativeScaleNormalizeWithoutExpandingTheValue() {
        assertThat(key(number(new BigDecimal("0E+999999")))).isEqualTo(key(number(0)));
        assertThat(key(number(new BigDecimal("1000.0"))))
                .isEqualTo(key(number(new BigDecimal("1E+3"))));
        assertThat(key(number(new BigDecimal("1E+999999"))).compareTo(key(number(1))))
                .isPositive();
    }

    @Test void stringsRetainWhitespaceAndLogicalDomainsNeverCoerce() {
        assertThat(key(text("abc "))).isNotEqualTo(key(text("abc")));
        assertThat(key(text("001"))).isNotEqualTo(key(number(1)));
        assertThat(key(bool(true))).isNotEqualTo(key(number(1)));
        assertThatThrownBy(() -> key(text("1")).compareTo(key(number(1))))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code()).isEqualTo(ValidationError.INCOMPATIBLE_KEY));
    }

    @Test void comparesAllDomainsBeforeReturningAnEarlierComponentOrder() {
        assertThatThrownBy(() -> key(number(1), text("a")).compareTo(key(number(2), number(3))))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.args()).containsEntry("component", 1));
        assertThatThrownBy(() -> key(number(1)).compareTo(key(number(1), number(2))))
                .isInstanceOf(TapstateException.class);
    }

    @Test void compositeKeysDoNotCollapseComponentBoundaries() {
        assertThat(key(text("a:b"), text("c"))).isNotEqualTo(key(text("a"), text("b:c")));
        assertThat(key(number(1), number(9)).compareTo(key(number(2), number(0)))).isNegative();
    }

    @Test void binaryKeysOwnTheirBytesAndRetainSubtype() {
        byte[] source = {1, 2};
        var original = key(binary(4, source));
        int hash = original.hashCode();
        source[0] = 9;
        ((byte[]) original.parts().getFirst().value())[1] = 9;
        assertThat(original).isEqualTo(key(binary(4, new byte[]{1, 2})));
        assertThat(original.hashCode()).isEqualTo(hash);
        assertThat(original).isNotEqualTo(key(binary(0, new byte[]{1, 2})));
        assertThatThrownBy(() -> original.compareTo(key(binary(0, new byte[]{1, 2}))))
                .isInstanceOf(TapstateException.class);
        assertThat(key(binary(0, new byte[]{(byte) 255})).compareTo(key(binary(0, new byte[]{1}))))
                .isPositive();
    }

    @Test void instantsKeepNanosecondsAndEquivalentOffsets() {
        Instant a = OffsetDateTime.parse("2026-10-21T12:00:00.123456+08:00").toInstant();
        Instant b = Instant.parse("2026-10-21T04:00:00.123456Z");
        assertThat(key(instant(a))).isEqualTo(key(instant(b)));
        assertThat(key(instant(a))).isNotEqualTo(key(instant(b.plusNanos(1))));
    }

    @Test void componentListIsOwnedAndDiagnosticsDoNotExposeKeyValues() {
        var mutable = new ArrayList<>(List.of(text("private-customer-key")));
        var key = new ValidationKey(mutable);
        mutable.clear();
        assertThat(key.parts()).hasSize(1);
        assertThatThrownBy(() -> key.parts().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(key.toString()).doesNotContain("private-customer-key");
        assertThat(key.parts().toString()).doesNotContain("private-customer-key");
    }

    @Test void rejectsOversizedComponentsAndAggregateKeys() {
        assertThatThrownBy(() -> binary(0, new byte[ValidationKey.MAX_COMPONENT_BYTES]))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code()).isEqualTo(ValidationError.KEY_LIMIT_EXCEEDED));
        assertThatThrownBy(() -> text("x".repeat(ValidationKey.MAX_COMPONENT_BYTES)))
                .isInstanceOf(TapstateException.class);
        var part = binary(0, new byte[16000]);
        assertThatThrownBy(() -> key(part, part, part, part, part)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationKey(java.util.Collections.nCopies(33, number(1))))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationKey(List.of())).isInstanceOf(TapstateException.class);
    }
}
