package io.tapstate.core.common;

import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.assertj.core.api.Assertions.*;

class ProductErrorDefinitionSetTest {
    @Test
    void declaringAConstantRegistersItWithoutASecondListAndSealsTheCollection() {
        var definitions = new ProductErrorDefinitionSet(ProductErrorModule.CONTROL);
        var first = definitions.define(1, Severity.ERROR, Set.of("reason"));
        var second = definitions.define(2, Severity.WARNING, Set.of());
        var snapshot = definitions.registered();
        assertThat(snapshot).containsExactly(first, second);
        assertThatThrownBy(() -> snapshot.clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> definitions.define(3, Severity.ERROR, Set.of()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("sealed");
        assertThat(definitions.registered()).containsExactly(first, second);
    }

    @Test
    void duplicateSerialsRefuseInsteadOfReplacingTheOriginalDefinition() {
        var definitions = new ProductErrorDefinitionSet(ProductErrorModule.CONTROL);
        var original = definitions.define(1, Severity.ERROR, Set.of());
        assertThatThrownBy(() -> definitions.define(1, Severity.WARNING, Set.of("reason")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("duplicate");
        assertThat(definitions.registered()).containsExactly(original);
    }
}
