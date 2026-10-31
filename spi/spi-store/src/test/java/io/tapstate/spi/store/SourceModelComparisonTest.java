package io.tapstate.spi.store;

import io.tapstate.core.common.StringType;
import io.tapstate.core.common.TapstateType;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class SourceModelComparisonTest {
    @Test void declaredWidthsRemainStructuralInBothRepresentations() {
        var old = text("varchar(36)", 36);
        var wider = text("varchar(72)", 72);
        assertThat(SourceModelComparison.DECLARED_STRUCTURE.acceptsObservation(old, wider)).isFalse();
        assertThat(SourceModelComparison.DECLARED_STRUCTURE.acceptsObservation(old, text("varchar(36)", 72))).isFalse();
        assertThat(SourceModelComparison.DECLARED_STRUCTURE.acceptsObservation(old, old)).isTrue();
    }

    @Test void sampledMembershipOrderKindsAndWidthsAreNotCollectionDeclarations() {
        var old = text("STRING(20480)", 20480);
        var changed = new SourceTable("orders", List.of(new SourceField("new", "INT32", TapstateType.INT64),
                new SourceField("name", "STRING(100)", TapstateType.STRING)), List.of(), List.of());
        assertThat(SourceModelComparison.SAMPLED_DOCUMENT.acceptsObservation(old, changed)).isTrue();
        assertThat(SourceModelComparison.SAMPLED_DOCUMENT.acceptsObservation(changed, old)).isTrue();
        assertThat(SourceModelComparison.DECLARED_STRUCTURE.acceptsObservation(old, changed)).isFalse();
        assertThat(SourceModelComparison.SAMPLED_DOCUMENT.acceptsObservation(old,
                new SourceTable("another", changed.fields(), List.of(), List.of()))).isFalse();
    }

    private static SourceTable text(String nativeType, long width) {
        return new SourceTable("orders", List.of(new SourceField("name", nativeType, TapstateType.STRING,
                null, null, new StringType(width, null, null, null, null))), List.of(), List.of());
    }
}
