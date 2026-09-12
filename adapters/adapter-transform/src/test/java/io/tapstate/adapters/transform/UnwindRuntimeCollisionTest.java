package io.tapstate.adapters.transform;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class UnwindRuntimeCollisionTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void anUndiscoveredParentColumnCannotBeOverwritten(boolean ordinal) {
        TransformBody.Unwind body = new TransformBody.Unwind("items",
                ordinal ? "region" : null, false, ordinal ? null : "region", null);
        var port = StatelessTransforms.unwind(UnwindSpec.from(body, List.of("id")));
        Map<String, Object> row = Map.of("id", 7L, "region", "eu",
                "items", List.of(Map.of("region", "a")));
        Throwable failure = catchThrowable(() -> port.transform(
                new Envelope(Op.READ, 1L, "orders", null, row, null)));
        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failure).code().code())
                .isEqualTo("dsl.unwind-column-already-exists");
        assertThat(row).containsEntry("region", "eu");
    }
}
