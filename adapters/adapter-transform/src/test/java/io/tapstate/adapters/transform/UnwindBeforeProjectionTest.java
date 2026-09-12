package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class UnwindBeforeProjectionTest {
    private static TransformPort projection(FieldRule rule) {
        return StatelessTransforms.requireCompleteBeforeImage(
                StatelessTransforms.map(MapSpec.from(new TransformBody.MapProjection(Map.of("stage", rule)))),
                "items", List.of("id"));
    }

    private static List<Envelope> expand(TransformPort map, Envelope event) {
        TransformPort unwind = StatelessTransforms.unwind(UnwindSpec.from(
                new TransformBody.Unwind("items", null, false, "sku", null), List.of("id")));
        return map.transform(event).stream().flatMap(row -> unwind.transform(row).stream()).toList();
    }

    @ParameterizedTest
    @EnumSource(value = Op.class, names = {"DELETE", "UPDATE"})
    void literalCannotMakeAKeyOnlyImageComplete(Op op) {
        assertIncomplete(op, FieldRule.literal("prod"));
    }

    @ParameterizedTest
    @EnumSource(value = Op.class, names = {"DELETE", "UPDATE"})
    void computationCannotMakeAKeyOnlyImageComplete(Op op) {
        assertIncomplete(op, FieldRule.computed("after.id"));
    }

    private static void assertIncomplete(Op op, FieldRule rule) {
        Envelope event = new Envelope(op, 1, "orders", Map.of("id", 1),
                op == Op.UPDATE ? Map.of("id", 1, "items", List.of(Map.of("sku", "a"))) : null, null);
        TapstateException error = catchThrowableOfType(TapstateException.class,
                () -> expand(projection(rule), event));
        assertThat(error).isNotNull();
        assertThat(error.code()).isEqualTo(TransformError.UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE);
    }

    @Test
    void completeImageWithAnAbsentOptionalArrayRemainsValid() {
        assertThat(expand(projection(FieldRule.literal("prod")),
                Envelope.delete(1, "orders", Map.of("id", 1, "customer", "ada"), null))).isEmpty();
    }

    @Test
    void snapshotAndCompleteDeleteProduceTheSameExpandedIdentity() {
        TransformPort map = projection(FieldRule.literal("prod"));
        Map<String, Object> row = Map.of("id", 1, "items", List.of(Map.of("sku", "a")));
        Envelope snapshot = expand(map, Envelope.read(1, "orders", row, null)).getFirst();
        Envelope deleted = expand(map, Envelope.delete(2, "orders", row, null)).getFirst();
        assertThat(snapshot.after()).isEqualTo(deleted.before());
        assertThat(deleted.op()).isEqualTo(Op.DELETE);
    }
}
