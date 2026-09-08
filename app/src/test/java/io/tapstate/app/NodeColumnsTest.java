package io.tapstate.app;

import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The dispatch every node of a pipeline is worked out through, and the guarantee that a node nobody
 * wrote an answer for cannot reach a run.
 *
 * <p>What is under test here is mostly not a behaviour - it is that a ninth kind of node cannot be
 * added without the compiler stopping whoever added it. That guarantee has no runtime shape, so the
 * cases below cover the two things around it that do: every kind alive today reaches an arm of its
 * own, and the number of kinds each face carries is the number this class answers for. The second is
 * what survives somebody silencing the compiler with a catch-all instead of writing the arm.
 */
class NodeColumnsTest {

    @Test
    @DisplayName("every kind of transform reaches an arm of its own")
    void everyKindOfTransformReachesAnArmOfItsOwn() {
        List<TransformBody> bodies = List.of(
                new TransformBody.Js("emit(record)"),
                new TransformBody.MapProjection(Map.of("a", FieldRule.rename("b"))),
                new TransformBody.Filter("row.a > 1"),
                new TransformBody.Union(),
                new TransformBody.Nest("id", null, new NestRoot("orders", null, null, null, null)),
                new TransformBody.Join(JoinEngine.BUILTIN, "select 1"));

        Set<String> reasons = new LinkedHashSet<>();
        for (TransformBody body : bodies) {
            NodeColumns answer = NodeColumns.of(body);
            assertThat(answer.known()).isFalse();
            assertThat(answer.unknownBecause()).startsWith(body.type() + ":");
            reasons.add(answer.unknownBecause());
        }
        // One arm per kind, not one arm shared by six: a single answer covering the lot would read as
        // six answers here unless the reasons are counted.
        assertThat(reasons).hasSameSizeAs(bodies);
    }

    @Test
    @DisplayName("both push formats and the default envelope reach arms of their own")
    void bothPushFormatsAndTheDefaultEnvelopeReachArmsOfTheirOwn() {
        String cel = NodeColumns.of(push(PushFormat.cel("record"))).unknownBecause();
        String fields = NodeColumns.of(push(PushFormat.fields(Map.of("a", FieldRule.drop())))).unknownBecause();
        String envelope = NodeColumns.of(push(null)).unknownBecause();

        assertThat(cel).startsWith("serve.push cel:");
        assertThat(fields).startsWith("serve.push fields:");
        // A push element with no format of its own is not a missing node - it publishes the envelope,
        // which is a shape like any other. Folded into either of the two above it would be answered
        // for by a rule written about something else.
        assertThat(envelope).startsWith("serve.push envelope:");
        assertThat(Set.of(cel, fields, envelope)).hasSize(3);
    }

    @Test
    @DisplayName("a view reaches an arm of its own")
    void aViewReachesAnArmOfItsOwn() {
        ViewBlock.Inline view = new ViewBlock.Inline("v", FromRef.literal("orders"), "id", null, null);

        NodeColumns answer = NodeColumns.of(view);

        assertThat(answer.known()).isFalse();
        assertThat(answer.unknownBecause()).startsWith("view:");
    }

    @Test
    @DisplayName("a serve block hands back every push node it carries, and none when it carries none")
    void aServeBlockHandsBackEveryPushNodeItCarries() {
        PushElement first = push(PushFormat.cel("record"));
        PushElement second = push(null);

        assertThat(NodeColumns.pushNodesOf(serve(List.of(first, second))))
                .containsExactly(first, second);
        assertThat(NodeColumns.pushNodesOf(serve(null))).isEmpty();
    }

    @Test
    @DisplayName("a use-reference crashes bare: the derivation runs on a pipeline whose references are expanded")
    void aUseReferenceCrashesBare() {
        ViewBlock.Use view = new ViewBlock.Use(null, "shared_view", FromRef.literal("orders"));
        ServeBlock.Use serve = new ServeBlock.Use(null, "shared_serve", FromRef.literal("orders"));

        // Not a coded error: reaching here with a reference means the caller skipped the expansion,
        // which is a defect on this side and nothing an author can act on.
        assertThatThrownBy(() -> NodeColumns.of(view)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shared_view");
        assertThatThrownBy(() -> NodeColumns.pushNodesOf(serve)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("shared_serve");
    }

    @Test
    @DisplayName("a ninth kind of node cannot land without an arm here")
    void aNinthKindOfNodeCannotLandWithoutAnArmHere() {
        // The compiler is what actually enforces this - every switch over these faces is exhaustive and
        // carries no catch-all, so a new variant stops the build until somebody answers for it. This
        // case covers the one way that guarantee is lost without the build ever going red: a catch-all
        // added to make the error go away. The counts are the arms; a variant absorbed by a catch-all
        // moves the count and not the arms.
        assertThat(TransformBody.class.getPermittedSubclasses()).hasSize(6);
        assertThat(PushFormat.class.getPermittedSubclasses()).hasSize(2);
        assertThat(ViewBlock.class.getPermittedSubclasses()).hasSize(2);
        assertThat(ServeBlock.class.getPermittedSubclasses()).hasSize(2);
    }

    @Test
    @DisplayName("a known answer carries columns and no reason; an unknown one carries a reason and no columns")
    void aKnownAnswerCarriesColumnsAndAnUnknownOneCarriesAReason() {
        // Declared in an order a hash map does not keep, because the declared order is the output order:
        // a node rebuilt with its columns reordered records a schema that differs from the one before it
        // for a reason nothing about the pipeline moved.
        Map<String, String> declared = new LinkedHashMap<>();
        declared.put("zip", "STRING NULL");
        declared.put("id", "INT64 NOT NULL");
        declared.put("amount", "DECIMAL NULL");
        declared.put("created_at", "DATETIME NULL");
        NodeColumns known = NodeColumns.known(declared);
        NodeColumns unknown = NodeColumns.unknown("js: nobody can say");

        assertThat(known.known()).isTrue();
        assertThat(known.columns()).containsExactlyEntriesOf(declared);
        assertThat(known.unknownBecause()).isNull();
        // Empty rather than null, so a caller walking the columns of whatever it was handed does not
        // have to ask which of the two it got before it can walk nothing.
        assertThat(unknown.known()).isFalse();
        assertThat(unknown.columns()).isEmpty();
    }

    private static PushElement push(PushFormat format) {
        return new PushElement(null, "orders_src", "topic", format, null);
    }

    private static ServeBlock.Inline serve(List<PushElement> push) {
        return new ServeBlock.Inline("s", FromRef.literal("orders"), null, null, push);
    }
}
