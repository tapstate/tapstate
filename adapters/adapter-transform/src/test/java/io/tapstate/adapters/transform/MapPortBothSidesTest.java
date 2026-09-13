package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A projection applies to the row an event replaces as well as to the row it carries.
 *
 * <p>It used to apply only to the new row: an earlier row travelled through untouched and a delete,
 * which carries nothing else, passed through whole. That leaves the two sides of one event in
 * different shapes, and every consumer that has to line them up is then lining up a projected row
 * against an unprojected one. A rename is the plain case - the new row arrives under the new
 * column name and the earlier row under the old one, so a target told to remove a row is told a
 * name it does not have, and the row stays.
 *
 * <p><b>The reverse half of this suite matters as much as the forward half.</b> {@code map} is in
 * every pipeline anyone has already built, and changing what it does to a row is the kind of change
 * no gate here would otherwise notice: an insert or a read has no earlier row, so nothing about
 * them may move, and this asserts the whole projected row rather than a field of it so that a rule
 * quietly gaining or losing a field is caught.
 */
class MapPortBothSidesTest {

    private static TransformPort map(LinkedHashMap<String, FieldRule> fields) {
        return StatelessTransforms.map(MapSpec.from(new TransformBody.MapProjection(fields)));
    }

    private static LinkedHashMap<String, FieldRule> fields(Object... pairs) {
        LinkedHashMap<String, FieldRule> f = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            f.put((String) pairs[i], (FieldRule) pairs[i + 1]);
        }
        return f;
    }

    /** Rename, drop, literal and a computed value - one of each, so no rule kind goes unexercised. */
    private static TransformPort everyRuleKind() {
        return map(fields(
                "order_id", FieldRule.rename("id"),
                "secret", FieldRule.drop(),
                "stage", FieldRule.literal("prod"),
                "loud", FieldRule.computed("after.region")));
    }

    private static Map<String, Object> row(Object id, String region) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", id);
        r.put("region", region);
        r.put("secret", "hidden");
        r.put("note", "kept");
        return r;
    }

    /** What every rule kind makes of {@link #row}: declared outputs in order, then the leftovers. */
    private static Map<String, Object> projected(Object id, String region) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("order_id", id);
        p.put("stage", "prod");
        p.put("loud", region);
        p.put("region", region);
        p.put("note", "kept");
        return p;
    }

    // ---- the half that changed ----------------------------------------------------------

    /**
     * A delete carries only the row that is going, so before this it was the one event a projection
     * never touched at all. A rename makes the consequence plain: the target is asked to remove a
     * row addressed by a column name that only exists upstream of this step.
     */
    @Test
    @DisplayName("a delete's row is projected, not passed through whole")
    void aDeleteIsProjected() {
        Envelope out = everyRuleKind()
                .transform(new Envelope(Op.DELETE, 1L, "orders", row(1, "eu"), null, null))
                .get(0);

        assertThat(out.op()).isEqualTo(Op.DELETE);
        assertThat(out.after()).isNull();
        assertThat(out.before()).containsExactlyEntriesOf(projected(1, "eu"));
    }

    /**
     * Both sides of an update come out in one shape. Projecting only the new one leaves a pairing
     * downstream comparing {@code order_id} against {@code id} - never equal, so every row reads as
     * new and whatever the earlier row identified is never taken away.
     */
    @Test
    @DisplayName("an update comes out with both of its rows in the same shape")
    void anUpdateIsProjectedOnBothSides() {
        Envelope out = everyRuleKind()
                .transform(new Envelope(Op.UPDATE, 1L, "orders", row(1, "eu"), row(1, "us"), null))
                .get(0);

        assertThat(out.before()).containsExactlyEntriesOf(projected(1, "eu"));
        assertThat(out.after()).containsExactlyEntriesOf(projected(1, "us"));
    }

    /**
     * A computed value on the earlier row is computed from the earlier row. Anything else would put
     * the new row's value on both sides, which is worse than not projecting at all: the two would
     * agree on a column that actually changed, and a consumer comparing them would conclude nothing
     * moved.
     */
    @Test
    @DisplayName("a computed column on the earlier row reads the earlier row's values")
    void aComputedColumnReadsTheRowItIsProjecting() {
        Envelope out = everyRuleKind()
                .transform(new Envelope(Op.UPDATE, 1L, "orders", row(1, "eu"), row(1, "us"), null))
                .get(0);

        assertThat(out.before()).containsEntry("loud", "eu");
        assertThat(out.after()).containsEntry("loud", "us");
    }

    // ---- the half that must not have changed --------------------------------------------

    @Test
    @DisplayName("an insert is projected exactly as it always was")
    void anInsertIsUnchanged() {
        Envelope out = everyRuleKind()
                .transform(new Envelope(Op.INSERT, 1L, "orders", null, row(1, "eu"), null))
                .get(0);

        assertThat(out.op()).isEqualTo(Op.INSERT);
        assertThat(out.before()).isNull();
        assertThat(out.after()).containsExactlyEntriesOf(projected(1, "eu"));
    }

    @Test
    @DisplayName("a snapshot read is projected exactly as it always was")
    void aReadIsUnchanged() {
        Envelope out = everyRuleKind()
                .transform(new Envelope(Op.READ, 1L, "orders", null, row(2, "ap"), null))
                .get(0);

        assertThat(out.after()).containsExactlyEntriesOf(projected(2, "ap"));
    }

    @Test
    @DisplayName("a schema change still passes through whole")
    void aDdlStillPassesThrough() {
        Envelope ddl = Envelope.ddl(1L, "orders", Map.of("kind", "add-column"));

        assertThat(everyRuleKind().transform(ddl)).containsExactly(ddl);
    }

    /**
     * What an earlier step said a row no longer has is carried across both sides, unchanged. It is
     * the only way a target hears that a field went, and rebuilding two rows instead of one is two
     * chances to drop it.
     */
    @Test
    @DisplayName("a removal declared upstream survives a projection of either side")
    void aRemovalSurvivesBothSides() {
        Envelope in = new Envelope(Op.DELETE, 1L, "orders", row(1, "eu"), null, null)
                .withRemoved(Set.of("customer"));

        assertThat(everyRuleKind().transform(in).get(0).removed()).containsExactly("customer");
    }

    /**
     * An event with neither row is not an event this projection has anything to say about. Without
     * this the both-sides rewrite reads naturally as "project whatever is there", and a delete with
     * no earlier row would come out carrying a row built entirely of literals.
     */
    @Test
    @DisplayName("an event carrying no row at all comes out carrying no row")
    void nothingToProjectStaysNothing() {
        Envelope empty = new Envelope(Op.DELETE, 1L, "orders", null, null, null);

        Envelope out = everyRuleKind().transform(empty).get(0);

        assertThat(out.before()).isNull();
        assertThat(out.after()).isNull();
    }
}
