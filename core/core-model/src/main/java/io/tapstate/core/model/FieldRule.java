package io.tapstate.core.model;

import java.util.Objects;

/**
 * One projection rule of a {@code map} step / push {@code format} object (ADR-0016 §5,
 * X11): {@code $src} rename, {@code false} drop, literal value, or {@code =CEL} computed
 * row expression. Unlisted fields pass through (runtime semantics, not modeled here).
 */
@Doc("One field projection rule: rename a source field, drop a field, set a literal value, or compute a value from a CEL expression.")
public sealed interface FieldRule {

    static Rename rename(String sourceField) {
        return new Rename(sourceField);
    }

    static Drop drop() {
        return new Drop();
    }

    static Literal literal(Object value) {
        return new Literal(value);
    }

    static Computed computed(String celExpr) {
        return new Computed(celExpr);
    }

    /** {@code output: $source_field} — rename; the source field is consumed. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Rename(String sourceField) implements FieldRule {
        public Rename {
            Objects.requireNonNull(sourceField, "sourceField");
        }
    }

    /**
     * {@code output: false} — the field is not carried onward from this step.
     *
     * <p><b>It says nothing about what a target already holds.</b> Rows written from here on do not carry
     * the field, so a target populated from them never gains it; a target that already has a value for it,
     * written before this rule existed or by something else, keeps that value. Clearing what is already
     * there is a separate act and this is not it.
     *
     * <p>Worth stating because the other reading is the natural one, and it is wrong in a way nothing
     * reports: a rule added to stop sending a field leaves whatever was sent before exactly where it is,
     * beside fields that go on updating normally.
     */
    @YamlForm(YamlForm.Form.FALSE)
    record Drop() implements FieldRule {
    }

    /** {@code output: <literal>} — add a constant. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Literal(Object value) implements FieldRule {
        public Literal {
            Objects.requireNonNull(value, "value");
        }
    }

    /** {@code output: =<CEL>} — add a computed row expression (§12); stored without the marker. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Computed(String celExpr) implements FieldRule {
        public Computed {
            Objects.requireNonNull(celExpr, "celExpr");
        }
    }
}
