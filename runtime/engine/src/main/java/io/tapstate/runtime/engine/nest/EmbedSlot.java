package io.tapstate.runtime.engine.nest;

import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.model.EmbedAs;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Where one embed is held and what it contributes to the assembled document. {@code stateField} is the
 * key used inside a parent's state; {@code path} is the output field for array and object shapes and is
 * absent for flat and rollup. Keeping the two separate is what lets a pathless embed retain durable identity.
 *
 * <p>{@code rollups} are the fields computed over this embed's live elements and written on the parent, checked
 * and typed when the tree was compiled.
 *
 * <p>The shape is a property of the declared tree, not of the data, which is why it is handed to a
 * render rather than remembered per document: an array embed that has never received a row still
 * renders an empty array, and only the declaration says so.
 */
public record EmbedSlot(
        String stateField,
        String path,
        String diagnosticPath,
        EmbedAs as,
        List<String> referenceFields,
        String lookupMap,
        List<EmbedSlot> children,
        List<RollupSpec> rollups) implements Serializable {

    /** A slot that computes nothing over its elements, which is every slot written before rollups existed. */
    public EmbedSlot(String stateField, String path, String diagnosticPath, EmbedAs as,
            List<String> referenceFields, String lookupMap, List<EmbedSlot> children) {
        this(stateField, path, diagnosticPath, as, referenceFields, lookupMap, children, List.of());
    }

    /**
     * An embed whose rows are grouped under this level, which is every embed that points at nothing. Its
     * elements arrive here and are held here, so there is nothing to read them by and nowhere to read them
     * from.
     */
    public EmbedSlot(String path, EmbedAs as, List<EmbedSlot> children) {
        this(path, path, path, as, null, null, children);
    }

    /** A referenced object or array embed using its output path as its state identity. */
    public EmbedSlot(String path, EmbedAs as, List<String> referenceFields, String lookupMap,
            List<EmbedSlot> children) {
        this(path, path, path, as, referenceFields, lookupMap, children);
    }

    public EmbedSlot {
        Objects.requireNonNull(stateField, "stateField");
        Objects.requireNonNull(diagnosticPath, "diagnosticPath");
        Objects.requireNonNull(as, "as");
        boolean pathless = as == EmbedAs.FLAT || as == EmbedAs.ROLLUP;
        if (pathless && path != null) {
            throw new IllegalArgumentException("a " + as.yaml() + " slot has no output path");
        }
        if (!pathless && path == null) {
            throw new IllegalArgumentException("an " + as.yaml() + " slot needs an output path");
        }
        referenceFields = referenceFields == null ? null : List.copyOf(referenceFields);
        children = List.copyOf(children);
        rollups = rollups == null ? List.of() : List.copyOf(rollups);
        if (referenceFields == null ^ lookupMap == null) {
            throw new IllegalArgumentException(
                    "slot " + path + " names one half of a reference and not the other");
        }
    }

    /**
     * Whether the row at this slot is one the level points at, read by key at render time rather than
     * arriving here and being held. The fields naming it are on the level's own row, which is why nothing
     * extra is stored per document to know where to look.
     */
    public boolean isReference() {
        return lookupMap != null;
    }
}
