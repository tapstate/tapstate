package io.tapstate.runtime.engine.nest;

import io.tapstate.core.model.EmbedAs;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Where one embed sits in the assembled document and what shape it takes there: the field it occupies
 * under its parent, whether that field holds an array of elements or a single object, and the embeds
 * nested beneath it.
 *
 * <p>The shape is a property of the declared tree, not of the data, which is why it is handed to a
 * render rather than remembered per document: an array embed that has never received a row still
 * renders an empty array, and only the declaration says so.
 */
public record EmbedSlot(String path, EmbedAs as, List<String> referenceFields, String lookupMap,
        List<EmbedSlot> children) implements Serializable {

    /**
     * An embed whose rows are grouped under this level, which is every embed that points at nothing. Its
     * elements arrive here and are held here, so there is nothing to read them by and nowhere to read them
     * from.
     */
    public EmbedSlot(String path, EmbedAs as, List<EmbedSlot> children) {
        this(path, as, null, null, children);
    }

    public EmbedSlot {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(as, "as");
        referenceFields = referenceFields == null ? null : List.copyOf(referenceFields);
        children = List.copyOf(children);
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
