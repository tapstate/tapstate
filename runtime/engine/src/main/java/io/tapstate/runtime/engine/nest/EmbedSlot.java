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
public record EmbedSlot(String path, EmbedAs as, List<EmbedSlot> children) implements Serializable {

    public EmbedSlot {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(as, "as");
        children = List.copyOf(children);
    }
}
