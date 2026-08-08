package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Where one element belongs in a nested document: which embed it is part of, which row it hangs under,
 * and who it is once it gets there.
 *
 * <p>{@code pathId} is the sequence of field names from the root down to this element's embed, so it
 * names the embed itself rather than a level: two embeds side by side under one parent are different
 * paths, and an element of one can never be mistaken for an element of the other even when their rows
 * carry the same key value. {@code parentIdentity} is the value this row's join key points at, which
 * the parent row answers to; at depth one the parent is the root itself and this is null.
 *
 * <p>{@code elementKey} is the element's identity inside the document — what an update updates and a
 * delete deletes. {@code identity} is the separate value this element's own children point at, null
 * for an embed with no children. The two are deliberately distinct: a document usually shows a
 * business key while child rows carry a surrogate foreign key, and nothing requires them to agree.
 */
public record ElementRef(
        List<String> pathId,
        Object parentIdentity,
        List<Object> elementKey,
        Object identity) implements Serializable {

    public ElementRef {
        Objects.requireNonNull(pathId, "pathId");
        Objects.requireNonNull(elementKey, "elementKey");
        if (pathId.isEmpty()) {
            throw new IllegalArgumentException("pathId names the embed and cannot be empty");
        }
        pathId = List.copyOf(pathId);
        elementKey = Collections.unmodifiableList(new ArrayList<>(elementKey));
    }

    /**
     * The embed of this element's parent — empty when the parent is the root. A copy rather than a
     * view: it is held as a key of state that gets stored, and a sublist view does not serialize.
     */
    List<String> parentPathId() {
        return List.copyOf(pathId.subList(0, pathId.size() - 1));
    }

    /** The field this element's embed occupies under its parent. */
    String field() {
        return pathId.get(pathId.size() - 1);
    }
}
