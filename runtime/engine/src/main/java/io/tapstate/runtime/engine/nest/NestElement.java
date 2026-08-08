package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One change to one element of a document: where the element belongs, the row that puts it there, the
 * order that decides whether it wins, and the positions it occupies on the chains it came from.
 *
 * <p>Everything here is settled where the change enters the tree and never recomputed on the way up.
 * That is possible because an element's place is written on its own row: the parent it hangs under is
 * the value its join key names, its own identity is the column its children will point at, and both are
 * read once at the entry vertex. What the climb resolves is only <em>which document</em> the element
 * belongs to - and that is carried beside this rather than inside it, since an element waiting for its
 * parent has no answer to it yet.
 *
 * <p>{@code fields} absent means the change removes the element rather than putting a row there; the
 * one accessor {@link #deletion()} is how that is asked, so no second flag can disagree with it.
 *
 * <p>The positions are not decoration. An element held for a parent that has not arrived has been
 * consumed and not emitted, so the durable frontier must not be allowed past it - and a bound can only
 * be reported for a position that is still here to be read. An element held without its position would
 * let the frontier claim coverage of a change that has not reached a sink, which after a restart is a
 * change that can neither be replayed nor found.
 *
 * <p>The maps are copied on the way in, so a change cannot be altered from under the state holding it.
 * {@link Serializable} because a waiting bucket outlives a single run. An order is never null: a null
 * order is an engine invariant violation and crashes bare rather than being reported as a diagnosable
 * error, because comparing a missing order would silently reorder data instead.
 */
public record NestElement(
        ElementRef ref,
        Map<String, Object> fields,
        SourceOrder order,
        Map<String, ChainPosition> positions) implements Serializable {

    public NestElement {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(positions, "positions");
        positions = Collections.unmodifiableMap(new LinkedHashMap<>(positions));
        fields = fields == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    /** Whether this change removes the element rather than putting a row there. */
    public boolean deletion() {
        return fields == null;
    }
}
