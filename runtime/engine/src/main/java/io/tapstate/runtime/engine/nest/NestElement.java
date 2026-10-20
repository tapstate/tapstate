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
 * <p>The positions remain attached while a change waits or is handed onward. A durable waiting copy
 * lets the frontier pass only after its write returns. Releasing it retains a recoverable handoff until
 * downstream coverage is confirmed; losing both copies would leave nothing to replay or load.
 *
 * <p>The source's own time for the change is deliberately absent. Nothing here may be decided by it: a
 * source clock can jump, repeat and run backwards, and two sources have two clocks, so ordering by it
 * would silently reorder data — {@code order} is the only thing anything is decided by. A wait here is
 * ended by a parent turning up; an ordinary missing-parent wait has no clock deadline. Separate
 * relocation ownership and abandonment rules remain independent of that wait.
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
        Map<String, ChainPosition> positions,
        ElementRef movedFrom,
        boolean departed) implements Serializable {

    public NestElement {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(positions, "positions");
        positions = Collections.unmodifiableMap(new LinkedHashMap<>(positions));
        fields = fields == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        if (movedFrom != null && !movedFrom.pathId().equals(ref.pathId())) {
            throw new IllegalArgumentException("an element moves within its own embed: " + movedFrom + " -> " + ref);
        }
    }

    /** A change that puts the element where it already was - which is what nearly every change is. */
    public NestElement(ElementRef ref, Map<String, Object> fields, SourceOrder order,
            Map<String, ChainPosition> positions) {
        this(ref, fields, order, positions, null, false);
    }

    /** A change that knows where the element was, and that nothing was sent to where it came from. */
    public NestElement(ElementRef ref, Map<String, Object> fields, SourceOrder order,
            Map<String, ChainPosition> positions, ElementRef movedFrom) {
        this(ref, fields, order, positions, movedFrom, false);
    }

    /** Whether this change removes the element rather than putting a row there. */
    public boolean deletion() {
        return fields == null;
    }

    /**
     * Whether this change says the element used to be somewhere else in the document, which is what a
     * structural key change comes down to once it is read off a row: the same element, addressed
     * differently. Asked instead of comparing refs so that a change putting an element where it already
     * sits is never mistaken for one.
     */
    public boolean moves() {
        return movedFrom != null && !movedFrom.equals(ref);
    }

    /**
     * Whether this change says the element has left here for somewhere else, rather than that it was
     * deleted. The two are told apart by where the change came from and never by what it carries: a
     * deletion is read off a single row and can name no earlier one, so an absent row beside an earlier
     * address can only be the half of a move that stays behind.
     *
     * <p>It cannot be asked as "the address changed" instead. An embed hanging straight off the root is
     * addressed the same way in every document there is - what decides which document it lands in is the
     * key it joins on, which is not part of an address at all - so for those the two addresses are equal
     * and the departure would read as an ordinary edit.
     */
    public boolean departure() {
        return fields == null && movedFrom != null;
    }

    /**
     * Whether something was sent to the key this element used to hang from, saying it has gone. It is the
     * one thing the arriving half cannot work out for itself: whether the element changed documents is
     * decided by the key it joins on, and the level that read the row is the only one that saw both values
     * of it.
     *
     * <p>Without it, an arrival would have to treat every change that knows its old address as one that may
     * be owed a subtree - and most of them are an element renamed inside the document it is already in, for
     * which nothing was ever sent anywhere. Waiting for those is waiting for something that will never come,
     * once per change, for as long as the pipeline runs.
     */
    public boolean departed() {
        return departed;
    }
}
