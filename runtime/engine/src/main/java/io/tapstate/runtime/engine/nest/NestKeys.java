package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reading the few things a nest vertex needs off an event, the same way at every vertex. */
final class NestKeys {

    private NestKeys() {
    }

    /**
     * The values {@code fields} name on {@code row}, in that order. A key is always a list even when one
     * field long, so a composite key and a single one are the same kind of value everywhere downstream.
     * Nulls are kept rather than rejected: a key column that is null is a data problem to be seen in the
     * document, not an invariant for this to decide.
     */
    static List<Object> valuesOf(Map<String, Object> row, List<String> fields) {
        List<Object> values = new ArrayList<>(fields.size());
        for (String field : fields) {
            values.add(row.get(field));
        }
        return Collections.unmodifiableList(values);
    }

    /** The row an event carries: what it became, or what it was when that is all a deletion leaves. */
    static Map<String, Object> rowOf(Envelope event) {
        Map<String, Object> row = event.after() != null ? event.after() : event.before();
        if (row == null) {
            throw new IllegalStateException("event " + event.op() + " on " + event.src() + " carries no row");
        }
        return row;
    }

    /**
     * The order to compare this event on. Absent is an engine invariant violation rather than a
     * diagnosable error: an event reaching a stateful node without one cannot be placed against what is
     * already there, and guessing would silently reorder data.
     */
    static SourceOrder orderOf(Envelope event) {
        ChainPosition at = event.position();
        return Objects.requireNonNull(at == null ? null : at.order(),
                "event on " + event.src() + " reached a stateful node with no order");
    }

    /** Whether this event removes what it names rather than putting a row there. */
    static boolean isDeletion(Envelope event) {
        return event.op() == Op.DELETE;
    }

    /**
     * Stops the job on an update that arrives without the row it replaces, where the author asked for
     * structural key changes to be followed on this stream.
     *
     * <p>The after image alone cannot answer the only question that matters here. A row that moved to
     * another parent and a row that had an unrelated column edited arrive looking the same - a row sitting
     * where it now sits - and following the first as though it were the second writes the element into its
     * new place while leaving it in the old one, so the document keeps a copy the source no longer has.
     * Nothing downstream can notice that, which is why it fails here instead of being worked around.
     *
     * <p>Only updates are refused. An insert has no earlier row at all and a deletion carries one as the
     * only row it has, so refusing either would be refusing the shape of the event rather than a source
     * that sends too little.
     */
    static void requireBeforeImageWhereKeysAreTracked(NestInbound edge, Envelope event) {
        if (!edge.tracksKeyChanges() || event.op() != Op.UPDATE || event.before() != null) {
            return;
        }
        throw new TapstateException(NestError.KEY_CHANGE_TRACKING_REQUIRES_BEFORE_IMAGE,
                Map.of("alias", edge.alias(), "table", edge.table()), null);
    }

    /**
     * Stops the job on an update of a stream whose rows are recorded against what they point at, where
     * that update arrives without the row it replaces.
     *
     * <p>Where a row points is read off the row itself, so recording it needs nothing more. Taking that
     * record back out is the other half, and only the earlier row can say which entry to take it out of -
     * an update naming a different row and an update that only edited a column arrive looking the same.
     *
     * <p><b>Refused rather than passed over, because passing over it is invisible.</b> Every document
     * still renders correctly and every count downstream is right; what grows is the record of who points
     * where, and nothing reads that out loud. Left alone it surfaces as one of two things much later - a
     * row nothing points at any more kept for the life of the job, or an edit refused on a fanout that was
     * never real.
     *
     * <p>Only updates are refused, for the same reason as the tracking above: an insert points somewhere
     * for the first time and leaves nothing behind, and a deletion is taken out on the other edge, where it
     * happens whether this one carried an earlier row or not.
     */
    static void requireBeforeImageWhereReferencesAreRecorded(NestLookup lookup, Envelope event) {
        if (event.op() != Op.UPDATE || saysWhereItPointed(event.before(), lookup)) {
            return;
        }
        throw new TapstateException(NestError.REFERENCE_TRACKING_REQUIRES_BEFORE_IMAGE,
                Map.of("alias", lookup.referrerAlias(), "refPath", NestTopology.render(lookup.pathId())),
                null);
    }

    /**
     * Whether an earlier row says enough to find the entry to take out - the columns holding the
     * reference, and the ones identifying the row making it.
     *
     * <p><b>Which columns are there, not whether the row is.</b> A change stream with no pre-image
     * configured sends an earlier row that is present and holds nothing, which is the shape most sources
     * actually produce and is a different value from none at all. Read as a row it says this one used to
     * point at null, so the entry taken out is one nobody ever wrote while the real one stays - the same
     * leak, reached through the branch that looks like it is handling it.
     *
     * <p>A column that is genuinely null is left alone, which is why this asks for the key and never for
     * the value: the column is present, so the key built from it is the key the entry was written under.
     */
    private static boolean saysWhereItPointed(Map<String, Object> was, NestLookup lookup) {
        return was != null && was.keySet().containsAll(lookup.referenceFields())
                && was.keySet().containsAll(lookup.referrerIdentity());
    }

    /**
     * The row this event replaces, where there is one to compare against and the author asked for it to be
     * compared. Absent everywhere else, which is what keeps a tree that tracks nothing on exactly the path
     * it was on before: no comparison is made, so no source has to send anything more.
     *
     * <p>An update is the only event with two rows to compare. An insert puts an element somewhere for the
     * first time and a deletion takes one away, and neither can be a row addressed one way arriving as the
     * same row addressed another.
     */
    static Map<String, Object> replacedRow(NestInbound edge, Envelope event) {
        return edge.tracksKeyChanges() && event.op() == Op.UPDATE ? event.before() : null;
    }
}
