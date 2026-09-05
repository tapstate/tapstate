package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * One edge arriving at a nest vertex: which stream travels it, and where that stream's rows carry the
 * value the vertex is keyed by. The {@code ordinal} is the inbound ordinal the edge is drawn on, which
 * is how a processor tells the rows apart once they arrive - the ordinal alone says which embed it is
 * looking at, so nothing has to be inferred from the row.
 *
 * <p>{@code keyFields} names the fields the key is read off, positionally matched to the vertex's
 * partition key, and {@code elementKey} the fields that identify one element of this stream inside the
 * document it lands in. A cascading edge carries changes an upstream vertex already routed and whose
 * place in the document it already settled, so it names neither, nor an alias: those absences are the
 * same fact, and {@link #isCascade()} is the one way to ask it.
 *
 * <p>{@code tracksKeyChanges} is the author's switch for this stream, carried here rather than looked up
 * because the ordinal already decides everything else about a row and this is one more thing decided at
 * compile time. {@code table} is the source table behind the alias, resolved once while compiling so a
 * failure can name what has to be reconfigured rather than only what the author wrote. Both are absent on
 * a cascading edge: no row of a source arrives on one, so there is nothing to track and no table to name.
 */
public record NestInbound(int ordinal, String alias, List<String> pathId, List<String> keyFields,
        List<String> elementKey, boolean tracksKeyChanges, String table,
        boolean carriesDepartures, boolean carriesTouches) implements Serializable {

    public NestInbound {
        Objects.requireNonNull(pathId, "pathId");
        pathId = List.copyOf(pathId);
        keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        elementKey = elementKey == null ? List.of() : List.copyOf(elementKey);
        if ((alias == null) != keyFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "an edge either names the stream it carries and the fields keying it, or neither");
        }
        if (alias == null && tracksKeyChanges) {
            throw new IllegalArgumentException("no row arrives on a cascading edge to track key changes on");
        }
        if (carriesDepartures && !tracksKeyChanges) {
            throw new IllegalArgumentException(
                    "only a stream whose key changes are followed has departures to carry");
        }
        if (carriesTouches && (carriesDepartures || tracksKeyChanges)) {
            throw new IllegalArgumentException(
                    "no row of a stream arrives on a touch edge to track or to re-key");
        }
    }

    /** An edge over rows nobody asked to have key changes followed on - the shape most of them are. */
    public NestInbound(int ordinal, String alias, List<String> pathId, List<String> keyFields,
            List<String> elementKey) {
        this(ordinal, alias, pathId, keyFields, elementKey, false, null, false, false);
    }

    /** An edge carrying rows as they now are, which is what all but the departure edges carry. */
    public NestInbound(int ordinal, String alias, List<String> pathId, List<String> keyFields,
            List<String> elementKey, boolean tracksKeyChanges, String table) {
        this(ordinal, alias, pathId, keyFields, elementKey, tracksKeyChanges, table, false, false);
    }

    /**
     * The edge a level is told on that a row it points at has changed. It arrives from the vertex that
     * files those rows rather than from any source, so the alias it names is the pointed-at stream and is
     * here for one thing: what a level may promise about a chain is worked out per edge, and this is the
     * only edge in the whole tree over which that stream's chain ever reaches a document.
     *
     * <p>{@code keyFields} names what identifies this level's own rows - the same key the word is routed by.
     * It is read off the word rather than off a row, because there is no row: the vertex that sends it
     * worked the key out from what it had recorded about who points where.
     */
    public static NestInbound touchesOf(int ordinal, String alias, List<String> pathId,
            List<String> referrerIdentity) {
        return new NestInbound(ordinal, alias, pathId, referrerIdentity, List.of(), false, null, false, true);
    }

    /**
     * The twin of a tracked edge, carrying the same rows keyed by where they <em>were</em>.
     *
     * <p>It exists because a row whose join key changed only ever arrives where its new key belongs, while
     * the state it is leaving is held by another instance of this vertex. Reaching for that state across
     * instances loses writes; being sent the row a second time, keyed by the value it is leaving, puts the
     * work where the state already is. The upstream needs no say in it - a processor emits to every
     * outbound edge it has, so a second edge is a second delivery.
     *
     * <p>Every row travels it, not only the ones that moved: an edge cannot filter. What tells them apart is
     * that the two keys differ, and for a row carrying no earlier image they never do.
     */
    public static NestInbound departuresOf(int ordinal, NestInbound tracked) {
        return new NestInbound(ordinal, tracked.alias(), tracked.pathId(), tracked.keyFields(),
                tracked.elementKey(), tracked.tracksKeyChanges(), tracked.table(), true, false);
    }

    /** Whether these rows arrive already stamped with the parent key by the vertex one level down. */
    public boolean isCascade() {
        return alias == null;
    }
}
