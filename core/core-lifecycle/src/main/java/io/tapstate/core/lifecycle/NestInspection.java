package io.tapstate.core.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What a nest step holds for one root key, as a rendered view rather than the stored bytes: whether the root
 * row has arrived and at what source order, every part attached beneath it with its own order, the parts held
 * waiting for a row they hang under, the references the document points at that have no row, and the
 * document as it last went out or will next go out.
 *
 * <p>The view is versioned by {@link #VERSION}; the stored state behind it is free to change shape without
 * this one changing, which is the point of rendering it. A reader keys on the version, never on how the
 * state happens to be laid out.
 *
 * <p>A source order is written {@code epoch:seq}, the two numbers a change is ordered by: a higher one wins.
 *
 * @param version the shape of this view
 * @param pipelineId the pipeline
 * @param step the nest step
 * @param rootKeyColumns the columns the root key is made of, in order
 * @param rootKey the root key asked about, as typed values
 * @param rootPresent whether the root row is in the document now - false before it arrives and after it is
 *     deleted, and the parts held under the key then wait for it
 * @param rootOrder the source order of the root row, or null when no root row was ever taken in
 * @param parts every part held beneath the root, depth first
 * @param waiting parts held for a parent row that has not arrived, by the parent they wait for
 * @param missing references the document points at whose row was not found
 * @param unsent how many changes taken in have not yet gone out in a document; changes to the root row, or to a
 *     row it points at, are folded into one rewrite and count once together
 * @param inTransit whether a subtree is being handed between this document and another
 * @param document the document as it last went out when {@code unsent} is zero, or as it will next go out
 *     otherwise; null while the root is absent, since no document exists then
 */
public record NestInspection(int version, String pipelineId, String step, List<String> rootKeyColumns,
        List<Object> rootKey, boolean rootPresent, String rootOrder, List<Part> parts, List<Waiting> waiting,
        List<Missing> missing, long unsent, boolean inTransit, Map<String, Object> document) {

    /** The shape this view is written in. Bumped whenever a field changes meaning or goes. */
    public static final int VERSION = 1;

    public NestInspection {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(step, "step");
        rootKeyColumns = List.copyOf(rootKeyColumns);
        rootKey = Collections.unmodifiableList(new ArrayList<>(rootKey));
        parts = List.copyOf(parts);
        waiting = List.copyOf(waiting);
        missing = List.copyOf(missing);
        document = document == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(document));
    }

    /**
     * One part attached beneath the root.
     *
     * @param path the dotted path of embeds from the root down to the part
     * @param key the part's own key values
     * @param order the source order of the part's row
     * @param deleted whether the part is a record of a deletion: gone from the document, kept so that a
     *     replay cannot bring it back
     */
    public record Part(String path, List<Object> key, String order, boolean deleted) {
        public Part {
            Objects.requireNonNull(path, "path");
            key = Collections.unmodifiableList(new ArrayList<>(key));
        }
    }

    /**
     * Parts held until the row they hang under arrives.
     *
     * @param path the dotted path of the parent they hang under
     * @param parentKey the parent's identity they wait for, as text
     * @param held how many changes are held for it
     */
    public record Waiting(String path, String parentKey, int held) {
        public Waiting {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(parentKey, "parentKey");
        }
    }

    /**
     * A reference the document points at whose row was not found.
     *
     * @param path the dotted path of the reference
     * @param key the key pointed at
     */
    public record Missing(String path, List<Object> key) {
        public Missing {
            Objects.requireNonNull(path, "path");
            key = Collections.unmodifiableList(new ArrayList<>(key));
        }
    }
}
