package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A word to one document that a row it points at has changed, and nothing more.
 *
 * <p>It carries no fields, because it is not a change to the document: what the document should now show
 * is read where it is rendered, out of the namespace the edited row was just filed in. Sending the row
 * itself would put a second copy of it in flight and, where the same row is pointed at from a thousand
 * documents, a thousand of them.
 *
 * <p>{@code key} is the identity of the level doing the pointing, not of the row that changed - it is what
 * the edge is partitioned by, so the word lands on the instance already holding that level, and from there
 * climbs to the document exactly as that level's own rows do. {@code ts} is the edited row's time, so a
 * document redrawn because of it is stamped from the change that caused it rather than from the epoch.
 *
 * <p>{@code positions} is where the edit sat on its own chains. It travels because the edit reaches a sink
 * only inside the documents this wakes: nothing else downstream carries it, so a frontier allowed past it
 * before those documents have gone would leave a change that is neither delivered nor replayable, and every
 * document pointing at that row silently stale after a restart.
 */
public record NestTouch(Object key, long ts, Map<String, ChainPosition> positions) implements Serializable {

    public NestTouch {
        Objects.requireNonNull(key, "key");
        positions = Collections.unmodifiableMap(new LinkedHashMap<>(positions));
    }

    /** The same word, addressed to the level above. */
    public NestTouch routedBy(Object key) {
        return new NestTouch(key, ts, positions);
    }
}
