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
 */
public record NestInbound(int ordinal, String alias, List<String> pathId, List<String> keyFields,
        List<String> elementKey) implements Serializable {

    public NestInbound {
        Objects.requireNonNull(pathId, "pathId");
        pathId = List.copyOf(pathId);
        keyFields = keyFields == null ? List.of() : List.copyOf(keyFields);
        elementKey = elementKey == null ? List.of() : List.copyOf(elementKey);
        if ((alias == null) != keyFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "an edge either names the stream it carries and the fields keying it, or neither");
        }
    }

    /** Whether these rows arrive already stamped with the parent key by the vertex one level down. */
    public boolean isCascade() {
        return alias == null;
    }
}
