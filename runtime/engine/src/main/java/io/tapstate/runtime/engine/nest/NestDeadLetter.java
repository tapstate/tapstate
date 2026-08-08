package io.tapstate.runtime.engine.nest;

import java.io.Serializable;

/**
 * Where changes that can never reach a document go.
 *
 * <p>A child whose parent row is known deleted, and a child still waiting when that parent is deleted,
 * are both past saving: no later event will give them a document to sit in. They must still go somewhere.
 * Dropping them loses data that no assertion about a document can ever see, because those rows were never
 * going to appear in one - and holding them instead would pin the durable frontier for events that will
 * never be emitted.
 *
 * <p>Whoever builds the vertex decides where they go; the vertex only insists that they go.
 */
@FunctionalInterface
public interface NestDeadLetter extends Serializable {

    /** Hands over a change whose parent row is known to be gone. */
    void parentAbsent(NestElement element);
}
