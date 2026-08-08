package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.Objects;

/**
 * One element change and the key it is routed by right now - what travels between nest vertices.
 *
 * <p>The key climbs while the change does not. It starts as the identity the element's own row names,
 * and each resolver replaces it with the identity of the level above, until at the assembler it is the
 * key of the document the element belongs to. Keeping it beside the change rather than inside is what
 * lets an element wait for a parent it cannot yet name: a waiting bucket holds changes, not routes.
 *
 * <p>{@link Serializable} because it crosses between members.
 */
public record KeyedElement(Object key, NestElement element) implements Serializable {

    public KeyedElement {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(element, "element");
    }

    /** The same change, routed by the key of the level above. */
    public KeyedElement routedBy(Object key) {
        return new KeyedElement(key, element);
    }
}
