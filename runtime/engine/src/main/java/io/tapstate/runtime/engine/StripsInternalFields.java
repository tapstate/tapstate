package io.tapstate.runtime.engine;

/**
 * A writer factory whose target may ask to receive nothing the product adds to a row, not even the
 * markers meant for consumers. A factory that wraps another passes the wrapped factory's answer on.
 */
public interface StripsInternalFields {

    /** True when the target opted out of every product-added field. */
    boolean stripsInternalFields();
}
