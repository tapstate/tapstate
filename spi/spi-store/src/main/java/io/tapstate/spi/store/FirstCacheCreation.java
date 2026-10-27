package io.tapstate.spi.store;

/** Non-serializable, implementation-private invocation delivered only to the confirmed first binding winner. */
public interface FirstCacheCreation {
    CachePreparation preparation();
    /** Consumed before callbacks or IO; loading configuration facts never reconstructs this capability. */
    void consume();
    /** Original CONFIG authority and lifetime proof; check outside protected CACHE callbacks. */
    void requireActive();
}
