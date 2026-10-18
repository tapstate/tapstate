package io.tapstate.spi.store;

/** Opens SRS facets for an already captured business identity, without creating or selecting a newer scope. */
@FunctionalInterface
public interface SrsStoreViews {
    String USER_CONTEXT_KEY = "tapstate.scoped-srs-stores";
    SrsStores srs(CacheBusinessScope originalScope,Runnable requireActive);
}
