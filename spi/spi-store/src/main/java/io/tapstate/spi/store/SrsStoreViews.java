package io.tapstate.spi.store;

/** Opens SRS facets for an already captured business identity, without creating or selecting a newer scope. */
@FunctionalInterface
public interface SrsStoreViews {
    String USER_CONTEXT_KEY = "tapstate.scoped-srs-stores";
    SrsStores srs(CacheBusinessScope originalScope,Runnable requireActive);

    default boolean supportsAdmittedCaptureStartup() { return false; }

    /** Uses admitted structures only during a synchronous startup step on the original CACHE. */
    default <T> T withCaptureStartup(CacheBusinessScope scope, java.util.function.Supplier<T> step) {
        return step.get();
    }

    /** Opens the private-channel facet under the same original business owner, without creating history. */
    default DirectChannelStore directChannels(CacheBusinessScope originalScope,Runnable requireActive) {
        throw new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED,java.util.Map.of(),null);
    }
}
