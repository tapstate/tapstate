package io.tapstate.spi.store;

/** Opens SRS facets for an already captured business identity, without creating or selecting a newer scope. */
@FunctionalInterface
public interface SrsStoreViews {
    String USER_CONTEXT_KEY = "tapstate.scoped-srs-stores";
    SrsStores srs(CacheBusinessScope originalScope,Runnable requireActive);

    /** Opens the private-channel facet under the same original business owner, without creating history. */
    default DirectChannelStore directChannels(CacheBusinessScope originalScope,Runnable requireActive) {
        throw new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED,java.util.Map.of(),null);
    }
}
