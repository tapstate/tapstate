package io.tapstate.runtime.probe;

import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowserChangeListener;
import io.tapstate.spi.store.DataBrowserSubscription;
import io.tapstate.spi.store.DataBrowserTailRequest;

/**
 * The synchronous call that answers "show me what happens next": one follow of one collection. It is
 * the whitelist's only member that does not end when it returns — what comes back is a handle on a
 * stream that keeps delivering until it is closed.
 *
 * <p>That makes it a synchronous call all the same, and the reason it crosses this seam rather than
 * decoupling through the store is the same as its neighbours': it is triggered on demand and its
 * result is waited for. What survives the call is the stream, not a request somebody has to go and
 * collect.
 *
 * <p>The request names a collection and deliberately nothing else. There is no database, so a follow
 * reaches only what the connection already points at; there is no filter, because the read function
 * behind it takes a table rather than a query, and a filter the seam accepted and could not honour
 * would be worse than one it never offered. Narrowing what a reader sees happens at the far end of
 * the listener.
 *
 * <p>The whitelist of such calls is a closed set of seven — the connection probe, the discovery probe,
 * the four data-browser calls, and the Pipeline preview probe. Any further synchronous control-to-runtime call is a
 * deliberate widening of the seam that must change the gate and the sync-whitelist decision, not slip
 * in beside them.
 */
public interface DataBrowserTailProbe {

    /**
     * Follows {@code request}'s collection on {@code config}'s own database, delivering each change
     * to {@code listener} until the returned subscription is closed. Whoever starts it owns closing
     * it: a stream nobody closes does not fail, it holds a connector open.
     */
    DataBrowserSubscription tail(
            ConnectionConfig config, DataBrowserTailRequest request, DataBrowserChangeListener listener);
}
