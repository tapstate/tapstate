package io.tapstate.runtime.probe;

import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowserPreview;
import io.tapstate.spi.store.DataBrowserQuery;

/**
 * The synchronous call that answers "show me the rows": one bounded read of one collection. It is a
 * one-shot request/response — a request goes over, the rows it matched come back — which is why it
 * crosses this seam rather than decoupling through the store, and why nothing survives the call on
 * the runtime side for a later one to resume from.
 *
 * <p>The request names a collection, a filter, an optional order and a bound, and deliberately
 * nothing else. There is no
 * command to dispatch on, so no request spelling reaches anything but a query; there is no database,
 * so a read reaches only what the connection already points at. Both omissions are the seam's, not
 * the caller's to restore.
 *
 * <p>The whitelist of such calls is a closed set of seven — the connection probe, the discovery probe,
 * the four data-browser calls, and the Pipeline preview probe. Any further synchronous control-to-runtime call is a
 * deliberate widening of the seam that must change the gate and the sync-whitelist decision, not slip
 * in beside them.
 */
public interface DataBrowserFindProbe {

    /**
     * Runs {@code query} against {@code config}'s own database and returns the rows it matched, along
     * with what could be told cheaply about how much was left behind.
     */
    DataBrowserPreview find(ConnectionConfig config, DataBrowserQuery query);
}
