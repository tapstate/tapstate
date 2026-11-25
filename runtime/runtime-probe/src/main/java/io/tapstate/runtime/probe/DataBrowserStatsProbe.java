package io.tapstate.runtime.probe;

import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowserTableInfo;

/**
 * The synchronous call that answers "how much is in there": what a connector reports about one
 * collection's size. It reads the store's own metadata rather than counting, so it stays cheap on a
 * large collection — which is why it is a channel of its own instead of something a read could
 * compute from the rows it returned.
 *
 * <p>Its signature carries no database, so this path reaches only the database the connection already
 * points at; the collection is a name within that database, never a way to name another one.
 *
 * <p>The whitelist of such calls is a closed set of seven — the connection probe, the discovery probe,
 * the four data-browser calls, and the Pipeline preview probe. Any further synchronous control-to-runtime call is a
 * deliberate widening of the seam that must change the gate and the sync-whitelist decision, not slip
 * in beside them.
 */
public interface DataBrowserStatsProbe {

    /** Reports what {@code config}'s connector knows about {@code collection}'s size. */
    DataBrowserTableInfo stats(ConnectionConfig config, String collection);
}
