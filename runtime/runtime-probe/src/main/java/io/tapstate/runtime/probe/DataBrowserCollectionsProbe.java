package io.tapstate.runtime.probe;

import io.tapstate.spi.store.ConnectionConfig;
import java.util.List;

/**
 * The synchronous call that answers "what is in there": the collections a source's own database
 * exposes. Like the connection test and the schema discovery it drives the target connector against
 * the live source, so it runs where the connectors run and crosses the same narrow seam rather than
 * decoupling through the store.
 *
 * <p>Listing is not discovery, and the two do not fold into one channel. Discovery reports the
 * modelled shape a source was declared with; this reports what the connected database holds right
 * now, which is what a user browsing their data is asking about — a source may be referenced purely
 * as a connection supplier, declaring no tables at all, and still have collections to list.
 *
 * <p>The whitelist of such calls is a closed set of seven — the connection probe, the discovery probe,
 * the four data-browser calls, and the Pipeline preview probe. Any further synchronous control-to-runtime call is a
 * deliberate widening of the seam that must change the gate and the sync-whitelist decision, not slip
 * in beside them.
 */
public interface DataBrowserCollectionsProbe {

    /** Lists the collections {@code config}'s own database holds, in the order the connector reports them. */
    List<String> collections(ConnectionConfig config);
}
