package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.ViewResource;
import io.tapstate.runtime.probe.DataBrowserCollectionsProbe;
import io.tapstate.runtime.probe.DataBrowserFindProbe;
import io.tapstate.runtime.probe.DataBrowserStatsProbe;
import io.tapstate.runtime.probe.DataBrowserTailProbe;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowserQuery;
import io.tapstate.spi.store.DataBrowserSubscription;
import io.tapstate.spi.store.DataBrowserChange;
import io.tapstate.spi.store.DataBrowserChangeListener;
import io.tapstate.spi.store.DataBrowserTailRequest;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * The control plane's three data-browser verbs: list a declared source's collections, report one
 * collection's size, read one collection's rows. Each resolves the source to the connection its reads
 * run on and drives one of the whitelisted runtime probes; the probes are injected, so when the
 * control and runtime roles split the same calls travel across instances.
 *
 * <p>Nothing here persists and nothing is audited — these verbs look, they do not touch. That is what
 * separates them from the two connection probes, which store what they found.
 *
 * <p>A browsable source is any stored {@link SourceResource}, and the collections are the ones its
 * connected database actually holds rather than the ones it declared. Those are different sets, and
 * the declared one is the wrong answer: a source referenced purely as a connection supplier may
 * declare no tables at all, which is exactly the shape the bundled preview store ships in.
 *
 * <p>Which database a read reaches follows from that source's own connection. The probes take no
 * database and the query request has no field for one, so nothing a caller sends can move a read off
 * the connection it was resolved from — the control plane's own tables sit on the same server.
 *
 * <p>What crosses in and out is the control ring's own vocabulary, never the storage ports': the surfaces
 * send a {@link DataBrowserSortOrder} and receive a {@link DataBrowserStatsReport} /
 * {@link DataBrowserPreviewReport}, and this class is where those meet the port types. A face that had to
 * name a port type to read a row would be a face reaching past this service.
 */
public final class DataBrowserService {

    /** How many rows a read that asks for no particular number is served. */
    public static final int DEFAULT_LIMIT = 10;

    /**
     * The most rows one read will serve. What keeps a preview a preview: rows are read whole into
     * memory in one shot and handed to a terminal, so an unbounded request would be a way to pull a
     * collection through this face. Taking everything is what the query service is for.
     */
    public static final int MAX_LIMIT = 200;

    private final ArtifactStore store;
    private final SchemaStore schemaStore;
    private final DataBrowserCollectionsProbe collectionsProbe;
    private final DataBrowserStatsProbe statsProbe;
    private final DataBrowserFindProbe findProbe;
    private final DataBrowserTailProbe tailProbe;

    public DataBrowserService(
            ArtifactStore store,
            SchemaStore schemaStore,
            DataBrowserCollectionsProbe collectionsProbe,
            DataBrowserStatsProbe statsProbe,
            DataBrowserFindProbe findProbe,
            DataBrowserTailProbe tailProbe) {
        this.store = Objects.requireNonNull(store, "store");
        this.schemaStore = Objects.requireNonNull(schemaStore, "schemaStore");
        this.collectionsProbe = Objects.requireNonNull(collectionsProbe, "collectionsProbe");
        this.statsProbe = Objects.requireNonNull(statsProbe, "statsProbe");
        this.findProbe = Objects.requireNonNull(findProbe, "findProbe");
        this.tailProbe = Objects.requireNonNull(tailProbe, "tailProbe");
    }

    /**
     * Lists the collections the source's own database holds, in the order the connector reports them,
     * each with what is known about it beyond its name.
     *
     * <p>The three derived answers are read from what already exists rather than produced here. The
     * fields come from the latest discovery stored for this source's connection, so listing never
     * triggers one — that would turn a read into a write, and an audited one. The kind and the
     * description both come from the view that declares the collection as where it materializes.
     * None of the three has a source for every collection, and where there is none the answer is
     * left out rather than emptied or invented: an empty field list says the collection has no
     * fields, which is not what "nobody looked" means, and a kind of "view" on a collection somebody
     * made by hand says a pipeline materializes it, which is not what "nobody declared it" means.
     */
    public List<DataBrowserCollection> collections(String sourceId) {
        ConnectionConfig config = connection(sourceId);
        List<String> names = collectionsProbe.collections(config);
        Map<String, List<String>> discovered = discoveredFields(config.id());
        Map<String, ViewResource> declared = declaringViews();
        List<DataBrowserCollection> listed = new ArrayList<>(names.size());
        for (String name : names) {
            ViewResource declaring = declared.get(name);
            listed.add(new DataBrowserCollection(
                    name,
                    declaring == null ? null : DataBrowserCollection.VIEW,
                    discovered.get(name),
                    authoredDescription(declaring)));
        }
        return List.copyOf(listed);
    }

    /**
     * The top-level field names the latest discovery on this connection reported, by collection. A
     * connection nothing has been discovered on contributes no entries, so every collection on it is
     * left without fields rather than given an empty list.
     */
    private Map<String, List<String>> discoveredFields(String connectionId) {
        Optional<DiscoveredSourceModel> latest = schemaStore.get(connectionId);
        if (latest.isEmpty()) {
            return Map.of();
        }
        Map<String, List<String>> byCollection = new LinkedHashMap<>();
        for (SourceTable table : latest.get().model().tables()) {
            byCollection.put(table.name(), table.fields().stream().map(SourceField::name).toList());
        }
        return byCollection;
    }

    /**
     * The view declaring each collection as its warm destination, by collection name. A view that
     * declares no warm collection names none, so it contributes no entry; a collection absent from
     * this map is one no view declares, which is what leaves it without a kind.
     *
     * <p>Whether the declaration says anything about the collection is a separate question, answered
     * by {@link #authoredDescription} — a view is a view whether or not its author wrote a sentence.
     */
    private Map<String, ViewResource> declaringViews() {
        Map<String, ViewResource> byCollection = new LinkedHashMap<>();
        for (Resource stored : ReadableArtifactInventory.list(store)) {
            if (stored instanceof ViewResource view
                    && view.storage() != null
                    && view.storage().warm() != null) {
                byCollection.putIfAbsent(view.storage().warm().collection(), view);
            }
        }
        return byCollection;
    }

    /** What the declaring view's author wrote, or null for no view or nothing written. */
    private static String authoredDescription(ViewResource declaring) {
        if (declaring == null || declaring.metadata() == null) {
            return null;
        }
        String written = declaring.metadata().description();
        return written == null || written.isBlank() ? null : written;
    }

    /** Reports what the connector knows about one of the source's collections. */
    public DataBrowserStatsReport stats(String sourceId, String collection) {
        ConnectionConfig config = requireCollection(sourceId, collection);
        return DataBrowserStatsReport.from(statsProbe.stats(config, collection));
    }

    /**
     * Reads up to {@code limit} rows matching {@code filter} from one of the source's collections, in
     * {@code sort}'s order when one is asked for.
     *
     * <p>Both {@code sort} and {@code limit} are nullable, and null is a request rather than a gap: no
     * order asked for means the database's own, and no size asked for means this face's default. They
     * are settled here, once, because every surface that offers this verb reaches it — a default per
     * surface would be several defaults drifting apart under a face whose whole claim is that they
     * are one request.
     */
    public DataBrowserPreviewReport find(
            String sourceId,
            String collection,
            DataBrowserCriteria filter,
            DataBrowserSortOrder sort,
            Integer limit) {
        int bound = limit == null ? DEFAULT_LIMIT : limit;
        // Before the collection is resolved, which costs a round trip to the connector: a request that
        // cannot be served whatever comes back should not pay for it, or reach a connector at all.
        if (bound < 1 || bound > MAX_LIMIT) {
            throw new TapstateException(
                    DataBrowserError.INVALID_LIMIT,
                    Map.of("limit", String.valueOf(bound), "max", String.valueOf(MAX_LIMIT)),
                    null);
        }
        requireOrderable(sort);
        ConnectionConfig config = requireBrowsable(connection(sourceId));
        requireHolds(config, sourceId, collection);
        DataBrowserQuery query = new DataBrowserQuery(
                collection,
                filter == null ? null : filter.toPortRequest(),
                sort == null ? null : sort.toPortRequest(),
                bound);
        return DataBrowserPreviewReport.from(findProbe.find(config, query));
    }

    /**
     * Refuses an order on a field whose own name holds a dot.
     *
     * <p>A filter can name such a field, because a query may carry an expression and an expression can
     * name a field instead of pathing to it. An order has no second form: a sort key is a path and only
     * a path, so the request would travel as a path resolving nowhere, every row would sort equal, and
     * the rows would come back in no particular order with nothing reported. That is the same silence
     * this whole face is built to remove, so it is refused rather than served.
     *
     * <p>Before the collection is resolved, for the same reason the row bound is: a request that cannot
     * be served should not pay for a round trip to the connector first.
     */
    private static void requireOrderable(DataBrowserSortOrder sort) {
        if (sort == null || ReadableField.of(sort.field()).isPlainPath()) {
            return;
        }
        throw new TapstateException(
                DataBrowserError.UNORDERABLE_FIELD,
                // As the reader wrote it. Named by its parsed form they would go looking for a different
                // field, one that may well also exist.
                Map.of("field", sort.field()),
                null);
    }

    /**
     * Follows one of the source's collections, delivering each change that {@code filter} admits to
     * {@code listener} until the returned subscription is closed. A null filter admits everything.
     *
     * <p>The same confinement the reads get, applied again on a different road. The follow reaches the
     * store through another function entirely, and that function takes its table from whoever calls it
     * — so the check that the collection is one this source's own database holds has to happen here
     * too. A rule enforced on one of two paths is not a rule.
     *
     * <p>The filter is applied here, to each change as it arrives, and that is a choice with a cost
     * worth stating: it narrows what the caller is shown, never what the store captures. Everything
     * the collection does is still streamed and still costs what it costs. It is not an oversight that
     * it is not pushed down — the read function behind this takes a table, not a query, so there is
     * nowhere to push it to.
     */
    public DataBrowserFollow tail(
            String sourceId,
            String collection,
            DataBrowserCriteria filter,
            DataBrowserChangeSink sink) {
        Objects.requireNonNull(sink, "sink");
        ConnectionConfig config = requireBrowsable(connection(sourceId));
        requireHolds(config, sourceId, collection);
        DataBrowserSubscription subscription = tailProbe.tail(
                config, new DataBrowserTailRequest(collection), new DataBrowserChangeListener() {
            @Override
            public void onChange(DataBrowserChange change) {
                // Tested against the row as it now is, or as it was when that is all the change
                // carries. A change with neither is admitted: withholding it because there was
                // nothing to test would drop an event the store really made, which is what a follow
                // exists to report.
                if (filter == null || change.subject() == null || filter.matches(change.subject())) {
                    sink.onChange(DataBrowserChangeEvent.from(change));
                }
            }

            @Override
            public void onError(Throwable error) {
                // The stream ended and cannot be restarted from here. What the face is told is that
                // it ended, not what broke: this reaches somebody watching a collection, while the
                // connector's own account of the failure belongs in the log, where the words it used
                // survive intact.
                sink.onEnded(DataBrowserError.FOLLOW_STOPPED);
            }
        });
        return subscription::close;
    }

    /**
     * The connection a read of {@code collection} runs on, refused with a code if that collection is
     * not one the source's database holds.
     *
     * <p>The check happens here, before the read, because the read cannot report it afterwards. A query
     * against a collection that does not exist comes back empty — the same answer as a collection that
     * holds nothing — so a caller who mistyped a name would be told, indistinguishably, that their data
     * is not there. Paying one listing per read buys a refusal that names what was wrong.
     */
    private ConnectionConfig requireCollection(String sourceId, String collection) {
        ConnectionConfig config = connection(sourceId);
        requireHolds(config, sourceId, collection);
        return config;
    }

    private void requireHolds(ConnectionConfig config, String sourceId, String collection) {
        if (!collectionsProbe.collections(config).contains(collection)) {
            throw new TapstateException(
                    DataBrowserError.UNKNOWN_COLLECTION,
                    Map.of("source", sourceId, "collection", collection),
                    null);
        }
    }

    /**
     * The connectors a request for rows may be sent to, and the only place that set is written down.
     *
     * <p>A closed set rather than something derived, because there is nothing to derive it from: the
     * command a row read travels on is registered by most of the catalogue, and registering it says
     * only that the connector will be handed the request, not that it can read one. Adding a connector
     * here is therefore a decision somebody makes and a reviewer sees, which is the same shape the
     * synchronised-operation set takes and for the same reason.
     */
    private static final Set<String> BROWSABLE_CONNECTORS = Set.of("mongodb");

    /**
     * Refuses a row read against a connector this face cannot ask in, before anything is sent.
     *
     * <p>Before, not after, and the difference is the whole value: sent anyway, the request reaches a
     * driver that cannot find a statement in it and fails inside the connector, so what comes back is a
     * server failure quoting a driver error about the shape of a query nobody wrote. Refused here it is
     * a judgement about the request, made by the layer that knows the answer, in words that name the
     * source's connector and what can be browsed instead.
     */
    private static ConnectionConfig requireBrowsable(ConnectionConfig config) {
        if (!BROWSABLE_CONNECTORS.contains(config.connectorId())) {
            throw new TapstateException(
                    DataBrowserError.CONNECTOR_NOT_BROWSABLE,
                    Map.of("connector", config.connectorId(),
                            "browsable", String.join(", ", new TreeSet<>(BROWSABLE_CONNECTORS))),
                    null);
        }
        return config;
    }

    /** The stored source's connection, or a coded not-found naming the id the caller asked for. */
    private ConnectionConfig connection(String sourceId) {
        Objects.requireNonNull(sourceId, "sourceId");
        SourceResource source = store.get(sourceId)
                .filter(SourceResource.class::isInstance)
                .map(SourceResource.class::cast)
                .orElseThrow(() -> new TapstateException(
                        SourceError.NOT_FOUND, Map.of("id", sourceId), null));
        return new ConnectionConfig(source.id(), source.connector(), source.config());
    }
}
