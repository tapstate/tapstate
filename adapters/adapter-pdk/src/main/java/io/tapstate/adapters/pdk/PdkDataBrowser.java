package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowser;
import io.tapstate.spi.store.DataBrowserFilter;
import io.tapstate.spi.store.DataBrowserFilter.All;
import io.tapstate.spi.store.DataBrowserFilter.Any;
import io.tapstate.spi.store.DataBrowserFilter.Conjunct;
import io.tapstate.spi.store.DataBrowserFilter.Match;
import io.tapstate.spi.store.DataBrowserFilter.Operator;
import io.tapstate.spi.store.DataBrowserPreview;
import io.tapstate.spi.store.DataBrowserChange;
import io.tapstate.spi.store.DataBrowserChangeListener;
import io.tapstate.spi.store.DataBrowserQuery;
import io.tapstate.spi.store.FieldPath;
import io.tapstate.spi.store.DataBrowserSubscription;
import io.tapstate.spi.store.DataBrowserTailRequest;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.spi.store.DataBrowserSort;
import io.tapstate.spi.store.DataBrowserTableInfo;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.functions.connection.GetTableInfoFunction;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.source.ExecuteCommandFunction;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.StringJoiner;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * The PDK bridge for reading a store: it takes a live connector instance from the pool, refuses the
 * read with a code if the connector will not load or does not register the function the verb needs,
 * and drives the three read functions — listing its collections, reporting one collection's size, and
 * running a query. The PDK types stay inside this class; neither the requests nor the results carry
 * any of them.
 *
 * <p>Instances are pooled rather than opened per read, and the reason is not only cost: opening a
 * connector builds a fresh isolated class loader and — once initialized — the connector's own driver
 * connection pool, so a read face that opened one per call could not carry a caller that reads on a
 * timer. Initialization therefore happens once, when the instance enters the pool, not on every read.
 * The pool is live state, so this reader owns a lifecycle: {@link #close()} hands it all back.
 *
 * <p>Unlike the capture read, this face is reachable by a user asking to look at their data, so a
 * connector that does not register the function a verb needs is a coded refusal naming the connector
 * and the capability — not the bare crash a caller-invariant violation would take. A connector that
 * fails while reading is likewise coded.
 *
 * <p>Three properties of the frozen contract shape the drive, and each one fails silently if
 * ignored:
 *
 * <ul>
 *   <li>Both the name listing and the query hand their results to a consumer they may call <em>more
 *       than once</em> — a batch at a time. Keeping only the last call returns a truncated answer
 *       that nothing downstream can tell apart from a complete one.
 *   <li>A query reports its failure through the result it hands back, not only by throwing. Reading
 *       just the rows off that result turns a failed query into an empty page.
 *   <li>A connector fills a param it needs but was not given into the caller's own map, so the map
 *       handed to it is mutable. An immutable one throws — and only on the requests that omit that
 *       param, which is why it stays green until it does not.
 * </ul>
 */
public final class PdkDataBrowser implements DataBrowser {

    /**
     * The one command the read face dispatches. A connector routes writes through this same function
     * under other command names, so pinning it here is what makes the face read-only: it is assembled,
     * never accepted from a caller, so no request spelling reaches anything but a query.
     */
    private static final String QUERY_COMMAND = "executeQuery";

    /** How many collection names to ask for per consumer call; the listing is collected whole regardless. */
    private static final int NAME_BATCH_SIZE = 1000;

    private final ConnectorInstancePool<PdkConnector> pool;
    private final ScheduledExecutorService evictions;

    /**
     * The capture side, driven for follows. A follow is a change stream, and driving one is already
     * solved here: discovering the tables, filling their field types onto the context, deriving a
     * position to start from, and skipping the control events a stream carries to say it is alive. A
     * second copy of that loop in this class would be a second place for any of it to go stale.
     */
    private final CapturePort capture;

    public PdkDataBrowser(ConnectorProvisioner provisioner) {
        this(provisioner, ConnectorInstancePool.DEFAULTS, Clock.systemUTC());
    }

    PdkDataBrowser(ConnectorProvisioner provisioner, ConnectorInstancePool.Limits limits, Clock clock) {
        this.pool = new ConnectorInstancePool<>(
                config -> openAndInitialize(provisioner, config), PdkDataBrowser::shutDown, limits, clock);
        this.capture = new PdkCapturePort(provisioner);
        this.evictions = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "data-browser-eviction");
            thread.setDaemon(true);
            return thread;
        });
        // The pool keeps no timer of its own, so an idle instance is only given up if something asks it
        // to be. Checking on the next read instead would never fire on the face nobody is using, which
        // is exactly the face whose connections should have been handed back.
        long period = Math.max(1, limits.idle().toMillis() / 2);
        evictions.scheduleWithFixedDelay(pool::sweep, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<String> collections(ConnectionConfig config) {
        return pool.call(config, connector -> {
            GetTableNamesFunction names = require(
                    connector, connector.functions().getGetTableNamesFunction(), "getTableNames");
            List<String> collected = new ArrayList<>();
            // The consumer is called once per batch, so every call accumulates - assigning here would
            // return only the connector's last batch and lose every collection before it.
            drive(connector, () -> {
                names.tableNames(connector.context(), NAME_BATCH_SIZE, collected::addAll);
                return null;
            });
            return collected;
        });
    }

    @Override
    public DataBrowserTableInfo stats(ConnectionConfig config, String collection) {
        return pool.call(config, connector -> {
            GetTableInfoFunction info = require(
                    connector, connector.functions().getGetTableInfoFunction(), "getTableInfo");
            TableInfo reported = drive(connector, () -> info.getTableInfo(connector.context(), collection));
            return reported == null
                    ? new DataBrowserTableInfo(null, null, null)
                    : new DataBrowserTableInfo(reported.getNumOfRows(), reported.getStorageSize(), reported.getAvgObjSize());
        });
    }

    @Override
    public DataBrowserPreview find(ConnectionConfig config, DataBrowserQuery query) {
        return pool.call(config, connector -> {
            ExecuteCommandFunction execute = require(
                    connector, connector.functions().getExecuteCommandFunction(), "executeCommand");
            List<Map<String, Object>> rows = new ArrayList<>();
            // A failure arrives through the result rather than as a throw, so it is captured as the drive
            // runs and raised after it - returning the rows collected so far would report a failed query
            // as a short page, which reads exactly like a complete one.
            AtomicReference<Throwable> reported = new AtomicReference<>();
            drive(connector, () -> {
                TapExecuteCommand command = TapExecuteCommand.create()
                        .command(QUERY_COMMAND)
                        .params(params(config, query, beyond(query.limit())));
                execute.execute(connector.context(), command, result -> collect(result, rows, reported));
                return null;
            });
            Throwable failure = reported.get();
            if (failure != null) {
                throw readFailed(connector.connectorId(), failure);
            }
            // A read that was stopped mid-flight returns here looking like one that finished: the loop
            // that gave up neither threw nor reported, and the rows that did arrive are a short answer
            // no other signal contradicts. Asking afterwards is the only thing that separates the two,
            // and this is the one verb it applies to - the name listing never asks about its own
            // liveness, so it cannot end early without saying so.
            if (!connector.isAlive()) {
                throw new TapstateException(ConnectorError.READ_ABANDONED,
                        Map.of("connector", connector.connectorId()), null);
            }
            // The row past the bound arrived, so the collection holds more than this read carries. The
            // row itself is not part of the answer: a caller that asked for ten and is handed eleven has
            // had its bound broken to satisfy a footnote.
            boolean moreAvailable = rows.size() > query.limit();
            if (moreAvailable) {
                rows.subList(query.limit(), rows.size()).clear();
            }
            return new DataBrowserPreview(rows, approximateTotal(connector, query), moreAvailable);
        });
    }

    @Override
    public DataBrowserSubscription tail(
            ConnectionConfig config, DataBrowserTailRequest request, DataBrowserChangeListener listener) {
        // A follow holds its connector for as long as somebody is watching, so it cannot take a pooled
        // instance - but it is still an instance, and it counts where they count.
        ConnectorInstancePool.Reservation slot = pool.reserveOutsidePool();
        try {
            Subscription stream = capture.cdc(
                    new CaptureConfig(config.connectorId(), config.settings(), List.of(request.collection())),
                    new CaptureListener() {
                        @Override
                        public void onEvent(Envelope event) {
                            DataBrowserChange change = project(event);
                            // A stream also carries schema changes, which are not a row changing and
                            // have nowhere to go in a view of rows.
                            if (change != null) {
                                listener.onChange(change);
                            }
                        }

                        @Override
                        public void onError(Throwable error) {
                            listener.onError(error);
                        }
                    });
            AtomicBoolean closed = new AtomicBoolean();
            return () -> {
                if (!closed.compareAndSet(false, true)) {
                    return;
                }
                // The place in the ceiling is given back after the stream is stopped, and in a finally:
                // a close that threw on the way out would otherwise leak the count rather than the
                // connector, which is the half nothing would ever report.
                try {
                    stream.close();
                } finally {
                    slot.close();
                }
            };
        } catch (RuntimeException | Error failedToStart) {
            slot.close();
            throw failedToStart;
        }
    }

    /**
     * One change, as a view of rows sees it. Null for anything that is not a row changing.
     *
     * <p>The three kinds the store distinguishes are kept as three. A reader following a collection
     * is watching what the store did, and "a row appeared" and "a row changed" are different facts
     * about that -- folding them together would be this layer deciding they are not.
     */
    private static DataBrowserChange project(Envelope event) {
        // Both rows are passed through as the connector supplied them, including their absence. How
        // completely a change describes itself is the connector's business, and a layer that filled a
        // gap here would hand the reader an answer that looks like the stream's. The one thing done to
        // them is giving each value a spelling JSON has; nothing is added, dropped or filled in.
        return switch (event.op()) {
            case INSERT, READ -> new DataBrowserChange(
                    DataBrowserChange.Kind.INSERT, null, writable(event.after()), event.ts());
            case UPDATE -> new DataBrowserChange(
                    DataBrowserChange.Kind.UPDATE, writable(event.before()), writable(event.after()),
                    event.ts());
            case DELETE -> new DataBrowserChange(
                    DataBrowserChange.Kind.DELETE, writable(event.before()), null, event.ts());
            case DDL -> null;
        };
    }

    /**
     * The row with every value in a form JSON has a spelling for. A connector hands on whatever its
     * driver produced, and a document store's own key is a driver object rather than a string or a
     * number - so a row can arrive holding values no writer of JSON can write.
     *
     * <p>Rendering it here rather than where it is written is what keeps the failure from being
     * invisible: everything past this seam runs on the connector's stream thread, where a value that
     * cannot be written ends the stream, and a reader following a collection cannot tell an ended
     * stream from a collection nobody is changing.
     *
     * <p>The rule is deliberately connector-blind: anything outside map / list / string / number /
     * boolean is rendered as its own text, whatever its type. A rule per driver type would be exact
     * for the drivers it knew and would go on producing this same failure for the first one it did
     * not. Where the connector did supply a conversion of its own, that conversion has already run
     * and what arrives here is its result inside a carrier; the carrier is unwrapped and the result
     * rendered, so a connector's answer is preferred to this fallback wherever it gave one.
     *
     * <p><b>Both faces of this reader go through here</b>, and that is the point: the query face used
     * to hand the driver's object on untouched while the follow face rendered it, so one document read
     * two ways read as two documents - each face internally consistent, neither losing anything, and
     * nothing anywhere reporting a disagreement.
     */
    private static Map<String, Object> writable(Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        Map<String, Object> written = new LinkedHashMap<>();
        row.forEach((name, value) -> written.put(String.valueOf(name), writableValue(value)));
        return written;
    }

    private static Object writableValue(Object value) {
        return switch (value) {
            case null -> null;
            case Map<?, ?> nested -> {
                Map<String, Object> written = new LinkedHashMap<>();
                nested.forEach((name, held) -> written.put(String.valueOf(name), writableValue(held)));
                yield written;
            }
            case List<?> held -> {
                List<Object> written = new ArrayList<>(held.size());
                held.forEach(each -> written.add(writableValue(each)));
                yield written;
            }
            // A value the connector converted for travel renders as what it converted it to, not as
            // the carrier around it: the carrier's own text spells out its internals and would read
            // as neither the value nor the shape the other face shows.
            case ConvertedValue carried -> writableValue(carried.value());
            case String text -> text;
            case Number number -> number;
            case Boolean flag -> flag;
            default -> String.valueOf(value);
        };
    }



    /** Hands back every pooled connector and stops evicting. */
    @Override
    public void close() {
        evictions.shutdownNow();
        pool.close();
    }

    /**
     * One row past what the caller asked for: whether it arrives is the only honest way to tell a
     * collection that ends at the bound from one that merely reaches it. A full answer looks like the
     * obvious signal and is wrong exactly when the collection holds the bound and not one more.
     *
     * <p>An unbounded request has no row past it to ask for, so it is left as it is; nothing is being
     * held back from a caller that asked for everything.
     */
    private static int beyond(int limit) {
        return limit == Integer.MAX_VALUE ? limit : limit + 1;
    }

    /**
     * How many rows the collection holds, or null when that could not be told cheaply. Offered only
     * for an unfiltered read, and only off the store's own metadata: a connector counts a filtered
     * collection by scanning it, which on a large one spends the whole read budget answering a
     * footnote — and is the one query that cannot be narrowed to finish sooner.
     *
     * <p>A count nobody can supply is a missing footnote, never a failed read. A connector that
     * registers no size function, one that reports nothing, and one that fails reporting all land on
     * null, which this face already reads as "not reported" rather than as zero — refusing instead
     * would deny a working read over a connector that offers one function fewer.
     */
    private static Long approximateTotal(PdkConnector connector, DataBrowserQuery query) {
        if (query.filter() != null) {
            return null;
        }
        GetTableInfoFunction info = connector.functions().getGetTableInfoFunction();
        if (info == null) {
            return null;
        }
        try {
            TableInfo reported = drive(connector, () -> info.getTableInfo(connector.context(), query.collection()));
            return reported == null ? null : reported.getNumOfRows();
        } catch (RuntimeException failedToCount) {
            return null;
        }
    }

    // ---- the pooled instance ---------------------------------------------------------------------

    /**
     * Opens one connector for {@code config} and initializes it, which is the step that builds whatever
     * the connector holds at runtime — for a database connector, its own connection pool. Pooling an
     * un-initialized instance would rebuild that on every read and save nothing.
     */
    private static PdkConnector openAndInitialize(ConnectorProvisioner provisioner, ConnectionConfig config) {
        PdkConnector connector = PdkConnector.open(
                config.connectorId(), provisioner.resolve(config.connectorId()), config.settings());
        try {
            drive(connector, () -> {
                connector.connector().init(connector.context());
                return null;
            });
        } catch (RuntimeException | Error e) {
            shutDown(connector);
            throw e;
        }
        return connector;
    }

    /** Stops and closes one pooled connector; the pool calls this exactly once per instance. */
    private static void shutDown(PdkConnector connector) {
        connector.stopQuietly();
        connector.close();
    }

    // ---- drive helpers ---------------------------------------------------------------------------

    /**
     * The params one query is driven with. Mutable by contract, not by accident: a connector fills a
     * param it needs but was not given into this very map, so an immutable one throws inside the
     * connector — on those requests only.
     *
     * <p>The map carries what the read needs and nothing that would widen it: no command to dispatch
     * on, and no database, so the read reaches only what the connection already points at.
     */
    private static Map<String, Object> params(ConnectionConfig config, DataBrowserQuery query, int limit) {
        Map<String, Object> params = new LinkedHashMap<>();
        // Which database a read may touch follows from the connection and is assembled here, never taken
        // from the request. Omitting it leans on a fallback only one connector has, and a read that lands
        // in the wrong database reports nothing wrong.
        //
        // Only when the connection names one. Nothing validates that it does, and sending the key with a
        // null value is worse than sending nothing: it names no database and, being present, stops the
        // connector filling its own in.
        Object database = config.settings().get("database");
        if (database != null) {
            params.put("database", database);
        }
        params.put("collection", query.collection());
        // A filter is always present, empty meaning every row: a connector hands its absence straight to
        // the driver as a null filter, which the driver rejects.
        params.put("filter", translate(query.filter()));
        // Only when an order was asked for. Absent means the database's own order, and the key has to be
        // left out to say that: a connector reads a present sort and an absent one differently, so an
        // empty map here would be a request for an order rather than the absence of one.
        DataBrowserSort sort = query.sort();
        if (sort != null) {
            Map<String, Object> ordering = new LinkedHashMap<>();
            ordering.put(sort.field(), sort.direction() == DataBrowserSort.Direction.ASC ? 1 : -1);
            params.put("sort", ordering);
        }
        // An int, not a long: the connector casts this straight to int, so a long fails at the cast.
        // What is sent is the drive's own bound, one past what the caller asked for, so the request the
        // caller made stays what it was and the extra row never leaves this class.
        params.put("limit", limit);
        return params;
    }

    /**
     * One filter in the query language the driven connector speaks. This is the only place that knows
     * that language: the seam above carries Tapstate's own vocabulary, so nothing a caller sends is ever
     * a fragment of a backend query, and a second backend shape is a second translation here rather than
     * a change to any surface.
     *
     * <p>A null filter becomes the empty document, which every row matches. The absence has to be spelt
     * out rather than left out, because a connector hands an absent param straight to its driver as a
     * null filter and the driver refuses that.
     */
    private static Map<String, Object> translate(DataBrowserFilter filter) {
        return switch (filter) {
            case null -> new LinkedHashMap<>();
            case Match match -> term(match);
            case Any any -> combination("$or", any.terms().stream().map(t -> (Conjunct) t).toList());
            case All all -> combination("$and", all.terms());
        };
    }

    /**
     * One connective over its members. A member is itself translated, so an alternative sitting inside a
     * conjunction comes through as its own nested connective rather than being flattened into the outer
     * one — flattened, {@code a AND (b OR c)} becomes {@code a AND b AND c}, which is a stricter filter
     * that still returns rows.
     */
    private static Map<String, Object> combination(String connective, List<Conjunct> members) {
        List<Map<String, Object>> translated = new ArrayList<>(members.size());
        members.forEach(member -> translated.add(translate(member)));
        Map<String, Object> document = new LinkedHashMap<>();
        document.put(connective, translated);
        return document;
    }

    /**
     * One term as an operator-keyed test on its field. Keyed even for equality, where the bare
     * {@code {field: value}} form would read the same: that form takes on a second meaning when the value
     * is itself a document, and the value here comes from a caller.
     */
    private static Map<String, Object> term(Match match) {
        FieldPath path = FieldPath.of(match.field());
        if (!path.isPlainPath()) {
            return namedTerm(path, match);
        }
        Map<String, Object> test = new LinkedHashMap<>();
        test.put(operator(match.operator()), value(match));
        Map<String, Object> document = new LinkedHashMap<>();
        // The steps as parsed, not the spelling as written: a spelling reaches here still carrying its
        // escapes, and a backslash sent as part of a key asks for a field nobody named.
        document.put(String.join(".", path.segments()), test);
        return document;
    }

    /**
     * One term on a field whose own name holds a dot. The query language spells a path with dots and has
     * no escape for one, so such a field cannot be addressed in it at all — the obvious spelling asks for
     * a nested field, matches nothing, and reports nothing wrong. The expression language can name a field
     * instead of pathing to it, and a query may carry an expression, so that is the way through.
     *
     * <p>Reached only when the plain form cannot express the request. What it costs is real: no index
     * serves it, and none could — an index key is spelt as a path too, so a field named this way has no
     * index to miss.
     */
    private static Map<String, Object> namedTerm(FieldPath path, Match match) {
        Map<String, Object> field = Map.of("$getField", String.join(".", path.segments()));
        Object test = switch (match.operator()) {
            // The presence test has no expression-language twin. Asking what type the field holds does the
            // same work: a field that is not there reports the one type no value has.
            case EXISTS -> Map.of(Boolean.TRUE.equals(match.value()) ? "$ne" : "$eq",
                    List.of(Map.of("$type", field), "missing"));
            // Guarded by type because the two languages disagree about a pattern met by a non-string: the
            // query language does not match it, the expression language fails the whole read. The guard
            // keeps the answer the one the vocabulary already gives.
            case CONTAINS -> Map.of("$cond", List.of(
                    Map.of("$eq", List.of(Map.of("$type", field), "string")),
                    Map.of("$regexMatch", Map.of("input", field, "regex", value(match))),
                    false));
            default -> Map.of(operator(match.operator()), List.of(field, value(match)));
        };
        return Map.of("$expr", test);
    }

    private static String operator(Operator operator) {
        return switch (operator) {
            case EQ -> "$eq";
            case NE -> "$ne";
            case GT -> "$gt";
            case GTE -> "$gte";
            case LT -> "$lt";
            case LTE -> "$lte";
            case IN -> "$in";
            case EXISTS -> "$exists";
            // The vocabulary has no pattern in it, so this is the one operator whose value is not sent as
            // it arrived: it is a substring to find, and it reaches the backend as a pattern.
            case CONTAINS -> "$regex";
        };
    }

    private static Object value(Match match) {
        if (match.operator() != Operator.CONTAINS) {
            return match.value();
        }
        // Quoted whole, so every character of the value is a character to find rather than a pattern to
        // run. Spliced in raw, the one operator that takes free text would be a way to express a pattern
        // through a vocabulary that deliberately has none — including patterns that never finish.
        return Pattern.quote((String) match.value());
    }

    /** Accumulates one result batch, remembering the first failure a batch reports instead of its rows. */
    private static void collect(ExecuteResult<?> result, List<Map<String, Object>> rows,
                                AtomicReference<Throwable> reported) {
        if (result == null) {
            return;
        }
        if (result.getError() != null) {
            reported.compareAndSet(null, result.getError());
            return;
        }
        if (!(result.getResult() instanceof List<?> batch)) {
            return;
        }
        for (Object row : batch) {
            if (row instanceof Map<?, ?> fields) {
                // The same spelling the follow face gives a value. Handing these over as the driver
                // returned them is what made one face render a key as its own text and the other as
                // whatever a serializer made of the driver's object - the same row, read two ways.
                Map<String, Object> copy = new LinkedHashMap<>();
                fields.forEach((name, value) -> copy.put(String.valueOf(name), writableValue(value)));
                rows.add(copy);
            }
        }
    }

    /** Runs a read action under the connector loader, mapping a connector-side failure to a code. */
    private static <T> T drive(PdkConnector connector, PdkConnector.Action<T> action) {
        try {
            return connector.underLoader(action);
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw readFailed(connector.connectorId(), t);
        }
    }

    private static TapstateException readFailed(String connectorId, Throwable cause) {
        return new TapstateException(ConnectorError.READ_FAILED,
                Map.of("connector", connectorId, "detail", PdkSchemaDiscoverer.detail(cause)), cause);
    }

    /** The registered function, or a coded refusal naming the connector and the capability it lacks. */
    private static <T> T require(PdkConnector connector, T function, String capability) {
        if (function == null) {
            throw new TapstateException(ConnectorError.CAPABILITY_MISSING,
                    Map.of("connector", connector.connectorId(), "capability", capability), null);
        }
        return function;
    }
}
