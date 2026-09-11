package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.SchemaDiscoverer;
import io.tapstate.core.common.TapstateType;
import io.tapdata.entity.schema.type.TapType;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The PDK implementation of the schema-discovery port: it provisions a connector, refuses it with a
 * code if it will not load or its declared API level is incompatible, drives the connector's frozen
 * {@code discoverSchema} to enumerate the source's streams, and normalizes each PDK table into a
 * {@link SourceTable} — its fields, primary key and indexes. The connector is inited once for the
 * discovery and stopped afterwards; the PDK types stay inside this class and neither the port nor the
 * model carries any of them.
 *
 * <p>A connector that throws out of {@code discoverSchema} could not complete discovery — a coded
 * connector-domain failure, distinct from an empty source. A field carries both the type the connector
 * reported it with, verbatim, and that type resolved onto the tapstate type namespace — resolved here,
 * because the connector's own declarations are what resolves it and they are unreachable once it closes.
 * An un-loadable or level-incompatible connector is refused up front by the shared open path, never
 * downgraded into an empty model.
 */
public final class PdkSchemaDiscoverer implements SchemaDiscoverer {

    /**
     * How long the counting phase of one discovery may run for. Discovery is a synchronous verb its
     * callers give a fixed window — thirty seconds on the shipped clients — and the schema is what the
     * window is for; counting is a measurement taken alongside it. This leaves the larger part of that
     * window to discovery itself, which is what the caller is actually waiting on.
     */
    public static final Duration DEFAULT_COUNT_BUDGET = Duration.ofSeconds(10);

    private final ConnectorProvisioner provisioner;
    private final Duration countBudget;

    public PdkSchemaDiscoverer(ConnectorProvisioner provisioner) {
        this(provisioner, DEFAULT_COUNT_BUDGET);
    }

    public PdkSchemaDiscoverer(ConnectorProvisioner provisioner, Duration countBudget) {
        this.provisioner = provisioner;
        this.countBudget = requirePositive(countBudget);
    }

    /**
     * A budget that reaches no table at all is a wiring fault, not a configuration meaning "do not
     * count": it would read as configured while every table came back uncounted with nothing saying why.
     */
    private static Duration requirePositive(Duration countBudget) {
        Objects.requireNonNull(countBudget, "countBudget");
        if (countBudget.isZero() || countBudget.isNegative()) {
            throw new IllegalArgumentException("countBudget must be positive");
        }
        return countBudget;
    }

    private static final Logger LOG = LoggerFactory.getLogger(PdkSchemaDiscoverer.class);

    @Override
    public SourceModel discover(ConnectionConfig config) {
        PdkConnector connector = PdkConnector.open(
                config.connectorId(), provisioner.resolve(config.connectorId()), config.settings());
        try {
            List<TapTable> tables = drive(connector);
            return toSourceModel(tables, counts(connector, tables));
        } finally {
            connector.stopQuietly();
            connector.close();
        }
    }

    /**
     * How many rows each discovered table holds, for the tables the connector will answer for. Counting
     * is a capability of its own, separate from discovering: a connector registering no count function
     * contributes no counts at all, and one whose count fails contributes none for that table.
     *
     * <p>A failure here never reaches the caller as a failure. The schema is what discovery was asked
     * for and the count is taken alongside it; refusing the schema because a number could not be
     * produced would take away what the author needs over what only sizes their state, and the reader
     * of a count already has to handle its absence — no connector is obliged to offer one. The failure
     * is logged rather than swallowed, so a count that never arrives can still be explained.
     *
     * <p>The phase is held to a wall-clock budget, checked before each table. A source wide enough, or
     * slow enough to count, would otherwise spend the window its caller gives the whole verb, and hand
     * the author a timeout where the schema they asked for should have been. What the budget bounds is
     * which counts are started, not how long one runs: the frozen connector API offers no way to cut a
     * count short, so the phase costs at most the budget plus one table's count. Tables past it are left
     * uncounted — the same absence an uncountable connector already produces — and the shortfall is
     * logged, so "why does this table have no count" still has an answer.
     */
    private Map<TapTable, Long> counts(PdkConnector connector, List<TapTable> tables) {
        BatchCountFunction count = connector.functions().getBatchCountFunction();
        if (count == null) {
            return Map.of();
        }
        Map<TapTable, Long> counted = new IdentityHashMap<>();
        try {
            connector.underLoader(() -> {
                long deadline = System.nanoTime() + countBudget.toNanos();
                for (int i = 0; i < tables.size(); i++) {
                    if (System.nanoTime() - deadline >= 0) {
                        LOG.warn("connector {} spent its {} count budget, leaving {} of {} tables uncounted",
                                connector.connectorId(), countBudget, tables.size() - i, tables.size());
                        break;
                    }
                    TapTable table = tables.get(i);
                    try {
                        counted.put(table, count.count(connector.context(), table));
                    } catch (Throwable t) {
                        // Per table, so one source refusing to be counted does not cost the rest theirs.
                        LOG.warn("connector {} could not count table {}: {}",
                                connector.connectorId(), table.getId(), detail(t));
                    }
                }
                return null;
            });
        } catch (Throwable t) {
            LOG.warn("connector {} could not be asked to count: {}", connector.connectorId(), detail(t));
        }
        return counted;
    }

    /**
     * Inits the connector once and drives its discoverSchema over every stream (an empty stream list =
     * all), collecting the reported tables. A connector that throws out of discovery could not complete
     * it — a coded connector-domain failure.
     */
    private static List<TapTable> drive(PdkConnector connector) {
        try {
            return connector.underLoader(() -> {
                connector.connector().init(connector.context());
                List<TapTable> tables = new ArrayList<>();
                connector.connector().discoverSchema(connector.context(), List.of(), Integer.MAX_VALUE, tables::addAll);
                // Resolving a column's type is the connector's own job, off its own declarations, so it
                // happens here while the connector is still open - never after the model has left.
                tables.forEach(connector::fillFieldTypes);
                return tables;
            });
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw new TapstateException(ConnectorError.DISCOVER_FAILED,
                    Map.of("connector", connector.connectorId(), "detail", detail(t)), t);
        }
    }

    private static SourceModel toSourceModel(List<TapTable> tables, Map<TapTable, Long> counts) {
        List<SourceTable> mapped = new ArrayList<>(tables.size());
        for (TapTable table : tables) {
            mapped.add(new SourceTable(
                    table.getId(), fields(table), primaryKey(table), indexes(table), counts.get(table)));
        }
        return new SourceModel(mapped);
    }

    /**
     * One discovered column, carrying what the connector declared, what that resolved to, and - where
     * nothing did - the mapping's own attribution rather than a restatement of it. This is the point at
     * which the connector is still open, and afterwards nobody can tell the several unknowns apart.
     */
    private static SourceField field(String name, String dataType, TapType declared) {
        PdkTypeMapping.Resolved resolved = PdkTypeMapping.resolve(declared);
        return new SourceField(name, dataType, resolved.type(), resolved.unknownBecause(), PdkTypeMapping.numericType(declared));
    }

    private static List<SourceField> fields(TapTable table) {
        List<SourceField> fields = new ArrayList<>();
        if (table.getNameFieldMap() != null) {
            table.getNameFieldMap().forEach((name, field) ->
                    fields.add(field(name, field.getDataType(), field.getTapType())));
        }
        return fields;
    }

    /** The primary-key column names in key order, as the table derives them from its fields. */
    private static List<String> primaryKey(TapTable table) {
        return new ArrayList<>(table.primaryKeys());
    }

    private static List<SourceIndex> indexes(TapTable table) {
        List<SourceIndex> indexes = new ArrayList<>();
        if (table.getIndexList() != null) {
            for (TapIndex index : table.getIndexList()) {
                List<String> named = indexFieldNames(index);
                // A part that named no column was dropped, so what is reported covers strictly fewer
                // columns than the database constrains. Uniqueness does not survive that: UNIQUE
                // (user_id, scene, (expr)) permits duplicate (user_id, scene) pairs, and reporting the
                // pair as unique would state a guarantee the database does not make - to a reader
                // choosing an upsert key on the strength of it.
                boolean whole = named.size() == parts(index);
                indexes.add(new SourceIndex(
                        index.getName(), named, whole && Boolean.TRUE.equals(index.getUnique())));
            }
        }
        return indexes;
    }

    /**
     * The columns an index names, skipping any part that names none.
     *
     * <p>Not every part of an index is a column. A functional index covers an expression over the row —
     * {@code UNIQUE KEY (user_id, scene, (ifnull(circle_type,'')))} — and the database reports no column
     * name for that part, because there is no column to name. Carrying the absent name into the model
     * fails the whole discovery, and it fails it for every table in the database rather than the one
     * holding the index, so a single ordinary index made an entire source unusable.
     *
     * <p>Skipping is right rather than merely safe: these names exist to say what a write could be
     * keyed by, and an expression is not something a row can be matched on. An index that mixes the two
     * keeps the columns it does name - but not its uniqueness, which the caller drops, since what is
     * left covers fewer columns than the constraint does.
     */
    private static List<String> indexFieldNames(TapIndex index) {
        List<String> names = new ArrayList<>();
        if (index.getIndexFields() != null) {
            for (TapIndexField field : index.getIndexFields()) {
                if (field != null && field.getName() != null) {
                    names.add(field.getName());
                }
            }
        }
        return names;
    }

    /** How many parts an index declares, named or not - what the kept names are measured against. */
    private static int parts(TapIndex index) {
        return index.getIndexFields() == null ? 0 : index.getIndexFields().size();
    }

    /**
     * A diagnosable one-liner for a connector's failure: the chain of causes, each named and carrying its
     * own message where it has one. A connector fault often surfaces as a wrapper with no message of its
     * own — an {@code ExceptionInInitializerError}, a reflective {@code InvocationTargetException} — and
     * reporting only that wrapper's class name buries the real fault. Walking the chain keeps the fault in
     * view. Bounded in length, and guarded against a self- or cycle-referential cause.
     */
    static String detail(Throwable t) {
        StringBuilder chain = new StringBuilder();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable each = t; each != null && seen.add(each) && chain.length() < 500; each = each.getCause()) {
            if (chain.length() > 0) {
                chain.append(" <- ");
            }
            chain.append(each.getClass().getSimpleName());
            String message = each.getMessage();
            if (message != null && !message.isBlank()) {
                chain.append(": ").append(message.strip());
            }
        }
        return chain.toString();
    }
}
