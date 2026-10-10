package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.BoundedSnapshotQueryPort;
import io.tapstate.spi.capture.BoundedQueryCancellation;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest;
import io.tapstate.spi.capture.BoundedSnapshotQueryResult;
import io.tapstate.spi.capture.FieldSchema;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.Projection;
import io.tapdata.pdk.apis.entity.SortOn;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** PDK-backed finite reads for preview. It never falls back to an unbounded batch scan. */
public final class PdkBoundedSnapshotQueryPort implements BoundedSnapshotQueryPort {

    private final ConnectorProvisioner provisioner;
    private final Clock clock;

    public PdkBoundedSnapshotQueryPort(ConnectorProvisioner provisioner) {
        this(provisioner, Clock.systemUTC());
    }

    PdkBoundedSnapshotQueryPort(ConnectorProvisioner provisioner, Clock clock) {
        this.provisioner = Objects.requireNonNull(provisioner, "provisioner");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public String cacheIdentity(BoundedSnapshotQueryRequest request) {
        return provisioner.cacheIdentity(request.connectorId());
    }

    @Override
    public BoundedSnapshotQueryResult query(BoundedSnapshotQueryRequest request) {
        return query(request, new BoundedQueryCancellation());
    }

    @Override
    public BoundedSnapshotQueryResult query(
            BoundedSnapshotQueryRequest request, BoundedQueryCancellation cancellation) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(cancellation, "cancellation");
        cancellation.throwIfCancelled();
        ensureBeforeDeadline(request);
        PdkConnector connector = PdkConnector.open(
                request.connectorId(), provisioner.resolve(request.connectorId()), request.settings());
        BoundedQueryCancellation.Registration stopOnCancel = cancellation.onCancel(connector::stopQuietly);
        try {
            cancellation.throwIfCancelled();
            QueryByAdvanceFilterFunction query = connector.functions().getQueryByAdvanceFilterFunction();
            if (query == null) {
                throw new TapstateException(ConnectorError.CAPABILITY_MISSING,
                        Map.of("connector", connector.connectorId(), "capability", "query-by-advance-filter"), null);
            }
            TapTable table = discover(connector, request);
            List<Envelope> rows = new ArrayList<>(Math.min(request.limit(), 1024));
            int[] queryCount = {0};
            boolean[] hasMore = {false};
            long[] bytesUsed = {0L};

            if (request.selection() instanceof BoundedSnapshotQueryRequest.AllRows) {
                List<Envelope> found = queryOnce(
                        connector, query, table, request, Map.of(), request.limit(), queryCount, bytesUsed,
                        cancellation);
                if (found.size() > request.limit()) {
                    rows.addAll(found.subList(0, request.limit()));
                    hasMore[0] = true;
                } else {
                    rows.addAll(found);
                }
            } else if (request.selection() instanceof BoundedSnapshotQueryRequest.ExactTuples exact) {
                for (Map<String, Object> tuple : exact.tuples()) {
                    ensureBeforeDeadline(request);
                    cancellation.throwIfCancelled();
                    int remaining = request.limit() - rows.size();
                    if (remaining == 0) {
                        hasMore[0] = true;
                        break;
                    }
                    List<Envelope> found = queryOnce(
                            connector, query, table, request, tuple, remaining, queryCount, bytesUsed,
                            cancellation);
                    if (found.size() > remaining) {
                        rows.addAll(found.subList(0, remaining));
                        hasMore[0] = true;
                        break;
                    }
                    rows.addAll(found);
                }
            } else {
                throw new IllegalStateException("unhandled bounded query selection: "
                        + request.selection().getClass().getName());
            }

            if (!connector.isAlive()) {
                throw new TapstateException(ConnectorError.READ_ABANDONED,
                        Map.of("connector", connector.connectorId()), null);
            }
            ensureBeforeDeadline(request);
            cancellation.throwIfCancelled();
            boolean complete = !hasMore[0];
            return new BoundedSnapshotQueryResult(rows, complete, hasMore[0],
                    !request.stableOrder().isEmpty(), queryCount[0], clock.instant());
        } finally {
            stopOnCancel.close();
            connector.stopQuietly();
            connector.close();
        }
    }

    private TapTable discover(PdkConnector connector, BoundedSnapshotQueryRequest request) {
        try {
            return connector.underLoader(() -> {
                connector.connector().init(connector.context());
                List<TapTable> discovered = new ArrayList<>();
                connector.connector().discoverSchema(connector.context(),
                        List.of(request.table().name()), Integer.MAX_VALUE, discovered::addAll);
                List<TapTable> matching = discovered.stream()
                        .filter(candidate -> request.table().name().equals(candidate.getId())
                                || request.table().name().equals(candidate.getName()))
                        .toList();
                if (matching.size() != 1) {
                    throw new TapstateException(ConnectorError.DISCOVER_FAILED,
                            Map.of("connector", connector.connectorId(),
                                    "detail", "preview table discovery did not resolve exactly one table"), null);
                }
                TapTable table = matching.getFirst();
                connector.fillFieldTypes(table);
                verifySchema(connector, request, table);
                return table;
            });
        } catch (TapstateException failure) {
            throw failure;
        } catch (Throwable failure) {
            throw new TapstateException(ConnectorError.DISCOVER_FAILED,
                    Map.of("connector", connector.connectorId(), "detail", "preview table discovery failed"), null);
        }
    }

    private static void verifySchema(PdkConnector connector, BoundedSnapshotQueryRequest request, TapTable table) {
        Map<String, TapField> fields = table.getNameFieldMap();
        if (fields == null) {
            if (!request.table().fields().isEmpty() || !request.stableOrder().isEmpty()
                    || request.selection() instanceof BoundedSnapshotQueryRequest.ExactTuples) {
                throw schemaChanged(connector, request);
            }
            return;
        }
        Map<String, String> compiled = new LinkedHashMap<>();
        for (FieldSchema field : request.table().fields()) {
            compiled.put(field.name(), field.type());
        }
        for (Map.Entry<String, String> expected : compiled.entrySet()) {
            TapField live = fields.get(expected.getKey());
            if (live == null || (expected.getValue() != null
                    && !Objects.equals(expected.getValue(), live.getDataType()))) {
                throw schemaChanged(connector, request);
            }
        }
        List<String> required = new ArrayList<>(request.stableOrder());
        if (request.selection() instanceof BoundedSnapshotQueryRequest.ExactTuples exact
                && !exact.tuples().isEmpty()) {
            required.addAll(exact.tuples().getFirst().keySet());
        }
        required.addAll(request.projection());
        if (!fields.keySet().containsAll(required)) {
            throw schemaChanged(connector, request);
        }
    }

    private List<Envelope> queryOnce(
            PdkConnector connector,
            QueryByAdvanceFilterFunction query,
            TapTable table,
            BoundedSnapshotQueryRequest request,
            Map<String, Object> tuple,
            int remaining,
            int[] queryCount,
            long[] bytesUsed,
            BoundedQueryCancellation cancellation) {
        cancellation.throwIfCancelled();
        int queryLimit = remaining + 1;
        TapAdvanceFilter filter = TapAdvanceFilter.create()
                .match(match(connector, table, request, tuple))
                .limit(queryLimit)
                .batchSize(Math.min(queryLimit, 1000));
        for (String field : request.stableOrder()) {
            filter.sort(SortOn.ascending(field));
        }
        if (!request.projection().isEmpty()) {
            Projection projection = Projection.create();
            request.projection().forEach(projection::include);
            filter.projection(projection);
        }

        QueryRows rows = new QueryRows(queryLimit);
        AtomicReference<Throwable> reported = new AtomicReference<>();
        queryCount[0]++;
        try {
            connector.underLoader(() -> {
                query.query(connector.context(), filter, table, results -> collect(
                        connector, request, table, queryLimit, results, rows, reported, bytesUsed, cancellation));
                return null;
            });
        } catch (Throwable failure) {
            if (reported.get() != null) {
                throw readFailed(connector, reported.get());
            }
            throw readFailed(connector, failure);
        }
        if (reported.get() != null) {
            throw readFailed(connector, reported.get());
        }
        if (!connector.isAlive()) {
            throw new TapstateException(ConnectorError.READ_ABANDONED,
                    Map.of("connector", connector.connectorId()), null);
        }
        ensureBeforeDeadline(request);
        cancellation.throwIfCancelled();
        return rows.snapshot();
    }

    private void collect(
            PdkConnector connector,
            BoundedSnapshotQueryRequest request,
            TapTable table,
            int cap,
            FilterResults result,
            QueryRows rows,
            AtomicReference<Throwable> reported,
            long[] bytesUsed,
            BoundedQueryCancellation cancellation) {
        synchronized (rows.monitor) {
            if (reported.get() != null) {
                return;
            }
            if (cancellation.isCancelled()) {
                reported.compareAndSet(null, new java.util.concurrent.CancellationException(
                        "bounded source query was cancelled"));
                return;
            }
            if (result == null) {
                reported.compareAndSet(null, new IllegalStateException("connector returned a null query result"));
                return;
            }
            if (result.getError() != null) {
                reported.compareAndSet(null, result.getError());
                return;
            }
            if (result.getResults() == null) {
                return;
            }
            for (Map<String, Object> sourceRow : result.getResults()) {
                if (reported.get() != null || cancellation.isCancelled()) {
                    return;
                }
                if (rows.values.size() >= cap) {
                    return;
                }
                try {
                    Map<String, Object> copy = stringKeyed(sourceRow);
                    TapEvent event = TapInsertRecordEvent.create().table(table.getId()).after(copy);
                    Envelope envelope = TapEventCodec.decodeSnapshotRow(event, connector.codecs(), declaredTypes(table));
                    long remainingBytes = request.maxBytes() - bytesUsed[0];
                    // Byte accounting renders PDK temporal values without changing the typed row.
                    long rowBytes = PdkPreviewJsonValues.encodedSize(envelope.after(), remainingBytes);
                    if (rowBytes > remainingBytes) {
                        reported.compareAndSet(null, new TapstateException(ConnectorError.READ_FAILED,
                                Map.of("connector", connector.connectorId(),
                                        "detail", "bounded preview sample exceeds its byte limit"), null));
                        return;
                    }
                    bytesUsed[0] += rowBytes;
                    rows.values.add(envelope);
                } catch (RuntimeException failure) {
                    reported.compareAndSet(null, new IllegalArgumentException(
                            "connector row could not be decoded for preview table " + request.table().name(), failure));
                    return;
                }
            }
        }
    }

    /** The callback monitor stays private even when a completed sample is handed to its caller. */
    private static final class QueryRows {
        private final Object monitor = new Object();
        private final List<Envelope> values;

        private QueryRows(int limit) {
            values = new ArrayList<>(Math.min(limit, 1024));
        }

        private List<Envelope> snapshot() {
            synchronized (monitor) {
                return List.copyOf(values);
            }
        }
    }

    private static DataMap match(
            PdkConnector connector,
            TapTable table,
            BoundedSnapshotQueryRequest request,
            Map<String, Object> tuple) {
        if (tuple.isEmpty()) {
            return DataMap.create();
        }
        TapEvent encoded = TapEventCodec.encode(Envelope.read(0, table.getId(), tuple, null), connector.codecs());
        @SuppressWarnings("unchecked")
        Map<String, Object> values = ((TapInsertRecordEvent) encoded).getAfter();
        return DataMap.create(new LinkedHashMap<>(values));
    }

    private static Map<String, String> declaredTypes(TapTable table) {
        Map<String, String> declared = new LinkedHashMap<>();
        if (table.getNameFieldMap() != null) {
            table.getNameFieldMap().forEach((name, field) -> {
                if (field != null && field.getDataType() != null) {
                    declared.put(name, field.getDataType());
                }
            });
        }
        return declared;
    }

    private static Map<String, Object> stringKeyed(Map<String, Object> row) {
        if (row == null) {
            throw new IllegalArgumentException("connector returned a null row");
        }
        Map<String, Object> copy = new LinkedHashMap<>();
        row.forEach((name, value) -> {
            if (name == null) {
                throw new IllegalArgumentException("connector returned a row with a null field name");
            }
            copy.put(name, value);
        });
        return copy;
    }

    private void ensureBeforeDeadline(BoundedSnapshotQueryRequest request) {
        if (!clock.instant().isBefore(request.deadline())) {
            throw new TapstateException(ConnectorError.READ_TIMEOUT,
                    Map.of("connector", request.connectorId(), "timeout", "preview deadline"), null);
        }
    }

    private static TapstateException schemaChanged(PdkConnector connector, BoundedSnapshotQueryRequest request) {
        return new TapstateException(ConnectorError.READ_FAILED,
                Map.of("connector", connector.connectorId(),
                        "detail", "source schema changed after preview compilation for " + request.table().name()),
                null);
    }

    private static TapstateException readFailed(PdkConnector connector, Throwable cause) {
        if (cause instanceof TapstateException coded) {
            return coded;
        }
        return new TapstateException(ConnectorError.READ_FAILED,
                Map.of("connector", connector.connectorId(), "detail", "bounded preview query failed"), null);
    }
}
