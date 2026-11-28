package io.tapstate.e2e.mongowitness;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Captures only identities and times from the unchanged connector's CDC and confirmation callbacks. */
public final class LatencyWitness {
    private LatencyWitness() {}

    static StreamReadFunction source(Path file, StreamReadFunction delegate, boolean recordPositions) {
        return (context, tables, offset, size, downstream) -> delegate.streamRead(context, tables, offset, size,
                new StreamReadConsumer() {
                    @Override
                    public void accept(List<TapEvent> events, Object latest) {
                        if (active(file)) {
                            var lines = new StringBuilder();
                            long readMillis = recordPositions ? System.currentTimeMillis() : 0;
                            for (var event : events) {
                                if (event instanceof TapRecordEvent record) {
                                    lines.append(identity(record));
                                    if (recordPositions) {
                                        lines.append('\t').append(record.getTableId())
                                                .append('\t').append(record.getExactlyOnceId())
                                                .append('\t').append(readMillis);
                                    }
                                    lines.append('\n');
                                }
                            }
                            append(file, lines);
                        }
                        downstream.accept(events, latest);
                    }

                    @Override
                    public void streamReadStarted() {
                        downstream.streamReadStarted();
                    }

                    @Override
                    public void streamReadEnded() {
                        downstream.streamReadEnded();
                    }

                    @Override
                    public void asyncMethodAndNoRetry() {
                        downstream.asyncMethodAndNoRetry();
                    }

                    @Override
                    public int getState() {
                        return downstream.getState();
                    }

                    @Override
                    public boolean isAsyncMethodAndNoRetry() {
                        return downstream.isAsyncMethodAndNoRetry();
                    }
                });
    }

    public static WriteRecordFunction target(Path file, WriteRecordFunction delegate) {
        return target(file, delegate, LatencyWitness::identity);
    }

    static WriteRecordFunction snapshotTarget(Path file, WriteRecordFunction delegate) {
        return target(file, delegate, LatencyWitness::snapshotKey);
    }

    private static WriteRecordFunction target(Path file, WriteRecordFunction delegate,
            Function<TapRecordEvent, String> describe) {
        return (context, events, table, consumer) -> {
            if (!active(file)) {
                delegate.writeRecord(context, events, table, consumer);
                return;
            }
            List<String> batch = events.stream().map(describe).toList();
            var confirmation = new Confirmation();
            delegate.writeRecord(context, events, table, result -> {
                confirmation.callbacks++;
                confirmation.confirmed = result.getInsertedCount() + result.getModifiedCount() + result.getRemovedCount();
                confirmation.error = result.getErrorMap() != null && !result.getErrorMap().isEmpty();
                confirmation.epochMillis = System.currentTimeMillis();
                consumer.accept(result);
            });
            // A result that covers the entire successful call proves every member was acknowledged
            // at this callback. Split results have no reliable per-event membership.
            if (confirmation.callbacks == 1 && !confirmation.error && confirmation.confirmed == batch.size()) {
                var lines = new StringBuilder();
                for (String event : batch) {
                    lines.append(event).append('\t').append(confirmation.epochMillis).append('\n');
                }
                append(file.resolveSibling(file.getFileName() + ".ack"), lines);
            }
        };
    }

    private static boolean active(Path file) {
        return Files.isRegularFile(file.resolveSibling(file.getFileName() + ".active"));
    }

    private static String identity(TapRecordEvent event) {
        Map<String, Object> row;
        int operation;
        switch (event) {
            case TapInsertRecordEvent insert -> { operation = 0; row = insert.getAfter(); }
            case TapUpdateRecordEvent update -> { operation = 1; row = update.getAfter(); }
            case TapDeleteRecordEvent delete -> { operation = 2; row = delete.getBefore(); }
            default -> { return "invalid"; }
        }
        if (row == null || !(row.get("_id") instanceof Number key) || event.getReferenceTime() == null) {
            return "invalid";
        }
        return operation + "\t" + key.longValue() + "\t" + event.getReferenceTime();
    }

    private static String snapshotKey(TapRecordEvent event) {
        if (event instanceof TapInsertRecordEvent insert && insert.getAfter() != null
                && insert.getAfter().get("_id") instanceof Number key) {
            return Long.toString(key.longValue());
        }
        return "invalid";
    }

    private static synchronized void append(Path file, StringBuilder lines) {
        if (lines.isEmpty()) return;
        try {
            Files.writeString(file, lines, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException failure) {
            // The missing ledger evidence disqualifies latency without changing the pipeline outcome.
        }
    }

    private static final class Confirmation {
        int callbacks;
        long confirmed;
        long epochMillis;
        boolean error;
    }
}
