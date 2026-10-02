package io.tapstate.e2e;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapstate.e2e.connector.CsvConnector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/** An offset-aware source journal, packaged as a normal PDK connector for recovery witnesses. */
public final class CdcRecoveryFixture {

    private static final String JOURNAL = "changes.log";
    private static final String SPEC = "cdc-recovery-spec.json";

    private CdcRecoveryFixture() {}

    static byte[] connectorJar(Path delegate) throws IOException, URISyntaxException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(new Attributes.Name("PDK-API-Version"), "2.0.8");
        manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, delegate.toAbsolutePath().toUri().toString());
        var resource = CdcRecoveryFixture.class.getClassLoader().getResource("io/tapstate/e2e/");
        if (resource == null) throw new AssertionError("the recovery fixture was not compiled");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream out = new JarOutputStream(bytes, manifest);
                var classes = Files.list(Path.of(resource.toURI()))) {
            for (Path file : classes.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith("CdcRecoveryFixture"))
                    .filter(path -> path.getFileName().toString().endsWith(".class")).sorted().toList()) {
                put(out, "io/tapstate/e2e/" + file.getFileName(), Files.readAllBytes(file));
            }
            put(out, SPEC, ("{\"properties\":{\"id\":\"e2e_file\"},"
                    + "\"dataTypes\":{\"string\":{\"to\":\"TapString\",\"byte\":65535}}}")
                    .getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }

    static void change(Path source, long sequence, String table, String id, String before, String after)
            throws IOException {
        append(source, sequence + "," + System.currentTimeMillis() + ","
                + table + "," + id + "," + before + "," + after + "\n");
    }

    static void updates(Path source, long firstSequence, int count, String table, String id) throws IOException {
        StringBuilder batch = new StringBuilder();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            batch.append(firstSequence + i).append(',').append(timestamp).append(',').append(table)
                    .append(',').append(id).append(',').append(i == 0 ? "Low" : i % 2 == 0 ? "High" : "Low")
                    .append(',').append(i % 2 == 0 ? "Low" : "High").append('\n');
        }
        append(source, batch.toString());
    }

    private static void append(Path source, String changes) throws IOException {
        Path journal = source.resolve(JOURNAL);
        Path staged = Files.createTempFile(source, "journal-", ".staging");
        try {
            Files.writeString(staged, (Files.exists(journal) ? Files.readString(journal) : "") + changes);
            Files.move(staged, journal, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(staged);
        }
    }

    static List<Path> streams(Path source) {
        try (var files = Files.list(source)) {
            return files.filter(path -> path.getFileName().toString().startsWith("stream-"))
                    .sorted().toList();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    static List<String> lines(Path file) {
        try {
            return Files.exists(file) ? Files.readAllLines(file) : List.of();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    private static void put(JarOutputStream out, String name, byte[] bytes) throws IOException {
        out.putNextEntry(new JarEntry(name));
        out.write(bytes);
        out.closeEntry();
    }

    /** Snapshot discovery and target writes use the existing fixture; its tail uses a resumable journal. */
    @TapConnectorClass(SPEC)
    public static final class Connector extends CsvConnector {

        private volatile boolean stopped;

        @Override
        public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
            super.registerCapabilities(functions, codecs);
            functions.supportTimestampToStreamOffset((context, startTime) -> positionAt(context, startTime))
                    .supportStreamRead((context, tables, offset, size, consumer) ->
                            tail(context, tables, offset, size, consumer));
        }

        @Override
        public void init(TapConnectionContext context) {
            super.init(context);
            stopped = false;
        }

        @Override
        public void stop(TapConnectionContext context) {
            stopped = true;
            super.stop(context);
        }

        private static Map<String, Long> positionAt(TapConnectionContext context, Long startTime) {
            long sequence = 0;
            for (Change change : changes(directory(context))) {
                if (startTime == null || change.timestamp() <= startTime) sequence = change.sequence();
            }
            return position(sequence);
        }

        private void tail(TapConnectionContext context, List<String> tables, Object offset, int size,
                StreamReadConsumer consumer) {
            if (!(offset instanceof Map<?, ?> position) || !(position.get("sequence") instanceof Number number)) {
                throw new IllegalArgumentException("the recovery fixture requires its recorded source position");
            }
            long delivered = number.longValue();
            Path source = directory(context);
            Path witness = source.resolve("stream-" + ProcessHandle.current().pid() + "-" + UUID.randomUUID());
            note(witness, "START " + delivered + " " + String.join("|", tables));
            consumer.streamReadStarted();
            try {
                while (!stopped && !Thread.currentThread().isInterrupted()
                        && !Files.exists(source.resolve("end-stream"))) {
                    List<TapEvent> batch = new ArrayList<>();
                    long through = delivered;
                    for (Change change : changes(source)) {
                        if (change.sequence() <= delivered) continue;
                        through = change.sequence();
                        if (tables.contains(change.table())) batch.add(change.event());
                        if (batch.size() >= size) break;
                    }
                    if (through > delivered) {
                        if (!batch.isEmpty()) {
                            long started = System.nanoTime();
                            consumer.accept(batch, position(through));
                            note(witness, "BATCH " + delivered + " " + through + " " + batch.size()
                                    + " " + Duration.ofNanos(System.nanoTime() - started).toMillis());
                        }
                        delivered = through;
                    }
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException cancelled) {
                        Thread.currentThread().interrupt();
                    }
                }
            } finally {
                note(witness, "END " + delivered);
                consumer.streamReadEnded();
            }
        }

        private static Path directory(TapConnectionContext context) {
            return Path.of(String.valueOf(context.getConnectionConfig().getObject("uri")));
        }

        private static Map<String, Long> position(long sequence) {
            LinkedHashMap<String, Long> position = new LinkedHashMap<>();
            position.put("sequence", sequence);
            return position;
        }

        private static void note(Path witness, String text) {
            try {
                Path staged = Files.createTempFile(witness.getParent(), "witness-", ".staging");
                try {
                    Files.writeString(staged, (Files.exists(witness) ? Files.readString(witness) : "") + text + "\n");
                    Files.move(staged, witness, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } finally {
                    Files.deleteIfExists(staged);
                }
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }

        private static List<Change> changes(Path source) {
            List<Change> changes = new ArrayList<>();
            for (String line : lines(source.resolve(JOURNAL))) {
                if (line.isBlank()) continue;
                String[] values = line.split(",", -1);
                if (values.length != 6) throw new IllegalArgumentException("invalid recovery journal entry: " + line);
                changes.add(new Change(Long.parseLong(values[0]), Long.parseLong(values[1]),
                        values[2], values[3], values[4], values[5]));
            }
            return changes;
        }

        private record Change(long sequence, long timestamp, String table, String id, String before, String after) {
            TapEvent event() {
                Map<String, Object> afterRow = Map.of("id", id, "priority", after);
                if (before.isEmpty()) {
                    return TapInsertRecordEvent.create().table(table).referenceTime(timestamp).after(afterRow);
                }
                return TapUpdateRecordEvent.create().table(table).referenceTime(timestamp)
                        .before(Map.of("id", id, "priority", before)).after(afterRow);
            }
        }
    }
}
