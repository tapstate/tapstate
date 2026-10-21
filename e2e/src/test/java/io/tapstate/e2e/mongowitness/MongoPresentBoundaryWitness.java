package io.tapstate.e2e.mongowitness;

import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

/** Records only the known integer-second boundary, never an offset token or its string form. */
public final class MongoPresentBoundaryWitness {
    private MongoPresentBoundaryWitness() {}

    public static void install(ConnectorFunctions functions, Path journal) {
        var timestamp = functions.getTimestampToStreamOffsetFunction();
        if (timestamp != null) functions.supportTimestampToStreamOffset((context, start) -> {
            Object offset = timestamp.timestampToStreamOffset(context, start);
            append(journal, "RESOLVE\t" + (start == null) + "\t" + seconds(offset));
            return offset;
        });
        var stream = functions.getStreamReadFunction();
        if (stream != null) functions.supportStreamRead((context, tables, offset, size, consumer) -> {
            append(journal, "STREAM\t" + seconds(offset));
            stream.streamRead(context, tables, offset, size, consumer);
        });
    }

    private static String seconds(Object offset) {
        if (offset instanceof Map<?, ?> fields && fields.get("cdcOffset") instanceof Integer value) {
            return Long.toString(Integer.toUnsignedLong(value));
        }
        return "unknown";
    }

    private static void append(Path journal, String event) {
        synchronized (MongoPresentBoundaryWitness.class) {
            try (var output = new FileOutputStream(journal.toFile(), true)) {
                output.write((event + "\n").getBytes(StandardCharsets.UTF_8));
            } catch (IOException unavailable) {
                System.out.println("MONGO_PRESENT_WITNESS_UNAVAILABLE");
            }
        }
    }
}
