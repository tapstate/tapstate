package io.tapstate.e2e.mongowitness;

import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeSet;

/** Observes the call boundary without replacing the connector or changing any argument. */
public final class WriteTableWitness {
    private WriteTableWitness() {}

    public static WriteRecordFunction record(Path file, WriteRecordFunction delegate) {
        return (context, events, table, consumer) -> {
            String fields = table.getNameFieldMap() == null ? "" : String.join(",", new TreeSet<>(table.getNameFieldMap().keySet()));
            // One append per invocation, before the real callback can mutate its inputs. The harness
            // stops the pipeline before reading this ledger, including all attempts, not just the last.
            synchronized (WriteTableWitness.class) {
                Files.writeString(file, table.getId() + "\t" + events.size() + "\t" + fields + "\n",
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            delegate.writeRecord(context, events, table, consumer);
        };
    }
}
