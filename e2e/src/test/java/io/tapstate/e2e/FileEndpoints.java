package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The file endpoints a specification lays data on and reads data from: one comma-separated file per
 * table under a directory, addressed by the directory path the resource carries as its {@code uri}.
 *
 * <p>This driver exists so a specification can move real rows through a real connector without a
 * database to host them. It shares nothing with the connector that reads and writes the same files -
 * not a class, not a constant - because a count taken through the connector's own code would agree
 * with it by construction. Two independent readers of one format is the whole point: the format is
 * the contract, and it is plain enough to read by eye when a specification disagrees.
 *
 * <p>Row shape mirrors the Mongo driver's - an id and a sequence. The {@code seed} generator vocabulary
 * is still only {@code rows: N}, but what a specification may depend on is now more than the count: the
 * ids are the whole numbers 1..N, and an insert continues them. A published example that filters has to
 * name something in a row to filter on, and a predicate that cannot say which rows it drops witnesses
 * nothing. Widening the generator later is free; changing what these ids are is not, and would be read
 * here first.
 */
final class FileEndpoints implements Endpoints {

    private static final String HEADER = "id,seq";
    private static final String SUFFIX = ".csv";

    /** The setting this store is addressed by: the directory holding one file per table. */
    private static final String DIRECTORY = "uri";

    /** Lays {@code rows} rows down, numbered from one, replacing whatever the table held. */
    @Override
    public void seed(EndpointAddress address, String table, long rows) {
        List<Row> seeded = new ArrayList<>();
        for (long id = 1; id <= rows; id++) {
            seeded.add(new Row(id, id));
        }
        write(file(address, table), seeded);
    }

    /** Produces {@code rows} changes of one kind against a table that is already seeded. */
    @Override
    public void cdc(EndpointAddress address, String table, CdcOp op, long rows) {
        Path file = file(address, table);
        if (!Files.exists(file)) {
            throw new EnvelopeException(
                    "the table " + table + " at " + address.text(DIRECTORY)
                            + " has not been seeded, so there is nothing to change");
        }
        List<Row> current = read(file);
        write(file, switch (op) {
            case INSERT -> inserted(current, rows);
            case UPDATE -> updated(current, rows);
            case DELETE -> deleted(current, rows);
        });
    }

    /**
     * The rows the table holds now, or none when the table is not there. A table the product has not
     * written yet is absent rather than empty, and the honest count of it is zero: a specification that
     * waits for a first write is waiting for exactly this reading to move.
     */
    @Override
    public long count(EndpointAddress address, String table) {
        Path file = file(address, table);
        return Files.exists(file) ? read(file).size() : 0L;
    }

    @Override
    public void close() {
        // Nothing is held open: every reading opens the file, reads it and closes it again.
    }

    private static List<Row> inserted(List<Row> current, long rows) {
        long highest = current.stream().mapToLong(Row::id).max().orElse(0L);
        List<Row> next = new ArrayList<>(current);
        for (long id = highest + 1; id < highest + 1 + rows; id++) {
            next.add(new Row(id, id));
        }
        return next;
    }

    /** Rewrites the sequence of the lowest {@code rows} ids, leaving the row count alone. */
    private static List<Row> updated(List<Row> current, long rows) {
        List<Row> ordered = byId(current);
        List<Row> next = new ArrayList<>(ordered);
        for (int i = 0; i < rows && i < next.size(); i++) {
            next.set(i, new Row(next.get(i).id(), -next.get(i).id()));
        }
        return next;
    }

    private static List<Row> deleted(List<Row> current, long rows) {
        List<Row> ordered = byId(current);
        return new ArrayList<>(ordered.subList((int) Math.min(rows, ordered.size()), ordered.size()));
    }

    private static List<Row> byId(List<Row> rows) {
        return rows.stream().sorted(Comparator.comparingLong(Row::id)).toList();
    }

    private static Path file(EndpointAddress address, String table) {
        String uri = address.text(DIRECTORY);
        Path directory = Path.of(uri);
        if (!Files.isDirectory(directory)) {
            throw new EnvelopeException("the endpoint at " + uri + " is not a directory, so it holds no tables");
        }
        return directory.resolve(table + SUFFIX);
    }

    private static List<Row> read(Path file) {
        List<Row> rows = new ArrayList<>();
        try {
            List<String> lines = Files.readAllLines(file);
            // The header names the columns; every line after it is a row.
            for (String line : lines.subList(Math.min(1, lines.size()), lines.size())) {
                if (!line.isBlank()) {
                    rows.add(Row.parse(file, line));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the table at " + file, e);
        }
        return rows;
    }

    private static void write(Path file, List<Row> rows) {
        StringBuilder text = new StringBuilder(HEADER).append('\n');
        for (Row row : rows) {
            text.append(row.id()).append(',').append(row.seq()).append('\n');
        }
        try {
            Files.writeString(file, text.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the table at " + file, e);
        }
    }

    /** One row of the format: an id and a sequence, both whole numbers. */
    private record Row(long id, long seq) {

        static Row parse(Path file, String line) {
            String[] columns = line.split(",", -1);
            if (columns.length != 2) {
                throw new EnvelopeException(
                        "the table at " + file + " holds a row that is not " + HEADER + ": " + line);
            }
            try {
                return new Row(Long.parseLong(columns[0].trim()), Long.parseLong(columns[1].trim()));
            } catch (NumberFormatException e) {
                throw new EnvelopeException("the table at " + file + " holds a row that is not " + HEADER + ": " + line, e);
            }
        }
    }
}
