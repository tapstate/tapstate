package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    /**
     * What a table is staged under while it is being replaced. The format's suffix is absent from it on
     * purpose, so a staging file nobody moved is not read as a table by anything that reads this format.
     */
    private static final String STAGING_SUFFIX = ".staging";

    /** The setting this store is addressed by: the directory holding one file per table. */
    private static final String DIRECTORY = "uri";

    /**
     * Lays the given rows down, replacing whatever the table held. This store's format is a contract
     * with a second, independent reader, so it carries exactly the generated shape - an id and a
     * sequence; rows with other columns name a widening of the format, not of this method.
     */
    @Override
    public void seed(EndpointAddress address, String table, List<Map<String, Object>> rows) {
        List<Row> seeded = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (!row.keySet().equals(Set.of(SeedRows.ID, SeedRows.SEQ))) {
                throw new EnvelopeException(
                        "a file store holds rows of exactly id and seq; seeding columns " + row.keySet()
                                + " means widening the file format and both of its readers first");
            }
            seeded.add(new Row(longOf(row, SeedRows.ID), longOf(row, SeedRows.SEQ)));
        }
        write(file(address, table), seeded);
    }

    /**
     * The column as the whole number this format holds. The shape check above holds the row to these
     * two columns but says nothing about what is in them, and the vocabulary admits a string wherever
     * it admits a number - so a seed writing {@code seq: two} passes every earlier check and would
     * reach the cast, which fails with no example, no column and no value in what it says.
     */
    private static long longOf(Map<String, Object> row, String column) {
        Object value = row.get(column);
        if (!(value instanceof Number number)) {
            throw new EnvelopeException(
                    "a file store holds " + SeedRows.ID + " and " + SeedRows.SEQ
                            + " as whole numbers; seeding " + column + " as '" + value
                            + "' means widening the file format and both of its readers first");
        }
        return number.longValue();
    }

    /** The one row the settings locate, in the two columns this format has. */
    @Override
    public Optional<Map<String, Object>> fetch(EndpointAddress address, String table, Map<String, Object> where) {
        Path file = file(address, table);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        List<Row> matches = read(file).stream().filter(row -> matches(row, where)).toList();
        if (matches.isEmpty()) {
            return Optional.empty();
        }
        if (matches.size() > 1) {
            throw new EnvelopeException(
                    "more than one row in " + table + " matches " + where
                            + "; a document read must locate exactly one");
        }
        Map<String, Object> document = new LinkedHashMap<>();
        document.put(SeedRows.ID, matches.getFirst().id());
        document.put(SeedRows.SEQ, matches.getFirst().seq());
        return Optional.of(document);
    }

    private static boolean matches(Row row, Map<String, Object> where) {
        for (Map.Entry<String, Object> setting : where.entrySet()) {
            Long actual = switch (setting.getKey()) {
                case SeedRows.ID -> row.id();
                case SeedRows.SEQ -> row.seq();
                default -> null;
            };
            if (actual == null || !(setting.getValue() instanceof Number expected)
                    || actual != expected.longValue()) {
                return false;
            }
        }
        return true;
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
     * Sets columns on the one row the settings locate. This driver's rows carry {@code id} and
     * {@code seq} and nothing else, so a column outside that pair is refused by name rather than
     * written and silently dropped on the next read.
     */
    @Override
    public void update(
            EndpointAddress address, String table, Map<String, Object> where, Map<String, Object> set) {
        List<Row> current = seeded(address, table);
        Row located = locate(current, table, where, "update");
        long id = located.id();
        long seq = located.seq();
        for (Map.Entry<String, Object> column : set.entrySet()) {
            if (!(column.getValue() instanceof Number value)) {
                throw new EnvelopeException(
                        "the table " + table + " holds whole numbers, so " + column.getKey()
                                + " cannot be set to " + column.getValue());
            }
            switch (column.getKey()) {
                case SeedRows.ID -> id = value.longValue();
                case SeedRows.SEQ -> seq = value.longValue();
                default -> throw new EnvelopeException(
                        "the table " + table + " has no column " + column.getKey()
                                + "; its rows carry " + SeedRows.ID + " and " + SeedRows.SEQ);
            }
        }
        Row replacement = new Row(id, seq);
        write(file(address, table), current.stream().map(row -> row == located ? replacement : row).toList());
    }

    @Override
    public void delete(EndpointAddress address, String table, Map<String, Object> where) {
        List<Row> current = seeded(address, table);
        Row located = locate(current, table, where, "delete");
        write(file(address, table), current.stream().filter(row -> row != located).toList());
    }

    /**
     * Adds rows to a seeded table. This driver's rows carry id and seq and nothing else, so a row naming
     * other columns is refused by name rather than written and silently dropped on the next read.
     */
    @Override
    public void insert(EndpointAddress address, String table, List<Map<String, Object>> rows) {
        List<Row> current = new ArrayList<>(seeded(address, table));
        for (Map<String, Object> row : rows) {
            if (!row.keySet().equals(Set.of(SeedRows.ID, SeedRows.SEQ))) {
                throw new EnvelopeException(
                        "the table " + table + " holds " + SeedRows.ID + " and " + SeedRows.SEQ
                                + ", and this row carries " + row.keySet());
            }
            current.add(new Row(number(row.get(SeedRows.ID)), number(row.get(SeedRows.SEQ))));
        }
        write(file(address, table), current);
    }

    private static long number(Object value) {
        if (!(value instanceof Number number)) {
            throw new EnvelopeException("the table holds whole numbers, so " + value + " cannot be written");
        }
        return number.longValue();
    }

    private List<Row> seeded(EndpointAddress address, String table) {
        Path file = file(address, table);
        if (!Files.exists(file)) {
            throw new EnvelopeException(
                    "the table " + table + " at " + address.text(DIRECTORY)
                            + " has not been seeded, so there is nothing to change");
        }
        return read(file);
    }

    /**
     * The one row the settings locate. Matching none is refused rather than passed over: the case is
     * about to wait for the change downstream, and a silent no-op turns that wait into a timeout that
     * reads like the product lost a change nobody made.
     */
    private static Row locate(List<Row> rows, String table, Map<String, Object> where, String what) {
        List<Row> matches = rows.stream().filter(row -> matches(row, where)).toList();
        if (matches.size() != 1) {
            throw new EnvelopeException(
                    "a " + what + " of " + table + " matching " + where + " moved " + matches.size()
                            + " rows; a valued change names exactly one");
        }
        return matches.getFirst();
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

    /**
     * Replaces the table's file with the given rows in one step: the text is staged beside the file and
     * moved into place, never written over it.
     *
     * <p>This driver is seeded and driven while the connector reads the same files, and both read a file
     * whole. Writing in place empties the file before the new text lands, so for the length of the write
     * the table reads as holding nothing while every row is still there - a reading that cannot be told
     * apart from an empty table. A move is a single step, so no reader is ever shown a table mid-write.
     */
    private static void write(Path file, List<Row> rows) {
        StringBuilder text = new StringBuilder(HEADER).append('\n');
        for (Row row : rows) {
            text.append(row.id()).append(',').append(row.seq()).append('\n');
        }
        try {
            Path staged = Files.createTempFile(file.getParent(), file.getFileName() + ".", STAGING_SUFFIX);
            try {
                Files.writeString(staged, text.toString());
                publish(staged, file);
            } finally {
                // Nothing reads a staging file, so one a failed write leaves behind is one nothing ever
                // collects. After the move it is already gone, and this call does nothing.
                Files.deleteIfExists(staged);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the table at " + file, e);
        }
    }

    /**
     * Puts the staged content where the table is, in one step, with the mode a reader other than this
     * process needs.
     *
     * <p>A temp file is created readable by its owner alone, and a move carries that mode onto the table.
     * The write this replaces left the mode the process's umask gives instead - readable by anyone. These
     * directories are read across users: a build running in a container and a harness running out here
     * share one, each reading what the other wrote. So the staged file is given that mode back before it
     * is moved into place, or a table this process wrote is one the other user cannot open.
     */
    private static void publish(Path staged, Path file) throws IOException {
        try {
            Files.setPosixFilePermissions(staged, PosixFilePermissions.fromString("rw-r--r--"));
        } catch (UnsupportedOperationException noPosixPermissions) {
            // A filesystem that carries no POSIX permissions has nothing to widen.
        }
        Files.move(staged, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
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
