package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * What the harness connector noted writing, read back from the directory its {@code write_witness} setting names.
 *
 * <p>Which writer a row goes to is decided inside the product, and the far end is the one witness of it the product
 * cannot shape: each of a sink's writers opens a connector of its own, and the connector notes every row it has put
 * in a table - which process and which writer, in which of that writer's batches, as what change. Each process
 * appends to a file of its own, in the order its writers wrote, so the lines of one writer are in its write order.
 */
final class WriteWitness {

    /** One row one writer wrote: {@code op} is {@code i}, {@code u} or {@code d}; {@code seq} as the row carried it. */
    record Written(long pid, String writer, long batch, String op, String table, String id, String seq) {

        /** The writer, told apart from every other writer in every process. */
        String writerId() {
            return pid + ":" + writer;
        }

        /** The batch, told apart from every other writer's batches. */
        String batchId() {
            return writerId() + ":" + batch;
        }
    }

    private final Path directory;

    WriteWitness(Path directory) {
        this.directory = directory;
    }

    /** Where the connector is told to note its writes: the value of its {@code write_witness} setting. */
    Path directory() {
        return directory;
    }

    /** Every row noted so far, process file by process file, each in its own write order. */
    List<Written> rows() {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        List<Written> rows = new ArrayList<>();
        try (Stream<Path> files = Files.list(directory)) {
            for (Path file : files.filter(path -> path.getFileName().toString().startsWith("writes-")).sorted()
                    .toList()) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    String[] cells = line.split("\t", -1);
                    if (cells.length == 7) {
                        rows.add(new Written(Long.parseLong(cells[0]), cells[1], Long.parseLong(cells[2]), cells[3],
                                cells[4], cells[5], cells[6]));
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("reading the write witness at " + directory, e);
        }
        return List.copyOf(rows);
    }

    /** The rows of {@code table} noted so far. */
    List<Written> rowsOf(String table) {
        return rows().stream().filter(row -> row.table().equals(table)).toList();
    }
}
